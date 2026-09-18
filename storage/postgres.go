// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	_ "github.com/jackc/pgx/v5/stdlib"

	"goactor/message/mm"
)

const defaultPostgresTable = "actor"

// PostgresStore 是基于 PostgreSQL 的 Actor 快照存储。
//
// 表结构由 EnsureSchema 创建。PostgreSQL 中一个 actor 对应一行，
// version 同时作为快照版本和写入 fencing 条件。
type PostgresStore struct {
	db    *sql.DB
	table string
	owned bool
}

// NewPostgres 创建并连接 PostgreSQL，返回的存储负责连接生命周期。
// 调用方应在首次 Save/Load 前调用 EnsureSchema。
func NewPostgres(ctx context.Context, dsn, table string) (*PostgresStore, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: empty postgres dsn", ErrInvalid)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store, err := NewPostgresWithDB(db, table)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.owned = true
	return store, nil
}

// NewPostgresWithDB 使用调用方提供的 database/sql 连接池。
func NewPostgresWithDB(db *sql.DB, table string) (*PostgresStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: postgres db is nil", ErrInvalid)
	}
	if strings.TrimSpace(table) == "" {
		table = defaultPostgresTable
	}
	quotedTable, err := quoteQualifiedIdentifier(table)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, table: quotedTable}, nil
}

// EnsureSchema 创建 PostgreSQL 快照表。已存在时不修改现有表。
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if err := s.ready(); err != nil {
		return err
	}
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    actor_id   TEXT PRIMARY KEY,
    actor_type TEXT NOT NULL DEFAULT '',
    version    BIGINT NOT NULL,
    "offset"  BIGINT NOT NULL,
    state      JSONB,
    dedup      JSONB NOT NULL DEFAULT '[]'::jsonb
)`, s.table)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create postgres snapshot table: %w", err)
	}
	return nil
}

// Close 仅关闭由 NewPostgres 创建的连接池。
func (s *PostgresStore) Close(context.Context) error {
	if err := s.ready(); err != nil {
		return err
	}
	if !s.owned {
		return nil
	}
	return s.db.Close()
}

// Save 根据输入数量选择单行写入或批量写入。
func (s *PostgresStore) Save(ctx context.Context, inputs ...mm.ActorSnapshot) ([]string, error) {
	switch len(inputs) {
	case 0:
		return []string{}, nil
	case 1:
		if err := s.saveOnePostgres(ctx, inputs[0]); err != nil {
			return nil, err
		}
		return []string{inputs[0].ActorId}, nil
	default:
		return s.saveBatchPostgres(ctx, inputs)
	}
}

func (s *PostgresStore) saveOnePostgres(ctx context.Context, input mm.ActorSnapshot) error {
	if err := s.ready(); err != nil {
		return err
	}
	doc, state, dedup, err := encodePostgresSnapshot(input)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
INSERT INTO %s AS target
    (actor_id, actor_type, version, "offset", state, dedup)
VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
ON CONFLICT (actor_id) DO UPDATE SET
    actor_type = EXCLUDED.actor_type,
    version    = EXCLUDED.version,
    "offset"  = EXCLUDED."offset",
    state      = EXCLUDED.state,
    dedup      = EXCLUDED.dedup
WHERE target.version <= EXCLUDED.version
RETURNING actor_id`, s.table)

	var actorID string
	err = s.db.QueryRowContext(ctx, query,
		doc.ActorID,
		doc.ActorType,
		doc.Version,
		doc.Offset,
		state,
		dedup,
	).Scan(&actorID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("save %q: %w", doc.ActorID, ErrStale)
	}
	if err != nil {
		return fmt.Errorf("save %q: %w", doc.ActorID, err)
	}
	return nil
}

func (s *PostgresStore) saveBatchPostgres(ctx context.Context, inputs []mm.ActorSnapshot) ([]string, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := validateBatch(inputs); err != nil {
		return nil, err
	}

	args := make([]any, 0, len(inputs)*6)
	values := make([]string, 0, len(inputs))
	for index, input := range inputs {
		doc, state, dedup, err := encodePostgresSnapshot(input)
		if err != nil {
			return nil, fmt.Errorf("batch item %d: %w", index, err)
		}
		base := index*6 + 1
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d::jsonb, $%d::jsonb)",
			base, base+1, base+2, base+3, base+4, base+5))
		args = append(args, doc.ActorID, doc.ActorType, doc.Version, doc.Offset, state, dedup)
	}

	query := fmt.Sprintf(`
INSERT INTO %s AS target
    (actor_id, actor_type, version, "offset", state, dedup)
VALUES %s
ON CONFLICT (actor_id) DO UPDATE SET
    actor_type = EXCLUDED.actor_type,
    version    = EXCLUDED.version,
    "offset"  = EXCLUDED."offset",
    state      = EXCLUDED.state,
    dedup      = EXCLUDED.dedup
WHERE target.version <= EXCLUDED.version
RETURNING actor_id`, s.table, strings.Join(values, ", "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("save postgres snapshots: %w", err)
	}
	defer rows.Close()

	success := make([]string, 0, len(inputs))
	for rows.Next() {
		var actorID string
		if err := rows.Scan(&actorID); err != nil {
			return success, fmt.Errorf("scan saved actor id: %w", err)
		}
		success = append(success, actorID)
	}
	if err := rows.Err(); err != nil {
		return success, fmt.Errorf("read saved actor ids: %w", err)
	}
	if len(success) == len(inputs) {
		return success, nil
	}

	successSet := make(map[string]struct{}, len(success))
	for _, actorID := range success {
		successSet[actorID] = struct{}{}
	}
	stale := make([]string, 0, len(inputs)-len(success))
	for _, input := range inputs {
		if _, ok := successSet[input.ActorId]; !ok {
			stale = append(stale, input.ActorId)
		}
	}
	return success, fmt.Errorf("%w: actors=%v", ErrStale, stale)
}

// Load 原子地读取一行并将 version 加一，返回自增后的快照。
func (s *PostgresStore) Load(ctx context.Context, actorID string) (mm.ActorSnapshot, error) {
	if err := s.ready(); err != nil {
		return mm.ActorSnapshot{}, err
	}
	if err := validateActorID(actorID); err != nil {
		return mm.ActorSnapshot{}, err
	}

	query := fmt.Sprintf(`
UPDATE %s
SET version = version + 1
WHERE actor_id = $1
RETURNING actor_id, actor_type, version, "offset", state, dedup`, s.table)

	var (
		doc       Snapshot
		stateJSON []byte
		dedupJSON []byte
	)
	if err := s.db.QueryRowContext(ctx, query, actorID).Scan(
		&doc.ActorID,
		&doc.ActorType,
		&doc.Version,
		&doc.Offset,
		&stateJSON,
		&dedupJSON,
	); errors.Is(err, sql.ErrNoRows) {
		return mm.ActorSnapshot{}, fmt.Errorf("load %q: %w", actorID, ErrNotFound)
	} else if err != nil {
		return mm.ActorSnapshot{}, fmt.Errorf("load %q: %w", actorID, err)
	}

	if err := decodePostgresState(stateJSON, dedupJSON, &doc); err != nil {
		return mm.ActorSnapshot{}, fmt.Errorf("load %q: %w", actorID, err)
	}
	return doc.message(), nil
}

func encodePostgresSnapshot(input mm.ActorSnapshot) (Snapshot, []byte, []byte, error) {
	doc, err := snapshotFromMessage(input)
	if err != nil {
		return Snapshot{}, nil, nil, err
	}
	state, err := json.Marshal(doc.State)
	if err != nil {
		return Snapshot{}, nil, nil, fmt.Errorf("encode state: %w", err)
	}
	dedupValue := doc.Dedup
	if dedupValue == nil {
		dedupValue = []string{}
	}
	dedup, err := json.Marshal(dedupValue)
	if err != nil {
		return Snapshot{}, nil, nil, fmt.Errorf("encode dedup: %w", err)
	}
	return doc, state, dedup, nil
}

func decodePostgresState(stateJSON, dedupJSON []byte, doc *Snapshot) error {
	if len(stateJSON) != 0 && !isJSONNull(stateJSON) {
		if err := json.Unmarshal(stateJSON, &doc.State); err != nil {
			return fmt.Errorf("decode state: %w", err)
		}
	}
	if len(dedupJSON) != 0 && !isJSONNull(dedupJSON) {
		if err := json.Unmarshal(dedupJSON, &doc.Dedup); err != nil {
			return fmt.Errorf("decode dedup: %w", err)
		}
	}
	return nil
}

func isJSONNull(value []byte) bool {
	return strings.TrimSpace(string(value)) == "null"
}

func quoteQualifiedIdentifier(value string) (string, error) {
	parts := strings.Split(value, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if !validIdentifierPart(part) {
			return "", fmt.Errorf("%w: invalid postgres table name %q", ErrInvalid, value)
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func validIdentifierPart(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if index == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func (s *PostgresStore) ready() error {
	if s == nil || s.db == nil || s.table == "" {
		return ErrUninitialized
	}
	return nil
}

var _ IStore = (*PostgresStore)(nil)
