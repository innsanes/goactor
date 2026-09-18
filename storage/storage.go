// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"goactor/message/mm"
)

const (
	DefaultCollection = "actor"
)

var (
	ErrNotFound      = errors.New("storage: snapshot not found")
	ErrStale         = errors.New("storage: snapshot version is stale")
	ErrInvalid       = errors.New("storage: invalid snapshot")
	ErrUninitialized = errors.New("storage: storage is not initialized")
)

// IStore 定义 Actor 快照的基础操作。
type IStore interface {
	// Save 根据快照数量选择单写或批量写，返回已经成功写入的 Actor ID。
	Save(ctx context.Context, snapshots ...mm.ActorSnapshot) (success []string, err error)
	Load(ctx context.Context, actorID string) (mm.ActorSnapshot, error)
}

// Snapshot 是 MongoDB 中保存的一条 Actor 快照。
type Snapshot struct {
	ActorID   string   `bson:"_id"`
	ActorType string   `bson:"actorType,omitempty"`
	Version   int64    `bson:"version"`
	Offset    int64    `bson:"offset"`
	State     any      `bson:"state"`
	Dedup     []string `bson:"dedup"`
}

func snapshotFromMessage(in mm.ActorSnapshot) (Snapshot, error) {
	if strings.TrimSpace(in.ActorId) == "" || !utf8.ValidString(in.ActorId) {
		return Snapshot{}, fmt.Errorf("%w: invalid actor id", ErrInvalid)
	}
	if in.Version < 0 || in.Offset < 0 {
		return Snapshot{}, fmt.Errorf("%w: negative version or offset", ErrInvalid)
	}
	return Snapshot{
		ActorID:   in.ActorId,
		ActorType: in.ActorType,
		Version:   in.Version,
		Offset:    in.Offset,
		State:     in.State,
		Dedup:     append([]string(nil), in.Dedup...),
	}, nil
}

func (s Snapshot) message() mm.ActorSnapshot {
	return mm.ActorSnapshot{
		ActorId:   s.ActorID,
		ActorType: s.ActorType,
		Version:   s.Version,
		Offset:    s.Offset,
		State:     s.State,
		Dedup:     append([]string(nil), s.Dedup...),
	}
}

func validateBatch(snapshots []mm.ActorSnapshot) error {
	seen := make(map[string]struct{}, len(snapshots))
	for i, item := range snapshots {
		if _, err := snapshotFromMessage(item); err != nil {
			return fmt.Errorf("batch item %d: %w", i, err)
		}
		if _, exists := seen[item.ActorId]; exists {
			return fmt.Errorf("%w: duplicate actor id %q", ErrInvalid, item.ActorId)
		}
		seen[item.ActorId] = struct{}{}
	}
	return nil
}

func validateActorID(actorID string) error {
	if strings.TrimSpace(actorID) == "" || !utf8.ValidString(actorID) {
		return fmt.Errorf("%w: invalid actor id", ErrInvalid)
	}
	return nil
}

var _ IStore = (*MongoStore)(nil)
