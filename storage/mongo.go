// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"goactor/message/mm"
)

// MongoStore 是基于 MongoDB 的 Actor 快照存储。
type MongoStore struct {
	client *mongo.Client
	coll   *mongo.Collection
	owned  bool
}

// NewMongo 创建并连接 MongoDB，返回的存储负责连接生命周期。
func NewMongo(ctx context.Context, uri, database, collection string) (*MongoStore, error) {
	if strings.TrimSpace(uri) == "" {
		return nil, fmt.Errorf("%w: empty uri", ErrInvalid)
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}
	if err = client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	store, err := NewMongoWithClient(client, database, collection)
	if err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	store.owned = true
	return store, nil
}

// NewMongoWithClient 使用调用方提供的 Mongo client。
func NewMongoWithClient(client *mongo.Client, database, collection string) (*MongoStore, error) {
	if client == nil || strings.TrimSpace(database) == "" {
		return nil, fmt.Errorf("%w: invalid mongo client or database", ErrInvalid)
	}
	if strings.TrimSpace(collection) == "" {
		collection = DefaultCollection
	}
	return &MongoStore{
		client: client,
		coll:   client.Database(database).Collection(collection),
	}, nil
}

// Close 仅关闭由 NewMongo 创建的连接。
func (s *MongoStore) Close(ctx context.Context) error {
	if err := s.ready(); err != nil {
		return err
	}
	if !s.owned {
		return nil
	}
	return s.client.Disconnect(ctx)
}

// Save 根据输入数量选择单写或批量写。
func (s *MongoStore) Save(ctx context.Context, inputs ...mm.ActorSnapshot) ([]string, error) {
	switch len(inputs) {
	case 0:
		return []string{}, nil
	case 1:
		if err := s.saveOne(ctx, inputs[0]); err != nil {
			return nil, err
		}
		return []string{inputs[0].ActorId}, nil
	default:
		return s.saveBatch(ctx, inputs)
	}
}

// saveOne 保存单个快照。库中版本高于传入版本时拒绝写入。
func (s *MongoStore) saveOne(ctx context.Context, input mm.ActorSnapshot) error {
	if err := s.ready(); err != nil {
		return err
	}
	doc, err := snapshotFromMessage(input)
	if err != nil {
		return err
	}

	filter := bson.M{
		"_id": doc.ActorID,
		"$or": bson.A{
			bson.M{"version": bson.M{"$lte": doc.Version}},
			bson.M{"version": bson.M{"$exists": false}},
		},
	}
	result, err := s.coll.UpdateOne(ctx, filter, bson.M{
		"$set": bson.M{
			"actorType": doc.ActorType,
			"version":   doc.Version,
			"offset":    doc.Offset,
			"state":     doc.State,
			"dedup":     doc.Dedup,
		},
	}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("save %q: %w", doc.ActorID, ErrStale)
		}
		return fmt.Errorf("save %q: %w", doc.ActorID, err)
	}
	if result.MatchedCount == 0 && result.UpsertedCount == 0 {
		return fmt.Errorf("save %q: %w", doc.ActorID, ErrStale)
	}
	return nil
}

// saveBatch 使用 unordered bulk write 保存多个快照，并返回成功写入的 Actor ID。
func (s *MongoStore) saveBatch(ctx context.Context, inputs []mm.ActorSnapshot) ([]string, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		return []string{}, nil
	}
	if err := validateBatch(inputs); err != nil {
		return nil, err
	}

	models := make([]mongo.WriteModel, 0, len(inputs))
	for _, input := range inputs {
		doc, _ := snapshotFromMessage(input)
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{
				"_id": doc.ActorID,
				"$or": bson.A{
					bson.M{"version": bson.M{"$lte": doc.Version}},
					bson.M{"version": bson.M{"$exists": false}},
				},
			}).
			SetUpdate(bson.M{"$set": bson.M{
				"actorType": doc.ActorType,
				"version":   doc.Version,
				"offset":    doc.Offset,
				"state":     doc.State,
				"dedup":     doc.Dedup,
			}}).
			SetUpsert(true))
	}

	_, err := s.coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err == nil {
		success := make([]string, 0, len(inputs))
		for _, input := range inputs {
			success = append(success, input.ActorId)
		}
		return success, nil
	}
	success := successfulActorIDs(inputs, err)
	return success, classifyBulkError(inputs, err)
}

// Load 原子地读取快照并将 version 加一，返回自增后的版本。
func (s *MongoStore) Load(ctx context.Context, actorID string) (mm.ActorSnapshot, error) {
	if err := s.ready(); err != nil {
		return mm.ActorSnapshot{}, err
	}
	if err := validateActorID(actorID); err != nil {
		return mm.ActorSnapshot{}, err
	}

	var doc Snapshot
	err := s.coll.FindOneAndUpdate(ctx,
		bson.M{"_id": actorID},
		bson.M{"$inc": bson.M{"version": 1}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return mm.ActorSnapshot{}, fmt.Errorf("load %q: %w", actorID, ErrNotFound)
		}
		return mm.ActorSnapshot{}, fmt.Errorf("load %q: %w", actorID, err)
	}
	return doc.message(), nil
}

func classifyBulkError(inputs []mm.ActorSnapshot, err error) error {
	var bulk mongo.BulkWriteException
	if !errors.As(err, &bulk) {
		return err
	}
	stale := make([]string, 0)
	other := make([]error, 0)
	for _, item := range bulk.WriteErrors {
		id := "?"
		if item.Index >= 0 && item.Index < len(inputs) {
			id = inputs[item.Index].ActorId
		}
		if mongo.IsDuplicateKeyError(item) {
			stale = append(stale, id)
		} else {
			other = append(other, fmt.Errorf("actor %q: %w", id, item))
		}
	}
	if bulk.WriteConcernError != nil {
		other = append(other, *bulk.WriteConcernError)
	}

	if len(stale) != 0 {
		other = append(other, fmt.Errorf("%w: actors=%v", ErrStale, stale))
	}
	if len(other) == 0 {
		return err
	}
	return errors.Join(other...)
}

func successfulActorIDs(inputs []mm.ActorSnapshot, err error) []string {
	var bulk mongo.BulkWriteException
	if !errors.As(err, &bulk) {
		return nil
	}
	if bulk.WriteConcernError != nil {
		// Write concern failure does not identify which operations became
		// durable, so do not claim any ID as confirmed successful.
		return nil
	}

	failed := make(map[int]struct{}, len(bulk.WriteErrors))
	for _, item := range bulk.WriteErrors {
		failed[item.Index] = struct{}{}
	}

	success := make([]string, 0, len(inputs)-len(failed))
	for index, input := range inputs {
		if _, exists := failed[index]; !exists {
			success = append(success, input.ActorId)
		}
	}
	return success
}

func (s *MongoStore) ready() error {
	if s == nil || s.client == nil || s.coll == nil {
		return ErrUninitialized
	}
	return nil
}
