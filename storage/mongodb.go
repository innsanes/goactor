package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const defaultMongoCollection = "actor_state"

type MongoDB struct {
	client     *mongo.Client
	collection *mongo.Collection
}

var _ IStorage = (*MongoDB)(nil)

// NewMongoDB creates a storage instance using an existing MongoDB client.
func NewMongoDB(client *mongo.Client, database, collection string) (*MongoDB, error) {
	if client == nil {
		return nil, fmt.Errorf("new MongoDB storage: client is nil")
	}
	if strings.TrimSpace(database) == "" {
		return nil, fmt.Errorf("new MongoDB storage: database is empty")
	}
	if strings.TrimSpace(collection) == "" {
		collection = defaultMongoCollection
	}

	return &MongoDB{
		client:     client,
		collection: client.Database(database).Collection(collection),
	}, nil
}

// ConnectMongoDB connects to MongoDB and returns a storage instance.
func ConnectMongoDB(ctx context.Context, uri, database, collection string) (*MongoDB, error) {
	if strings.TrimSpace(uri) == "" {
		return nil, fmt.Errorf("connect MongoDB storage: uri is empty")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect MongoDB storage: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping MongoDB storage: %w", err)
	}

	storage, err := NewMongoDB(client, database, collection)
	if err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return storage, nil
}

// Close disconnects the MongoDB client owned by this storage instance.
func (m *MongoDB) Close(ctx context.Context) error {
	if m == nil || m.client == nil {
		return nil
	}
	if err := m.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect MongoDB storage: %w", err)
	}
	return nil
}

// Get loads the latest persistent state for actorID.
func (m *MongoDB) Get(ctx context.Context, actorID string) (*Actor, error) {
	if err := m.validateInitialized(); err != nil {
		return nil, err
	}
	if err := validateActorID(actorID); err != nil {
		return nil, err
	}

	var actor Actor
	err := m.collection.FindOne(ctx, bson.D{{Key: "_id", Value: actorID}}).Decode(&actor)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("get actor storage %q: %w", actorID, ErrStorageNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get actor storage %q: %w", actorID, err)
	}
	return &actor, nil
}

// Set writes actor only when its state version is newer than the stored one.
func (m *MongoDB) Set(ctx context.Context, actor Actor, state any) error {
	if err := m.validateInitialized(); err != nil {
		return err
	}
	if err := validateActor(actor); err != nil {
		return err
	}
	if actor.UpdatedAt.IsZero() {
		actor.UpdatedAt = time.Now().UTC()
	}

	filter := bson.D{
		{Key: "_id", Value: actor.ActorID},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "state_version", Value: bson.D{{Key: "$lt", Value: actor.StateVersion}}}},
			bson.D{{Key: "state_version", Value: bson.D{{Key: "$exists", Value: false}}}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "state_version", Value: actor.StateVersion},
		{Key: "updated_at", Value: actor.UpdatedAt},
		{Key: "state", Value: actor.State},
		{Key: "timers", Value: actor.Timers},
	}}}

	result, err := m.collection.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("set actor storage %q version %d: %w", actor.ActorID, actor.StateVersion, ErrStaleStorage)
	}
	if err != nil {
		return fmt.Errorf("set actor storage %q version %d: %w", actor.ActorID, actor.StateVersion, err)
	}
	if result.MatchedCount == 0 && result.UpsertedCount == 0 {
		return fmt.Errorf("set actor storage %q version %d: %w", actor.ActorID, actor.StateVersion, ErrStaleStorage)
	}
	return nil
}

func (m *MongoDB) validateInitialized() error {
	if m == nil || m.collection == nil {
		return fmt.Errorf("MongoDB storage is not initialized")
	}
	return nil
}

func validateActorID(actorID string) error {
	if strings.TrimSpace(actorID) == "" {
		return fmt.Errorf("actor storage: %w: actor ID is empty", ErrInvalidStorage)
	}
	return nil
}

func validateActor(actor Actor) error {
	if err := validateActorID(actor.ActorID); err != nil {
		return fmt.Errorf("set actor storage: %w", err)
	}
	if actor.StateVersion < 0 {
		return fmt.Errorf("set actor storage %q: %w: state version is negative", actor.ActorID, ErrInvalidStorage)
	}
	if err := validateDocument("state", actor.State); err != nil {
		return fmt.Errorf("set actor storage %q: %w", actor.ActorID, err)
	}

	timerKeys := make(map[string]struct{}, len(actor.Timers))
	for index, timer := range actor.Timers {
		if strings.TrimSpace(timer.Key) == "" {
			return fmt.Errorf("set actor storage %q: %w: timer %d key is empty", actor.ActorID, ErrInvalidStorage, index)
		}
		if _, exists := timerKeys[timer.Key]; exists {
			return fmt.Errorf("set actor storage %q: %w: duplicate timer key %q", actor.ActorID, ErrInvalidStorage, timer.Key)
		}
		timerKeys[timer.Key] = struct{}{}
	}
	return nil
}

func validateDocument(name string, document bson.Raw) error {
	if len(document) == 0 {
		return fmt.Errorf("%w: %s document is empty", ErrInvalidStorage, name)
	}
	if err := document.Validate(); err != nil {
		return fmt.Errorf("%w: %s document is invalid: %w", ErrInvalidStorage, name, err)
	}
	return nil
}

// EncodeDocument encodes an actor-owned Go value as a BSON document.
func (m *MongoDB) EncodeDocument(value any) (bson.Raw, error) {
	document, err := bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode BSON document: %w", err)
	}
	return bson.Raw(document), nil
}

// DecodeDocument decodes a BSON document into an actor-owned Go value.
func (m *MongoDB) DecodeDocument(document bson.Raw, value any) error {
	if len(document) == 0 {
		return fmt.Errorf("decode BSON document: %w: document is empty", ErrInvalidStorage)
	}
	if err := document.Validate(); err != nil {
		return fmt.Errorf("decode BSON document: %w: %w", ErrInvalidStorage, err)
	}
	if err := bson.Unmarshal(document, value); err != nil {
		return fmt.Errorf("decode BSON document: %w", err)
	}
	return nil
}
