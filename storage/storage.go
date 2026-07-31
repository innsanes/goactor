package storage

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	// ErrStorageNotFound is returned when no storage exists for an actor.
	ErrStorageNotFound = errors.New("actor storage not found")
	// ErrStaleStorage is returned when storage already contains an equal or newer storage.
	ErrStaleStorage = errors.New("actor storage is stale")
	// ErrInvalidStorage is returned when a storage cannot be stored safely.
	ErrInvalidStorage = errors.New("invalid actor storage")
)

type Actor struct {
	ActorID      string       `bson:"_id"`
	StateVersion int64        `bson:"state_version"`
	UpdatedAt    time.Time    `bson:"updated_at"`
	State        bson.Raw     `bson:"state"`
	Timers       []ActorTimer `bson:"timers"`
}

type ActorTimer struct {
	Key     string `bson:"key"`
	Message any    `bson:"message"`
	When    int64  `bson:"when"`
}

type IStorage interface {
	Get(ctx context.Context, actorID string) (*Actor, error)
	Set(ctx context.Context, actor Actor, state any) error
}
