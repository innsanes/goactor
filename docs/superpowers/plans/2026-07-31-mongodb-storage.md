# MongoDB Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a MongoDB-backed Actor snapshot storage API with whole-snapshot Get and version-protected Set operations.

**Architecture:** `storage.Snapshot` carries the Actor identity, schema version, monotonic state version, update timestamp, and separate BSON `State` and `Timers` documents. `MongoDB` owns a Mongo client and collection; `Set` upserts a snapshot only when its state version is newer than the stored one, preventing a delayed checkpoint from replacing a newer snapshot.

**Tech Stack:** Go 1.26, official MongoDB Go driver, BSON.

---

### Task 1: Define the storage contract

**Files:**
- Modify: `storage/storage.go`

- [x] **Step 1: Add the snapshot type and storage interface**

```go
type Snapshot struct {
	ActorID       string    `bson:"actor_id"`
	SchemaVersion int64     `bson:"schema_version"`
	StateVersion  int64     `bson:"state_version"`
	UpdatedAt     time.Time `bson:"updated_at"`
	State         bson.Raw  `bson:"state"`
	Timers        bson.Raw  `bson:"timers"`
}

type IStorage interface {
	Get(ctx context.Context, actorID string) (*Snapshot, error)
	Set(ctx context.Context, snapshot Snapshot) error
}
```

### Task 2: Implement MongoDB Get and Set

**Files:**
- Modify: `storage/mongodb.go`
- Modify: `go.mod`
- Modify: `go.sum`

- [x] **Step 1: Add the official MongoDB driver**

Run: `go get go.mongodb.org/mongo-driver/v2/mongo`

- [x] **Step 2: Add MongoDB construction and lifecycle helpers**

Create `NewMongoDB(client *mongo.Client, database, collection string) (*MongoDB, error)`, `ConnectMongoDB(ctx, uri, database, collection string) (*MongoDB, error)`, and `Close(ctx)`.

- [x] **Step 3: Implement Get**

Use `FindOne` with `bson.D{{Key: "_id", Value: actorID}}`. Translate `mongo.ErrNoDocuments` to `storage.ErrSnapshotNotFound`.

- [x] **Step 4: Implement Set**

Validate the snapshot. Use `UpdateOne` with a filter that permits a missing document or an existing document whose `state_version` is less than the proposed version. Set `updated_at` when omitted. Return `ErrStaleSnapshot` when the database contains an equal or newer version.

- [x] **Step 5: Format and verify compilation**

Run: `gofmt -w storage/storage.go storage/mongodb.go` and `go test ./...`.

Automated tests are intentionally omitted at the user's explicit request; this task uses compilation verification only.
