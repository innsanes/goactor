package core

import "time"

const (
	MActorCheckpoint = "MActorCheckpoint"
	MActorSnapShot   = "MActorSnapShot"
	MActorStorage    = "MActorStorage"
	MActorIdle       = "MActorIdle"
	MActorAlive      = "MActorAlive"
	MActorStop       = "MActorStop"
)

type Checkpoint struct {
}

type Snapshot struct {
	ActorID string
	Version int64
	Offset  int64
	State   any
	Dedup   []string
}

type Storage struct {
	ActorID string
	Offset  int64
}

type Idle struct{}

type Stop struct{}

type Alive struct {
	ActorID string
	Time    time.Time
}
