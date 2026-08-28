package core

const (
	MActorCheckpoint = "MActorCheckpoint"
	MActorSnapShot   = "MActorSnapShot"
	MActorIdle       = "MActorIdle"
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

type Idle struct{}

type Stop struct{}
