package core

const (
	MessageActorCheckpoint int = iota
	MessageActorSnapShotReady
	MessageActorIdle
	MessageActorStop
)

type Checkpoint struct {
}

type SnapshotReady struct {
	ActorID string
	Version int64
	Offset  int64
	State   []byte
	Dedup   []byte
}

type Idle struct{}

type Stop struct{}
