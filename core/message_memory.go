package core

const (
	MessageActorCheckpoint int = iota
	MessageActorSnapShot
	MessageActorIdle
	MessageActorStop
)

type Checkpoint struct {
}

type Snapshot struct {
	ActorID string
	Version int64
	Offset  int64
	State   []byte
	Dedup   []byte
}

type Idle struct{}

type Stop struct{}
