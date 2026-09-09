package core

import "time"

const (
	MActorCheckpoint   = "MActorCheckpoint"
	MActorSnapShot     = "MActorSnapShot"
	MActorStorage      = "MActorStorage"
	MActorIdle         = "MActorIdle"
	MActorAlive        = "MActorAlive"
	MActorStop         = "MActorStop"
	MActorChannelFull  = "MActorChannelFull"
	MActorChannelReady = "MActorChannelReady"
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

type Idle struct {
	ActorID string
}

type Stop struct{}

type Alive struct {
	ActorID string
	Time    time.Time
}

type ChannelFull struct{}

type ChannelReady struct {
	ActorID string
}
