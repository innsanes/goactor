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
	Offset int64
}

type Idle struct {
}

type Stop struct{}

type Alive struct {
	Time time.Time
}

type ChannelFull struct{}

type ChannelReady struct {
}
