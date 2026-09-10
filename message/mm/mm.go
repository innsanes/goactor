package mm

import "time"

const (
	CmdActorSnapshot = "actor/snapshot"
	CmdActorStorage  = "actor/storage"
	CmdActorIdle     = "actor/idle"
	CmdActorAlive    = "actor/alive"
	CmdActorReady    = "actor/ready"
)

type ActorSnapshot struct {
	ActorID string
	Version int64
	Offset  int64
	State   any
	Dedup   []string
}

type ActorStorage struct {
	Offset int64
}

type ActorIdle struct {
}

type ActorAlive struct {
	Time time.Time
}

type ActorReady struct {
}
