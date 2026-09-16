package mm

import "time"

const (
	CmdActorSnapshot = "actor/snapshot"
	CmdActorIdle     = "actor/idle"
	CmdActorAlive    = "actor/alive"
	CmdActorReady    = "actor/ready"
	CmdActorStopped  = "actor/stopped"
)

type ActorSnapshot struct {
	ActorID string
	Version int64
	Offset  int64
	State   any
	Dedup   []string
}

type ActorIdle struct {
}

type ActorAlive struct {
	Time time.Time
}

type ActorReady struct {
}

type ActorStopped struct {
}
