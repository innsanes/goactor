package core

const (
	MessageActorSave int = iota
	MessageActorSnapShot
)

type RequestCheckpoint struct {
}

type ResponseSnapshotReady struct {
	ActorID string
	Version int64
	Offset  int64
	State   []byte
	Dedup   []byte
}
