package core

const (
	ActorLevelUnit   int8 = iota
	ActorLevelPlayer int8 = iota
	ActorLevelGroup
	ActorLevelServer
	ActorLevelGlobal
)

type Message struct{}
