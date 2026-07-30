package core

const (
	ActorLevelUnit   int8 = iota
	ActorLevelPlayer int8 = iota
	ActorLevelGroup
	ActorLevelServer
	ActorLevelGlobal
)

type Message struct{}

func SendMessage(manager *Manager, actor *Actor, target string, msg Message) error {
	if target == actor.id {
		err := actor.Receive(msg)
		return err
	}

	targetActor, exist := manager.GetActor(target)
	if exist {
		err := targetActor.Receive(msg)
		return err
	}

	return nil
}
