package core

import "errors"

type ActorRegistry struct {
	list map[string]func(config ActorConfig) IActor
}

func NewActorRegistry() *ActorRegistry {
	return &ActorRegistry{
		list: make(map[string]func(config ActorConfig) IActor),
	}
}

func (r *ActorRegistry) Register(actorType string, f func(config ActorConfig) IActor) {
	r.list[actorType] = f
}

func (r *ActorRegistry) GetFunc(actorType string) (f func(config ActorConfig) IActor, err error) {
	f, ok := r.list[actorType]
	if !ok {
		return nil, errors.New("actor " + actorType + " not found")
	}
	return f, nil
}
