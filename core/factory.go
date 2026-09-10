package core

import "errors"

type Factory struct {
	list map[string]func(config ActorConfig) IActor
}

func NewFactory() *Factory {
	return &Factory{
		list: make(map[string]func(config ActorConfig) IActor),
	}
}

//r := core.NewFactory()
//type A struct{}
//r.Register("a", core.FactoryWarp[A](core.NewHandler[A]()))

func FactoryWarp[T IState](handler *Handlers[T]) func(config ActorConfig) IActor {
	return func(config ActorConfig) IActor {
		actor := NewActor[T](config)
		actor.handler = handler
		return actor
	}
}

func (r *Factory) Register(actorType string, f func(config ActorConfig) IActor) {
	r.list[actorType] = f
}

func (r *Factory) New(config ActorConfig) (actor IActor, err error) {
	f, ok := r.list[config.Type]
	if !ok {
		return nil, errors.New("actor " + config.Type + " not found")
	}
	actor = f(config)
	return actor, nil
}
