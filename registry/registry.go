package registry

import "context"

type IRegistry interface {
	Register(ctx context.Context, key, node string) (IRegistration, error)
	Get(ctx context.Context, key string) (string, error)
}

type IRegistration interface {
	Done() <-chan struct{}
	Close() error
}
