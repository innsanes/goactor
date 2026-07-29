package registry

import "context"

type IRegistry interface {
	Register(ctx context.Context, key, node string, ttl int) error
	Done() <-chan struct{}
	Close() error
}
