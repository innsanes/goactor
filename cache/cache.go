package cache

import (
	"context"
	"time"
)

type ICache interface {
	HSet(ctx context.Context, key, field string, value any) error
	HGet(ctx context.Context, key, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	Expire(ctx context.Context, key string, duration time.Duration) error
}
