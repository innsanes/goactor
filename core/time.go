package core

import (
	"sync/atomic"
	"time"
)

var GlobalTimeOffset int64

func Now() int64 {
	return time.Now().Unix() + atomic.LoadInt64(&GlobalTimeOffset)
}

func IncrGlobalTimeOffset(inc int64) {
	atomic.AddInt64(&GlobalTimeOffset, inc)
}
