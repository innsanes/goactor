package core

import (
	"sync/atomic"
	"time"
)

var GlobalTimeOffset int64

func NowUnix() int64 {
	return time.Now().Unix() + atomic.LoadInt64(&GlobalTimeOffset)
}

func Now() time.Time {
	return time.Unix(NowUnix(), 0)
}

func IncrGlobalTimeOffset(inc int64) {
	atomic.AddInt64(&GlobalTimeOffset, inc)
}
