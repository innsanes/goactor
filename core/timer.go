package core

import (
	"context"
	"errors"
	"goactor/cache"
	"goactor/cc"
	"goactor/structure"
	"time"

	"github.com/redis/go-redis/v9"
)

type Timer struct {
	actorId string
	timer   *time.Timer
	heap    *structure.QuadHeap[string, Message, int64]
	cache   cache.ICache
	//storage storage.Istorage
}

type TimerData struct {
	key  string
	msg  Message
	when int64
}

func TimerDataDecode(b []byte) (*TimerData, error) {
	return &TimerData{}, nil
}

func TimerDataEncode(*TimerData) []byte {
	return []byte{}
}

func NewTimer(id string) *Timer {
	timer := time.NewTimer(time.Second)
	timer.Stop()
	return &Timer{
		actorId: id,
		timer:   timer,
		heap:    structure.NewQuadHeap[string, Message, int64](1),
		//cache: cache.NewRedis(),
	}
}

func (t *Timer) cacheKey() string {
	return cache.BuildKey(t.actorId, cc.CacheActorTimer, t.actorId)
}

func (t *Timer) CacheRestore() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	vals, err := t.cache.HGetAll(ctx, t.cacheKey())
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}

	for key, val := range vals {
		var data *TimerData
		data, err = TimerDataDecode([]byte(val))
		if err != nil {
			// log
			continue
		}
		t.heap.Upsert(key, data.msg, data.when)
	}
	t.Calibration()
	return nil
}

func (t *Timer) Add(key string, m Message, when int64) {
	peek, ok := t.heap.Peek()
	t.heap.Upsert(key, m, when)

	if ok && peek.When <= when {
		return
	}

	t.Calibration()
}

func (t *Timer) Upsert(key string, m Message, when int64) {
	t.heap.Upsert(key, m, when)
	t.cacheUpsert(key, m, when)
}

func (t *Timer) cacheUpsert(key string, m Message, when int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	data := TimerDataEncode(&TimerData{
		key:  key,
		msg:  m,
		when: when,
	})

	err := t.cache.HSet(ctx, t.cacheKey(), key, data)
	if err != nil {
		// log
		return
	}
	err = t.cache.Expire(ctx, t.cacheKey(), time.Hour*6)
	if err != nil {
		// log
		return
	}
}

func (t *Timer) Del(key string) {
	peek, ok := t.heap.Peek()
	t.heap.Remove(key)

	if !ok || peek.Key != key {
		return
	}

	t.Calibration()
}

func (t *Timer) Remove(key string) {
	t.heap.Remove(key)
	t.cacheRemove(key)
}

func (t *Timer) cacheRemove(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	err := t.cache.HDel(ctx, t.cacheKey(), key)
	if err != nil {
		// log
		return
	}
	err = t.cache.Expire(ctx, t.cacheKey(), time.Hour*6)
	if err != nil {
		// log
		return
	}
}

func (t *Timer) Chan() <-chan time.Time {
	return t.timer.C
}

func (t *Timer) Calibration() {
	peek, ok := t.heap.Peek()
	if !ok {
		t.timer.Stop()
		return
	}
	t.timer.Reset(time.Second * time.Duration(peek.When-NowUnix()))
}

func (t *Timer) Peek() (structure.HeapItem[string, Message, int64], bool) {
	return t.heap.Peek()
}

func (t *Timer) All() []structure.HeapItem[string, Message, int64] {
	return []structure.HeapItem[string, Message, int64]{}
}
