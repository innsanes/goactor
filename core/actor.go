package core

import (
	"context"
	"errors"
	"fmt"
	"goactor/cache"
	"goactor/cc"
	"goactor/registry"
	"goactor/storage"
	"goactor/structure"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ActorChannelCap = 1024
)

type IActor interface {
	structure.IId
	Start() error
	Stop()
	Receive(...Message) error
}

type Actor[T any] struct {
	id           string
	ch           chan Message
	reg          registry.IRegistry
	ren          registry.IRegistration
	stop         chan struct{}
	node         string
	level        int8
	timer        *Timer
	ctx          context.Context
	cancel       context.CancelFunc
	closing      bool
	state        T
	stateVersion int64
	store        storage.IStorage
	cache        cache.ICache
}

func NewActor[T any](id string, node string) *Actor[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Actor[T]{
		id:      id,
		ch:      make(chan Message, ActorChannelCap),
		stop:    make(chan struct{}),
		node:    node,
		level:   ActorLevelUnit,
		timer:   NewTimer(id),
		ctx:     ctx,
		cancel:  cancel,
		closing: false,
	}
}

func (a *Actor[T]) Id() string {
	return a.id
}
func (a *Actor[T]) SetLevel(level int8) {
	a.level = level
}

func (a *Actor[T]) Start() error {
	err := a.prepare()
	if err != nil {
		return err
	}
	err = a.beforeStart()
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			msg := recover()
			if msg != nil {

			}
			a.shutdown()
		}()

		for {
			select {
			case msg := <-a.ch:
				a.handle(msg)
			case <-a.timer.Chan():
				a.handleTimer()
			case <-a.ren.Done():
				a.close()
				return
			case <-a.stop:
				a.close()
				a.drain()
				a.beforeStop()
				return
			}
		}
	}()

	return nil
}

func (a *Actor[T]) Stop() {
	select {
	case <-a.stop:
		return
	default:
		close(a.stop)
	}
}

func (a *Actor[T]) prepare() error {
	reg, err := a.reg.Register(a.ctx, a.id, a.node)
	if err != nil {
		a.cancel()
		return err
	}
	a.ren = reg
	return nil
}

func (a *Actor[T]) close() {
	a.closing = true
}

func (a *Actor[T]) drain() {

	for {
		select {
		case task := <-a.ch:
			a.handle(task)
		default:
			return
		}
	}
}

func (a *Actor[T]) shutdown() {
	close(a.ch)
	_ = a.ren.Close()
	a.cancel()
}

func (a *Actor[T]) beforeStart() error {
	err := a.GetStorageData()
	if err != nil {
		return err
	}
	err = a.GetCacheData()
	if err != nil {
		return err
	}
	return nil
}

func (a *Actor[T]) beforeStop() {
	err := a.SetStorageData()
	if err != nil {
		return
	}
}

func (a *Actor[T]) Receive(msg ...Message) error {
	if a.closing == true {
		return errors.New("actor is closing")
	}
	for i := range msg {
		select {
		case a.ch <- msg[i]:
		default:
			return fmt.Errorf("actor channel full")
		}
	}
	return nil
}

func (a *Actor[T]) handleTimer() {
	if a.closing == true {
		return
	}

	now := NowUnix()
	for {
		item, ok := a.timer.Peek()
		if !ok || item.When > now {
			break
		}

		select {
		case a.ch <- item.Value:
			a.timer.Remove(item.Key)
		default:
			break
		}
	}

	a.timer.Calibration()
	return
}

func (a *Actor[T]) handle(msg Message) {
}

func (a *Actor[T]) SetStorageData() error {
	a.stateVersion++
	timers := a.timer.All()
	data := storage.Actor{
		ActorID:      a.id,
		StateVersion: a.stateVersion,
		UpdatedAt:    Now(),
		Timers:       make([]storage.ActorTimer, 0, len(timers)),
	}
	for i := range timers {
		data.Timers = append(data.Timers, storage.ActorTimer{
			Key:     timers[i].Key,
			Message: timers[i].Value,
			When:    timers[i].When,
		})
	}
	timeout, cancelFunc := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancelFunc()
	err := a.store.Set(timeout, data, a.state)
	if err != nil {
		return err
	}
	return nil
}

func (a *Actor[T]) GetStorageData() error {
	timeout, cancelFunc := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancelFunc()
	data, err := a.store.Get(timeout, a.id)
	if err != nil {
		return err
	}
	a.stateVersion = data.StateVersion
	//a.state = data.State
	err = a.timer.CacheRestore()
	return nil
}

func (a *Actor[T]) CacheKey() string {
	return cache.BuildKey(a.id, cc.CacheActorState, a.id)
}

func (a *Actor[T]) SetCacheData(field string, value any) error {
	timeout, cancelFunc := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancelFunc()
	err := a.cache.HSet(timeout, a.CacheKey(), field, value)
	if err != nil {
		return err
	}
	return nil
}

func (a *Actor[T]) GetCacheData() error {
	timeout, cancelFunc := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancelFunc()
	vals, err := a.cache.HGetAll(timeout, a.CacheKey())
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	err = a.timer.CacheRestore()
	if err != nil {
		return err
	}
	for range vals {
	}
	return nil
}
