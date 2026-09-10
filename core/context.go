package core

import (
	"context"
	"time"
)

type RootContext struct {
}

type Context[T IState] struct {
	actor     *Actor[T]
	message   Message
	sequences map[string]uint64
}

func NewContext[T IState](actor *Actor[T], message Message) *Context[T] {
	return &Context[T]{
		actor:     actor,
		message:   message,
		sequences: make(map[string]uint64),
	}
}

func NewRootContext[T IState](actor *Actor[T], cause string) *Context[T] {
	traceId := GenerateMessageID(cause, "", actor.id, 0)
	ref := MessageRef{
		Type: actor.typ,
		Id:   actor.id,
	}
	message := Message{
		Sender:    ref,
		Receiver:  ref,
		TraceId:   traceId,
		MessageId: traceId,
		Type:      MessageTypeMemory,
		Command:   "",
		Payload:   nil,
	}
	return &Context[T]{
		actor:     actor,
		message:   message,
		sequences: make(map[string]uint64),
	}
}

func (c *Context[T]) GetState() *T {
	return &c.actor.state
}

func (c *Context[T]) GetActorId() string {
	return c.actor.id
}

func (c *Context[T]) WithTimeout(t time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.actor.ctx, t)
	return ctx, cancel
}

func (c *Context[T]) WithDeadline(t time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(c.actor.ctx, t)
	return ctx, cancel
}

func (c *Context[T]) NewTimer(key string, cmd string, payload any, when int64) {
	c.actor.AddTimer(key, cmd, payload, when)
}

func (c *Context[T]) NewMessage(receiverId, receiverType, command string, payload []byte) Message {
	return Message{
		Command: command,
		Sender: MessageRef{
			Type: c.actor.id,
			Id:   c.actor.typ,
		},
		Receiver: MessageRef{
			Type: receiverType,
			Id:   receiverId,
		},
		TraceId:   c.message.TraceId,
		MessageId: c.generateMessageId(c.actor.id, receiverId),
		Payload:   payload,
	}
}

func (c *Context[T]) nextSequence(receiverId string) uint64 {
	seq := c.sequences[receiverId]
	c.sequences[receiverId] = seq + 1
	return seq
}

func (c *Context[T]) generateMessageId(senderId, receiverId string) string {
	seq := c.nextSequence(receiverId)
	messageID := GenerateMessageID(c.message.MessageId, senderId, receiverId, seq)
	return messageID
}
