// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

// Package mq 提供 Kafka 消息发布，以及“显式指定分区”的消费能力。
//
// 这里故意不使用 Kafka consumer group 自动 rebalance：在 Actor 架构中，
// shard 的归属由外部 Controller/lease 决定，Kafka 只负责持久化消息和 checkpoint。
// 这样可以把“谁拥有这个 shard”和“从哪个 offset 继续消费”分开管理。
package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"goactor/message/mm"
)

const (
	// OffsetBeginning 从分区当前仍保留的最早消息开始消费。
	OffsetBeginning int64 = -2
	// OffsetEnd 从当前分区末尾开始，只消费之后新到达的消息。
	OffsetEnd int64 = -1
)

var ErrInvalid = errors.New("mq: invalid argument")

type TopicPartition struct {
	Topic     string
	Partition int32
}

// ShardOffset 表示配置 Topic 下的 shard 及要提交的下一条 offset。
type ShardOffset struct {
	ShardId int32
	Offset  int64
}

type Header = mm.Header

// PublishResult 是 broker 确认后的消息位置。
type PublishResult struct {
	TopicPartition
	Offset    int64
	Timestamp time.Time
}

// Message 是本包对 Kafka record 的抽象，与 core 使用什么业务序列化格式无关。
// Value 仍然是原始字节，由上层 Actor 决定如何反序列化。
type Message struct {
	TopicPartition
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// DecodeError 保存无法解码的原始 record（独立拥有字节缓冲区）。
// 调用方必须处理该位置或 Seek 重试，不能越过它提交 checkpoint。
type DecodeError struct {
	Message Message
	Err     error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("decode %s/%d/%d: %v", e.Message.Topic, e.Message.Partition, e.Message.Offset, e.Err)
}

func (e *DecodeError) Unwrap() error { return e.Err }

// IMQ 消费调用方显式分配的分区，永远不会自动提交 offset。
//
// 调用方应在 Snapshot/业务处理完成后，再把“连续完成的下一 offset”传给
// CommitOffset。并发约定：
//   - Subscribe/Unsubscribe/Seek/Pause/Resume/CommitOffset 由一个控制 goroutine 串行调用。
//   - Poll 由一个独立 goroutine 调用；Publish/DLQ 可由多个 goroutine 调用。
//   - Seek/Unsubscribe 前停止并等待 Poll 协程，停用 mailbox 中的旧消息后再切换。
//
// 本层不管理 mailbox，不撤回在途消息，也不负责提交顺序、重试或跨节点 fencing。
// 所有 shard ID 均为 int32，topic 只从配置读取。
type IMQ interface {
	Publish(ctx context.Context, message mm.Message) (PublishResult, error)
	Subscribe(ctx context.Context, shards ...int32) error
	Unsubscribe(ctx context.Context, shards ...int32) error
	SeekMessage(ctx context.Context, shard int32, offset int64) error
	PausePartition(ctx context.Context, shard int32) error
	ResumePartition(ctx context.Context, shard int32) error
	CommitOffset(ctx context.Context, shardOffsets ...ShardOffset) error
	DLQ(ctx context.Context, message mm.Message, reason string) error
	// Poll 返回当前可用的一批消息；消息和错误可能同时返回，必须分别检查。
	// 没有消息时阻塞，直到数据、错误、取消或客户端关闭。
	Poll(ctx context.Context) ([]mm.Message, error)
	// Close 由控制方调用一次；先停止新操作。它也会唤醒正在等待的 Poll/Publish。
	// 调用方负责取消投递循环并等待自己的 goroutine 退出。
	Close()
}
