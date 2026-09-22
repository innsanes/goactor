// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

// Package mq 提供“显式指定分区”的 Kafka 消费能力。
//
// 这里故意不使用 Kafka consumer group 自动 rebalance：在 Actor 架构中，
// shard 的归属由外部 Controller/lease 决定，Kafka 只负责持久化消息和 checkpoint。
// 这样可以把“谁拥有这个 shard”和“从哪个 offset 继续消费”分开管理。
package mq

import (
	"context"
	"errors"
	"time"
)

const (
	// OffsetBeginning 从分区当前仍保留的最早消息开始消费。
	OffsetBeginning int64 = -2
	// OffsetEnd 从当前分区末尾开始，只消费之后新到达的消息。
	OffsetEnd int64 = -1
	// OffsetCommitted 是 Subscribe 内部使用的 checkpoint 控制值；调用方不需要传入。
	OffsetCommitted int64 = -3
)

var (
	ErrInvalid           = errors.New("mq: invalid argument")
	ErrClosed            = errors.New("mq: closed")
	ErrNotSubscribed     = errors.New("mq: partition is not subscribed")
	ErrAlreadySubscribed = errors.New("mq: partition is already subscribed")
)

type TopicPartition struct {
	Topic     string
	Partition int32
}

// TopicPartitionOffset 表示一个分区以及要提交的下一条 offset。
type TopicPartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Header struct {
	Key   string
	Value []byte
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

// IMQ 消费调用方显式分配的分区，永远不会自动提交 offset。
//
// 调用方应在 Snapshot/业务处理完成后，再把“连续完成的下一 offset”传给
// CommitOffset。Seek 或释放分区前，调用方还必须停止处理已经 Poll 出去的消息；
// 这些已经交付给调用方的消息，本包无法撤回。
type IMQ interface {
	// Subscribe 批量显式订阅分区，并由实现内部从 GroupID checkpoint 恢复。
	Subscribe(ctx context.Context, partitions ...TopicPartition) error
	Unsubscribe(partitions ...TopicPartition) error
	SeekMessage(partition TopicPartition, offset int64) error
	PausePartition(partition TopicPartition) error
	ResumePartition(partition TopicPartition) error
	// CommitOffset 提交下一条要消费的 offset，而不是最后一条已处理的 offset。
	CommitOffset(ctx context.Context, partition ...TopicPartitionOffset) error
	// DLQ 把无法处理的消息写入死信队列，并等待 broker 确认；不会提交源分区 offset。
	DLQ(ctx context.Context, message Message, reason string) error
	// Poll 可能同时返回消息和错误：错误可能只属于某个分区，不能因此丢弃其他分区的消息。
	Poll(ctx context.Context, maxRecords int) ([]Message, error)
	Close()
}
