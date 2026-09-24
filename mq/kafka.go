// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"goactor/message/mm"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string // 仅用于保存 checkpoint，不启用 consumer group 自动分配。
	DLQTopic string // 可为空；为空时不能调用 DLQ。
	ClientID string
	Timeout  time.Duration // 连接、发布和 checkpoint 请求超时，默认 5 秒。
}

// Kafka 是 franz-go 的最小 adapter，没有后台协程或额外锁。
// 必须遵守 IMQ 的调用约定；订阅状态和在途消息由控制方维护。
type Kafka struct {
	client *kgo.Client
	admin  *kadm.Client
	config KafkaConfig
}

var _ IMQ = (*Kafka)(nil)

func NewKafka(ctx context.Context, config KafkaConfig) (*Kafka, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(config.Brokers) == 0 || strings.TrimSpace(config.Topic) == "" ||
		strings.TrimSpace(config.GroupID) == "" || config.Timeout < 0 ||
		(config.DLQTopic != "" && (strings.TrimSpace(config.DLQTopic) == "" || config.DLQTopic == config.Topic)) {
		return nil, fmt.Errorf("%w: brokers, topic, group id and a distinct optional DLQ topic are required", ErrInvalid)
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("%w: empty broker", ErrInvalid)
		}
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return &Kafka{client: client, admin: kadm.NewClient(client), config: config}, nil
}

// Subscribe 内部读取 checkpoint；没有 checkpoint 则从头开始。
// 调用方只对尚未订阅的 shard 调用，避免重复添加导致消费位置变化。
func (k *Kafka) Subscribe(ctx context.Context, shards ...int32) error {
	if err := validateShards(ctx, shards...); err != nil {
		return err
	}
	if len(shards) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, k.config.Timeout)
	defer cancel()
	saved, err := k.admin.FetchOffsetsForTopics(ctx, k.config.GroupID, k.config.Topic)
	if err != nil {
		return err
	}
	offsets := make(map[int32]kgo.Offset, len(shards))
	for _, shard := range shards {
		checkpoint, exists := saved.Lookup(k.config.Topic, shard)
		if !exists {
			return fmt.Errorf("%w: shard %d not found", ErrInvalid, shard)
		}
		if checkpoint.Err != nil {
			return checkpoint.Err
		}
		offset := checkpoint.At
		if offset < 0 {
			offset = OffsetBeginning
		}
		offsets[shard] = kgo.NoResetOffset().At(offset)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	k.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{k.config.Topic: offsets})
	return nil
}

func (k *Kafka) Unsubscribe(parent context.Context, shards ...int32) error {
	if err := validateShards(parent, shards...); err != nil {
		return err
	}
	partitions := map[string][]int32{k.config.Topic: shards}
	k.client.RemoveConsumePartitions(partitions)
	k.client.ResumeFetchPartitions(partitions) // 清除暂停标记，供下次订阅使用。
	return nil
}

// SeekMessage 只改变本地位置，不提交 checkpoint；保留暂停状态。
// 调用方必须先停止 Poll 并停用已经投递的旧消息，且 shard 应已订阅。
func (k *Kafka) SeekMessage(ctx context.Context, shard int32, offset int64) error {
	if err := validateShards(ctx, shard); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("%w: seek offset must be nonnegative", ErrInvalid)
	}
	k.client.RemoveConsumePartitions(map[string][]int32{k.config.Topic: {shard}})
	k.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		k.config.Topic: {shard: kgo.NoResetOffset().At(offset)},
	})
	return nil
}

func (k *Kafka) PausePartition(ctx context.Context, shard int32) error {
	if err := validateShards(ctx, shard); err != nil {
		return err
	}
	k.client.PauseFetchPartitions(map[string][]int32{k.config.Topic: {shard}})
	return nil
}

func (k *Kafka) ResumePartition(ctx context.Context, shard int32) error {
	if err := validateShards(ctx, shard); err != nil {
		return err
	}
	k.client.ResumeFetchPartitions(map[string][]int32{k.config.Topic: {shard}})
	return nil
}

// CommitOffset 原样提交 next offset；控制方保证连续完成、单调推进及失败重试。
func (k *Kafka) CommitOffset(ctx context.Context, shardOffsets ...ShardOffset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(shardOffsets) == 0 {
		return nil
	}
	offsets := make(kadm.Offsets)
	for _, item := range shardOffsets {
		if item.ShardId < 0 || item.Offset < 0 {
			return fmt.Errorf("%w: negative shard or offset", ErrInvalid)
		}
		offsets.AddOffset(k.config.Topic, item.ShardId, item.Offset, -1)
	}
	ctx, cancel := context.WithTimeout(ctx, k.config.Timeout)
	defer cancel()
	responses, err := k.admin.CommitOffsets(ctx, k.config.GroupID, offsets)
	if err != nil {
		return err
	}
	return responses.Error() // 分区级失败不能只检查顶层 err。
}

func (k *Kafka) Publish(ctx context.Context, message mm.Message) (PublishResult, error) {
	record, err := encodeRecord(message, k.config.Topic)
	if err != nil {
		return PublishResult{}, err
	}
	return k.produce(ctx, record)
}

func (k *Kafka) DLQ(ctx context.Context, message mm.Message, reason string) error {
	if k.config.DLQTopic == "" || message.Offset < 0 {
		return fmt.Errorf("%w: DLQ topic and nonnegative source offset are required", ErrInvalid)
	}
	record, err := encodeRecord(message, k.config.Topic)
	if err != nil {
		return err
	}
	record.Headers = append(record.Headers,
		kgo.RecordHeader{Key: "mq.source.topic", Value: []byte(record.Topic)},
		kgo.RecordHeader{Key: "mq.source.partition", Value: []byte(strconv.FormatInt(int64(record.Partition), 10))},
		kgo.RecordHeader{Key: "mq.source.offset", Value: []byte(strconv.FormatInt(message.Offset, 10))},
		kgo.RecordHeader{Key: "mq.reason", Value: []byte(reason)},
	)
	record.Topic = k.config.DLQTopic
	_, err = k.produce(ctx, record)
	return err // 写死信不提交源 offset。
}

// Poll 独立协程调用。SDK 负责等待和预取，本层没有定时轮询或自建缓存。
func (k *Kafka) Poll(ctx context.Context) ([]mm.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decodeFetches(k.client.PollFetches(ctx))
}

func (k *Kafka) Close() { k.client.Close() }

func (k *Kafka) produce(ctx context.Context, record *kgo.Record) (PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return PublishResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, k.config.Timeout)
	defer cancel()
	results := k.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return PublishResult{}, err
	}
	if len(results) != 1 || results[0].Record == nil {
		return PublishResult{}, fmt.Errorf("publish: missing broker result")
	}
	r := results[0].Record
	return PublishResult{
		TopicPartition: TopicPartition{Topic: r.Topic, Partition: r.Partition},
		Offset:         r.Offset, Timestamp: r.Timestamp,
	}, nil
}

func encodeRecord(message mm.Message, topic string) (*kgo.Record, error) {
	m, err := FromMMMessage(message, topic)
	if err != nil {
		return nil, err
	}
	record := &kgo.Record{Topic: m.Topic, Partition: m.Partition, Key: m.Key, Value: m.Value}
	for _, h := range m.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return record, nil
}

// 转换整批 records；保留所有正常消息和所有分区/解码错误。
func decodeFetches(fetches kgo.Fetches) ([]mm.Message, error) {
	var failures []error
	for _, e := range fetches.Errors() {
		failures = append(failures, fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err))
	}
	result := make([]mm.Message, 0, fetches.NumRecords())
	fetches.EachRecord(func(r *kgo.Record) {
		m := Message{
			TopicPartition: TopicPartition{Topic: r.Topic, Partition: r.Partition},
			Offset:         r.Offset, Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value), Timestamp: r.Timestamp,
		}
		for _, h := range r.Headers {
			m.Headers = append(m.Headers, Header{Key: h.Key, Value: bytes.Clone(h.Value)})
		}
		message, err := ToMMMessage(m)
		if err != nil {
			failures = append(failures, &DecodeError{Message: m, Err: err})
		} else {
			result = append(result, message)
		}
	})
	return result, errors.Join(failures...)
}

func validateShards(ctx context.Context, shards ...int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, shard := range shards {
		if shard < 0 {
			return fmt.Errorf("%w: shard must be nonnegative", ErrInvalid)
		}
	}
	return nil
}
