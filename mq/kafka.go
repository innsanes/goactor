// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaConfig 配置一个“直接指定分区”的消费者以及一个 DLQ 生产者。
// GroupID 在这里仅作为 checkpoint 的命名空间使用；不要和 Kafka 自动管理的
// consumer group 共用，否则两套消费语义可能互相覆盖 offset。
type KafkaConfig struct {
	Brokers  []string
	GroupID  string
	DLQTopic string
	ClientID string
	// Timeout 限制连接、读取 checkpoint、提交 offset 和写 DLQ 的等待时间，默认 5 秒。
	Timeout time.Duration
}

// 这里拆出两个私有接口，是为了让单元测试可以用 fake client 验证 IMQ 语义，
// 而不需要启动真实 Kafka。它们不是给业务方实现的扩展接口。
type kafkaClient interface {
	AddConsumePartitions(map[string]map[int32]kgo.Offset)
	RemoveConsumePartitions(map[string][]int32)
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
	PollRecords(context.Context, int) kgo.Fetches
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type offsetAdmin interface {
	FetchOffsetsForTopics(context.Context, string, ...string) (kadm.OffsetResponses, error)
	CommitOffsets(context.Context, string, kadm.Offsets) (kadm.OffsetResponses, error)
}

// Kafka 串行化分区分配、checkpoint、暂停/恢复和缓存读取。
//
// 它没有偷偷把消息投递到后台 channel，而是由 Poll 明确返回消息，因此 Seek
// 后不会继续泄漏 Seek 之前已经缓存在后台 channel 里的旧消息。每个分区只能
// 同时由一个拥有者处理；这个独占性必须由外部 Controller/lease/fencing 保证，
// 本类型本身不负责节点间抢占和故障转移。
type Kafka struct {
	mu            sync.Mutex
	client        kafkaClient
	admin         offsetAdmin
	config        KafkaConfig
	subscriptions map[TopicPartition]bool // value: paused
	done          chan struct{}
	closed        bool
}

var _ IMQ = (*Kafka)(nil)

func NewKafka(ctx context.Context, config KafkaConfig) (*Kafka, error) {
	if len(config.Brokers) == 0 || strings.TrimSpace(config.GroupID) == "" || strings.TrimSpace(config.DLQTopic) == "" || config.Timeout < 0 {
		return nil, fmt.Errorf("%w: brokers, group id and DLQ topic are required; timeout cannot be negative", ErrInvalid)
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("%w: empty broker", ErrInvalid)
		}
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		// 不设置 ConsumerGroup：分区由 Subscribe 显式指定，checkpoint 由 CommitOffset 显式提交。
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if config.ClientID != "" {
		opts = append(opts, kgo.ClientID(config.ClientID))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return &Kafka{client: client, admin: kadm.NewClient(client), config: config,
		subscriptions: make(map[TopicPartition]bool), done: make(chan struct{})}, nil
}

// Subscribe 一次显式接管一个或多个分区，并从 Kafka checkpoint 开始消费。
//
// 批量订阅时，所有 partition 会合并为一次 FetchOffsetsForTopics 请求，避免
// 每个 partition 重复查询整个 Topic 的 offset。没有历史 checkpoint 时从
// OffsetBeginning 开始。需要重放时，订阅后使用 SeekMessage。重复订阅会报错，
// 防止悄悄回退正在处理中的分区。
func (k *Kafka) Subscribe(ctx context.Context, partitions ...TopicPartition) error {
	if len(partitions) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.ready(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	seen := make(map[TopicPartition]struct{}, len(partitions))
	checkpointTopics := make([]string, 0)
	checkpointTopicSeen := make(map[string]struct{})
	for _, p := range partitions {
		if err := validatePartition(p); err != nil {
			return err
		}
		if _, duplicate := seen[p]; duplicate {
			return fmt.Errorf("%w: duplicate partition %v", ErrAlreadySubscribed, p)
		}
		seen[p] = struct{}{}
		if _, exists := k.subscriptions[p]; exists {
			return ErrAlreadySubscribed
		}
		if _, exists := checkpointTopicSeen[p.Topic]; !exists {
			checkpointTopicSeen[p.Topic] = struct{}{}
			checkpointTopics = append(checkpointTopics, p.Topic)
		}
	}

	var checkpoints kadm.OffsetResponses
	if len(checkpointTopics) > 0 {
		// 一个请求覆盖本次订阅涉及的所有 Topic；返回结果中仍按
		// topic/partition 查找具体 checkpoint。
		requestCtx, cancel := context.WithTimeout(ctx, k.config.Timeout)
		defer cancel()
		var err error
		checkpoints, err = k.admin.FetchOffsetsForTopics(requestCtx, k.config.GroupID, checkpointTopics...)
		if err != nil {
			return fmt.Errorf("fetch checkpoint: %w", err)
		}
	}

	assignments := make(map[string]map[int32]kgo.Offset)
	for _, p := range partitions {
		saved, exists := checkpoints.Lookup(p.Topic, p.Partition)
		if !exists {
			return fmt.Errorf("%w: partition not found: %v", ErrInvalid, p)
		}
		if saved.Err != nil {
			return fmt.Errorf("fetch checkpoint %v: %w", p, saved.Err)
		}
		offset := saved.At
		if offset < 0 {
			// 没有历史 checkpoint 时从该分区最早仍保留的消息开始。
			offset = OffsetBeginning
		}
		if assignments[p.Topic] == nil {
			assignments[p.Topic] = make(map[int32]kgo.Offset)
		}
		assignments[p.Topic][p.Partition] = kgo.NoResetOffset().At(offset)
	}
	k.client.AddConsumePartitions(assignments)
	for _, p := range partitions {
		k.subscriptions[p] = false
	}
	return nil
}

// Unsubscribe drops unpolled buffers without committing. It is idempotent.
func (k *Kafka) Unsubscribe(partitions ...TopicPartition) error {
	if len(partitions) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.ready(); err != nil {
		return err
	}
	remove := make(map[string][]int32)
	seen := make(map[TopicPartition]struct{}, len(partitions))
	for _, p := range partitions {
		if err := validatePartition(p); err != nil {
			return err
		}
		if _, duplicate := seen[p]; duplicate {
			continue
		}
		seen[p] = struct{}{}
		if _, exists := k.subscriptions[p]; exists {
			remove[p.Topic] = append(remove[p.Topic], p.Partition)
		}
	}
	if len(remove) == 0 {
		return nil
	}
	k.client.RemoveConsumePartitions(remove)
	// franz-go 的暂停状态可能在 Remove 后保留；恢复一次，避免下次 Subscribe
	// 接管同一分区时意外保持暂停状态。
	k.client.ResumeFetchPartitions(remove)
	for _, p := range partitions {
		delete(k.subscriptions, p)
	}
	return nil
}

// SeekMessage 丢弃尚未 Poll 交付的缓存，并把下一次拉取位置重置到绝对 offset。
// 它只改变本地消费位置，不修改 Kafka 中已经提交的 checkpoint，也不会自动恢复暂停。
func (k *Kafka) SeekMessage(p TopicPartition, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("%w: seek requires an absolute offset", ErrInvalid)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.checkSubscribed(p); err != nil {
		return err
	}
	// 这里采用 Remove + Add，而不是只调用 SetOffsets：刚 Subscribe 但尚未 Poll
	// 的分区也能立即生效，并且会使客户端已有的预取缓存失效，避免先交付旧消息。
	k.client.RemoveConsumePartitions(partitionMap(p))
	k.client.AddConsumePartitions(assignment(p, offset))
	return nil
}

func (k *Kafka) PausePartition(p TopicPartition) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.checkSubscribed(p); err != nil {
		return err
	}
	// Pause 只停止继续从该分区拉取，不会撤回已经 Poll 返回给上层的消息。
	k.client.PauseFetchPartitions(partitionMap(p))
	k.subscriptions[p] = true
	return nil
}

func (k *Kafka) ResumePartition(p TopicPartition) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.checkSubscribed(p); err != nil {
		return err
	}
	// 恢复后，Poll 才会继续交付这个分区的新消息；不会改变当前位置或 checkpoint。
	k.client.ResumeFetchPartitions(partitionMap(p))
	k.subscriptions[p] = false
	return nil
}

// CommitOffset 一次原样提交一个或多个 partition 的 nextOffset。
//
// 例如消息 100 已处理完成，表示 0..100 的连续前缀已完成，应提交 101。
// core 应先根据 Actor snapshot/Inflight 算出每个 partition 的连续完成前缀，再
// 批量调用这里；Seek 回退只会改变本地重放位置，不代表可以把持久化 checkpoint
// 一起回退。所有 partition 会通过一次 Kafka 请求提交。
func (k *Kafka) CommitOffset(ctx context.Context, partitions ...TopicPartitionOffset) error {
	if len(partitions) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	offsets := make(kadm.Offsets)
	seen := make(map[TopicPartition]struct{}, len(partitions))
	for _, p := range partitions {
		if p.Offset < 0 {
			return fmt.Errorf("%w: negative commit offset for %v", ErrInvalid, topicPartition(p))
		}
		tp := topicPartition(p)
		if _, duplicate := seen[tp]; duplicate {
			return fmt.Errorf("%w: duplicate commit partition %v", ErrInvalid, tp)
		}
		seen[tp] = struct{}{}
		if err := k.checkSubscribed(tp); err != nil {
			return err
		}
		offsets.AddOffset(p.Topic, p.Partition, p.Offset, -1)
	}
	requestCtx, cancel := context.WithTimeout(ctx, k.config.Timeout)
	defer cancel()
	responses, err := k.admin.CommitOffsets(requestCtx, k.config.GroupID, offsets)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	for _, p := range partitions {
		tp := topicPartition(p)
		response, exists := responses.Lookup(p.Topic, p.Partition)
		if !exists {
			return fmt.Errorf("commit %v: missing broker response", tp)
		}
		if response.Err != nil {
			return fmt.Errorf("commit %v at %d: %w", tp, p.Offset, response.Err)
		}
	}
	return nil
}

func topicPartition(p TopicPartitionOffset) TopicPartition {
	return TopicPartition{Topic: p.Topic, Partition: p.Partition}
}

func partitionMap(p TopicPartition) map[string][]int32 {
	// franz-go 的分区 API 使用 topic -> []partition；这里一次操作一个分区。
	return map[string][]int32{p.Topic: {p.Partition}}
}

func assignment(p TopicPartition, offset int64) map[string]map[int32]kgo.Offset {
	// NoResetOffset().At(offset) 把 offset 解释为明确的绝对位置，避免客户端
	// 因默认 reset 策略偷偷跳到开头或结尾。
	return map[string]map[int32]kgo.Offset{p.Topic: {p.Partition: kgo.NoResetOffset().At(offset)}}
}

// DLQ 保留原消息的 key、payload 和 headers，并追加源消息元数据后写入死信 topic。
//
// 写 DLQ 和提交源 offset 不是一个事务：如果写入结果不明确而重试，可能产生重复
// 的 DLQ 消息，可用 source topic/partition/offset 做幂等去重。只有 DLQ 得到 broker
// 确认后，上层才应把源消息纳入连续完成前缀并提交源 offset。
func (k *Kafka) DLQ(ctx context.Context, message Message, reason string) error {
	if err := validatePartition(message.TopicPartition); err != nil {
		return err
	}
	if message.Offset < 0 {
		return fmt.Errorf("%w: negative message offset", ErrInvalid)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.ready(); err != nil {
		return err
	}
	if message.Topic == k.config.DLQTopic {
		return fmt.Errorf("%w: refusing DLQ loop", ErrInvalid)
	}
	record := &kgo.Record{Topic: k.config.DLQTopic, Key: append([]byte(nil), message.Key...), Value: append([]byte(nil), message.Value...)}
	for _, h := range message.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)})
	}
	for _, h := range []Header{
		// 这些 header 让运维或重试程序可以定位原始消息；如果原消息里已有同名
		// header，后追加的值会成为 Kafka 客户端读取时看到的最后一个值。
		{Key: "mq.source.topic", Value: []byte(message.Topic)},
		{Key: "mq.source.partition", Value: []byte(strconv.FormatInt(int64(message.Partition), 10))},
		{Key: "mq.source.offset", Value: []byte(strconv.FormatInt(message.Offset, 10))},
		{Key: "mq.source.timestamp", Value: []byte(message.Timestamp.UTC().Format(time.RFC3339Nano))},
		{Key: "mq.reason", Value: []byte(reason)},
	} {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	requestCtx, cancel := context.WithTimeout(ctx, k.config.Timeout)
	defer cancel()
	if err := k.client.ProduceSync(requestCtx, record).FirstErr(); err != nil {
		return fmt.Errorf("DLQ: %w", err)
	}
	return nil
}

// Poll 等待消息或 context 取消。
//
// 每轮只读取客户端当前已经缓存的 records，不在 mutex 内阻塞等 broker；这样即使
// 某分区空闲，其他控制操作（Pause/Seek/Unsubscribe/Close）也能及时拿到锁执行。
// Poll 可能同时返回部分消息和部分分区错误，调用方必须分别处理。
func (k *Kafka) Poll(ctx context.Context, maxRecords int) ([]Message, error) {
	if maxRecords <= 0 {
		return nil, fmt.Errorf("%w: maxRecords must be positive", ErrInvalid)
	}
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		k.mu.Lock()
		if err := k.ready(); err != nil {
			k.mu.Unlock()
			return nil, err
		}
		// nil context 表示这里只消费本地缓存；等待由下面的 10ms ticker 和 ctx.Done
		// 控制，避免 franz-go 的 PollRecords 长时间持锁等待网络。
		fetches := k.client.PollRecords(nil, maxRecords)
		messages := make([]Message, 0, fetches.NumRecords())
		fetches.EachRecord(func(r *kgo.Record) {
			m := Message{TopicPartition: TopicPartition{r.Topic, r.Partition}, Offset: r.Offset, Timestamp: r.Timestamp, Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...)}
			for _, h := range r.Headers {
				m.Headers = append(m.Headers, Header{h.Key, append([]byte(nil), h.Value...)})
			}
			messages = append(messages, m)
		})
		var failures []error
		for _, e := range fetches.Errors() {
			failures = append(failures, fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err))
		}
		k.mu.Unlock()
		if len(messages) != 0 || len(failures) != 0 {
			return messages, errors.Join(failures...)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-k.done:
			return nil, ErrClosed
		case <-timer.C:
		}
	}
}

func (k *Kafka) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return
	}
	k.closed = true
	if k.done != nil {
		close(k.done)
	}
	if k.client != nil {
		k.client.Close()
	}
}

func (k *Kafka) ready() error {
	if k.closed || k.client == nil {
		return ErrClosed
	}
	return nil
}

func (k *Kafka) checkSubscribed(p TopicPartition) error {
	if err := validatePartition(p); err != nil {
		return err
	}
	if err := k.ready(); err != nil {
		return err
	}
	if _, exists := k.subscriptions[p]; !exists {
		return ErrNotSubscribed
	}
	return nil
}

func validatePartition(p TopicPartition) error {
	if strings.TrimSpace(p.Topic) == "" || p.Partition < 0 {
		return fmt.Errorf("%w: topic must be nonempty and partition nonnegative", ErrInvalid)
	}
	return nil
}
