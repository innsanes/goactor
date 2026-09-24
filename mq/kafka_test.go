// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func oneFetch(record *kgo.Record, otherError error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: record.Topic,
		Partitions: []kgo.FetchPartition{
			{Partition: record.Partition, Records: []*kgo.Record{record}},
			{Partition: -1, Err: otherError},
		},
	}}}}
}

func TestSingleRecordAndPartitionError(t *testing.T) {
	original := conversionMessage()
	record, err := encodeRecord(original, "actors")
	requireNoError(t, err)
	record.Offset = 42
	messages, err := decodeFetches(oneFetch(record, kerr.UnknownTopicOrPartition))
	if !errors.Is(err, kerr.UnknownTopicOrPartition) || len(messages) != 1 ||
		messages[0].Offset != 42 || messages[0].MessageId != original.MessageId ||
		!reflect.DeepEqual(messages[0].Headers, original.Headers) {
		t.Fatalf("record/error lost: %#v, %v", messages, err)
	}
	again, err := encodeRecord(messages[0], "actors")
	requireNoError(t, err)
	if !reflect.DeepEqual(again.Value, record.Value) || !reflect.DeepEqual(again.Headers, record.Headers) {
		t.Fatal("binary payload or headers changed")
	}
}

func TestDecodeWholeBatch(t *testing.T) {
	records := make([]*kgo.Record, 4)
	for i := range records {
		record, err := encodeRecord(conversionMessage(), "actors")
		requireNoError(t, err)
		record.Offset = int64(10 + i)
		record.Partition = int32(i / 2)
		records[i] = record
	}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: "actors",
		Partitions: []kgo.FetchPartition{
			{Partition: 0, Records: records[:2]},
			{Partition: 1, Records: records[2:]},
		},
	}}}}
	messages, err := decodeFetches(fetches)
	requireNoError(t, err)
	if len(messages) != len(records) {
		t.Fatalf("decoded %d messages, want %d", len(messages), len(records))
	}
	for i, message := range messages {
		if message.Offset != records[i].Offset {
			t.Fatalf("record %d lost or reordered: %#v", i, message)
		}
	}

	// 同一批中正常记录、两个解码错误、一个分区错误必须全部保留。
	records[1].Headers = nil
	records[3].Headers = nil
	fetches[0].Topics[0].Partitions = append(fetches[0].Topics[0].Partitions,
		kgo.FetchPartition{Partition: 2, Err: kerr.UnknownTopicOrPartition})
	messages, err = decodeFetches(fetches)
	if len(messages) != 2 || messages[0].Offset != 10 || messages[1].Offset != 12 ||
		!errors.Is(err, ErrInvalid) || !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("partial batch lost data/errors: %#v, %v", messages, err)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 3 {
		t.Fatalf("batch errors lost: %v", err)
	}
	var failedOffsets []int64
	for _, failure := range joined.Unwrap() {
		var decode *DecodeError
		if errors.As(failure, &decode) {
			failedOffsets = append(failedOffsets, decode.Message.Offset)
		}
	}
	if !reflect.DeepEqual(failedOffsets, []int64{11, 13}) {
		t.Fatalf("failed record locations lost: %v", failedOffsets)
	}
}

func TestDecodeErrorOwnsOriginalRecord(t *testing.T) {
	record := &kgo.Record{
		Topic: "actors", Partition: 0, Offset: 12,
		Key: []byte("actor"), Value: []byte("raw"),
		Headers: []kgo.RecordHeader{{Key: sharedMetadataHeader, Value: []byte("{")}},
	}
	messages, err := decodeFetches(oneFetch(record, nil))
	var decode *DecodeError
	if len(messages) != 0 || !errors.Is(err, ErrInvalid) || !errors.As(err, &decode) {
		t.Fatalf("missing decode error: %v", err)
	}
	record.Key[0], record.Value[0], record.Headers[0].Value[0] = 'X', 'X', 'X'
	if decode.Message.Offset != 12 || string(decode.Message.Key) != "actor" ||
		string(decode.Message.Value) != "raw" || string(decode.Message.Headers[0].Value) != "{" {
		t.Fatal("error lost source location or aliases driver buffers")
	}
}

func TestPollCloseAndCancelErrors(t *testing.T) {
	for _, want := range []error{kgo.ErrClientClosed, context.Canceled, context.DeadlineExceeded} {
		messages, err := decodeFetches(kgo.NewErrFetch(want))
		if len(messages) != 0 || !errors.Is(err, want) {
			t.Fatalf("error lost: %v", err)
		}
	}
}

func TestCanceledOperationsDoNotNeedClient(t *testing.T) {
	// 未配置 client：若预先取消仍触发客户端调用，此测试会 panic。
	k := &Kafka{config: KafkaConfig{Topic: "actors", DLQTopic: "dead", Timeout: time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, call := range []func() error{
		func() error { _, err := k.Poll(ctx); return err },
		func() error { return k.Subscribe(ctx, 0) },
		func() error { return k.Unsubscribe(ctx, 0) },
		func() error { return k.SeekMessage(ctx, 0, 0) },
		func() error { return k.PausePartition(ctx, 0) },
		func() error { return k.ResumePartition(ctx, 0) },
		func() error { return k.CommitOffset(ctx, ShardOffset{ShardId: 0, Offset: 1}) },
		func() error { _, err := k.Publish(ctx, conversionMessage()); return err },
		func() error { return k.DLQ(ctx, conversionMessage(), "test") },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled operation: %v", err)
		}
	}
}

func TestInvalidInputDoesNotNeedBroker(t *testing.T) {
	ctx := context.Background()
	for _, config := range []KafkaConfig{
		{}, {Brokers: []string{""}, Topic: "actors", GroupID: "test"},
		{Brokers: []string{"localhost:9092"}, Topic: "actors", GroupID: "test", DLQTopic: "actors"},
	} {
		if _, err := NewKafka(ctx, config); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted invalid config: %v", err)
		}
	}
	if err := validateShards(ctx, -1); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted negative shard")
	}
	requireNoError(t, validateShards(ctx, int32(40000)))
}

func TestConcurrentRecordEncoding(t *testing.T) {
	message := conversionMessage()
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			record, err := encodeRecord(message, "actors")
			if err != nil {
				t.Error(err)
				return
			}
			record.Value[0] = 123
			record.Headers[1].Value[0] = 'X'
		}()
	}
	workers.Wait()
	if message.Headers[0].Value[0] != '0' || message.Payload.([]byte)[0] != 0 {
		t.Fatal("concurrent encoding mutated caller data")
	}
}
