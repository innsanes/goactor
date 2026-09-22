// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafka struct {
	added           map[string]map[int32]kgo.Offset
	removed         map[string][]int32
	paused          bool
	fetches         kgo.Fetches
	produced        []*kgo.Record
	produceErr      error
	saved           kadm.OffsetResponses
	fetchErr        error
	fetchCalls      int
	fetchedTopics   []string
	commits         kadm.Offsets
	commitCalls     int
	group           string
	commitErr       error
	partitionErr    error
	missingResponse bool
	closes          int
}

func (f *fakeKafka) AddConsumePartitions(p map[string]map[int32]kgo.Offset) { f.added = p }
func (f *fakeKafka) RemoveConsumePartitions(p map[string][]int32)           { f.removed = p; f.fetches = nil }
func (f *fakeKafka) PauseFetchPartitions(p map[string][]int32) map[string][]int32 {
	f.paused = true
	return p
}
func (f *fakeKafka) ResumeFetchPartitions(map[string][]int32) { f.paused = false }
func (f *fakeKafka) PollRecords(context.Context, int) kgo.Fetches {
	if f.paused {
		return nil
	}
	records := f.fetches
	f.fetches = nil
	return records
}
func (f *fakeKafka) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.produced = append(f.produced, records...)
	return kgo.ProduceResults{{Record: records[0], Err: f.produceErr}}
}
func (f *fakeKafka) Close() { f.closes++ }
func (f *fakeKafka) FetchOffsetsForTopics(_ context.Context, _ string, topics ...string) (kadm.OffsetResponses, error) {
	f.fetchCalls++
	f.fetchedTopics = append([]string(nil), topics...)
	return f.saved, f.fetchErr
}
func (f *fakeKafka) CommitOffsets(_ context.Context, group string, offsets kadm.Offsets) (kadm.OffsetResponses, error) {
	f.commitCalls++
	f.commits, f.group = offsets, group
	responses := make(kadm.OffsetResponses)
	if !f.missingResponse {
		for _, ps := range offsets {
			for _, offset := range ps {
				responses.Add(kadm.OffsetResponse{Offset: offset, Err: f.partitionErr})
			}
		}
	}
	return responses, f.commitErr
}
func newFakeKafka() (*Kafka, *fakeKafka) {
	f := &fakeKafka{saved: make(kadm.OffsetResponses)}
	for partition := int32(0); partition < 4; partition++ {
		f.saved.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: "actors", Partition: partition, At: -1}})
	}
	return &Kafka{client: f, admin: f, config: KafkaConfig{GroupID: "actors", DLQTopic: "dead", Timeout: time.Second}, subscriptions: make(map[TopicPartition]bool), done: make(chan struct{})}, f
}

func at(p TopicPartition, offset int64) TopicPartitionOffset {
	return TopicPartitionOffset{Topic: p.Topic, Partition: p.Partition, Offset: offset}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPartitionLifecycle(t *testing.T) {
	k, f := newFakeKafka()
	p := TopicPartition{"actors", 2}
	ctx := context.Background()
	if !errors.Is(k.PausePartition(p), ErrNotSubscribed) {
		t.Fatal("unsubscribed pause")
	}
	requireNoError(t, k.Subscribe(ctx, p))
	if !reflect.DeepEqual(f.added, assignment(p, OffsetBeginning)) {
		t.Fatal("initial offset")
	}
	if !errors.Is(k.Subscribe(ctx, p), ErrAlreadySubscribed) {
		t.Fatal("duplicate subscribe must not rewind")
	}
	requireNoError(t, k.PausePartition(p))
	requireNoError(t, k.PausePartition(p))
	requireNoError(t, k.SeekMessage(p, 10))
	if !f.paused || !reflect.DeepEqual(f.added, assignment(p, 10)) || !reflect.DeepEqual(f.removed, partitionMap(p)) {
		t.Fatal("seek must invalidate buffers and preserve pause")
	}
	if f.commits != nil {
		t.Fatal("seek committed offsets")
	}
	requireNoError(t, k.ResumePartition(p))
	if f.paused {
		t.Fatal("not resumed")
	}
	requireNoError(t, k.PausePartition(p))
	requireNoError(t, k.Unsubscribe(p))
	requireNoError(t, k.Unsubscribe(p))
	if f.paused {
		t.Fatal("pause leaked past unsubscribe")
	}
	if !errors.Is(k.CommitOffset(ctx, at(p, 11)), ErrNotSubscribed) {
		t.Fatal("commit after unsubscribe")
	}
	requireNoError(t, k.Subscribe(ctx, p))
	k.Close()
	k.Close()
	if f.closes != 1 {
		t.Fatal("close is not idempotent")
	}
	if !errors.Is(k.SeekMessage(p, 0), ErrClosed) {
		t.Fatal("operation after close")
	}
}

func TestSubscribeBatch(t *testing.T) {
	k, f := newFakeKafka()
	p0 := TopicPartition{"actors", 0}
	p1 := TopicPartition{"actors", 1}
	f.saved = make(kadm.OffsetResponses)
	f.saved.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: p0.Topic, Partition: p0.Partition, At: 42}})
	f.saved.Add(kadm.OffsetResponse{Offset: kadm.Offset{Topic: p1.Topic, Partition: p1.Partition, At: -1}})
	requireNoError(t, k.Subscribe(context.Background(), p0, p1))
	want := map[string]map[int32]kgo.Offset{
		"actors": {
			0: kgo.NoResetOffset().At(42),
			1: kgo.NoResetOffset().At(OffsetBeginning),
		},
	}
	if !reflect.DeepEqual(f.added, want) {
		t.Fatalf("wrong assignments: got %#v want %#v", f.added, want)
	}
	if f.fetchCalls != 1 || !reflect.DeepEqual(f.fetchedTopics, []string{"actors"}) {
		t.Fatalf("checkpoint lookup was not batched: calls=%d topics=%v", f.fetchCalls, f.fetchedTopics)
	}
	if err := k.Subscribe(context.Background(), p0); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatal("duplicate subscription must be rejected")
	}
}

func TestCommitNextOffsetAndErrors(t *testing.T) {
	k, f := newFakeKafka()
	p := TopicPartition{"actors", 0}
	p1 := TopicPartition{"actors", 1}
	ctx := context.Background()
	requireNoError(t, k.Subscribe(ctx, p, p1))
	requireNoError(t, k.CommitOffset(ctx, at(p, 51), at(p1, 73)))
	actual, ok := f.commits.Lookup(p.Topic, p.Partition)
	if !ok || actual.At != 51 || actual.LeaderEpoch != -1 || f.group != "actors" {
		t.Fatal("offset was incremented or group lost")
	}
	actual1, ok := f.commits.Lookup(p1.Topic, p1.Partition)
	if !ok || actual1.At != 73 || actual1.LeaderEpoch != -1 {
		t.Fatal("batch commit lost the second partition")
	}
	if f.commitCalls != 1 {
		t.Fatalf("batch commit used %d broker requests", f.commitCalls)
	}
	f.partitionErr = kerr.GroupAuthorizationFailed
	if !errors.Is(k.CommitOffset(ctx, at(p, 52)), kerr.GroupAuthorizationFailed) {
		t.Fatal("broker error lost")
	}
	f.partitionErr = nil
	f.commitErr = context.DeadlineExceeded
	if !errors.Is(k.CommitOffset(ctx, at(p, 52)), context.DeadlineExceeded) {
		t.Fatal("request error lost")
	}
	f.commitErr = nil
	f.missingResponse = true
	if k.CommitOffset(ctx, at(p, 52)) == nil {
		t.Fatal("missing confirmation treated as success")
	}
}

func TestDLQAcknowledgementAndMetadata(t *testing.T) {
	k, f := newFakeKafka()
	message := Message{TopicPartition: TopicPartition{"actors", 3}, Offset: 99, Key: []byte("player"), Value: []byte{0, 1, 2}, Headers: []Header{{"custom", []byte("value")}}, Timestamp: time.Unix(12, 0)}
	requireNoError(t, k.DLQ(context.Background(), message, "decode failed"))
	r := f.produced[0]
	if r.Topic != "dead" || !reflect.DeepEqual(r.Value, message.Value) || !reflect.DeepEqual(r.Key, message.Key) {
		t.Fatal("payload changed")
	}
	headers := map[string]string{}
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["mq.source.offset"] != "99" || headers["mq.source.partition"] != "3" || headers["mq.source.topic"] != "actors" || headers["mq.reason"] != "decode failed" || headers["custom"] != "value" {
		t.Fatal(headers)
	}
	if f.commits != nil {
		t.Fatal("DLQ committed source offset")
	}
	f.produceErr = errors.New("produce failed")
	if !errors.Is(k.DLQ(context.Background(), message, "failed"), f.produceErr) {
		t.Fatal("DLQ error lost")
	}
	message.Topic = "dead"
	if !errors.Is(k.DLQ(context.Background(), message, "loop"), ErrInvalid) {
		t.Fatal("DLQ loop allowed")
	}
}

func TestPollRecordsAndPartitionErrors(t *testing.T) {
	k, f := newFakeKafka()
	p := TopicPartition{"actors", 0}
	requireNoError(t, k.Subscribe(context.Background(), p))
	source := &kgo.Record{Topic: p.Topic, Partition: p.Partition, Offset: 10, Value: []byte("hello")}
	f.fetches = kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: p.Topic, Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{source}}, {Partition: 1, Err: kerr.UnknownTopicOrPartition}}}}}}
	records, err := k.Poll(context.Background(), 2)
	if len(records) != 1 || records[0].Offset != 10 || !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("%v %v", records, err)
	}
	source.Value[0] = 'X'
	if string(records[0].Value) != "hello" {
		t.Fatal("message aliases driver buffer")
	}
}

func TestIdlePollDoesNotBlockControls(t *testing.T) {
	k, _ := newFakeKafka()
	p := TopicPartition{"actors", 0}
	requireNoError(t, k.Subscribe(context.Background(), p))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := k.Poll(ctx, 1); result <- err }()
	requireNoError(t, k.PausePartition(p))
	requireNoError(t, k.SeekMessage(p, 5))
	requireNoError(t, k.Unsubscribe(p))
	k.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("poll did not stop")
	}
}

func TestInvalidArguments(t *testing.T) {
	k, _ := newFakeKafka()
	ctx := context.Background()
	p := TopicPartition{"actors", 0}
	for _, err := range []error{
		k.Subscribe(ctx, TopicPartition{"", 0}), k.Subscribe(ctx, TopicPartition{"actors", -1}),
		k.SeekMessage(p, -1), k.CommitOffset(ctx, at(p, -1)),
	} {
		if !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := k.Poll(ctx, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := k.Poll(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := k.Subscribe(canceled, p); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
