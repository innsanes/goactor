// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"bytes"
	"encoding/json"
	"fmt"

	"goactor/message/mm"
)

const sharedMetadataHeader = "mq.message.meta"

type sharedMetadata struct {
	Sender    mm.MessageRef  `json:"sender"`
	Receiver  mm.MessageRef  `json:"receiver"`
	TraceID   string         `json:"trace_id"`
	MessageID string         `json:"message_id"`
	Type      mm.MessageType `json:"type"`
	Command   string         `json:"command"`
}

// ToMMMessage 将 Kafka 消息转换为共享消息。Kafka key 是 Actor ID，
// 因此转换后写入 mm.Message.Receiver.Id；offset 保留在 mm.Message.Offset。
// Payload 以 json.RawMessage 返回，保留原始消息字节。
// 框架 header 必须恰好有一个且合法；TraceId 可为空。其他 headers 原样保留。
func ToMMMessage(message Message) (mm.Message, error) {
	var metadata sharedMetadata
	found := false
	for _, header := range message.Headers {
		if header.Key != sharedMetadataHeader {
			continue
		}
		if found {
			return mm.Message{}, fmt.Errorf("%w: duplicate %s", ErrInvalid, sharedMetadataHeader)
		}
		found = true
		if err := json.Unmarshal(header.Value, &metadata); err != nil {
			return mm.Message{}, fmt.Errorf("%w: decode %s: %w", ErrInvalid, sharedMetadataHeader, err)
		}
	}
	if !found {
		return mm.Message{}, fmt.Errorf("%w: missing %s", ErrInvalid, sharedMetadataHeader)
	}
	if err := validateMetadata(metadata); err != nil {
		return mm.Message{}, err
	}
	if string(message.Key) != metadata.Receiver.Id {
		return mm.Message{}, fmt.Errorf("%w: key does not match receiver actor id", ErrInvalid)
	}
	result := mm.Message{
		MessageMeta: mm.MessageMeta{
			Offset:    message.Offset,
			Sender:    metadata.Sender,
			Receiver:  metadata.Receiver,
			TraceId:   metadata.TraceID,
			MessageId: metadata.MessageID,
			Type:      metadata.Type,
		},
		Command: metadata.Command,
		Payload: json.RawMessage(bytes.Clone(message.Value)),
	}
	for _, header := range message.Headers {
		if header.Key == sharedMetadataHeader {
			continue
		}
		result.Headers = append(result.Headers, Header{Key: header.Key, Value: bytes.Clone(header.Value)})
	}
	return result, nil
}

func validateMetadata(metadata sharedMetadata) error {
	if metadata.Receiver.Id == "" || metadata.MessageID == "" || metadata.Command == "" {
		return fmt.Errorf("%w: receiver actor id, message id and command must be nonempty", ErrInvalid)
	}
	switch metadata.Type {
	case mm.MessageTypeMemory, mm.MessageTypeTimer, mm.MessageTypeNetwork:
		return nil
	default:
		return fmt.Errorf("%w: unknown message type %d", ErrInvalid, metadata.Type)
	}
}

// FromMMMessage 将共享消息转换为 Kafka 消息。
// topic 使用固定配置值，partition 使用 Actor shard ID，key 使用 Actor ID。
// []byte/json.RawMessage payload 原样发送，其余可序列化 payload 使用 JSON 编码。
func FromMMMessage(message mm.Message, topic string) (Message, error) {
	if topic == "" {
		return Message{}, fmt.Errorf("%w: topic must be nonempty", ErrInvalid)
	}
	actorID := message.Receiver.Id
	meta := sharedMetadata{
		Sender: message.Sender, Receiver: message.Receiver,
		TraceID: message.TraceId, MessageID: message.MessageId,
		Type: message.Type, Command: message.Command,
	}
	if err := validateMetadata(meta); err != nil {
		return Message{}, err
	}
	var value []byte
	switch raw := message.Payload.(type) {
	case []byte:
		value = bytes.Clone(raw)
	case json.RawMessage:
		// 消费后再发布/DLQ 时也保持非 JSON payload 和 JSON 空白不变。
		value = bytes.Clone(raw)
	default:
		encoded, err := json.Marshal(message.Payload)
		if err != nil {
			return Message{}, fmt.Errorf("encode message payload: %w", err)
		}
		value = encoded
	}
	metadata, err := json.Marshal(meta)
	if err != nil {
		return Message{}, fmt.Errorf("encode message metadata: %w", err)
	}
	headers := []Header{{Key: sharedMetadataHeader, Value: metadata}}
	for _, header := range message.Headers {
		if header.Key == sharedMetadataHeader {
			return Message{}, fmt.Errorf("%w: %s is reserved", ErrInvalid, sharedMetadataHeader)
		}
		headers = append(headers, Header{Key: header.Key, Value: bytes.Clone(header.Value)})
	}
	return Message{
		TopicPartition: TopicPartition{
			Topic: topic, Partition: mm.ActorShardID(actorID),
		},
		Offset:  message.Offset,
		Key:     []byte(actorID),
		Value:   value,
		Headers: headers,
	}, nil
}
