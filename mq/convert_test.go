// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

package mq

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"goactor/message/mm"
)

func conversionMessage() mm.Message {
	return mm.Message{
		MessageMeta: mm.MessageMeta{
			Receiver:  mm.MessageRef{Id: "actor-1", Type: "actor"},
			MessageId: "message-1", Type: mm.MessageTypeNetwork,
		},
		Command: "update",
		Payload: []byte{0, 255, 1},
		Headers: []mm.Header{
			{Key: "traceparent", Value: []byte("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")},
			{Key: "tracestate", Value: []byte("vendor=value")},
			{Key: "custom", Value: []byte("first")},
			{Key: "custom", Value: []byte("second")},
		},
	}
}

func TestExtensionsAndRawPayloadRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{{0, 255, 1}, []byte("{ \"n\" : 1 }\n")} {
		original := conversionMessage()
		original.Payload = payload
		wire, err := FromMMMessage(original, "actors")
		requireNoError(t, err)
		decoded, err := ToMMMessage(wire)
		requireNoError(t, err)
		// TraceId 是可选的；扩展 header 并不会被误当作自定义 TraceId。
		if decoded.TraceId != "" || !reflect.DeepEqual(decoded.Headers, original.Headers) {
			t.Fatalf("metadata changed: %#v", decoded)
		}
		again, err := FromMMMessage(decoded, "actors")
		requireNoError(t, err)
		if !bytes.Equal(again.Value, payload) || !reflect.DeepEqual(wire.Headers, again.Headers) {
			t.Fatalf("round trip changed payload or headers: %#v", again)
		}
		wire.Value[0] ^= 1
		wire.Headers[1].Value[0] = 'X'
		if !bytes.Equal(decoded.Payload.(json.RawMessage), payload) ||
			!reflect.DeepEqual(decoded.Headers, original.Headers) {
			t.Fatal("decode aliases wire buffers")
		}
		decoded.Headers[0].Value[0] = 'Y'
		if again.Headers[1].Value[0] != '0' || original.Headers[0].Value[0] != '0' {
			t.Fatal("encode aliases caller buffers")
		}
	}
}

func TestMetadataValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Message)
	}{
		{"missing", func(m *Message) { m.Headers = nil }},
		{"malformed", func(m *Message) { m.Headers[0].Value = []byte("{") }},
		{"null", func(m *Message) { m.Headers[0].Value = []byte("null") }},
		{"empty", func(m *Message) { m.Headers[0].Value = []byte("{}") }},
		{"duplicate", func(m *Message) { m.Headers = append(m.Headers, m.Headers[0]) }},
		{"mismatched key", func(m *Message) { m.Key = []byte("other-actor") }},
		{"missing key", func(m *Message) { m.Key = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := FromMMMessage(conversionMessage(), "actors")
			requireNoError(t, err)
			tc.mutate(&wire)
			if _, err := ToMMMessage(wire); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
	for _, field := range []string{"receiver", "message id", "command", "type"} {
		t.Run(field, func(t *testing.T) {
			message := conversionMessage()
			switch field {
			case "receiver":
				message.Receiver.Id = ""
			case "message id":
				message.MessageId = ""
			case "command":
				message.Command = ""
			case "type":
				message.Type = 99
			}
			if _, err := FromMMMessage(message, "actors"); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
			meta, err := json.Marshal(sharedMetadata{
				Receiver: message.Receiver, MessageID: message.MessageId,
				Command: message.Command, Type: message.Type,
			})
			requireNoError(t, err)
			if _, err := ToMMMessage(Message{
				Key: []byte(message.Receiver.Id), Headers: []Header{{Key: sharedMetadataHeader, Value: meta}},
			}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("decode accepted invalid metadata: %v", err)
			}
		})
	}
	message := conversionMessage()
	message.Headers = append(message.Headers, Header{Key: sharedMetadataHeader, Value: []byte("{}")})
	if _, err := FromMMMessage(message, "actors"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("accepted reserved header: %v", err)
	}
}

func TestEmptyAndNilBytesPreserved(t *testing.T) {
	for _, payload := range [][]byte{nil, {}} {
		message := conversionMessage()
		message.Payload = payload
		message.Headers = []Header{{Key: "empty", Value: []byte{}}, {Key: "nil"}}
		wire, err := FromMMMessage(message, "actors")
		requireNoError(t, err)
		decoded, err := ToMMMessage(wire)
		requireNoError(t, err)
		record, err := encodeRecord(decoded, "actors")
		requireNoError(t, err)
		if !reflect.DeepEqual(record.Value, payload) || record.Headers[1].Value == nil || record.Headers[2].Value != nil {
			t.Fatal("empty and nil bytes became indistinguishable")
		}
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
