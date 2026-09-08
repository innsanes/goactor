package core

type MessageType int8

const (
	MessageTypeMemory MessageType = iota
	MessageTypeTimer
	MessageTypeNetwork
)

type MessageRef struct {
	Type string
	Id   string
}

type MessageMeta struct {
	Sender    MessageRef
	Receiver  MessageRef
	TraceId   string
	MessageId string
	Offset    int64
	Type      MessageType
}

type Message struct {
	MessageMeta
	Command string
	Payload any
}
