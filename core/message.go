package core

type MessageType int8

const (
	MessageTypeMemory MessageType = iota
	MessageTypeTimer
	MessageTypeNetwork
)

type MessageMeta struct {
	Sender    string
	Receiver  string
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
