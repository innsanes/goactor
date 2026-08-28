package core

type MessageMeta struct {
	Sender    string
	Receiver  string
	TraceId   string
	MessageId string
}

type MessageExtra struct {
	Offset int64
}

type Message struct {
	MessageMeta
	Command string
	Payload []byte
}
