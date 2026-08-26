package core

type MessageMeta struct {
	Command   string
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
	MessageExtra
	Payload []byte
}
