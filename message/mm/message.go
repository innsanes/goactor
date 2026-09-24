package mm

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

// Header 是不依赖具体 MQ 实现的扩展元数据；保留顺序和重复 key。
type Header struct {
	Key   string
	Value []byte
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
	// Headers 仅保存扩展元数据。框架元数据由 MQ adapter 从类型化字段生成，
	// 不应在这里重复设置。转发同一消息时保留，创建子消息时不自动继承。
	Headers []Header
}
