package core

type RootContext struct {
}

type Context struct {
	message   Message
	sequences map[string]uint64
}

func NewContext(message Message) *Context {
	return &Context{
		message:   message,
		sequences: make(map[string]uint64),
	}
}

func NewRootContext(message Message) *Context {
	return &Context{
		message:   message,
		sequences: make(map[string]uint64),
	}
}

func (c *Context) NewMessage(senderId, receiverId, command string, payload []byte) Message {
	return Message{
		Command:   command,
		Sender:    senderId,
		Receiver:  receiverId,
		TraceId:   c.message.TraceId,
		MessageId: c.generateMessageId(senderId, receiverId),
		Payload:   payload,
	}
}

func (c *Context) nextSequence(receiverId string) uint64 {
	seq := c.sequences[receiverId]
	c.sequences[receiverId] = seq + 1
	return seq
}

func (c *Context) generateMessageId(senderId, receiverId string) string {
	seq := c.nextSequence(receiverId)
	messageID := GenerateMessageID(c.message.MessageId, senderId, receiverId, seq)
	return messageID
}
