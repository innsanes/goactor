package core

type Mailbox struct {
	messages map[string]struct{}
}
