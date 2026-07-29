package core

import (
	"goactor/structure"
)

type Manager struct {
	actors *structure.SyncMap[IActor]
}

func NewManager(options ...ManagerMakeOption) *Manager {
	newServer := &Manager{
		actors: structure.NewSyncMap[IActor](),
	}
	for _, option := range options {
		option(newServer)
	}
	return newServer
}

type ManagerMakeOption func(manager *Manager)

func WithServer() ManagerMakeOption {
	return func(manager *Manager) {}
}
