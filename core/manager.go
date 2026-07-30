package core

import (
	"goactor/structure"
)

type Manager struct {
	node   string
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

func (m *Manager) AddActor(actor IActor) {
	m.actors.Add(actor)
}

func (m *Manager) DelActor(id string) {
	m.actors.Del(id)
}

func (m *Manager) GetActor(id string) (IActor, bool) {
	return m.actors.Get(id)
}
