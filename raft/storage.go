package raft

import "sync"

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	HasData() bool
}

type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewStorage() *MapStorage {
	return &MapStorage{
		m: make(map[string][]byte),
	}
}

func (m *MapStorage) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var v, ok = m.m[key]

	return v, ok
}

func (m *MapStorage) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[key] = value
}

func (m *MapStorage) HasData() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.m) > 0
}
