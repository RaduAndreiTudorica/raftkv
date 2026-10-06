package raft

import (
	"os"
	"sync"
)

type Persister struct {
	mutex    sync.Mutex
	filePath string
}

func NewPersister(path string) *Persister {
	return &Persister{filePath: path}
}

func (pr *Persister) saveState(data []byte) error {
	pr.mutex.Lock()
	defer pr.mutex.Unlock()

	return os.WriteFile(pr.filePath, data, 0644)
}

func (pr *Persister) readState() []byte {
	pr.mutex.Lock()
	defer pr.mutex.Unlock()

	data, err := os.ReadFile(pr.filePath)
	if err != nil {
		return nil
	}

	return data
}
