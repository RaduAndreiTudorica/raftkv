package raft

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	if err := os.MkdirAll(filepath.Dir(pr.filePath), 0755); err != nil {
		return err
	}

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

func (pr *Persister) saveCommitIndex(commitIndex int64) error {
	pr.mutex.Lock()
	defer pr.mutex.Unlock()

	_ = os.MkdirAll(filepath.Dir(pr.filePath), 0755)
	commitFile := pr.filePath + ".commit"
	return os.WriteFile(commitFile, []byte(strconv.FormatInt(commitIndex, 10)), 0644)
}

func (pr *Persister) readCommitIndex() int64 {
	pr.mutex.Lock()
	defer pr.mutex.Unlock()

	commitFile := pr.filePath + ".commit"
	data, err := os.ReadFile(commitFile)
	if err != nil {
		return 0
	}

	val, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return val
}
