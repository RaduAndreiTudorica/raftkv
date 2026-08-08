package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrKeyNotFound = errors.New("value does not exist")

type Message struct {
	Command string
	Key     []byte
	Value   []byte
}
type Store struct {
	mutex   sync.Mutex
	walPath string
	data    map[string][]byte
}

func NewStore(walPath string) *Store {
	return &Store{
		walPath: walPath,
		data:    make(map[string][]byte),
	}
}

func (store *Store) writeLog(command string, args ...[]byte) error {
	var message Message
	switch command {
	case "put":
		message = Message{Command: command, Key: args[0], Value: args[1]}
	case "delete":
		message = Message{Command: command, Key: args[0]}
	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	err := os.MkdirAll(store.walPath, 0755)
	if err != nil {
		return err
	}

	fileName := filepath.Join(store.walPath, "raftkv.wol")
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	err = json.NewEncoder(file).Encode(message)
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) readLog() ([]Message, error) {
	var messages []Message
	fileName := filepath.Join(store.walPath, "raftkv.wol")
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return []Message{}, err
	}

	decoder := json.NewDecoder(file)

	for {
		var message Message
		err = decoder.Decode(&message)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return messages, err
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func (store *Store) RetrieveData() error {
	messages, err := store.readLog()
	if err != nil {
		return err
	}

	for _, message := range messages {
		switch message.Command {
		case "put":
			key := string(message.Key)
			value := message.Value
			store.data[key] = value
		case "delete":
			key := string(message.Key)
			_, ok := store.data[key]
			if !ok {
				continue
			}
			delete(store.data, key)
		default:
			return fmt.Errorf("unknown command: %s", message.Command)
		}
	}
	return nil
}

func (store *Store) Put(key, value []byte) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	err := store.writeLog("put", key, value)
	if err != nil {
		return err
	}
	store.data[string(key)] = value
	return nil
}

func (store *Store) Get(key []byte) ([]byte, bool) {
	value, ok := store.data[string(key)]
	return value, ok
}

func (store *Store) Delete(key []byte) error {
	err := store.writeLog("delete", key)
	if err != nil {
		return err
	}
	_, ok := store.data[string(key)]
	if !ok {
		return ErrKeyNotFound
	}
	delete(store.data, string(key))
	return nil
}
