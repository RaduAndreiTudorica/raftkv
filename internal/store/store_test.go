package store

import (
	"errors"
	"sync"
	"testing"
)

type Pair struct {
	Key   string
	Value string
}

var testPairs = []Pair{
	{"foo", "bar"},
	{"ping", "pong"},
	{"hello", "world"},
	{"answer", "42"},
	{"raft", "consensus"},
	{"go", "routine"},
	{"key1", "value1"},
	{"key2", "value2"},
	{"key3", "value3"},
	{"key4", "value4"},
}

var sameKeyValues = []string{
	"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
}

func TestStore_Put_Get(t *testing.T) {
	store := NewStore(t.TempDir())
	key := []byte("hello")
	value := []byte("world")

	err := store.Put(key, value)
	if err != nil {
		t.Error(err)
	}

	valueStored, _ := store.Get(key)

	if string(value) != string(valueStored) {
		t.Errorf("different values stored")
	}
}

func TestStore_Get_NonExistentKey(t *testing.T) {
	store := NewStore(t.TempDir())
	key := []byte("hello")
	_, exists := store.Get(key)
	if exists == true {
		t.Errorf("value should not exist")
	}

}

func TestStore_Delete(t *testing.T) {
	store := NewStore(t.TempDir())
	key := []byte("hello")
	value := []byte("world")

	err := store.Put(key, value)
	if err != nil {
		t.Error(err)
	}

	err = store.Delete(key)
	if err != nil {
		t.Error(err)
	}

	_, exists := store.Get(key)
	if exists == true {
		t.Errorf("value should not exist")
	}
}

func TestStore_Delete_NonExistentKey(t *testing.T) {
	store := NewStore(t.TempDir())
	key := []byte("hello")

	err := store.Delete(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Error(err)
	}
}

func TestStore_Put_Overwrite(t *testing.T) {
	store := NewStore(t.TempDir())
	key := []byte("hello")
	value := []byte("world")
	value2 := []byte("moon")

	err := store.Put(key, value)
	if err != nil {
		t.Error(err)
	}

	err = store.Put(key, value2)
	if err != nil {
		t.Error(err)
	}

	valueStored, _ := store.Get(key)

	if string(value2) != string(valueStored) {
		t.Errorf("different values stored")
	}
}

func TestStore_WAL_Persistence(t *testing.T) {
	dir := t.TempDir()

	store1 := NewStore(dir)
	key := []byte("hello")
	value := []byte("world")
	store1.Put(key, value)

	store2 := NewStore(dir)
	store2.RetrieveData()
	valueStored, _ := store2.Get(key)
	if string(value) != string(valueStored) {
		t.Logf("expected: %s, got: %s\n", string(value), string(valueStored))
		t.Errorf("different values stored")
	}
}

func TestStore_WAL_ReplayOrder(t *testing.T) {
	dir := t.TempDir()

	store1 := NewStore(dir)
	key := []byte("hello")
	value := []byte("world")
	store1.Put(key, value)

	value = []byte("moon")
	store1.Put(key, value)

	store1.Delete(key)

	value = []byte("sun")
	store1.Put(key, value)

	store2 := NewStore(dir)
	store2.RetrieveData()

	valueStored, _ := store2.Get(key)
	if string(value) != string(valueStored) {
		t.Errorf("different values stored")
	}
}

func TestStore_WAL_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)

	err := store.RetrieveData()
	if err != nil {
		t.Error(err)
	}
}

func TestStore_ConcurrentPuts(t *testing.T) {
	wg := sync.WaitGroup{}
	dir := t.TempDir()
	store := NewStore(dir)

	for _, pair := range testPairs {
		wg.Add(1)
		go func(p Pair) {
			defer wg.Done()
			store.Put([]byte(p.Key), []byte(p.Value))
		}(pair)
	}
	wg.Wait()

	for _, pair := range testPairs {
		value, _ := store.Get([]byte(pair.Key))
		if string(value) != pair.Value {
			t.Errorf("different values stored")
		}
	}
}

func TestStore_ConcurrentPutsSameKey(t *testing.T) {
	wg := sync.WaitGroup{}
	dir := t.TempDir()
	store := NewStore(dir)
	key := []byte("hello")

	for _, value := range sameKeyValues {
		byteValue := []byte(value)
		wg.Add(1)
		go func(byteValue []byte) {
			defer wg.Done()
			store.Put(key, byteValue)
		}(byteValue)
	}
	wg.Wait()

	valueStored, _ := store.Get(key)
	ok := false
	for _, v := range sameKeyValues {
		if v == string(valueStored) {
			ok = true
		}
	}

	if !ok {
		t.Errorf("different values stored")
	}
}
