package raft

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RaduAndreiTudorica/raftkv/proto"

	"google.golang.org/grpc"
)

var applyCh chan ApplyMsg

type MockNetwork struct {
	mu        sync.Mutex
	nodes     map[int]*Raft
	connected map[int]bool

	persisters map[int]*Persister
	peers      map[int][]proto.RaftClient
}

type MockClient struct {
	net      *MockNetwork
	senderID int
	serverID int
}

func (c *MockClient) AppendEntries(ctx context.Context, in *proto.AppendEntriesArgs, opts ...grpc.CallOption) (*proto.AppendEntriesReply, error) {
	c.net.mu.Lock()
	senderConnected := c.net.connected[c.senderID]
	targetConnected := c.net.connected[c.serverID]
	targetNode := c.net.nodes[c.serverID]
	c.net.mu.Unlock()

	if !senderConnected || !targetConnected {
		return nil, fmt.Errorf("rpc timeout: conexiune întreruptă")
	}

	time.Sleep(5 * time.Millisecond)
	return targetNode.AppendEntries(ctx, in)
}

func (c *MockClient) RequestVote(ctx context.Context, in *proto.RequestVoteArgs, opts ...grpc.CallOption) (*proto.RequestVoteReply, error) {
	c.net.mu.Lock()
	senderConnected := c.net.connected[c.senderID]
	targetConnected := c.net.connected[c.serverID]
	targetNode := c.net.nodes[c.serverID]
	c.net.mu.Unlock()

	if !senderConnected || !targetConnected {
		return nil, fmt.Errorf("rpc timeout: conexiune întreruptă")
	}

	time.Sleep(5 * time.Millisecond)
	return targetNode.RequestVote(ctx, in)
}

func CreateMockNetwork(t *testing.T, numNodes int) (*MockNetwork, []*Raft) {
	net := &MockNetwork{
		nodes:      make(map[int]*Raft),
		connected:  make(map[int]bool),
		persisters: make(map[int]*Persister),
		peers:      make(map[int][]proto.RaftClient),
	}

	rafts := make([]*Raft, numNodes)

	for i := 0; i < numNodes; i++ {
		net.connected[i] = true
	}

	for i := 0; i < numNodes; i++ {
		localPeers := make([]proto.RaftClient, numNodes)
		for j := 0; j < numNodes; j++ {
			localPeers[j] = &MockClient{
				net:      net,
				senderID: i,
				serverID: j,
			}
		}

		stateFile := filepath.Join(t.TempDir(), fmt.Sprintf("state_node_%d.bin", i))
		localPersister := NewPersister(stateFile)

		net.peers[i] = localPeers
		net.persisters[i] = localPersister

		rafts[i] = NewRaft(i, localPeers, localPersister, applyCh)
		net.nodes[i] = rafts[i]
	}

	for i := 0; i < numNodes; i++ {
		go rafts[i].ticker()
	}

	return net, rafts
}

func (net *MockNetwork) Disconnect(serverID int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connected[serverID] = false
}

func (net *MockNetwork) Connect(serverID int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connected[serverID] = true
}

func (net *MockNetwork) IsConnected(serverID int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.connected[serverID]
}

func TestRaft_InitialElection(t *testing.T) {
	_, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)

	leaders := 0
	leaderTerm := int64(0)

	for i := 0; i < 3; i++ {
		state := rafts[i].GetState()
		if state.IsLeader {
			leaders++
			leaderTerm = state.CurrentTerm
		}
	}

	if leaders != 1 {
		t.Fatalf("Failed selection: expected 1 leader, found %d", leaders)
	}

	for i := 0; i < 3; i++ {
		state := rafts[i].GetState()
		if !state.IsLeader && state.CurrentTerm != leaderTerm {
			t.Fatalf("Term mismatch: node %d has %d, leader has %d", i, state.CurrentTerm, leaderTerm)
		}
	}
}

func TestRaft_ReElection(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)
	oldLeaderID := -1
	var oldLeaderTerm int64

	for i := 0; i < 3; i++ {
		state := rafts[i].GetState()
		if state.IsLeader {
			oldLeaderID = i
			oldLeaderTerm = state.CurrentTerm
			break
		}
	}

	if oldLeaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	network.Disconnect(oldLeaderID)
	time.Sleep(3 * time.Second)

	newLeaderID := -1
	var newLeaderTerm int64
	for i := 0; i < 3; i++ {
		if i == oldLeaderID {
			continue
		}

		state := rafts[i].GetState()
		if state.IsLeader {
			newLeaderID = i
			newLeaderTerm = state.CurrentTerm
			break
		}
	}

	if newLeaderID == -1 {
		t.Fatalf("failed to elect new leader after partition")
	}

	if oldLeaderTerm >= newLeaderTerm {
		t.Fatalf("term mismatch: old term %d, new term %d", oldLeaderTerm, newLeaderTerm)
	}

	network.Connect(oldLeaderID)
	time.Sleep(500 * time.Millisecond)

	leaders := 0
	for i := 0; i < 3; i++ {
		state := rafts[i].GetState()
		if state.IsLeader {
			leaders++
		}
		if i == oldLeaderID && state.IsLeader {
			t.Fatalf("old leader failed to step down as follower")
		}
	}

	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader in cluster, found %d", leaders)
	}
}

func TestRaft_BasicAgree(t *testing.T) {
	_, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	testCommand := []byte("SET x=1")
	index, _, isLeader := rafts[leaderID].Start(testCommand)

	if !isLeader {
		t.Fatalf("node %d lost the leader role before receiving the command", leaderID)
	}

	time.Sleep(500 * time.Millisecond)
	agreed := 0
	for i := 0; i < 3; i++ {
		rafts[i].mutex.Lock()
		lastIdx, _ := rafts[i].lastLog()

		if lastIdx >= int64(index) {
			agreed++
		}
		rafts[i].mutex.Unlock()
	}

	if agreed < 2 {
		t.Fatalf("replication failed: only %d nodes have the command; at least 2 were expected.", agreed)
	}
}

func TestRaft_FailAgree(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	network.Disconnect(2)
	network.Disconnect(4)

	testCommand := []byte("SET x=1")
	index, _, isLeader := rafts[leaderID].Start(testCommand)

	if !isLeader {
		t.Fatalf("node %d lost the leader role before receiving the command", leaderID)
	}

	time.Sleep(500 * time.Millisecond)
	agreed := 0
	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()
		lastIdx, _ := rafts[i].lastLog()

		if lastIdx >= int64(index) {
			agreed++
		}
		rafts[i].mutex.Unlock()
	}

	if agreed < 3 {
		t.Fatalf("replication failed: only %d nodes have the command; at least 3 were expected.", agreed)
	}
}

func TestRaft_FailNoAgree(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	network.Disconnect(2)
	network.Disconnect(3)
	network.Disconnect(4)

	testCommand := []byte("SET x=1")
	index, _, isLeader := rafts[leaderID].Start(testCommand)

	if !isLeader {
		t.Fatalf("node %d lost the leader role before receiving the command", leaderID)
	}

	time.Sleep(500 * time.Millisecond)
	agreed := 0
	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()
		lastIdx, _ := rafts[i].lastLog()

		if lastIdx >= int64(index) {
			agreed++
		}
		rafts[i].mutex.Unlock()
	}

	if agreed > 2 {
		t.Fatalf("replication passed: %d nodes have the command; no more than 2 were expected.", agreed)
	}
}

func TestRaft_Rejoin(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)

	time.Sleep(1 * time.Second)
	oldLeaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			oldLeaderID = i
			break
		}
	}

	if oldLeaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	testCommand := []byte("SET x=1")
	_, _, isLeader := rafts[oldLeaderID].Start(testCommand)

	if !isLeader {
		t.Fatalf("node %d lost the leader role before receiving the command", oldLeaderID)
	}

	network.Disconnect(oldLeaderID)

	minorCommand := []byte("SET y=2")
	_, _, _ = rafts[oldLeaderID].Start(minorCommand)

	time.Sleep(2 * time.Second)
	newLeaderID := -1
	for i := 0; i < 5; i++ {
		if i != oldLeaderID && rafts[i].GetState().IsLeader {
			newLeaderID = i
			break
		}
	}

	if newLeaderID == -1 {
		t.Fatalf("the majority didn't elect a new leader")
	}

	for i := 0; i < 5; i++ {
		state := rafts[i].GetState()
		if i != oldLeaderID && state.IsLeader {
			newLeaderID = i
			break
		}
	}

	majorCommand := []byte("GET x")
	rafts[newLeaderID].Start(majorCommand)
	time.Sleep(500 * time.Millisecond)

	network.Connect(oldLeaderID)
	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()

		foundMajor := false
		for _, entry := range rafts[i].Log {
			if string(entry.Command) == "GET x" {
				foundMajor = true
			}
			if string(entry.Command) == "SET y=2" {
				t.Fatalf("node %d didn't delete the invalid minority command", i)
			}
		}
		rafts[i].mutex.Unlock()

		if !foundMajor {
			t.Fatalf("node %d didn't receive the majority command", i)
		}
	}
}

func TestRaft_ConcurrentStarts(t *testing.T) {
	numCommands := 100
	var commands [][]byte

	for i := 0; i < numCommands; i++ {
		cmdStr := fmt.Sprintf("SET concurrent_key_%d=val_%d", i, i)
		commands = append(commands, []byte(cmdStr))
	}

	_, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	var wg sync.WaitGroup
	wg.Add(numCommands)
	for _, command := range commands {
		go func(cmd []byte) {
			defer wg.Done()
			rafts[leaderID].Start(cmd)
		}(command)
	}

	wg.Wait()
	time.Sleep(1 * time.Second)
}

func TestRaft_PersistBasic(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	command := []byte("SET x=1")
	rafts[leaderID].Start(command)
	time.Sleep(500 * time.Millisecond)

	network.Disconnect(2)
	rafts[2] = NewRaft(2, network.peers[2], network.persisters[2], applyCh)
	network.nodes[2] = rafts[2]

	network.Connect(2)

	time.Sleep(1 * time.Second)

	rafts[2].mutex.Lock()
	lastIdx := len(rafts[2].Log) - 1
	if lastIdx < 1 || !bytes.Equal(rafts[2].Log[lastIdx].Command, command) {
		t.Fatalf("node 2 couldn't reload the log")
	}
	rafts[2].mutex.Unlock()
}

func TestRaft_PersistMore(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)
	leaderID := -1

	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	command := []byte("SET x=1")
	rafts[leaderID].Start(command)
	time.Sleep(500 * time.Millisecond)

	rafts[0].Kill()
	rafts[1].Kill()
	rafts[2].Kill()

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		rafts[i] = NewRaft(i, network.peers[i], network.persisters[i], applyCh)

		network.mu.Lock()
		network.nodes[i] = rafts[i]
		network.mu.Unlock()

		go rafts[i].ticker()
	}

	time.Sleep(2 * time.Second)
	newLeaderID := -1
	for i := 0; i < 3; i++ {
		if rafts[i].GetState().IsLeader {
			newLeaderID = i
			break
		}
	}

	if newLeaderID == -1 {
		t.Fatalf("no leader was elected after cluster reboot")
	}

	for i := 0; i < 3; i++ {
		rafts[i].mutex.Lock()
		lastIdx := len(rafts[i].Log) - 1
		if lastIdx < 1 || !bytes.Equal(rafts[i].Log[lastIdx].Command, command) {
			t.Fatalf("node %d couldn't reload the log", i)
		}
		rafts[i].mutex.Unlock()
	}
}

func TestRaft_PersistPartition(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)

	time.Sleep(1 * time.Second)
	oldLeaderID := -1
	for i := 0; i < 5; i++ {
		if rafts[i].GetState().IsLeader {
			oldLeaderID = i
			break
		}
	}

	if oldLeaderID == -1 {
		t.Fatalf("no initial leader elected")
	}

	minorityNode := (oldLeaderID + 1) % 5
	network.Disconnect(oldLeaderID)
	network.Disconnect(minorityNode)

	minorCommand := []byte("SET minority=1")
	rafts[oldLeaderID].Start(minorCommand)

	time.Sleep(2 * time.Second)
	newLeaderID := -1
	for i := 0; i < 5; i++ {
		if i != oldLeaderID && i != minorityNode && rafts[i].GetState().IsLeader {
			newLeaderID = i
			break
		}
	}

	if newLeaderID == -1 {
		t.Fatalf("the majority didn't elect a new leader")
	}

	majorCommand := []byte("SET majority=2")
	rafts[newLeaderID].Start(majorCommand)
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		rafts[i].Kill()
	}
	time.Sleep(100 * time.Millisecond)

	network.Connect(oldLeaderID)
	network.Connect(minorityNode)

	for i := 0; i < 5; i++ {
		rafts[i] = NewRaft(i, network.peers[i], network.persisters[i], applyCh)

		network.mu.Lock()
		network.nodes[i] = rafts[i]
		network.mu.Unlock()

		go rafts[i].ticker()
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()

		foundMajor := false
		for _, entry := range rafts[i].Log {
			if string(entry.Command) == "SET majority=2" {
				foundMajor = true
			}
			if string(entry.Command) == "SET minority=1" {
				t.Fatalf("node %d retained the uncommitted minority command after crash and recovery", i)
			}
		}
		rafts[i].mutex.Unlock()

		if !foundMajor {
			t.Fatalf("node %d lost the committed majority command after crash and recovery", i)
		}
	}
}

func TestRaft_Figure8(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)
	defer func() {
		for i := 0; i < 5; i++ {
			rafts[i].Kill()
		}
	}()

	time.Sleep(1 * time.Second)

	for i := 0; i < 100; i++ {
		leaderID := -1
		for j := 0; j < 5; j++ {
			if rafts[j].GetState().IsLeader {
				leaderID = j
				break
			}
		}

		if leaderID != -1 {
			cmd := fmt.Appendf(nil, "CMD_%d", i)
			rafts[leaderID].Start(cmd)
		}

		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)

		nodeToDisconnect := rand.Intn(5)
		nodeToConnect := rand.Intn(5)

		network.Disconnect(nodeToDisconnect)
		network.Connect(nodeToConnect)
	}

	for i := 0; i < 5; i++ {
		network.Connect(i)
	}

	time.Sleep(1 * time.Second)

	leaderID := -1
	for i := 0; i < 5; i++ {
		if rafts[i].GetState().IsLeader {
			leaderID = i
			break
		}
	}

	if leaderID == -1 {
		t.Fatalf("Figure 8 failure: No leader elected after network healed")
	}

	rafts[leaderID].Start([]byte("FINAL_SYNC"))

	time.Sleep(2 * time.Second)

	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()

		syncIndex := -1
		for idx, entry := range rafts[i].Log {
			if bytes.Equal(entry.Command, []byte("FINAL_SYNC")) {
				syncIndex = idx
				break
			}
		}
		rafts[i].mutex.Unlock()

		if syncIndex == -1 {
			t.Fatalf("Figure 8 failure: Node %d did not get the FINAL_SYNC command", i)
		}

		for j := i + 1; j < 5; j++ {
			rafts[j].mutex.Lock()

			for k := 1; k <= syncIndex; k++ {
				rafts[i].mutex.Lock()
				cmdI := rafts[i].Log[k].Command
				rafts[i].mutex.Unlock()

				if !bytes.Equal(cmdI, rafts[j].Log[k].Command) {
					rafts[j].mutex.Unlock()
					t.Fatalf("Figure 8 failure: Logs diverged at index %d between node %d and node %d", k, i, j)
				}
			}
			rafts[j].mutex.Unlock()
		}
	}
}

func TestRaft_UnreliableNetwork(t *testing.T) {
	network, rafts := CreateMockNetwork(t, 5)
	defer func() {
		for i := 0; i < 5; i++ {
			rafts[i].Kill()
		}
	}()

	time.Sleep(1 * time.Second)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				node1 := rand.Intn(5)
				node2 := rand.Intn(5)
				network.Disconnect(node1)

				time.Sleep(time.Duration(rand.Intn(15)) * time.Millisecond)

				network.Connect(node2)
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(cmdNum int) {
			defer wg.Done()
			cmd := fmt.Appendf(nil, "STRESS_%d", cmdNum)

			for j := 0; j < 5; j++ {
				if rafts[j].GetState().IsLeader {
					rafts[j].Start(cmd)
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}(i)
	}

	wg.Wait()

	close(done)
	for i := 0; i < 5; i++ {
		network.Connect(i)
	}

	time.Sleep(3 * time.Second)

	agreed := 0
	for i := 0; i < 5; i++ {
		rafts[i].mutex.Lock()
		if len(rafts[i].Log) > 1 {
			agreed++
		}
		rafts[i].mutex.Unlock()
	}

	if agreed < 3 {
		t.Fatalf("Unreliable network test failed: cluster could not replicate data after network healed")
	}
}
