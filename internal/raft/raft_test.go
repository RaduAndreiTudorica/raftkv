package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RaduAndreiTudorica/raftkv/proto"
	"google.golang.org/grpc"
)

type MockNetwork struct {
	mu        sync.Mutex
	nodes     map[int]*Raft
	connected map[int]bool
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
		nodes:     make(map[int]*Raft),
		connected: make(map[int]bool),
	}

	rafts := make([]*Raft, numNodes)

	for i := 0; i < numNodes; i++ {
		net.connected[i] = true
	}

	for i := 0; i < numNodes; i++ {
		peers := make([]proto.RaftClient, numNodes)
        for j := 0; j < numNodes; j++ {
            peers[j] = &MockClient{
                net:      net, 
                senderID: i,
                serverID: j,
            }
        }

		stateFile := filepath.Join(t.TempDir(), fmt.Sprintf("state_node_%d.bin", i))

		persister := newPersister(stateFile)

		rafts[i] = NewRaft(i, peers, persister)
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

func TestRaft_InitialElectiont(t *testing.T) {
	_, rafts := CreateMockNetwork(t, 3)

	time.Sleep(1 * time.Second)

	leaders := 0
	leaderTerm := int64(0)

	for i := 0 ; i < 3; i++ {
		state := rafts[i].GetState()
		if state.isLeader {
			leaders++
			leaderTerm = state.currentTerm
		}
	}

	if leaders != 1 {
		t.Fatalf("Failed selection: expected 1 leader, found %d", leaders)
	}

	for i := 0 ; i < 3; i++ { 
		state := rafts[i].GetState()
        if !state.isLeader && state.currentTerm != leaderTerm {
            t.Fatalf("Term mismatch: node %d has %d, leader has %d", i, state.currentTerm, leaderTerm)
        }
	}
}

func TestReElection(t *testing.T) {
    network, rafts := CreateMockNetwork(t, 3)

    time.Sleep(1 * time.Second)
    oldLeaderID := -1
    var oldLeaderTerm int64

    for i := 0; i < 3; i++ {
        state := rafts[i].GetState()
        if state.isLeader {
            oldLeaderID = i
            oldLeaderTerm = state.currentTerm
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
        if state.isLeader {
            newLeaderID = i
            newLeaderTerm = state.currentTerm
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
        if state.isLeader {
            leaders++
        }
        if i == oldLeaderID && state.isLeader {
            t.Fatalf("old leader failed to step down as follower")
        }
    }

    if leaders != 1 {
        t.Fatalf("expected exactly 1 leader in cluster, found %d", leaders)
    }
}

func TestBasicAgree(t *testing.T) {
	_, rafts := CreateMockNetwork(t, 3)
	
}
