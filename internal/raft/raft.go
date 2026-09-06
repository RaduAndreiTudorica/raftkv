package raft

import (
	"context"
	"sync"
	"time"

	"github.com/RaduAndreiTudorica/raftkv/proto"
)

const (
	Leader    string = "leader"
	Candidate string = "candidate"
	Follower  string = "follower"
)

type Raft struct {
	proto.UnimplementedRaftServer

	mutex           sync.Mutex
	Peers           []proto.RaftClient
	Me              int
	Role            string
	CurrentTerm     int
	VotedFor        int
	LastMessage     time.Time
	CommitIndex     int
	LastApllied     int
	NextIndex       []int
	MatchIndex      []int
	ElectionTimeout time.Duration
}

type State struct {
	isLeader    bool
	currentTerm int
	lastMessage time.Time
}

func NewRaft(me int, peers []proto.RaftClient) *Raft {
	return &Raft{
		Peers:           peers,
		Me:              me,
		Role:            Follower,
		CurrentTerm:     0,
		VotedFor:        -1,
		CommitIndex:     0,
		LastApllied:     0,
		NextIndex:       nil,
		MatchIndex:      nil,
		ElectionTimeout: time.Duration(100) * time.Millisecond,
	}
}

func (rf *Raft) GetState() State {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	isLeader := rf.Role == Leader

	return State{
		isLeader:    isLeader,
		currentTerm: rf.CurrentTerm,
		lastMessage: rf.LastMessage,
	}
}

// func (rf *Raft) ticker() {
// 	for true {
// 		state := rf.GetState()
// 		if state.isLeader && time.Since(state.lastMessage) > rf.ElectionTimeout {

// 		} else {

// 		}

// 		ms := 300 + rand.Intn(300)
// 		time.Sleep(time.Duration(ms) * time.Millisecond)
// 	}
// }

func (rf *Raft) SendRequestVote(peer proto.RaftClient, ctx context.Context, request *proto.RequestVoteArgs) {

}

func (rf *Raft) RequestVote(ctx context.Context, request *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	return nil, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, append *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	return nil, nil
}
