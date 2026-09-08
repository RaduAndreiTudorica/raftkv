package raft

import (
	"context"
	"math/rand"
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
	CurrentTerm     int64
	VotedFor        int
	LastMessage     time.Time
	CommitIndex     int
	LastApplied     int
	NextIndex       []int
	MatchIndex      []int
	ElectionTimeout time.Duration
	Log             []proto.LogEntry
}

type State struct {
	isLeader    bool
	currentTerm int64
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
		LastApplied:     0,
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

func (rf *Raft) lastLog() (int64, int64) {
	if len(rf.Log) == 0 {
		return 0, 0
	}
	lastIdx := len(rf.Log) - 1
	return rf.Log[lastIdx].Index, rf.Log[lastIdx].Term
}

func (rf *Raft) makeRequestVote() *proto.RequestVoteArgs {
	lastIndex, lastTerm := rf.lastLog()
	rf.CurrentTerm++
	voteArgs := &proto.RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  int64(rf.Me),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	return voteArgs
}

func (rf *Raft) heartBeat() {
	rf.mutex.Lock()

	if rf.Role != Leader {
		rf.mutex.Unlock()
	}

	rf.LastMessage = time.Now()
	me := rf.Me
	peers := rf.Peers
	args := &proto.AppendEntriesArgs{
		Term:     rf.CurrentTerm,
		LeaderId: int64(me),
		Entries:  nil,
	}
	rf.mutex.Unlock()

	for index, peer := range peers {
		if index == me {
			continue
		}

		go func(p proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			reply, err := p.AppendEntries(ctx, args)
			if err != nil {
				return
			}

			rf.mutex.Lock()
			defer rf.mutex.Unlock()
			if reply.Term > rf.CurrentTerm {
				rf.Role = Follower
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term
			}
		}(peer)
	}
}

func (rf *Raft) startElection() {
	rf.mutex.Lock()
	rf.Role = Candidate
	rf.ElectionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.LastMessage = time.Now()

	voteArgs := rf.makeRequestVote()
	peers := rf.Peers
	me := rf.Me
	rf.mutex.Unlock()

	votes := 1
	var voteMu sync.Mutex

	for index, peer := range peers {
		if index == me {
			continue
		}

		go func(p proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			reply, err := p.RequestVote(ctx, voteArgs)
			if err != nil {
				return
			}

			rf.mutex.Lock()
			defer rf.mutex.Unlock()

			if reply.Term > rf.CurrentTerm {
				rf.Role = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				return
			}

			if rf.Role != Candidate || rf.CurrentTerm != voteArgs.Term {
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				gotQuorum := votes > len(peers)/2
				voteMu.Unlock()

				if gotQuorum && rf.Role == Candidate {
					rf.Role = Leader
					go rf.heartBeat()
				}
			}
		}(peer)
	}
}

func (rf *Raft) ticker() {
	for {
		rf.mutex.Lock()
		if rf.Role != Leader && time.Since(rf.LastMessage) > rf.ElectionTimeout {
			rf.mutex.Unlock()
			rf.startElection()
		} else if rf.Role == Leader && time.Since(rf.LastMessage) > 100*time.Millisecond {
			rf.mutex.Unlock()
			go rf.heartBeat()
		} else {
			rf.mutex.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) RequestVote(ctx context.Context, request *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	return nil, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, append *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	return nil, nil
}
