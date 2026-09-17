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

	mutex sync.Mutex

	// Node metadata & identity
	Me   int
	Role string

	// Persistent state on all servers
	CurrentTerm int64
	VotedFor    int
	Log         []*proto.LogEntry

	// Snapshotting & compaction window
	LastIncludedIndex int64
	LastIncludedTerm  int64

	// Volatile state on all servers
	CommitIndex int64
	LastApplied int64

	// Network connectivity & election timers
	Peers           []proto.RaftClient
	LastMessage     time.Time
	ElectionTimeout time.Duration

	// Volatile state on leaders
	NextIndex  []int64
	MatchIndex []int64
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
		LastMessage:     time.Now(),
		Log:             []*proto.LogEntry{{Index: 0, Term: 0}},
		ElectionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
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
		return rf.LastIncludedIndex, rf.LastIncludedTerm
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
	rf.VotedFor = rf.Me
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

				if gotQuorum {

					shouldHeartbeat := false
					rf.mutex.Lock()
					if rf.Role == Candidate && rf.CurrentTerm == voteArgs.Term {
						rf.Role = Leader
						shouldHeartbeat = true

						lastIdx, _ := rf.lastLog()
						rf.NextIndex = make([]int64, len(rf.Peers))
						rf.MatchIndex = make([]int64, len(rf.Peers))
						for i := range rf.Peers {
							rf.NextIndex[i] = lastIdx + 1
							rf.MatchIndex[i] = 0
						}
					}
					rf.mutex.Unlock()

					if shouldHeartbeat {
						go rf.heartBeat()
					}
				}

			}
		}(peer)
	}
}

func (rf *Raft) ticker() {
	for {
		rf.mutex.Lock()

		switch rf.Role {
		case Leader:
			if time.Since(rf.LastMessage) > 100*time.Millisecond {
				go rf.heartBeat()
			} else {
				rf.mutex.Unlock()
			}
		default:
			if time.Since(rf.LastMessage) > rf.ElectionTimeout {
				rf.mutex.Unlock()
				rf.startElection()
			} else {
				rf.mutex.Unlock()
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) RequestVote(ctx context.Context, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if args.Term < rf.CurrentTerm {
		return &proto.RequestVoteReply{
			Term:        rf.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	if args.Term > rf.CurrentTerm {
		rf.Role = Follower
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}

	if rf.VotedFor != -1 && rf.VotedFor != int(args.CandidateId) {
		return &proto.RequestVoteReply{
			Term:        rf.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	lastLogIndex, lastLogTerm := rf.lastLog()

	if args.LastLogTerm < int64(lastLogTerm) {
		return &proto.RequestVoteReply{
			Term:        rf.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	if args.LastLogTerm == int64(lastLogTerm) && args.LastLogIndex < int64(lastLogIndex) {
		return &proto.RequestVoteReply{
			Term:        rf.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	rf.LastMessage = time.Now()
	rf.VotedFor = int(args.CandidateId)

	// write in a file in case of a crash

	return &proto.RequestVoteReply{
		Term:        rf.CurrentTerm,
		VoteGranted: true,
	}, nil
}

func (rf *Raft) getEntryTerm(idx int) int64 {
	globalIndex := int64(idx) - rf.LastIncludedIndex

	if globalIndex >= 0 && globalIndex < int64(len(rf.Log)) {
		return rf.Log[globalIndex].Term
	} else {
		return -1
	}
}

func (rf *Raft) AppendEntries(ctx context.Context, args *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if args.Term < rf.CurrentTerm {
		return &proto.AppendEntriesReply{
			Term:    rf.CurrentTerm,
			Success: false,
		}, nil
	}

	lastLogIndex, _ := rf.lastLog()
	if lastLogIndex < args.PrevLogIndex {
		return &proto.AppendEntriesReply{
			Term:          rf.CurrentTerm,
			Success:       false,
			ConflictIndex: lastLogIndex + 1,
			ConflictTerm:  0,
		}, nil
	}

	entryTerm := rf.getEntryTerm(int(args.PrevLogIndex))
	if entryTerm != args.PrevLogTerm {
		conflictIdx := args.PrevLogIndex
		for conflictIdx > rf.LastIncludedIndex && rf.getEntryTerm(int(conflictIdx)-1) == entryTerm {
			conflictIdx--
		}
		return &proto.AppendEntriesReply{
			Term:          rf.CurrentTerm,
			Success:       false,
			ConflictTerm:  entryTerm,
			ConflictIndex: conflictIdx,
		}, nil
	}

	for i, entry := range args.Entries {
		entryIndex := args.PrevLogIndex + 1 + int64(i)
		lastLogIdx, _ := rf.lastLog()

		if entryIndex <= lastLogIdx {
			if rf.getEntryTerm(int(entryIndex)) != args.PrevLogTerm {
				sliceIndex := entryIndex - rf.LastIncludedIndex
				rf.Log = rf.Log[:sliceIndex]

				rf.Log = append(rf.Log, args.Entries[i:]...)

				break
			}
		} else {
			rf.Log = append(rf.Log, entry)
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		lastIndex, _ := rf.lastLog()
		rf.CommitIndex = min(lastIndex, args.LeaderCommit)
	}

	return &proto.AppendEntriesReply{
		Success: true,
	}, nil
}
