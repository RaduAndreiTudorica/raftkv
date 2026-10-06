package raft

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "google.golang.org/protobuf/proto"

	"github.com/RaduAndreiTudorica/raftkv/proto"
)

const (
	Leader    string = "leader"
	Candidate string = "candidate"
	Follower  string = "follower"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	proto.UnimplementedRaftServer

	mutex sync.Mutex

	// Node metadata & identity
	Me   int
	Role string
	dead atomic.Bool

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Persistent state on all servers
	persister   *Persister
	CurrentTerm int64
	VotedFor    int
	LeaderId    int
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
	IsLeader    bool
	LeaderId    int
	CurrentTerm int64
	LastMessage time.Time
}

func NewRaft(me int, peers []proto.RaftClient, pr *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		Peers:       peers,
		Me:          me,
		Role:        Follower,
		persister:   pr,
		applyCh:     applyCh,
		LeaderId:    -1,
		CurrentTerm: 0,
		VotedFor:    -1,
		CommitIndex: 0,
		LastApplied: 0,
		NextIndex:   nil,
		MatchIndex:  nil,
		LastMessage: time.Now(),
		Log:         []*proto.LogEntry{{Index: 0, Term: 0}},
	}

	rf.applyCond = sync.NewCond(&rf.mutex)

	rf.resetElectionTimeout()

	data := rf.persister.readState()
	rf.readPersist(data)

	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) resetElectionTimeout() {
	base := 200 + (rf.Me * 70)
	jitter := rand.Intn(150)
	rf.ElectionTimeout = time.Duration(base+jitter) * time.Millisecond
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}

	raftState := &proto.RaftState{}
	err := pb.Unmarshal(data, raftState)
	if err != nil {
		return
	}

	rf.CurrentTerm = raftState.CurrentTerm
	rf.VotedFor = int(raftState.VotedFor)
	rf.LastIncludedIndex = raftState.LastIncludedIndex
	rf.LastIncludedTerm = raftState.LastIncludedTerm
	rf.Log = raftState.Entries
}

func (rf *Raft) persist() {
	raftState := &proto.RaftState{
		CurrentTerm:       rf.CurrentTerm,
		VotedFor:          int64(rf.VotedFor),
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Entries:           rf.Log,
	}

	data, err := pb.Marshal(raftState)
	if err != nil {
		return
	}

	err = rf.persister.saveState(data)
	if err != nil {
		return
	}
}

func (rf *Raft) GetState() State {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	isLeader := rf.Role == Leader

	return State{
		IsLeader:    isLeader,
		LeaderId:    rf.LeaderId,
		CurrentTerm: rf.CurrentTerm,
		LastMessage: rf.LastMessage,
	}
}

func (rf *Raft) lastLog() (int64, int64) {
	if len(rf.Log) == 0 {
		return rf.LastIncludedIndex, rf.LastIncludedTerm
	}
	lastIdx := len(rf.Log) - 1
	return rf.Log[lastIdx].Index, rf.Log[lastIdx].Term
}

func (rf *Raft) heartBeat() {
	rf.mutex.Lock()
	if rf.Role != Leader {
		rf.mutex.Unlock()
		return
	}
	rf.LastMessage = time.Now()
	me := rf.Me
	peers := rf.Peers
	currentTerm := rf.CurrentTerm
	rf.mutex.Unlock()

	for index, peer := range peers {
		if index == me {
			continue
		}

		go func(targetID int, p proto.RaftClient) {
			rf.mutex.Lock()
			if rf.Role != Leader || rf.CurrentTerm != currentTerm {
				rf.mutex.Unlock()
				return
			}

			nextIdx := rf.NextIndex[targetID]
			prevLogIdx := nextIdx - 1
			prevLogTerm := rf.getEntryTerm(int(prevLogIdx))

			sliceStart := nextIdx - rf.LastIncludedIndex
			var entries []*proto.LogEntry
			if sliceStart >= 0 && sliceStart < int64(len(rf.Log)) {
				entries = rf.Log[sliceStart:]
			}

			args := &proto.AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     int64(rf.Me),
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.CommitIndex,
			}
			rf.mutex.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			reply, err := p.AppendEntries(ctx, args)
			if err != nil {
				return
			}

			rf.mutex.Lock()
			defer rf.mutex.Unlock()

			if rf.Role != Leader || rf.CurrentTerm != args.Term {
				return
			}
			if reply.Term > rf.CurrentTerm {
				rf.Role = Follower
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term
				rf.persist()
				return
			}

			if reply.Success {
				rf.MatchIndex[targetID] = args.PrevLogIndex + int64(len(args.Entries))
				rf.NextIndex[targetID] = rf.MatchIndex[targetID] + 1
				lastLogIndex, _ := rf.lastLog()
				for N := lastLogIndex; N > rf.CommitIndex; N-- {
					if rf.getEntryTerm(int(N)) == rf.CurrentTerm {
						replicas := 1
						for i := range rf.Peers {
							if i != rf.Me && rf.MatchIndex[i] >= N {
								replicas++
							}
						}
						if replicas > len(rf.Peers)/2 {
							rf.CommitIndex = N
							rf.applyCond.Broadcast()
							break
						}
					}
				}
			} else {
				if reply.ConflictIndex > 0 {
					rf.NextIndex[targetID] = reply.ConflictIndex
				} else {
					rf.NextIndex[targetID] = max(int64(1), rf.NextIndex[targetID]-1)
				}
			}
		}(index, peer)
	}
}

func (rf *Raft) startElection() {
	rf.mutex.Lock()
	rf.Role = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.Me
	rf.resetElectionTimeout()
	rf.LastMessage = time.Now()
	rf.persist()

	lastIndex, lastTerm := rf.lastLog()
	voteArgs := &proto.RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  int64(rf.Me),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	currentTerm := rf.CurrentTerm
	peers := rf.Peers
	me := rf.Me
	rf.mutex.Unlock()

	votes := 1
	var voteMu sync.Mutex

	for index, peer := range peers {
		if index == me {
			continue
		}

		go func(targetID int, p proto.RaftClient) {
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
				rf.persist()
				return
			}

			if rf.Role != Candidate || rf.CurrentTerm != currentTerm {
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				gotQuorum := votes > len(peers)/2
				voteMu.Unlock()

				if gotQuorum && rf.Role == Candidate {
					rf.Role = Leader

					lastIdx, _ := rf.lastLog()
					rf.NextIndex = make([]int64, len(rf.Peers))
					rf.MatchIndex = make([]int64, len(rf.Peers))
					for i := range rf.Peers {
						rf.NextIndex[i] = lastIdx + 1
						rf.MatchIndex[i] = 0
					}

					go rf.heartBeat()
				}
			}
		}(index, peer)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mutex.Lock()

		switch rf.Role {
		case Leader:
			if time.Since(rf.LastMessage) > 100*time.Millisecond {
				rf.mutex.Unlock()
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
		rf.persist()
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
	rf.persist()

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

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}

	rf.Role = Follower
	rf.LeaderId = int(args.LeaderId)
	rf.LastMessage = time.Now()

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

	var changed bool
	for i, entry := range args.Entries {
		entryIndex := args.PrevLogIndex + 1 + int64(i)
		lastLogIdx, _ := rf.lastLog()

		if entryIndex <= lastLogIdx {
			if rf.getEntryTerm(int(entryIndex)) != entry.Term {
				sliceIndex := entryIndex - rf.LastIncludedIndex
				rf.Log = rf.Log[:sliceIndex]

				rf.Log = append(rf.Log, args.Entries[i:]...)
				changed = true
				break
			}
		} else {
			rf.Log = append(rf.Log, entry)
			changed = true
		}
	}

	if changed {
		rf.persist()
	}

	if args.LeaderCommit > rf.CommitIndex {
		lastIndex, _ := rf.lastLog()
		rf.CommitIndex = min(lastIndex, args.LeaderCommit)
	}

	return &proto.AppendEntriesReply{
		Success: true,
	}, nil
}

func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if rf.Role != Leader {
		return -1, -1, false
	}

	lastIndex, _ := rf.lastLog()
	newIndex := lastIndex + 1
	term := rf.CurrentTerm

	entry := &proto.LogEntry{
		Index:   newIndex,
		Term:    term,
		Command: command,
	}

	rf.Log = append(rf.Log, entry)
	rf.MatchIndex[rf.Me] = newIndex
	rf.NextIndex[rf.Me] = newIndex + 1
	rf.persist()

	go rf.heartBeat()

	return int(newIndex), int(term), true
}

func (rf *Raft) applier() {
	for {
		rf.mutex.Lock()
		for rf.CommitIndex <= rf.LastApplied {
			rf.applyCond.Wait()
		}

		var commands []interface{}
		var indexes []int
		for index := rf.LastApplied + 1; index <= rf.CommitIndex; index++ {
			commands = append(commands, rf.Log[index].Command)
			indexes = append(indexes, int(index))
		}
		rf.LastApplied = rf.CommitIndex
		rf.mutex.Unlock()

		for i, cmd := range commands {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: indexes[i],
			}

		}
	}
}

func (rf *Raft) Kill() {
	rf.dead.Store(true)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}
