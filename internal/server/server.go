package server

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/protobuf/proto"

	"github.com/RaduAndreiTudorica/raftkv/internal/raft"
	"github.com/RaduAndreiTudorica/raftkv/internal/store"
	"github.com/RaduAndreiTudorica/raftkv/proto"
)

type MapStore interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Delete([]byte) error
	Snapshot() (io.Reader, error)
	Restore(reader io.Reader) error
	Close() error
}

type Server struct {
	proto.UnimplementedKVServer
	Store   *store.Store
	Raft    *raft.Raft
	ApplyCh chan raft.ApplyMsg

	mu       sync.Mutex
	notifyCh map[int]chan struct{}
}

func NewServer(store *store.Store, nodeRaft *raft.Raft, applyCh chan raft.ApplyMsg) *Server {
	server := &Server{
		Store:    store,
		Raft:     nodeRaft,
		ApplyCh:  applyCh,
		notifyCh: make(map[int]chan struct{}),
	}

	go server.applier()

	return server
}

func (server *Server) applier() {
	for msg := range server.ApplyCh {
		if msg.CommandValid {
			cmdBytes := msg.Command.([]byte)

			var wrapper proto.InternalCommand
			if err := pb.Unmarshal(cmdBytes, &wrapper); err != nil {
				continue
			}

			switch wrapper.Type {
			case proto.OpType_PUT:
				var req proto.PutRequest
				if err := pb.Unmarshal(wrapper.Payload, &req); err == nil {
					err := server.Store.Put(req.Key, req.Value)
					if err != nil {
						return
					}
				}
			case proto.OpType_DELETE:
				var req proto.DeleteRequest
				if err := pb.Unmarshal(wrapper.Payload, &req); err == nil {
					err := server.Store.Delete(req.Key)
					if err != nil {
						return
					}
				}
			}

			server.mu.Lock()
			ch, exists := server.notifyCh[msg.CommandIndex]
			if exists {
				ch <- struct{}{}
				delete(server.notifyCh, msg.CommandIndex)
			}
			server.mu.Unlock()
		}
	}
}

func (server *Server) Ping(ctx context.Context, request *proto.PingRequest) (*proto.PingResponse, error) {
	state := server.Raft.GetState()

	return &proto.PingResponse{
		IsLeader: state.IsLeader,
		LeaderId: int32(state.LeaderId),
	}, nil
}

func (server *Server) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	value, exists := server.Store.Get(request.Key)
	return &proto.GetResponse{Value: value, Exists: exists}, nil
}

func (server *Server) Put(ctx context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	payload, err := pb.Marshal(request)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to marshal")
	}

	command := &proto.InternalCommand{
		Type:    proto.OpType_PUT,
		Payload: payload,
	}

	commandBytes, err := pb.Marshal(command)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to marshal")
	}

	index, _, isLeader := server.Raft.Start(commandBytes)
	if !isLeader {
		return nil, status.Errorf(codes.Unavailable, "not the leader: %d", server.Raft.LeaderId)
	}

	server.mu.Lock()
	ch := make(chan struct{}, 1)
	server.notifyCh[index] = ch
	server.mu.Unlock()

	select {
	case <-ch:
		return &proto.PutResponse{}, nil
	case <-time.After(2 * time.Second):
		server.mu.Lock()
		delete(server.notifyCh, index)
		server.mu.Unlock()
		return nil, status.Error(codes.DeadlineExceeded, "raft consensus timeout")
	}
}

func (server *Server) Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	payload, err := pb.Marshal(request)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to marshal")
	}

	command := &proto.InternalCommand{
		Type:    proto.OpType_DELETE,
		Payload: payload,
	}

	commandBytes, err := pb.Marshal(command)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to marshal")
	}

	index, _, isLeader := server.Raft.Start(commandBytes)
	if !isLeader {
		return nil, status.Errorf(codes.Unavailable, "not the leader: %d", server.Raft.LeaderId)
	}

	server.mu.Lock()
	ch := make(chan struct{}, 1)
	server.notifyCh[index] = ch
	server.mu.Unlock()

	select {
	case <-ch:
		return &proto.DeleteResponse{}, nil
	case <-time.After(2 * time.Second):
		server.mu.Lock()
		delete(server.notifyCh, index)
		server.mu.Unlock()
		return nil, status.Error(codes.DeadlineExceeded, "raft consensus timeout")
	}
}
