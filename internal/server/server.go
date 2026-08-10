package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/RaduAndreiTudorica/raftkv/internal/store"
	"github.com/RaduAndreiTudorica/raftkv/proto"
)

type Server struct {
	proto.UnimplementedKVServer
	Store *store.Store
}

func NewServer(store *store.Store) *Server {
	return &Server{Store: store}
}

func (server *Server) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	value, exists := server.Store.Get(request.Key)
	return &proto.GetResponse{Value: value, Exists: exists}, nil
}

func (server *Server) Put(ctx context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	key := request.Key
	value := request.Value
	err := server.Store.Put(key, value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &proto.PutResponse{}, nil
}

func (server *Server) Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	key := request.Key
	err := server.Store.Delete(key)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return &proto.DeleteResponse{}, nil
}
