package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/RaduAndreiTudorica/raftkv/internal/raft"
	"github.com/RaduAndreiTudorica/raftkv/internal/server"
	"github.com/RaduAndreiTudorica/raftkv/internal/store"
	"github.com/RaduAndreiTudorica/raftkv/proto"
)

func connectToPeers(peerAddrs []string) []proto.RaftClient {
	var clients []proto.RaftClient

	for _, addr := range peerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to peer %s: %v", addr, err)
		}
		clients = append(clients, proto.NewRaftClient(conn))
	}

	return clients
}

func main() {
	homeDir, err := os.UserHomeDir()

	if err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(homeDir, ".raftkv", "data")
	walPath := flag.String("walPath", path, "path to the write-ahead log file")
	raftPath := flag.String("raftPath", filepath.Join(path, "raft_state.dat"), "path to the raft state file")
	port := flag.String("port", "50051", "port the server listens to")
	flag.Parse()

	if os.Getenv("PORT") != "" {
		*port = os.Getenv("PORT")
	}

	if os.Getenv("WALPATH") != "" {
		*walPath = os.Getenv("WALPATH")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	newStore, err := store.NewStore(*walPath)
	if err != nil {
		log.Fatalf("failed to initialize the store: %v", err)
	}

	nodeIDStr := os.Getenv("NODE_ID")
	myID, _ := strconv.Atoi(nodeIDStr)

	peersEnv := os.Getenv("RAFT_PEERS")
	peerAddr := strings.Split(peersEnv, ",")
	raftClients := connectToPeers(peerAddr)

	persister := raft.NewPersister(*raftPath)

	applyCh := make(chan raft.ApplyMsg)

	nodeRaft := raft.NewRaft(myID, raftClients, persister, applyCh)

	s := grpc.NewServer()
	proto.RegisterKVServer(s, server.NewServer(newStore, nodeRaft, applyCh))
	proto.RegisterRaftServer(s, nodeRaft)

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
