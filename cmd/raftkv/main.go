package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/RaduAndreiTudorica/raftkv/internal/server"
	"github.com/RaduAndreiTudorica/raftkv/internal/store"
	"github.com/RaduAndreiTudorica/raftkv/proto"
)

func main() {
	homeDir, err := os.UserHomeDir()

	if err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(homeDir, ".raftkv", "data")
	walPath := flag.String("walPath", path, "path to the write-ahead log file")
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

	s := grpc.NewServer()
	proto.RegisterKVServer(s, server.NewServer(newStore))
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
