package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/RaduAndreiTudorica/raftkv/proto"
)

var (
	addrs            = flag.String("addrs", "localhost:60000,localhost:60001,localhost:60002", "the address to connect to")
	nodes            []string
	conn             *grpc.ClientConn
	client           proto.KVClient
	ErrValueNotFound = errors.New("not found")
)

const raftBanner = " .  ~  .        ___       __ _   _  ____   __\n~  .  ~  .     | _ \\__ _ / _| |_| |/ " +
	"/\\ \\ / /\n ~ [====] ~    |   / _` |  _|  _| ' <  \\ V /\n~  ~   ~  ~    |_|_\\__,_|_|  \\__|_|\\_\\  " +
	"|_|\n . ~  .  ~            raft consensus  .  v0.1.0"

func invalidNumberOfArgs(token string, expected, got int) error {
	err := fmt.Sprintf("invalid number of arguments for %s: expected %d, got %d", token, expected, got)
	return errors.New(err)
}

func unknownCommand(command string) error {
	err := fmt.Sprintf("unknown command: %s", command)
	return errors.New(err)
}

func printHelp() {
	fmt.Println(`Available commands:
  put <key> <value>   Store a value under the given key
  get <key>            Retrieve the value for the given key
  delete <key>         Remove the given key and its value
  help                 Show this message

Examples:
  > put username JeaniCurcubeu
  > get username
  > delete username`)
}

func parseCommand(command string) (string, []string, error) {
	parsedString := strings.Fields(command)
	if len(parsedString) == 0 {
		return "", nil, errors.New("no command")
	}

	token := parsedString[0]
	args := parsedString[1:]

	switch token {
	case "put":
		if len(args) == 2 {
			return token, args, nil
		}

		return "", nil, invalidNumberOfArgs(token, 2, len(args))
	case "get":
		if len(args) == 1 {
			return token, args, nil
		}

		return "", nil, invalidNumberOfArgs(token, 1, len(args))
	case "delete":
		if len(args) == 1 {
			return token, args, nil
		}

		return "", nil, invalidNumberOfArgs(token, 1, len(args))
	case "help":
		if len(args) == 0 {
			return token, args, nil
		}

		return "", nil, invalidNumberOfArgs(token, 0, len(args))
	case "exit":
		if len(args) == 0 {
			return token, args, nil
		}

		return "", nil, invalidNumberOfArgs(token, 0, len(args))
	default:
		return "", nil, unknownCommand(token)
	}
}

func putMessage(args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := []byte(args[0])
	value := []byte(args[1])

	_, err := client.Put(ctx, &proto.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}

	return nil
}

func getMessage(args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := []byte(args[0])

	response, err := client.Get(ctx, &proto.GetRequest{Key: key})
	if err != nil {
		return "", err
	}

	if !response.Exists {
		return "", ErrValueNotFound
	}

	return string(response.Value), nil
}

func deleteMessage(args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := []byte(args[0])

	_, err := client.Delete(ctx, &proto.DeleteRequest{Key: key})
	if err != nil {
		return ErrValueNotFound
	}

	return nil
}

func cli() {
	fmt.Println(raftBanner)
	fmt.Println("Enter <help> to view all the commands")
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("> ")
	for scanner.Scan() {
		command := scanner.Text()
		token, args, err := parseCommand(command)

		if err != nil {
			fmt.Println(err)
		} else {
			switch token {
			case "put":
				err := putMessage(args)
				if err != nil {
					st, ok := status.FromError(err)
					if ok && st.Code() == codes.Unavailable && strings.Contains(st.Message(), "not the leader") {
						fmt.Println("[System] Leader changed or wrong node. Rediscovering cluster...")
						recoverConnection()
						err = putMessage(args)
					}
				}

				if err != nil {
					fmt.Println(err)
				}

			case "get":
				value, err := getMessage(args)
				if err != nil {
					st, ok := status.FromError(err)
					if ok && st.Code() == codes.Unavailable && strings.Contains(st.Message(), "not the leader") {
						fmt.Println("[System] Leader changed or wrong node. Rediscovering cluster...")
						recoverConnection()
						value, err = getMessage(args)
					}
				}

				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(value)
				}

			case "delete":
				err := deleteMessage(args)
				if err != nil {
					st, ok := status.FromError(err)
					if ok && st.Code() == codes.Unavailable && strings.Contains(st.Message(), "not the leader") {
						fmt.Println("[System] Leader changed or wrong node. Rediscovering cluster...")
						recoverConnection()
						err = deleteMessage(args)
					}
				}

				if err != nil {
					fmt.Println(err)
				}
			case "help":
				printHelp()
			case "exit":
				os.Exit(0)
			}
		}
		fmt.Print("> ")
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}
}

func ping(nodes []string) string {
	for _, addr := range nodes {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := proto.NewKVClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		reply, err := client.Ping(ctx, &proto.PingRequest{})
		cancel()
		conn.Close()

		if err != nil {
			fmt.Printf("[Debug] Ping către %s a eșuat: %v\n", addr, err)
			continue
		}

		if err == nil {
			if reply.IsLeader {
				return addr
			}

			if reply.LeaderId >= 0 && int(reply.LeaderId) < len(nodes) {
				return nodes[reply.LeaderId]
			}
		}
	}

	return ""
}

func recoverConnection() {
	if conn != nil {
		conn.Close()
	}

	leader := ping(nodes)
	if leader == "" {
		log.Fatalf("cluster is currently unavailable: no leader found")
	}

	var err error
	conn, err = grpc.NewClient(leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	client = proto.NewKVClient(conn)
}

func main() {
	flag.Parse()
	nodes = strings.Split(*addrs, ",")

	recoverConnection()
	defer conn.Close()

	cli()
}
