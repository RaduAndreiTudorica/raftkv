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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/RaduAndreiTudorica/raftkv/proto"
)

var (
	addr             = flag.String("addr", "localhost:60000", "the address to connect to")
	client           proto.KVClient
	ErrValueNotFound = errors.New("not Found")
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
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := scanner.Text()
		token, args, err := parseCommand(command)

		if err != nil {
			fmt.Println(err)
		} else {
			switch token {
			case "put":
				err := putMessage(args)
				if err != nil {
					fmt.Println(err)
				}
			case "get":
				value, err := getMessage(args)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(value)
				}
			case "delete":
				err := deleteMessage(args)
				if err != nil {
					fmt.Println(err)
				}
			case "help":
				printHelp()
			case "exit":
				os.Exit(0)
			}
		}

	}
}

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client = proto.NewKVClient(conn)
	cli()
}
