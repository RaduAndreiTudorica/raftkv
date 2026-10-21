# raftkv

> A distributed, replicated key-value store built on the Raft consensus algorithm.
> Written in Go. Deployed on Kubernetes.

[![CI](https://github.com/RaduAndreiTudorica/raftkv/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/RaduAndreiTudorica/raftkv/actions/workflows/ci.yml)[![License: BSD-3](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

---

## What it is

`raftkv` is a fault-tolerant key-value store. Multiple nodes keep the same data
in sync via the Raft consensus protocol, so the cluster keeps serving reads and
writes even when a minority of nodes crash. It exposes a gRPC API, an interactive
CLI client, and ships with metrics and Kubernetes manifests.

## Architecture

```
         ┌──────────┐   gRPC   ┌──────────┐
client-> │  node A  │ <-Raft-> │  node B  │
         │ (leader) │          │(follower)│
         └────┬─────┘          └────┬─────┘
              │        Raft         │
              │     ┌──────────┐    │
              └────>│  node C  │<───┘
                    │(follower)│
                    └──────────┘

  each node = Go server (gRPC + Raft consensus) ──▶ storage engine (WAL + KV State Machine)
```

## Features

- Strong consistency via Raft (leader election, log replication)
- gRPC / Protocol Buffers API: `Get`, `Put`, `Delete`
- Write-Ahead Log for durability
- Pluggable storage backend (in-memory map with WAL persistence)
- Prometheus metrics + Grafana dashboards
- Kubernetes-native (StatefulSet, headless Service, PVCs)

## Quickstart

Run a 3-node cluster locally:

```bash
docker compose -f deployments/docker-compose.yaml up --build          # 3 nodes on ports 60000-60002
```

Run the cli client
```bash
# the client will use the port 60000
cd raftkv
docker build -f Dockerfile.cli -t raftkv-cli .
docker run -it --add-host=host.docker.internal:host-gateway raftkv-cli --addrs host.docker.internal:60000,host.docker.internal:60001,host.docker.internal:60002
```


Deploy on Kubernetes (minikube):

```bash
# 0. Start minikube
minikube start
# 1. Apply the configuration, headless service, statefulset, and client deployment
kubectl apply -f deploy/k8s/

# 2. Wait for the 3 Raft nodes and the client pod to be Ready (1/1)
kubectl get pods -w

# 3. Connect to the interactive CLI using the client pod running in the background
kubectl exec -it deployment/raftkv-client -- /raftkv-cli --addrs raftkv-0.raftkv-service:50051,raftkv-1.raftkv-service:50051,raftkv-2.raftkv-service:50051

# 5. Close the containers
kubectl delete statefulset raftkv
kubectl delete service raftkv-service
kubectl delete configmap raftkv-config
kubectl delete deployment raftkv-client

## Project structure

| Path            | Purpose                                       |
|-----------------|-----------------------------------------------|
| `cmd/`          | Binary entry points (server & CLI)            |
| `internal/raft` | Consensus layer                               |
| `internal/store`| KV state machine + WAL                         |
| `proto/`        | gRPC / protobuf definitions                    |
| `deploy/`       | Dockerfile, docker-compose, k8s manifests, Helm|

## Development

```bash
go build ./...
go test -race -cover ./...     # -race is mandatory here
golangci-lint run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for commit conventions and workflow.

## Roadmap

- [x] Single-node KV store (gRPC + WAL)
- [x] CLI client (`raftkv-cli`)
- [x] Docker + compose
- [x] Raft consensus & replication
- [x] Kubernetes deployment (StatefulSet)
- [ ] Prometheus + Grafana observability
- [ ] Sharding via consistent hashing

## License

BSD-3-Clause. See [LICENSE](LICENSE).
