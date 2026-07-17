# raftkv

> A distributed, replicated key-value store built on the Raft consensus algorithm.
> Written in Go, with a pluggable storage engine in Rust and a real-time cluster
> dashboard in Elixir. Deployed on Kubernetes.

[![CI](https://github.com/RaduAndreiTudorica/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/RaduAndreiTudorica/raftkv/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/RaduAndreiTudorica/raftkv)](https://goreportcard.com/report/github.com/RaduAndreiTudorica/raftkv)
[![License: BSD-3](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

---

## What it is

`raftkv` is a fault-tolerant key-value store. Multiple nodes keep the same data
in sync via the Raft consensus protocol, so the cluster keeps serving reads and
writes even when a minority of nodes crash. It exposes a gRPC API and ships with
metrics, a live dashboard, and Kubernetes manifests.

## Architecture

```
        ┌──────────┐   gRPC    ┌──────────┐
client ─▶│  node A  │◀─Raft──▶│  node B  │
        │ (leader) │          │(follower)│
        └────┬─────┘          └────┬─────┘
             │        Raft         │
             │     ┌──────────┐    │
             └────▶│  node C  │◀───┘
                   │(follower)│
                   └──────────┘

  each node = Go server (gRPC + Raft) ──▶ storage engine (Rust: WAL + LSM)
  Elixir/Phoenix LiveView dashboard subscribes to cluster state (leader, lag, up/down)
```

## Features

- Strong consistency via Raft (leader election, log replication, snapshots)
- gRPC / Protocol Buffers API: `Get`, `Put`, `Delete`
- Write-Ahead Log for durability
- Pluggable storage backend (in-memory map, or Rust LSM engine)
- Prometheus metrics + Grafana dashboards
- Kubernetes-native (StatefulSet, headless Service, PVCs)

## Quickstart

Run a 3-node cluster locally:

```bash
docker compose up --build          # 3 nodes on ports 8081-8083
grpcurl -plaintext localhost:8081 raftkv.KV/Put ...
```

Deploy on Kubernetes (kind/minikube):

```bash
kubectl apply -f deploy/k8s/
kubectl get pods -l app=raftkv
```

## Project structure

| Path            | Purpose                                       |
|-----------------|-----------------------------------------------|
| `cmd/`          | Binary entry points                           |
| `internal/raft` | Consensus layer                               |
| `internal/store`| KV state machine + WAL                         |
| `proto/`        | gRPC / protobuf definitions                    |
| `engine/`       | Rust storage engine (LSM + WAL)                |
| `dashboard/`    | Elixir / Phoenix LiveView cluster dashboard    |
| `deploy/`       | Dockerfile, docker-compose, k8s manifests, Helm|

## Development

```bash
go build ./...
go test -race -cover ./...     # -race is mandatory here
golangci-lint run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for commit conventions and workflow.

## Roadmap

- [ ] Single-node KV store (gRPC + WAL)
- [ ] Docker + compose
- [ ] Raft replication (hashicorp/raft)
- [ ] Kubernetes deployment (StatefulSet)
- [ ] Prometheus + Grafana observability
- [ ] Elixir LiveView dashboard
- [ ] Rust storage engine (LSM-tree)
- [ ] Sharding via consistent hashing

## License

BSD-3-Clause. See [LICENSE](LICENSE).
