# Contributing to raftkv

Even as a solo project, this repo follows a production-style workflow. The point
is to build the reflexes real engineering teams rely on.

## Workflow

1. `main` is protected. Never commit to it directly.
2. Branch per unit of work: `feat/raft-snapshot`, `fix/wal-corruption`, `docs/readme`.
3. Open a Pull Request — yes, even for your own changes. Write a real description.
4. CI must be green (build + tests + lint) before merge.
5. Merge with **squash** or **rebase** to keep history linear and readable.

## Commit messages

We follow the Linux-kernel / Unikraft style: a component prefix and an
imperative summary.

```
subsystem: Imperative summary under ~50 chars

Body wrapped at 72 columns. Explain WHY the change is needed and what it
does at a high level. The diff already shows HOW. Reference papers or
issues where relevant (e.g. Raft paper section 5.2, closes #12).

Signed-off-by: Your Name <you@example.com>
```

Rules:

- **Component prefix + colon.** Use one of: `raft`, `store`, `wal`, `grpc`,
  `proto`, `cluster`, `metrics`, `k8s`, `docker`, `engine`, `dashboard`, `ci`,
  `docs`, `cmd`.
- **Imperative mood:** "Add", "Fix", "Remove" — not "Added" / "Fixes".
- **No trailing period** on the subject line.
- **One logical change per commit.** Split unrelated changes.
- **Sign off every commit** with `git commit -s`. This adds the
  `Signed-off-by:` line and certifies the change under the Developer
  Certificate of Origin (DCO), same as the Unikraft project.

Good examples:

```
raft: Add election timeout jitter to avoid split votes
store: Persist WAL entries before acking writes
k8s: Use StatefulSet for stable node identity
```

Set the shared template once:

```bash
git config commit.template .gitmessage
```

## Before you push

```bash
gofmt -l .            # must print nothing
go vet ./...
golangci-lint run
go test -race -cover ./...
```

Or install the hooks so this runs automatically:

```bash
lefthook install
```

## Testing conventions

- Table-driven tests in Go; always run with `-race`.
- Integration tests spin up a multi-node cluster and assert survival after a
  node is killed.
- Keep unit tests next to the code (`foo_test.go`); end-to-end tests under `test/`.
