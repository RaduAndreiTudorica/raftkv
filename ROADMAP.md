# raftkv — Roadmap de invatare

Regula de aur: **nu inveti totul inainte.** Fiecare faza iti spune ce inveti
exact atunci, cu 2-4 resurse alese (nu 20). Ordinea conteaza: ancoreaza-te pe Go,
Rust si Elixir sunt accente.

## Cele 2 resurse care acopera tot proiectul

Citeste-le in paralel cu fazele, nu inainte:

- **MIT 6.5840** (fost 6.824), Distributed Systems — `pdos.csail.mit.edu/6.824/`.
  Public: lecturi pe YouTube + laboratoare in Go care implementeaza Raft, un KV
  tolerant la erori si sharding. Practic *este* proiectul tau. Cea mai buna
  resursa care exista pentru asta.
- **Designing Data-Intensive Applications** (DDIA), Martin Kleppmann — biblia.
  Replicare, consens, storage engines, partitionare. Citeste cap. 3, 5, 9.

---

## Faza 0 — Go + design (~25-35h)

**Construiesti:** schema arhitecturii + un HTTP/gRPC server minimal care porneste.

**Inveti:** sintaxa Go, structs, interfaces, goroutines, channels, error handling,
`go mod`, layout de proiect.

**Resurse:**
- *A Tour of Go* — `go.dev/tour` — intro interactiv oficial.
- *Learn Go with Tests* — `github.com/quii/learn-go-with-tests` — gratis, prin
  TDD; perfect fiindca oricum ne pasa de teste.
- *Effective Go* — `go.dev/doc/effective_go` — idiomurile limbii.
- *Go by Example* — `gobyexample.com` — referinta rapida cand te blochezi.

**Gata cand:** poti scrie o goroutine cu channel fara sa te uiti pe net.

---

## Faza 1 — KV store single-node (~25-30h)

**Construiesti:** API gRPC (`Get/Put/Delete`) + persistenta printr-un Write-Ahead Log.

**Inveti:** gRPC, Protocol Buffers, serializare, ce e un WAL si de ce exista.

**Resurse:**
- gRPC Go Quickstart — `grpc.io/docs/languages/go/quickstart/`.
- Protocol Buffers — `protobuf.dev`.
- WAL + storage: DDIA cap. 3 (LSM, B-trees, log-structured storage).

**Gata cand:** oprii serverul, il repornesti, si datele scrise inainte sunt inca acolo.

---

## Faza 2 — Docker (~10h)

**Construiesti:** Dockerfile multi-stage + `docker-compose` cu 3 noduri local.

**Inveti:** imagini, layere, multi-stage builds, networking intre containere.

**Resurse:**
- Docker oficial — `docs.docker.com/get-started/`.
- *Docker Deep Dive*, Nigel Poulton — daca vrei sa intelegi in profunzime.

**Gata cand:** `docker compose up` ridica 3 noduri care se vad intre ele.

---

## Faza 3 — Raft (~35-45h) — PIESA DE REZISTENTA

**Construiesti:** replicare prin consens cu `hashicorp/raft` (leader election, log
replication, snapshots). NU implementa Raft de la zero prima data.

**Inveti:** consens, leader election, log replication, split votes, quorum,
persistenta si snapshots.

**Resurse (in ordinea asta):**
- *The Secret Lives of Data* — `thesecretlivesofdata.com/raft/` — animatie,
  cea mai buna intuitie de start.
- *Raft paper* — "In Search of an Understandable Consensus Algorithm (Extended)",
  Ongaro & Ousterhout — via `raft.github.io`. Citeste sectiunile 5.1-5.4.
- **MIT 6.5840, lab-ul de Raft** — aici il implementezi cu adevarat; are teste
  care simuleaza pierderi de pachete, partitii si crash-uri.
- *Students' Guide to Raft* — `thesquareplanet.com/blog/students-guide-to-raft/` —
  capcanele practice pe care le vei lovi garantat.
- Docs `hashicorp/raft` — libraria pe care o folosim aici.

**Gata cand:** omori leaderul in timpul unei scrieri si clusterul alege alt leader
fara sa piarda date.

---

## Faza 4 — Kubernetes (~35-45h)

**Construiesti:** deploy pe K8s local (`kind`/`minikube`) cu StatefulSet, headless
Service, PersistentVolumeClaim, liveness/readiness probes.

**Inveti:** de ce un sistem stateful are nevoie de StatefulSet (identitate stabila
per nod), cum functioneaza serviciile si storage-ul persistent.

**Resurse:**
- Tutoriale oficiale — `kubernetes.io/docs/tutorials/`; in special cel cu aplicatie
  stateful replicata (e aproape proiectul tau).
- `kind` — `kind.sigs.k8s.io`.
- *Kubernetes Up & Running* (Burns, Beda, Hightower) — cartea standard.

**Gata cand:** `kubectl delete pod raftkv-1` si clusterul isi revine singur.

---

## Faza 5 — Observabilitate (~15-20h)

**Construiesti:** metrici Prometheus (latenta, replication lag, cine e leader) +
dashboard Grafana + logging structurat cu `slog`.

**Inveti:** instrumentare, metrici (counter/gauge/histogram), logging structurat.

**Resurse:**
- Prometheus + `client_golang` — `prometheus.io/docs/`.
- Grafana — `grafana.com/docs/`.
- Go `slog` — `pkg.go.dev/log/slog` + blogul Go despre structured logging.

**Gata cand:** vezi pe un dashboard cine e leaderul si cat lag are fiecare follower.

---

## Faza 6 — Dashboard real-time in Elixir (~15-20h)

**Construiesti:** un Phoenix LiveView care arata live starea clusterului (noduri
up/down, leader, lag). Valorifici Elixirul pe care il stii deja.

**Inveti:** LiveView, PubSub, integrare cu un sistem extern (nodurile Go).

**Resurse:**
- Docs Phoenix LiveView — `hexdocs.pm/phoenix_live_view`.
- *Programming Phoenix LiveView* (Pragmatic Bookshelf), Tate & DeBenedetto.
- Cursul Pragmatic Studio LiveView — daca vrei ghidaj video.

**Gata cand:** deschizi pagina, omori un nod din alt terminal, si vezi live cum pica.

---

## Faza 7 — Storage engine in Rust (~70-90h) — cel mai mare bloc

**Construiesti:** un engine de stocare (LSM-tree: memtable + SSTable + WAL) pe care
il expui ca backend pluggable in locul map-ului din Go.

**Inveti:** Rust si paradigma lui speciala — ownership si borrow checker exact acolo
unde chiar isi merita existenta (memorie, performanta, zero GC). Plus interne de
baze de date.

**Resurse:**
- *The Rust Programming Language* (the Book) — `doc.rust-lang.org/book` — gratis,
  canonic.
- *Rustlings* — `github.com/rust-lang/rustlings` — exercitii hands-on.
- *Too Many Linked Lists* — `rust-unofficial.github.io/too-many-lists/` — te invata
  ownership fix prin structuri de date (perfect inainte de LSM).
- *mini-lsm* de skyzh — `github.com/skyzh/mini-lsm` ("Build an LSM in a Week") —
  construiesti un storage engine LSM in Rust pas cu pas. Match perfect.
- *Database Internals*, Alex Petrov — teoria din spatele LSM/B-tree/WAL.

**Gata cand:** raftkv scrie prin engine-ul Rust in loc de map, cu aceleasi teste verzi.

---

## Faza 8 — Stretch (fiecare = o linie in plus pe CV)

- **Sharding** cu consistent hashing (partitionare orizontala) — MIT 6.5840 are
  lab-ul de sharded KV pentru asta.
- **Helm chart** pentru deploy.
- **CI/CD** complet cu GitHub Actions (build + test + lint + release).
- **Chaos testing** — omori noduri / simulezi partitii de retea automat.
- **Benchmark-uri** + profiling (`pprof`).
- Flexul suprem: **reimplementeaza Raft de la zero**, fara hashicorp/raft.

---

## Concepte de prins pe parcurs (nu inainte)

- Teorema CAP — de ce alegi consistenta in fata disponibilitatii aici.
- Strong vs eventual consistency; linearizability.
- Idempotenta la retry-uri (exactly-once vs at-least-once).
- Cum functioneaza un WAL si snapshot-urile.

Toate sunt acoperite de MIT 6.5840 + DDIA. Le prinzi cand ajungi la ele.

---

## Bugetul de timp (recap)

- Core demn de CV (fazele 0-4): ~130-160h → il pui pe CV cand e gata, nu astepta restul.
- Cu observabilitate + Elixir (0-6): ~180-220h.
- Tot, inclusiv Rust (0-7): ~250-300h.
- Pune +30% buffer mental: debugging-ul la race conditions si K8s inghite timp.
- La ~8-10h/saptamana: core-ul pana in ~decembrie, restul pe parcurs.