# kbatch (Go runtime)

Go binaries for the KafkaBatch control plane and backend workers.

## Binaries

| Command | Role |
|---------|------|
| `kbatch daemon` | Control plane — fairness dispatch, events, retry, callbacks, schedule; consumes **ruby** job topics only |
| `kbatch worker` | Go backend — consumes **go** plain, priority, and fair-ready topics; runs handlers in-process |
| `kbatch serve` | **Deprecated** — Phase 2 sidecar for pure Ruby Karafka + `executor :go` only |

## Build

```bash
cd go
go build -o kbatch-daemon ./cmd/kbatch-daemon   # link your handlers via kbatch.Register
go build -o kbatch-worker ./cmd/kbatch-worker-ittest  # or your worker main
```

Integration test binaries:

```bash
go build -o ../bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest
go build -o ../bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest
KAFKA_BATCH_INTEGRATION=1 bundle exec rspec spec/integration/go_*.rb
```

## Three-tier deployment (v1.1+)

1. **Ruby gem** — `daemon_mode: true`, produce only
2. **`kbatch daemon`** — internal topics + ruby execution (unix socket to worker server)
3. **`kbatch worker`** — all `runtime: go` handlers

See the main [README](../README.md#go-stack-deployment-v110) for full deployment docs.

## Legacy sidecar

`kbatch serve` is retained for Karafka-only apps that have not migrated to `daemon_mode`. Do not run it alongside `kbatch daemon` or `kbatch worker`.
