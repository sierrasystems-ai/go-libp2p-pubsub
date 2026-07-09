# AGENTS.md

## Cursor Cloud specific instructions

This repository is the **`go-libp2p-pubsub`** library (module `github.com/libp2p/go-libp2p-pubsub`). It is a Go library, not a deployable service — there is no server/daemon to start. "Running" it means building it and exercising it via tests or a small consumer program.

### Toolchain
- Requires **Go ≥ 1.25** (see the `go` directive in `go.mod`). The distro's default `/usr/bin/go` (1.22) is too old and will fail with `toolchain not available` because network toolchain downloads are restricted. Go 1.26.x is installed at `/usr/local/go` and added to `PATH` via `~/.bashrc`. If `go version` is missing from `PATH` in a non-interactive shell, use `/usr/local/go/bin/go`.

### Standard commands (run from repo root)
- Build: `go build ./...`
- Lint: `go vet ./...` and `gofmt -l .` (CI `go-check` also runs staticcheck via the shared ipdxco workflow).
- Test: `go test ./...` (full suite takes ~35s; the main package dominates). CI uses `go test -timeout 30m ./...` and `go test -race ./...`.
- Raise the fd limit before large test runs (`ulimit -n 4096`); tests spin up many in-process libp2p hosts over loopback.

### Notes
- Tests use in-memory / simulated libp2p hosts (`marcopolo/simnet`); **no external services** (DB, broker, network daemons) are required.
- `pb/*.pb.go` are generated (see `pb/Makefile`) and committed; regenerating requires `protoc` and is not needed for normal development.
- To smoke-test the library as a consumer, create a throwaway module with a `replace github.com/libp2p/go-libp2p-pubsub => /workspace` directive and use `pubsub.NewGossipSub` with two `libp2p.New` hosts.
