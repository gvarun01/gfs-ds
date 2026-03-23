GFS Distributed Storage System
==============================

A from-scratch implementation of the Google File System (GFS) with exactly-once record append semantics, 64MB chunks, leases, replication, and operational tooling suitable for demos and local experimentation.

Quick Start
-----------
- Requirements: Go 1.23+ (tested), protoc at `/home/gvarun01/protoc/bin/protoc` or export `PROTOC_BIN`, Go protobuf plugins on PATH (or set `PROTOC_PLUGIN_PATH`).
- Build: `make build`
- Run cluster locally (1 master, 3 chunkservers, client tools):
  - Start: `./scripts/gfs-cluster.sh start`
  - Status: `./scripts/gfs-cluster.sh status`
  - Tests (system + edge): `./scripts/gfs-cluster.sh test`
  - Stop: `./scripts/gfs-cluster.sh stop`
  - Clean data/logs: `./scripts/gfs-cluster.sh clean`
- Binaries land in `bin/`: `master`, `chunkserver`, `client` (CLI).

Configuration
-------------
- Defaults live in `configs/`:
  - Master: `configs/general-config.yml`
  - Chunkserver: `configs/chunkserver-config.yml`
  - Client: `configs/client-config.yml`
- Development samples: `configs/development/*.yaml` (not auto-used).
- Each binary accepts `-config` (master/chunkserver) or `--config` (client) or env overrides:
  - `GFS_MASTER_CONFIG`, `GFS_CHUNKSERVER_CONFIG`, `GFS_CLIENT_CONFIG`.
- Paths are relative to repo root (e.g., storage under `storage/`).

Key Features
------------
- 64MB chunks, replication factor 3 by default.
- Leases with renewal, heartbeat-based chunkserver liveness.
- Exactly-once record append and configurable chunk size on client.
- Protobuf/gRPC APIs for master-client and chunkserver interactions.
- Deterministic Makefile: `make build`, `make proto`, `make test`, `make clean`.
- Cluster helper script for lifecycle + tests: `scripts/gfs-cluster.sh`.

Repository Layout
-----------------
- `api/proto/` – protobuf definitions for master/chunkserver/client.
- `cmd/` – entrypoints (`master`, `chunkserver`, `client`).
- `configs/` – config files; `development/` samples.
- `internal/` – master, chunkserver, client implementations.
- `pkg/` – shared utilities (config resolver, constants, errors, config loader).
- `scripts/` – cluster management helper.
- `test_gfs_system.go`, `test_scripts/` – system and edge-case tests.
- `Makefile` – build/test/proto targets.

Typical Flows
-------------
1) Build + start cluster
   - `make build`
   - `./scripts/gfs-cluster.sh start`
2) Run tests
   - `./scripts/gfs-cluster.sh test`
3) Stop/clean
   - `./scripts/gfs-cluster.sh stop`
   - `./scripts/gfs-cluster.sh clean`

Client CLI Usage
----------------
- Launch: `./bin/client` (uses config resolver; pass `--config` if custom).
- Commands: `create`, `rename`, `delete`, `read`, `write`, `append`, `writefile`, `chunks`, `push`, `ls`, `help`, `exit`.

Notes
-----
- Storage and logs live under `storage/` and `logs/` when using provided configs/scripts.
- Protos must be regenerated if API definitions change: `make proto` (uses `PROTOC_BIN` and `PROTOC_PLUGIN_PATH`).
- Integration tests expect the cluster running when invoked via `scripts/gfs-cluster.sh test`.
