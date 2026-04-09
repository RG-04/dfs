# Distributed File System — Phase 1

A GFS-inspired distributed file system built with Python asyncio and gRPC.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         DFS Client                          │
│   create / delete / read(path, offset, length) /            │
│              write(path, offset, data)                      │
│                                                             │
│  Decomposes file-level offsets into block operations:       │
│    block_index        = file_offset // block_size           │
│    intra_block_offset = file_offset %  block_size           │
└────────┬──────────────────────────────────┬────────────────┘
         │ GetBlockInfo / CreateFile /       │ ReadBlock /
         │ DeleteFile                        │ WriteBlock
         ▼                                   ▼
┌─────────────────┐              ┌──────────────────────────┐
│   MasterNode    │─RegisterBlock▶  DataNode 0 (dn0)        │
│                 │─RegisterBlock▶  DataNode 1 (dn1)        │
│  Namespace +    │─RegisterBlock▶  DataNode 2 (dn2)        │
│  block→DN map   │                                         │
│  (persisted)    │  Blocks stored as raw files on disk.    │
└─────────────────┘  Partial reads/writes at any offset.   │
                     Manifest tracks owned block IDs.       │
                     └──────────────────────────────────────┘
```

**MasterNode** — single source of truth for the file namespace and block routing.  
**DataNode** — stores block data as flat files; rejects writes for blocks it does not own.  
**DFSClient** — pure library (no daemon); zero caching; decomposes every read/write into per-block gRPC calls.

---

## Setup

```bash
# 1. Create virtual environment and install dependencies
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. Generate Python gRPC stubs from .proto files
bash scripts/gen_proto.sh
```

---

## Configuration

Edit `config.yaml` to adjust block size, ports, and storage paths:

```yaml
block_size: 1048576   # 1 MiB per block

master:
  host: "127.0.0.1"
  port: 50051
  metadata_dir: "/tmp/dfs/master"

datanodes:
  - id: dn0
    host: "127.0.0.1"
    port: 50052
    data_dir: "/tmp/dfs/dn0"
  - id: dn1
    host: "127.0.0.1"
    port: 50053
    data_dir: "/tmp/dfs/dn1"
  - id: dn2
    host: "127.0.0.1"
    port: 50054
    data_dir: "/tmp/dfs/dn2"
```

`block_size` can be increased to 64–128 MiB for production; keep it under the 128 MiB gRPC message limit configured in the code.

---

## Running the cluster

Open four terminals from the project root and activate the venv in each:

```bash
source .venv/bin/activate
```

**Terminal 1 — MasterNode:**
```bash
python bin/master.py
```

**Terminal 2 — DataNode 0:**
```bash
python bin/datanode.py dn0
```

**Terminal 3 — DataNode 1:**
```bash
python bin/datanode.py dn1
```

**Terminal 4 — DataNode 2:**
```bash
python bin/datanode.py dn2
```

The MasterNode waits until **all** configured DataNodes register before serving client requests. DataNodes send a registration heartbeat every 10 seconds, so the MasterNode can be restarted independently and will become ready again automatically.

---

## Running the tests

The test suite manages the cluster lifecycle automatically — no need to start nodes manually:

```bash
.venv/bin/pytest test_dfs.py -v
```

Expected output:

```
test_dfs.py::TestCreate::test_create_new_file                      PASSED
test_dfs.py::TestCreate::test_duplicate_create_raises              PASSED
test_dfs.py::TestMultiBlockIO::test_write_and_read_two_full_blocks PASSED
test_dfs.py::TestMultiBlockIO::test_cross_block_read               PASSED
test_dfs.py::TestMultiBlockIO::test_write_spanning_block_boundary  PASSED
test_dfs.py::TestSmallIO::test_small_write_and_read_within_block   PASSED
test_dfs.py::TestSmallIO::test_surrounding_bytes_unchanged_after_small_write PASSED
test_dfs.py::TestSmallIO::test_single_byte_write_and_read          PASSED
test_dfs.py::TestBoundaryErrors::test_read_past_last_block_raises  PASSED
test_dfs.py::TestBoundaryErrors::test_skip_block_index_raises      PASSED
test_dfs.py::TestDelete::test_read_after_delete_raises             PASSED
test_dfs.py::TestDelete::test_double_delete_raises                 PASSED
```

### What the tests cover

| Class | What is verified |
|---|---|
| `TestCreate` | Fresh create succeeds; duplicate create raises `DFSError` |
| `TestMultiBlockIO` | 2-block write + full read-back; cross-block read; write straddling a boundary |
| `TestSmallIO` | 7-byte mid-block overwrite; surrounding bytes unchanged; single-byte write |
| `TestBoundaryErrors` | Read past last block raises; skipping a block index raises |
| `TestDelete` | Read after delete raises; double delete raises |

---

## Using the client library

```python
import asyncio, yaml
from dfs.client.client import DFSClient, DFSError

async def main():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    client = DFSClient(config)

    # Create a file
    await client.create("/data/hello.bin")

    # Write — offset is a raw file-level byte position
    await client.write("/data/hello.bin", offset=0, data=b"Hello, DFS!")

    # Read back (any offset + length, spans block boundaries automatically)
    data = await client.read("/data/hello.bin", offset=0, length=11)
    print(data)  # b'Hello, DFS!'

    # Delete
    await client.delete("/data/hello.bin")

asyncio.run(main())
```

Writes can target any byte offset in the file. If the write extends past the last allocated block, the MasterNode allocates the next block automatically. Blocks must be allocated sequentially from 0 — gaps are rejected.

---

## Persistence and restarts

All state survives process restarts:

- **MasterNode** writes `metadata.json` atomically (via rename) to `master.metadata_dir`. On restart it reloads the full namespace.
- **DataNode** writes a `manifest.json` (owned block IDs) atomically to `data_dir`. Block data files live alongside it. On restart the node reloads its manifest and is ready to serve immediately.

After restarting a DataNode, it re-registers with the MasterNode on the next heartbeat (within 10 seconds by default).

---

## Phase 2 — Replication roadmap

Phase 2 will replace each single DataNode with a **3-node Raft group**, providing fault-tolerant block storage. The MasterNode and DFSClient require no changes.

### Planned changes

| Component | Change |
|---|---|
| `DataNode` service | Implement the same `datanode.proto` interface on the Raft **leader**. `WriteBlock` becomes a linearised Raft log entry replicated to followers before ACKing the client. |
| `RegisterBlock` | Raft leader registers block and replicates the fact to followers. |
| `DeleteBlock` | Leader commits deletion through the log. |
| `ReadBlock` | Leader serves reads (or followers after a lease check for linearisability). |
| MasterNode | Unchanged — it still assigns a group ID (formerly DataNode ID) to each block. |
| DFSClient | Unchanged — it still contacts the address returned by `GetBlockInfo`. |
| Config | Each `datanodes` entry becomes a Raft group entry with 3 member addresses. |

### Key design invariants preserved

- The `DataNode` gRPC service is the **only interface boundary** between the storage layer and the rest of the system.
- Block ownership is enforced at the DataNode level — the leader rejects requests for blocks it has not committed to its log.
- The client's offset-decomposition logic (`block_index = file_offset // block_size`) is independent of the replication strategy.
