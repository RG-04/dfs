# Distributed File System

A GFS-inspired distributed filesystem built with Python asyncio and gRPC, featuring a hierarchical namespace, per-block Raft replication, and automatic failure detection.

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                           DFS Client                              │
│   mkdir / rmdir / stat / ls / create / delete /                   │
│          read(path, offset, length) / write(path, offset, data)   │
│                                                                   │
│  File offsets decomposed into block operations:                   │
│    block_index        = file_offset // block_size                 │
│    intra_block_offset = file_offset %  block_size                 │
└──────────┬────────────────────────────────────┬───────────────────┘
           │ namespace + block routing           │ ReadBlock / WriteBlock
           ▼                                     ▼
┌──────────────────────┐           ┌─────────────────────────────────┐
│      MasterNode      │           │  DataNode 0  (dn0)              │
│                      │           │  DataNode 1  (dn1)              │
│  • Directory tree    │──Raft────▶│  DataNode 2  (dn2)              │
│  • Block→DN routing  │  group    │                                 │
│  • Leader tracking   │           │  Each block has its own Raft    │
│  • Heartbeat watchdog│           │  group (independent per-block   │
│  (state persisted)   │           │  consensus + leader election).  │
└──────────────────────┘           └─────────────────────────────────┘
```

**MasterNode** — single source of truth for the file/directory namespace and block routing. Tracks the current Raft leader per block; routes all reads and writes to the leader. Persists namespace and block metadata atomically.

**DataNode** — stores block data as flat files. Each block participates in its own Raft group: the leader serialises writes, replicates them to followers via AppendEntries, and commits only after a quorum acknowledges. Raft state (`current_term`, `voted_for`, log) is persisted to disk before responding to any RPC.

**DFSClient** — pure library (no daemon, no cache). Decomposes every read/write into per-block gRPC calls.

---

## Namespace model

Directories must be created explicitly before files can be placed inside them. The root `/` always exists.

```
/                    ← root, always present
├── data/            ← created with mkdir("/data")
│   ├── records.bin  ← created with create("/data/records.bin")
│   └── logs/        ← created with mkdir("/data/logs")
│       └── app.bin
└── tmp/
```

- `create("/file.bin")` — valid (parent is `/`)
- `create("/data/file.bin")` — requires `/data` to exist first
- `rmdir` fails if the directory still contains files or subdirectories

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

`config.yaml` controls block size, replication, timeouts, and node addresses:

```yaml
block_size: 1048576        # 1 MiB per block
replication_factor: 3      # blocks replicated across this many DataNodes
heartbeat_timeout: 15      # seconds before a silent leader's block is marked unavailable

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

**Terminals 2–4 — DataNodes:**
```bash
python bin/datanode.py dn0
python bin/datanode.py dn1
python bin/datanode.py dn2
```

The MasterNode waits until all configured DataNodes have registered before serving client requests. DataNodes re-register on restart, so the MasterNode can be restarted independently.

---

## Running the tests

The test suite manages the full cluster lifecycle automatically:

```bash
.venv/bin/pytest test_dfs.py -v
```

Expected output (23 tests + 14 new namespace tests = 37 total):

```
test_dfs.py::TestNamespace::test_mkdir_and_rmdir                            PASSED
test_dfs.py::TestNamespace::test_duplicate_mkdir_raises                     PASSED
test_dfs.py::TestNamespace::test_mkdir_missing_parent_raises                PASSED
test_dfs.py::TestNamespace::test_rmdir_non_empty_raises                     PASSED
test_dfs.py::TestNamespace::test_rmdir_missing_raises                       PASSED
test_dfs.py::TestNamespace::test_create_file_without_parent_raises          PASSED
test_dfs.py::TestNamespace::test_stat_directory                             PASSED
test_dfs.py::TestNamespace::test_stat_file                                  PASSED
test_dfs.py::TestNamespace::test_stat_file_with_blocks                      PASSED
test_dfs.py::TestNamespace::test_stat_missing_raises                        PASSED
test_dfs.py::TestNamespace::test_ls_root                                    PASSED
test_dfs.py::TestNamespace::test_ls_shows_files_and_dirs                    PASSED
test_dfs.py::TestNamespace::test_ls_missing_raises                          PASSED
test_dfs.py::TestNamespace::test_stat_root                                  PASSED
test_dfs.py::TestCreate::test_create_new_file                               PASSED
test_dfs.py::TestCreate::test_duplicate_create_raises                       PASSED
test_dfs.py::TestMultiBlockIO::test_write_and_read_two_full_blocks          PASSED
test_dfs.py::TestMultiBlockIO::test_cross_block_read                        PASSED
test_dfs.py::TestMultiBlockIO::test_write_spanning_block_boundary           PASSED
test_dfs.py::TestSmallIO::test_small_write_and_read_within_block            PASSED
test_dfs.py::TestSmallIO::test_surrounding_bytes_unchanged_after_small_write PASSED
test_dfs.py::TestSmallIO::test_single_byte_write_and_read                   PASSED
test_dfs.py::TestBoundaryErrors::test_read_past_last_block_raises           PASSED
test_dfs.py::TestBoundaryErrors::test_skip_block_index_raises               PASSED
test_dfs.py::TestDelete::test_read_after_delete_raises                      PASSED
test_dfs.py::TestDelete::test_double_delete_raises                          PASSED
test_dfs.py::TestRaftReplication::test_data_readable_after_write            PASSED
test_dfs.py::TestRaftReplication::test_replicated_to_followers_on_disk      PASSED
test_dfs.py::TestRaftReplication::test_multiple_sequential_writes           PASSED
test_dfs.py::TestFailureDetection::test_one_follower_failure_write_succeeds PASSED
test_dfs.py::TestFailureDetection::test_leader_failure_triggers_election    PASSED
test_dfs.py::TestFailureDetection::test_master_marks_cluster_unavailable_after_timeout PASSED
test_dfs.py::TestFailureDetection::test_new_block_denied_when_insufficient_nodes PASSED
test_dfs.py::TestRaftRecovery::test_raft_state_files_persisted_on_disk      PASSED
test_dfs.py::TestRaftRecovery::test_follower_restart_and_catchup            PASSED
test_dfs.py::TestRaftRecovery::test_leader_restart_and_rejoin               PASSED
test_dfs.py::TestRaftRecovery::test_full_cluster_restart_data_survives      PASSED
```

### Test coverage

| Class | What is verified |
|---|---|
| `TestNamespace` | mkdir/rmdir lifecycle; parent-enforcement on create; stat on files and dirs; ls listing; error cases (missing parent, non-empty rmdir, double mkdir) |
| `TestCreate` | Fresh create succeeds; duplicate create raises |
| `TestMultiBlockIO` | 2-block write + full read-back; cross-block read; write straddling a boundary |
| `TestSmallIO` | 7-byte mid-block overwrite; surrounding bytes unchanged; single-byte write |
| `TestBoundaryErrors` | Read past last block; skipping a block index |
| `TestDelete` | Read after delete raises; double delete raises |
| `TestRaftReplication` | Data readable after write; replicated to all DataNode disks; sequential writes linearised |
| `TestFailureDetection` | Follower failure — writes still succeed; leader failure — new election elected; master marks block unavailable; new block denied with insufficient nodes |
| `TestRaftRecovery` | Raft state files on disk; follower restart + catchup; leader restart + rejoin; full cluster restart with data survival |

---

## Client API

```python
import asyncio, yaml
from dfs.client.client import DFSClient, DFSError

async def main():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    client = DFSClient(config)

    # ── Namespace ──────────────────────────────────────────────────────────
    await client.mkdir("/data")               # create directory
    await client.create("/data/hello.bin")    # create file (parent must exist)

    info = await client.stat("/data/hello.bin")
    print(info.type, info.name, info.num_blocks)  # "file" "hello.bin" 0

    entries = await client.ls("/data")        # list directory
    for e in entries:
        print(e.type, e.name)

    # ── Data I/O ───────────────────────────────────────────────────────────
    await client.write("/data/hello.bin", offset=0, data=b"Hello, DFS!")
    data = await client.read("/data/hello.bin", offset=0, length=11)
    print(data)  # b'Hello, DFS!'

    # ── Cleanup ────────────────────────────────────────────────────────────
    await client.delete("/data/hello.bin")    # delete file
    await client.rmdir("/data")               # remove empty directory

asyncio.run(main())
```

`write` and `read` accept any byte offset and length; they split operations across block boundaries automatically. Writing past the last allocated block causes the MasterNode to allocate the next block. Blocks must be allocated sequentially from 0.

---

## Persistence and restarts

All state survives process restarts:

| Component | Persisted state |
|---|---|
| MasterNode | `metadata.json` — full file/directory namespace + block group membership + current leader per block. Written atomically via rename. |
| DataNode | `manifest.json` — owned block IDs. `raft_<block_id>.json` — Raft term, `voted_for`, `last_applied`. `raft_<block_id>.log` — binary Raft log (`[8B term][8B intra_offset][4B data_len][data]` per entry). All written with `fsync` before responding to any RPC. |

On DataNode restart the node loads its Raft state and autonomously rejoins each Raft group as a follower — no MasterNode intervention required. The live leader catches up any missing entries via AppendEntries.

---

## Raft implementation

Each block has its own independent Raft group composed of `replication_factor` DataNodes. A DataNode participates in many Raft groups simultaneously.

**Key properties:**
- Sequential consistency — all reads and writes routed to the current Raft leader.
- Failure tolerance of ⌊(replication_factor − 1) / 2⌋ nodes per block (1 node with RF=3).
- Leader election timeout: randomised 150–300 ms.
- Leader heartbeat to MasterNode every 3 s (watchdog timeout 15 s by default).
- Persistent state written atomically before every RPC response (Raft §5.7).
- No snapshotting — logs are kept in full.
