# Distributed File System — Implementation Plan

A step-by-step plan for building a GFS-inspired Distributed File System in Python (`asyncio`)
with Raft-based replication across DataNodes and a never-failing MasterNode.

---

## Architecture Overview

```
┌─────────────┐        namespace ops        ┌──────────────────────────────────────┐
│  DFS Client │ ──────────────────────────► │             MasterNode               │
│  (POSIX API)│ ◄────────────────────────── │  - File/Dir namespace (in-memory +   │
└──────┬──────┘    block location reply     │    persistent WAL/snapshot)          │
       │                                    │  - Block → Raft-group mapping        │
       │  block read/write (via leader)     │  - Raft leader registry              │
       ▼                                    └──────────────────────────────────────┘
┌─────────────────────────────────────────────────────┐
│                   Raft Group (3 DataNodes)           │
│                                                      │
│   ┌───────────┐   ┌───────────┐   ┌───────────┐    │
│   │ DataNode A│   │ DataNode B│   │ DataNode C│    │
│   │ (Leader) ◄├──►│(Follower) │◄─►│(Follower) │    │
│   └───────────┘   └───────────┘   └───────────┘    │
└─────────────────────────────────────────────────────┘
```

**Key design decisions:**
- All namespace metadata lives in the MasterNode.
- Each block is replicated across exactly **3 DataNodes** forming a **Raft group**.
- All reads and writes go through the **Raft leader** of the group → sequential consistency.
- Re-replication (adding new nodes on failure) is **not** supported. Recovery of a previously
  failed node rejoining its Raft group **is** supported (standard Raft log catch-up).
- Everything is `asyncio`-based; no threads.
- RPC is done over raw asyncio TCP with a lightweight length-prefixed JSON (or msgpack) framing.

---

## Repository Layout

```
dfs/
├── CLAUDE.md
├── IMPLEMENTATION_PLAN.md
├── requirements.txt
├── config/
│   └── default.yaml          # ports, block size, heartbeat intervals, etc.
├── common/
│   ├── __init__.py
│   ├── rpc.py                # async TCP RPC client + server base
│   ├── messages.py           # all message dataclasses (shared by all nodes)
│   └── utils.py              # logging helpers, async retry, etc.
├── master/
│   ├── __init__.py
│   ├── server.py             # MasterNode asyncio server entry point
│   ├── namespace.py          # in-memory namespace tree (dirs + files)
│   ├── block_manager.py      # block ID allocation, block→group mapping
│   └── wal.py                # write-ahead log + snapshot for master state
├── datanode/
│   ├── __init__.py
│   ├── server.py             # DataNode asyncio server entry point
│   ├── store.py              # local block storage (files on disk)
│   ├── raft/
│   │   ├── __init__.py
│   │   ├── node.py           # Raft state machine (leader/follower/candidate)
│   │   ├── log.py            # Raft log (in-memory + persistent)
│   │   ├── state.py          # persistent Raft state (currentTerm, votedFor)
│   │   └── rpc_handlers.py   # RequestVote, AppendEntries handlers
│   └── heartbeat.py          # sends periodic heartbeats to MasterNode
├── client/
│   ├── __init__.py
│   ├── dfs_client.py         # public POSIX-like API
│   └── cache.py              # optional: block-location cache
└── tests/
    ├── unit/
    │   ├── test_namespace.py
    │   ├── test_raft_log.py
    │   └── test_raft_election.py
    └── integration/
        ├── test_basic_io.py
        ├── test_leader_failover.py
        └── test_node_recovery.py
```

---

## Phase 1 — Project Scaffold & RPC Layer

**Goal:** Every component can talk to every other component over async TCP.

### Tasks
1. Set up `pyproject.toml` / `requirements.txt` (`pyyaml`, `msgpack`, `aiofiles`, `pytest-asyncio`).
2. Implement `common/rpc.py`:
   - `RpcServer`: wraps `asyncio.start_server`, dispatches incoming length-prefixed messages
     to registered async handler functions.
   - `RpcClient`: maintains a persistent async TCP connection, sends request, awaits response.
   - Wire format: `[4-byte big-endian length][msgpack payload]`.
   - Every message has a `type` field and a `request_id` for matching responses.
3. Implement `common/messages.py` — define all request/response dataclasses used across
   all phases (can be extended later, but define the skeleton now).
4. Write a round-trip unit test: server echoes back the message.

### Exit criterion
`RpcClient` can call a handler on `RpcServer` and receive a typed response, fully async.

---

## Phase 2 — MasterNode: Namespace & Block Management

**Goal:** MasterNode can manage a POSIX-like namespace and hand out block locations to clients.

### Tasks

#### 2a — Namespace (`master/namespace.py`)
- In-memory tree of `DirNode` / `FileNode`.
- Operations:
  - `create(path)` → allocates file inode, returns file handle.
  - `mkdir(path)`
  - `delete(path)` — marks file deleted, returns list of block IDs to reclaim.
  - `stat(path)` → metadata (size, mtime, block list).
  - `list(path)` → directory entries.
- All operations protected by `asyncio.Lock`.

#### 2b — Block Manager (`master/block_manager.py`)
- Assigns a monotonically increasing `block_id` on each `allocate_block(file_handle)` call.
- Maintains `block_id → [datanode_id × 3]` mapping (which Raft group owns this block).
- Maintains `block_id → current_leader_datanode_id` (updated via heartbeat from DataNodes).
- API:
  - `allocate_block(file_id) → (block_id, [dn_addr × 3], leader_addr)`
  - `get_block_location(block_id) → (leader_addr, [replica_addrs])`
  - `update_leader(block_id, new_leader_id)`

#### 2c — Persistence (`master/wal.py`)
- Append-only WAL: every namespace mutation is logged before being applied.
- Periodic snapshot of full namespace state to disk.
- On startup: load latest snapshot, replay WAL tail.

#### 2d — MasterNode RPC Handlers (`master/server.py`)
Expose these RPC endpoints:
| Method | Caller | Description |
|---|---|---|
| `CREATE` | Client | Create file, return first block location |
| `OPEN` | Client | Resolve path → block list + leader addresses |
| `DELETE` | Client | Remove file, return block IDs |
| `MKDIR` / `LIST` / `STAT` | Client | Namespace ops |
| `ALLOCATE_BLOCK` | Client (on append) | Get next block location for a file |
| `REPORT_LEADER` | DataNode | DataNode reports itself as new Raft leader |
| `REGISTER_DATANODE` | DataNode | DataNode announces itself on startup |

### Exit criterion
`master/server.py` can be started; a simple script can create/open/delete files and list
directories by calling it over RPC.

---

## Phase 3 — DataNode: Block Storage (without Raft)

**Goal:** DataNodes can store and serve raw blocks. No replication yet.

### Tasks

#### 3a — Local Block Store (`datanode/store.py`)
- Each block stored as a flat file: `{data_dir}/{block_id}.blk`.
- API:
  - `async write_block(block_id, data: bytes)`
  - `async read_block(block_id) → bytes`
  - `async delete_block(block_id)`
  - `async list_blocks() → [block_id]`
- Use `aiofiles` for non-blocking disk I/O.

#### 3b — DataNode RPC Handlers (`datanode/server.py`)
Expose:
| Method | Caller | Description |
|---|---|---|
| `WRITE_BLOCK` | Client | Write block data |
| `READ_BLOCK` | Client | Read block data |
| `DELETE_BLOCK` | Master | Reclaim block after file deletion |
| `GET_BLOCKS` | Master | List blocks stored (for reconciliation) |

#### 3c — Registration & Heartbeat (`datanode/heartbeat.py`)
- On startup: send `REGISTER_DATANODE` to MasterNode with own address + list of stored blocks.
- Every `heartbeat_interval` seconds: send heartbeat to MasterNode (alive signal).
- MasterNode marks a DataNode as unavailable after `missed_heartbeats` threshold.

### Exit criterion
Client can write a block to a single DataNode and read it back, with the MasterNode
knowing which DataNode has which block.

---

## Phase 4 — Client: POSIX-like API

**Goal:** A usable `DfsClient` that ties MasterNode + DataNode together.

### Tasks

#### 4a — `client/dfs_client.py`
Public API (all `async`):
```python
await client.create(path: str) -> FileHandle
await client.open(path: str, mode: str) -> FileHandle   # mode: 'r' | 'w' | 'a'
await client.write(fh: FileHandle, data: bytes)
await client.read(fh: FileHandle, offset: int, length: int) -> bytes
await client.delete(path: str)
await client.mkdir(path: str)
await client.list(path: str) -> list[str]
await client.stat(path: str) -> FileStat
await client.close(fh: FileHandle)
```

#### 4b — Block-level logic inside client
- `write`: splits data into fixed-size blocks (`block_size` from config).
  For each block: calls `ALLOCATE_BLOCK` on Master → gets `(block_id, leader_addr)` →
  calls `WRITE_BLOCK` on leader DataNode.
- `read`: calls `OPEN` on Master → gets block list + leader addresses → calls `READ_BLOCK`
  on leader DataNode for each needed block → reassembles bytes.
- `append`: like write but extends the last block if space remains, then allocates new blocks.

#### 4c — Block location cache (`client/cache.py`)
- TTL-based cache of `block_id → leader_addr`.
- On stale-leader error from DataNode, invalidate cache and re-query Master.

### Exit criterion
Full round-trip: `create → write → close → open → read` works end-to-end on a single
DataNode (replication comes next).

---

## Phase 5 — Raft Consensus Implementation

**Goal:** A correct, standalone Raft implementation that DataNodes will use.
This is the most complex phase; test it independently before integrating.

### Tasks

#### 5a — Raft State (`datanode/raft/state.py`)
Persistent state (must survive restarts):
- `current_term: int`
- `voted_for: Optional[str]` (node ID)
- Stored in a small JSON file, flushed to disk (via `aiofiles`) before responding to any RPC.

#### 5b — Raft Log (`datanode/raft/log.py`)
- `LogEntry(term, index, command)` where `command` is a block-write payload or no-op.
- Append-only log file on disk.
- In-memory index for fast access.
- API: `append`, `get(index)`, `slice(from, to)`, `truncate_from(index)`, `last_index`, `last_term`.

#### 5c — Raft Node (`datanode/raft/node.py`)
States: `Follower | Candidate | Leader`

Key timers (all implemented as `asyncio` tasks):
- **Election timeout** (randomised 150–300 ms): fires when no heartbeat received → become Candidate.
- **Heartbeat timer** (fixed, e.g. 50 ms): fires when Leader → send `AppendEntries` (possibly empty).

Leader behaviour:
- Maintain `next_index[]` and `match_index[]` per follower.
- On client write: append to local log → replicate to followers → commit when majority ACK →
  apply to state machine → respond to client.
- All reads also go through leader (read index / lease approach for linearisability).

Follower behaviour:
- Reset election timer on valid `AppendEntries`.
- Truncate log on conflict.

Candidate behaviour:
- Increment term, vote for self, send `RequestVote` to peers.
- Become Leader on majority votes; revert to Follower on higher term.

#### 5d — Raft RPC Handlers (`datanode/raft/rpc_handlers.py`)
| RPC | Description |
|---|---|
| `REQUEST_VOTE` | Standard Raft RequestVote |
| `APPEND_ENTRIES` | Standard Raft AppendEntries (also used as heartbeat) |
| `CLIENT_WRITE` | Client (or DataNode server) submits a log entry |
| `CLIENT_READ` | Client requests a committed read |

#### 5e — Raft Unit Tests
- Single-node leader election.
- Three-node election: one leader elected.
- Log replication: leader replicates entries to two followers.
- Leader crash → follower wins new election.
- Partitioned node rejoins and catches up via log replication.
- Term safety: old leader with stale term is rejected.

### Exit criterion
All Raft unit tests pass with a simulated network (no real sockets needed — inject an
`AbstractTransport` so tests run without `asyncio.start_server`).

---

## Phase 6 — Integrate Raft into DataNodes

**Goal:** Every block group runs Raft; all writes/reads go through the leader.

### Tasks

#### 6a — DataNode as a Raft peer
- On startup, each DataNode instantiates a `RaftNode` with its group membership
  (3 peer addresses provided via config or assigned by MasterNode at registration).
- The `RaftNode`'s state machine `apply(command)` calls `store.write_block(...)`.
- The DataNode's `WRITE_BLOCK` RPC handler submits the command to `RaftNode.client_write()`.
- The DataNode's `READ_BLOCK` RPC handler calls `RaftNode.client_read()` then `store.read_block()`.

#### 6b — Leader reporting
- Whenever a DataNode's `RaftNode` transitions to Leader, call `REPORT_LEADER` on MasterNode
  with `(block_id, self_address)`.
- MasterNode updates its leader registry.

#### 6c — Redirect non-leaders
- If a client contacts a Follower DataNode for a write/read, the DataNode responds with
  `NOT_LEADER` + `current_leader_addr`.
- Client follows the redirect (at most once; then re-queries Master).

#### 6d — Block group assignment
- MasterNode selects 3 DataNodes for each new block (round-robin or least-loaded).
- MasterNode sends a `FORM_GROUP` RPC to all 3 DataNodes with the full peer list.
- DataNodes initialise a `RaftNode` for that group.

### Exit criterion
Write a block through the client → goes to leader → replicated to 2 followers → committed →
client gets success. Read block back → leader serves it.

---

## Phase 7 — Failure Detection & Recovery

**Goal:** DataNode failures are detected; recovered nodes rejoin their Raft groups correctly.

### Tasks

#### 7a — Failure detection (MasterNode side)
- MasterNode marks a DataNode `UNAVAILABLE` after N missed heartbeats.
- Marks all blocks whose leader was that DataNode as `LEADER_UNKNOWN`.
- Does **not** attempt re-replication (out of scope).

#### 7b — Client-side resilience
- On timeout or connection refused from a DataNode:
  - Re-query MasterNode for current leader.
  - Retry up to `max_retries` times with exponential backoff.

#### 7c — Node recovery (Raft log catch-up)
- When a previously failed DataNode restarts:
  1. Reads persistent `current_term` and `voted_for` from disk.
  2. Reads its Raft log from disk.
  3. Re-registers with MasterNode (`REGISTER_DATANODE`).
  4. For each Raft group it belongs to, re-instantiates `RaftNode` in `Follower` state.
  5. The current leader will detect it via heartbeat and send missing `AppendEntries`
     to bring its log up to date (standard Raft catch-up — no special code needed).
  6. Once caught up (`match_index` reaches `commit_index`), it's a healthy replica again.

#### 7d — Stale reads prevention
- Leader uses a **ReadIndex** approach: before serving a read, confirm leadership with
  a round of heartbeats (or use a lease if latency is critical).

### Exit criterion
- Kill one DataNode → client can still read/write (2 of 3 nodes = majority).
- Restart the killed DataNode → it catches up automatically, verified by reading its
  local log and block store.
- Kill 2 DataNodes → client receives a clear `UNAVAILABLE` error (no majority).

---

## Phase 8 — Integration Testing & Hardening

### Tasks
1. **`tests/integration/test_basic_io.py`**: create, write large file (multi-block), read back,
   delete. Assert byte-perfect round-trip.
2. **`tests/integration/test_leader_failover.py`**: write, kill leader mid-write,
   assert either success or clean error + client can retry.
3. **`tests/integration/test_node_recovery.py`**: write, kill one follower, write more,
   restart follower, assert follower's store matches leader's.
4. **Chaos test**: random kill/restart of DataNodes while client does continuous writes.
   Assert no data loss for committed writes.
5. Performance baseline: measure throughput and latency for 1 MB sequential write,
   record in `docs/benchmarks.md`.

---

## Configuration Reference (`config/default.yaml`)

```yaml
master:
  host: "127.0.0.1"
  port: 9000

datanode:
  data_dir: "./data"
  heartbeat_interval_s: 1.0
  missed_heartbeats_before_unavailable: 3

block:
  size_bytes: 65536   # 64 KB

raft:
  election_timeout_ms: [150, 300]   # random range
  heartbeat_interval_ms: 50
  rpc_timeout_ms: 100
  max_retries: 3
```

---

## Recommended Build Order Summary

| Phase | Deliverable | Tests |
|-------|-------------|-------|
| 1 | RPC layer | Unit: round-trip |
| 2 | MasterNode namespace + block manager | Unit: namespace ops |
| 3 | DataNode block store + heartbeat | Unit: store, integration: register |
| 4 | DFS Client (single node, no replication) | Integration: basic I/O |
| 5 | Raft (standalone) | Unit: election, replication, recovery |
| 6 | Raft integrated into DataNodes | Integration: replicated write/read |
| 7 | Failure detection + node recovery | Integration: failover, catch-up |
| 8 | Hardening & chaos tests | Full integration suite |

---

## Key Invariants to Preserve Throughout

1. **No write is acknowledged to the client until it is committed in Raft** (majority replicated + fsync'd).
2. **All reads go through the Raft leader** — no stale reads from followers.
3. **MasterNode namespace mutations are WAL'd before responding** — namespace is durable.
4. **Raft persistent state (`current_term`, `voted_for`, log) is flushed to disk before
   sending any RPC response** — election safety.
5. **Block ID is globally unique and monotonically increasing** — assigned only by MasterNode.