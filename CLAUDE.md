# CLAUDE.md

This file gives Claude the context needed to work effectively in this repository.

---

## Project Overview

A **Distributed File System (DFS)** written in Python, inspired by GFS, with the following
key properties:

- **MasterNode** manages all namespace metadata (files, directories, block mappings).
  It is assumed to never fail.
- **DataNodes** store block data. They can fail. There are always **3 replicas per block**.
- **Raft consensus** runs within each block's 3-node DataNode group to ensure
  **sequential consistency**. All reads and writes go through the Raft leader.
- **No re-replication** on failure. A failed DataNode may rejoin its Raft group and
  catch up via standard Raft log replay.
- Everything is built on **Python `asyncio`** — no threads, no sync I/O.

See `IMPLEMENTATION_PLAN.md` for the full phased build plan.

---

## Tech Stack

| Concern | Choice |
|---|---|
| Language | Python 3.11+ |
| Async runtime | `asyncio` (standard library) |
| Disk I/O | `aiofiles` |
| Serialisation | `msgpack` (RPC wire format) |
| Config | `pyyaml` |
| Testing | `pytest` + `pytest-asyncio` |

---

## Repository Layout

```
dfs/
├── CLAUDE.md                  ← you are here
├── IMPLEMENTATION_PLAN.md     ← phased build plan
├── config/default.yaml        ← all tuneable parameters
├── common/                    ← shared RPC, messages, utils
├── master/                    ← MasterNode server + namespace + WAL
├── datanode/                  ← DataNode server + block store + Raft
│   └── raft/                  ← standalone Raft implementation
├── client/                    ← DFS client (POSIX-like API)
└── tests/
    ├── unit/
    └── integration/
```

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Start MasterNode
python -m master.server --config config/default.yaml

# Start DataNodes (run 3 instances, each with a unique port + data_dir)
python -m datanode.server --config config/default.yaml --port 9001 --data-dir ./data/dn1
python -m datanode.server --config config/default.yaml --port 9002 --data-dir ./data/dn2
python -m datanode.server --config config/default.yaml --port 9003 --data-dir ./data/dn3

# Run tests
pytest tests/unit/
pytest tests/integration/
```

---

## Architecture Decisions & Rationale

### Why Raft instead of GFS-style replication?
GFS uses a primary-backup scheme (one primary per chunk, mutation order defined by primary).
This gives weaker consistency — concurrent writes can leave replicas in different states.
By using Raft per block group, we get **sequential consistency**: every committed write is
linearisable across all replicas.

### Why is MasterNode assumed never to fail?
Simplicity. Adding Paxos/Raft to the master is a separate and substantial problem.
The master's namespace is durably written via a WAL + snapshot so it can be restarted
without data loss if needed, but leader election for the master is out of scope.

### Why all reads through the leader?
Follower reads can serve stale data. Leader reads use a **ReadIndex** check (leader confirms
it is still the current leader by getting a heartbeat majority before serving a read), which
preserves sequential consistency without extra round trips.

### Why no re-replication?
Out of scope for this project. The system degrades gracefully: a block group with one dead
node still has a majority (2 of 3) and remains available. A group with two dead nodes loses
majority and becomes unavailable for writes until nodes recover.

### RPC wire format
All messages are `msgpack`-encoded and framed with a 4-byte big-endian length prefix.
Every request carries a `request_id` (UUID4 string) so the client can match async responses.
Every response carries the same `request_id` plus a `status` field (`ok` / `error`).

---

## Key Interfaces

### DfsClient (public API)
```python
# All methods are async
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

### MasterNode RPC endpoints
| Method | Caller | Purpose |
|---|---|---|
| `CREATE` | Client | Create file |
| `OPEN` | Client | Resolve path → block list + leader addresses |
| `DELETE` | Client | Remove file |
| `MKDIR` / `LIST` / `STAT` | Client | Namespace operations |
| `ALLOCATE_BLOCK` | Client | Get next block location for append |
| `REPORT_LEADER` | DataNode | DataNode reports itself as Raft leader |
| `REGISTER_DATANODE` | DataNode | DataNode announces itself on startup |

### DataNode RPC endpoints
| Method | Caller | Purpose |
|---|---|---|
| `WRITE_BLOCK` | Client | Submit block write (forwarded to Raft) |
| `READ_BLOCK` | Client | Read committed block data |
| `DELETE_BLOCK` | Master | Reclaim block storage |
| `REQUEST_VOTE` | Raft peer | Raft leader election |
| `APPEND_ENTRIES` | Raft peer | Raft log replication + heartbeat |

---

## Invariants — Never Break These

1. **A write is only acknowledged to the client after it is committed in Raft**
   (majority-replicated and fsync'd to the leader's log).
2. **All client reads go through the Raft leader** — never serve from a follower.
3. **Raft persistent state (`current_term`, `voted_for`, log entries) is flushed to disk
   before sending any RPC response** — this is the foundation of election safety.
4. **MasterNode namespace mutations are WAL'd before the response is sent** — namespace
   is always durable.
5. **Block IDs are globally unique and monotonically increasing** — allocated only by the
   MasterNode, never by DataNodes or clients.

---

## Testing Philosophy

- **Unit tests** (`tests/unit/`): test each component in isolation. Raft in particular
  should be tested with a simulated/injected transport so tests are fast and deterministic.
- **Integration tests** (`tests/integration/`): spin up real asyncio servers in-process
  using random ports. Test full client → master → datanode flows.
- **Chaos tests**: randomly kill/restart DataNodes during concurrent writes. Assert no
  data loss for writes that received a success response.

Every Raft behaviour listed in the Raft paper (§5) should have at least one unit test.

---

## Common Pitfalls & Guidance for Claude

- **Don't use `asyncio.run()` inside library code** — only call it from entry-point scripts.
  All internal code should be plain `async def` functions or methods.
- **Always `await` file flushes** before returning from any function that writes Raft
  persistent state or the master WAL. Missing an `await` here violates safety invariants.
- **Election timeouts must be randomised per node** to avoid split votes. Use
  `random.uniform(*config.raft.election_timeout_ms)` converted to seconds.
- **`asyncio.Lock` vs `asyncio.Event`**: use `Lock` for exclusive access to shared state,
  `Event` to signal between tasks (e.g., new log entry available).
- **`asyncio.Task` cancellation**: always handle `CancelledError` in long-running tasks
  (heartbeat loop, election timer) and clean up properly.
- **Test timeouts**: `pytest-asyncio` tests should have a global timeout (e.g., 10 s) to
  catch infinite waits in Raft loops during tests.
- **No blocking calls in the event loop**: `time.sleep`, `open()`, `socket.recv()` etc.
  must never appear in async code. Use `asyncio.sleep`, `aiofiles.open`, `asyncio` streams.

---

## Current Implementation Status

Track progress here as phases are completed.

- [ ] Phase 1 — RPC layer
- [ ] Phase 2 — MasterNode namespace & block manager
- [ ] Phase 3 — DataNode block store & heartbeat
- [ ] Phase 4 — DFS Client (single node)
- [ ] Phase 5 — Raft (standalone, unit-tested)
- [ ] Phase 6 — Raft integrated into DataNodes
- [ ] Phase 7 — Failure detection & node recovery
- [ ] Phase 8 — Integration tests & hardening