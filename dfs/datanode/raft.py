"""Per-block Raft consensus node — Phase 2 (with persistence + dynamic membership).

Every RaftNode persists the three Raft "durable" fields before replying to
any RPC, exactly as the paper requires:

    current_term  — updated on term bump or vote-grant
    voted_for     — updated when voting
    log[]         — entries appended atomically before replication

Additionally we persist *last_applied* so a restarting node knows which
entries are already reflected in the on-disk block file and need not be
re-applied.

Dynamic membership uses the single-server-at-a-time approach:
  • The Raft leader appends a CONFIG log entry containing the new peer list.
  • All nodes switch to the new peer list when they apply that entry.
  • Adding a peer: leader first sends InstallSnapshot to the new node so it
    catches up with the current block state, then appends the CONFIG entry.
  • Removing a peer: leader appends the CONFIG entry directly; the removed
    node is excluded from future AppendEntries once the entry commits.

On-disk layout (inside the DataNode's data_dir)
───────────────────────────────────────────────
  raft_{block_id}.json        – JSON: current_term, voted_for,
                                last_applied, peers
  raft_{block_id}.log         – binary: sequential log entries
  raft_{block_id}.log.tmp     – atomic-rename temp file

Binary log entry format (big-endian):
  [8 B term] [8 B intra_offset] [4 B data_len] [data_len B data]
  [1 B entry_type]  (0=DATA, 1=CONFIG)
  For CONFIG entries: after the data field comes a peers section:
    [4 B peers_count] followed by peers_count × [4 B addr_len][addr bytes]

Startup recovery
────────────────
  If a state file already exists the node restarts as FOLLOWER regardless
  of whether it was previously leader.  The Raft leader will catch it up
  via AppendEntries / heartbeats.
"""

import asyncio
import json
import logging
import os
import random
import struct
import time
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from grpc import aio

from dfs.proto import datanode_pb2, datanode_pb2_grpc
from dfs.proto import master_pb2, master_pb2_grpc

logger = logging.getLogger(__name__)

# ── Timing constants ───────────────────────────────────────────────────────

ELECTION_TIMEOUT_MIN   = 5   # 150 ms
ELECTION_TIMEOUT_MAX   = 6   # 300 ms
HEARTBEAT_INTERVAL     = 1.0   # 50 ms  (Raft internal peer heartbeat)
MASTER_HB_INTERVAL     = 5.0    # 5 s    (leader → master heartbeat)
RPC_TIMEOUT            = 1.0   # 80 ms  (peer RPC deadline)
WRITE_TIMEOUT          = 10.0    # 5 s    (client write replication deadline)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]

# Binary log-entry header: term(u64) intra_offset(u64) data_len(u32) entry_type(u8)
_LOG_HDR = struct.Struct(">QQIb")

ENTRY_DATA   = 0
ENTRY_CONFIG = 1


# ── Data structures ────────────────────────────────────────────────────────

class State(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


class _LogEntry:
    __slots__ = ("term", "intra_offset", "data", "entry_type", "new_peers")

    def __init__(
        self,
        term: int,
        intra_offset: int,
        data: bytes,
        entry_type: int = ENTRY_DATA,
        new_peers: Optional[List[str]] = None,
    ) -> None:
        self.term         = term
        self.intra_offset = intra_offset
        self.data         = data
        self.entry_type   = entry_type
        self.new_peers    = new_peers or []


# ── RaftNode ───────────────────────────────────────────────────────────────

class RaftNode:
    """Raft consensus module for a single block."""

    def __init__(
        self,
        block_id: str,
        node_id: str,                              # "host:port" of this node
        state_dir: Path,                           # directory for persisted files
        peers: List[str],                          # initial peer list (overridden if state file present)
        is_initial_leader: bool,                   # ignored if state file present
        master_addr: str,
        apply_fn: Callable[[int, bytes], Awaitable[None]],
    ) -> None:
        self.block_id  = block_id
        self.node_id   = node_id
        self._state_dir = state_dir
        self._master   = master_addr
        self._apply_fn = apply_fn

        # ── Raft persistent state ──────────────────────────────────────────
        self.current_term: int        = 0
        self.voted_for: Optional[str] = None

        # ── Raft volatile state ────────────────────────────────────────────
        self._log:           List[_LogEntry] = []
        self.commit_index:   int = 0
        self._last_applied:  int = 0

        # ── Leader volatile state ──────────────────────────────────────────
        self._next_index:  Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}

        # ── Node state ─────────────────────────────────────────────────────
        self._state:     State           = State.FOLLOWER
        self._leader_id: Optional[str]   = None
        self._lock       = asyncio.Lock()
        self._hb_event   = asyncio.Event()   # valid AppendEntries received
        self._rep_event  = asyncio.Event()   # new log entry added (wake leader loop)
        self._shutdown   = False

        # Pending write futures: log_index (1-based) → Future
        self._write_futures: Dict[int, asyncio.Future] = {}

        # ── Load or initialise persistent state ────────────────────────────
        saved = self._load_state()
        if saved:
            # ── Restart path: resume from persisted state ──────────────────
            self.current_term  = saved["current_term"]
            self.voted_for     = saved.get("voted_for")
            self._last_applied = saved.get("last_applied", 0)
            self.peers         = saved.get("peers", peers)
            self._log          = self._load_log()
            self.commit_index  = self._last_applied   # safe lower bound
            # Always restart as follower; leader will catch us up.
            self._state        = State.FOLLOWER
            logger.info(
                "block[%.8s] %s  RESTART  term=%d voted=%s applied=%d log_len=%d peers=%s",
                block_id, node_id,
                self.current_term, self.voted_for,
                self._last_applied, len(self._log),
                self.peers,
            )
        else:
            # ── Fresh allocation path ──────────────────────────────────────
            self.peers = peers
            if is_initial_leader:
                self.current_term = 1
                self.voted_for    = node_id
                self._state       = State.LEADER
                self._leader_id   = node_id
                for p in peers:
                    self._next_index[p]  = 1
                    self._match_index[p] = 0
                logger.info(
                    "block[%.8s] %s  FRESH LEADER  peers=%s",
                    block_id, node_id, peers,
                )
            else:
                logger.info(
                    "block[%.8s] %s  FRESH FOLLOWER  peers=%s",
                    block_id, node_id, peers,
                )
            self._save_state()   # persist initial state before any RPCs

    # ── File paths ─────────────────────────────────────────────────────────

    def _state_path(self) -> Path:
        return self._state_dir / f"raft_{self.block_id}.json"

    def _log_path(self) -> Path:
        return self._state_dir / f"raft_{self.block_id}.log"

    # ── Persistence: state file ────────────────────────────────────────────

    def _save_state(self) -> None:
        """Atomically persist current_term, voted_for, last_applied, peers."""
        payload = {
            "current_term":  self.current_term,
            "voted_for":     self.voted_for,
            "last_applied":  self._last_applied,
            "peers":         self.peers,
        }
        tmp = self._state_path().with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            tmp.rename(self._state_path())
            logger.debug(
                "block[%.8s] %s  persisted state  term=%d voted=%s applied=%d peers=%s",
                self.block_id, self.node_id,
                payload["current_term"], payload["voted_for"],
                payload["last_applied"], payload["peers"],
            )
        except Exception as exc:
            logger.error("block[%.8s] _save_state FAILED: %s", self.block_id, exc)
            raise

    def _load_state(self) -> Optional[dict]:
        p = self._state_path()
        if not p.exists():
            return None
        try:
            with open(p) as f:
                return json.load(f)
        except Exception as exc:
            logger.error(
                "block[%.8s] _load_state FAILED (treating as fresh): %s",
                self.block_id, exc,
            )
            return None

    # ── Persistence: log file ──────────────────────────────────────────────

    def _append_log_entry_locked(self, entry: _LogEntry) -> None:
        """Append one entry to in-memory log AND binary log file.
        Must hold self._lock.
        """
        p = self._log_path()
        try:
            with open(p, "ab") as f:
                f.write(_LOG_HDR.pack(
                    entry.term, entry.intra_offset, len(entry.data), entry.entry_type
                ))
                f.write(entry.data)
                if entry.entry_type == ENTRY_CONFIG:
                    # Encode peers: [4B count] + per-peer [4B len][bytes]
                    f.write(struct.pack(">I", len(entry.new_peers)))
                    for peer in entry.new_peers:
                        pb = peer.encode()
                        f.write(struct.pack(">I", len(pb)))
                        f.write(pb)
                f.flush()
                os.fsync(f.fileno())
        except Exception as exc:
            logger.error("block[%.8s] _append_log_entry FAILED: %s", self.block_id, exc)
            raise
        self._log.append(entry)
        logger.debug(
            "block[%.8s] %s  log entry persisted  idx=%d term=%d type=%d bytes=%d",
            self.block_id, self.node_id, len(self._log),
            entry.term, entry.entry_type, len(entry.data),
        )

    def _truncate_log_locked(self, new_len: int) -> None:
        """Truncate log to *new_len* entries (in-memory and on disk).
        Must hold self._lock.
        """
        if new_len >= len(self._log):
            return
        logger.debug(
            "block[%.8s] %s  LOG TRUNCATE len %d → %d",
            self.block_id, self.node_id, len(self._log), new_len,
        )
        keep = self._log[:new_len]
        p    = self._log_path()
        tmp  = p.with_suffix(".tmp")
        try:
            with open(tmp, "wb") as f:
                for e in keep:
                    f.write(_LOG_HDR.pack(
                        e.term, e.intra_offset, len(e.data), e.entry_type
                    ))
                    f.write(e.data)
                    if e.entry_type == ENTRY_CONFIG:
                        f.write(struct.pack(">I", len(e.new_peers)))
                        for peer in e.new_peers:
                            pb = peer.encode()
                            f.write(struct.pack(">I", len(pb)))
                            f.write(pb)
                f.flush()
                os.fsync(f.fileno())
            tmp.rename(p)
        except Exception as exc:
            logger.error("block[%.8s] _truncate_log FAILED: %s", self.block_id, exc)
            raise
        self._log = keep

    def _load_log(self) -> List[_LogEntry]:
        p = self._log_path()
        if not p.exists():
            return []
        entries: List[_LogEntry] = []
        try:
            with open(p, "rb") as f:
                idx = 1
                while True:
                    hdr = f.read(_LOG_HDR.size)
                    if not hdr:
                        break
                    if len(hdr) < _LOG_HDR.size:
                        logger.warning(
                            "block[%.8s] truncated log header at entry %d — discarding tail",
                            self.block_id, idx,
                        )
                        break
                    term, intra_offset, data_len, entry_type = _LOG_HDR.unpack(hdr)
                    data = f.read(data_len)
                    if len(data) < data_len:
                        logger.warning(
                            "block[%.8s] truncated log data at entry %d — discarding tail",
                            self.block_id, idx,
                        )
                        break
                    new_peers: List[str] = []
                    if entry_type == ENTRY_CONFIG:
                        count_bytes = f.read(4)
                        if len(count_bytes) < 4:
                            break
                        count = struct.unpack(">I", count_bytes)[0]
                        for _ in range(count):
                            ln_bytes = f.read(4)
                            if len(ln_bytes) < 4:
                                break
                            ln = struct.unpack(">I", ln_bytes)[0]
                            addr = f.read(ln)
                            if len(addr) < ln:
                                break
                            new_peers.append(addr.decode())
                    entries.append(_LogEntry(term, intra_offset, data, entry_type, new_peers))
                    idx += 1
        except Exception as exc:
            logger.error("block[%.8s] _load_log FAILED at entry %d: %s",
                         self.block_id, len(entries) + 1, exc)
        logger.debug("block[%.8s] loaded %d log entries from disk", self.block_id, len(entries))
        return entries

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """Schedule background Raft tasks on the running event loop."""
        logger.debug(
            "block[%.8s] %s  START  state=%s term=%d peers=%s",
            self.block_id, self.node_id, self._state.value, self.current_term, self.peers,
        )
        asyncio.create_task(self._apply_loop(), name=f"apply-{self.block_id[:8]}")
        asyncio.create_task(self._main_loop(),  name=f"main-{self.block_id[:8]}")
        if self._state == State.LEADER:
            asyncio.create_task(
                self._leader_loop(), name=f"leader-{self.block_id[:8]}"
            )
            asyncio.create_task(self._notify_master())

    def stop(self) -> None:
        logger.debug(
            "block[%.8s] %s  STOP  state=%s term=%d",
            self.block_id, self.node_id, self._state.value, self.current_term,
        )
        self._shutdown = True
        self._hb_event.set()
        self._rep_event.set()

    # ── Public API ─────────────────────────────────────────────────────────

    def is_leader(self) -> bool:
        return self._state == State.LEADER

    @property
    def leader_id(self) -> Optional[str]:
        return self._leader_id

    async def append_and_replicate(
        self, intra_offset: int, data: bytes
    ) -> Tuple[bool, str]:
        """Append a DATA write to the log and wait for majority commit."""
        return await self._append_entry_and_wait(
            _LogEntry(0, intra_offset, data, ENTRY_DATA)
        )

    async def append_config_and_replicate(
        self, new_peers: List[str]
    ) -> Tuple[bool, str]:
        """Append a CONFIG membership-change entry and wait for majority commit."""
        return await self._append_entry_and_wait(
            _LogEntry(0, 0, b"", ENTRY_CONFIG, new_peers)
        )

    async def _append_entry_and_wait(self, entry: _LogEntry) -> Tuple[bool, str]:
        loop = asyncio.get_running_loop()
        async with self._lock:
            if self._state != State.LEADER:
                return False, f"not leader (leader={self._leader_id})"
            entry.term = self.current_term
            self._append_log_entry_locked(entry)
            log_index = len(self._log)
            fut = loop.create_future()
            self._write_futures[log_index] = fut
            logger.debug(
                "block[%.8s] %s  APPEND  idx=%d term=%d type=%d",
                self.block_id, self.node_id, log_index, entry.term, entry.entry_type,
            )

        self._rep_event.set()

        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout=WRITE_TIMEOUT)
            logger.debug(
                "block[%.8s] %s  REPLICATED  idx=%d",
                self.block_id, self.node_id, log_index,
            )
            return True, ""
        except asyncio.TimeoutError:
            async with self._lock:
                self._write_futures.pop(log_index, None)
            return False, "replication timeout"
        except Exception as exc:
            return False, str(exc)

    # ── Dynamic membership ─────────────────────────────────────────────────

    async def add_peer(self, new_peer: str, block_path: Path) -> Tuple[bool, str]:
        """Add *new_peer* to this Raft group.

        Steps:
          1. Send InstallSnapshot to the new peer.
          2. Append a CONFIG log entry with the updated peer list.
          3. After commit, initialise replication tracking for the new peer.

        Returns (ok, error).
        """
        async with self._lock:
            if self._state != State.LEADER:
                return False, f"not leader (leader={self._leader_id})"
            if new_peer in self.peers:
                return True, ""   # already a member
            current_peers = list(self.peers)
            snapshot_index = len(self._log)
            snapshot_term  = self._log[-1].term if self._log else self.current_term
            term = self.current_term

        # Read the block file for snapshot data.
        try:
            if block_path.exists():
                with open(block_path, "rb") as fh:
                    snapshot_data = fh.read()
            else:
                snapshot_data = b""
        except Exception as exc:
            return False, f"could not read block file for snapshot: {exc}"

        # Send snapshot to the new peer so it has block data before joining.
        logger.info(
            "block[%.8s] %s  InstallSnapshot → %s  snap_idx=%d bytes=%d",
            self.block_id, self.node_id, new_peer, snapshot_index, len(snapshot_data),
        )
        ok, err = await self._install_snapshot_rpc(
            new_peer, term, snapshot_index, snapshot_term,
            snapshot_data, current_peers,
        )
        if not ok:
            return False, f"InstallSnapshot failed: {err}"

        # Append the CONFIG entry (includes new_peer).
        new_peer_list = current_peers + [new_peer]
        ok, err = await self.append_config_and_replicate(new_peer_list)
        if not ok:
            return False, f"CONFIG entry replication failed: {err}"

        # Initialise leader tracking for new peer (post-commit; already unlocked).
        async with self._lock:
            self._next_index[new_peer]  = snapshot_index + 1
            self._match_index[new_peer] = snapshot_index

        logger.info(
            "block[%.8s] %s  ADD PEER DONE  new_peer=%s peers=%s",
            self.block_id, self.node_id, new_peer, self.peers,
        )
        return True, ""

    async def remove_peer(self, peer: str) -> Tuple[bool, str]:
        """Remove *peer* from this Raft group by appending a CONFIG entry."""
        async with self._lock:
            if self._state != State.LEADER:
                return False, f"not leader (leader={self._leader_id})"
            if peer not in self.peers and peer != self.node_id:
                return True, ""   # not a member, nothing to do
            new_peer_list = [p for p in self.peers if p != peer]

        ok, err = await self.append_config_and_replicate(new_peer_list)
        if not ok:
            return False, f"CONFIG entry replication failed: {err}"

        logger.info(
            "block[%.8s] %s  REMOVE PEER DONE  removed=%s peers=%s",
            self.block_id, self.node_id, peer, self.peers,
        )
        return True, ""

    async def handle_install_snapshot(
        self,
        term: int,
        leader_id: str,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
        peers: List[str],
        block_path: Path,
    ) -> Tuple[int, bool, str]:
        """Apply an InstallSnapshot from the leader.

        Returns (current_term, success, error).
        """
        async with self._lock:
            if term < self.current_term:
                return self.current_term, False, "stale term"

            if term > self.current_term:
                self._become_follower_locked(term)

            self._leader_id = leader_id
            self._hb_event.set()

            # Write snapshot data to block file.
            try:
                tmp = block_path.with_suffix(".snap_tmp")
                with open(tmp, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                tmp.rename(block_path)
            except Exception as exc:
                return self.current_term, False, f"snapshot write failed: {exc}"

            # Discard any log entries covered by the snapshot.
            self._log = []
            self._last_applied   = last_included_index
            self.commit_index    = last_included_index
            self.peers           = [p for p in peers if p != self.node_id]
            self._save_state()

            # Truncate log file.
            lp = self._log_path()
            if lp.exists():
                lp.unlink()

        logger.info(
            "block[%.8s] %s  INSTALL SNAPSHOT  leader=%s idx=%d peers=%s bytes=%d",
            self.block_id, self.node_id, leader_id,
            last_included_index, self.peers, len(data),
        )
        return self.current_term, True, ""

    # ── Incoming Raft RPC handlers ─────────────────────────────────────────

    async def handle_request_vote(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        """Handle RequestVote RPC.  Return (current_term, vote_granted)."""
        async with self._lock:
            state_changed = False

            if term < self.current_term:
                logger.debug(
                    "block[%.8s] %s  RequestVote REJECT  from=%s term=%d < cur=%d",
                    self.block_id, self.node_id, candidate_id, term, self.current_term,
                )
                return self.current_term, False

            if term > self.current_term:
                logger.info(
                    "block[%.8s] %s  TERM BUMP  %d → %d (saw RequestVote from %s)",
                    self.block_id, self.node_id, self.current_term, term, candidate_id,
                )
                self._become_follower_locked(term)
                state_changed = True

            my_last_idx  = len(self._log)
            my_last_term = self._log[-1].term if self._log else 0
            log_ok = (last_log_term > my_last_term) or (
                last_log_term == my_last_term and last_log_index >= my_last_idx
            )

            if (
                (self.voted_for is None or self.voted_for == candidate_id)
                and log_ok
            ):
                if self.voted_for != candidate_id:
                    self.voted_for = candidate_id
                    state_changed = True
                logger.info(
                    "block[%.8s] %s  VOTE GRANTED  to=%s term=%d",
                    self.block_id, self.node_id, candidate_id, term,
                )
                if state_changed:
                    self._save_state()
                self._hb_event.set()
                return self.current_term, True

            logger.debug(
                "block[%.8s] %s  RequestVote REJECT  from=%s "
                "(voted_for=%s log_ok=%s)",
                self.block_id, self.node_id, candidate_id,
                self.voted_for, log_ok,
            )
            if state_changed:
                self._save_state()
            return self.current_term, False

    async def handle_append_entries(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[Tuple],   # (term, intra_offset, data[, entry_type, new_peers])
        leader_commit: int,
    ) -> Tuple[int, bool, int]:
        """Handle AppendEntries RPC.  Return (current_term, success, match_index)."""
        async with self._lock:
            state_dirty = False

            if term < self.current_term:
                logger.debug(
                    "block[%.8s] %s  AppendEntries STALE  from=%s term=%d < cur=%d",
                    self.block_id, self.node_id, leader_id, term, self.current_term,
                )
                return self.current_term, False, 0

            if term > self.current_term:
                logger.info(
                    "block[%.8s] %s  TERM BUMP  %d → %d (AppendEntries from %s)",
                    self.block_id, self.node_id, self.current_term, term, leader_id,
                )
                self._become_follower_locked(term)
                state_dirty = True

            if self._state == State.CANDIDATE:
                logger.info(
                    "block[%.8s] %s  CANDIDATE → FOLLOWER  (saw leader %s term=%d)",
                    self.block_id, self.node_id, leader_id, term,
                )
                self._state    = State.FOLLOWER
                self.voted_for = None
                state_dirty    = True

            self._leader_id = leader_id
            self._hb_event.set()

            # ── Log consistency check ──────────────────────────────────────
            if prev_log_index > 0:
                if len(self._log) < prev_log_index:
                    logger.debug(
                        "block[%.8s] %s  AppendEntries MISSING  "
                        "prev_idx=%d have=%d",
                        self.block_id, self.node_id, prev_log_index, len(self._log),
                    )
                    if state_dirty:
                        self._save_state()
                    return self.current_term, False, len(self._log)

                if self._log[prev_log_index - 1].term != prev_log_term:
                    logger.info(
                        "block[%.8s] %s  LOG CONFLICT  prev_idx=%d "
                        "want_term=%d have_term=%d — truncating",
                        self.block_id, self.node_id, prev_log_index,
                        prev_log_term, self._log[prev_log_index - 1].term,
                    )
                    self._truncate_log_locked(prev_log_index - 1)
                    if state_dirty:
                        self._save_state()
                    return self.current_term, False, prev_log_index - 1

            # ── Append / reconcile new entries ─────────────────────────────
            new_entries = 0
            for i, raw in enumerate(entries):
                e_term, e_off, e_data = raw[0], raw[1], raw[2]
                e_type     = raw[3] if len(raw) > 3 else ENTRY_DATA
                e_peers    = list(raw[4]) if len(raw) > 4 else []
                pos = prev_log_index + i
                if pos < len(self._log):
                    if self._log[pos].term != e_term:
                        logger.info(
                            "block[%.8s] %s  LOG CONFLICT at pos=%d "
                            "want_term=%d have_term=%d — truncating",
                            self.block_id, self.node_id, pos + 1,
                            e_term, self._log[pos].term,
                        )
                        self._truncate_log_locked(pos)
                        self._append_log_entry_locked(
                            _LogEntry(e_term, e_off, e_data, e_type, e_peers)
                        )
                        new_entries += 1
                else:
                    self._append_log_entry_locked(
                        _LogEntry(e_term, e_off, e_data, e_type, e_peers)
                    )
                    new_entries += 1

            if new_entries:
                logger.debug(
                    "block[%.8s] %s  AppendEntries OK  "
                    "from=%s new=%d log_len=%d",
                    self.block_id, self.node_id, leader_id,
                    new_entries, len(self._log),
                )

            # ── Advance commit index ───────────────────────────────────────
            new_commit = min(leader_commit, len(self._log))
            if new_commit > self.commit_index:
                logger.debug(
                    "block[%.8s] %s  COMMIT  %d → %d",
                    self.block_id, self.node_id, self.commit_index, new_commit,
                )
                self.commit_index = new_commit

            if state_dirty:
                self._save_state()

            return self.current_term, True, len(self._log)

    # ── Background loops ───────────────────────────────────────────────────

    async def _main_loop(self) -> None:
        """Drive follower ↔ candidate transitions (election timer)."""
        while not self._shutdown:
            if self._state == State.LEADER:
                await asyncio.sleep(0.05)
                continue

            self._hb_event.clear()
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            try:
                await asyncio.wait_for(self._hb_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                if not self._shutdown and self._state != State.LEADER:
                    logger.info(
                        "block[%.8s] %s  ELECTION TIMEOUT  state=%s timeout=%.0fms",
                        self.block_id, self.node_id, self._state.value,
                        timeout * 1000,
                    )
                    await self._start_election()

    async def _leader_loop(self) -> None:
        """Send periodic AppendEntries / heartbeats to all peers while leader."""
        master_hb_at = time.monotonic()

        while not self._shutdown and self._state == State.LEADER:
            self._rep_event.clear()
            try:
                await asyncio.wait_for(
                    self._rep_event.wait(), timeout=HEARTBEAT_INTERVAL
                )
            except asyncio.TimeoutError:
                pass

            if self._shutdown or self._state != State.LEADER:
                return

            async with self._lock:
                term  = self.current_term
                peers = list(self.peers)

            await asyncio.gather(
                *[self._send_append_entries(p, term) for p in peers],
                return_exceptions=True,
            )

            now = time.monotonic()
            if now >= master_hb_at:
                asyncio.create_task(self._send_master_heartbeat())
                master_hb_at = now + MASTER_HB_INTERVAL

    async def _apply_loop(self) -> None:
        """Apply committed entries to the block-file state machine.

        DATA entries call the apply_fn (file write).
        CONFIG entries update the peer list and notify the master.
        """
        while not self._shutdown:
            async with self._lock:
                if self._last_applied >= self.commit_index:
                    await asyncio.sleep(0.005)
                    continue
                idx   = self._last_applied + 1   # 1-based
                entry = self._log[idx - 1]

            # Apply without holding the lock.
            try:
                if entry.entry_type == ENTRY_DATA:
                    await self._apply_fn(entry.intra_offset, entry.data)
                    logger.debug(
                        "block[%.8s] %s  APPLY DATA  idx=%d intra=%d len=%d",
                        self.block_id, self.node_id, idx,
                        entry.intra_offset, len(entry.data),
                    )
                elif entry.entry_type == ENTRY_CONFIG:
                    await self._apply_config(entry.new_peers)
                    logger.info(
                        "block[%.8s] %s  APPLY CONFIG  idx=%d peers=%s",
                        self.block_id, self.node_id, idx, entry.new_peers,
                    )
            except Exception as exc:
                logger.error(
                    "block[%.8s] apply error at log[%d]: %s",
                    self.block_id, idx, exc,
                )
                await asyncio.sleep(0.05)
                continue

            async with self._lock:
                if idx > self._last_applied:
                    self._last_applied = idx
                    self._save_state()
                fut = self._write_futures.pop(idx, None)
                if fut and not fut.done():
                    fut.set_result(True)

    async def _apply_config(self, new_peers: List[str]) -> None:
        """Switch to the new peer list when a CONFIG entry is applied."""
        async with self._lock:
            old_peers = list(self.peers)
            # The peers list in the state file excludes self.
            self.peers = [p for p in new_peers if p != self.node_id]
            self._save_state()

            if self._state == State.LEADER:
                # Add tracking for any newly added peers.
                for p in self.peers:
                    if p not in self._next_index:
                        self._next_index[p]  = len(self._log) + 1
                        self._match_index[p] = 0
                # Remove tracking for any removed peers.
                for p in list(self._next_index.keys()):
                    if p not in self.peers:
                        del self._next_index[p]
                        self._match_index.pop(p, None)

        logger.info(
            "block[%.8s] %s  CONFIG APPLIED  old=%s new=%s",
            self.block_id, self.node_id, old_peers, self.peers,
        )

        # Notify master of updated membership (fire-and-forget).
        if self._state == State.LEADER:
            asyncio.create_task(self._notify_master_peers(new_peers))

    # ── Election ───────────────────────────────────────────────────────────

    async def _start_election(self) -> None:
        async with self._lock:
            if self._state == State.LEADER:
                return
            self._state        = State.CANDIDATE
            self.current_term += 1
            self.voted_for     = self.node_id
            self._save_state()
            term      = self.current_term
            last_idx  = len(self._log)
            last_term = self._log[-1].term if self._log else 0

        logger.info(
            "block[%.8s] %s  ELECTION START  term=%d last_idx=%d last_term=%d",
            self.block_id, self.node_id, term, last_idx, last_term,
        )

        results = await asyncio.gather(
            *[self._request_vote_rpc(p, term, last_idx, last_term)
              for p in self.peers],
            return_exceptions=True,
        )

        votes = 1   # self-vote
        for peer, r in zip(self.peers, results):
            if isinstance(r, Exception):
                logger.debug(
                    "block[%.8s] %s  vote from %s — ERROR: %s",
                    self.block_id, self.node_id, peer, r,
                )
                continue
            peer_term, granted = r
            logger.debug(
                "block[%.8s] %s  vote from %s — term=%d granted=%s",
                self.block_id, self.node_id, peer, peer_term, granted,
            )
            if peer_term > term:
                async with self._lock:
                    logger.info(
                        "block[%.8s] %s  HIGHER TERM SEEN  %d > %d — reverting",
                        self.block_id, self.node_id, peer_term, term,
                    )
                    self._become_follower_locked(peer_term)
                return
            if granted:
                votes += 1

        total    = 1 + len(self.peers)
        majority = total // 2 + 1
        logger.debug(
            "block[%.8s] %s  vote tally: %d/%d (need %d)",
            self.block_id, self.node_id, votes, total, majority,
        )

        async with self._lock:
            if self._state != State.CANDIDATE or self.current_term != term:
                logger.debug(
                    "block[%.8s] %s  ELECTION STALE  state=%s term=%d (wanted %d)",
                    self.block_id, self.node_id, self._state.value,
                    self.current_term, term,
                )
                return
            if votes >= majority:
                logger.info(
                    "block[%.8s] %s  ELECTED LEADER  term=%d votes=%d/%d",
                    self.block_id, self.node_id, term, votes, total,
                )
                self._become_leader_locked()
            else:
                logger.info(
                    "block[%.8s] %s  ELECTION LOST  term=%d votes=%d/%d — back to follower",
                    self.block_id, self.node_id, term, votes, total,
                )
                self._state = State.FOLLOWER

    def _become_follower_locked(self, term: int) -> None:
        """Transition to follower with *term*.  Must hold self._lock."""
        prev_state = self._state.value
        self._state       = State.FOLLOWER
        self.current_term = term
        self.voted_for    = None
        self._save_state()
        logger.debug(
            "block[%.8s] %s  → FOLLOWER  (was %s) term=%d",
            self.block_id, self.node_id, prev_state, term,
        )

    def _become_leader_locked(self) -> None:
        """Transition to leader.  Must hold self._lock."""
        self._state     = State.LEADER
        self._leader_id = self.node_id
        log_len = len(self._log)
        for p in self.peers:
            self._next_index[p]  = log_len + 1
            self._match_index[p] = 0
        logger.debug(
            "block[%.8s] %s  → LEADER  term=%d log_len=%d next_index=%s",
            self.block_id, self.node_id, self.current_term, log_len,
            {p: log_len + 1 for p in self.peers},
        )
        asyncio.create_task(
            self._leader_loop(), name=f"leader-{self.block_id[:8]}"
        )
        asyncio.create_task(self._notify_master())

    def _check_commit_locked(self) -> None:
        """Advance commit_index to the highest safely replicatable entry.

        Only entries from the *current term* are directly committed (§5.4.2).
        Must hold self._lock.
        """
        if self._state != State.LEADER:
            return
        total    = 1 + len(self.peers)
        majority = total // 2 + 1

        for n in range(len(self._log), self.commit_index, -1):
            if self._log[n - 1].term != self.current_term:
                continue
            count = 1   # self
            for p in self.peers:
                if self._match_index.get(p, 0) >= n:
                    count += 1
            logger.debug(
                "block[%.8s] %s  check_commit idx=%d term=%d replicated=%d/%d",
                self.block_id, self.node_id, n, self._log[n - 1].term, count, total,
            )
            if count >= majority:
                if n > self.commit_index:
                    logger.debug(
                        "block[%.8s] %s  COMMIT ADVANCE  %d → %d",
                        self.block_id, self.node_id, self.commit_index, n,
                    )
                    self.commit_index = n
                break

    # ── Outgoing peer RPCs ─────────────────────────────────────────────────

    async def _request_vote_rpc(
        self, peer: str, term: int, last_log_index: int, last_log_term: int
    ) -> Tuple[int, bool]:
        logger.debug(
            "block[%.8s] %s  → RequestVote  peer=%s term=%d",
            self.block_id, self.node_id, peer, term,
        )
        try:
            async with aio.insecure_channel(peer, options=_GRPC_OPTIONS) as ch:
                stub = datanode_pb2_grpc.DataNodeStub(ch)
                resp = await asyncio.wait_for(
                    stub.RequestVote(
                        datanode_pb2.VoteRequest(
                            block_id=self.block_id,
                            term=term,
                            candidate_id=self.node_id,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term,
                        )
                    ),
                    timeout=RPC_TIMEOUT,
                )
            return resp.term, resp.vote_granted
        except Exception as exc:
            logger.debug(
                "block[%.8s] RequestVote → %s FAILED: %r", self.block_id, peer, exc
            )
            return 0, False

    async def _send_append_entries(self, peer: str, term: int) -> None:
        """Send AppendEntries (with any missing entries) to one peer."""
        async with self._lock:
            if self._state != State.LEADER or self.current_term != term:
                return
            next_idx       = self._next_index.get(peer, len(self._log) + 1)
            prev_log_index = next_idx - 1
            prev_log_term  = (
                self._log[prev_log_index - 1].term
                if prev_log_index > 0 and prev_log_index <= len(self._log)
                else 0
            )
            entries_to_send = self._log[prev_log_index:]
            commit          = self.commit_index

        entries_proto = [
            datanode_pb2.LogEntry(
                term=e.term,
                intra_block_offset=e.intra_offset,
                data=e.data,
                index=prev_log_index + i + 1,
                entry_type=(
                    datanode_pb2.LogEntryType.CONFIG
                    if e.entry_type == ENTRY_CONFIG
                    else datanode_pb2.LogEntryType.DATA
                ),
                new_peers=e.new_peers,
            )
            for i, e in enumerate(entries_to_send)
        ]

        if entries_to_send:
            logger.debug(
                "block[%.8s] %s  → AppendEntries  peer=%s "
                "term=%d prev_idx=%d sending=%d commit=%d",
                self.block_id, self.node_id, peer,
                term, prev_log_index, len(entries_to_send), commit,
            )

        try:
            async with aio.insecure_channel(peer, options=_GRPC_OPTIONS) as ch:
                stub = datanode_pb2_grpc.DataNodeStub(ch)
                resp = await asyncio.wait_for(
                    stub.AppendEntries(
                        datanode_pb2.AppendEntriesRequest(
                            block_id=self.block_id,
                            term=term,
                            leader_id=self.node_id,
                            prev_log_index=prev_log_index,
                            prev_log_term=prev_log_term,
                            entries=entries_proto,
                            leader_commit=commit,
                        )
                    ),
                    timeout=RPC_TIMEOUT,
                )

            async with self._lock:
                if resp.term > self.current_term:
                    logger.info(
                        "block[%.8s] %s  STEP DOWN  "
                        "peer=%s sent term=%d > cur=%d",
                        self.block_id, self.node_id,
                        peer, resp.term, self.current_term,
                    )
                    self._become_follower_locked(resp.term)
                    return
                if self._state != State.LEADER or self.current_term != term:
                    return
                if resp.success:
                    new_match = prev_log_index + len(entries_to_send)
                    if new_match > self._match_index.get(peer, 0):
                        self._match_index[peer] = new_match
                        self._next_index[peer]  = new_match + 1
                        self._check_commit_locked()
                    if entries_to_send:
                        logger.debug(
                            "block[%.8s] %s  AppendEntries ACK  peer=%s match=%d",
                            self.block_id, self.node_id, peer, new_match,
                        )
                else:
                    hint = resp.match_index
                    self._next_index[peer] = max(1, hint + 1)
                    logger.debug(
                        "block[%.8s] %s  AppendEntries NACK  peer=%s hint=%d next=%d",
                        self.block_id, self.node_id, peer,
                        hint, self._next_index[peer],
                    )

        except Exception as exc:
            logger.debug(
                "block[%.8s] AppendEntries → %s FAILED: %r",
                self.block_id, peer, exc,
            )

    async def _install_snapshot_rpc(
        self,
        peer: str,
        term: int,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
        peers: List[str],
    ) -> Tuple[bool, str]:
        try:
            async with aio.insecure_channel(peer, options=_GRPC_OPTIONS) as ch:
                stub = datanode_pb2_grpc.DataNodeStub(ch)
                resp = await asyncio.wait_for(
                    stub.InstallSnapshot(
                        datanode_pb2.InstallSnapshotRequest(
                            block_id=self.block_id,
                            term=term,
                            leader_id=self.node_id,
                            last_included_index=last_included_index,
                            last_included_term=last_included_term,
                            data=data,
                            peers=peers + [peer],
                        )
                    ),
                    timeout=30.0,   # snapshots can be large
                )
            if resp.success:
                return True, ""
            return False, resp.error
        except Exception as exc:
            return False, str(exc)

    # ── Master notifications ───────────────────────────────────────────────

    async def _notify_master(self) -> None:
        """Inform the MasterNode that this node is the new leader."""
        async with self._lock:
            term = self.current_term
        logger.info(
            "block[%.8s] %s  NOTIFY MASTER  I am leader term=%d",
            self.block_id, self.node_id, term,
        )
        try:
            async with aio.insecure_channel(self._master, options=_GRPC_OPTIONS) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                await stub.NotifyLeader(
                    master_pb2.NotifyLeaderRequest(
                        block_id=self.block_id,
                        leader_id=self.node_id,
                        term=term,
                    )
                )
        except Exception as exc:
            logger.warning(
                "block[%.8s] notify_master FAILED: %s — will retry on next heartbeat",
                self.block_id, exc,
            )

    async def _notify_master_peers(self, new_peers: List[str]) -> None:
        """Tell the MasterNode the updated full peer list after a CONFIG commit."""
        try:
            async with aio.insecure_channel(self._master, options=_GRPC_OPTIONS) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                async with self._lock:
                    term = self.current_term
                await stub.UpdateBlockPeers(
                    master_pb2.UpdateBlockPeersRequest(
                        block_id=self.block_id,
                        peers=new_peers,
                        term=term,
                    )
                )
            logger.info(
                "block[%.8s] %s  UpdateBlockPeers ACK  peers=%s",
                self.block_id, self.node_id, new_peers,
            )
        except Exception as exc:
            logger.warning(
                "block[%.8s] notify_master_peers FAILED: %s",
                self.block_id, exc,
            )

    async def _send_master_heartbeat(self) -> None:
        async with self._lock:
            if self._state != State.LEADER:
                return
            term = self.current_term
        logger.debug(
            "block[%.8s] %s  → BlockHeartbeat master=%s term=%d",
            self.block_id, self.node_id, self._master, term,
        )
        try:
            async with aio.insecure_channel(self._master, options=_GRPC_OPTIONS) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                resp = await stub.BlockHeartbeat(
                    master_pb2.BlockHeartbeatRequest(
                        block_id=self.block_id,
                        leader_id=self.node_id,
                        term=term,
                    )
                )
            if not resp.ok:
                logger.debug(
                    "block[%.8s] %s  BlockHeartbeat NACK — re-notifying master",
                    self.block_id, self.node_id,
                )
                await self._notify_master()
            else:
                logger.debug(
                    "block[%.8s] %s  BlockHeartbeat ACK", self.block_id, self.node_id
                )
        except Exception as exc:
            logger.debug(
                "block[%.8s] master heartbeat FAILED: %r", self.block_id, exc
            )
