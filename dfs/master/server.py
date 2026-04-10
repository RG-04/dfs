"""MasterNode gRPC server — Phase 2 (Raft-aware, directory namespace).

Namespace model
---------------
* The root directory ``/`` always exists and cannot be removed.
* Files may only be created inside an existing directory.
* Directories must be created explicitly with ``Mkdir`` before use.
* ``Rmdir`` fails if the directory still contains files or subdirectories.
* ``Stat`` and ``ListDir`` work on both files and directories.

Raft integration
----------------
* Block metadata stores the full Raft-group peer list plus the current leader.
* ``GetBlockInfo`` allocates ``replication_factor`` DataNodes and calls
  ``RegisterBlock`` on ALL of them (one is designated initial leader).
* ``GetBlockInfo`` routes both reads and writes to the *current* Raft leader.
* ``NotifyLeader`` and ``BlockHeartbeat`` let DataNodes keep the Master
  informed about leadership changes.
* A background watchdog marks blocks unavailable after ``heartbeat_timeout``
  seconds of silence from the leader.
* New block allocation is denied when fewer than ``replication_factor``
  DataNodes are currently registered.
"""

import asyncio
import json
import logging
import posixpath
import time
import uuid
from pathlib import Path
from typing import Dict, List, Set

import grpc
from grpc import aio

from dfs.proto import master_pb2, master_pb2_grpc
from dfs.proto import datanode_pb2, datanode_pb2_grpc

logger = logging.getLogger(__name__)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]


class MasterNodeServicer(master_pb2_grpc.MasterNodeServicer):
    def __init__(self, config: dict) -> None:
        self._block_size:        int = config["block_size"]
        self._replication_factor: int = config.get("replication_factor", 3)
        self._hb_timeout:        float = float(config.get("heartbeat_timeout", 15))
        # How long since the last registration heartbeat before a DataNode is
        # considered unreachable for new block allocation.  Defaults to the
        # DataNode heartbeat interval (10 s) plus a generous margin.
        self._dn_liveness_timeout: float = float(
            config.get("dn_liveness_timeout", 30)
        )

        # Metadata persistence
        meta_dir = Path(config["master"]["metadata_dir"])
        meta_dir.mkdir(parents=True, exist_ok=True)
        self._meta_file = meta_dir / "metadata.json"

        # Expected DataNodes from static config
        self._expected_dns: Dict[str, dict] = {
            dn["id"]: dn for dn in config["datanodes"]
        }

        # Runtime registration state
        self._registered_dns: Dict[str, dict] = {}   # dn_id → {host, port, last_seen}
        self._addr_to_dn:     Dict[str, str]  = {}   # "host:port" → dn_id
        self._ready  = asyncio.Event()
        self._lock   = asyncio.Lock()

        # Round-robin counter for group assignment
        self._rr_counter: int = 0

        # File namespace.
        # Block metadata (Phase 2):
        #   {"block_id": str,
        #    "peers":    [dn_id, ...],   # all group members
        #    "leader_id": str}           # current leader's dn_id
        self._files: Dict[str, dict] = {}
        # Directory set — "/" is always present.
        self._dirs:  Set[str]        = {"/"}
        # Persistent deletion queue.
        # Each entry: {"block_id": str, "pending_peers": [dn_id, ...]}
        # Survives master restarts; entries are removed once all peers confirm.
        self._delete_queue: List[dict] = []
        self._load_metadata()

        # Per-block leader tracking (volatile — rebuilt from NotifyLeader /
        # BlockHeartbeat after restart).
        # block_id → {leader_id, term, last_hb, available}
        self._block_status: Dict[str, dict] = {}

        # Signals the deletion loop that new work has been enqueued.
        self._delete_event = asyncio.Event()

    # ── Persistence ────────────────────────────────────────────────────────

    def _load_metadata(self) -> None:
        if self._meta_file.exists():
            with open(self._meta_file) as fh:
                saved = json.load(fh)
            self._files        = saved.get("files", {})
            self._dirs         = set(saved.get("dirs", ["/"])) | {"/"}
            self._delete_queue = saved.get("delete_queue", [])
            logger.info(
                "Loaded metadata: %d file(s), %d dir(s), %d pending deletion(s)",
                len(self._files), len(self._dirs), len(self._delete_queue),
            )

    def _save_metadata(self) -> None:
        tmp = self._meta_file.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump(
                {
                    "files":        self._files,
                    "dirs":         sorted(self._dirs),
                    "delete_queue": self._delete_queue,
                },
                fh, indent=2,
            )
        tmp.rename(self._meta_file)

    # ── Path helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _parent(path: str) -> str:
        """Return the parent directory of *path* using POSIX semantics."""
        parent = posixpath.dirname(path.rstrip("/"))
        return parent or "/"

    @staticmethod
    def _basename(path: str) -> str:
        return posixpath.basename(path.rstrip("/")) or "/"

    # ── Readiness gate ─────────────────────────────────────────────────────

    async def _assert_ready(self, context) -> bool:
        if self._ready.is_set():
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=5.0)
            return True
        except asyncio.TimeoutError:
            registered = set(self._registered_dns)
            missing    = set(self._expected_dns) - registered
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"Master not ready — waiting for DataNodes: {sorted(missing)}",
            )
            return False

    # ── DataNode registration ──────────────────────────────────────────────

    async def RegisterDataNode(
        self,
        request: master_pb2.RegisterDNRequest,
        context,
    ) -> master_pb2.RegisterDNResponse:
        dn_id = request.datanode_id
        if dn_id not in self._expected_dns:
            return master_pb2.RegisterDNResponse(
                ok=False, error=f"Unknown DataNode: {dn_id}"
            )

        async with self._lock:
            now = time.monotonic()
            is_new = dn_id not in self._registered_dns
            self._registered_dns[dn_id] = {
                "host":      request.host,
                "port":      request.port,
                "last_seen": now,
            }
            self._addr_to_dn[f"{request.host}:{request.port}"] = dn_id
            if is_new:
                logger.info(
                    "DataNode %s registered (%s:%d)", dn_id, request.host, request.port
                )
            else:
                logger.debug(
                    "DataNode %s heartbeat (%s:%d)", dn_id, request.host, request.port
                )
            if set(self._registered_dns) >= set(self._expected_dns):
                if not self._ready.is_set():
                    self._ready.set()
                    logger.info("All DataNodes registered — MasterNode is ready")

        return master_pb2.RegisterDNResponse(ok=True)

    # ── Raft leader notifications ──────────────────────────────────────────

    async def NotifyLeader(
        self,
        request: master_pb2.NotifyLeaderRequest,
        context,
    ) -> master_pb2.NotifyLeaderResponse:
        block_id  = request.block_id
        # leader_id from RaftNode is "host:port"; translate to dn_id.
        leader_id = self._addr_to_dn.get(request.leader_id, request.leader_id)
        term      = request.term

        async with self._lock:
            existing = self._block_status.get(block_id, {})
            # Accept only if the term is newer (or first notification).
            if term >= existing.get("term", -1):
                self._block_status[block_id] = {
                    "leader_id": leader_id,
                    "term":      term,
                    "last_hb":   time.monotonic(),
                    "available": True,
                }
                # Also update the persisted leader_id in block metadata.
                for file_meta in self._files.values():
                    for blk in file_meta["blocks"]:
                        if blk["block_id"] == block_id:
                            blk["leader_id"] = leader_id
                            break
                self._save_metadata()
                logger.info(
                    "NotifyLeader: block %.8s leader=%s term=%d",
                    block_id, leader_id, term,
                )

        return master_pb2.NotifyLeaderResponse(ok=True)

    async def BlockHeartbeat(
        self,
        request: master_pb2.BlockHeartbeatRequest,
        context,
    ) -> master_pb2.BlockHeartbeatResponse:
        block_id  = request.block_id
        leader_id = self._addr_to_dn.get(request.leader_id, request.leader_id)
        term      = request.term

        async with self._lock:
            status = self._block_status.get(block_id)
            if status and status["leader_id"] == leader_id and term >= status["term"]:
                status["last_hb"]   = time.monotonic()
                status["available"] = True

        return master_pb2.BlockHeartbeatResponse(ok=True)

    # ── File namespace operations ──────────────────────────────────────────

    async def CreateFile(
        self,
        request: master_pb2.CreateFileRequest,
        context,
    ) -> master_pb2.CreateFileResponse:
        if not await self._assert_ready(context):
            return master_pb2.CreateFileResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path
            if path in self._files:
                return master_pb2.CreateFileResponse(
                    ok=False, error=f"File already exists: {path}"
                )
            if path in self._dirs:
                return master_pb2.CreateFileResponse(
                    ok=False, error=f"Path is a directory: {path}"
                )
            parent = self._parent(path)
            if parent not in self._dirs:
                return master_pb2.CreateFileResponse(
                    ok=False,
                    error=f"Parent directory does not exist: {parent}",
                )
            self._files[path] = {"blocks": []}
            self._save_metadata()
            logger.info("CreateFile: %s", path)
            return master_pb2.CreateFileResponse(ok=True)

    # ── Directory operations ───────────────────────────────────────────────

    async def Mkdir(
        self,
        request: master_pb2.MkdirRequest,
        context,
    ) -> master_pb2.MkdirResponse:
        if not await self._assert_ready(context):
            return master_pb2.MkdirResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path.rstrip("/") or "/"
            if path == "/":
                return master_pb2.MkdirResponse(ok=False, error="Root already exists")
            if path in self._dirs:
                return master_pb2.MkdirResponse(
                    ok=False, error=f"Directory already exists: {path}"
                )
            if path in self._files:
                return master_pb2.MkdirResponse(
                    ok=False, error=f"Path is a file: {path}"
                )
            parent = self._parent(path)
            if parent not in self._dirs:
                return master_pb2.MkdirResponse(
                    ok=False,
                    error=f"Parent directory does not exist: {parent}",
                )
            self._dirs.add(path)
            self._save_metadata()
            logger.info("Mkdir: %s", path)
            return master_pb2.MkdirResponse(ok=True)

    async def Rmdir(
        self,
        request: master_pb2.RmdirRequest,
        context,
    ) -> master_pb2.RmdirResponse:
        if not await self._assert_ready(context):
            return master_pb2.RmdirResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path.rstrip("/") or "/"
            if path == "/":
                return master_pb2.RmdirResponse(ok=False, error="Cannot remove root")
            if path not in self._dirs:
                if path in self._files:
                    return master_pb2.RmdirResponse(
                        ok=False, error=f"Not a directory: {path}"
                    )
                return master_pb2.RmdirResponse(
                    ok=False, error=f"Directory not found: {path}"
                )
            # Fail if any file lives directly inside this directory.
            prefix = path + "/"
            for fpath in self._files:
                if fpath.startswith(prefix):
                    return master_pb2.RmdirResponse(
                        ok=False, error=f"Directory not empty: {path}"
                    )
            # Fail if any subdirectory lives directly inside.
            for dpath in self._dirs:
                if dpath != path and dpath.startswith(prefix):
                    return master_pb2.RmdirResponse(
                        ok=False, error=f"Directory not empty: {path}"
                    )
            self._dirs.discard(path)
            self._save_metadata()
            logger.info("Rmdir: %s", path)
            return master_pb2.RmdirResponse(ok=True)

    async def Stat(
        self,
        request: master_pb2.StatRequest,
        context,
    ) -> master_pb2.StatResponse:
        if not await self._assert_ready(context):
            return master_pb2.StatResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path
            name = self._basename(path)
            if path in self._dirs or path.rstrip("/") == "":
                return master_pb2.StatResponse(
                    ok=True,
                    entry=master_pb2.StatEntry(
                        type=master_pb2.StatEntry.DIR,
                        name=name,
                        num_blocks=0,
                    ),
                )
            if path in self._files:
                num_blocks = len(self._files[path]["blocks"])
                return master_pb2.StatResponse(
                    ok=True,
                    entry=master_pb2.StatEntry(
                        type=master_pb2.StatEntry.FILE,
                        name=name,
                        num_blocks=num_blocks,
                    ),
                )
            return master_pb2.StatResponse(
                ok=False, error=f"No such file or directory: {path}"
            )

    async def ListDir(
        self,
        request: master_pb2.ListDirRequest,
        context,
    ) -> master_pb2.ListDirResponse:
        if not await self._assert_ready(context):
            return master_pb2.ListDirResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path.rstrip("/") or "/"
            if path not in self._dirs:
                if path in self._files:
                    return master_pb2.ListDirResponse(
                        ok=False, error=f"Not a directory: {path}"
                    )
                return master_pb2.ListDirResponse(
                    ok=False, error=f"Directory not found: {path}"
                )

            entries: List[master_pb2.StatEntry] = []

            # Immediate subdirectories
            for dpath in sorted(self._dirs):
                if dpath == path:
                    continue
                parent = self._parent(dpath)
                if parent == path:
                    entries.append(master_pb2.StatEntry(
                        type=master_pb2.StatEntry.DIR,
                        name=self._basename(dpath),
                        num_blocks=0,
                    ))

            # Immediate files
            for fpath in sorted(self._files):
                if self._parent(fpath) == path:
                    entries.append(master_pb2.StatEntry(
                        type=master_pb2.StatEntry.FILE,
                        name=self._basename(fpath),
                        num_blocks=len(self._files[fpath]["blocks"]),
                    ))

            return master_pb2.ListDirResponse(ok=True, entries=entries)

    async def DeleteFile(
        self,
        request: master_pb2.DeleteFileRequest,
        context,
    ) -> master_pb2.DeleteFileResponse:
        if not await self._assert_ready(context):
            return master_pb2.DeleteFileResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path
            if path not in self._files:
                return master_pb2.DeleteFileResponse(
                    ok=False, error=f"File not found: {path}"
                )
            file_meta = self._files.pop(path)
            self._save_metadata()
            logger.info(
                "DeleteFile: %s (%d block(s))", path, len(file_meta["blocks"])
            )

        # Enqueue deletions for all blocks durably, then signal the retry loop.
        async with self._lock:
            for blk in file_meta["blocks"]:
                block_id = blk["block_id"]
                peers    = [p for p in blk.get("peers", [blk.get("datanode_id")]) if p]
                self._block_status.pop(block_id, None)
                self._delete_queue.append({
                    "block_id":      block_id,
                    "pending_peers": list(peers),
                })
            self._save_metadata()

        self._delete_event.set()
        return master_pb2.DeleteFileResponse(ok=True)

    async def _deletion_loop(self) -> None:
        """Background loop that drains the persistent deletion queue.

        Retries pending DeleteBlock RPCs with exponential backoff.  An entry
        is removed from the queue only after every peer confirms deletion.
        Survives master restarts because the queue is persisted to disk.
        """
        _MAX_BACKOFF = 60.0
        backoff = 2.0

        while True:
            # Wait until there is work or until the backoff expires.
            try:
                await asyncio.wait_for(self._delete_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            self._delete_event.clear()

            async with self._lock:
                if not self._delete_queue:
                    backoff = 2.0
                    continue
                # Snapshot the queue so we can release the lock during RPCs.
                snapshot = [dict(e) for e in self._delete_queue]

            any_failure = False
            for entry in snapshot:
                block_id      = entry["block_id"]
                still_pending = list(entry["pending_peers"])

                for dn_id in list(still_pending):
                    dn_info = self._registered_dns.get(dn_id)
                    if not dn_info:
                        logger.debug(
                            "deletion[%.8s]: %s not registered yet — will retry",
                            block_id, dn_id,
                        )
                        any_failure = True
                        continue
                    try:
                        async with aio.insecure_channel(
                            f"{dn_info['host']}:{dn_info['port']}",
                            options=_GRPC_OPTIONS,
                        ) as ch:
                            stub = datanode_pb2_grpc.DataNodeStub(ch)
                            resp = await stub.DeleteBlock(
                                datanode_pb2.DeleteBlockRequest(block_id=block_id)
                            )
                        if resp.ok:
                            still_pending.remove(dn_id)
                            logger.info(
                                "deletion[%.8s]: confirmed on %s", block_id, dn_id
                            )
                        else:
                            logger.warning(
                                "deletion[%.8s]: %s rejected: %s",
                                block_id, dn_id, resp.error,
                            )
                            any_failure = True
                    except Exception as exc:
                        logger.warning(
                            "deletion[%.8s]: cannot reach %s: %s — will retry",
                            block_id, dn_id, exc,
                        )
                        any_failure = True

                entry["pending_peers"] = still_pending

            # Write updated queue back; drop fully-confirmed entries.
            async with self._lock:
                # Merge results back: update pending_peers in the live queue.
                by_id = {e["block_id"]: e for e in snapshot}
                self._delete_queue = [
                    {**q, "pending_peers": by_id[q["block_id"]]["pending_peers"]}
                    if q["block_id"] in by_id else q
                    for q in self._delete_queue
                ]
                before = len(self._delete_queue)
                self._delete_queue = [
                    e for e in self._delete_queue if e["pending_peers"]
                ]
                completed = before - len(self._delete_queue)
                if completed:
                    logger.info("deletion loop: %d block(s) fully deleted", completed)
                self._save_metadata()

            backoff = min(_MAX_BACKOFF, backoff * 2) if any_failure else 2.0

    # ── Block routing ──────────────────────────────────────────────────────

    async def GetBlockInfo(
        self,
        request: master_pb2.GetBlockInfoRequest,
        context,
    ) -> master_pb2.GetBlockInfoResponse:
        if not await self._assert_ready(context):
            return master_pb2.GetBlockInfoResponse(ok=False, error="Master not ready")

        path        = request.path
        block_index = request.block_index
        is_write    = (request.intent == master_pb2.Intent.WRITE)

        async with self._lock:
            if path not in self._files:
                return master_pb2.GetBlockInfoResponse(
                    ok=False, error=f"File not found: {path}"
                )

            file_meta  = self._files[path]
            num_blocks = len(file_meta["blocks"])

            if is_write and block_index == num_blocks:
                # ── Allocate a new block ───────────────────────────────────
                avail = self._available_dns()
                if len(avail) < self._replication_factor:
                    return master_pb2.GetBlockInfoResponse(
                        ok=False,
                        error=(
                            f"Cannot allocate block: only {len(avail)} DataNode(s) "
                            f"available, need {self._replication_factor}"
                        ),
                    )

                group     = self._pick_group(avail, self._replication_factor)
                block_id  = str(uuid.uuid4())
                leader_dn = group[0]   # master designates the first as initial leader

                # Build the "host:port" address list for the Raft group.
                peer_addrs = [
                    f"{self._registered_dns[dn_id]['host']}:{self._registered_dns[dn_id]['port']}"
                    for dn_id in group
                ]

                # Call RegisterBlock on every group member.
                ok, err = await self._register_block_on_group(
                    block_id, group, peer_addrs, leader_dn
                )
                if not ok:
                    return master_pb2.GetBlockInfoResponse(ok=False, error=err)

                file_meta["blocks"].append({
                    "block_id":  block_id,
                    "peers":     group,
                    "leader_id": leader_dn,
                })
                self._save_metadata()

                # Optimistically mark the block as available with the
                # designated initial leader (updated by NotifyLeader shortly).
                self._block_status[block_id] = {
                    "leader_id": leader_dn,
                    "term":      1,
                    "last_hb":   time.monotonic(),
                    "available": True,
                }

                logger.info(
                    "Allocated block[%d] %s group=%s leader=%s for %s",
                    block_index, block_id[:8], group, leader_dn, path,
                )

            elif is_write and block_index > num_blocks:
                return master_pb2.GetBlockInfoResponse(
                    ok=False,
                    error=(
                        f"Block index {block_index} skips ahead "
                        f"(file has {num_blocks} block(s) — allocate sequentially)"
                    ),
                )
            else:
                # READ or existing block WRITE
                if block_index >= num_blocks:
                    return master_pb2.GetBlockInfoResponse(
                        ok=False,
                        error=(
                            f"Block index {block_index} out of range "
                            f"(file has {num_blocks} block(s))"
                        ),
                    )

            blk       = file_meta["blocks"][block_index]
            block_id  = blk["block_id"]
            leader_dn = blk.get("leader_id") or blk.get("datanode_id")

            # Check cluster availability.
            status = self._block_status.get(block_id)
            if status:
                if not status["available"]:
                    return master_pb2.GetBlockInfoResponse(
                        ok=False,
                        error=(
                            f"Block {block_id[:8]} cluster unavailable "
                            f"(no heartbeat from leader {status['leader_id']})"
                        ),
                    )
                # Use the most up-to-date leader from live notifications.
                leader_dn = status["leader_id"]

            dn_info = self._registered_dns.get(leader_dn)
            if not dn_info:
                return master_pb2.GetBlockInfoResponse(
                    ok=False,
                    error=f"Leader DataNode {leader_dn} not registered",
                )

            # min_leader_term is one below the last known election term.
            # A DataNode must have current_term > min_leader_term to serve
            # the request, which means it must be at term >= last_known_term.
            # This rejects isolated leaders that were superseded by a newer
            # election the Master already knows about.
            last_known_term = status["term"] if status else 1
            min_leader_term = max(0, last_known_term - 1)

            return master_pb2.GetBlockInfoResponse(
                ok=True,
                block=master_pb2.BlockInfo(
                    block_id=block_id,
                    datanode_id=leader_dn,
                    datanode_host=dn_info["host"],
                    datanode_port=dn_info["port"],
                    min_leader_term=min_leader_term,
                ),
            )

    # ── Block allocation helpers ───────────────────────────────────────────

    def _available_dns(self) -> List[str]:
        """Return IDs of DataNodes that have sent a heartbeat recently enough
        to be considered alive for new block allocation."""
        cutoff = time.monotonic() - self._dn_liveness_timeout
        live   = []
        for dn_id, info in self._registered_dns.items():
            last_seen = info.get("last_seen", 0)
            if last_seen >= cutoff:
                live.append(dn_id)
            else:
                logger.debug(
                    "DataNode %s excluded from allocation (last seen %.1fs ago)",
                    dn_id, time.monotonic() - last_seen,
                )
        return sorted(live)

    def _pick_group(self, avail: List[str], size: int) -> List[str]:
        """Pick *size* DataNodes using round-robin, return as ordered list."""
        group = []
        for _ in range(size):
            dn_id = avail[self._rr_counter % len(avail)]
            self._rr_counter += 1
            if dn_id not in group:
                group.append(dn_id)
            else:
                # Skip duplicates by looking further
                for dn in avail:
                    if dn not in group:
                        group.append(dn)
                        break
        return group[:size]

    async def _register_block_on_group(
        self,
        block_id: str,
        group: List[str],
        peer_addrs: List[str],
        leader_dn: str,
    ) -> tuple:
        """Call RegisterBlock on every DataNode in the group.

        Returns (ok, error_string).
        """
        for dn_id in group:
            dn_info   = self._registered_dns[dn_id]
            is_leader = (dn_id == leader_dn)
            try:
                async with aio.insecure_channel(
                    f"{dn_info['host']}:{dn_info['port']}",
                    options=_GRPC_OPTIONS,
                ) as ch:
                    stub = datanode_pb2_grpc.DataNodeStub(ch)
                    resp = await stub.RegisterBlock(
                        datanode_pb2.RegisterBlockRequest(
                            block_id=block_id,
                            peer_addresses=peer_addrs,
                            is_initial_leader=is_leader,
                        )
                    )
                if not resp.ok:
                    return False, (
                        f"DataNode {dn_id} rejected RegisterBlock: {resp.error}"
                    )
            except Exception as exc:
                return False, f"Could not reach DataNode {dn_id}: {exc}"
        return True, ""

    # ── Heartbeat watchdog ─────────────────────────────────────────────────

    async def _heartbeat_watchdog(self) -> None:
        """Periodically mark block clusters as unavailable on leader timeout."""
        while True:
            await asyncio.sleep(self._hb_timeout / 3)
            now = time.monotonic()
            async with self._lock:
                for block_id, status in self._block_status.items():
                    if status["available"]:
                        age = now - status["last_hb"]
                        if age > self._hb_timeout:
                            status["available"] = False
                            logger.warning(
                                "block[%.8s] cluster UNAVAILABLE "
                                "(no heartbeat from %s for %.1fs)",
                                block_id, status["leader_id"], age,
                            )


# ── Server bootstrap ───────────────────────────────────────────────────────

async def serve(config: dict) -> None:
    servicer = MasterNodeServicer(config)
    server   = aio.server(options=_GRPC_OPTIONS)
    master_pb2_grpc.add_MasterNodeServicer_to_server(servicer, server)

    host = config["master"]["host"]
    port = config["master"]["port"]
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    logger.info("MasterNode listening on %s:%d", host, port)

    # Start background tasks.
    asyncio.create_task(servicer._heartbeat_watchdog())
    asyncio.create_task(servicer._deletion_loop())
    if servicer._delete_queue:
        logger.info(
            "Resuming %d pending block deletion(s) from previous run",
            len(servicer._delete_queue),
        )
        servicer._delete_event.set()

    await server.wait_for_termination()
