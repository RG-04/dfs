"""DataNode gRPC server — Phase 2 (Raft-replicated, persistent).

Startup / recovery
──────────────────
On every start the DataNode loads its block manifest and re-creates a
RaftNode (in FOLLOWER state) for every block that has a persisted Raft
state file.  This allows the node to re-join an active Raft group after
a crash or a clean restart without requiring any MasterNode intervention.

New-block path (RegisterBlock)
──────────────────────────────
The MasterNode calls RegisterBlock exactly once per block (at allocation
time).  The DataNode creates a fresh RaftNode, persists the initial
state, and starts it.

RPC surface
───────────
  • RegisterBlock / DeleteBlock   — MasterNode lifecycle calls
  • ReadBlock / WriteBlock        — DFS client (leader-only)
  • RequestVote / AppendEntries   — Raft peer (DataNode ↔ DataNode)
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Set

import grpc
from grpc import aio

from dfs.proto import datanode_pb2, datanode_pb2_grpc
from dfs.proto import master_pb2, master_pb2_grpc
from dfs.datanode.raft import RaftNode

logger = logging.getLogger(__name__)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]

_HEARTBEAT_INTERVAL = 10   # seconds — registration heartbeat to master


class DataNodeServicer(datanode_pb2_grpc.DataNodeServicer):
    def __init__(self, dn_id: str, config: dict) -> None:
        self._dn_id  = dn_id
        dn_conf      = next(dn for dn in config["datanodes"] if dn["id"] == dn_id)

        self._host      = dn_conf["host"]
        self._port      = dn_conf["port"]
        self._node_addr = f"{self._host}:{self._port}"
        self._data_dir  = Path(dn_conf["data_dir"])
        self._data_dir.mkdir(parents=True, exist_ok=True)

        master_cfg        = config["master"]
        self._master_addr = f"{master_cfg['host']}:{master_cfg['port']}"

        self._lock = asyncio.Lock()

        self._owned:      Set[str]          = self._load_manifest()
        self._raft_nodes: Dict[str, RaftNode] = {}

        logger.info(
            "DataNode %s: %d block(s) in manifest at startup",
            dn_id, len(self._owned),
        )

    # ── Startup recovery ───────────────────────────────────────────────────

    async def startup(self) -> None:
        """Re-create RaftNodes for all blocks that have persisted Raft state.

        Called from serve() after the gRPC server is up so that
        asyncio.create_task() is available.
        """
        recovered = 0
        async with self._lock:
            for block_id in list(self._owned):
                if block_id in self._raft_nodes:
                    continue
                state_file = self._data_dir / f"raft_{block_id}.json"
                if not state_file.exists():
                    logger.warning(
                        "DataNode %s: block %.8s in manifest but no Raft state "
                        "— skipping (will be registered by master on next allocation)",
                        self._dn_id, block_id,
                    )
                    continue

                raft = self._make_raft_node(block_id, peers=[], is_initial_leader=False)
                self._raft_nodes[block_id] = raft
                recovered += 1

        if recovered:
            logger.info(
                "DataNode %s: starting %d recovered RaftNode(s)", self._dn_id, recovered
            )
            for block_id in list(self._raft_nodes):
                self._raft_nodes[block_id].start()

    # ── Manifest persistence ───────────────────────────────────────────────

    def _manifest_path(self) -> Path:
        return self._data_dir / "manifest.json"

    def _load_manifest(self) -> Set[str]:
        p = self._manifest_path()
        if p.exists():
            with open(p) as fh:
                return set(json.load(fh))
        return set()

    def _save_manifest(self) -> None:
        tmp = self._manifest_path().with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump(sorted(self._owned), fh, indent=2)
        tmp.rename(self._manifest_path())

    # ── Block paths ────────────────────────────────────────────────────────

    def _block_path(self, block_id: str) -> Path:
        return self._data_dir / block_id

    # ── Apply function (passed to RaftNode) ────────────────────────────────

    async def _apply_write(
        self, block_id: str, intra_offset: int, data: bytes
    ) -> None:
        p    = self._block_path(block_id)
        mode = "r+b" if p.exists() else "wb"
        with open(p, mode) as fh:
            fh.seek(0, 2)
            current_size = fh.tell()
            if intra_offset > current_size:
                fh.write(b"\x00" * (intra_offset - current_size))
            fh.seek(intra_offset)
            fh.write(data)

    # ── RaftNode factory ───────────────────────────────────────────────────

    def _make_raft_node(
        self,
        block_id: str,
        peers: list,
        is_initial_leader: bool,
    ) -> RaftNode:
        async def _apply(intra_offset: int, data: bytes) -> None:
            await self._apply_write(block_id, intra_offset, data)

        return RaftNode(
            block_id=block_id,
            node_id=self._node_addr,
            state_dir=self._data_dir,
            peers=peers,
            is_initial_leader=is_initial_leader,
            master_addr=self._master_addr,
            apply_fn=_apply,
        )

    # ── Block lifecycle ────────────────────────────────────────────────────

    async def RegisterBlock(
        self,
        request: datanode_pb2.RegisterBlockRequest,
        context,
    ) -> datanode_pb2.RegisterBlockResponse:
        block_id   = request.block_id
        peer_addrs = list(request.peer_addresses)
        is_leader  = request.is_initial_leader

        # Peers for THIS node = all group members excluding itself.
        peers = [p for p in peer_addrs if p != self._node_addr]

        async with self._lock:
            self._owned.add(block_id)
            self._save_manifest()

            # Stop any pre-existing Raft node (e.g., stale state from a
            # previous incarnation of this block).
            old = self._raft_nodes.pop(block_id, None)
            if old:
                logger.warning(
                    "DataNode %s: RegisterBlock %.8s — stopping existing RaftNode",
                    self._dn_id, block_id,
                )
                old.stop()

            raft = self._make_raft_node(block_id, peers=peers,
                                        is_initial_leader=is_leader)
            self._raft_nodes[block_id] = raft

        raft.start()
        logger.info(
            "DataNode %s: RegisterBlock %.8s  leader=%s  peers=%s",
            self._dn_id, block_id[:8], is_leader, peers,
        )
        return datanode_pb2.RegisterBlockResponse(ok=True)

    async def DeleteBlock(
        self,
        request: datanode_pb2.DeleteBlockRequest,
        context,
    ) -> datanode_pb2.DeleteBlockResponse:
        block_id = request.block_id

        async with self._lock:
            raft = self._raft_nodes.pop(block_id, None)

        if raft:
            raft.stop()

        async with self._lock:
            self._owned.discard(block_id)
            self._save_manifest()

            # Remove block data file.
            p = self._block_path(block_id)
            if p.exists():
                p.unlink()

            # Remove Raft state and log files.
            for suffix in (".json", ".log", ".json.tmp", ".log.tmp"):
                rp = self._data_dir / f"raft_{block_id}{suffix}"
                if rp.exists():
                    rp.unlink()

        logger.info("DataNode %s: DeleteBlock %.8s", self._dn_id, block_id[:8])
        return datanode_pb2.DeleteBlockResponse(ok=True)

    # ── Data plane ─────────────────────────────────────────────────────────

    async def WriteBlock(
        self,
        request: datanode_pb2.WriteBlockRequest,
        context,
    ) -> datanode_pb2.WriteBlockResponse:
        block_id = request.block_id

        async with self._lock:
            if block_id not in self._owned:
                await context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    f"Block {block_id} not owned by DataNode {self._dn_id}",
                )
                return datanode_pb2.WriteBlockResponse(ok=False, error="Not owned")
            raft = self._raft_nodes.get(block_id)

        if raft is None:
            return datanode_pb2.WriteBlockResponse(
                ok=False, error="Raft node not initialised for block"
            )

        if not raft.is_leader():
            return datanode_pb2.WriteBlockResponse(
                ok=False,
                error=f"not leader (leader={raft.leader_id})",
            )

        ok, err = await raft.append_and_replicate(
            request.intra_block_offset, request.data
        )
        if not ok:
            return datanode_pb2.WriteBlockResponse(ok=False, error=err)
        return datanode_pb2.WriteBlockResponse(ok=True)

    async def ReadBlock(
        self,
        request: datanode_pb2.ReadBlockRequest,
        context,
    ) -> datanode_pb2.ReadBlockResponse:
        block_id = request.block_id

        async with self._lock:
            if block_id not in self._owned:
                await context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    f"Block {block_id} not owned by DataNode {self._dn_id}",
                )
                return datanode_pb2.ReadBlockResponse(ok=False, error="Not owned")
            raft = self._raft_nodes.get(block_id)

        # Only the leader serves reads (sequential consistency).
        if raft and not raft.is_leader():
            return datanode_pb2.ReadBlockResponse(
                ok=False,
                error=f"not leader (leader={raft.leader_id})",
            )

        p = self._block_path(block_id)
        if not p.exists():
            return datanode_pb2.ReadBlockResponse(ok=True, data=b"")

        try:
            with open(p, "rb") as fh:
                fh.seek(request.intra_block_offset)
                data = fh.read(request.length)
            return datanode_pb2.ReadBlockResponse(ok=True, data=data)
        except Exception as exc:
            logger.error("ReadBlock %s error: %s", block_id, exc)
            return datanode_pb2.ReadBlockResponse(ok=False, error=str(exc))

    # ── Raft peer RPCs ─────────────────────────────────────────────────────

    async def RequestVote(
        self,
        request: datanode_pb2.VoteRequest,
        context,
    ) -> datanode_pb2.VoteResponse:
        async with self._lock:
            raft = self._raft_nodes.get(request.block_id)
        if raft is None:
            return datanode_pb2.VoteResponse(term=0, vote_granted=False)
        term, granted = await raft.handle_request_vote(
            request.term,
            request.candidate_id,
            request.last_log_index,
            request.last_log_term,
        )
        return datanode_pb2.VoteResponse(term=term, vote_granted=granted)

    async def AppendEntries(
        self,
        request: datanode_pb2.AppendEntriesRequest,
        context,
    ) -> datanode_pb2.AppendEntriesResponse:
        async with self._lock:
            raft = self._raft_nodes.get(request.block_id)
        if raft is None:
            return datanode_pb2.AppendEntriesResponse(
                term=0, success=False, match_index=0
            )
        entries = [
            (e.term, e.intra_block_offset, e.data) for e in request.entries
        ]
        term, success, match_idx = await raft.handle_append_entries(
            request.term,
            request.leader_id,
            request.prev_log_index,
            request.prev_log_term,
            entries,
            request.leader_commit,
        )
        return datanode_pb2.AppendEntriesResponse(
            term=term, success=success, match_index=match_idx
        )


# ── Registration / heartbeat ───────────────────────────────────────────────

async def _register_loop(
    dn_id: str,
    dn_host: str,
    dn_port: int,
    master_host: str,
    master_port: int,
) -> None:
    first = True
    while True:
        try:
            async with aio.insecure_channel(
                f"{master_host}:{master_port}", options=_GRPC_OPTIONS
            ) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                resp = await stub.RegisterDataNode(
                    master_pb2.RegisterDNRequest(
                        datanode_id=dn_id,
                        host=dn_host,
                        port=dn_port,
                    )
                )
            if resp.ok:
                if first:
                    logger.info(
                        "DataNode %s registered with Master at %s:%d",
                        dn_id, master_host, master_port,
                    )
                    first = False
            else:
                logger.warning("Registration rejected: %s", resp.error)
        except Exception as exc:
            logger.warning(
                "Cannot reach Master (%s:%d): %s — retrying in %ds",
                master_host, master_port, exc, _HEARTBEAT_INTERVAL,
            )

        await asyncio.sleep(_HEARTBEAT_INTERVAL)


# ── Server bootstrap ───────────────────────────────────────────────────────

async def serve(dn_id: str, config: dict) -> None:
    dn_conf = next(dn for dn in config["datanodes"] if dn["id"] == dn_id)

    servicer = DataNodeServicer(dn_id, config)
    server   = aio.server(options=_GRPC_OPTIONS)
    datanode_pb2_grpc.add_DataNodeServicer_to_server(servicer, server)

    host = dn_conf["host"]
    port = dn_conf["port"]
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    logger.info("DataNode %s listening on %s:%d", dn_id, host, port)

    # Re-create RaftNodes for blocks that survived a previous crash/restart.
    await servicer.startup()

    master_host = config["master"]["host"]
    master_port = config["master"]["port"]

    asyncio.create_task(
        _register_loop(dn_id, host, port, master_host, master_port)
    )

    await server.wait_for_termination()
