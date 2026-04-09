"""DataNode gRPC server.

Stores block data as individual files under a configurable data directory.
Maintains a persisted manifest of block IDs it owns so it can reject
requests for foreign blocks.

Phase-2 note: Replace this module with a Raft-group implementation that
exposes the same DataNode service.  The MasterNode and client are unchanged.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Set

import grpc
from grpc import aio

from dfs.proto import datanode_pb2, datanode_pb2_grpc
from dfs.proto import master_pb2, master_pb2_grpc

logger = logging.getLogger(__name__)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]

_HEARTBEAT_INTERVAL = 10  # seconds


class DataNodeServicer(datanode_pb2_grpc.DataNodeServicer):
    def __init__(self, dn_id: str, config: dict) -> None:
        self._dn_id = dn_id
        dn_conf = next(dn for dn in config["datanodes"] if dn["id"] == dn_id)
        self._data_dir = Path(dn_conf["data_dir"])
        self._data_dir.mkdir(parents=True, exist_ok=True)

        self._lock = asyncio.Lock()

        # Owned block IDs — authoritative list of blocks this node may serve.
        self._owned: Set[str] = self._load_manifest()
        logger.info(
            "DataNode %s: %d block(s) in manifest at startup", dn_id, len(self._owned)
        )

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
        """Atomic write via rename."""
        tmp = self._manifest_path().with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump(sorted(self._owned), fh, indent=2)
        tmp.rename(self._manifest_path())

    # ── Block path ─────────────────────────────────────────────────────────

    def _block_path(self, block_id: str) -> Path:
        return self._data_dir / block_id

    # ── Block lifecycle (called by MasterNode) ─────────────────────────────

    async def RegisterBlock(
        self, request: datanode_pb2.RegisterBlockRequest, context
    ) -> datanode_pb2.RegisterBlockResponse:
        block_id = request.block_id
        async with self._lock:
            self._owned.add(block_id)
            self._save_manifest()
        logger.debug("RegisterBlock: %s", block_id)
        return datanode_pb2.RegisterBlockResponse(ok=True)

    async def DeleteBlock(
        self, request: datanode_pb2.DeleteBlockRequest, context
    ) -> datanode_pb2.DeleteBlockResponse:
        block_id = request.block_id
        async with self._lock:
            self._owned.discard(block_id)
            self._save_manifest()
            p = self._block_path(block_id)
            if p.exists():
                p.unlink()
        logger.info("DeleteBlock: %s", block_id)
        return datanode_pb2.DeleteBlockResponse(ok=True)

    # ── Data plane (called by DFS client) ──────────────────────────────────

    async def ReadBlock(
        self, request: datanode_pb2.ReadBlockRequest, context
    ) -> datanode_pb2.ReadBlockResponse:
        block_id = request.block_id

        if block_id not in self._owned:
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"Block {block_id} not owned by DataNode {self._dn_id}",
            )
            return datanode_pb2.ReadBlockResponse(ok=False, error="Not owned")

        p = self._block_path(block_id)
        if not p.exists():
            # Registered but no data written yet.
            return datanode_pb2.ReadBlockResponse(ok=True, data=b"")

        try:
            # intra_block_offset is the byte position *within this block file*,
            # not a file-level offset.  The client computed it as:
            #   intra_block_offset = file_byte_offset % block_size
            with open(p, "rb") as fh:
                fh.seek(request.intra_block_offset)
                data = fh.read(request.length)
            return datanode_pb2.ReadBlockResponse(ok=True, data=data)
        except Exception as exc:
            logger.error("ReadBlock %s error: %s", block_id, exc)
            return datanode_pb2.ReadBlockResponse(ok=False, error=str(exc))

    async def WriteBlock(
        self, request: datanode_pb2.WriteBlockRequest, context
    ) -> datanode_pb2.WriteBlockResponse:
        block_id = request.block_id

        if block_id not in self._owned:
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"Block {block_id} not owned by DataNode {self._dn_id}",
            )
            return datanode_pb2.WriteBlockResponse(ok=False, error="Not owned")

        p = self._block_path(block_id)
        # intra_block_offset is the byte position *within this block file*.
        intra = request.intra_block_offset
        data = request.data

        try:
            # Open for update if the file already exists, otherwise create.
            mode = "r+b" if p.exists() else "wb"
            with open(p, mode) as fh:
                # Pad with zeros if writing past current EOF (sparse-free).
                fh.seek(0, 2)
                current_size = fh.tell()
                if intra > current_size:
                    fh.write(b"\x00" * (intra - current_size))
                fh.seek(intra)
                fh.write(data)
            return datanode_pb2.WriteBlockResponse(ok=True)
        except Exception as exc:
            logger.error("WriteBlock %s error: %s", block_id, exc)
            return datanode_pb2.WriteBlockResponse(ok=False, error=str(exc))


# ── Registration / heartbeat ───────────────────────────────────────────────

async def _register_loop(
    dn_id: str,
    dn_host: str,
    dn_port: int,
    master_host: str,
    master_port: int,
) -> None:
    """Register with the MasterNode, retrying until successful.
    Then keep a periodic heartbeat so the Master recovers after a restart.
    """
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
    server = aio.server(options=_GRPC_OPTIONS)
    datanode_pb2_grpc.add_DataNodeServicer_to_server(servicer, server)

    host = dn_conf["host"]
    port = dn_conf["port"]
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    logger.info("DataNode %s listening on %s:%d", dn_id, host, port)

    master_host = config["master"]["host"]
    master_port = config["master"]["port"]

    # Fire-and-forget: register + heartbeat in background.
    asyncio.create_task(
        _register_loop(dn_id, host, port, master_host, master_port)
    )

    await server.wait_for_termination()
