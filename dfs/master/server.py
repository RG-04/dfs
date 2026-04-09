"""MasterNode gRPC server.

Single source of truth for namespace and block→DataNode mapping.
Waits for every configured DataNode to register before accepting client
requests.  All metadata is written atomically to disk so the node
survives restarts.

Phase-2 note: The MasterNode will require changes to track the current
leader of each Raft group and route block operations accordingly.
"""

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Dict

import grpc
from grpc import aio

from dfs.proto import master_pb2, master_pb2_grpc
from dfs.proto import datanode_pb2, datanode_pb2_grpc

logger = logging.getLogger(__name__)

# gRPC channel/server options — allow blocks up to 128 MiB per message.
_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]


class MasterNodeServicer(master_pb2_grpc.MasterNodeServicer):
    def __init__(self, config: dict) -> None:
        self._block_size: int = config["block_size"]

        # Metadata persistence
        meta_dir = Path(config["master"]["metadata_dir"])
        meta_dir.mkdir(parents=True, exist_ok=True)
        self._meta_file = meta_dir / "metadata.json"

        # Expected DataNodes from static config
        self._expected_dns: Dict[str, dict] = {
            dn["id"]: dn for dn in config["datanodes"]
        }

        # Runtime registration state
        self._registered_dns: Dict[str, dict] = {}  # id → {host, port}
        self._ready = asyncio.Event()
        self._lock = asyncio.Lock()

        # Global round-robin counter for DataNode assignment
        self._rr_counter: int = 0

        # File namespace: path → {"blocks": [{"block_id": str, "datanode_id": str}]}
        self._files: Dict[str, dict] = {}
        self._load_metadata()

    # ── Persistence ────────────────────────────────────────────────────────

    def _load_metadata(self) -> None:
        if self._meta_file.exists():
            with open(self._meta_file) as fh:
                saved = json.load(fh)
            self._files = saved.get("files", {})
            logger.info("Loaded metadata: %d file(s)", len(self._files))

    def _save_metadata(self) -> None:
        """Atomic write via rename so a crash never leaves partial state."""
        tmp = self._meta_file.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump({"files": self._files}, fh, indent=2)
        tmp.rename(self._meta_file)

    # ── Readiness gate ─────────────────────────────────────────────────────

    async def _assert_ready(self, context: grpc.ServicerContext) -> bool:
        """Abort the RPC with UNAVAILABLE if not all DataNodes have registered."""
        if self._ready.is_set():
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=5.0)
            return True
        except asyncio.TimeoutError:
            registered = set(self._registered_dns)
            missing = set(self._expected_dns) - registered
            await context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"Master not ready — waiting for DataNodes: {sorted(missing)}",
            )
            return False

    # ── DataNode registration ──────────────────────────────────────────────

    async def RegisterDataNode(
        self, request: master_pb2.RegisterDNRequest, context
    ) -> master_pb2.RegisterDNResponse:
        dn_id = request.datanode_id
        if dn_id not in self._expected_dns:
            return master_pb2.RegisterDNResponse(
                ok=False, error=f"Unknown DataNode: {dn_id}"
            )

        async with self._lock:
            self._registered_dns[dn_id] = {"host": request.host, "port": request.port}
            logger.info("DataNode %s registered (%s:%d)", dn_id, request.host, request.port)

            if set(self._registered_dns) >= set(self._expected_dns):
                if not self._ready.is_set():
                    self._ready.set()
                    logger.info("All DataNodes registered — MasterNode is ready")

        return master_pb2.RegisterDNResponse(ok=True)

    # ── File namespace operations ──────────────────────────────────────────

    async def CreateFile(
        self, request: master_pb2.CreateFileRequest, context
    ) -> master_pb2.CreateFileResponse:
        if not await self._assert_ready(context):
            return master_pb2.CreateFileResponse(ok=False, error="Master not ready")

        async with self._lock:
            path = request.path
            if path in self._files:
                return master_pb2.CreateFileResponse(
                    ok=False, error=f"File already exists: {path}"
                )
            self._files[path] = {"blocks": []}
            self._save_metadata()
            logger.info("CreateFile: %s", path)
            return master_pb2.CreateFileResponse(ok=True)

    async def DeleteFile(
        self, request: master_pb2.DeleteFileRequest, context
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
            logger.info("DeleteFile: %s (%d block(s))", path, len(file_meta["blocks"]))

        # Lazy background deletion — metadata already purged above.
        asyncio.create_task(self._delete_blocks_bg(file_meta["blocks"]))
        return master_pb2.DeleteFileResponse(ok=True)

    async def _delete_blocks_bg(self, blocks: list) -> None:
        for blk in blocks:
            dn_id = blk["datanode_id"]
            block_id = blk["block_id"]
            dn_info = self._registered_dns.get(dn_id)
            if not dn_info:
                logger.warning(
                    "Cannot delete block %s — DataNode %s not registered", block_id, dn_id
                )
                continue
            try:
                async with aio.insecure_channel(
                    f"{dn_info['host']}:{dn_info['port']}", options=_GRPC_OPTIONS
                ) as ch:
                    stub = datanode_pb2_grpc.DataNodeStub(ch)
                    resp = await stub.DeleteBlock(
                        datanode_pb2.DeleteBlockRequest(block_id=block_id)
                    )
                    if not resp.ok:
                        logger.warning("DeleteBlock %s failed: %s", block_id, resp.error)
            except Exception as exc:
                logger.warning("Error deleting block %s from %s: %s", block_id, dn_id, exc)

    # ── Block routing ──────────────────────────────────────────────────────

    async def GetBlockInfo(
        self, request: master_pb2.GetBlockInfoRequest, context
    ) -> master_pb2.GetBlockInfoResponse:
        if not await self._assert_ready(context):
            return master_pb2.GetBlockInfoResponse(ok=False, error="Master not ready")

        path = request.path
        block_index = request.block_index
        is_write = request.intent == master_pb2.Intent.WRITE

        # --- Phase 1: acquire lock for the entire operation including the
        # RegisterBlock RPC.  The lock is asyncio-safe (not thread-blocking),
        # and the RPC is a fast local-network call, so this is acceptable.
        async with self._lock:
            if path not in self._files:
                return master_pb2.GetBlockInfoResponse(
                    ok=False, error=f"File not found: {path}"
                )

            file_meta = self._files[path]
            num_blocks = len(file_meta["blocks"])

            if is_write:
                if block_index > num_blocks:
                    return master_pb2.GetBlockInfoResponse(
                        ok=False,
                        error=(
                            f"Block index {block_index} skips ahead "
                            f"(file has {num_blocks} block(s) — allocate sequentially)"
                        ),
                    )
                if block_index == num_blocks:
                    # Allocate a new block on a chosen DataNode.
                    dn_id = self._round_robin_dn()
                    block_id = str(uuid.uuid4())
                    dn_info = self._registered_dns[dn_id]

                    # Tell the DataNode to expect this block before persisting.
                    try:
                        async with aio.insecure_channel(
                            f"{dn_info['host']}:{dn_info['port']}", options=_GRPC_OPTIONS
                        ) as ch:
                            stub = datanode_pb2_grpc.DataNodeStub(ch)
                            reg = await stub.RegisterBlock(
                                datanode_pb2.RegisterBlockRequest(block_id=block_id)
                            )
                        if not reg.ok:
                            return master_pb2.GetBlockInfoResponse(
                                ok=False,
                                error=f"DataNode {dn_id} rejected RegisterBlock: {reg.error}",
                            )
                    except Exception as exc:
                        return master_pb2.GetBlockInfoResponse(
                            ok=False,
                            error=f"Could not reach DataNode {dn_id}: {exc}",
                        )

                    file_meta["blocks"].append(
                        {"block_id": block_id, "datanode_id": dn_id}
                    )
                    self._save_metadata()
                    logger.info(
                        "Allocated block[%d] %s on %s for %s",
                        block_index, block_id, dn_id, path,
                    )
            else:
                # READ
                if block_index >= num_blocks:
                    return master_pb2.GetBlockInfoResponse(
                        ok=False,
                        error=(
                            f"Block index {block_index} out of range "
                            f"(file has {num_blocks} block(s))"
                        ),
                    )

            blk = file_meta["blocks"][block_index]
            dn_id = blk["datanode_id"]
            dn_info = self._registered_dns[dn_id]

            return master_pb2.GetBlockInfoResponse(
                ok=True,
                block=master_pb2.BlockInfo(
                    block_id=blk["block_id"],
                    datanode_id=dn_id,
                    datanode_host=dn_info["host"],
                    datanode_port=dn_info["port"],
                ),
            )

    def _round_robin_dn(self) -> str:
        """Assign the next block to a DataNode via global round-robin."""
        dn_ids = sorted(self._registered_dns)  # stable order
        dn_id = dn_ids[self._rr_counter % len(dn_ids)]
        self._rr_counter += 1
        return dn_id


# ── Server bootstrap ───────────────────────────────────────────────────────

async def serve(config: dict) -> None:
    servicer = MasterNodeServicer(config)
    server = aio.server(options=_GRPC_OPTIONS)
    master_pb2_grpc.add_MasterNodeServicer_to_server(servicer, server)

    host = config["master"]["host"]
    port = config["master"]["port"]
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    logger.info("MasterNode listening on %s:%d", host, port)
    await server.wait_for_termination()
