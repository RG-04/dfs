"""DFS client library.

Stateless: no metadata cache, no data cache.
Every public method opens fresh gRPC connections.

Offset decomposition
--------------------
All public methods take a **file-level byte offset** — the raw byte
position in the logical file, exactly like a POSIX seek position.

Internally, the client splits each operation across block boundaries:

    block_index        = file_offset // block_size
    intra_block_offset = file_offset %  block_size

For every block touched the client:
  1. Calls MasterNode.GetBlockInfo(path, block_index) to get the
     assigned DataNode address.
  2. Calls DataNode.ReadBlock / WriteBlock with the block_id and
     intra_block_offset so the DataNode can seek to the exact byte
     within its on-disk block file.

A single client call may touch 1, 2, … N blocks depending on how many
block boundaries the [offset, offset+length) range spans.  Each block
operation is issued sequentially (Phase-1; Phase-2 may pipeline).

Usage::

    import asyncio, yaml
    from dfs.client.client import DFSClient

    async def main():
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
        client = DFSClient(config)

        await client.create("/myfile.bin")
        # Write 5 bytes starting at file byte 1_000_005
        await client.write("/myfile.bin", offset=1_000_005, data=b"hello")
        # Read them back
        data = await client.read("/myfile.bin", offset=1_000_005, length=5)
        await client.delete("/myfile.bin")
"""

import logging
from typing import List

from grpc import aio

from dfs.proto import master_pb2, master_pb2_grpc
from dfs.proto import datanode_pb2, datanode_pb2_grpc

logger = logging.getLogger(__name__)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]


class DFSClient:
    def __init__(self, config: dict) -> None:
        self._block_size: int = config["block_size"]
        master = config["master"]
        self._master_addr = f"{master['host']}:{master['port']}"

    # ── Public API ─────────────────────────────────────────────────────────

    async def create(self, path: str) -> None:
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.CreateFile(master_pb2.CreateFileRequest(path=path))
        if not resp.ok:
            raise DFSError(f"create({path!r}): {resp.error}")

    async def delete(self, path: str) -> None:
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.DeleteFile(master_pb2.DeleteFileRequest(path=path))
        if not resp.ok:
            raise DFSError(f"delete({path!r}): {resp.error}")

    async def write(self, path: str, offset: int, data: bytes) -> None:
        """Write *data* to *path* starting at **file byte offset** *offset*.

        Splits the write across block boundaries.  If the write extends
        past the last allocated block the MasterNode allocates a new one.
        Blocks must be allocated sequentially from 0 — gaps are rejected.
        """
        if not data:
            return

        bs = self._block_size
        pos = 0  # bytes of *data* consumed so far

        while pos < len(data):
            # --- 1. Decompose file offset into block coordinates -----------
            file_offset        = offset + pos
            block_index        = file_offset // bs
            intra_block_offset = file_offset %  bs

            # Bytes available in this block from intra_block_offset to end.
            capacity = bs - intra_block_offset
            chunk    = data[pos : pos + capacity]   # may be smaller than capacity

            # --- 2. Ask MasterNode which DataNode owns this block ----------
            block_info = await self._get_block_info(path, block_index, write=True)

            # --- 3. Write the chunk to that DataNode ----------------------
            await self._write_block(block_info, intra_block_offset, chunk)

            pos += len(chunk)
            logger.debug(
                "write %s block[%d] intra_offset=%d bytes=%d",
                path, block_index, intra_block_offset, len(chunk),
            )

    async def read(self, path: str, offset: int, length: int) -> bytes:
        """Read *length* bytes from *path* starting at **file byte offset** *offset*.

        Returns however many bytes are available (may be fewer than *length*
        if the last block is partially filled).  Raises DFSError if the
        starting block index is beyond the last allocated block.
        """
        if length == 0:
            return b""

        bs = self._block_size
        chunks: List[bytes] = []
        pos = 0  # bytes fetched so far

        while pos < length:
            # --- 1. Decompose file offset into block coordinates -----------
            file_offset        = offset + pos
            block_index        = file_offset // bs
            intra_block_offset = file_offset %  bs

            # Bytes remaining in this block from intra_block_offset to end.
            capacity = bs - intra_block_offset
            to_read  = min(capacity, length - pos)

            # --- 2. Ask MasterNode which DataNode owns this block ----------
            block_info = await self._get_block_info(path, block_index, write=False)

            # --- 3. Read from that DataNode --------------------------------
            chunk = await self._read_block(block_info, intra_block_offset, to_read)
            chunks.append(chunk)
            pos += len(chunk)

            logger.debug(
                "read %s block[%d] intra_offset=%d requested=%d got=%d",
                path, block_index, intra_block_offset, to_read, len(chunk),
            )
            if len(chunk) < to_read:
                # Short read: end of written data in this block.
                break

        return b"".join(chunks)

    # ── Internal helpers ───────────────────────────────────────────────────

    async def _get_block_info(
        self, path: str, block_index: int, *, write: bool
    ) -> master_pb2.BlockInfo:
        intent = master_pb2.Intent.WRITE if write else master_pb2.Intent.READ
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.GetBlockInfo(
                master_pb2.GetBlockInfoRequest(
                    path=path,
                    block_index=block_index,
                    intent=intent,
                )
            )
        if not resp.ok:
            raise DFSError(
                f"GetBlockInfo({path!r}, block={block_index}, "
                f"{'WRITE' if write else 'READ'}): {resp.error}"
            )
        return resp.block

    async def _write_block(
        self,
        block: master_pb2.BlockInfo,
        intra_block_offset: int,
        data: bytes,
    ) -> None:
        addr = f"{block.datanode_host}:{block.datanode_port}"
        async with aio.insecure_channel(addr, options=_GRPC_OPTIONS) as ch:
            stub = datanode_pb2_grpc.DataNodeStub(ch)
            resp = await stub.WriteBlock(
                datanode_pb2.WriteBlockRequest(
                    block_id=block.block_id,
                    intra_block_offset=intra_block_offset,
                    data=data,
                )
            )
        if not resp.ok:
            raise DFSError(f"WriteBlock({block.block_id!r}): {resp.error}")

    async def _read_block(
        self,
        block: master_pb2.BlockInfo,
        intra_block_offset: int,
        length: int,
    ) -> bytes:
        addr = f"{block.datanode_host}:{block.datanode_port}"
        async with aio.insecure_channel(addr, options=_GRPC_OPTIONS) as ch:
            stub = datanode_pb2_grpc.DataNodeStub(ch)
            resp = await stub.ReadBlock(
                datanode_pb2.ReadBlockRequest(
                    block_id=block.block_id,
                    intra_block_offset=intra_block_offset,
                    length=length,
                )
            )
        if not resp.ok:
            raise DFSError(f"ReadBlock({block.block_id!r}): {resp.error}")
        return resp.data


class DFSError(RuntimeError):
    pass
