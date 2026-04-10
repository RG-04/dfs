"""DFS client library.

Stateless: no metadata cache, no data cache.
Every public method opens a fresh gRPC connection.

Namespace model
---------------
Directories must be explicitly created with ``mkdir`` before files can be
placed inside them.  The root ``/`` always exists.  Operations that accept
a path enforce that the parent directory exists.

Offset decomposition
--------------------
``read`` and ``write`` take a **file-level byte offset** — the raw byte
position in the logical file, exactly like a POSIX seek position.

Internally each operation is split across block boundaries:

    block_index        = file_offset // block_size
    intra_block_offset = file_offset %  block_size

For every block touched the client:
  1. Calls ``MasterNode.GetBlockInfo(path, block_index)`` to get the
     assigned DataNode address (always the current Raft leader).
  2. Calls ``DataNode.ReadBlock`` / ``WriteBlock`` with the block_id and
     intra_block_offset so the DataNode seeks to the exact byte within
     its on-disk block file.

A single call may span multiple blocks; each is issued sequentially.

Usage::

    import asyncio, yaml
    from dfs.client.client import DFSClient

    async def main():
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
        client = DFSClient(config)

        await client.mkdir("/data")
        await client.create("/data/hello.bin")
        await client.write("/data/hello.bin", offset=0, data=b"hello")
        data = await client.read("/data/hello.bin", offset=0, length=5)
        await client.delete("/data/hello.bin")
        await client.rmdir("/data")
"""

import logging
from typing import List

import grpc
from grpc import aio

from dfs.proto import master_pb2, master_pb2_grpc
from dfs.proto import datanode_pb2, datanode_pb2_grpc

logger = logging.getLogger(__name__)

_GRPC_OPTIONS = [
    ("grpc.max_send_message_length",    128 * 1024 * 1024),
    ("grpc.max_receive_message_length", 128 * 1024 * 1024),
]


class StatResult:
    """Metadata returned by :meth:`DFSClient.stat` and :meth:`DFSClient.ls`.

    Attributes:
        type:       ``"file"`` or ``"dir"``
        name:       basename of the path
        num_blocks: allocated block count (always 0 for directories)
    """
    __slots__ = ("type", "name", "num_blocks")

    def __init__(self, type: str, name: str, num_blocks: int) -> None:
        self.type       = type
        self.name       = name
        self.num_blocks = num_blocks

    def __repr__(self) -> str:
        return f"StatResult(type={self.type!r}, name={self.name!r}, num_blocks={self.num_blocks})"


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

    async def mkdir(self, path: str) -> None:
        """Create a directory.  Parent directory must already exist."""
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.Mkdir(master_pb2.MkdirRequest(path=path))
        if not resp.ok:
            raise DFSError(f"mkdir({path!r}): {resp.error}")

    async def rmdir(self, path: str) -> None:
        """Remove an empty directory."""
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.Rmdir(master_pb2.RmdirRequest(path=path))
        if not resp.ok:
            raise DFSError(f"rmdir({path!r}): {resp.error}")

    async def stat(self, path: str) -> "StatResult":
        """Return metadata for a file or directory.

        Returns a :class:`StatResult` with fields:
          - ``type``: ``"file"`` or ``"dir"``
          - ``name``: basename of the path
          - ``num_blocks``: number of allocated blocks (0 for directories)
        """
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.Stat(master_pb2.StatRequest(path=path))
        if not resp.ok:
            raise DFSError(f"stat({path!r}): {resp.error}")
        is_dir = (resp.entry.type == master_pb2.StatEntry.DIR)
        return StatResult(
            type="dir" if is_dir else "file",
            name=resp.entry.name,
            num_blocks=resp.entry.num_blocks,
        )

    async def ls(self, path: str) -> List["StatResult"]:
        """List the immediate children of a directory.

        Returns a list of :class:`StatResult` sorted by name.
        """
        async with aio.insecure_channel(self._master_addr, options=_GRPC_OPTIONS) as ch:
            stub = master_pb2_grpc.MasterNodeStub(ch)
            resp = await stub.ListDir(master_pb2.ListDirRequest(path=path))
        if not resp.ok:
            raise DFSError(f"ls({path!r}): {resp.error}")
        return [
            StatResult(
                type="dir" if e.type == master_pb2.StatEntry.DIR else "file",
                name=e.name,
                num_blocks=e.num_blocks,
            )
            for e in resp.entries
        ]

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
            logger.info(f"block_info for {path} block[{block_index}]: {block_info}")

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
        try:
            async with aio.insecure_channel(addr, options=_GRPC_OPTIONS) as ch:
                stub = datanode_pb2_grpc.DataNodeStub(ch)
                resp = await stub.WriteBlock(
                    datanode_pb2.WriteBlockRequest(
                        block_id=block.block_id,
                        intra_block_offset=intra_block_offset,
                        data=data,
                    )
                )
        except grpc.RpcError as exc:
            raise DFSError(
                f"WriteBlock({block.block_id!r}): transport error: {exc.details()}"
            ) from exc
        if not resp.ok:
            raise DFSError(f"WriteBlock({block.block_id!r}): {resp.error}")

    async def _read_block(
        self,
        block: master_pb2.BlockInfo,
        intra_block_offset: int,
        length: int,
    ) -> bytes:
        addr = f"{block.datanode_host}:{block.datanode_port}"
        try:
            async with aio.insecure_channel(addr, options=_GRPC_OPTIONS) as ch:
                stub = datanode_pb2_grpc.DataNodeStub(ch)
                resp = await stub.ReadBlock(
                    datanode_pb2.ReadBlockRequest(
                        block_id=block.block_id,
                        intra_block_offset=intra_block_offset,
                        length=length,
                    )
                )
        except grpc.RpcError as exc:
            raise DFSError(
                f"ReadBlock({block.block_id!r}): transport error: {exc.details()}"
            ) from exc
        if not resp.ok:
            raise DFSError(f"ReadBlock({block.block_id!r}): {resp.error}")
        return resp.data


class DFSError(RuntimeError):
    pass
