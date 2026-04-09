"""Integration tests for Phase-1 DFS.

The test session spins up the full cluster (master + all datanodes) as
subprocesses, runs every test, then tears the cluster down.  No manual
node management required:

    pytest test_dfs.py -v
"""

import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest
import pytest_asyncio
import yaml

from dfs.client.client import DFSClient, DFSError
from dfs.proto import master_pb2, master_pb2_grpc

# ── Constants ──────────────────────────────────────────────────────────────

_ROOT        = Path(__file__).parent
_CONFIG_PATH = str(_ROOT / "config.yaml")
_PYTHON      = sys.executable
_TEST_PATH   = "/test/integration.bin"

# ── Session fixtures: cluster lifecycle ───────────────────────────────────

@pytest.fixture(scope="session")
def cluster_config():
    with open(_CONFIG_PATH) as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="session")
def live_cluster(cluster_config):
    """Start master + all datanodes; wait for readiness; teardown after session."""
    import shutil

    master_cfg = cluster_config["master"]
    dn_cfgs    = cluster_config["datanodes"]

    # Kill any leftover node processes from previous runs and wait until
    # the ports are actually free before starting fresh ones.
    _evict_old_nodes(cluster_config)

    # Wipe stale on-disk state so every test run starts from scratch.
    for d in [master_cfg["metadata_dir"]] + [dn["data_dir"] for dn in dn_cfgs]:
        shutil.rmtree(d, ignore_errors=True)
        Path(d).mkdir(parents=True, exist_ok=True)

    procs = []
    procs.append(subprocess.Popen(
        [_PYTHON, str(_ROOT / "bin" / "master.py"), _CONFIG_PATH],
    ))
    time.sleep(0.5)  # give master a head start before datanodes connect

    for dn in dn_cfgs:
        procs.append(subprocess.Popen(
            [_PYTHON, str(_ROOT / "bin" / "datanode.py"), dn["id"], _CONFIG_PATH],
        ))

    _wait_ready(cluster_config)

    yield

    for p in procs:
        p.terminate()
    for p in procs:
        p.wait(timeout=5)


def _evict_old_nodes(config: dict) -> None:
    """Kill any processes already listening on the cluster ports."""
    import socket

    all_ports = [config["master"]["port"]] + [
        dn["port"] for dn in config["datanodes"]
    ]

    # Send SIGTERM to processes holding our ports.
    subprocess.run(["pkill", "-f", "bin/master.py"],   capture_output=True)
    subprocess.run(["pkill", "-f", "bin/datanode.py"], capture_output=True)

    # Wait until every port is actually free (up to 8 s).
    deadline = time.time() + 8.0
    while time.time() < deadline:
        busy = []
        for port in all_ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    busy.append(port)
        if not busy:
            return
        time.sleep(0.2)


def _wait_ready(config: dict, timeout: float = 20.0) -> None:
    """Block until MasterNode accepts RPCs (all DataNodes registered).

    Uses a blocking gRPC call with a long timeout so the master has time
    to wait for all DataNodes to register before declaring itself ready.
    """
    addr     = f"{config['master']['host']}:{config['master']['port']}"
    deadline = time.time() + timeout
    last_exc = None

    # Longer than the master's internal 5-second DataNode-wait so we always
    # get a real response (ok / "already exists" / UNAVAILABLE) rather than
    # a client-side DEADLINE_EXCEEDED.
    _RPC_TIMEOUT = 8.0

    while time.time() < deadline:
        try:
            with grpc.insecure_channel(addr) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                stub.CreateFile(
                    master_pb2.CreateFileRequest(path="/__probe__"),
                    timeout=_RPC_TIMEOUT,
                )
                return  # Any response (even "file exists") means cluster is ready.
        except grpc.RpcError as exc:
            # UNAVAILABLE → master not up yet or datanodes not registered.
            # ALREADY_EXISTS / other → master responded, cluster is ready.
            if exc.code() not in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ):
                return
            last_exc = exc
        except Exception as exc:
            last_exc = exc
        time.sleep(0.3)

    raise RuntimeError(f"Cluster not ready after {timeout}s: {last_exc}")


# ── Per-test fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def dfs_client(cluster_config, live_cluster):
    return DFSClient(cluster_config)


@pytest.fixture
def block_size(cluster_config):
    return cluster_config["block_size"]


@pytest_asyncio.fixture
async def created_file(dfs_client):
    """Ensure the test file exists before a test and is deleted after."""
    # Clean up leftovers from a crashed previous test.
    try:
        await dfs_client.delete(_TEST_PATH)
    except DFSError:
        pass

    await dfs_client.create(_TEST_PATH)
    yield _TEST_PATH

    try:
        await dfs_client.delete(_TEST_PATH)
    except DFSError:
        pass


# ── Tests ──────────────────────────────────────────────────────────────────

class TestCreate:
    async def test_create_new_file(self, dfs_client):
        path = "/test/new_file.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.delete(path)

    async def test_duplicate_create_raises(self, created_file, dfs_client):
        with pytest.raises(DFSError, match="already exists"):
            await dfs_client.create(created_file)


class TestMultiBlockIO:
    async def test_write_and_read_two_full_blocks(
        self, created_file, dfs_client, block_size
    ):
        data = b"A" * block_size + b"B" * block_size
        await dfs_client.write(created_file, offset=0, data=data)
        result = await dfs_client.read(created_file, offset=0, length=len(data))
        assert result == data

    async def test_cross_block_read(self, created_file, dfs_client, block_size):
        # Write two full blocks: 'A' then 'B'
        data = b"A" * block_size + b"B" * block_size
        await dfs_client.write(created_file, offset=0, data=data)

        # Read 10 bytes that straddle the block boundary
        chunk    = await dfs_client.read(created_file, offset=block_size - 5, length=10)
        expected = b"A" * 5 + b"B" * 5
        assert chunk == expected

    async def test_write_spanning_block_boundary(
        self, created_file, dfs_client, block_size
    ):
        # First allocate both blocks with a full write so the second block exists.
        await dfs_client.write(created_file, offset=0, data=b"\x00" * (2 * block_size))

        # Now overwrite 4 bytes that straddle the boundary (2 bytes in each block).
        straddle_offset = block_size - 2
        straddle_data   = b"XYZW"
        await dfs_client.write(created_file, offset=straddle_offset, data=straddle_data)

        result = await dfs_client.read(
            created_file, offset=straddle_offset, length=len(straddle_data)
        )
        assert result == straddle_data


class TestSmallIO:
    async def test_small_write_and_read_within_block(
        self, created_file, dfs_client, block_size
    ):
        # Allocate block 0 with known content.
        await dfs_client.write(created_file, offset=0, data=b"A" * block_size)

        # Overwrite 7 bytes at a small offset deep inside the block.
        patch_offset = 100
        patch        = b"PATCHED"
        await dfs_client.write(created_file, offset=patch_offset, data=patch)

        assert await dfs_client.read(created_file, offset=patch_offset, length=7) == patch

    async def test_surrounding_bytes_unchanged_after_small_write(
        self, created_file, dfs_client, block_size
    ):
        await dfs_client.write(created_file, offset=0, data=b"A" * block_size)
        patch_offset = 100
        await dfs_client.write(created_file, offset=patch_offset, data=b"PATCHED")

        # One byte immediately before and after the patch must still be 'A'.
        before = await dfs_client.read(created_file, offset=patch_offset - 1, length=1)
        after  = await dfs_client.read(
            created_file, offset=patch_offset + len(b"PATCHED"), length=1
        )
        assert before == b"A"
        assert after  == b"A"

    async def test_single_byte_write_and_read(
        self, created_file, dfs_client
    ):
        await dfs_client.write(created_file, offset=0, data=b"Z")
        assert await dfs_client.read(created_file, offset=0, length=1) == b"Z"


class TestBoundaryErrors:
    async def test_read_past_last_block_raises(
        self, created_file, dfs_client, block_size
    ):
        # Allocate one block only.
        await dfs_client.write(created_file, offset=0, data=b"X" * block_size)
        with pytest.raises(DFSError, match="out of range"):
            await dfs_client.read(created_file, offset=block_size, length=1)

    async def test_skip_block_index_raises(
        self, created_file, dfs_client, block_size
    ):
        # Writing at offset 2*block_size without allocating block 1 first must fail.
        with pytest.raises(DFSError, match="skips ahead"):
            await dfs_client.write(
                created_file, offset=2 * block_size, data=b"X"
            )


class TestDelete:
    async def test_read_after_delete_raises(self, dfs_client):
        path = "/test/delete_me.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.write(path, offset=0, data=b"hello")
        await dfs_client.delete(path)

        with pytest.raises(DFSError, match="not found|File not found"):
            await dfs_client.read(path, offset=0, length=5)

    async def test_double_delete_raises(self, dfs_client):
        path = "/test/double_delete.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.delete(path)

        with pytest.raises(DFSError, match="not found|File not found"):
            await dfs_client.delete(path)
