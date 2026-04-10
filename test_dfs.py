"""Integration tests for Phase-2 DFS (Raft-replicated blocks).

The test session spins up the full cluster (master + all datanodes) as
subprocesses, runs every test, then tears the cluster down.  No manual
node management required:

    .venv/bin/pytest test_dfs.py -v
"""

import asyncio
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import grpc
from grpc import aio
import pytest
import pytest_asyncio
import yaml

from dfs.client.client import DFSClient, DFSError
from dfs.proto import master_pb2, master_pb2_grpc
from dfs.proto import datanode_pb2, datanode_pb2_grpc

# ── Constants ──────────────────────────────────────────────────────────────

_ROOT        = Path(__file__).parent
_CONFIG_PATH = str(_ROOT / "config.yaml")
_PYTHON      = sys.executable
_TEST_DIR    = "/test"
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

    _evict_old_nodes(cluster_config)

    for d in [master_cfg["metadata_dir"]] + [dn["data_dir"] for dn in dn_cfgs]:
        shutil.rmtree(d, ignore_errors=True)
        Path(d).mkdir(parents=True, exist_ok=True)

    procs = {}
    procs["master"] = subprocess.Popen(
        [_PYTHON, str(_ROOT / "bin" / "master.py"), _CONFIG_PATH],
    )
    time.sleep(0.5)

    for dn in dn_cfgs:
        procs[dn["id"]] = subprocess.Popen(
            [_PYTHON, str(_ROOT / "bin" / "datanode.py"), dn["id"], _CONFIG_PATH],
        )

    _wait_ready(cluster_config)

    # Create the shared test directory used by all integration tests.
    asyncio.run(_ensure_dir(cluster_config, _TEST_DIR))

    yield procs   # expose proc dict so failure tests can kill individual nodes

    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    for p in procs.values():
        p.wait(timeout=5)


def _start_cluster(config: dict, procs: dict, data_dirs_exist: bool = False) -> None:
    """Start master + all datanodes described by *config* into *procs*.

    If *data_dirs_exist* is False the data directories are wiped first.
    Writes a temporary config file so the processes pick up the right values.
    Returns the path to the temp config file (caller must clean up).
    """
    import shutil, tempfile

    master_cfg = config["master"]
    dn_cfgs    = config["datanodes"]

    if not data_dirs_exist:
        for d in [master_cfg["metadata_dir"]] + [dn["data_dir"] for dn in dn_cfgs]:
            shutil.rmtree(d, ignore_errors=True)
            Path(d).mkdir(parents=True, exist_ok=True)

    # Write config to a temp file so subprocesses see the overridden values.
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, prefix="dfs_test_cfg_"
    )
    import yaml as _yaml
    _yaml.dump(config, tmp)
    tmp.flush()
    cfg_path = tmp.name
    tmp.close()

    procs["master"] = subprocess.Popen(
        [_PYTHON, str(_ROOT / "bin" / "master.py"), cfg_path],
    )
    time.sleep(0.5)
    for dn in dn_cfgs:
        procs[dn["id"]] = subprocess.Popen(
            [_PYTHON, str(_ROOT / "bin" / "datanode.py"), dn["id"], cfg_path],
        )
    _wait_ready(config)
    return cfg_path


def _stop_cluster(procs: dict) -> None:
    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    for p in procs.values():
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def _evict_old_nodes(config: dict) -> None:
    import socket

    all_ports = [config["master"]["port"]] + [
        dn["port"] for dn in config["datanodes"]
    ]

    subprocess.run(["pkill", "-f", "bin/master.py"],   capture_output=True)
    subprocess.run(["pkill", "-f", "bin/datanode.py"], capture_output=True)

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


def _wait_ready(config: dict, timeout: float = 30.0) -> None:
    """Block until MasterNode accepts RPCs (all DataNodes registered)."""
    addr     = f"{config['master']['host']}:{config['master']['port']}"
    deadline = time.time() + timeout
    last_exc = None
    _RPC_TIMEOUT = 8.0

    while time.time() < deadline:
        try:
            with grpc.insecure_channel(addr) as ch:
                stub = master_pb2_grpc.MasterNodeStub(ch)
                stub.CreateFile(
                    master_pb2.CreateFileRequest(path="/__probe__"),
                    timeout=_RPC_TIMEOUT,
                )
                return
        except grpc.RpcError as exc:
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


async def _ensure_dir(config: dict, path: str) -> None:
    """Create *path* if it does not already exist (ignores 'already exists')."""
    client = DFSClient(config)
    try:
        await client.mkdir(path)
    except DFSError:
        pass  # already exists — that's fine


def _restart_datanode(
    dn_id: str,
    config: dict,
    procs: dict,
    config_path: str = _CONFIG_PATH,
) -> None:
    """Kill the given DataNode process and restart it, updating *procs*."""
    old = procs.get(dn_id)
    if old and old.poll() is None:
        old.terminate()
        old.wait(timeout=5)

    dn_cfgs = {dn["id"]: dn for dn in config["datanodes"]}
    port    = dn_cfgs[dn_id]["port"]

    # Wait until the port is free.
    import socket
    deadline = time.time() + 8.0
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if s.connect_ex(("127.0.0.1", port)) != 0:
                break
        time.sleep(0.2)

    procs[dn_id] = subprocess.Popen(
        [_PYTHON, str(_ROOT / "bin" / "datanode.py"), dn_id, config_path],
    )
    time.sleep(1.0)   # give it a moment to register


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


# ── Phase-1 tests (must still pass) ───────────────────────────────────────

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
        data = b"A" * block_size + b"B" * block_size
        await dfs_client.write(created_file, offset=0, data=data)

        chunk    = await dfs_client.read(created_file, offset=block_size - 5, length=10)
        expected = b"A" * 5 + b"B" * 5
        assert chunk == expected

    async def test_write_spanning_block_boundary(
        self, created_file, dfs_client, block_size
    ):
        await dfs_client.write(created_file, offset=0, data=b"\x00" * (2 * block_size))

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
        await dfs_client.write(created_file, offset=0, data=b"A" * block_size)

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
        await dfs_client.write(created_file, offset=0, data=b"X" * block_size)
        with pytest.raises(DFSError, match="out of range"):
            await dfs_client.read(created_file, offset=block_size, length=1)

    async def test_skip_block_index_raises(
        self, created_file, dfs_client, block_size
    ):
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


# ── Namespace tests ───────────────────────────────────────────────────────

class TestNamespace:
    """Tests for directory operations: mkdir, rmdir, stat, ls, and the rule
    that files must live inside an existing directory."""

    async def test_mkdir_and_rmdir(self, dfs_client):
        await dfs_client.mkdir("/ns_test_dir")
        # Verify it appears in root listing.
        entries = await dfs_client.ls("/")
        names = [e.name for e in entries]
        assert "ns_test_dir" in names
        # Remove it.
        await dfs_client.rmdir("/ns_test_dir")
        entries = await dfs_client.ls("/")
        assert "ns_test_dir" not in [e.name for e in entries]

    async def test_duplicate_mkdir_raises(self, dfs_client):
        await dfs_client.mkdir("/dup_dir")
        try:
            with pytest.raises(DFSError, match="already exists"):
                await dfs_client.mkdir("/dup_dir")
        finally:
            await dfs_client.rmdir("/dup_dir")

    async def test_mkdir_missing_parent_raises(self, dfs_client):
        with pytest.raises(DFSError, match="[Pp]arent"):
            await dfs_client.mkdir("/no_such_parent/child")

    async def test_rmdir_non_empty_raises(self, dfs_client):
        await dfs_client.mkdir("/nonempty_dir")
        await dfs_client.create("/nonempty_dir/file.bin")
        try:
            with pytest.raises(DFSError, match="[Nn]ot empty"):
                await dfs_client.rmdir("/nonempty_dir")
        finally:
            await dfs_client.delete("/nonempty_dir/file.bin")
            await dfs_client.rmdir("/nonempty_dir")

    async def test_rmdir_missing_raises(self, dfs_client):
        with pytest.raises(DFSError, match="[Nn]ot found"):
            await dfs_client.rmdir("/does_not_exist_xyz")

    async def test_create_file_without_parent_raises(self, dfs_client):
        with pytest.raises(DFSError, match="[Pp]arent"):
            await dfs_client.create("/no_such_dir/file.bin")

    async def test_stat_directory(self, dfs_client):
        await dfs_client.mkdir("/stat_dir")
        try:
            result = await dfs_client.stat("/stat_dir")
            assert result.type == "dir"
            assert result.name == "stat_dir"
        finally:
            await dfs_client.rmdir("/stat_dir")

    async def test_stat_file(self, dfs_client, block_size):
        await dfs_client.mkdir("/stat_file_dir")
        await dfs_client.create("/stat_file_dir/f.bin")
        try:
            result = await dfs_client.stat("/stat_file_dir/f.bin")
            assert result.type == "file"
            assert result.name == "f.bin"
            assert result.num_blocks == 0  # no data written yet
        finally:
            await dfs_client.delete("/stat_file_dir/f.bin")
            await dfs_client.rmdir("/stat_file_dir")

    async def test_stat_file_with_blocks(self, dfs_client, block_size):
        await dfs_client.mkdir("/stat_blocks_dir")
        await dfs_client.create("/stat_blocks_dir/big.bin")
        try:
            await dfs_client.write("/stat_blocks_dir/big.bin", offset=0, data=b"x" * block_size)
            result = await dfs_client.stat("/stat_blocks_dir/big.bin")
            assert result.type == "file"
            assert result.num_blocks == 1
        finally:
            await dfs_client.delete("/stat_blocks_dir/big.bin")
            await dfs_client.rmdir("/stat_blocks_dir")

    async def test_stat_missing_raises(self, dfs_client):
        with pytest.raises(DFSError, match="[Nn]o such"):
            await dfs_client.stat("/definitely_not_there_xyz")

    async def test_ls_root(self, dfs_client):
        """Root listing must at least include the /test directory created at startup."""
        entries = await dfs_client.ls("/")
        names = [e.name for e in entries]
        assert "test" in names

    async def test_ls_shows_files_and_dirs(self, dfs_client):
        await dfs_client.mkdir("/ls_test")
        await dfs_client.mkdir("/ls_test/subdir")
        await dfs_client.create("/ls_test/file.bin")
        try:
            entries = await dfs_client.ls("/ls_test")
            by_name = {e.name: e for e in entries}
            assert "subdir" in by_name and by_name["subdir"].type == "dir"
            assert "file.bin" in by_name and by_name["file.bin"].type == "file"
        finally:
            await dfs_client.delete("/ls_test/file.bin")
            await dfs_client.rmdir("/ls_test/subdir")
            await dfs_client.rmdir("/ls_test")

    async def test_ls_missing_raises(self, dfs_client):
        with pytest.raises(DFSError, match="[Nn]ot found"):
            await dfs_client.ls("/no_such_dir_xyz")

    async def test_stat_root(self, dfs_client):
        result = await dfs_client.stat("/")
        assert result.type == "dir"


# ── Phase-2 tests: replication and failure ────────────────────────────────

class TestRaftReplication:
    async def test_data_readable_after_write(
        self, created_file, dfs_client, block_size
    ):
        """Write data, immediately read it back — tests Raft commit path."""
        data = b"RAFT_TEST" * 100
        await dfs_client.write(created_file, offset=0, data=data)
        result = await dfs_client.read(created_file, offset=0, length=len(data))
        assert result == data

    async def test_replicated_to_followers_on_disk(
        self, created_file, dfs_client, cluster_config, block_size
    ):
        """After a write, the block file must exist on all DataNode disks."""
        data = b"X" * 1024
        await dfs_client.write(created_file, offset=0, data=data)

        # Give followers' apply loops time to finish.
        await asyncio.sleep(0.3)

        # Find the block_id from the master's metadata file.
        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blocks = meta["files"][created_file]["blocks"]
        assert blocks, "No blocks allocated"
        block_id = blocks[0]["block_id"]

        # Check that the block file exists on all DataNode data directories.
        for dn in cluster_config["datanodes"]:
            p = Path(dn["data_dir"]) / block_id
            assert p.exists(), (
                f"Block {block_id[:8]} missing on {dn['id']} at {p}"
            )

    async def test_multiple_sequential_writes(
        self, created_file, dfs_client, block_size
    ):
        """Sequential writes must all be linearised by the leader."""
        for i in range(5):
            await dfs_client.write(
                created_file, offset=i, data=bytes([i])
            )
        # Verify last write is visible.
        result = await dfs_client.read(created_file, offset=0, length=5)
        assert result == bytes(range(5))


class TestFailureDetection:
    async def test_one_follower_failure_write_succeeds(
        self, cluster_config, live_cluster, block_size
    ):
        """Killing one follower must not prevent writes (majority = 2/3)."""
        path = "/test/follower_fail.bin"
        try:
            await DFSClient(cluster_config).delete(path)
        except DFSError:
            pass

        client = DFSClient(cluster_config)
        await client.create(path)

        # Write a block — this designates a leader and replicates.
        data_in = b"A" * 512
        await client.write(path, offset=0, data=data_in)

        # Determine which DataNode is the follower (not the leader) for block 0.
        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        peers     = blk["peers"]
        follower  = next(p for p in peers if p != leader_id)

        # Kill the follower.
        proc = live_cluster.get(follower)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        try:
            # Cluster should still serve writes and reads with 2/3 nodes.
            data_extra = b"B" * 512
            await client.write(path, offset=512, data=data_extra)
            result = await client.read(path, offset=0, length=1024)
            assert result == data_in + data_extra
        finally:
            # Restart the follower so subsequent tests are unaffected.
            _restart_datanode(follower, cluster_config, live_cluster)
            await asyncio.sleep(1.0)
            try:
                await client.delete(path)
            except DFSError:
                pass

    async def test_leader_failure_triggers_election(
        self, cluster_config, live_cluster, block_size
    ):
        """Killing the Raft leader must eventually elect a new leader that
        can serve reads and writes."""
        path = "/test/leader_fail.bin"
        try:
            await DFSClient(cluster_config).delete(path)
        except DFSError:
            pass

        client = DFSClient(cluster_config)
        await client.create(path)

        # Write data to allocate and replicate block 0.
        data_before = b"BEFORE" * 100
        await client.write(path, offset=0, data=data_before)

        # Find the current leader.
        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]

        # Kill the leader.
        proc = live_cluster.get(leader_id)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        # Wait for the Raft election (election timeout ≤ 300 ms, plus
        # the master's NotifyLeader processing, plus some margin).
        await asyncio.sleep(2.0)

        try:
            # A new leader should be elected; reads/writes must succeed.
            data_after = b"AFTER" * 100
            await client.write(path, offset=len(data_before), data=data_after)
            result = await client.read(path, offset=0, length=len(data_before))
            assert result == data_before
        finally:
            _restart_datanode(leader_id, cluster_config, live_cluster)
            await asyncio.sleep(1.0)
            try:
                await client.delete(path)
            except DFSError:
                pass

    async def test_master_marks_cluster_unavailable_after_timeout(
        self, cluster_config, live_cluster, block_size
    ):
        """After both remaining followers (non-leader) die, writing to an
        EXISTING block should still work (2/3 majority from leader alone is
        not enough — wait: leader alone is 1/3, not majority).

        This test verifies a different scenario: when ALL datanodes for a block
        are killed, the master eventually marks the block unavailable.
        """
        path = "/test/unavailable.bin"
        try:
            await DFSClient(cluster_config).delete(path)
        except DFSError:
            pass

        client = DFSClient(cluster_config)
        await client.create(path)
        await client.write(path, offset=0, data=b"X" * 64)

        # Kill all three DataNodes so no quorum is possible.
        killed = []
        for dn in cluster_config["datanodes"]:
            proc = live_cluster.get(dn["id"])
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
                killed.append(dn["id"])

        # Wait for the master's heartbeat_timeout (15 s) plus one watchdog
        # cycle (heartbeat_timeout / 3 ≈ 5 s) to mark the cluster unavailable.
        # To keep the test fast we patch heartbeat_timeout to 3 s in config,
        # but since the live cluster is already running with the real config we
        # wait a proportionate time.  With the default 15 s this is ~20 s total.
        # For CI speed we rely on the watchdog marking it faster than 15 s when
        # the initial heartbeat was set at allocation time.
        #
        # Here we simply verify that the write eventually fails.
        deadline = time.time() + 25.0
        got_error = False
        while time.time() < deadline:
            try:
                # Try to write to the EXISTING block; master routes to leader.
                # With leader dead the DataNode write will fail or master
                # will report unavailable.
                await client.write(path, offset=0, data=b"Y" * 64)
                await asyncio.sleep(1.0)
            except DFSError:
                got_error = True
                break

        assert got_error, "Expected DFSError after all DataNodes killed"

        # Restart all DataNodes for subsequent tests.
        for dn_id in killed:
            _restart_datanode(dn_id, cluster_config, live_cluster)
        await asyncio.sleep(2.0)

        # Wait until master is ready again (all DataNodes re-registered).
        _wait_ready(cluster_config, timeout=20.0)

        try:
            await client.delete(path)
        except DFSError:
            pass

    async def test_new_block_denied_when_insufficient_nodes(
        self, cluster_config, live_cluster, block_size
    ):
        """Block allocation must be denied when fewer DataNodes are available
        than the replication_factor (all 3 needed, kill 2)."""
        path = "/test/insufficient.bin"
        try:
            await DFSClient(cluster_config).delete(path)
        except DFSError:
            pass

        client = DFSClient(cluster_config)
        await client.create(path)

        # Kill two DataNodes — only one left, need 3.
        dn_ids = [dn["id"] for dn in cluster_config["datanodes"]]
        killed = dn_ids[:2]
        for dn_id in killed:
            proc = live_cluster.get(dn_id)
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)

        await asyncio.sleep(0.5)  # give master time to notice via registration gap

        try:
            with pytest.raises(DFSError):
                # This tries to allocate a NEW block — must be denied.
                await client.write(path, offset=0, data=b"X")
        finally:
            for dn_id in killed:
                _restart_datanode(dn_id, cluster_config, live_cluster)
            await asyncio.sleep(2.0)
            _wait_ready(cluster_config, timeout=20.0)
            try:
                await client.delete(path)
            except DFSError:
                pass


# ── Block deletion tests ──────────────────────────────────────────────────

class TestBlockDeletion:
    """Verify that block deletion is reliable: confirmed on all peers even
    when a DataNode is temporarily unavailable at delete time."""

    async def test_delete_removes_block_files_from_all_nodes(
        self, dfs_client, cluster_config
    ):
        """After a normal delete, the block file must be gone from every
        DataNode's data directory."""
        import json

        path = "/test/del_cleanup.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.write(path, offset=0, data=b"Z" * 512)

        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        block_id = meta["files"][path]["blocks"][0]["block_id"]

        await dfs_client.delete(path)

        # Give the deletion loop time to confirm on all peers.
        await asyncio.sleep(1.0)

        for dn in cluster_config["datanodes"]:
            block_file = Path(dn["data_dir"]) / block_id
            assert not block_file.exists(), (
                f"Block {block_id[:8]} still present on {dn['id']} after delete"
            )

    async def test_delete_queue_persists_and_retries_after_node_recovers(
        self, cluster_config, live_cluster
    ):
        """Kill one DataNode, delete a file, then restart the node.
        The deletion loop must eventually deliver the DeleteBlock RPC to the
        recovered node and remove the block file from its disk."""
        import json

        client = DFSClient(cluster_config)
        path   = "/test/del_retry.bin"
        try:
            await client.delete(path)
        except DFSError:
            pass
        await client.create(path)
        await client.write(path, offset=0, data=b"R" * 512)

        # Find the block and pick a peer that is NOT the leader to kill — so
        # Raft can still commit the write but one peer will be offline at delete.
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        block_id  = blk["block_id"]
        leader_id = blk["leader_id"]
        peers     = blk["peers"]
        offline    = next(p for p in peers if p != leader_id)
        dn_info    = next(dn for dn in cluster_config["datanodes"] if dn["id"] == offline)
        block_file = Path(dn_info["data_dir"]) / block_id

        # Wait for the follower's apply loop to write the block to disk before
        # killing it — otherwise the block file wouldn't be there to check.
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if block_file.exists():
                break
            await asyncio.sleep(0.1)
        assert block_file.exists(), (
            f"Block never appeared on {offline} — follower didn't apply in time"
        )

        # Kill the offline node before deleting.
        proc = live_cluster.get(offline)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        # Delete the file.  The deletion loop will queue the offline peer.
        await client.delete(path)
        await asyncio.sleep(0.5)
        assert block_file.exists(), (
            f"Block file already gone before node restarted — "
            "test precondition failed"
        )

        # Verify the deletion queue has the pending entry persisted.
        with open(meta_file) as fh:
            meta_after = json.load(fh)
        pending = [
            e for e in meta_after.get("delete_queue", [])
            if e["block_id"] == block_id
        ]
        assert pending, "Pending deletion not found in persisted delete_queue"
        assert offline in pending[0]["pending_peers"], (
            f"{offline} not in pending_peers: {pending[0]['pending_peers']}"
        )

        # Restart the node — the deletion loop will retry and clean up.
        _restart_datanode(offline, cluster_config, live_cluster)

        # Wait for the retry to land (initial backoff is 2 s, plus restart delay).
        deadline = time.time() + 15.0
        while time.time() < deadline:
            if not block_file.exists():
                break
            await asyncio.sleep(0.5)

        assert not block_file.exists(), (
            f"Block {block_id[:8]} still on {offline} after node recovered — "
            "deletion retry loop did not deliver"
        )

        # Confirm the queue entry is cleared from disk too.
        with open(meta_file) as fh:
            meta_final = json.load(fh)
        remaining = [
            e for e in meta_final.get("delete_queue", [])
            if e["block_id"] == block_id
        ]
        assert not remaining, (
            f"Delete queue entry for {block_id[:8]} not cleared after delivery"
        )


# ── DataNode liveness filter tests ───────────────────────────────────────

class TestDNLiveness:
    """Verify that dead-but-registered DataNodes are excluded from new block
    allocation once dn_liveness_timeout has elapsed.

    Uses its own short-lived cluster with tight timing so the test completes
    in well under 30 s.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def fast_cluster(self, cluster_config, tmp_path):
        """Spin up a cluster with dn_heartbeat_interval=2 and
        dn_liveness_timeout=6 so liveness expires quickly."""
        import copy, os

        cfg = copy.deepcopy(cluster_config)
        cfg["dn_heartbeat_interval"] = 2   # DataNodes heartbeat every 2 s
        cfg["dn_liveness_timeout"]   = 6   # declared dead after 6 s silence

        # Use separate data directories so we don't stomp on the main cluster.
        cfg["master"]["metadata_dir"] = str(tmp_path / "master")
        for dn in cfg["datanodes"]:
            dn["data_dir"] = str(tmp_path / dn["id"])

        # Use different ports to avoid clashing with the main live_cluster.
        cfg["master"]["port"] = 50151
        port_map = {"dn0": 50152, "dn1": 50153, "dn2": 50154}
        for dn in cfg["datanodes"]:
            dn["port"] = port_map[dn["id"]]

        self._cfg      = cfg
        self._procs    = {}
        self._cfg_file = _start_cluster(cfg, self._procs)

        await _ensure_dir(cfg, "/test")

        yield

        _stop_cluster(self._procs)
        try:
            os.unlink(self._cfg_file)
        except OSError:
            pass

    async def test_live_node_included_in_allocation(self):
        """Sanity: all three nodes alive → block allocation succeeds."""
        client = DFSClient(self._cfg)
        path   = "/test/liveness_ok.bin"
        await client.create(path)
        await client.write(path, offset=0, data=b"ok")
        result = await client.read(path, offset=0, length=2)
        assert result == b"ok"
        await client.delete(path)

    async def test_dead_node_excluded_after_liveness_timeout(self):
        """Kill 2 DataNodes, wait for dn_liveness_timeout to expire, then try
        to allocate a new block.  The master must refuse because only 1 live
        node remains and replication_factor=3."""
        client  = DFSClient(self._cfg)
        dn_ids  = [dn["id"] for dn in self._cfg["datanodes"]]
        to_kill = dn_ids[:2]

        # Kill two nodes.
        for dn_id in to_kill:
            proc = self._procs.get(dn_id)
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)

        # Wait for liveness_timeout + one extra heartbeat window so the master
        # has definitely not seen a heartbeat from the dead nodes.
        liveness = self._cfg["dn_liveness_timeout"]
        hb       = self._cfg["dn_heartbeat_interval"]
        await asyncio.sleep(liveness + hb + 1.0)

        path = "/test/liveness_dead.bin"
        await client.create(path)
        try:
            with pytest.raises(DFSError, match="[Cc]annot allocate|only .* DataNode"):
                await client.write(path, offset=0, data=b"X")
        finally:
            try:
                await client.delete(path)
            except DFSError:
                pass
            # Restart the killed nodes for cleanup.
            for dn_id in to_kill:
                _restart_datanode(dn_id, self._cfg, self._procs, self._cfg_file)

    async def test_reregistration_restores_liveness(self):
        """A node that was briefly unreachable but comes back and re-registers
        must be included in allocation again."""
        dn_id  = self._cfg["datanodes"][0]["id"]
        client = DFSClient(self._cfg)

        # Kill the node and let liveness expire.
        proc = self._procs.get(dn_id)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        liveness = self._cfg["dn_liveness_timeout"]
        hb       = self._cfg["dn_heartbeat_interval"]
        await asyncio.sleep(liveness + hb + 1.0)

        # Restart it — after one heartbeat it should be live again.
        _restart_datanode(dn_id, self._cfg, self._procs, self._cfg_file)
        await asyncio.sleep(hb + 1.0)   # give it time to heartbeat

        # All 3 nodes live again — allocation must succeed.
        path = "/test/liveness_restored.bin"
        await client.create(path)
        await client.write(path, offset=0, data=b"restored")
        result = await client.read(path, offset=0, length=8)
        assert result == b"restored"
        await client.delete(path)


# ── Term guard tests ──────────────────────────────────────────────────────

class TestTermGuard:
    """Verify the stale-leader term guard on the DataNode data plane.

    Normal client I/O uses the min_leader_term supplied by the Master.
    These tests also directly call the DataNode gRPC to probe the guard
    boundary without going through the client abstraction.
    """

    async def test_normal_write_and_read_pass_term_check(
        self, dfs_client, cluster_config, block_size
    ):
        """End-to-end write + read must succeed with the term supplied by
        the Master (regression: ensure the check does not break normal I/O)."""
        path = "/test/term_guard_normal.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass

        await dfs_client.create(path)
        data = b"TERM_GUARD_OK" * 50
        await dfs_client.write(path, offset=0, data=data)
        result = await dfs_client.read(path, offset=0, length=len(data))
        assert result == data

        await dfs_client.delete(path)

    async def test_inflated_min_term_rejects_write(
        self, dfs_client, cluster_config, block_size
    ):
        """A write with min_term set above the node's current term must be
        rejected with a 'stale leader' error."""
        import json

        path = "/test/term_guard_write.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)

        # Write once to allocate the block and elect a leader.
        await dfs_client.write(path, offset=0, data=b"X" * 64)

        # Read the block metadata to find the leader node's address.
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        dn_info   = next(
            dn for dn in cluster_config["datanodes"] if dn["id"] == leader_id
        )
        addr     = f"{dn_info['host']}:{dn_info['port']}"
        block_id = blk["block_id"]

        # Call WriteBlock directly with a min_term far above any realistic term.
        inflated_min_term = 9999
        async with aio.insecure_channel(addr) as ch:
            stub = datanode_pb2_grpc.DataNodeStub(ch)
            resp = await stub.WriteBlock(
                datanode_pb2.WriteBlockRequest(
                    block_id=block_id,
                    intra_block_offset=0,
                    data=b"STALE",
                    min_term=inflated_min_term,
                )
            )

        assert not resp.ok, "Expected rejection for inflated min_term"
        assert "stale" in resp.error.lower(), f"Unexpected error: {resp.error}"

        await dfs_client.delete(path)

    async def test_inflated_min_term_rejects_read(
        self, dfs_client, cluster_config, block_size
    ):
        """A read with min_term set above the node's current term must be
        rejected with a 'stale leader' error."""
        import json

        path = "/test/term_guard_read.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.write(path, offset=0, data=b"Y" * 64)

        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        dn_info   = next(
            dn for dn in cluster_config["datanodes"] if dn["id"] == leader_id
        )
        addr     = f"{dn_info['host']}:{dn_info['port']}"
        block_id = blk["block_id"]

        inflated_min_term = 9999
        async with aio.insecure_channel(addr) as ch:
            stub = datanode_pb2_grpc.DataNodeStub(ch)
            resp = await stub.ReadBlock(
                datanode_pb2.ReadBlockRequest(
                    block_id=block_id,
                    intra_block_offset=0,
                    length=64,
                    min_term=inflated_min_term,
                )
            )

        assert not resp.ok, "Expected rejection for inflated min_term"
        assert "stale" in resp.error.lower(), f"Unexpected error: {resp.error}"

        await dfs_client.delete(path)

    async def test_zero_min_term_always_passes(
        self, dfs_client, cluster_config, block_size
    ):
        """min_term=0 must never block a legitimate leader (own_term >= 1)."""
        import json

        path = "/test/term_guard_zero.bin"
        try:
            await dfs_client.delete(path)
        except DFSError:
            pass
        await dfs_client.create(path)
        await dfs_client.write(path, offset=0, data=b"Z" * 64)

        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        dn_info   = next(
            dn for dn in cluster_config["datanodes"] if dn["id"] == leader_id
        )
        addr     = f"{dn_info['host']}:{dn_info['port']}"
        block_id = blk["block_id"]

        async with aio.insecure_channel(addr) as ch:
            stub = datanode_pb2_grpc.DataNodeStub(ch)
            resp = await stub.ReadBlock(
                datanode_pb2.ReadBlockRequest(
                    block_id=block_id,
                    intra_block_offset=0,
                    length=64,
                    min_term=0,
                )
            )

        assert resp.ok, f"Expected success with min_term=0, got: {resp.error}"

        await dfs_client.delete(path)


# ── Phase-2 tests: Raft persistence and recovery ──────────────────────────

class TestRaftRecovery:
    """Tests that verify Raft state is correctly persisted and recovered."""

    async def test_follower_restart_and_catchup(
        self, cluster_config, live_cluster, block_size
    ):
        """Kill a follower after initial writes, write more data, restart the
        follower, and verify its on-disk block file is fully up to date."""
        path = "/test/recovery_follower.bin"
        client = DFSClient(cluster_config)
        try:
            await client.delete(path)
        except DFSError:
            pass

        await client.create(path)
        data_before = b"BEFORE" * 200   # 1200 bytes
        await client.write(path, offset=0, data=data_before)

        # Identify the follower for block 0.
        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        peers     = blk["peers"]
        follower  = next(p for p in peers if p != leader_id)

        # Kill the follower.
        proc = live_cluster.get(follower)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        # Write more data while the follower is down (2/3 majority still works).
        data_after = b"AFTER_" * 200   # 1200 bytes
        await client.write(path, offset=len(data_before), data=data_after)

        # Restart the follower — it should load persisted Raft state and
        # receive the missing log entries via AppendEntries.
        _restart_datanode(follower, cluster_config, live_cluster)

        # Allow the leader to catch up the restarted follower.
        # Heartbeat interval = 50 ms; should be caught up well within 2 s.
        await asyncio.sleep(2.0)

        # Verify the block file on the restarted follower has all data.
        dn_cfgs   = {dn["id"]: dn for dn in cluster_config["datanodes"]}
        block_id  = blk["block_id"]
        block_file = Path(dn_cfgs[follower]["data_dir"]) / block_id
        assert block_file.exists(), f"Block file missing on recovered follower {follower}"

        with open(block_file, "rb") as fh:
            on_disk = fh.read()

        expected = data_before + data_after
        assert on_disk[:len(expected)] == expected, (
            f"Follower {follower} block file content mismatch after restart"
        )

        # Verify readable through normal DFS path.
        result = await client.read(path, offset=0, length=len(expected))
        assert result == expected

        try:
            await client.delete(path)
        except DFSError:
            pass

    async def test_leader_restart_and_rejoin(
        self, cluster_config, live_cluster, block_size
    ):
        """Kill the leader, let followers elect a new leader, write more data,
        restart the old leader, and verify it rejoins as a follower and catches
        up the data it missed."""
        path = "/test/recovery_leader.bin"
        client = DFSClient(cluster_config)
        try:
            await client.delete(path)
        except DFSError:
            pass

        await client.create(path)
        data_before = b"OLD_LEADER" * 100   # 1000 bytes
        await client.write(path, offset=0, data=data_before)

        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        blk       = meta["files"][path]["blocks"][0]
        leader_id = blk["leader_id"]
        block_id  = blk["block_id"]

        # Kill the leader.
        proc = live_cluster.get(leader_id)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)

        # Wait for a new election (≤ 300 ms timeout + notify master + margin).
        await asyncio.sleep(2.0)

        # Write new data through the new leader.
        data_after = b"NEW_LEADER" * 100
        await client.write(path, offset=len(data_before), data=data_after)

        # Restart the old leader — it must start as FOLLOWER (persistent term
        # higher than original, so it won't immediately think it's leader).
        _restart_datanode(leader_id, cluster_config, live_cluster)
        await asyncio.sleep(2.0)   # catch-up window

        # Verify the restarted old-leader has the new data in its block file.
        dn_cfgs    = {dn["id"]: dn for dn in cluster_config["datanodes"]}
        block_file = Path(dn_cfgs[leader_id]["data_dir"]) / block_id
        assert block_file.exists(), (
            f"Block file missing on restarted old-leader {leader_id}"
        )
        with open(block_file, "rb") as fh:
            on_disk = fh.read()

        expected = data_before + data_after
        assert on_disk[:len(expected)] == expected, (
            f"Old-leader {leader_id} did not catch up after restart"
        )

        try:
            await client.delete(path)
        except DFSError:
            pass

    async def test_full_cluster_restart_data_survives(
        self, cluster_config, live_cluster, block_size
    ):
        """Commit data, kill ALL nodes (including master), restart everything,
        and verify that all committed data is still readable.

        This exercises the full Raft persistence path:
          1. term / voted_for persisted → correct election after restart
          2. log persisted → committed entries survive
          3. last_applied persisted → block files already contain the data
        """
        import shutil

        path = "/test/full_restart.bin"
        client = DFSClient(cluster_config)
        try:
            await client.delete(path)
        except DFSError:
            pass

        await client.create(path)
        expected = b"SURVIVE_RESTART" * 300   # 4500 bytes
        await client.write(path, offset=0, data=expected)

        # Read back before kill to confirm data is committed.
        result = await client.read(path, offset=0, length=len(expected))
        assert result == expected, "Data mismatch before restart"

        # Give all DataNodes time to apply and persist last_applied.
        await asyncio.sleep(0.5)

        # ── Kill everything ────────────────────────────────────────────────
        all_nodes = ["master"] + [dn["id"] for dn in cluster_config["datanodes"]]
        for node_id in all_nodes:
            proc = live_cluster.get(node_id)
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)

        # ── Restart master (keep its metadata directory intact). ───────────
        live_cluster["master"] = subprocess.Popen(
            [_PYTHON, str(_ROOT / "bin" / "master.py"), _CONFIG_PATH],
        )
        time.sleep(0.5)

        # ── Restart DataNodes (keep their data directories intact). ─────────
        for dn in cluster_config["datanodes"]:
            live_cluster[dn["id"]] = subprocess.Popen(
                [_PYTHON, str(_ROOT / "bin" / "datanode.py"), dn["id"], _CONFIG_PATH],
            )

        # Wait for master ready (all DataNodes re-registered).
        _wait_ready(cluster_config, timeout=30.0)

        # Wait for a new Raft election and master notification.
        await asyncio.sleep(3.0)

        # ── Verify data survives ───────────────────────────────────────────
        # Retry for up to 10 s in case the new leader hasn't notified the
        # master yet (master routes to last-known leader from metadata, which
        # may be stale until NotifyLeader arrives).
        deadline = time.time() + 10.0
        last_err: Optional[Exception] = None
        while time.time() < deadline:
            try:
                result = await client.read(path, offset=0, length=len(expected))
                last_err = None
                break
            except DFSError as exc:
                last_err = exc
                await asyncio.sleep(0.5)

        if last_err:
            raise AssertionError(
                f"Could not read data after full cluster restart: {last_err}"
            )

        assert result == expected, (
            "Data mismatch after full cluster restart — "
            "persistence or recovery is broken"
        )

        try:
            await client.delete(path)
        except DFSError:
            pass

    async def test_raft_state_files_persisted_on_disk(
        self, cluster_config, live_cluster, block_size
    ):
        """Verify that Raft state and log files are written to the DataNode
        data directories after a write.  This is a sanity check that
        persistence is actually happening (not just in-memory)."""
        path = "/test/persistence_check.bin"
        client = DFSClient(cluster_config)
        try:
            await client.delete(path)
        except DFSError:
            pass

        await client.create(path)
        await client.write(path, offset=0, data=b"CHECK" * 400)   # 2000 bytes

        # Give apply loop time to persist last_applied.
        await asyncio.sleep(0.5)

        import json
        meta_file = Path(cluster_config["master"]["metadata_dir"]) / "metadata.json"
        with open(meta_file) as fh:
            meta = json.load(fh)
        block_id = meta["files"][path]["blocks"][0]["block_id"]

        for dn in cluster_config["datanodes"]:
            data_dir  = Path(dn["data_dir"])
            state_file = data_dir / f"raft_{block_id}.json"
            log_file   = data_dir / f"raft_{block_id}.log"

            assert state_file.exists(), (
                f"Raft state file missing on {dn['id']}: {state_file}"
            )
            assert log_file.exists(), (
                f"Raft log file missing on {dn['id']}: {log_file}"
            )

            # State file must have a valid term and last_applied ≥ 1.
            with open(state_file) as fh:
                state = json.load(fh)
            assert state["current_term"] >= 1, (
                f"{dn['id']}: current_term={state['current_term']} < 1"
            )
            assert state["last_applied"] >= 1, (
                f"{dn['id']}: last_applied={state['last_applied']} < 1"
            )

        try:
            await client.delete(path)
        except DFSError:
            pass
