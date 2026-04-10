#!/usr/bin/env python3
"""DFS interactive shell.

Usage:
    python bin/shell.py [config.yaml]

Commands
--------
  ls [path]                    list directory contents (default: cwd)
  cd <path>                    change working directory
  pwd                          print working directory
  stat <path>                  show metadata for a file or directory
  mkdir <path>                 create a directory (parent must exist)
  rmdir <path>                 remove an empty directory
  touch <path>                 create an empty file
  rm <path>                    delete a file
  write <path> <offset> <data> write text <data> at byte offset (creates if needed)
  cat <path>                   print file contents (decoded as UTF-8 where possible)
  put <local> [dfs_path]       upload a local file into DFS
  get <dfs_path> [local]       download a DFS file to the local filesystem
  help                         show this message
  exit / quit                  exit the shell
"""

import asyncio
import os
import posixpath
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import readline  # noqa: F401 — activates history/editing for input()
import yaml

from dfs.client.client import DFSClient, DFSError

# ── ANSI colours ──────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"


def _c(text: str, *codes: str) -> str:
    return "".join(codes) + text + RESET


def _err(msg: str) -> None:
    print(_c(f"error: {msg}", RED), file=sys.stderr)


def _ok(msg: str) -> None:
    print(_c(msg, GREEN))


def _info(msg: str) -> None:
    print(_c(msg, DIM))


# ── Path helpers ───────────────────────────────────────────────────────────────

def _resolve(cwd: str, path: str) -> str:
    """Resolve *path* against *cwd*, normalising . and .."""
    if not path.startswith("/"):
        path = posixpath.join(cwd, path)
    return posixpath.normpath(path) or "/"


# ── Command implementations ────────────────────────────────────────────────────

async def cmd_ls(client: DFSClient, cwd: str, args: list[str]) -> None:
    path = _resolve(cwd, args[0]) if args else cwd
    try:
        entries = await client.ls(path)
    except DFSError as exc:
        _err(str(exc))
        return
    if not entries:
        _info("(empty)")
        return
    for e in entries:
        if e.type == "dir":
            print(_c(f"{e.name}/", CYAN, BOLD))
        else:
            blocks = f"  [{e.num_blocks} block{'s' if e.num_blocks != 1 else ''}]"
            print(f"{e.name}" + _c(blocks, DIM))


async def cmd_stat(client: DFSClient, cwd: str, args: list[str]) -> None:
    if not args:
        _err("usage: stat <path>")
        return
    path = _resolve(cwd, args[0])
    try:
        s = await client.stat(path)
    except DFSError as exc:
        _err(str(exc))
        return
    kind = _c("dir", CYAN, BOLD) if s.type == "dir" else _c("file", WHITE)
    print(f"  path   : {path}")
    print(f"  type   : {kind}")
    print(f"  name   : {s.name}")
    if s.type == "file":
        print(f"  blocks : {s.num_blocks}")


async def cmd_mkdir(client: DFSClient, cwd: str, args: list[str]) -> None:
    if not args:
        _err("usage: mkdir <path>")
        return
    path = _resolve(cwd, args[0])
    try:
        await client.mkdir(path)
        _ok(f"created directory {path}")
    except DFSError as exc:
        _err(str(exc))


async def cmd_rmdir(client: DFSClient, cwd: str, args: list[str]) -> None:
    if not args:
        _err("usage: rmdir <path>")
        return
    path = _resolve(cwd, args[0])
    try:
        await client.rmdir(path)
        _ok(f"removed {path}")
    except DFSError as exc:
        _err(str(exc))


async def cmd_touch(client: DFSClient, cwd: str, args: list[str]) -> None:
    if not args:
        _err("usage: touch <path>")
        return
    path = _resolve(cwd, args[0])
    try:
        await client.create(path)
        _ok(f"created {path}")
    except DFSError as exc:
        _err(str(exc))


async def cmd_rm(client: DFSClient, cwd: str, args: list[str]) -> None:
    if not args:
        _err("usage: rm <path>")
        return
    path = _resolve(cwd, args[0])
    try:
        await client.delete(path)
        _ok(f"deleted {path}")
    except DFSError as exc:
        _err(str(exc))


async def cmd_write(client: DFSClient, cwd: str, args: list[str]) -> None:
    if len(args) < 3:
        _err('usage: write <path> <offset> <data>')
        return
    path   = _resolve(cwd, args[0])
    try:
        offset = int(args[1])
    except ValueError:
        _err(f"offset must be an integer, got {args[1]!r}")
        return
    # Remaining tokens joined back (allows spaces inside unquoted data).
    data = " ".join(args[2:]).encode()
    try:
        await client.write(path, offset=offset, data=data)
        _ok(f"wrote {len(data)} byte(s) to {path} at offset {offset}")
    except DFSError as exc:
        _err(str(exc))


async def cmd_cat(client: DFSClient, cwd: str, args: list[str], block_size: int) -> None:
    if not args:
        _err("usage: cat <path>")
        return
    path = _resolve(cwd, args[0])
    chunks = []
    offset = 0
    while True:
        try:
            chunk = await client.read(path, offset=offset, length=block_size)
        except DFSError as exc:
            if offset == 0:
                _err(str(exc))
                return
            break  # read past last block — end of file
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        if len(chunk) < block_size:
            break  # short read = end of written data

    raw = b"".join(chunks)
    if not raw:
        _info("(empty file)")
        return
    try:
        print(raw.decode("utf-8"))
    except UnicodeDecodeError:
        # Fall back to hex dump for binary content.
        print(_c("(binary content — hex dump)", DIM))
        for i in range(0, min(len(raw), 512), 16):
            row   = raw[i:i+16]
            hex_  = " ".join(f"{b:02x}" for b in row)
            ascii = "".join(chr(b) if 32 <= b < 127 else "." for b in row)
            print(f"  {i:06x}  {hex_:<48}  {ascii}")
        if len(raw) > 512:
            print(_c(f"  ... ({len(raw)} bytes total, showing first 512)", DIM))


async def cmd_put(client: DFSClient, cwd: str, args: list[str], block_size: int) -> None:
    if not args:
        _err("usage: put <local_path> [dfs_path]")
        return
    local = Path(args[0])
    if not local.exists():
        _err(f"local file not found: {local}")
        return
    dfs_path = _resolve(cwd, args[1]) if len(args) > 1 else _resolve(cwd, local.name)

    # Create the file first (ignore "already exists").
    try:
        await client.create(dfs_path)
    except DFSError as exc:
        if "already exists" not in str(exc).lower():
            _err(str(exc))
            return

    total = local.stat().st_size
    written = 0
    with open(local, "rb") as fh:
        while True:
            chunk = fh.read(block_size)
            if not chunk:
                break
            try:
                await client.write(dfs_path, offset=written, data=chunk)
            except DFSError as exc:
                _err(f"write failed at offset {written}: {exc}")
                return
            written += len(chunk)
            pct = written * 100 // total if total else 100
            print(f"\r  {written}/{total} bytes ({pct}%)", end="", flush=True)
    print()
    _ok(f"uploaded {local} → {dfs_path}  ({written} bytes)")


async def cmd_get(client: DFSClient, cwd: str, args: list[str], block_size: int) -> None:
    if not args:
        _err("usage: get <dfs_path> [local_path]")
        return
    dfs_path  = _resolve(cwd, args[0])
    local     = Path(args[1]) if len(args) > 1 else Path(posixpath.basename(dfs_path))

    offset = 0
    written = 0
    with open(local, "wb") as fh:
        while True:
            try:
                chunk = await client.read(dfs_path, offset=offset, length=block_size)
            except DFSError as exc:
                if offset == 0:
                    _err(str(exc))
                    fh.close()
                    local.unlink(missing_ok=True)
                    return
                break
            if not chunk:
                break
            fh.write(chunk)
            offset  += len(chunk)
            written += len(chunk)
            print(f"\r  {written} bytes", end="", flush=True)
            if len(chunk) < block_size:
                break
    print()
    _ok(f"downloaded {dfs_path} → {local}  ({written} bytes)")


def cmd_help() -> None:
    print(__doc__)


# ── REPL ───────────────────────────────────────────────────────────────────────

async def repl(config: dict) -> None:
    client     = DFSClient(config)
    block_size = config["block_size"]
    cwd        = "/"

    print(_c("DFS shell  —  type 'help' for commands, 'exit' to quit", BOLD))
    print(_c(f"master: {config['master']['host']}:{config['master']['port']}", DIM))

    while True:
        prompt = _c("dfs", CYAN, BOLD) + _c(f":{cwd}", YELLOW) + _c("$ ", BOLD)
        try:
            line = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line or line.startswith("#"):
            continue

        tokens = line.split()
        cmd    = tokens[0].lower()
        args   = tokens[1:]

        if cmd in ("exit", "quit"):
            break
        elif cmd == "help":
            cmd_help()
        elif cmd == "pwd":
            print(cwd)
        elif cmd == "cd":
            if not args:
                cwd = "/"
            else:
                target = _resolve(cwd, args[0])
                # Validate that the target is an actual directory.
                try:
                    s = await client.stat(target)
                    if s.type != "dir":
                        _err(f"not a directory: {target}")
                    else:
                        cwd = target
                except DFSError as exc:
                    _err(str(exc))
        elif cmd == "ls":
            await cmd_ls(client, cwd, args)
        elif cmd == "stat":
            await cmd_stat(client, cwd, args)
        elif cmd == "mkdir":
            await cmd_mkdir(client, cwd, args)
        elif cmd == "rmdir":
            await cmd_rmdir(client, cwd, args)
        elif cmd in ("touch", "create"):
            await cmd_touch(client, cwd, args)
        elif cmd in ("rm", "delete"):
            await cmd_rm(client, cwd, args)
        elif cmd == "write":
            await cmd_write(client, cwd, args)
        elif cmd == "cat":
            await cmd_cat(client, cwd, args, block_size)
        elif cmd == "put":
            await cmd_put(client, cwd, args, block_size)
        elif cmd == "get":
            await cmd_get(client, cwd, args, block_size)
        else:
            _err(f"unknown command {cmd!r}  (try 'help')")


def main() -> None:
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    with open(cfg_path) as fh:
        config = yaml.safe_load(fh)
    asyncio.run(repl(config))


if __name__ == "__main__":
    main()
