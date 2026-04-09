#!/usr/bin/env python3
"""Small standalone client script demonstrating `DFSClient` usage.

Creates a file, writes some bytes, reads them back, then deletes the file.
Usage: `./bin/client_sample.py [config.yaml]`
"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))  # Allow running from project root without installing package.

import yaml

from dfs.client.client import DFSClient


logging.basicConfig(level=logging.INFO)


async def main() -> None:
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    with open(cfg_path) as fh:
        config = yaml.safe_load(fh)

    client = DFSClient(config)
    for x in range(3):
        path = f"/example_{x}.bin"
        payload = b"hello dfs\n"

        print(f"Creating {path}...")
        await client.create(path)

        print(f"Writing {len(payload)} bytes to {path}...")
        await client.write(path, offset=0, data=payload)

        print(f"Reading back {len(payload)} bytes from {path}...")
        data = await client.read(path, offset=0, length=len(payload))
        print("Read bytes:", data)

        print(f"Deleting {path}...")
        await client.delete(path)
    print("Done")


if __name__ == "__main__":
    asyncio.run(main())
