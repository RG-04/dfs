#!/usr/bin/env python3
"""Entry-point for a DataNode.

Usage:
    python bin/datanode.py <datanode-id> [config.yaml]

Example:
    python bin/datanode.py dn0
    python bin/datanode.py dn1 config.yaml
"""

import asyncio
import logging
import sys
from pathlib import Path

import yaml

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from dfs.datanode.server import serve

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    dn_id = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else "config.yaml"

    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    known_ids = [dn["id"] for dn in config.get("datanodes", [])]
    if dn_id not in known_ids:
        print(f"Unknown datanode id {dn_id!r}. Known: {known_ids}", file=sys.stderr)
        sys.exit(1)

    asyncio.run(serve(dn_id, config))


if __name__ == "__main__":
    main()
