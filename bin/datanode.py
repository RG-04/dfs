#!/usr/bin/env python3
"""Entry-point for a DataNode.

Usage:
    python bin/datanode.py [--debug] <datanode-id> [config.yaml]

Options:
    --debug     Enable DEBUG-level logging (overrides config.yaml debug flag).

Example:
    python bin/datanode.py dn0
    python bin/datanode.py --debug dn1 config.yaml
"""

import asyncio
import logging
import sys
from pathlib import Path

import yaml

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from dfs.datanode.server import serve


def main() -> None:
    args = sys.argv[1:]
    debug_flag = "--debug" in args
    args = [a for a in args if a != "--debug"]

    if not args:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    dn_id = args[0]
    config_path = args[1] if len(args) > 1 else "config.yaml"

    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    known_ids = [dn["id"] for dn in config.get("datanodes", [])]
    if dn_id not in known_ids:
        print(f"Unknown datanode id {dn_id!r}. Known: {known_ids}", file=sys.stderr)
        sys.exit(1)

    debug = debug_flag or config.get("debug", False)
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    )
    if debug:
        logging.getLogger().info("DEBUG logging enabled for DataNode %s", dn_id)

    asyncio.run(serve(dn_id, config))


if __name__ == "__main__":
    main()
