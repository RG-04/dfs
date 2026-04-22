#!/usr/bin/env python3
"""Entry-point for the MasterNode.

Usage:
    python bin/master.py [--debug] [config.yaml]

Options:
    --debug     Enable DEBUG-level logging (overrides config.yaml debug flag).
"""

import asyncio
import logging
import sys
from pathlib import Path

import yaml

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from dfs.master.server import serve


def main() -> None:
    args = sys.argv[1:]
    debug_flag = "--debug" in args
    args = [a for a in args if a != "--debug"]

    config_path = args[0] if args else "config.yaml"
    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    debug = debug_flag or config.get("debug", False)
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    )
    if debug:
        logging.getLogger().info("DEBUG logging enabled")

    asyncio.run(serve(config))


if __name__ == "__main__":
    main()
