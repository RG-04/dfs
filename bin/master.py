#!/usr/bin/env python3
"""Entry-point for the MasterNode.

Usage:
    python bin/master.py [config.yaml]
"""

import asyncio
import logging
import sys
from pathlib import Path

import yaml

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from dfs.master.server import serve

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    with open(config_path) as fh:
        config = yaml.safe_load(fh)
    asyncio.run(serve(config))


if __name__ == "__main__":
    main()
