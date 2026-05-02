#!/usr/bin/env python3
"""Entry-point for a DataNode.

Usage:
    python bin/datanode.py [--debug] <datanode-id> [config.yaml]
    python bin/datanode.py [--debug] --host HOST --port PORT --data-dir DIR [--id ID] [config.yaml]

Options:
    --debug             Enable DEBUG-level logging.
    --host HOST         Override host for this DataNode.
    --port PORT         Override port for this DataNode.
    --data-dir DIR      Override data directory for this DataNode.
    --id ID             DataNode ID (default: "host:port").

The datanode-id may be one of the IDs listed under 'datanodes' in config.yaml,
OR any new ID/address not in the config (dynamic membership).  In the latter
case --host, --port, and --data-dir must be supplied.

Examples:
    python bin/datanode.py dn0
    python bin/datanode.py --debug dn1 config.yaml
    python bin/datanode.py --host 127.0.0.1 --port 50055 --data-dir /tmp/dfs/dn3
"""

import asyncio
import logging
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from dfs.datanode.server import serve


def main() -> None:
    args = sys.argv[1:]
    debug_flag = "--debug" in args
    args = [a for a in args if a != "--debug"]

    if not args:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    # Parse optional --host / --port / --data-dir / --id overrides.
    def _pop(flag: str) -> str | None:
        nonlocal args
        if flag in args:
            idx = args.index(flag)
            val = args[idx + 1]
            args = args[:idx] + args[idx + 2:]
            return val
        return None

    host_override     = _pop("--host")
    port_override     = _pop("--port")
    data_dir_override = _pop("--data-dir")
    id_override       = _pop("--id")

    # Remaining positional args: [dn_id] [config.yaml]
    positional  = [a for a in args if not a.startswith("--")]
    config_path = positional[1] if len(positional) > 1 else "config.yaml"

    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    known_dns = {dn["id"]: dn for dn in config.get("datanodes", [])}

    if positional:
        dn_id = positional[0]
    elif id_override:
        dn_id = id_override
    elif host_override and port_override:
        dn_id = id_override or f"{host_override}:{port_override}"
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    if dn_id in known_dns:
        # Well-known node — use config values, allow overrides.
        dn_conf = dict(known_dns[dn_id])
        if host_override:
            dn_conf["host"] = host_override
        if port_override:
            dn_conf["port"] = int(port_override)
        if data_dir_override:
            dn_conf["data_dir"] = data_dir_override
    else:
        # Dynamic node — must supply host/port/data-dir.
        if not (host_override and port_override and data_dir_override):
            print(
                f"DataNode id {dn_id!r} not in config.  "
                "Supply --host, --port, and --data-dir for a dynamic node.",
                file=sys.stderr,
            )
            sys.exit(1)
        dn_conf = {
            "id":       dn_id,
            "host":     host_override,
            "port":     int(port_override),
            "data_dir": data_dir_override,
        }
        # Inject into config so DataNodeServicer can find it.
        config.setdefault("datanodes", []).append(dn_conf)

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
