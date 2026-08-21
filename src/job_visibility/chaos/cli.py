"""Read-only catalog and validation CLI for bounded Spec 005 experiments."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import TypeAdapter

from job_visibility.testing.resource_pressure import ResourcePressureController

from .catalog import SCENARIOS, catalog_json
from .model import FaultRule


def main() -> None:
    parser = argparse.ArgumentParser(description="Spec 005 chaos experiment interface")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("list", help="list allowlisted Spec 005 scenarios")
    validate = subcommands.add_parser("validate", help="validate a fault-rule JSON file")
    validate.add_argument("path", type=Path)
    pressure = subcommands.add_parser("pressure", help="apply bounded container pressure")
    pressure.add_argument("kind", choices=("cpu", "memory", "disk"))
    pressure.add_argument("service")
    pressure.add_argument("--project", required=True)
    pressure.add_argument("--amount", required=True, type=float)
    pressure.add_argument("--duration", type=int, default=15)
    cleanup = subcommands.add_parser("cleanup", help="remove bounded pressure artifacts")
    cleanup.add_argument("service")
    cleanup.add_argument("--project", required=True)
    stats = subcommands.add_parser("stats", help="sample one target container")
    stats.add_argument("service")
    stats.add_argument("--project", required=True)
    args = parser.parse_args()
    if args.command == "list":
        print(json.dumps(catalog_json(), indent=2))
        return
    if args.command == "validate":
        rules = TypeAdapter(list[FaultRule]).validate_json(args.path.read_text())
        print(json.dumps({"valid": True, "rules": len(rules), "scenarios": len(SCENARIOS)}))
        return
    controller = ResourcePressureController(args.project)
    if args.command == "cleanup":
        controller.cleanup(args.service)
        return
    if args.command == "stats":
        print(json.dumps(controller.stats(args.service), indent=2))
        return
    if args.kind == "cpu":
        controller.cpu(args.service, cpus=args.amount, duration_seconds=args.duration)
    elif args.kind == "memory":
        controller.memory(
            args.service,
            megabytes=int(args.amount),
            duration_seconds=args.duration,
        )
    else:
        controller.disk(args.service, megabytes=int(args.amount))


if __name__ == "__main__":
    main()
