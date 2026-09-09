#!/usr/bin/env python3
"""Kill one local Spec 007 worker and verify this run's read-committed results."""

import argparse
import json
import subprocess
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.spec007.yaml"]


def command(args):
    try:
        result = subprocess.run(args, cwd=ROOT, check=True, text=True, capture_output=True)
    except FileNotFoundError as error:
        raise RuntimeError(f"Required command is unavailable: {args[0]}") from error
    except subprocess.CalledProcessError as error:
        details = error.stderr.strip() or error.stdout.strip() or "no command output"
        raise RuntimeError(f"Command failed ({error.returncode}): {' '.join(args)}\n{details}") from error
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=360)
    args = parser.parse_args()
    if args.count < 1 or args.timeout < 1:
        parser.error("count and timeout must be positive")
    command(COMPOSE + ["config", "--quiet"])
    container = command(COMPOSE + ["ps", "--quiet", "spec007-subscriber-0"]).strip()
    if not container:
        raise RuntimeError("Start scripts/spec007 baseline first; no subscriber container is running")
    produced = command(
        COMPOSE
        + [
            "exec",
            "-T",
            "spec007-subscriber-0",
            "java",
            "-cp",
            "/app/worker.jar",
            "com.example.jobs.pull.Operations",
            "produce",
            str(args.count),
        ]
    )
    report = next(json.loads(line) for line in produced.splitlines() if line.startswith('{"runId"'))
    started = time.monotonic()
    command(["docker", "kill", "--signal=KILL", container])
    command(COMPOSE + ["up", "-d", "spec007-subscriber-0"])
    reader = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"spec007-chaos-observer-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    observed = set()
    try:
        reader.subscribe(["v007-subscriber-rerate-results.v1"])
        while len(observed) < args.count:
            if time.monotonic() - started > args.timeout:
                raise TimeoutError(f"Only {len(observed)}/{args.count} outcomes after worker kill")
            message = reader.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            result = json.loads(message.value())
            if result.get("correlationId") != report["runId"]:
                continue
            if result["status"] != "SUCCEEDED" or result["jobId"] in observed:
                raise AssertionError("Failed or duplicated prefix result")
            observed.add(result["jobId"])
    finally:
        reader.close()
    report.update(
        readCommittedResults=len(observed),
        killedContainer=container,
        recoverySeconds=time.monotonic() - started,
        externalSideEffectProof=False,
    )
    output = ROOT / "vertx-pull-worker" / "target" / "capacity-evidence" / "container-kill.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(str(error)) from error
