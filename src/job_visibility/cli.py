from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from job_visibility.scenarios import CI_SCENARIOS, SCENARIOS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run scheduled-job visibility simulations")
    parser.add_argument(
        "scenario",
        nargs="*",
        help="Scenario IDs (default: all). Use 'ci' for the CI scenario set.",
    )
    parser.add_argument("--output", type=Path, help="Write the JSON report to this path")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    return parser


def run(selected: Sequence[str]) -> tuple[list[dict], int]:
    scenario_ids = list(selected)
    if not scenario_ids:
        scenario_ids = list(SCENARIOS)
    elif scenario_ids == ["ci"]:
        scenario_ids = CI_SCENARIOS
    unknown = sorted(set(scenario_ids) - SCENARIOS.keys())
    if unknown:
        raise SystemExit(f"Unknown scenario(s): {', '.join(unknown)}")
    results = [SCENARIOS[scenario_id]().to_dict() for scenario_id in scenario_ids]
    failures = sum(result["result"] != "PASS" for result in results)
    return results, failures


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    results, failures = run(args.scenario)
    report = {
        "summary": {
            "total": len(results),
            "passed": len(results) - failures,
            "failed": failures,
        },
        "scenarios": results,
    }
    rendered = json.dumps(report, indent=2 if args.pretty else None)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(
            f"{report['summary']['passed']}/{report['summary']['total']} scenarios passed; "
            f"report: {args.output}"
        )
    else:
        print(rendered)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
