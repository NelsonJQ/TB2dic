import argparse
import json
import os
from typing import List

from src import run_pipeline_batch

from testings.common import summarize_failures, utc_stamp, write_json


def parse_csv_arg(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a smoke test for TB2dic modular pipeline")
    parser.add_argument("--games", required=True, help="Comma-separated game tags, example: DOFUS,TOUCH")
    parser.add_argument("--languages", required=True, help="Comma-separated language codes, example: es,pt")
    parser.add_argument("--sample", type=int, default=200, help="Step 4 sample size (0 means full)")
    parser.add_argument("--workers", type=int, default=8, help="Inner Step 4 workers")
    parser.add_argument("--pair-workers", type=int, default=1, help="Outer pair parallelism")
    parser.add_argument("--strict-mode", action="store_true", help="Fail fast on first pair error")
    parser.add_argument("--output-json", default="testings/reports/smoke_latest.json")
    args = parser.parse_args()

    games = parse_csv_arg(args.games)
    languages = parse_csv_arg(args.languages)

    result = run_pipeline_batch(
        languages=languages,
        games=games,
        sample=args.sample,
        workers=args.workers,
        pair_workers=args.pair_workers,
        strict_mode=args.strict_mode,
    )

    result["report_type"] = "smoke"
    result["timestamp"] = utc_stamp()
    write_json(args.output_json, result)

    failed = summarize_failures(result.get("runs", []))
    print(f"Smoke summary: status={result.get('summary', {}).get('status')} pairs={result.get('summary', {}).get('processed_pairs')}")
    if failed:
        print("Failures:")
        for line in failed:
            print(f"  - {line}")
        return 1

    print(f"Report written to: {os.path.abspath(args.output_json)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
