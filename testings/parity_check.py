import argparse
import os
from typing import Dict, List, Tuple

from src import run_pipeline_batch

from testings.common import run_signature, utc_stamp, write_json


def parse_csv_arg(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def pair_key(run: Dict) -> Tuple[str, str]:
    return str(run.get("game", "")), str(run.get("language_input", run.get("language", "")))


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare sequential vs pair-parallel outputs")
    parser.add_argument("--games", required=True, help="Comma-separated game tags")
    parser.add_argument("--languages", required=True, help="Comma-separated language codes")
    parser.add_argument("--sample", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--parallel-pairs", type=int, default=2, help="pair_workers value for parallel run")
    parser.add_argument("--output-json", default="testings/reports/parity_latest.json")
    args = parser.parse_args()

    games = parse_csv_arg(args.games)
    languages = parse_csv_arg(args.languages)
    total_pairs = len(games) * len(languages)
    analysis_note = ""
    if total_pairs == 1:
        analysis_note = (
            "Single pair detected: this parity run validates output stability, "
            "not pair-level concurrency throughput."
        )
        print(f"WARNING: {analysis_note}")

    baseline = run_pipeline_batch(
        languages=languages,
        games=games,
        sample=args.sample,
        workers=args.workers,
        pair_workers=1,
        strict_mode=False,
    )
    candidate = run_pipeline_batch(
        languages=languages,
        games=games,
        sample=args.sample,
        workers=args.workers,
        pair_workers=max(1, args.parallel_pairs),
        strict_mode=False,
    )

    base_map = {pair_key(r): run_signature(r) for r in baseline.get("runs", [])}
    cand_map = {pair_key(r): run_signature(r) for r in candidate.get("runs", [])}

    mismatches: List[Dict] = []
    for key in sorted(set(base_map.keys()) | set(cand_map.keys())):
        b = base_map.get(key)
        c = cand_map.get(key)
        if b != c:
            mismatches.append({"pair": key, "baseline": b, "candidate": c})

    report = {
        "report_type": "parity",
        "timestamp": utc_stamp(),
        "analysis_note": analysis_note,
        "games": games,
        "languages": languages,
        "sample": args.sample,
        "workers": args.workers,
        "parallel_pairs": max(1, args.parallel_pairs),
        "baseline_summary": baseline.get("summary", {}),
        "candidate_summary": candidate.get("summary", {}),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "pass": len(mismatches) == 0,
    }
    write_json(args.output_json, report)

    if mismatches:
        print(f"Parity FAILED with {len(mismatches)} mismatched pairs")
        print(f"Report written to: {os.path.abspath(args.output_json)}")
        return 1

    print("Parity PASS: sequential and pair-parallel signatures match")
    print(f"Report written to: {os.path.abspath(args.output_json)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
