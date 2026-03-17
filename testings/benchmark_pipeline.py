import argparse
import statistics
import time
from typing import Dict, List

from src import run_pipeline_batch

from testings.common import utc_stamp, write_json


def parse_csv_arg(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def timed_run(*, games: List[str], languages: List[str], sample: int, workers: int, pair_workers: int) -> Dict:
    t0 = time.perf_counter()
    result = run_pipeline_batch(
        languages=languages,
        games=games,
        sample=sample,
        workers=workers,
        pair_workers=pair_workers,
        strict_mode=False,
    )
    elapsed = time.perf_counter() - t0
    return {"elapsed_seconds": elapsed, "summary": result.get("summary", {}), "runs": result.get("runs", [])}


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark TB2dic pipeline with repeated runs")
    parser.add_argument("--games", required=True, help="Comma-separated game tags")
    parser.add_argument("--languages", required=True, help="Comma-separated language codes")
    parser.add_argument("--sample", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--pair-workers", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-json", default="testings/reports/benchmark_latest.json")
    args = parser.parse_args()

    games = parse_csv_arg(args.games)
    languages = parse_csv_arg(args.languages)
    repeats = max(1, args.repeats)
    total_pairs = len(games) * len(languages)
    analysis_note = ""
    if total_pairs == 1:
        analysis_note = (
            "Single pair detected: pair_workers does not measure outer-pair scaling here; "
            "focus on workers (Step 4 inner parallelism) for performance tuning."
        )
        print(f"WARNING: {analysis_note}")

    sequential_times: List[float] = []
    parallel_times: List[float] = []
    sequential_runs: List[Dict] = []
    parallel_runs: List[Dict] = []

    for _ in range(repeats):
        seq = timed_run(games=games, languages=languages, sample=args.sample, workers=args.workers, pair_workers=1)
        par = timed_run(
            games=games,
            languages=languages,
            sample=args.sample,
            workers=args.workers,
            pair_workers=max(1, args.pair_workers),
        )
        sequential_times.append(seq["elapsed_seconds"])
        parallel_times.append(par["elapsed_seconds"])
        sequential_runs.append(seq)
        parallel_runs.append(par)

    seq_med = statistics.median(sequential_times)
    par_med = statistics.median(parallel_times)
    speedup = (seq_med / par_med) if par_med > 0 else 0.0

    report = {
        "report_type": "benchmark",
        "timestamp": utc_stamp(),
        "analysis_note": analysis_note,
        "games": games,
        "languages": languages,
        "sample": args.sample,
        "workers": args.workers,
        "pair_workers": max(1, args.pair_workers),
        "repeats": repeats,
        "sequential_times": sequential_times,
        "parallel_times": parallel_times,
        "sequential_median": seq_med,
        "parallel_median": par_med,
        "speedup_ratio": speedup,
        "last_sequential_summary": sequential_runs[-1]["summary"] if sequential_runs else {},
        "last_parallel_summary": parallel_runs[-1]["summary"] if parallel_runs else {},
    }

    write_json(args.output_json, report)
    print(f"Benchmark complete. seq_med={seq_med:.3f}s par_med={par_med:.3f}s speedup={speedup:.3f}x")
    print(f"Report written to: {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
