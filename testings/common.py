import hashlib
import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List


def utc_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


def stable_text_hash(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def run_signature(run: Dict[str, Any]) -> Dict[str, Any]:
    metrics = run.get("metrics", {})
    artifacts = run.get("artifacts", {})
    return {
        "game": run.get("game"),
        "language": run.get("language") or run.get("language_input"),
        "status": run.get("status"),
        "token_count": metrics.get("token_count", 0),
        "wordform_rows": metrics.get("wordform_rows", 0),
        "stale_removed": metrics.get("stale_removed", 0),
        "compressed_dic_hash": stable_text_hash(artifacts.get("compressed_dic", "")),
        "compressed_aff_hash": stable_text_hash(artifacts.get("compressed_aff", "")),
        "casing_csv_hash": stable_text_hash(artifacts.get("casing_csv", "")),
    }


def summarize_failures(runs: Iterable[Dict[str, Any]]) -> List[str]:
    failures: List[str] = []
    for run in runs:
        if run.get("status") != "ok":
            failures.append(
                f"{run.get('game')}/{run.get('language_input')}: {run.get('error', 'unknown error')}"
            )
    return failures
