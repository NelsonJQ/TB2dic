# Batch orchestrator for Steps 1-5 (tokenize/filter -> wordforms -> munch)
import concurrent.futures
import os
import glob
import threading
import time
from typing import Any, Dict, List, Tuple, Union

from .batchfiltering import batch_filter_tokens_by_dictionary, resolve_hunspell_paths
from .findincorpus import find_corpus_wordforms, load_i18n_corpus
from .munching import munch_to_compressed_dic
from .provenance import build_consolidated_provenance_report
from .params import DIC_FOLDER, INTERMEDIARY_DIR, LANG_CODES, TB_PATHS, i18n_PATHS
from .prepro import load_and_tokenize_terminology_base, normalize_language_code


# Map language codes to available tokenizer modes used by load_and_tokenize_terminology_base().
_TOKENIZE_LANGUAGE_BY_CODE = {
    "en": "english",
    "pt": "portuguese",
    "pt-br": "portuguese",
    "pt-pt": "portuguese",
    "es": "default",
    "fr": "french",
    "fr-fr": "french",
    "de": "default",
    "it": "default",
}

# Session-level prewarm guard: (game, lang) pairs already warmed in this kernel.
_PIPELINE_PREWARM_DONE = set()


def _resolve_tokenize_language_mode(lang_code: str) -> str:
    """Return tokenizer mode for a language code, defaulting to 'default'."""
    code = str(lang_code or "").strip().lower()
    return _TOKENIZE_LANGUAGE_BY_CODE.get(code, _TOKENIZE_LANGUAGE_BY_CODE.get(normalize_language_code(code), "default"))


def _build_pair_paths(game: str, lang: str, work_dir: str) -> Dict[str, str]:
    """Build canonical intermediary file paths for one game/lang pair."""
    return {
        "token_txt": os.path.join(work_dir, f"{game}_{lang}_tokens.txt"),
        "filtered_dic": os.path.join(work_dir, f"{game}_{lang}_filtered_tokens.dic"),
        "propernoun_sidecar": os.path.join(work_dir, f"{game}_{lang}_propernoun_tokens.json"),
        "filter_audit_csv": os.path.join(work_dir, f"{game}_{lang}_filtered_tokens_filter_audit.csv"),
        "step4_provenance_jsonl": os.path.join(work_dir, f"{game}_{lang}_corpus_wordforms_provenance.jsonl"),
        "step5_munch_provenance_jsonl": os.path.join(work_dir, f"{game}_{lang}_munch_provenance.jsonl"),
        "consolidated_csv": os.path.join(work_dir, f"{game}_{lang}_token_provenance_report.csv"),
        "consolidated_jsonl": os.path.join(work_dir, f"{game}_{lang}_token_provenance_report.jsonl"),
    }


def _cleanup_pair_outputs(game: str, lang: str, work_dir: str) -> List[str]:
    """Remove stale intermediary files for deterministic reruns."""
    removed: List[str] = []
    patterns = [
        os.path.join(work_dir, f"{game}_{lang}_filtered_tokens*.dic"),
        os.path.join(work_dir, f"{game}_{lang}_tokens*.txt"),
        os.path.join(work_dir, f"{game}_{lang}_*provenance*.jsonl"),
        os.path.join(work_dir, f"{game}_{lang}_*provenance*.csv"),
        os.path.join(work_dir, f"{game}_{lang}_*filter_audit.csv"),
    ]
    for pattern in patterns:
        for path in glob.glob(pattern):
            try:
                os.remove(path)
                removed.append(path)
            except OSError:
                pass
    return removed


def run_pipeline_batch(
    languages: Union[List[str], str],  # list[str] ou 'all'
    games: List[str],                  # list[str] des jeux a traiter (cles TB_PATHS)
    sample: int = 0,                   # 0 = full run, >0 = echantillon de mots
    workers: int = 8,                  # nb de workers pour Step 4
    batch_size: int = 50,              # taille des lots envoyes aux workers
    add_verb_flags: bool = False,      # True = inclure flags verbaux candidats
    quorum: float = 0.5,               # seuil de validation des flags (0.0-1.0)
    cleanup_stale: bool = True,        # supprime anciens outputs intermediaires
    strict_mode: bool = False,         # stop au 1er echec si True
    pair_workers: int = 1,             # parallelisme externe sur les paires (game/lang)
    skip_step_corpusforms: bool = False,         # True = saute find_corpus_wordforms, Step 5 reste active
    output_folder: str | None = None,  # dossier de travail (None => INTERMEDIARY_DIR)
    final_output_folder: str | None = None,  # dossier final export (None => OUTPUT_DIR)
    prewarm_i18n: bool = True,         # precharge i18n filtre seulement si pas deja warm
    provenance_level: str = "detailed",  # off | light | detailed
    provenance_formats: List[str] | None = None,  # csv/jsonl
    step4_compact_corpus_map: bool = False,  # True = one true-case form/token in Step 4 corpus map
    step4_std_dic_mode: str = "expanded",  # expanded | headwords | off
    step4_retain_known_forms: bool = True,  # False = only keep NEW forms from Step 4 to save RAM
    step4_wordform_cache_max: int | None = None,  # optional cache cap override for generate_word_forms
    step4_clear_wordform_cache_every_batches: int = 0,  # periodic cache clear during Step 4
) -> Dict[str, Any]:
    """
    Run the full Steps 1-5 pipeline for each (game, language) pair.

    Steps reused from TESTS:
      1-3) Tokenization and dictionary filtering
      4)   find_corpus_wordforms()
      5)   munch_to_compressed_dic()

    Required core params:
      - languages: list[str] or 'all'
      - games: list[str]

        Optional behavior:
            - skip_step_corpusforms: when True, bypass Step 4 corpus wordform matching and
                run Step 5 with an empty wordform list (minimal compressed output).

    Returns:
      Dict with keys:
        - runs: list of per-pair structured results
        - summary: aggregate totals and elapsed time
    """
    t_start = time.time()

    if isinstance(languages, str):
        _lang_token = languages.strip().lower()
        if _lang_token == 'all':
            languages_to_run = sorted({code.lower() for code in LANG_CODES.values() if code})
        elif _lang_token:
            languages_to_run = [_lang_token]
        else:
            languages_to_run = []
    else:
        languages_to_run = [str(l).strip() for l in languages if str(l).strip()]

    if not languages_to_run:
        raise ValueError("languages must contain at least one language code (or 'all')")
    if not games:
        raise ValueError("games must contain at least one game tag")

    work_dir = output_folder or INTERMEDIARY_DIR
    os.makedirs(work_dir, exist_ok=True)

    runs: List[Dict[str, Any]] = []

    print("=" * 80)
    print("BATCH PIPELINE (STEPS 1-5)")
    print("=" * 80)
    print(f"Games     : {games}")
    print(f"Languages : {languages_to_run}")
    print(f"Work dir  : {work_dir}")
    print(
        f"Options   : sample={sample}, workers={workers}, batch_size={batch_size}, "
        f"add_verb_flags={add_verb_flags}, quorum={quorum}, "
        f"cleanup_stale={cleanup_stale}, strict_mode={strict_mode}, pair_workers={pair_workers}, "
        f"skip_step_corpusforms={skip_step_corpusforms}, "
        f"provenance_level={provenance_level}, provenance_formats={provenance_formats or ['csv', 'jsonl']}, "
        f"step4_compact_corpus_map={step4_compact_corpus_map}, step4_std_dic_mode={step4_std_dic_mode}, "
        f"step4_retain_known_forms={step4_retain_known_forms}, "
        f"step4_wordform_cache_max={step4_wordform_cache_max}, "
        f"step4_clear_wordform_cache_every_batches={step4_clear_wordform_cache_every_batches}"
    )
    print("=" * 80)

    pair_specs: List[Tuple[str, str]] = [
        (game, lang_input)
        for game in games
        for lang_input in languages_to_run
    ]
    pair_index = {pair: idx for idx, pair in enumerate(pair_specs)}

    requested_pair_workers = max(1, int(pair_workers or 1))
    effective_pair_workers = requested_pair_workers
    if len(pair_specs) == 1 and effective_pair_workers > 1:
        print("Single pair detected => forcing pair_workers=1 (no outer parallelism benefit)")
        effective_pair_workers = 1
    if strict_mode:
        if effective_pair_workers > 1:
            print("strict_mode=True => forcing pair_workers=1 for fail-fast determinism")
        effective_pair_workers = 1

    # Preflight dictionary resolution once for all requested languages.
    preflight_dicts: Dict[str, Dict[str, Any]] = {}
    for _lang_input in languages_to_run:
        try:
            _lang_norm = normalize_language_code(_lang_input)
            preflight_dicts[_lang_norm] = resolve_hunspell_paths(_lang_norm, dic_folder=DIC_FOLDER)
        except Exception as preflight_err:
            preflight_dicts[str(_lang_input).strip().lower()] = {
                "language": str(_lang_input),
                "ok": False,
                "dic": "",
                "aff": "",
                "checks": [],
                "error": f"{type(preflight_err).__name__}: {preflight_err}"
            }

    print("Dictionary preflight:")
    for _lang_key, _info in preflight_dicts.items():
        if _info.get("ok"):
            print(f"  ✅ {_lang_key}: {_info['dic']}")
        else:
            print(f"  ❌ {_lang_key}: no valid dic+aff pair")
            if _info.get("error"):
                print(f"     {_info['error']}")
            for _chk in _info.get("checks", []):
                print(
                    f"     - DIC {'OK' if _chk['dic_exists'] else 'MISSING'} | "
                    f"AFF {'OK' if _chk['aff_exists'] else 'MISSING'} :: {_chk['dic']}"
                )

    _prewarm_lock = threading.Lock()

    def _process_pair(pair: Tuple[str, str]) -> Dict[str, Any]:
        game, lang_input = pair
        run_t0 = time.time()
        run_result: Dict[str, Any] = {
            "game": game,
            "language_input": lang_input,
            "language": None,
            "status": "ok",
            "error": "",
            "timings": {},
            "artifacts": {},
            "metrics": {},
        }

        print("\n" + "-" * 80)
        print(f"▶ Processing pair: game={game} | language={lang_input}")
        print("-" * 80)

        try:
            setup_t0 = time.time()
            if game not in TB_PATHS:
                raise KeyError(f"Game '{game}' is not configured in TB_PATHS")

            lang = normalize_language_code(lang_input)
            run_result["language"] = lang

            dict_status = preflight_dicts.get(lang, {"ok": False, "checks": []})
            run_result["metrics"]["dictionary_preflight"] = dict_status
            if not dict_status.get("ok"):
                checks = dict_status.get("checks", [])
                checked_paths = [c.get("dic", "") for c in checks]
                raise FileNotFoundError(
                    f"No valid Hunspell dic+aff pair for '{lang}'. Checked: {checked_paths}"
                )

            paths = _build_pair_paths(game=game, lang=lang, work_dir=work_dir)
            tokenize_language = _resolve_tokenize_language_mode(lang_input)
            tb_path = TB_PATHS[game]
            run_result["timings"]["step_0_setup"] = round(time.time() - setup_t0, 3)
            run_result["metrics"]["pair_workers_requested"] = requested_pair_workers
            run_result["metrics"]["pair_workers_effective"] = effective_pair_workers

            if cleanup_stale:
                cleanup_t0 = time.time()
                removed = _cleanup_pair_outputs(game=game, lang=lang, work_dir=work_dir)
                if removed:
                    print(f"Removed stale outputs: {len(removed)}")
                run_result["metrics"]["stale_removed"] = len(removed)
                run_result["timings"]["step_0_cleanup_stale"] = round(time.time() - cleanup_t0, 3)

            # Optional i18n prewarm so ES-first-lower heuristics have local evidence.
            prewarm_t0 = time.time()
            if prewarm_i18n:
                pair_key = (game, lang)
                with _prewarm_lock:
                    game_i18n_entry = i18n_PATHS.get(game, {})
                    if isinstance(game_i18n_entry, dict):
                        current_i18n = game_i18n_entry.get(lang, "")
                    else:
                        current_i18n = ""
                        run_result["metrics"]["i18n_status"] = "missing_config"
                        print(f"Prewarm skipped for {game}/{lang} [no i18n config]")

                    filtered_json = os.path.join(INTERMEDIARY_DIR, f"{game}_{lang}_i18n_filtered.json")
                    filtered_props = os.path.join(INTERMEDIARY_DIR, f"{game}_{lang}_i18n_filtered.properties")

                    # If a filtered i18n file already exists on disk, reuse it directly.
                    existing_filtered = ""
                    if os.path.exists(filtered_json):
                        existing_filtered = filtered_json
                    elif os.path.exists(filtered_props):
                        existing_filtered = filtered_props
                    if existing_filtered:
                        if isinstance(i18n_PATHS.get(game), dict):
                            i18n_PATHS[game][lang] = existing_filtered
                        current_i18n = existing_filtered

                    already_filtered = bool(current_i18n) and os.path.exists(current_i18n) and (
                        os.path.basename(current_i18n) == f"{game}_{lang}_i18n_filtered.json"
                        or os.path.basename(current_i18n) == f"{game}_{lang}_i18n_filtered.properties"
                    )
                    prewarm_done = pair_key in _PIPELINE_PREWARM_DONE

                if prewarm_done or already_filtered:
                    if already_filtered:
                        with _prewarm_lock:
                            _PIPELINE_PREWARM_DONE.add(pair_key)
                    run_result["metrics"]["i18n_status"] = "cached_filtered"
                    print(f"Prewarm skipped for {game}/{lang} [cached]")
                elif not current_i18n:
                    if run_result["metrics"].get("i18n_status") != "missing_config":
                        run_result["metrics"]["i18n_status"] = "missing_file"
                        print(f"Prewarm skipped for {game}/{lang} [missing i18n file]")
                else:
                    try:
                        print(f"Prewarming filtered i18n for {game}/{lang}...")
                        load_i18n_corpus(lang, [game], source_type="i18n", lang_detect=True)
                        with _prewarm_lock:
                            _PIPELINE_PREWARM_DONE.add(pair_key)
                        run_result["metrics"]["i18n_status"] = "prewarmed"
                    except Exception as prewarm_err:
                        run_result["metrics"]["i18n_status"] = "prewarm_failed"
                        print(f"WARNING: Prewarm failed for {game}/{lang}: {prewarm_err}")
            elif not prewarm_i18n:
                run_result["metrics"]["i18n_status"] = "prewarm_disabled"
            else:
                run_result["metrics"]["i18n_status"] = "prewarm_loader_missing"
            run_result["timings"]["step_0_prewarm_i18n"] = round(time.time() - prewarm_t0, 3)

            # Steps 1-3: tokenize TB and dictionary-filter against Hunspell
            step_t0 = time.time()
            tokens_list, _ = load_and_tokenize_terminology_base(
                excel_file_path=tb_path,
                language_code=lang,
                tokenize_language=tokenize_language,
                output_file_path=paths["token_txt"],
                save_propernoun_sidecar=True,
                game_tag=game,
                allow_language_fallback=False,
            )
            run_result["timings"]["step_1_tokenize"] = round(time.time() - step_t0, 3)
            run_result["metrics"]["token_count"] = len(tokens_list)

            # load_and_tokenize_terminology_base currently writes sidecar in
            # INTERMEDIARY_DIR; when running with custom output_folder, keep a
            # fallback so Step 2/3 can still consume token->TB key lineage.
            propernoun_sidecar_for_filter = paths["propernoun_sidecar"]
            if not os.path.isfile(propernoun_sidecar_for_filter):
                sidecar_fallback = os.path.join(INTERMEDIARY_DIR, os.path.basename(paths["propernoun_sidecar"]))
                if os.path.isfile(sidecar_fallback):
                    propernoun_sidecar_for_filter = sidecar_fallback

            step_t0 = time.time()
            batch_results = batch_filter_tokens_by_dictionary(
                input_folder=work_dir,
                target_languages=[lang],
                dic_folder=DIC_FOLDER,
                output_folder=work_dir,
                propernoun_sidecar_path=propernoun_sidecar_for_filter,
            )
            run_result["timings"]["step_2_3_filter"] = round(time.time() - step_t0, 3)

            if not os.path.isfile(paths["filtered_dic"]):
                raise FileNotFoundError(
                    f"Expected filtered dic not found after batch filtering: {paths['filtered_dic']}"
                )

            run_result["artifacts"]["token_txt"] = paths["token_txt"]
            run_result["artifacts"]["filtered_dic"] = paths["filtered_dic"]
            run_result["artifacts"]["propernoun_sidecar"] = (
                propernoun_sidecar_for_filter if os.path.isfile(propernoun_sidecar_for_filter) else None
            )
            run_result["metrics"]["batch_filter_results"] = batch_results
            for batch_row in batch_results:
                if os.path.basename(batch_row.get("output_file", "")) == os.path.basename(paths["filtered_dic"]):
                    if batch_row.get("filter_audit_csv"):
                        run_result["artifacts"]["filter_audit_csv"] = batch_row["filter_audit_csv"]
                    break

            # Step 4: find corpus wordforms from filtered .dic into i18n corpus
            if skip_step_corpusforms:
                wordform_results = []
                run_result["timings"]["step_4_wordforms"] = 0.0
                run_result["metrics"]["wordform_rows"] = 0
                run_result["metrics"]["step_4_skipped"] = True
                print(f"Step 4 skipped for {game}/{lang}; Step 5 will run with empty wordforms")
            else:
                step_t0 = time.time()
                wordform_results = find_corpus_wordforms(
                    dic_path=paths["filtered_dic"],
                    lang=lang,
                    games=[game],
                    source_type="i18n",
                    sample=sample,
                    workers=workers,
                    batch_size=batch_size,
                    propernoun_sidecar=run_result["artifacts"]["propernoun_sidecar"],
                    add_verb_flags=add_verb_flags,
                    quorum=quorum,
                    provenance_level=provenance_level,
                    provenance_output_folder=work_dir,
                    compact_corpus_map=step4_compact_corpus_map,
                    std_dic_mode=step4_std_dic_mode,
                    retain_known_forms=step4_retain_known_forms,
                    wordform_cache_max=step4_wordform_cache_max,
                    clear_wordform_cache_every_batches=step4_clear_wordform_cache_every_batches,
                )
                run_result["timings"]["step_4_wordforms"] = round(time.time() - step_t0, 3)
                run_result["metrics"]["wordform_rows"] = len(wordform_results)
                run_result["metrics"]["step_4_skipped"] = False

            # Step 5: munch outputs (flat + compressed dic)
            step_t0 = time.time()
            munch_result = munch_to_compressed_dic(
                dic_path=paths["filtered_dic"],
                lang=lang,
                games=[game],
                wordform_results=wordform_results,
                propernoun_sidecar=run_result["artifacts"]["propernoun_sidecar"],
                filter_audit_csv_path=run_result["artifacts"].get("filter_audit_csv", ""),
                provenance_level=provenance_level,
                provenance_output_folder=work_dir,
                final_output_folder=final_output_folder,
            )
            run_result["timings"]["step_5_munch"] = round(time.time() - step_t0, 3)

            report_result: Dict[str, Any] = {}
            if str(provenance_level or "off").strip().lower() != "off":
                step_t0 = time.time()
                report_result = build_consolidated_provenance_report(
                    game=game,
                    lang=lang,
                    work_dir=work_dir,
                    token_txt_path=paths["token_txt"],
                    filtered_dic_path=paths["filtered_dic"],
                    munch_provenance_jsonl_path=munch_result.get("munch_provenance_jsonl_path", ""),
                    step4_provenance_jsonl_path=paths["step4_provenance_jsonl"],
                    filter_audit_csv_path=run_result["artifacts"].get("filter_audit_csv", ""),
                    output_formats=provenance_formats or ["csv", "jsonl"],
                )
                run_result["timings"]["step_5b_consolidated_provenance"] = round(time.time() - step_t0, 3)
                run_result["metrics"]["provenance_rows"] = report_result.get("report_rows", 0)

            artifact_t0 = time.time()
            run_result["artifacts"].update({
                "wordforms_csv": os.path.join(work_dir, f"{game}_{lang}_missing_wordforms.csv"),
                "flat_dic": munch_result.get("flat_dic_path"),
                "compressed_dic": munch_result.get("compressed_dic_path"),
                "compressed_aff": munch_result.get("compressed_aff_path"),
                "full_dic_dir": munch_result.get("full_dic_dir"),
                "full_dic": munch_result.get("full_dic_path"),
                "full_aff": munch_result.get("full_aff_path"),
                "casing_csv": munch_result.get("casing_csv_path"),
                "step4_provenance_jsonl": paths["step4_provenance_jsonl"] if os.path.isfile(paths["step4_provenance_jsonl"]) else None,
                "munch_provenance_jsonl": munch_result.get("munch_provenance_jsonl_path"),
                "token_provenance_csv": report_result.get("csv_path"),
                "token_provenance_jsonl": report_result.get("jsonl_path"),
            })
            run_result["metrics"]["munch_stats"] = munch_result.get("stats", {})
            run_result["timings"]["step_6_artifact_pack"] = round(time.time() - artifact_t0, 3)

        except Exception as err:
            run_result["status"] = "failed"
            run_result["error"] = f"{type(err).__name__}: {err}"
            print(f"❌ Failed pair {game}/{lang_input}: {run_result['error']}")

        run_result["timings"]["total"] = round(time.time() - run_t0, 3)
        timed_sum = sum(
            value
            for key, value in run_result["timings"].items()
            if key != "total"
        )
        run_result["timings"]["timed_sum"] = round(timed_sum, 3)
        run_result["timings"]["overhead"] = round(
            run_result["timings"]["total"] - run_result["timings"]["timed_sum"],
            3,
        )
        return run_result

    if effective_pair_workers == 1:
        for pair in pair_specs:
            run_result = _process_pair(pair)
            runs.append(run_result)
            if strict_mode and run_result["status"] == "failed":
                total_elapsed = round(time.time() - t_start, 3)
                return {
                    "runs": runs,
                    "summary": {
                        "status": "failed",
                        "reason": "strict_mode abort on first error",
                        "pair_workers_requested": requested_pair_workers,
                        "pair_workers_effective": effective_pair_workers,
                        "total_pairs": len(pair_specs),
                        "processed_pairs": len(runs),
                        "ok_pairs": sum(1 for r in runs if r["status"] == "ok"),
                        "failed_pairs": [
                            f"{r['game']}/{r['language_input']}"
                            for r in runs
                            if r["status"] == "failed"
                        ],
                        "failed_pairs_count": sum(1 for r in runs if r["status"] == "failed"),
                        "elapsed_seconds": total_elapsed,
                    },
                }
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_pair_workers) as executor:
            futures = [executor.submit(_process_pair, pair) for pair in pair_specs]
            for future in concurrent.futures.as_completed(futures):
                runs.append(future.result())
        runs.sort(key=lambda r: pair_index.get((r["game"], r["language_input"]), 0))

    total_elapsed = round(time.time() - t_start, 3)
    ok_pairs = sum(1 for r in runs if r["status"] == "ok")
    failed_pairs = [
        f"{r['game']}/{r['language_input']}"
        for r in runs
        if r["status"] == "failed"
    ]
    failed_pairs_count = len(failed_pairs)

    summary = {
        "status": "ok" if failed_pairs_count == 0 else "partial",
        "pair_workers_requested": requested_pair_workers,
        "pair_workers_effective": effective_pair_workers,
        "total_pairs": len(games) * len(languages_to_run),
        "processed_pairs": len(runs),
        "ok_pairs": ok_pairs,
        "failed_pairs": failed_pairs,
        "failed_pairs_count": failed_pairs_count,
        "elapsed_seconds": total_elapsed,
    }

    print("\n" + "=" * 80)
    print("BATCH PIPELINE COMPLETE")
    print(f"Status        : {summary['status']}")
    print(f"Processed     : {summary['processed_pairs']}/{summary['total_pairs']}")
    print(f"OK / Failed   : {summary['ok_pairs']} / {summary['failed_pairs_count']}")
    if summary['failed_pairs']:
        print(f"Failed pairs  : {summary['failed_pairs']}")
    print(f"Elapsed (sec) : {summary['elapsed_seconds']}")
    print("=" * 80)

    return {"runs": runs, "summary": summary}


def run_pipeline_single(
    language: str,
    game: str,
    **kwargs,
) -> Dict[str, Any]:
    """Convenience wrapper for one (game, language) pair."""
    result = run_pipeline_batch(languages=[language], games=[game], **kwargs)
    return result["runs"][0] if result.get("runs") else {}


# Example:
# batch_report = run_pipeline_batch(
#     languages=["es", "pt"],
#     games=["TOUCH", "DOFUS"],
#     sample=0,
#     workers=8,
#     batch_size=50,
#     strict_mode=False,
# )
# batch_report["summary"]