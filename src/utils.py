# ANK consolidation from existing exports only (no TB/i18n recomputation)
import os
import glob
import shutil
import codecs
import time
from typing import Any, Dict, List, Set, Tuple, Union

from .batchfiltering import resolve_hunspell_paths
from .filtering import parse_aff_file
from .munching import _build_custom_aff, _extract_raw_aff_blocks
from .params import DIC_FOLDER, GAMES, HUNSPELL_PATHS, INTERMEDIARY_DIR, LANG_CODES, OUTPUT_DIR, i18n_PATHS
from .prepro import normalize_language_code


def erase_intermediary_and_output_dirs(
    keep_filtered_i18n: bool = True,
    intermediary_dir: str = INTERMEDIARY_DIR,
    output_dir: str = OUTPUT_DIR,
) -> Dict[str, Any]:
    """
    Erase folder contents for INTERMEDIARY_DIR and OUTPUT_DIR.

    When keep_filtered_i18n is True, preserve only files named:
      - *_i18n_filtered.json
      - *_i18n_filtered.properties
    inside intermediary_dir.
    """

    def _is_preserved_i18n_filtered(path: str) -> bool:
        base = os.path.basename(path)
        return base.endswith("_i18n_filtered.json") or base.endswith("_i18n_filtered.properties")

    def _clear_dir_contents(root_dir: str, preserve_filtered_i18n: bool) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "root": root_dir,
            "exists": os.path.isdir(root_dir),
            "removed": [],
            "preserved": [],
        }

        if not os.path.isdir(root_dir):
            return result

        for entry in os.listdir(root_dir):
            full_path = os.path.join(root_dir, entry)
            if preserve_filtered_i18n and os.path.isfile(full_path) and _is_preserved_i18n_filtered(full_path):
                result["preserved"].append(full_path)
                continue

            try:
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path)
                else:
                    os.remove(full_path)
                result["removed"].append(full_path)
            except OSError:
                pass

        os.makedirs(root_dir, exist_ok=True)
        return result

    intermediary_result = _clear_dir_contents(
        root_dir=intermediary_dir,
        preserve_filtered_i18n=keep_filtered_i18n,
    )
    output_result = _clear_dir_contents(
        root_dir=output_dir,
        preserve_filtered_i18n=False,
    )

    return {
        "keep_filtered_i18n": keep_filtered_i18n,
        "intermediary": intermediary_result,
        "output": output_result,
        "removed_count": len(intermediary_result["removed"]) + len(output_result["removed"]),
        "preserved_count": len(intermediary_result["preserved"]),
    }


def prewarm_all_available_i18n(
    lang_detect: bool = True,
    min_words: int = 7,
    min_confidence: float = 0.12,
    short_confidence: float = 0.9,
) -> Dict[str, Any]:
    """
    Prewarm all available i18n files by running load_i18n_corpus for each game/lang pair.

    "Available" means a configured i18n path exists on disk or already points to a
    previously filtered file that still exists.
    """
    from .findincorpus import load_i18n_corpus

    t0 = time.time()
    runs: List[Dict[str, Any]] = []

    def _is_filtered_path(game: str, lang: str, path: str) -> bool:
        base = os.path.basename(path)
        return base in {
            f"{game}_{lang}_i18n_filtered.json",
            f"{game}_{lang}_i18n_filtered.properties",
        }

    candidate_pairs: List[Tuple[str, str, str]] = []
    for game, paths in i18n_PATHS.items():
        if not isinstance(paths, dict):
            continue
        for raw_lang, path in paths.items():
            lang = normalize_language_code(str(raw_lang))
            file_path = str(path or "").strip()
            if not file_path:
                continue
            candidate_pairs.append((str(game), lang, file_path))

    # Deduplicate by normalized (game, lang), keep first configured occurrence.
    seen_pairs: Set[Tuple[str, str]] = set()
    unique_pairs: List[Tuple[str, str, str]] = []
    for game, lang, file_path in candidate_pairs:
        key = (game, lang)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        unique_pairs.append((game, lang, file_path))

    runs_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    games_to_process_by_lang: Dict[str, List[str]] = {}

    for game, lang, file_path in unique_pairs:
        run: Dict[str, Any] = {
            "game": game,
            "language": lang,
            "configured_path": file_path,
            "status": "pending",
            "error": "",
            "tokens": 0,
        }

        # Fast path: already-filtered existing files are considered prewarmed.
        if _is_filtered_path(game, lang, file_path) and os.path.exists(file_path):
            run["status"] = "ok"
            run["resolved_path"] = file_path
            run["cached_filtered"] = True
            runs_by_key[(game, lang)] = run
            continue

        # If an unfiltered configured file is missing, skip early.
        if not os.path.exists(file_path) and not _is_filtered_path(game, lang, file_path):
            run["status"] = "skipped"
            run["error"] = f"Configured i18n file not found: {file_path}"
            runs_by_key[(game, lang)] = run
            continue

        # Slow path: process in language batches.
        run["status"] = "pending"
        runs_by_key[(game, lang)] = run
        games_to_process_by_lang.setdefault(lang, []).append(game)

    # Batch by language: one heavy call per language instead of per game/lang pair.
    for lang, games in games_to_process_by_lang.items():
        unique_games = list(dict.fromkeys(games))
        try:
            corpus_map = load_i18n_corpus(
                lang=lang,
                games=unique_games,
                source_type="i18n",
                lang_detect=lang_detect,
                min_words=min_words,
                min_confidence=min_confidence,
                short_confidence=short_confidence,
            )
            batch_tokens = len(corpus_map)
            for game in unique_games:
                run = runs_by_key[(game, lang)]
                run["status"] = "ok"
                run["tokens"] = batch_tokens
                run["batched"] = True
                run["resolved_path"] = (i18n_PATHS.get(game) or {}).get(lang, run["configured_path"])
        except Exception as err:
            err_txt = f"{type(err).__name__}: {err}"
            for game in unique_games:
                run = runs_by_key[(game, lang)]
                run["status"] = "failed"
                run["error"] = err_txt

    # Preserve deterministic output order from configuration scan.
    for game, lang, _ in unique_pairs:
        runs.append(runs_by_key[(game, lang)])

    summary = {
        "status": "ok" if all(r["status"] in {"ok", "skipped"} for r in runs) else "partial",
        "total": len(runs),
        "ok": sum(1 for r in runs if r["status"] == "ok"),
        "skipped": sum(1 for r in runs if r["status"] == "skipped"),
        "failed": sum(1 for r in runs if r["status"] == "failed"),
        "elapsed_seconds": round(time.time() - t0, 3),
    }

    return {
        "runs": runs,
        "summary": summary,
    }


def _write_flatlist_msword_unicode(flat_path: str, words: List[str]) -> str:
    """Write a Word-compatible flatlist (.DIC UTF-16 LE BOM, no count header)."""
    root, _ = os.path.splitext(flat_path)
    dst_path = root + ".DIC"
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    lines = [str(w).strip() for w in words if str(w).strip()]
    out_text = "\r\n".join(lines).rstrip("\r\n") + "\r\n"

    with open(dst_path, "wb") as fh:
        fh.write(codecs.BOM_UTF16_LE)
        fh.write(out_text.encode("utf-16-le"))

    return dst_path


def _supported_lang_codes() -> Set[str]:
    """Return normalized language codes supported by this notebook config."""
    codes: Set[str] = set()
    for v in LANG_CODES.values():
        try:
            codes.add(normalize_language_code(v))
        except Exception:
            pass
    for k in HUNSPELL_PATHS.keys():
        try:
            codes.add(normalize_language_code(k))
        except Exception:
            pass
    return codes


def _parse_lang_game_token(token: str) -> Tuple[str, str] | None:
    """Parse names like 'es_DOFUS' or 'DOFUS_es' into (lang, game)."""
    base = str(token or "").strip()
    if not base:
        return None

    stem = os.path.splitext(base)[0]
    if "_" not in stem:
        return None

    parts = [p for p in stem.split("_") if p]
    if len(parts) < 2:
        return None

    known_langs = _supported_lang_codes()
    known_games = {str(g).upper() for g in GAMES}
    known_games.add("ANK")

    # Orientation 1: lang_game
    try:
        lang_first = normalize_language_code(parts[0])
    except Exception:
        lang_first = ""
    game_rest_1 = "_".join(parts[1:]).upper()
    if lang_first in known_langs and game_rest_1 in known_games:
        return lang_first, game_rest_1

    # Orientation 2: game_lang
    try:
        lang_last = normalize_language_code(parts[-1])
    except Exception:
        lang_last = ""
    game_rest_2 = "_".join(parts[:-1]).upper()
    if lang_last in known_langs and game_rest_2 in known_games:
        return lang_last, game_rest_2

    return None


def _discover_available_export_pairs(output_dir: str = OUTPUT_DIR) -> Dict[str, Any]:
    """
    Discover available (lang, game) pairs from existing exports.
    Source of truth: OUTPUT_DIR/Compressed_dics, with Flatlists as fallback signal.
    """
    compressed_root = os.path.join(output_dir, "Compressed_dics")
    flat_root = os.path.join(output_dir, "Flatlists")

    discovered_pairs: Set[Tuple[str, str]] = set()
    compressed_pairs: Set[Tuple[str, str]] = set()
    flat_pairs: Set[Tuple[str, str]] = set()

    # 1) Discover from compressed bundles (preferred)
    if os.path.isdir(compressed_root):
        for entry in os.listdir(compressed_root):
            full_dir = os.path.join(compressed_root, entry)
            if not os.path.isdir(full_dir):
                continue

            parsed = _parse_lang_game_token(entry)
            if not parsed:
                continue
            lang, game = parsed

            # Accept either file orientation in the bundle folder.
            dic_candidates = [
                os.path.join(full_dir, f"{lang}_{game}.dic"),
                os.path.join(full_dir, f"{game}_{lang}.dic"),
            ]
            if any(os.path.isfile(p) for p in dic_candidates):
                compressed_pairs.add((lang, game))
                discovered_pairs.add((lang, game))

    # 2) Add flatlist-only pairs as fallback
    if os.path.isdir(flat_root):
        for pattern in ("*.DIC", "*.dic"):
            for path in glob.glob(os.path.join(flat_root, pattern)):
                parsed = _parse_lang_game_token(os.path.basename(path))
                if not parsed:
                    continue
                flat_pairs.add(parsed)
                discovered_pairs.add(parsed)

    langs = sorted({lang for lang, _ in discovered_pairs})
    games = sorted({game for _, game in discovered_pairs})

    return {
        "pairs": sorted(discovered_pairs),
        "languages": langs,
        "games": games,
        "from_compressed": sorted(compressed_pairs),
        "from_flatlists": sorted(flat_pairs),
        "compressed_root": compressed_root,
        "flat_root": flat_root,
    }


def _resolve_source_export_paths(lang: str, game: str, output_dir: str = OUTPUT_DIR) -> Dict[str, str | None]:
    """Resolve existing source files for one (lang, game) export pair."""
    compressed_root = os.path.join(output_dir, "Compressed_dics")
    flat_root = os.path.join(output_dir, "Flatlists")

    bundle_candidates = [
        os.path.join(compressed_root, f"{lang}_{game}"),
        os.path.join(compressed_root, f"{game}_{lang}"),
    ]

    dic_candidates: List[str] = []
    aff_candidates: List[str] = []
    for bdir in bundle_candidates:
        dic_candidates.extend([
            os.path.join(bdir, f"{lang}_{game}.dic"),
            os.path.join(bdir, f"{game}_{lang}.dic"),
        ])
        aff_candidates.extend([
            os.path.join(bdir, f"{lang}_{game}.aff"),
            os.path.join(bdir, f"{game}_{lang}.aff"),
        ])

    flat_candidates = [
        os.path.join(flat_root, f"{game}_{lang}.DIC"),
        os.path.join(flat_root, f"{lang}_{game}.DIC"),
        os.path.join(flat_root, f"{game}_{lang}.dic"),
        os.path.join(flat_root, f"{lang}_{game}.dic"),
    ]

    dic_path = next((p for p in dic_candidates if os.path.isfile(p)), None)
    aff_path = next((p for p in aff_candidates if os.path.isfile(p)), None)
    flat_path = next((p for p in flat_candidates if os.path.isfile(p)), None)

    return {
        "compressed_dic": dic_path,
        "compressed_aff": aff_path,
        "flat_dic": flat_path,
    }


def _read_hunspell_dic_entries(dic_path: str) -> List[Tuple[str, Set[str]]]:
    """Read .dic/.DIC lines as (word, flags_set), skipping count header if present."""
    entries: List[Tuple[str, Set[str]]] = []
    if not dic_path or not os.path.isfile(dic_path):
        return entries

    with open(dic_path, "rb") as fh:
        raw = fh.read()

    text = None
    if raw.startswith(codecs.BOM_UTF16_LE):
        try:
            text = raw[len(codecs.BOM_UTF16_LE):].decode("utf-16-le")
        except UnicodeDecodeError:
            text = None

    if text is None:
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue

    if text is None:
        text = raw.decode("latin-1", errors="replace")

    raw_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    if raw_lines and raw_lines[0].isdigit():
        data_lines = raw_lines[1:]
    else:
        data_lines = raw_lines

    for line in data_lines:
        base, _, tail = line.partition("/")
        word = base.strip()
        if not word:
            continue

        flags: Set[str] = set()
        tail = tail.strip()
        if tail:
            if "," in tail:
                flags.update([p.strip() for p in tail.split(",") if p.strip()])
            else:
                flags.update(list(tail))

        entries.append((word, flags))

    return entries


def _choose_surface(existing: str, candidate: str) -> str:
    """Deterministically choose a representative surface for one lowercase lemma."""
    if not existing:
        return candidate
    if not candidate:
        return existing
    if existing == candidate:
        return existing

    ex_cap = existing[0].isupper()
    ca_cap = candidate[0].isupper()
    if ex_cap != ca_cap:
        return candidate if ca_cap else existing

    # Stable tie-breaker: shorter first, then case-insensitive lexical.
    ex_key = (len(existing), existing.casefold(), existing)
    ca_key = (len(candidate), candidate.casefold(), candidate)
    return candidate if ca_key < ex_key else existing


def _merge_source_game_exports(
    lang: str,
    games: List[str],
    output_dir: str = OUTPUT_DIR,
    strict_mode: bool = False,
    source_type: str = "compressed",
) -> Dict[str, Any]:
    """Merge all selected source game exports for one language."""
    merged: Dict[str, Dict[str, Any]] = {}
    source_files: List[str] = []
    missing_pairs: List[str] = []

    if source_type not in {"compressed", "flat"}:
        raise ValueError("source_type must be 'compressed' or 'flat'")

    for game in games:
        if game == "ANK":
            continue

        paths = _resolve_source_export_paths(lang=lang, game=game, output_dir=output_dir)

        chosen = paths["compressed_dic"] if source_type == "compressed" else paths["flat_dic"]
        if chosen:
            source_files.append(chosen)
            source_entries = _read_hunspell_dic_entries(chosen)
        else:
            missing_pairs.append(f"{game}/{lang}")
            if strict_mode:
                raise FileNotFoundError(
                    f"No {source_type} export dictionary found for source pair {game}/{lang}"
                )
            continue

        for word, flags in source_entries:
            key = word
            if key not in merged:
                merged[key] = {
                    "surface": word,
                    "flags": set(flags),
                    "sources": {game},
                }
            else:
                merged[key]["flags"].update(flags)
                merged[key]["sources"].add(game)

    used_flags: Set[str] = set()
    for row in merged.values():
        used_flags.update(row["flags"])

    return {
        "lang": lang,
        "entries": merged,
        "source_files": sorted(set(source_files)),
        "missing_pairs": missing_pairs,
        "used_flags": used_flags,
        "entry_count": len(merged),
    }


def _copy_tree_no_overwrite(src_dir: str, dst_dir: str) -> int:
    """Copy files recursively, preserving existing destination files."""
    copied = 0
    for root, _, files in os.walk(src_dir):
        rel_root = os.path.relpath(root, src_dir)
        target_root = dst_dir if rel_root == "." else os.path.join(dst_dir, rel_root)
        os.makedirs(target_root, exist_ok=True)
        for name in files:
            src_file = os.path.join(root, name)
            dst_file = os.path.join(target_root, name)
            if os.path.exists(dst_file):
                continue
            shutil.copy2(src_file, dst_file)
            copied += 1
    return copied


def _write_ank_language_exports(
    lang: str,
    merged_rows: Dict[str, Dict[str, Any]],
    used_flags: Set[str],
    output_dir: str = OUTPUT_DIR,
) -> Dict[str, Any]:
    """Write ANK consolidated outputs for one language."""
    if not merged_rows:
        raise ValueError(f"No merged rows to export for language '{lang}'")

    out_flat = os.path.join(output_dir, "Flatlists")
    out_comp = os.path.join(output_dir, "Compressed_dics")
    out_full = os.path.join(output_dir, "Full_dics")
    os.makedirs(out_flat, exist_ok=True)
    os.makedirs(out_comp, exist_ok=True)
    os.makedirs(out_full, exist_ok=True)

    game_tag = "ANK"
    flat_key = f"{game_tag}_{lang}"   # keep current flat naming convention
    pair_key = f"{lang}_{game_tag}"   # keep current compressed/full naming convention

    sorted_items = sorted(merged_rows.values(), key=lambda r: r["surface"].casefold())

    # Flatlist (Word-compatible by default)
    flat_path = _write_flatlist_msword_unicode(
        os.path.join(out_flat, f"{flat_key}.DIC"),
        [row["surface"] for row in sorted_items],
    )

    # Resolve reference Hunspell dictionary for this language.
    resolved = resolve_hunspell_paths(lang, dic_folder=DIC_FOLDER)
    if not resolved.get("ok"):
        raise FileNotFoundError(
            f"Cannot resolve Hunspell dic+aff for '{lang}' while writing ANK export"
        )
    std_dic_path = resolved["dic"]
    std_aff_path = resolved["aff"]

    affixes = parse_aff_file(std_aff_path)
    aff_blocks = _extract_raw_aff_blocks(std_aff_path)
    flag_mode = affixes.get("flag_mode", "single")

    def _flags_to_str(flags: Set[str]) -> str:
        if not flags:
            return ""
        if flag_mode == "num":
            return "/" + ",".join(sorted(flags))
        return "/" + "".join(sorted(flags))

    # Compressed bundle
    comp_dir = os.path.join(out_comp, pair_key)
    os.makedirs(comp_dir, exist_ok=True)
    comp_dic_path = os.path.join(comp_dir, f"{pair_key}.dic")
    comp_aff_path = os.path.join(comp_dir, f"{pair_key}.aff")

    with open(comp_dic_path, "w", encoding="utf-8") as fh:
        fh.write(f"{len(sorted_items)}\n")
        for row in sorted_items:
            fh.write(row["surface"] + _flags_to_str(set(row["flags"])) + "\n")

    custom_aff_text = _build_custom_aff(std_aff_path, set(used_flags), lang, aff_blocks)
    with open(comp_aff_path, "w", encoding=aff_blocks["encoding"]) as fh:
        fh.write(custom_aff_text)

    # Full bundle: copy reference files + generated pair
    full_dir = os.path.join(out_full, pair_key)
    os.makedirs(full_dir, exist_ok=True)
    copied_ref_files = _copy_tree_no_overwrite(os.path.dirname(std_dic_path), full_dir)

    full_dic_path = os.path.join(full_dir, f"{pair_key}.dic")
    full_aff_path = os.path.join(full_dir, f"{pair_key}.aff")
    shutil.copy2(comp_dic_path, full_dic_path)
    shutil.copy2(comp_aff_path, full_aff_path)

    return {
        "flat_dic_path": flat_path,
        "compressed_dic_path": comp_dic_path,
        "compressed_aff_path": comp_aff_path,
        "full_dic_dir": full_dir,
        "full_dic_path": full_dic_path,
        "full_aff_path": full_aff_path,
        "copied_ref_files": copied_ref_files,
        "stats": {
            "entries": len(sorted_items),
            "flags_used": sorted(used_flags),
        },
    }


def _write_ank_flat_only(
    lang: str,
    merged_rows: Dict[str, Dict[str, Any]],
    output_dir: str = OUTPUT_DIR,
) -> str:
    """Write ANK flatlist from pre-merged rows only (Word-compatible)."""
    out_flat = os.path.join(output_dir, "Flatlists")
    os.makedirs(out_flat, exist_ok=True)
    sorted_items = sorted(merged_rows.values(), key=lambda r: r["surface"].casefold())
    return _write_flatlist_msword_unicode(
        os.path.join(out_flat, f"ANK_{lang}.DIC"),
        [row["surface"] for row in sorted_items],
    )


def _normalize_selector_games(games: Union[List[str], str], discovered_games: List[str]) -> List[str]:
    """Normalize games selector to an uppercase list, with all support."""
    if isinstance(games, str):
        token = games.strip().lower()
        if token == "all":
            return [g for g in discovered_games if g != "ANK"]
        if not token:
            return []
        return [games.strip().upper()]

    cleaned = [str(g).strip().upper() for g in games if str(g).strip()]
    return [g for g in cleaned if g != "ANK"]


def _normalize_selector_languages(
    languages: Union[List[str], str],
    discovered_languages: List[str],
) -> List[str]:
    """Normalize language selector to canonical codes, with all support."""
    if isinstance(languages, str):
        token = languages.strip().lower()
        if token == "all":
            return discovered_languages
        if not token:
            return []
        return [normalize_language_code(token)]

    normalized: List[str] = []
    for lang in languages:
        txt = str(lang).strip()
        if not txt:
            continue
        normalized.append(normalize_language_code(txt))

    # Preserve order and deduplicate.
    return list(dict.fromkeys(normalized))


def consolidate_ank_exports_by_language(
    games: Union[List[str], str] = "all",
    languages: Union[List[str], str] = "all",
    output_dir: str = OUTPUT_DIR,
    strict_mode: bool = False,
) -> Dict[str, Any]:
    """
    Consolidate existing game exports by language into synthetic ANK exports.

    Input scope restriction:
      - Reads only existing exports under output_dir (Compressed_dics/Flatlists).
      - Does not read TB, i18n corpus, or intermediary tokenization files.

    Args:
      games: source games list or 'all' discovered from exports
      languages: language code, list of codes, or 'all' discovered from exports
      output_dir: root output folder (default OUTPUT_DIR)
      strict_mode: fail fast on missing source pairs when True

    Returns:
      Dict with runs[] and summary, similar to run_pipeline_batch().
    """
    t0 = time.time()

    discovery = _discover_available_export_pairs(output_dir=output_dir)
    discovered_games = discovery["games"]
    discovered_languages = discovery["languages"]

    games_to_run = _normalize_selector_games(games, discovered_games)
    langs_to_run = _normalize_selector_languages(languages, discovered_languages)

    if not games_to_run:
        raise ValueError("No source games selected/found in exports")
    if not langs_to_run:
        raise ValueError("No languages selected/found in exports")

    print("=" * 80)
    print("ANK CONSOLIDATION FROM EXISTING EXPORTS")
    print("ANK CONSOLIDATION FROM EXISTING EXPORTS")
    print(f"Output dir        : {output_dir}")
    print(f"Available games   : {discovered_games}")
    print(f"Available langs   : {discovered_languages}")
    print(f"Selected games    : {games_to_run}")
    print(f"Selected languages: {langs_to_run}")
    print(f"Strict mode       : {strict_mode}")
    print("=" * 80)

    runs: List[Dict[str, Any]] = []
    runs: List[Dict[str, Any]] = []
    for lang in langs_to_run:
        run_t0 = time.time()
        run_result: Dict[str, Any] = {
            "language": lang,
            "target_game": "ANK",
            "status": "ok",
            "error": "",
            "source_games": games_to_run,
            "source_files": [],
            "missing_pairs": [],
            "artifacts": {},
            "metrics": {},
            "timings": {},
        }

        print("\n" + "-" * 80)
        print(f"▶ Consolidating language={lang} into ANK")
        print("-" * 80)

        try:
            merged_comp = _merge_source_game_exports(
                lang=lang,
                games=games_to_run,
                output_dir=output_dir,
                strict_mode=strict_mode,
                source_type="compressed",
            )
            merged_flat = _merge_source_game_exports(
                lang=lang,
                games=games_to_run,
                output_dir=output_dir,
                strict_mode=False,
                source_type="flat",
            )

            if merged_comp["entry_count"] == 0:
                raise ValueError(
                    f"No compressed source entries found for language '{lang}' with selected games"
                )

            write_result = _write_ank_language_exports(
                lang=lang,
                merged_rows=merged_comp["entries"],
                used_flags=merged_comp["used_flags"],
                output_dir=output_dir,
            )
            if merged_flat["entry_count"] > 0:
                write_result["flat_dic_path"] = _write_ank_flat_only(
                    lang=lang,
                    merged_rows=merged_flat["entries"],
                    output_dir=output_dir,
                )

            run_result["source_files"] = sorted(set(merged_comp["source_files"] + merged_flat["source_files"]))
            run_result["missing_pairs"] = sorted(set(merged_comp["missing_pairs"] + merged_flat["missing_pairs"]))
            run_result["artifacts"].update(write_result)
            run_result["metrics"]["merged_entries_compressed"] = merged_comp["entry_count"]
            run_result["metrics"]["merged_entries_flat"] = merged_flat["entry_count"]
            run_result["metrics"]["flags_used"] = sorted(merged_comp["used_flags"])
            run_result["metrics"]["source_files_count"] = len(run_result["source_files"])
            run_result["metrics"]["missing_pairs_count"] = len(run_result["missing_pairs"])

            print(f"  Source files used (all): {len(run_result['source_files'])}")
            if run_result["missing_pairs"]:
                print(f"  Missing source pairs: {len(run_result['missing_pairs'])}")
            print(f"  Merged entries (compressed): {merged_comp['entry_count']:,}")
            print(f"  Merged entries (flat)      : {merged_flat['entry_count']:,}")
            print(f"  Flat dic     -> {write_result['flat_dic_path']}")
            print(f"  Compressed   -> {write_result['compressed_dic_path']}")
            print(f"  Full bundle  -> {write_result['full_dic_dir']}")

        except Exception as err:
            run_result["status"] = "failed"
            run_result["error"] = f"{type(err).__name__}: {err}"
            print(f"❌ Failed consolidation for lang={lang}: {run_result['error']}")
            if strict_mode:
                run_result["timings"]["total"] = round(time.time() - run_t0, 3)
                runs.append(run_result)
                elapsed = round(time.time() - t0, 3)
                return {
                    "runs": runs,
                    "summary": {
                        "status": "failed",
                        "reason": "strict_mode abort on first error",
                        "processed_languages": len(runs),
                        "ok_languages": sum(1 for r in runs if r["status"] == "ok"),
                        "failed_languages": sum(1 for r in runs if r["status"] == "failed"),
                        "elapsed_seconds": elapsed,
                    },
                }

        run_result["timings"]["total"] = round(time.time() - run_t0, 3)
        runs.append(run_result)

    elapsed = round(time.time() - t0, 3)
    ok_languages = sum(1 for r in runs if r["status"] == "ok")
    failed_languages = sum(1 for r in runs if r["status"] == "failed")

    summary = {
        "status": "ok" if failed_languages == 0 else "partial",
        "selected_games": games_to_run,
        "selected_languages": langs_to_run,
        "processed_languages": len(runs),
        "ok_languages": ok_languages,
        "failed_languages": failed_languages,
        "elapsed_seconds": elapsed,
    }

    print("\n" + "=" * 80)
    print("ANK CONSOLIDATION COMPLETE")
    print(f"Status        : {summary['status']}")
    print(f"Processed     : {summary['processed_languages']}")
    print(f"OK / Failed   : {summary['ok_languages']} / {summary['failed_languages']}")
    print(f"Elapsed (sec) : {summary['elapsed_seconds']}")
    print("=" * 80)

    return {"runs": runs, "summary": summary}
    return {"runs": runs, "summary": summary}

# Examples:
# ank_report = consolidate_ank_exports_by_language(games="all", languages="all")
# ank_report = consolidate_ank_exports_by_language(games=["DOFUS", "WAKFU"], languages=["es", "pt"])