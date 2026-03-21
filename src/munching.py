# ==============================================================================
# 5.  MUNCH — Assemble flat + compressed dic from wordform results
# ==============================================================================
# Flag attribution (validation/verb) is done inside _wordform_match_worker()
# during find_corpus_wordforms().  This cell only does:
#   - mandatory flag assignment (no corpus check)
#   - casing inference + CSV export
#   - file I/O: flat .dic, compressed .dic + .aff

import os
import re
from time import time as _time_now
from typing import Dict, List, Optional, Set

from .filtering import generate_word_forms, parse_aff_file
from .params import (
    HUNSPELL_PATHS,
    INTERMEDIARY_DIR,
    MUNCH_FLAG_CONFIG,
    OUTPUT_COMPRESSED_DIR,
    OUTPUT_FLATLISTS_DIR,
    OUTPUT_FULL_DIR,
)
from .prepro import _apply_ankanimation_token_overrides


_EN_POSSESSIVE_RE = re.compile(r".*(?:'|’)s$", re.IGNORECASE)


def _is_english_possessive_form(token: str) -> bool:
    text = str(token or '').strip()
    return bool(text) and bool(_EN_POSSESSIVE_RE.match(text))


# ── Helper: extract raw AFF text blocks ──────────────────────────────────────

def _extract_raw_aff_blocks(aff_path: str) -> Dict:
    """
    Read a Hunspell .aff file and return:
      { 'header_lines': [str, ...],
        'SFX': {flag: [raw_lines]},
        'PFX': {flag: [raw_lines]},
        'encoding': str }
    Header lines are everything that is NOT a PFX/SFX line.
    """
    # Detect encoding
    _enc = 'utf-8'
    try:
        with open(aff_path, 'rb') as fb:
            for raw_line in fb:
                text = raw_line.decode('ascii', errors='ignore').strip()
                if text.upper().startswith('SET '):
                    _enc_name = text.split()[1].strip()
                    _enc = _enc_name.replace('ISO8859-', 'iso-8859-')
                    break
    except Exception:
        pass

    header_lines: List[str] = []
    sfx_blocks: Dict[str, List[str]] = {}
    pfx_blocks: Dict[str, List[str]] = {}

    with open(aff_path, 'r', encoding=_enc, errors='replace') as fh:
        for line in fh:
            stripped = line.rstrip('\n\r')
            parts = stripped.split()
            if len(parts) >= 2 and parts[0] in ('SFX', 'PFX'):
                rule_type = parts[0]
                flag = parts[1]
                bucket = sfx_blocks if rule_type == 'SFX' else pfx_blocks
                bucket.setdefault(flag, []).append(stripped)
            else:
                header_lines.append(stripped)

    return {
        'header_lines': header_lines,
        'SFX': sfx_blocks,
        'PFX': pfx_blocks,
        'encoding': _enc,
    }


def _normalize_aff_header_lines(header_lines: List[str]) -> List[str]:
    """
    Normalize AFF header directives used by exported game dictionaries:
      - WORDCHARS must include apostrophe, hyphen, and digits 0-9
      - TRY must include apostrophe and hyphen

    Existing directive content and order are preserved; only missing required
    characters are appended.
    """
    required_wordchars = "'-0123456789"
    required_try_chars = "-'"

    normalized: List[str] = []
    seen_wordchars = False
    seen_try = False

    for line in header_lines:
        stripped = line.strip()
        if not stripped:
            normalized.append(line)
            continue

        parts = stripped.split(None, 1)
        directive = parts[0].upper() if parts else ""

        if directive == 'WORDCHARS':
            seen_wordchars = True
            existing_chars = parts[1] if len(parts) > 1 else ""
            merged = existing_chars
            for ch in required_wordchars:
                if ch not in merged:
                    merged += ch
            normalized.append(f"WORDCHARS {merged}")
            continue

        if directive == 'TRY':
            seen_try = True
            existing_chars = parts[1] if len(parts) > 1 else ""
            merged = existing_chars
            for ch in required_try_chars:
                if ch not in merged:
                    merged += ch
            normalized.append(f"TRY {merged}")
            continue

        normalized.append(line)

    if not seen_wordchars:
        normalized.append(f"WORDCHARS {required_wordchars}")

    if not seen_try:
        normalized.append(f"TRY {required_try_chars}")

    return normalized


def _build_custom_aff(std_aff_path: str, used_flags: Set[str], lang: str,
                      aff_blocks: Optional[Dict] = None) -> str:
    """
    Build a custom .aff file string containing:
      - A normalized header from the standard .aff
      - Only the SFX/PFX blocks whose flag appears in `used_flags`.

    Header normalization keeps source directives intact while ensuring
    WORDCHARS and TRY include required punctuation for game terms.
    """
    if aff_blocks is None:
        aff_blocks = _extract_raw_aff_blocks(std_aff_path)

    parts: List[str] = []

    # ── Header ────────────────────────────────────────────────────────────────
    for hl in _normalize_aff_header_lines(aff_blocks['header_lines']):
        parts.append(hl)

    # ── SFX blocks (in original order) ────────────────────────────────────────
    for flag, lines in aff_blocks['SFX'].items():
        if flag in used_flags:
            for ln in lines:
                parts.append(ln)

    # ── PFX blocks ────────────────────────────────────────────────────────────
    for flag, lines in aff_blocks['PFX'].items():
        if flag in used_flags:
            for ln in lines:
                parts.append(ln)

    return '\n'.join(parts) + '\n'


# ── Helper: casing inference ─────────────────────────────────────────────────

def _infer_casing_variants(
    base_words: Set[str],
    corpus_exact_lowers: Set[str],
    propernoun_tokens: Set[str],
    use_propernoun_tokens: bool = True,
) -> List[Dict]:
    """
    For each Capitalised base word, check if its lowercase version appears
    in ``corpus_exact_lowers`` (exact lowercase evidence) and optionally in
    ``propernoun_tokens``.  Returns a list of dicts
    ``[{ 'original': 'Bwork', 'inferred_lower': 'bwork', 'source': '...' }, ...]``
    for export to a review CSV.
    """
    inferences: List[Dict] = []
    propernoun_lower = {t.lower() for t in propernoun_tokens}

    for bw in sorted(base_words):
        if not bw or not bw[0].isupper():
            continue
        lw = bw.lower()
        if lw == bw:
            continue  # already all-lower (e.g. single char)
        if lw in base_words:
            continue  # lowercase version already in dic explicitly

        source_parts = []
        if lw in corpus_exact_lowers:
            source_parts.append('corpus_exact_lower')
        if use_propernoun_tokens and lw in propernoun_lower:
            source_parts.append('propernoun_sidecar')
        if source_parts:
            inferences.append({
                'original': bw,
                'inferred_lower': lw,
                'source': ' + '.join(source_parts),
            })

    return inferences


# ══════════════════════════════════════════════════════════════════════════════
# MAIN FUNCTION — Assembly-only (flag attribution done in find_corpus_wordforms)
# ══════════════════════════════════════════════════════════════════════════════

def munch_to_compressed_dic(
    dic_path: str,
    lang: str,
    games: List[str],
    wordform_results: List[dict],
    propernoun_sidecar: str | None = None,
    filter_audit_csv_path: str | None = None,
    provenance_level: str = 'off',
    provenance_output_folder: str | None = None,
    final_output_folder: str | None = None,
) -> Dict:
    """
    Assemble filtered_tokens.dic + corpus-confirmed wordform results into:
      1. A flat consolidated .dic  (all words, one per line)
      2. A compressed .dic + .aff  (base words with affix flags)
      3. A casing-inference CSV     (capitalised → lowercase review)

    Flag attribution (validation/verb flags) has already been computed by
    ``find_corpus_wordforms()`` → ``_wordform_match_worker()`` and is
    available in ``wordform_results[i]['validated_flags']``.  This function
    only adds **mandatory** flags (from ``MUNCH_FLAG_CONFIG``), prunes
    redundant explicit forms covered by flagged entries, and writes outputs.

    Args:
        dic_path:           Path to filtered_tokens.dic (base words)
        lang:               Language code (es, de, en, pt, fr)
        games:              List of game tags (for file naming)
        wordform_results:   The list returned by find_corpus_wordforms()
        propernoun_sidecar: Path to propernoun sidecar JSON (for casing context)
        filter_audit_csv_path: Optional Step 2-3 audit CSV path used to
            suppress tokens marked as removed_known_word from re-entering
            outputs via Step 4 corpus-derived forms.

    Returns:
        Dict with keys: flat_dic_path, compressed_dic_path, compressed_aff_path,
                        casing_csv_path, stats
    """
    import csv as _csv
    import json as _json
    import shutil as _shutil
    t0 = _time_now()
    games_tag = "_".join(games)
    game_lang_key = f"{games_tag}_{lang}"
    lang_game_key = f"{lang}_{games_tag}"

    _prov_level = str(provenance_level or 'off').strip().lower()

    # ── 1. Load base dic words ────────────────────────────────────────────────
    with open(dic_path, 'r', encoding='utf-8-sig') as fh:
        lines = fh.read().splitlines()
    base_words_raw: List[str] = []
    for ln in lines[1:]:
        w = ln.strip().split('/')[0].strip()
        if w:
            base_words_raw.append(w)
    base_words_set = set(base_words_raw)
    base_words_original = set(base_words_set)
    print(f"  Loaded {len(base_words_set):,} base words from {os.path.basename(dic_path)}")

    # ── 2. Build confirmed-forms set from wordform_results ────────────────────
    # Also build a per-word validated_flags map
    family_map: Dict[str, Set[str]] = {}
    validated_flags_map: Dict[str, List[str]] = {}
    corpus_exact_lowers: Set[str] = set()
    form_lineage: Dict[str, Set[str]] = {}
    form_parents: Dict[str, Set[str]] = {}

    for bw in base_words_original:
        form_lineage.setdefault(bw, set()).add('tb_filtered_base')
        form_parents.setdefault(bw, set()).add(bw)

    for r in wordform_results:
        bw = r['base_word']
        forms: Set[str] = set()
        for col in ('found_forms', 'new_found_forms',
                     'ghost_forms', 'new_ghost_forms'):
            for f in r.get(col, []):
                if isinstance(f, str) and f.strip():
                    clean_f = f.strip()
                    forms.add(clean_f)
                    if col in ('ghost_forms', 'new_ghost_forms'):
                        form_lineage.setdefault(clean_f, set()).add('corpus_ghost_form')
                    else:
                        form_lineage.setdefault(clean_f, set()).add('corpus_aff_form')
                    form_parents.setdefault(clean_f, set()).add(bw)
                    if clean_f == clean_f.lower():
                        corpus_exact_lowers.add(clean_f)
        if forms:
            family_map.setdefault(bw, set()).update(forms)
        vf = r.get('validated_flags', [])
        if vf:
            validated_flags_map[bw] = list(vf)
    print(f"  Wordform results : {len(wordform_results):,} words, "
          f"{len(validated_flags_map):,} with validated flags")
    print(f"  Exact lowercase corpus forms: {len(corpus_exact_lowers):,}")

    # ── 3. Load propernoun sidecar (for casing context) ───────────────────────
    propernoun_tokens: Set[str] = set()
    if propernoun_sidecar and os.path.isfile(propernoun_sidecar):
        with open(propernoun_sidecar, 'r', encoding='utf-8') as fh:
            sidecar_data = _json.load(fh)
        if isinstance(sidecar_data, dict) and isinstance(sidecar_data.get('by_category'), dict):
            category_map = sidecar_data.get('by_category', {})
        else:
            category_map = sidecar_data
        if isinstance(category_map, dict):
            for _, tokens in category_map.items():
                if isinstance(tokens, list):
                    propernoun_tokens.update(str(tok).strip().lower() for tok in tokens if str(tok).strip())
        print(f"  Loaded {len(propernoun_tokens):,} propernoun tokens from sidecar")

    # ── 4. Build the flat confirmed-forms set ─────────────────────────────────
    all_confirmed: Set[str] = set(base_words_set)
    for forms in family_map.values():
        all_confirmed.update(forms)

    # Defense-in-depth for English: block trailing apostrophe-s forms from
    # leaking into Step 5 outputs even if upstream artifacts contain them.
    removed_possessive_count = 0
    if lang == 'en':
        blocked = {tok for tok in all_confirmed if _is_english_possessive_form(tok)}
        if blocked:
            all_confirmed -= blocked
            base_words_set -= blocked
            base_words_original -= blocked
            for tok in blocked:
                form_lineage.pop(tok, None)
                form_parents.pop(tok, None)
            removed_possessive_count = len(blocked)
            print(f"  English possessive guard removed: {removed_possessive_count:,}")

    # Language-agnostic known-word rebound guard:
    # if Step 2-3 explicitly removed a token as a known dictionary word,
    # keep it out of Step 5 outputs even when Step 4 sees corpus-attested forms.
    removed_known_word_lowers: Set[str] = set()
    if filter_audit_csv_path and os.path.isfile(filter_audit_csv_path):
        try:
            with open(filter_audit_csv_path, 'r', encoding='utf-8-sig', newline='') as _fh:
                _reader = _csv.DictReader(_fh)
                for _row in _reader:
                    _status = str(_row.get('status', '')).strip()
                    if _status != 'removed_known_word':
                        continue
                    _tok_lower = str(_row.get('token_lower', '')).strip().lower()
                    if not _tok_lower:
                        _tok_lower = str(_row.get('token', '')).strip().lower()
                    if _tok_lower:
                        removed_known_word_lowers.add(_tok_lower)
        except Exception as audit_err:
            print(f"  WARNING: failed loading filter audit for known-word guard: {audit_err}")

    removed_known_word_rebound_count = 0
    if removed_known_word_lowers:
        rebound_tokens = {tok for tok in all_confirmed if tok.lower() in removed_known_word_lowers}
        if rebound_tokens:
            all_confirmed -= rebound_tokens
            base_words_set -= rebound_tokens
            base_words_original -= rebound_tokens
            for tok in rebound_tokens:
                form_lineage.pop(tok, None)
                form_parents.pop(tok, None)
            removed_known_word_rebound_count = len(rebound_tokens)
            print(f"  Known-word rebound guard removed: {removed_known_word_rebound_count:,}")
    print(f"  Total confirmed forms (base + inflected): {len(all_confirmed):,}")

    # ── 5. Casing inference ───────────────────────────────────────────────────
    _use_sidecar_for_casing = False
    casing_inferences = _infer_casing_variants(
        base_words_set, corpus_exact_lowers, propernoun_tokens,
        use_propernoun_tokens=_use_sidecar_for_casing
    )
    for inf in casing_inferences:
        all_confirmed.add(inf['inferred_lower'])
        base_words_set.add(inf['inferred_lower'])
        form_lineage.setdefault(inf['inferred_lower'], set()).add('casing_inference')
        form_parents.setdefault(inf['inferred_lower'], set()).add(inf['original'])
    print(f"  Casing inferences: {len(casing_inferences):,} "
          f"(Capitalised → lowercase)")

    # Re-apply ANKANIMATION ES custom overrides at assembly time so corpus-found
    # capitalized variants do not leak back into Flatlists exports.
    if lang == 'es' and any(str(g).strip().upper() == 'ANKANIMATION' for g in games):
        _before_base = len(base_words_set)
        _before_all = len(all_confirmed)
        base_words_set = _apply_ankanimation_token_overrides(base_words_set, 'es')
        all_confirmed = _apply_ankanimation_token_overrides(all_confirmed, 'es')
        all_confirmed.update(base_words_set)
        print(
            f"  [ankassembly-overrides] base={_before_base:,}->{len(base_words_set):,} "
            f"all={_before_all:,}->{len(all_confirmed):,}"
        )

    # ── 6. Parse standard AFF (for file structure only) ──────────────────────
    std_dic_path = HUNSPELL_PATHS.get(lang)
    if not std_dic_path:
        print(f"  ⚠ No HUNSPELL_PATHS entry for '{lang}' — skipping munch")
        return {}
    std_aff_path = std_dic_path.rsplit('.', 1)[0] + '.aff'
    if not os.path.isfile(std_aff_path):
        print(f"  ⚠ AFF file not found: {std_aff_path}")
        return {}
    affixes = parse_aff_file(std_aff_path)
    aff_blocks = _extract_raw_aff_blocks(std_aff_path)
    print(f"  Parsed {std_aff_path}  (for flag_mode + AFF block extraction)")

    # ── 7. Assemble per-word flags: mandatory + validated ─────────────────────
    flag_cfg = MUNCH_FLAG_CONFIG.get(lang, {"mandatory": [], "validation": [], "verb": []})
    available_flags = set(affixes['SFX'].keys()) | set(affixes['PFX'].keys())
    mandatory_flags = set(flag_cfg['mandatory']) & available_flags

    compressed_entries: Dict[str, Set[str]] = {}
    flag_usage_count: Dict[str, int] = {}

    for bw in sorted(base_words_set):
        assigned: Set[str] = set(mandatory_flags)

        # Add validated flags from corpus results (already quorum-checked)
        # Check both the exact key and case variants
        for key_variant in (bw, bw.lower(), bw[0].upper() + bw[1:] if bw else ''):
            for fl in validated_flags_map.get(key_variant, []):
                assigned.add(fl)

        if assigned:
            compressed_entries[bw] = assigned
            for fl in assigned:
                flag_usage_count[fl] = flag_usage_count.get(fl, 0) + 1

    used_flags_all = set()
    for flags in compressed_entries.values():
        used_flags_all.update(flags)

    _total_words = len(base_words_set)
    print(f"\n  Munch complete (assembly-only — no re-generation):")
    print(f"    Mandatory flags : {sorted(mandatory_flags) or '(none)'}")
    print(f"    Words with flags: {sum(1 for f in compressed_entries.values() if f):,} "
          f"/ {_total_words:,}")
    print(f"    Flags used      : {sorted(used_flags_all)}")
    for fl in sorted(used_flags_all):
        print(f"      {fl!r:>6}  →  {flag_usage_count.get(fl, 0):>6,} words")

    # ── 8. Output files ──────────────────────────────────────────────────────
    final_root = str(final_output_folder).strip() if final_output_folder else ''
    if final_root:
        out_flatlists_dir = os.path.join(final_root, 'Flatlists')
        out_compressed_dir = os.path.join(final_root, 'Compressed_dics')
        out_full_dir = os.path.join(final_root, 'Full_dics')
    else:
        out_flatlists_dir = OUTPUT_FLATLISTS_DIR
        out_compressed_dir = OUTPUT_COMPRESSED_DIR
        out_full_dir = OUTPUT_FULL_DIR

    os.makedirs(out_flatlists_dir, exist_ok=True)
    os.makedirs(out_compressed_dir, exist_ok=True)
    os.makedirs(out_full_dir, exist_ok=True)
    os.makedirs(INTERMEDIARY_DIR, exist_ok=True)

    compressed_bundle_dir = os.path.join(out_compressed_dir, lang_game_key)
    full_bundle_dir = os.path.join(out_full_dir, lang_game_key)
    os.makedirs(compressed_bundle_dir, exist_ok=True)
    os.makedirs(full_bundle_dir, exist_ok=True)

    # 8a. Flat consolidated .DIC (Word-compatible by default)
    flat_words = sorted(all_confirmed, key=str.lower)
    flat_dic_path = os.path.join(out_flatlists_dir, f"{game_lang_key}.DIC")
    _flat_lines = [str(w).strip() for w in flat_words if str(w).strip()]
    _flat_text = "\r\n".join(_flat_lines).rstrip("\r\n") + "\r\n"
    with open(flat_dic_path, 'wb') as fh:
        fh.write(b"\xff\xfe")
        fh.write(_flat_text.encode('utf-16-le'))
    print(f"\n  Flat dic     → {flat_dic_path}  ({len(flat_words):,} words)")

    # 8b. Compressed .dic
    compressed_dic_path = os.path.join(compressed_bundle_dir,
                                       f"{lang_game_key}.dic")
    flag_mode = affixes.get('flag_mode', 'single')

    def _flags_to_str(flags: Set[str]) -> str:
        if not flags:
            return ''
        if flag_mode == 'long':
            return '/' + ''.join(sorted(flags))
        elif flag_mode == 'num':
            return '/' + ','.join(sorted(flags))
        else:  # 'single' or 'utf8'
            return '/' + ''.join(sorted(flags))

    # Keep compressed/full aligned with flat-confirmed forms (base + corpus forms).
    compressed_output_words: Set[str] = set(all_confirmed)

    # Prune explicit no-flag entries that are already generated by another
    # flagged entry (e.g., base/GS already covers explicit plural/feminine forms).
    generated_by_flagged: Dict[str, Set[str]] = {}
    generated_forms_by_word: Dict[str, Set[str]] = {}
    _prune_errors: List[str] = []
    for src_word in sorted(compressed_output_words, key=str.lower):
        src_flags = compressed_entries.get(src_word, set())
        if not src_flags:
            continue
        try:
            generated_forms = generate_word_forms(src_word, sorted(src_flags), affixes)
        except Exception as err:
            _prune_errors.append(f"{src_word}: {type(err).__name__}: {err}")
            continue
        generated_forms_by_word[src_word] = set(generated_forms)
        for form in generated_forms:
            form_txt = str(form).strip()
            if not form_txt or form_txt == src_word:
                continue
            if form_txt in compressed_output_words:
                generated_by_flagged.setdefault(form_txt, set()).add(src_word)

    dropped_redundant_words: Set[str] = set()
    for cand_word in generated_by_flagged:
        if not compressed_entries.get(cand_word, set()):
            dropped_redundant_words.add(cand_word)
    if dropped_redundant_words:
        compressed_output_words -= dropped_redundant_words
        print(f"  Pruned redundant explicit forms: {len(dropped_redundant_words):,}")
    if _prune_errors:
        print(f"  ⚠ Prune generation errors: {len(_prune_errors):,}")
    compressed_lines: List[str] = []
    for bw in sorted(compressed_output_words, key=str.lower):
        flags = compressed_entries.get(bw, set())
        compressed_lines.append(bw + _flags_to_str(flags))

    with open(compressed_dic_path, 'w', encoding='utf-8') as fh:
        fh.write(f"{len(compressed_lines)}\n")
        for ln in compressed_lines:
            fh.write(ln + '\n')
    print(f"  Compressed dic → {compressed_dic_path}  "
          f"({len(compressed_lines):,} entries)")

    # 8c. Compressed .aff (only used flag blocks)
    compressed_aff_path = os.path.join(compressed_bundle_dir,
                                       f"{lang_game_key}.aff")
    custom_aff_text = _build_custom_aff(std_aff_path, used_flags_all, lang,
                                         aff_blocks)
    with open(compressed_aff_path, 'w',
              encoding=aff_blocks['encoding']) as fh:
        fh.write(custom_aff_text)
    print(f"  Compressed aff → {compressed_aff_path}")

    # 8d. Full dictionary bundle: copy Hunspell reference folder + add generated pair
    def _copy_tree_no_overwrite(src_dir: str, dst_dir: str) -> int:
        copied = 0
        for root, _, files in os.walk(src_dir):
            rel_root = os.path.relpath(root, src_dir)
            target_root = dst_dir if rel_root == '.' else os.path.join(dst_dir, rel_root)
            os.makedirs(target_root, exist_ok=True)
            for name in files:
                src_file = os.path.join(root, name)
                dst_file = os.path.join(target_root, name)
                if os.path.exists(dst_file):
                    continue
                _shutil.copy2(src_file, dst_file)
                copied += 1
        return copied

    # Copy only the direct Hunspell language folder (e.g. .../es),
    # not every sibling language variant under *_dic.
    ref_src_dir = os.path.dirname(std_dic_path)
    legacy_ref_dir = os.path.join(full_bundle_dir, 'hunspell_ref')
    if os.path.isdir(legacy_ref_dir):
        _shutil.rmtree(legacy_ref_dir)
        print(f"    Removed legacy subfolder: {legacy_ref_dir}")
    copied_ref_files = _copy_tree_no_overwrite(ref_src_dir, full_bundle_dir)
    full_generated_dic_path = os.path.join(full_bundle_dir, f"{lang_game_key}.dic")
    full_generated_aff_path = os.path.join(full_bundle_dir, f"{lang_game_key}.aff")
    _shutil.copy2(compressed_dic_path, full_generated_dic_path)
    _shutil.copy2(compressed_aff_path, full_generated_aff_path)
    print(f"  Full bundle    → {full_bundle_dir}")
    print(f"    Hunspell ref copied (new files only): {copied_ref_files}")
    print(f"    Added generated dic: {full_generated_dic_path}")
    print(f"    Added generated aff: {full_generated_aff_path}")

    # 8e. Casing-inference CSV
    casing_csv_path = os.path.join(INTERMEDIARY_DIR,
                                    f"{games_tag}_{lang}_casing_inference.csv")
    with open(casing_csv_path, 'w', newline='', encoding='utf-8-sig') as fh:
        writer = _csv.DictWriter(fh,
                                fieldnames=['original', 'inferred_lower', 'source'])
        writer.writeheader()
        writer.writerows(casing_inferences)
    print(f"  Casing CSV   → {casing_csv_path}  "
          f"({len(casing_inferences):,} rows)")

    elapsed = _time_now() - t0
    print(f"\n  ⏱ Munch elapsed: {elapsed:.1f}s")

    provenance_jsonl_path = None
    if _prov_level != 'off':
        prov_dir = provenance_output_folder or INTERMEDIARY_DIR
        os.makedirs(prov_dir, exist_ok=True)
        provenance_jsonl_path = os.path.join(
            prov_dir,
            f"{games_tag}_{lang}_munch_provenance.jsonl"
        )
        with open(provenance_jsonl_path, 'w', encoding='utf-8') as fh:
            for token in sorted(all_confirmed, key=str.lower):
                assigned_flags = sorted(compressed_entries.get(token, set()))
                validated_flags = sorted(set(validated_flags_map.get(token, [])))
                mandatory_assigned = sorted([fl for fl in assigned_flags if fl in mandatory_flags])
                validated_assigned = sorted([fl for fl in assigned_flags if fl in set(validated_flags)])
                in_compressed = token in compressed_output_words
                dropped = token in dropped_redundant_words
                is_casing_inferred = any(
                    c.get('inferred_lower') == token for c in casing_inferences
                )
                is_corpus_form = 'corpus_aff_form' in form_lineage.get(token, set())
                is_ghost_form = 'corpus_ghost_form' in form_lineage.get(token, set())
                is_tb_base = token in base_words_original

                if is_tb_base and is_corpus_form:
                    origin_class = 'tb_and_corpus'
                elif is_tb_base:
                    origin_class = 'tb_only'
                elif is_corpus_form or is_ghost_form:
                    origin_class = 'corpus_only'
                elif is_casing_inferred:
                    origin_class = 'generated_casing_inference'
                else:
                    origin_class = 'generated_unknown'

                record = {
                    'token': token,
                    'token_lower': token.lower(),
                    'origin_class': origin_class,
                    'lineage_tags': sorted(form_lineage.get(token, set())),
                    'source_base_words': sorted(form_parents.get(token, {token})),
                    'is_tb_base': is_tb_base,
                    'is_corpus_form': is_corpus_form,
                    'is_ghost_form': is_ghost_form,
                    'is_casing_inferred': is_casing_inferred,
                    'in_flat_output': True,
                    'in_compressed_output': in_compressed,
                    'dropped_from_compressed': dropped,
                    'dropped_generated_by': sorted(generated_by_flagged.get(token, set())),
                    'assigned_flags': assigned_flags,
                    'mandatory_flags_assigned': mandatory_assigned,
                    'validated_flags_assigned': validated_assigned,
                    'validated_flags_candidates': validated_flags,
                    'flag_generated_forms': sorted(generated_forms_by_word.get(token, set())) if token in generated_forms_by_word else [],
                }
                fh.write(_json.dumps(record, ensure_ascii=False) + '\n')
        print(f"  Munch provenance JSONL -> {provenance_jsonl_path}")

    return {
        'game_lang_key':      game_lang_key,
        'lang_game_key':      lang_game_key,
        'flat_dic_path':       flat_dic_path,
        'compressed_dic_path': compressed_dic_path,
        'compressed_aff_path': compressed_aff_path,
        'full_dic_dir':        full_bundle_dir,
        'full_dic_path':       full_generated_dic_path,
        'full_aff_path':       full_generated_aff_path,
        'casing_csv_path':     casing_csv_path,
        'munch_provenance_jsonl_path': provenance_jsonl_path,
        'stats': {
            'base_words':        len(base_words_set),
            'flat_words':        len(flat_words),
            'compressed_entries': len(compressed_lines),
            'compressed_pruned_redundant': len(dropped_redundant_words),
            'english_possessive_removed': removed_possessive_count,
            'known_word_rebound_removed': removed_known_word_rebound_count,
            'flags_used':        sorted(used_flags_all),
            'casing_inferences': len(casing_inferences),
            'elapsed':           elapsed,
        },
    }