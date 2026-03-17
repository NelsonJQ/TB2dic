import os
import re
import json
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time as _time_now
from typing import Any, Dict, List, Set, Tuple, cast

from . import filtering as _filtering
from .filtering import _expand_chunk, generate_word_forms, parse_aff_file
from .params import HUNSPELL_PATHS, INTERMEDIARY_DIR, MUNCH_FLAG_CONFIG, i18n_PATHS
from .prepro import demorph_string, has_wip_markers, remove_html_tags, tokenize_text
try:
    from lingua import Language, LanguageDetectorBuilder
    _LINGUA_AVAILABLE = True
except ImportError:
    _LINGUA_AVAILABLE = False
    print("  [corpus] WARNING: lingua-language-detector not installed.\n"
          "           Run:  pip install lingua-language-detector")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION: Gender ghost generation for proper nouns
# ══════════════════════════════════════════════════════════════════════════════

# Language-specific suffix tables for generating "ghost" gender/plural forms
# from proper-noun base words.  These bypass standard AFF condition patterns
# so that words with non-standard endings (e.g. "bwork" → "bworka") are still
# checked against the i18n corpus.
#
# Each entry: {lang: {"suffixes": [...], "accent_map": {ending: replacement}}}
# accent_map handles accent-shifting: "jalatín" → strip accent → "jalatin" + "a"

GENDER_GHOST_SUFFIXES: Dict[str, dict] = {
    "es": {
        # Spanish: masculine -o → feminine -a, plurals -s/-es
        "suffixes": ["a", "as", "os", "es"],
        "accent_map": {
            "ín": "in",   # jalatín → jalatina / jalatinas
            "ón": "on",   # dragón  → dragona  / dragonas
            "án": "an",   # capitán → capitana / capitanas
            "és": "es",   # francés → francesa / francesas
        },
    },
    "pt": {
        # Portuguese: masculine → feminine -a, plurals -s/-es, -ão/-ões
        "suffixes": ["a", "as", "os", "es", "ões"],
        "accent_map": {
            "ão": "ã",    # dragão  → dragã    (rare for monsters)
            "ín": "in",
            "ón": "on",
            "ês": "es",   # português → portuguesa
        },
    },
    "de": {
        # German: feminine -in/-innen (not accent-dependent)
        "suffixes": ["in", "innen"],
        "accent_map": {},
    },
    "en": {
        # English: no grammatical gender inflection
        "suffixes": [],
        "accent_map": {},
    },
    "fr": {
        # French: not used (source language)
        "suffixes": [],
        "accent_map": {},
    },
}

# Spanish stems that repeatedly generate verb-like false positives in
# brute-force AFF expansion and should be excluded from ghost/noise paths.
SPANISH_GHOST_NOISE_BASES: Set[str] = {'tar', 'star', 'sir', 'over'}


def _generate_gender_ghosts(base_lower: str, lang: str) -> Set[str]:
    """
    Generate "ghost" gender/plural forms for a proper-noun base word.

    These are hypothetical inflected forms that might exist in the corpus
    even though the standard Hunspell AFF rules don't have conditions
    matching the stem ending (e.g. Spanish -o/-a doesn't fire for -k stems).

    Returns a set of lowercased candidate forms (excluding the base itself).
    """
    cfg = GENDER_GHOST_SUFFIXES.get(lang)
    if not cfg or not cfg["suffixes"]:
        return set()

    # Spanish guardrails for short/proper-name stems that tend to create
    # verb-like noise (e.g. tar -> tado, star -> prestadle).
    if lang == 'es':
        if base_lower in SPANISH_GHOST_NOISE_BASES:
            return set()
        if len(base_lower) <= 3:
            return set()
        if len(base_lower) <= 4 and base_lower.endswith('r'):
            return set()

    ghosts: Set[str] = set()
    suffixes  = cfg["suffixes"]
    acmap     = cfg["accent_map"]

    # ── Accent-neutralised stem ───────────────────────────────────────────────
    # If the word ends with an accented pattern, produce forms from the
    # de-accented stem + suffix.  E.g. "jalatín" → stem "jalatin", ghosts
    # "jalatina", "jalatinas".
    neutralised_stem = None
    for ending, replacement in acmap.items():
        if base_lower.endswith(ending):
            neutralised_stem = base_lower[:-len(ending)] + replacement
            break

    # ── Direct suffixing (for non-standard endings like "bwork") ─────────────
    for sfx in suffixes:
        ghosts.add(base_lower + sfx)

    # ── From neutralised stem ─────────────────────────────────────────────────
    if neutralised_stem:
        for sfx in suffixes:
            ghosts.add(neutralised_stem + sfx)
        # Also add the bare neutralised stem (e.g. "jalatin" without suffix)
        ghosts.add(neutralised_stem)

    ghosts.discard(base_lower)
    return ghosts


# ══════════════════════════════════════════════════════════════════════════════
# SECTION: Find missing word forms in i18n corpus
# ══════════════════════════════════════════════════════════════════════════════

# Maps the 5 two-letter game language codes to lingua Language enum values.
# Detector is built from all five so it can distinguish the target language
# from any of the four others that might appear as untranslated fallbacks.
_LINGUA_LANG_MAP: Dict[str, object] = {}
if _LINGUA_AVAILABLE:
    _LINGUA_LANG_MAP = {
        "fr": Language.FRENCH,       # type: ignore[union-attr]
        "en": Language.ENGLISH,      # type: ignore[union-attr]
        "es": Language.SPANISH,      # type: ignore[union-attr]
        "pt": Language.PORTUGUESE,   # type: ignore[union-attr]
        "de": Language.GERMAN,       # type: ignore[union-attr]
    }

# Session singleton for lingua detector to avoid rebuild per call.
_LINGUA_DETECTOR_SINGLETON = None
_LINGUA_DETECTOR_LANGS_SIG: Tuple[str, ...] = tuple()

# Backup of the original i18n_PATHS values, populated once on the first call
# to load_i18n_corpus.  Used to auto-recover when the in-place mutation points
# to a filtered file that has since been deleted.
_i18n_PATHS_original: Dict[str, Dict[str, str]] = {}


def load_i18n_corpus(
    lang            : str,
    games           : List[str],
    source_type     : str   = 'i18n',
    lang_detect     : bool  = True,
    min_words       : int   = 7,
    min_confidence  : float = 0.12,
    short_confidence: float = 0.9,
) -> Dict[str, Set[str]]:
    """
    Load and tokenize i18n strings for the given lang/games.

    Handles two file formats:
      * JSON        (DOFUS):  {"entries": {"<id>": "<text>", ...}}
      * .properties (WAKFU):  dotted.key=translated value  (Java properties)

    Each raw string pipeline:
      has_wip_markers() -> skip  ->  remove_html_tags()
      -> demorph_string()  ->  [lang_detect: skip if wrong language]
      -> tokenize_text()  ->  record true-case + lowercase key

    demorph_string() is intentionally called BEFORE language detection so that
    game-engine morphology markup such as {[1*]?a:o} or {~f} is expanded into
    plain text first.  Leaving the raw markup in the string confuses the lingua
    detector and causes false positives (e.g. "Repurgador{[1*]?a:} cristalin{[1*]?a:o}"
    being mis-classified as French/English and dropped even though the underlying
    content is valid Spanish).

    Language detection (lang_detect=True)
    --------------------------------------
    Entries absent in the target language are sometimes shown in French or
    English as a fallback.  The lingua detector filters those out by checking
    the confidence that each string belongs to *lang*.

    Two thresholds are applied:
      * Strings with >= min_words words  ->  min_confidence   (default 0.5)
        Most sentences only pass if the model is reasonably confident.
      * Strings with <  min_words words  ->  short_confidence (default 0.9)
        Very strict for short strings to avoid discarding game-specific
        neologisms, item names and monster names that the model might
        mis-classify (short text detection is inherently less reliable).

    Re-run detection
    ----------------
    If i18n_PATHS[game][lang] already points to a previously-written filtered
    file (_i18n_filtered.*), language detection is skipped for that entry to
    avoid double-removal.  A notice is printed to make re-runs visible.

    Side effects when lang_detect=True (first run only):
      * A filtered copy of each i18n file is written to INTERMEDIARY_DIR.
      * A removed-entries CSV is written to INTERMEDIARY_DIR with every
        excluded string, its confidence score, word count and threshold used.
      * i18n_PATHS[game][lang] is updated in-place to point to the filtered
        file so that subsequent calls in the same kernel session are
        idempotent.

    Args:
        lang            : 2-letter language code ('es', 'pt', 'en', 'de')
        games           : Game names matching keys in i18n_PATHS  (e.g. ['WAKFU'])
        source_type     : 'i18n' (only mode currently implemented)
        lang_detect     : Enable lingua language-detection filtering (default True)
        min_words       : Word-count boundary between the two thresholds
        min_confidence  : Minimum p(target lang) for strings with >= min_words words
        short_confidence: Minimum p(target lang) for strings with <  min_words words

    Returns:
        Dict[str, Set[str]]:
            Keys   = lowercased tokens
            Values = set of all original-case forms seen for that key in the corpus
            Example: {'jalatines': {'jalatines', 'Jalatines'}, 'abella': {'Abella'}}
    """
    if source_type != 'i18n':
        raise NotImplementedError(
            f"source_type={source_type!r} — only 'i18n' is currently supported"
        )

    # ── Lazily back up original paths before any in-place mutation ─────────────
    # _i18n_PATHS_original holds the pre-mutation values so we can auto-recover
    # if i18n_PATHS[game][lang] was previously mutated to point to a filtered
    # file that has since been deleted (e.g. after a kernel restart or cleanup).
    global _i18n_PATHS_original
    for _g, _paths in i18n_PATHS.items():
        if isinstance(_paths, dict) and _g not in _i18n_PATHS_original:
            _i18n_PATHS_original[_g] = dict(_paths)

    corpus_map: Dict[str, Set[str]] = {}
    total_strings  = 0
    skipped_wip    = 0
    skipped_lang   = 0
    skipped_lang_prefilter = 0
    lang_detector_calls = 0
    removed_entries: List[dict] = []   # {game, key, raw_val, demorphed_val, confidence, word_count}

    def _add_tokens(tokens):
        for tok in tokens:
            key = tok.lower()
            if key not in corpus_map:
                corpus_map[key] = set()
            corpus_map[key].add(tok)

    # ── Build lingua detector (session singleton) ───────────────────────────
    _lingua_detector  = None
    _lingua_target    = None
    _effective_detect = lang_detect and _LINGUA_AVAILABLE

    if lang_detect and not _LINGUA_AVAILABLE:
        print("  [corpus] WARNING: lingua not installed — "
              "language detection skipped for this call")
    elif _effective_detect:
        if lang not in _LINGUA_LANG_MAP:
            print(f"  [corpus] WARNING: lang={lang!r} not in _LINGUA_LANG_MAP — "
                  "language detection disabled for this call")
            _effective_detect = False
        else:
            global _LINGUA_DETECTOR_SINGLETON, _LINGUA_DETECTOR_LANGS_SIG
            _lingua_target   = _LINGUA_LANG_MAP[lang]
            _langs_sig = tuple(sorted(_LINGUA_LANG_MAP.keys()))
            _detector_state = "cached"
            if _LINGUA_DETECTOR_SINGLETON is None or _LINGUA_DETECTOR_LANGS_SIG != _langs_sig:
                lingua_languages = [cast(Any, l) for l in _LINGUA_LANG_MAP.values()]
                _LINGUA_DETECTOR_SINGLETON = (
                    LanguageDetectorBuilder          # type: ignore[union-attr]
                    .from_languages(*lingua_languages)
                    .build()
                )
                _LINGUA_DETECTOR_LANGS_SIG = _langs_sig
                _detector_state = "built"
            _lingua_detector = _LINGUA_DETECTOR_SINGLETON
            other_langs = [
                str(l).split(".")[-1]
                for l in _LINGUA_LANG_MAP.values()
                if l != _lingua_target
            ]
            print(f"  [corpus] Lingua detector  : target={lang.upper()}"
                  f"  fallbacks={other_langs}  [{_detector_state}]")
            print(f"  [corpus] Thresholds       : short(<{min_words}w)=compare vs EN/FR"
                  f"  long(>={min_words}w)={min_confidence}")

    def _strip_for_detection(text: str) -> str:
        """Remove leftover game-engine markup that would confuse the lingua detector.

        After demorph_string(), placeholder tags like [#1], [>1] and unexpanded
        curly-brace blocks {…} may still be present.  Strip them so the detector
        sees only plain words."""
        text = re.sub(r'\[[^\]]*\]', ' ', text)   # [#1], [>1], [1*], …
        text = re.sub(r'\{[^}]*\}',  ' ', text)   # any remaining {…} markup
        return re.sub(r'\s+', ' ', text).strip()

    def _score_language(text: str):
        """Return (is_wrong, confidence, word_count, threshold).

        Long strings (>= min_words):  require p(target) >= min_confidence.

        Short strings (< min_words):  instead of the strict 0.9 fixed threshold,
        compare p(target) against p(EN) and p(FR).  A short string is considered
        wrong only when *both* EN and FR score higher than the target language.
        Special cases:
          * target == FR  ->  skip filtering entirely (no self-comparison).
          * target == EN  ->  compare against FR only (no self-comparison).
        """
        nonlocal skipped_lang_prefilter, lang_detector_calls
        if _lingua_detector is None or _lingua_target is None:
            return False, 1.0, 0, 0.0
        clean = _strip_for_detection(text)
        word_count = len(clean.split()) if clean else 0
        if word_count == 0:
            return False, 1.0, 0, 0.0

        # Fast prefilter: skip detector for strings that are mostly non-lexical.
        alpha_count = sum(1 for ch in clean if ch.isalpha())
        if alpha_count < 3:
            skipped_lang_prefilter += 1
            return False, 1.0, word_count, 0.0
        if alpha_count / max(len(clean), 1) < 0.35:
            skipped_lang_prefilter += 1
            return False, 1.0, word_count, 0.0
        if clean.isupper() and alpha_count <= 4:
            skipped_lang_prefilter += 1
            return False, 1.0, word_count, 0.0
        if word_count >= min_words:
            lang_detector_calls += 1
            confidence = _lingua_detector.compute_language_confidence(clean, _lingua_target)  # type: ignore[union-attr]
            return confidence < min_confidence, confidence, word_count, min_confidence
        # ── Short-string path: compare target vs EN / FR ─────────────────────
        _lang_fr = _LINGUA_LANG_MAP.get('fr')
        _lang_en = _LINGUA_LANG_MAP.get('en')
        # If target IS French, no point in detection (would compare FR vs FR)
        if _lingua_target == _lang_fr:
            return False, 1.0, word_count, 0.0
        lang_detector_calls += 1
        confidence = _lingua_detector.compute_language_confidence(clean, _lingua_target)  # type: ignore[union-attr]
        comparators = []
        if _lang_en is not None and _lingua_target != _lang_en:
            lang_detector_calls += 1
            comparators.append(_lingua_detector.compute_language_confidence(clean, _lang_en))  # type: ignore[union-attr]
        if _lang_fr is not None:
            lang_detector_calls += 1
            comparators.append(_lingua_detector.compute_language_confidence(clean, _lang_fr))  # type: ignore[union-attr]
        # Wrong only if target score is strictly below ALL comparators
        is_wrong = bool(comparators) and all(confidence < c for c in comparators)
        return is_wrong, confidence, word_count, 0.0

    for game in games:
        paths_for_game = i18n_PATHS.get(game, {})
        if not paths_for_game:
            print(f"  [corpus] {game}: no i18n_PATHS entry — skipping")
            continue

        full_path = paths_for_game.get(lang, "")
        if not full_path or not os.path.exists(full_path):
            # Auto-recover: if the path is a stale filtered file, fall back to
            # the original path stored in _i18n_PATHS_original.
            _filtered_names = (
                f"{game}_{lang}_i18n_filtered.json",
                f"{game}_{lang}_i18n_filtered.properties",
            )
            if full_path and os.path.basename(full_path) in _filtered_names:
                _orig = (_i18n_PATHS_original.get(game) or {}).get(lang, "")
                if _orig and os.path.exists(_orig):
                    print(f"  [corpus] {game}/{lang}: filtered file missing, "
                          f"reverting to original -> {_orig!r}")
                    i18n_PATHS[game][lang] = _orig
                    full_path = _orig
                else:
                    print(f"  [corpus] {game}/{lang}: filtered file missing and original "
                          f"not found either ({_orig!r}) — skipping.  "
                          f"Re-run the config cell to reset i18n_PATHS.")
                    continue
            else:
                print(f"  [corpus] {game}/{lang}: file not found -> {full_path!r} — skipping")
                continue

        # Detect re-run: path already points to a previously-filtered file.
        # Skip detection to avoid double-removal and overwriting the same file.
        _already_filtered = os.path.basename(full_path) in (
            f"{game}_{lang}_i18n_filtered.json",
            f"{game}_{lang}_i18n_filtered.properties",
        )
        if _already_filtered:
            print(f"  [corpus] {game}/{lang}: reading already-filtered file "
                  f"(re-run detected — lang detection skipped)")
        else:
            print(f"  [corpus] Loading {game}/{lang}: {full_path}")

        _detect_this = _effective_detect and not _already_filtered
        game_count     = 0
        game_lang_skip = 0

        # ── JSON branch  (DOFUS: {"entries": {"id": "text"}}) ─────────────────
        if full_path.endswith('.json'):
            with open(full_path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            entries = data.get('entries', data)
            filtered_entries: Dict[str, str] = {}
            for entry_id, raw_val in entries.items():
                if not isinstance(raw_val, str):
                    continue
                total_strings += 1
                if has_wip_markers(raw_val):
                    skipped_wip += 1
                    continue
                clean_val     = remove_html_tags(raw_val)
                demorphed_val = demorph_string(clean_val)   # expand markup BEFORE detection
                if _detect_this:
                    is_wrong, conf, wc, thr = _score_language(demorphed_val)
                    if is_wrong:
                        skipped_lang   += 1
                        game_lang_skip += 1
                        removed_entries.append({
                            'game'         : game,
                            'key'          : entry_id,
                            'confidence'   : round(conf, 4),
                            'word_count'   : wc,
                            'threshold'    : thr,
                            'raw_val'      : raw_val,
                            'demorphed_val': demorphed_val,
                        })
                        continue
                filtered_entries[entry_id] = raw_val
                _add_tokens(tokenize_text(demorphed_val, lang))
                game_count += 1

            # Write filtered JSON and update path mapping
            if _detect_this:
                os.makedirs(INTERMEDIARY_DIR, exist_ok=True)
                filtered_path = os.path.join(
                    INTERMEDIARY_DIR, f"{game}_{lang}_i18n_filtered.json"
                )
                with open(filtered_path, 'w', encoding='utf-8') as fh:
                    json.dump({"entries": filtered_entries}, fh,
                              ensure_ascii=False, indent=2)
                i18n_PATHS[game][lang] = filtered_path
                print(f"  [corpus]   -> lang-filtered : {game_lang_skip:,} removed")
                print(f"  [corpus]   -> written       : {filtered_path}")
                print(f"  [corpus]   -> i18n_PATHS[{game!r}][{lang!r}] updated in-place")

        # ── .properties branch  (WAKFU: dotted.key=translated value) ──────────
        elif full_path.endswith('.properties'):
            accepted_lines: List[str] = []
            with open(full_path, 'r', encoding='utf-8') as fh:
                for line_raw in fh:
                    line = line_raw.rstrip('\n')
                    # Preserve blank lines and comment lines in the filtered file
                    if not line.strip() or line.lstrip().startswith('#'):
                        if _detect_this:
                            accepted_lines.append(line)
                        continue
                    if '=' not in line:
                        if _detect_this:
                            accepted_lines.append(line)
                        continue
                    _, _, raw_val = line.partition('=')
                    if not raw_val.strip():
                        if _detect_this:
                            accepted_lines.append(line)
                        continue
                    total_strings += 1
                    if has_wip_markers(raw_val):
                        skipped_wip += 1
                        continue
                    clean_val     = remove_html_tags(raw_val)
                    demorphed_val = demorph_string(clean_val)   # expand markup BEFORE detection
                    if _detect_this:
                        is_wrong, conf, wc, thr = _score_language(demorphed_val)
                        if is_wrong:
                            skipped_lang   += 1
                            game_lang_skip += 1
                            prop_key = line.partition('=')[0].strip()
                            removed_entries.append({
                                'game'         : game,
                                'key'          : prop_key,
                                'confidence'   : round(conf, 4),
                                'word_count'   : wc,
                                'threshold'    : thr,
                                'raw_val'      : raw_val.strip(),
                                'demorphed_val': demorphed_val,
                            })
                            continue
                        accepted_lines.append(line)
                    _add_tokens(tokenize_text(demorphed_val, lang))
                    game_count += 1
            # Write filtered .properties and update path mapping
            if _detect_this:
                os.makedirs(INTERMEDIARY_DIR, exist_ok=True)
                filtered_path = os.path.join(
                    INTERMEDIARY_DIR, f"{game}_{lang}_i18n_filtered.properties"
                )
                with open(filtered_path, 'w', encoding='utf-8') as fh:
                    fh.write('\n'.join(accepted_lines))
                    if accepted_lines:
                        fh.write('\n')
                i18n_PATHS[game][lang] = filtered_path
                print(f"  [corpus]   -> lang-filtered : {game_lang_skip:,} removed")
                print(f"  [corpus]   -> written       : {filtered_path}")
                print(f"  [corpus]   -> i18n_PATHS[{game!r}][{lang!r}] updated in-place")

        else:
            print(f"  [corpus] {game}/{lang}: unsupported format for {full_path!r} — skipping")
            continue

        print(f"  [corpus]   -> {game_count:,} strings processed from {game}")

    print(f"  [corpus] ─────────────────────────────────────────────────────")
    print(f"  [corpus] Total strings  : {total_strings:,}")
    print(f"  [corpus] WIP skipped    : {skipped_wip:,}")
    if _effective_detect:
        filter_rate = (skipped_lang / total_strings * 100) if total_strings else 0.0
        print(f"  [corpus] Lang filtered  : {skipped_lang:,}"
              f"  ({filter_rate:.1f}%  p(target)<threshold)")
        print(f"  [corpus] Lang prefilter : {skipped_lang_prefilter:,}  (detector skipped)")
        print(f"  [corpus] Detector calls : {lang_detector_calls:,}")
    print(f"  [corpus] Unique tokens  : {len(corpus_map):,}")

    # ── Save removed-entries log ──────────────────────────────────────────────
    if _effective_detect and removed_entries:
        os.makedirs(INTERMEDIARY_DIR, exist_ok=True)
        games_tag   = '_'.join(games)
        removed_csv = os.path.join(
            INTERMEDIARY_DIR, f"{games_tag}_{lang}_i18n_langdetect_removed.csv"
        )
        _removed_fields = ['game', 'key', 'confidence', 'word_count',
                           'threshold', 'raw_val', 'demorphed_val']
        with open(removed_csv, 'w', newline='', encoding='utf-8-sig') as _fh:
            _w = csv.DictWriter(_fh, fieldnames=_removed_fields)
            _w.writeheader()
            _w.writerows(removed_entries)
        print(f"  [corpus] Removed log    : {removed_csv}  ({len(removed_entries)} entries)")
        print(f"  [corpus] ─────────────────────────────────────────────────────")
        print(f"  [corpus] Removed entries (sorted by confidence asc):")
        for _r in sorted(removed_entries, key=lambda x: x['confidence'])[:10]:
            _raw_preview = (_r['raw_val'][:90] + '…') if len(_r['raw_val']) > 90 else _r['raw_val']
            _dem_preview = (_r['demorphed_val'][:90] + '…') if len(_r['demorphed_val']) > 90 else _r['demorphed_val']
            print(f"    [{_r['game']}]  conf={_r['confidence']:.3f}  "
                  f"wc={_r['word_count']:>2}  thr={_r['threshold']}  "
                  f"key={_r['key']}")
            print(f"      raw      : {_raw_preview}")
            print(f"      demorphed: {_dem_preview}")
        if len(removed_entries) > 10:
            print(f"    ... ({len(removed_entries) - 10} more — see {removed_csv})")

    return corpus_map


# Cache: lang -> fully-expanded standard-dic forms set (avoids re-expansion per session)
_std_dic_forms_cache: Dict[str, Set[str]] = {}


def build_std_dic_forms(lang: str, num_threads: int = 4) -> Set[str]:
    """
    Return every surface form (lowercase) that the standard Hunspell dictionary for *lang*
    recognises, including all affix-expanded inflections.

    The result is cached per lang in ``_std_dic_forms_cache`` so repeated calls within
    the same kernel session are essentially free.

    Replicates the expansion logic used by ``filter_tokens_by_dictionary_with_affixes``,
    delegating to the already-defined ``_expand_chunk`` thread worker.
    """
    global _std_dic_forms_cache
    if lang in _std_dic_forms_cache:
        print(f"  Standard dic forms ({lang}) : {len(_std_dic_forms_cache[lang]):,}  [cached]")
        return _std_dic_forms_cache[lang]

    t0 = _time_now()
    std_dic_path = HUNSPELL_PATHS.get(lang, "")
    if not std_dic_path:
        raise ValueError(f"No HUNSPELL_PATHS entry for lang={lang!r}")
    aff_path = std_dic_path.replace(".dic", ".aff")

    print(f"  Expanding standard dic ({lang})...")
    affixes = parse_aff_file(aff_path)

    _dic_enc = affixes.get('encoding', 'utf-8')
    if isinstance(_dic_enc, str) and _dic_enc.lower() in ('utf-8', 'utf8'):
        _dic_enc = 'utf-8-sig'          # handle optional BOM

    forbidden: Set[str] = set()
    entries: List[tuple] = []           # (base_lower, flags_str)

    try:
        with open(std_dic_path, 'r', encoding=_dic_enc, errors='replace') as fh:
            raw_lines = fh.readlines()
    except Exception as e:
        print(f"    Warning: could not read {std_dic_path!r}: {e}")
        return set()

    for ln in raw_lines[1:]:            # skip count header
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split('/', 1)
        base  = parts[0].split('\t')[0].rstrip('.')
        flags = parts[1].split('\t')[0] if len(parts) > 1 else ''
        base_lower = base.lower()
        if not base_lower:
            continue
        # Collect FORBIDDENWORD entries to exclude them later
        if 'FORBIDDENWORD' in affixes and flags:
            fw_flag = affixes.get('FORBIDDENWORD', '')
            if fw_flag and fw_flag in flags:
                forbidden.add(base_lower)
                continue
        entries.append((base_lower, flags))

    # Parallel AFF expansion via the already-defined _expand_chunk worker
    chunk_size = max(1, len(entries) // (num_threads * 4))
    chunks = [entries[i:i + chunk_size] for i in range(0, len(entries), chunk_size)]

    all_forms: Set[str] = set()
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(_expand_chunk, chunk, affixes) for chunk in chunks]
        for fut in as_completed(futures):
            all_forms.update(fut.result())

    # Remove any forms that belong to FORBIDDENWORD headwords
    all_forms -= forbidden
    # Add plain headwords (some headwords have no flags)
    all_forms.update(base for base, _ in entries)

    # CHECKSHARPS: ß <-> ss variants
    if affixes.get('CHECKSHARPS'):
        all_forms.update({f.replace('ß', 'ss') for f in all_forms if 'ß' in f})
        all_forms.update({f.replace('ss', 'ß') for f in all_forms if 'ss' in f})

    elapsed = _time_now() - t0
    print(f"  Standard dic forms ({lang}) : {len(all_forms):,}  ({elapsed:.1f}s)")
    _std_dic_forms_cache[lang] = all_forms
    return all_forms


def _wordform_match_worker(
    batch: List[str],
    affixes: Dict,
    corpus_map: Dict[str, Set[str]],
    all_flags: List[str],
    propernoun_lower: frozenset = frozenset(),
    lang: str = "es",
    candidate_flags: List[str] | None = None,
    quorum: float = 0.5,
    collect_flag_evidence: bool = False,
) -> List[dict]:
    """
    Thread worker — brute-forces all SFX+PFX flags against each base word,
    then intersects the generated (lowercased) forms with corpus_map keys.

    For base words in propernoun_lower, also generates gender "ghost" forms
    via _generate_gender_ghosts() and checks them against the corpus.  Ghost
    hits are tracked separately so the munch step can distinguish AFF-derived
    forms from gender ghosts.

    Flag attribution (munch pre-computation)
    -----------------------------------------
    When ``candidate_flags`` is provided, also tests each flag individually
    and records which flags pass the corpus quorum.  This avoids repeating
    the same generate_word_forms() + corpus-lookup work in a separate munch
    pass.  ``validated_flags`` is a list of flag strings whose generated
    forms met the quorum threshold against the corpus.

    True-case forms: all original-case variants stored in corpus_map[key] are
    collected, so 'jalatines' AND 'Jalatines' (if both appear in corpus) are
    both returned.

    Exclusion: any generated form whose lowercase equals base_lower is excluded
    (the base word is already in the custom dic).

    Returns:
        List of dicts {base_word, found_forms, count, ghost_forms, ghost_count,
        validated_flags}  -- only entries with >=1 hit (AFF or ghost).
        Words with 0 AFF/ghost hits but >=1 validated_flag are also included.
    """
    # Build a set of all lowercased corpus keys for fast membership/intersection checks.
    _corpus_keys: frozenset = frozenset(corpus_map.keys())

    results_batch = []
    for base_word in batch:
        base_lower = base_word.lower()

        # Targeted Spanish noise stems: skip brute-force generation entirely
        # so they do not create false verb-like families in outputs.
        if lang == 'es' and base_lower in SPANISH_GHOST_NOISE_BASES:
            continue
        try:
            forms_lower = generate_word_forms(base_lower, all_flags, affixes)
        except Exception:
            forms_lower = set()

        # Collect all true-case variants for each matching lowercase form,
        # excluding the base word itself.
        true_case_hits: Set[str] = set()
        matched_lower = (forms_lower - {base_lower}) & _corpus_keys
        for form_lower in matched_lower:
            true_case_hits.update(corpus_map[form_lower])

        # ── Gender ghost forms for proper nouns ───────────────────────────
        ghost_true_case_hits: Set[str] = set()
        if base_lower in propernoun_lower:
            ghost_forms_lower = _generate_gender_ghosts(base_lower, lang)
            # Exclude forms already found via AFF (avoid double-counting)
            ghost_forms_lower -= forms_lower
            ghost_forms_lower.discard(base_lower)
            for gf in (ghost_forms_lower & _corpus_keys):
                ghost_true_case_hits.update(corpus_map[gf])
            # Also remove any ghost hits that were already AFF hits
            ghost_true_case_hits -= true_case_hits

        # ── Per-flag attribution for munch ────────────────────────────────
        validated: List[str] = []
        flag_evidence: List[Dict[str, Any]] = []
        if candidate_flags:
            for flag in candidate_flags:
                try:
                    flag_forms = generate_word_forms(base_lower, [flag], affixes)
                except Exception:
                    continue
                derived = flag_forms - {base_lower}
                if not derived:
                    continue
                hit_forms_lower = sorted(f for f in derived if f in _corpus_keys)
                hits = len(hit_forms_lower)
                ratio = (hits / len(derived)) if derived else 0.0
                passed = bool(hits > 0 and ratio >= quorum)
                if passed:
                    validated.append(flag)
                if collect_flag_evidence:
                    true_case_hit_forms: Set[str] = set()
                    for hf in hit_forms_lower:
                        true_case_hit_forms.update(corpus_map.get(hf, set()))
                    flag_evidence.append({
                        'flag': flag,
                        'derived_count': len(derived),
                        'hit_count': hits,
                        'hit_ratio': round(ratio, 6),
                        'passed': passed,
                        'hit_forms_lower': hit_forms_lower,
                        'hit_forms_truecase': sorted(true_case_hit_forms, key=str.lower),
                    })

        if true_case_hits or ghost_true_case_hits or validated:
            results_batch.append({
                'base_word'       : base_word,
                'found_forms'     : sorted(true_case_hits, key=str.lower),
                'count'           : len(true_case_hits),
                'ghost_forms'     : sorted(ghost_true_case_hits, key=str.lower),
                'ghost_count'     : len(ghost_true_case_hits),
                'validated_flags' : validated,
                'flag_evidence'   : flag_evidence,
            })
    return results_batch


def find_corpus_wordforms(
    dic_path   : str,
    lang       : str,
    games      : List[str],
    source_type: str  = 'i18n',
    sample     : int  = 800,
    workers    : int  = 8,
    batch_size : int  = 50,
    propernoun_sidecar: str | None = None,
    add_verb_flags: bool = False,
    quorum: float = 0.5,
    provenance_level: str = 'off',
    provenance_output_folder: str | None = None,
) -> List[dict]:
    """
    For each token in the custom .dic, generate every Hunspell word form via
    brute-force application of ALL SFX+PFX flags, then check which generated
    forms appear in the i18n corpus.

    For tokens that belong to any category in PROPER_NOUN_KEY_PATTERNS (loaded
    from a sidecar JSON produced by load_and_tokenize_terminology_base), gender "ghost"
    forms are also generated and checked, even when the word's ending is
    incompatible with standard AFF conditions (e.g. bwork → bworka).

    Flag attribution (munch pre-computation)
    -----------------------------------------
    When MUNCH_FLAG_CONFIG defines validation/verb flags for *lang*, each
    candidate flag is tested per word inside the worker threads.  A flag is
    "validated" when >= *quorum* fraction of its generated forms appear in
    the corpus.  Verb flags are only tested when *add_verb_flags=True*.
    The result dict includes ``validated_flags`` per word so that the
    downstream ``munch_to_compressed_dic()`` no longer needs to repeat the
    generate + corpus-lookup work.

    True-case
    ---------
    Corpus tokens are stored with their original case; the CSV reports the
    exact form(s) as they appear in the i18n file.  If both 'Abella' and
    'abella' appear, both are listed.

    New-form filtering
    ------------------
    After collecting all corpus hits, each form is checked against:
      1. The custom .dic (dic_path)  -- ALL entries, not just the sample
      2. The standard Hunspell .dic  -- HUNSPELL_PATHS[lang]
    Forms already present in either dic are flagged as "known".

    CSV columns
    -----------
    base_word | new_found_forms | new_count | found_forms | count |
    ghost_forms | ghost_count | new_ghost_forms | new_ghost_count |
    validated_flags

    Args:
        dic_path   : Path to the custom filtered_tokens.dic file.
        lang       : 2-letter language code ('es', 'pt', 'en', 'de').
        games      : Games to source the i18n corpus from (e.g. ['WAKFU']).
        source_type: 'i18n' (only mode currently implemented).
        sample     : Maximum dic words to process (0 = all words).
        workers    : ThreadPoolExecutor max_workers.
        batch_size : Words fed to each thread per batch.
        propernoun_sidecar: Path to the JSON sidecar. If None, auto-resolves
            from {INTERMEDIARY_DIR}/{games[0]}_{lang}_propernoun_tokens.json.
        add_verb_flags: If True, include verb conjugation flags as candidates.
        quorum    : Minimum fraction of flag-generated forms that must be
            corpus-confirmed for the flag to be assigned (default 0.5).

    Returns:
        List of result dicts sorted by base_word.
    """
    _prov_level = str(provenance_level or 'off').strip().lower()
    _collect_flag_evidence = (_prov_level == 'detailed')

    t0 = _time_now()
    _cache_hits_before = _filtering._word_form_cache_hits
    _cache_misses_before = _filtering._word_form_cache_misses

    # ── Resolve AFF file ──────────────────────────────────────────────────────
    aff_template = HUNSPELL_PATHS.get(lang, "")
    if not aff_template:
        raise ValueError(f"No HUNSPELL_PATHS entry for lang={lang!r}")
    aff_path = aff_template.replace(".dic", ".aff")
    if not os.path.exists(aff_path):
        raise FileNotFoundError(f"AFF file not found: {aff_path!r}")
    # ── Parse AFF ─────────────────────────────────────────────────────────────
    std_dic_path = HUNSPELL_PATHS[lang]
    affixes   = parse_aff_file(aff_path)
    all_flags = sorted(set(affixes.get('SFX', {}).keys()) | set(affixes.get('PFX', {}).keys()))

    print("─" * 62)
    # ── Build candidate flags for munch pre-computation ───────────────────────
    flag_cfg = MUNCH_FLAG_CONFIG.get(lang, {"mandatory": [], "validation": [], "verb": []})
    _validation_flags = set(flag_cfg['validation'])
    _verb_flags       = set(flag_cfg['verb']) if add_verb_flags else set()
    _candidate_flags  = _validation_flags | _verb_flags
    _available_flags  = set(affixes['SFX'].keys()) | set(affixes['PFX'].keys())
    _candidate_flags &= _available_flags
    _candidate_flags_list = sorted(_candidate_flags) if _candidate_flags else None
    if _candidate_flags_list:
        print(f"  Candidate flags for munch: {_candidate_flags_list}  (quorum={quorum})")
        if _verb_flags:
            print(f"  Verb flags included      : {sorted(_verb_flags & _available_flags)}")
    else:
        print(f"  No candidate flags configured for lang={lang} — flag attribution skipped")

    # ── Load proper-noun sidecar ──────────────────────────────────────────────
    _propernoun_lower: frozenset = frozenset()
    if propernoun_sidecar is None:
        propernoun_sidecar = os.path.join(
            INTERMEDIARY_DIR, f"{games[0]}_{lang}_propernoun_tokens.json"
        )
    if os.path.exists(propernoun_sidecar):
        with open(propernoun_sidecar, 'r', encoding='utf-8') as _fh:
            _pn_data = json.load(_fh)
        # Merge all key_types from the sidecar
        # (every category present was already filtered by PROPER_NOUN_KEY_PATTERNS at tokenize time)
        _pn_tokens: Set[str] = set()
        for tok_list in _pn_data.values():
            _pn_tokens.update(tok_list)
        _propernoun_lower = frozenset(_pn_tokens)
        print(f"  [ghost] Proper-noun tokens loaded: {len(_propernoun_lower):,}"
              f"  (from {propernoun_sidecar})")
        _ghost_cfg = GENDER_GHOST_SUFFIXES.get(lang, {})
        if _ghost_cfg and _ghost_cfg.get("suffixes"):
            print(f"  [ghost] Suffixes ({lang}): {_ghost_cfg['suffixes']}")
        else:
            print(f"  [ghost] No gender ghost suffixes defined for lang={lang}")
    else:
        print(f"  [ghost] No propernoun sidecar found at {propernoun_sidecar}")

    # ── Load .dic words ───────────────────────────────────────────────────────
    with open(dic_path, 'r', encoding='utf-8') as fh:
        raw_lines = fh.readlines()
    dic_words_all = [ln.strip().split('/')[0] for ln in raw_lines[1:] if ln.strip()]
    if sample and sample < len(dic_words_all):
        dic_words = dic_words_all[:sample]
        print(f"  Dic words (sample) : {len(dic_words):,} of {len(dic_words_all):,} total")
    else:
        dic_words = dic_words_all
        print(f"  Dic words (full)   : {len(dic_words):,}")
    print("\nLoading i18n corpus...")
    # ── Build known-word sets (for new_found_forms filtering) ─────────────────
    print("\nBuilding known-word sets for filtering...")
    # Custom dic: ALL entries (not just the sample), lowercased
    custom_dic_lowers: Set[str] = {w.lower() for w in dic_words_all}
    print(f"  Custom dic entries (lowercase) : {len(custom_dic_lowers):,}")

    # Standard Hunspell dic -- full AFF expansion (all inflected forms, cached per session)
    std_dic_lowers = build_std_dic_forms(lang)

    corpus_map = load_i18n_corpus(lang, games, source_type)

    all_results: List[dict] = []
    if not corpus_map:
        print("\nNo i18n corpus tokens found for this pair — skipping wordform worker dispatch.")
    else:
        # ── Dispatch threads ──────────────────────────────────────────────────
        print(f"\nDispatching {len(dic_words):,} words across {workers} workers ...")
        batches = [dic_words[i:i + batch_size] for i in range(0, len(dic_words), batch_size)]
        done_count = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {
                pool.submit(_wordform_match_worker, b, affixes, corpus_map,
                            all_flags, _propernoun_lower, lang,
                            _candidate_flags_list, quorum,
                            _collect_flag_evidence): i
                for i, b in enumerate(batches)
            }
            for fut in as_completed(future_to_idx):
                batch_res = fut.result()
                all_results.extend(batch_res)
                done_count += 1
                if done_count % 5 == 0 or done_count == len(batches):
                    print(
                        f"  {done_count:>4}/{len(batches)} batches done   ({len(all_results)} words with hits so far)",
                        end='\r',
                        flush=True,
                    )

    print()

    # ── Enrich results with new_found_forms and new_ghost_forms ─────────────
    for r in all_results:
        new_forms = [
            f for f in r['found_forms']
            if f.lower() not in custom_dic_lowers
            and f.lower() not in std_dic_lowers
        ]
        r['new_found_forms'] = new_forms
        r['new_count']       = len(new_forms)

        # Ghost forms enrichment
        new_ghosts = [
            f for f in r.get('ghost_forms', [])
            if f.lower() not in custom_dic_lowers
            and f.lower() not in std_dic_lowers
        ]
        r['new_ghost_forms'] = new_ghosts
        r['new_ghost_count'] = len(new_ghosts)

    # ── Sort + summarise ──────────────────────────────────────────────────────
    all_results.sort(key=lambda r: r['base_word'].lower())
    words_with_hits     = len(all_results)
    words_with_new      = sum(1 for r in all_results if r['new_count'] > 0)
    total_form_hits     = sum(r['count']     for r in all_results)
    total_new_form_hits = sum(r['new_count'] for r in all_results)
    words_with_ghost    = sum(1 for r in all_results if r.get('ghost_count', 0) > 0)
    total_ghost_hits    = sum(r.get('ghost_count', 0)     for r in all_results)
    total_new_ghosts    = sum(r.get('new_ghost_count', 0) for r in all_results)
    words_with_flags    = sum(1 for r in all_results if r.get('validated_flags'))
    total_flags_assigned = sum(len(r.get('validated_flags', [])) for r in all_results)
    elapsed = _time_now() - t0

    print(f"\n{'─'*62}")
    print(f"  Words processed           : {len(dic_words):,}")
    print(f"  Words with >=1 corpus hit : {words_with_hits:,}")
    print(f"  Total corpus form matches : {total_form_hits:,}")
    print(f"  Words with >=1 NEW form   : {words_with_new:,}")
    print(f"  Total NEW form matches    : {total_new_form_hits:,}  (not in custom dic nor std dic)")
    if _propernoun_lower:
        print(f"  Words with ghost hits     : {words_with_ghost:,}")
        print(f"  Total ghost corpus hits   : {total_ghost_hits:,}")
        print(f"  Total NEW ghost forms     : {total_new_ghosts:,}")
    if _candidate_flags_list:
        print(f"  Words with validated flags: {words_with_flags:,}")
        print(f"  Total flags assigned      : {total_flags_assigned:,}")
    print(f"  Elapsed                   : {elapsed:.1f}s")
    _cache_hits_delta = _filtering._word_form_cache_hits - _cache_hits_before
    _cache_misses_delta = _filtering._word_form_cache_misses - _cache_misses_before
    _cache_total = _cache_hits_delta + _cache_misses_delta
    if _cache_total:
        _cache_rate = (_cache_hits_delta / _cache_total) * 100.0
        print(f"  Word-form cache           : hits={_cache_hits_delta:,}  misses={_cache_misses_delta:,}  hit_rate={_cache_rate:.1f}%")
    print(f"{'─'*62}")

    # ── Save CSV ──────────────────────────────────────────────────────────────
    games_tag  = "_".join(games)
    out_csv = os.path.join(INTERMEDIARY_DIR, f"{games_tag}_{lang}_corpus_wordforms.csv")
    fieldnames = [
        'base_word', 'new_found_forms', 'new_count',
        'found_forms', 'count',
        'ghost_forms', 'ghost_count',
        'new_ghost_forms', 'new_ghost_count',
        'validated_flags',
    ]
    with open(out_csv, 'w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in all_results:
            writer.writerow({
                'base_word'       : r['base_word'],
                'new_found_forms' : ' | '.join(r['new_found_forms']),
                'new_count'       : r['new_count'],
                'found_forms'     : ' | '.join(r['found_forms']),
                'count'           : r['count'],
                'new_ghost_forms' : ' | '.join(r.get('new_ghost_forms', [])),
                'new_ghost_count' : r.get('new_ghost_count', 0),
                'ghost_forms'     : ' | '.join(r.get('ghost_forms', [])),
                'ghost_count'     : r.get('ghost_count', 0),
                'validated_flags' : ' | '.join(r.get('validated_flags', [])),
            })
    print(f"\nSaved -> {out_csv}  ({words_with_hits} rows)")

    if _prov_level != 'off':
        prov_dir = provenance_output_folder or INTERMEDIARY_DIR
        os.makedirs(prov_dir, exist_ok=True)
        out_jsonl = os.path.join(
            prov_dir,
            f"{games_tag}_{lang}_corpus_wordforms_provenance.jsonl"
        )
        with open(out_jsonl, 'w', encoding='utf-8') as fh:
            for r in all_results:
                record = {
                    'base_word': r.get('base_word', ''),
                    'found_forms': r.get('found_forms', []),
                    'new_found_forms': r.get('new_found_forms', []),
                    'ghost_forms': r.get('ghost_forms', []),
                    'new_ghost_forms': r.get('new_ghost_forms', []),
                    'validated_flags': r.get('validated_flags', []),
                    'flag_evidence': r.get('flag_evidence', []),
                }
                fh.write(json.dumps(record, ensure_ascii=False) + '\n')
        print(f"Saved -> {out_jsonl}  ({words_with_hits} rows)")

    # ── Top-20 preview by new_count ───────────────────────────────────────────
    top20 = sorted(all_results, key=lambda r: -r['new_count'])[:20]
    if top20:
        print("\nTop-20 by new_count (forms not yet in any dic):")
        for r in top20:
            if r['new_count'] == 0:
                break
            preview = " | ".join(r['new_found_forms'][:6])
            if r['new_count'] > 6:
                preview += f"  ... (+{r['new_count'] - 6} more)"
            print(f"  {r['base_word']:<30}  {r['new_count']:>3} new   ->  {preview}")

    # ── Top-20 preview by new_ghost_count ─────────────────────────────────────
    if _propernoun_lower:
        top20g = sorted(all_results, key=lambda r: -r.get('new_ghost_count', 0))[:20]
        if top20g and top20g[0].get('new_ghost_count', 0) > 0:
            print("\nTop-20 by new_ghost_count (gender ghosts not in any dic):")
            for r in top20g:
                gc = r.get('new_ghost_count', 0)
                if gc == 0:
                    break
                gpreview = " | ".join(r.get('new_ghost_forms', [])[:6])
                if gc > 6:
                    gpreview += f"  ... (+{gc - 6} more)"
                print(f"  {r['base_word']:<30}  {gc:>3} ghost ->  {gpreview}")

    return all_results
