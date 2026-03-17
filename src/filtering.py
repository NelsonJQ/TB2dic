import concurrent.futures
import csv
import os
import re
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Set, Tuple

# ==============================================================================
# 2.1  AFF PARSING
# ==============================================================================

def parse_aff_file(aff_file_path: str) -> Dict:
    """
    Parse Hunspell .aff file and extract affix rules.

    Fully handles FLAG directives (single/long/utf8/num) so that multi-char
    flags (e.g. Spanish es_ES.aff uses FLAG long with 2-char flags like "L1",
    "S1") are parsed correctly.

    Also detects NEEDAFFIX, CIRCUMFIX, ONLYINCOMPOUND and FORBIDDENWORD
    pseudo-flags so generate_word_forms() can skip them.

    Now also parses compound-word directives:
      COMPOUNDBEGIN, COMPOUNDMIDDLE, COMPOUNDEND, COMPOUNDFLAG, COMPOUNDMIN,
      COMPOUNDPERMITFLAG, COMPOUNDRULE.

    Also parses:
      CHECKSHARPS  — enables ß↔SS equivalence for uppercase matching.
                     When set, filter_tokens_by_dictionary_with_affixes() adds
                     ss-variants of every form containing ß to the valid-forms
                     set, so tokens with 'ss' (common in game texts that avoid
                     the ß glyph) are still recognised.
      KEEPCASE     — flag whose carriers must be matched case-sensitively.
                     Parsed and stored; the filter function currently uses
                     case-insensitive (lowercased) matching, which is the safe
                     conservative direction for filtering known words.

    Performance note: each rule dict carries a pre-compiled 're.Pattern' object
    under the key 'compiled_condition'. condition_matches() uses it directly so
    that re.compile() is called once per rule rather than once per word-form
    check. For a .aff with ~1 K unique conditions called ~10 M times this makes
    condition checking ~3-5x faster.

    ── BUG FIX (header-detection false positive) ──────────────────────────────
    Previous versions used `parts[2].upper() in ('Y', 'N')` to decide whether
    a PFX/SFX line is a *header* or a *rule*.  This incorrectly classified rule
    lines whose STRIP field happened to be the letter 'n' or 'N' (e.g.
    `SFX Y  n  te  ern`) as duplicate headers, silently dropping them.
    In de_DE_frami.aff this affected all 18 out of 36 SFX Y rules that strip
    the infinitive '-n', causing German verb past-tense forms like
    'überforderte', 'berechnete', etc. to be missed.  The fix adds a
    `parts[3].isdigit()` guard — header lines always have a numeric count as
    the fourth field, while rule ADD strings are never pure digits except '0'
    (which can only mean "add nothing" and is not a valid 0-rule-count header).
    ────────────────────────────────────────────────────────────────────────────

    Returns:
        Dict with keys:
          'PFX'               -> {flag: {cross_product, rules}}
          'SFX'               -> {flag: {cross_product, rules}}
          'flag_mode'         -> 'single' | 'long' | 'utf8' | 'num'
          'NEEDAFFIX'         -> str | None
          'CIRCUMFIX'         -> str | None
          'ONLYINCOMPOUND'    -> str | None
          'FORBIDDENWORD'     -> str | None
          'CHECKSHARPS'       -> bool  (True if directive is present)
          'KEEPCASE'          -> str | None  (flag letter, if present)
          'COMPOUNDBEGIN'     -> str | None   (flag marking compound-start stems)
          'COMPOUNDMIDDLE'    -> str | None   (flag marking compound-middle stems)
          'COMPOUNDEND'       -> str | None   (flag marking compound-end stems)
          'COMPOUNDFLAG'      -> str | None   (flag for any-position compounding)
          'COMPOUNDMIN'       -> int          (minimum part length, default 3)
          'COMPOUNDPERMITFLAG'-> str | None   (affix allowed inside compounds)
          'COMPOUNDRULE'      -> List[str]    (rule pattern strings)
        Each rule dict has keys:
          strip, add, add_flags (List[str]), condition (str),
          compiled_condition (re.Pattern | None  — None means "match all").
    """
    affixes: Dict = {
        'PFX': {},
        'SFX': {},
        'flag_mode': 'single',
        'encoding': 'utf-8',
        'NEEDAFFIX': None,
        'CIRCUMFIX': None,
        'ONLYINCOMPOUND': None,
        'FORBIDDENWORD': None,
        'CHECKSHARPS':    False,  # True when CHECKSHARPS directive is present
        'KEEPCASE':       None,   # flag letter for KEEPCASE entries
        # ── Compound directives ───────────────────────────────────────────
        'COMPOUNDBEGIN':      None,  # flag: word may START a compound (German-style)
        'COMPOUNDMIDDLE':     None,  # flag: word may appear MID-compound
        'COMPOUNDEND':        None,  # flag: word may END a compound
        'COMPOUNDFLAG':       None,  # flag: word may appear ANYWHERE in a compound
        'COMPOUNDMIN':        3,     # minimum chars per compound part (default 3)
        'COMPOUNDPERMITFLAG': None,  # affix flag: this affix is allowed inside compounds
        'COMPOUNDRULE':       [],    # list of rule-pattern strings (flag-sequence regexes)
    }

    def _parse_flags(flag_str: str, flag_mode: str) -> List[str]:
        """Split a raw flag string into individual flag tokens."""
        if not flag_str:
            return []
        if flag_mode == 'long':
            return [flag_str[i:i+2] for i in range(0, len(flag_str), 2)
                    if len(flag_str[i:i+2]) == 2]
        elif flag_mode == 'num':
            return [f.strip() for f in flag_str.split(',') if f.strip()]
        else:  # 'single' or 'utf8' - one codepoint per flag
            return list(flag_str)

    def _compile_condition(condition: str, is_prefix: bool):
        """Pre-compile a Hunspell condition pattern.  Returns None for '.' (match all)."""
        if condition == '.':
            return None
        try:
            if is_prefix:
                return re.compile(f'^(?:{condition})')
            else:
                return re.compile(f'(?:{condition})$')
        except re.error:
            return None  # Conservative: treated as match-all in condition_matches()

    # Detect encoding from the SET directive (safe: read as bytes first)
    _enc = 'utf-8'
    _enc_map = {
        'UTF-8': 'utf-8', 'UTF8': 'utf-8',
        'ISO8859-1': 'latin-1', 'ISO-8859-1': 'latin-1',
        'ISO8859-2': 'iso-8859-2', 'ISO-8859-2': 'iso-8859-2',
        'ISO8859-15': 'iso-8859-15', 'ISO-8859-15': 'iso-8859-15',
        'KOI8-R': 'koi8-r', 'KOI8-U': 'koi8-u',
        'CP1251': 'cp1251', 'WINDOWS-1251': 'cp1251',
        'CP1252': 'cp1252', 'WINDOWS-1252': 'cp1252',
    }
    with open(aff_file_path, 'rb') as _f:
        for _raw_line in _f:
            _l = _raw_line.decode('latin-1').strip()
            if _l.upper().startswith('SET ') and len(_l.split()) >= 2:
                _set_val = _l.split(None, 1)[1].strip().upper()
                _enc = _enc_map.get(_set_val, _set_val.lower())
                break

    affixes['encoding'] = _enc

    with open(aff_file_path, 'r', encoding=_enc) as f:
        lines = f.readlines()

    current_affix = None
    current_type = None

    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        parts = line.split()
        if not parts:
            continue

        directive = parts[0].upper()

        # Global directives
        if directive == 'FLAG' and len(parts) >= 2:
            mode = parts[1].lower()
            if mode == 'long':
                affixes['flag_mode'] = 'long'
            elif mode in ('utf-8', 'utf8'):
                affixes['flag_mode'] = 'utf8'
            elif mode == 'num':
                affixes['flag_mode'] = 'num'
            continue

        if directive in ('NEEDAFFIX', 'CIRCUMFIX', 'ONLYINCOMPOUND', 'FORBIDDENWORD',
                         'KEEPCASE') and len(parts) >= 2:
            affixes[directive] = parts[1]
            continue

        # CHECKSHARPS has no argument — its mere presence activates the feature
        if directive == 'CHECKSHARPS':
            affixes['CHECKSHARPS'] = True
            continue

        # Compound directives
        if directive in ('COMPOUNDBEGIN', 'COMPOUNDMIDDLE', 'COMPOUNDEND',
                         'COMPOUNDFLAG', 'COMPOUNDPERMITFLAG') and len(parts) >= 2:
            affixes[directive] = parts[1]
            continue

        if directive == 'COMPOUNDMIN' and len(parts) >= 2:
            try:
                affixes['COMPOUNDMIN'] = int(parts[1])
            except ValueError:
                pass
            continue

        if directive == 'COMPOUNDRULE' and len(parts) >= 2:
            # Skip pure-integer count header; collect actual pattern strings
            if not parts[1].isdigit():
                affixes['COMPOUNDRULE'].append(parts[1])
            continue

        # PFX / SFX
        if directive not in ('PFX', 'SFX') or len(parts) < 3:
            continue

        affix_type = directive
        flag = parts[1]
        cross_product = parts[2].upper() == 'Y'

        # ── Header-line detection ────────────────────────────────────────────
        # A genuine header has the form:  SFX flag Y|N count
        # i.e. exactly 4 space-separated tokens where:
        #   parts[2] is the cross-product indicator ('Y' or 'N', case-insensitive)
        #   parts[3] is a non-negative integer count of the rule lines that follow
        #
        # PREVIOUS BUG: `parts[2].upper() in ('Y','N')` also matches rule lines
        # whose STRIP field is the letter 'n' (e.g. `SFX Y  n  te  ern`),
        # silently dropping them and causing entire paradigm classes to vanish
        # (German -n infinitive past-tense → 18/36 SFX Y rules were dropped,
        #  also affected SFX X, I, J, O and others).
        #
        # FIX: require exactly 4 tokens AND a pure-digit count field.
        # Rule lines always have ≥5 tokens (SFX flag strip add condition).
        # ────────────────────────────────────────────────────────────────────
        if (len(parts) == 4
                and parts[2].upper() in ('Y', 'N')
                and parts[3].isdigit()):
            # Header line: SFX A Y 14
            if flag not in affixes[affix_type]:
                affixes[affix_type][flag] = {'cross_product': cross_product, 'rules': []}
            current_affix = flag
            current_type = affix_type
            continue

        # Rule line: PFX/SFX flag strip add[/secondary] [condition]
        if len(parts) >= 4 and current_affix == flag and current_type == affix_type:
            strip = parts[2] if parts[2] != '0' else ''

            raw_add = parts[3] if parts[3] != '0' else ''
            if '/' in raw_add:
                add, add_flags_str = raw_add.split('/', 1)
            else:
                add, add_flags_str = raw_add, ''

            add_flags = _parse_flags(add_flags_str, affixes['flag_mode'])
            condition = parts[4] if len(parts) > 4 else '.'

            # Pre-compile condition pattern (is_prefix = True for PFX, False for SFX)
            compiled_condition = _compile_condition(condition, is_prefix=(affix_type == 'PFX'))

            if current_affix in affixes[current_type]:
                affixes[current_type][current_affix]['rules'].append({
                    'strip': strip,
                    'add': add,
                    'add_flags': add_flags,          # List[str], pre-split
                    'condition': condition,           # raw string (kept for debugging)
                    'compiled_condition': compiled_condition  # re.Pattern | None
                })

    return affixes


# ==============================================================================
# 2.2  CONDITION MATCHING
# ==============================================================================

def condition_matches(word: str, condition: str, is_prefix: bool = True,
                      compiled=None) -> bool:
    """
    Check if word matches the Hunspell affix condition pattern.

    Conditions are anchored: PFX conditions match from the left,
    SFX conditions match at the right end of the word.

    Args:
        word:       Word to test
        condition:  Hunspell condition pattern (supports '.', [abc], [^abc])
        is_prefix:  True -> match at start (PFX); False -> match at end (SFX)
        compiled:   Optional pre-compiled re.Pattern from parse_aff_file().
                    When provided the raw condition string is ignored and the
                    compiled pattern is used directly for ~3-5x speedup.

    Returns:
        bool: True if condition matches
    """
    if condition == '.' and compiled is None:
        return True
    if compiled is None:
        # Fallback: compile on the fly (slow path, kept for direct callers)
        try:
            if is_prefix:
                return bool(re.match(f'^(?:{condition})', word))
            else:
                return bool(re.search(f'(?:{condition})$', word))
        except re.error:
            return True  # Conservative: assume match

    # Fast path: use pre-compiled pattern
    # None means '.' (match all)
    if compiled is None:
        return True
    return bool(compiled.search(word))


# ==============================================================================
# 2.3  WORD-FORM GENERATION  (forward / exhaustive expansion)
# ==============================================================================

# Session cache for top-level generate_word_forms() calls.
# Key: (base_word, tuple(flags))  Value: Set[str] of generated forms
_WORD_FORM_CACHE_MAX = 200000
_word_form_cache: OrderedDict[Tuple[str, Tuple[str, ...]], Set[str]] = OrderedDict()
_word_form_cache_hits = 0
_word_form_cache_misses = 0

def generate_word_forms(base_word: str, flags: List[str], affixes: Dict,
                        _visited: Optional[Set[str]] = None, _depth: int = 0) -> Set[str]:
    """
    Generate all possible word forms using Hunspell affix rules.

    Uses pre-compiled condition patterns stored in each rule dict by
    parse_aff_file() for significantly faster condition matching.

    Implements cross-product (XPRODUCT): suffix-derived forms whose rule set
    has cross_product=True are further expanded with applicable prefixes,
    matching unmunch.cxx behaviour.

    Args:
        base_word: Base (lowercased) word to inflect
        flags:     List of flag identifiers (pre-parsed from .dic entry or add_flags)
        affixes:   Full affixes dict from parse_aff_file()
        _visited:  Internal cycle guard (do not set manually)
        _depth:    Internal recursion depth (do not set manually)

    Returns:
        Set[str]: All inflected forms including the base word
    """
    global _word_form_cache_hits, _word_form_cache_misses

    # Cache only top-level invocations: recursive calls depend on _visited.
    _use_cache = (_depth == 0 and _visited is None)
    _cache_key = None
    if _use_cache:
        _cache_key = (base_word, tuple(flags))
        cached_forms = _word_form_cache.get(_cache_key)
        if cached_forms is not None:
            _word_form_cache_hits += 1
            _word_form_cache.move_to_end(_cache_key)
            return set(cached_forms)
        _word_form_cache_misses += 1

    if _visited is None:
        _visited = set()

    word_forms: Set[str] = {base_word}

    if not flags or _depth > 3:
        return word_forms

    skip_flags: Set = {
        affixes.get('NEEDAFFIX'),
        affixes.get('CIRCUMFIX'),
        affixes.get('ONLYINCOMPOUND'),
        affixes.get('FORBIDDENWORD'),
        None, '',
    }

    cross_product_forms: List[Tuple[str, List[str]]] = []

    # Suffixes
    for flag in flags:
        if flag in skip_flags or flag not in affixes['SFX']:
            continue

        rule_set = affixes['SFX'][flag]
        for rule in rule_set['rules']:
            compiled = rule.get('compiled_condition')
            if not condition_matches(base_word, rule['condition'],
                                     is_prefix=False, compiled=compiled):
                continue
            if rule['strip']:
                if not base_word.endswith(rule['strip']):
                    continue
                derived = base_word[:-len(rule['strip'])] + rule['add']
            else:
                derived = base_word + rule['add']

            if not derived or derived in _visited:
                continue

            word_forms.add(derived)

            if rule_set['cross_product']:
                cross_product_forms.append((derived, rule['add_flags']))

            if rule['add_flags']:
                _visited.add(derived)
                word_forms.update(
                    generate_word_forms(derived, rule['add_flags'], affixes,
                                        _visited, _depth + 1)
                )

    # Prefixes
    for flag in flags:
        if flag in skip_flags or flag not in affixes['PFX']:
            continue

        rule_set = affixes['PFX'][flag]
        for rule in rule_set['rules']:
            compiled = rule.get('compiled_condition')
            if not condition_matches(base_word, rule['condition'],
                                     is_prefix=True, compiled=compiled):
                continue
            if rule['strip']:
                if not base_word.startswith(rule['strip']):
                    continue
                derived = rule['add'] + base_word[len(rule['strip']):]
            else:
                derived = rule['add'] + base_word

            if not derived or derived in _visited:
                continue

            word_forms.add(derived)

            if rule['add_flags']:
                _visited.add(derived)
                word_forms.update(
                    generate_word_forms(derived, rule['add_flags'], affixes,
                                        _visited, _depth + 1)
                )

    # Cross-product: apply prefixes to suffix-derived forms
    for derived_form, extra_flags in cross_product_forms:
        for flag in flags:
            if flag in skip_flags or flag not in affixes['PFX']:
                continue
            if not affixes['PFX'][flag]['cross_product']:
                continue
            for rule in affixes['PFX'][flag]['rules']:
                compiled = rule.get('compiled_condition')
                if not condition_matches(derived_form, rule['condition'],
                                         is_prefix=True, compiled=compiled):
                    continue
                if rule['strip']:
                    if not derived_form.startswith(rule['strip']):
                        continue
                    cross_derived = rule['add'] + derived_form[len(rule['strip']):]
                else:
                    cross_derived = rule['add'] + derived_form

                if cross_derived and cross_derived not in _visited:
                    word_forms.add(cross_derived)

    if _use_cache and _cache_key is not None:
        _word_form_cache[_cache_key] = set(word_forms)
        _word_form_cache.move_to_end(_cache_key)
        if len(_word_form_cache) > _WORD_FORM_CACHE_MAX:
            _word_form_cache.popitem(last=False)

    return word_forms


# ==============================================================================
# 2.4  REVERSE-LOOKUP  (alternative - fast but less exhaustive)
# ==============================================================================

def token_is_known(token_lower: str, base_words: Set[str], affixes: Dict,
                   sfx_rules: List[Tuple], pfx_rules: List[Tuple],
                   cross_sfx_rules: List[Tuple]) -> bool:
    """
    ALTERNATIVE reverse-lookup: check whether a token is derivable from the
    Hunspell dictionary without expanding every dictionary entry.

    STATUS: Correct and FAST (~seconds vs ~1 min for forward expansion).
    Kept as a documented alternative.  The primary path used by
    filter_tokens_by_dictionary_with_affixes() is generate_word_forms()
    (forward expansion), which is exhaustive and easier to reason about.

    Use token_is_known() directly if speed is critical and you accept
    that very deep chained derivations (depth > 1 affix) might be missed.

    Strategy: for each token, reconstruct a potential dictionary base word by
    reversing known suffix/prefix rules, then do a fast O(1) set lookup.

    Complexity: O(tokens x rules)  vs  O(entries x avg_forms) for forward.
    For pt_BR: ~few K tokens x ~few hundred rules  <<  312 K entries x 33 forms.

    Args:
        token_lower:     Lowercased token to test
        base_words:      Set of lowercased base words from the .dic file
        affixes:         Full affixes dict from parse_aff_file()
        sfx_rules:       Flat list of (add, strip, condition, cross_product)
        pfx_rules:       Flat list of (add, strip, condition, cross_product)
        cross_sfx_rules: SFX rules with cross_product=True

    Returns:
        bool: True if the token matches any dictionary form
    """
    if token_lower in base_words:
        return True

    for add, strip, condition, _ in sfx_rules:
        if add:
            if not token_lower.endswith(add):
                continue
            root = token_lower[:-len(add)]
        else:
            root = token_lower
        candidate = root + strip
        if not candidate:
            continue
        if not condition_matches(candidate, condition, is_prefix=False):
            continue
        if candidate in base_words:
            return True

    for add, strip, condition, _ in pfx_rules:
        if add:
            if not token_lower.startswith(add):
                continue
            rest = token_lower[len(add):]
        else:
            rest = token_lower
        candidate = strip + rest
        if not candidate:
            continue
        if not condition_matches(candidate, condition, is_prefix=True):
            continue
        if candidate in base_words:
            return True

    for pfx_add, pfx_strip, pfx_cond, pfx_xprod in pfx_rules:
        if not pfx_xprod:
            continue
        if pfx_add:
            if not token_lower.startswith(pfx_add):
                continue
            de_prefixed = token_lower[len(pfx_add):]
        else:
            de_prefixed = token_lower
        if not de_prefixed:
            continue
        for add, strip, condition, xprod in cross_sfx_rules:
            if add:
                if not de_prefixed.endswith(add):
                    continue
                root = de_prefixed[:-len(add)]
            else:
                root = de_prefixed
            candidate = root + strip
            if not candidate:
                continue
            if not condition_matches(candidate, condition, is_prefix=False):
                continue
            if not condition_matches(candidate, pfx_cond, is_prefix=True):
                continue
            if candidate in base_words:
                return True

    return False


# ==============================================================================
# 2.5  THREADING HELPERS  (module-level so threads can call them)
# ==============================================================================

def _expand_chunk(chunk: List[Tuple[str, List[str]]], affixes: Dict) -> Set[str]:
    """
    Thread worker: expand a list of (base_word, flags) pairs into all word
    forms using generate_word_forms().

    Defined at module level so it is safely callable from ThreadPoolExecutor
    workers without pickling issues.

    Args:
        chunk:   List of (base_word_lower, flags) tuples
        affixes: Full affixes dict (shared read-only across threads - safe
                 because it is never mutated after parse_aff_file() returns)

    Returns:
        Set[str]: All inflected surface forms for the entries in this chunk
    """
    forms: Set[str] = set()
    for base_word, flags in chunk:
        forms.update(generate_word_forms(base_word, flags, affixes))
    return forms


def _expand_chunk_compound(chunk: List[Tuple[str, List[str]]],
                           affixes: Dict) -> Dict[str, Set[str]]:
    """
    Thread worker: expand compound-capable entries, returning a per-entry mapping.

    Differs from _expand_chunk in two ways:
      1. Returns {base_word: Set[forms]} instead of a flat set so the caller
         can assign forms to role buckets (BEGIN / MIDDLE / END).
      2. The ONLYINCOMPOUND special flag is *not* skipped - stems that only
         exist inside a compound (e.g. German fuge-elements) must still be
         expanded so they can be recognised as valid compound parts.

    ── BUG FIX (dict-key collision on shared lowercased base_word) ─────────────
    German (and other) dictionaries list the same base word multiple times with
    different capitalisation and different flag sets, e.g.:
        Fluss/TpMmij   (capitalised entry — compound-BEGIN via flag j)
        fluss/TpMozm   (lowercase entry  — compound-END   via flag z)
        fluss/hke      (compound-MIDDLE form with fuge flags)
    All three lowercase to the key 'fluss'.  Overwriting with plain dict
    assignment (`result[key] = ...`) kept only the last entry's forms, dropping
    e.g. 'flusses' (genitive via SFX T from `fluss/TpMozm`) when `fluss/hke`
    happened to be written last.
    Fix: use set-union merge so EVERY entry's forms accumulate under the key.
    ────────────────────────────────────────────────────────────────────────────

    Args:
        chunk:   List of (base_word_lower, flags) tuples.
        affixes: Full affixes dict from parse_aff_file().

    Returns:
        Dict mapping each base_word to its UNION of all inflected surface forms
        across every .dic entry that shares that lowercased base string.
    """
    # Shallow-clone affixes with ONLYINCOMPOUND cleared so those stems expand
    compound_affixes = {**affixes, 'ONLYINCOMPOUND': None}
    result: Dict[str, Set[str]] = {}
    for base_word, flags in chunk:
        forms = generate_word_forms(base_word, flags, compound_affixes)
        if base_word in result:
            result[base_word].update(forms)   # ← merge, never overwrite
        else:
            result[base_word] = forms
    return result


# ==============================================================================
# 2.6  COMPOUND-WORD CHECKER
# ==============================================================================

def is_valid_compound(token: str,
                      begin_forms: Set[str],
                      middle_forms: Set[str],
                      end_forms: Set[str],
                      flag_forms: Set[str],
                      all_forms: Set[str],
                      compound_min: int = 3,
                      _depth: int = 0) -> bool:
    """
    Check whether a token is a valid Hunspell compound word.

    Handles three compound systems found in real-world dictionaries:

    1. COMPOUNDBEGIN / COMPOUNDMIDDLE / COMPOUNDEND  (German de_DE_frami style):
       Tries every split-point where each part is >= compound_min chars:
         - 2-part: left in begin_forms  AND  right in end_forms
         - 3-part: left in begin_forms  AND  mid in middle_forms  AND  right in end_forms
       In both cases the left (BEGIN) part may itself be a valid sub-compound,
       enabling recognition of long German chains like:
         'überschallschraubenzieher'
           → 'überschall' (sub-compound: 'über'+'schall') + 'schrauben' + 'zieher'
       Recursion depth is capped independently per call to avoid explosion.

    2. COMPOUNDFLAG  (any-position flag, simpler dictionaries):
       Both parts must be in flag_forms.

    3. Hyphenated tokens  (e.g. 'uebermagier-rune'):
       Splits on '-'; each sub-part must be in all_forms OR be itself a valid
       compound.  This mirrors Hunspell's European behaviour of treating the
       hyphen as a compound word separator.

    Recursion is capped at depth 4 to prevent runaway on adversarial inputs.

    Args:
        token:        Lowercased token to test.
        begin_forms:  Surface forms of COMPOUNDBEGIN-flagged dictionary entries.
        middle_forms: Surface forms of COMPOUNDMIDDLE-flagged dictionary entries.
        end_forms:    Surface forms of COMPOUNDEND-flagged dictionary entries.
        flag_forms:   Surface forms of COMPOUNDFLAG-flagged dictionary entries.
        all_forms:    Full set of known surface forms (for hyphen-split validation).
        compound_min: Minimum characters per compound segment (from COMPOUNDMIN).
        _depth:       Internal recursion guard - do not set manually.

    Returns:
        bool: True if the token is recognisable as a valid Hunspell compound.
    """
    if _depth > 4:
        return False
    n = len(token)
    if n < compound_min * 2:
        return False

    # ── Helper: is `s` a valid compound-BEGIN segment? ──────────────────────
    # A segment qualifies as a BEGIN part if it is directly in begin_forms OR
    # if it is itself a valid compound (recursive, depth-limited).  This lets
    # us handle long concatenative chains like 'überschall' = 'über'+'schall'.
    def _is_valid_begin(s: str) -> bool:
        if s in begin_forms:
            return True
        # Allow one level of recursion for sub-compounds (depth guard prevents
        # runaway; minimum length ensures we never recurse on trivial strings).
        if _depth < 3 and len(s) >= compound_min * 2:
            return is_valid_compound(s, begin_forms, middle_forms, end_forms,
                                     flag_forms, all_forms, compound_min, _depth + 1)
        return False

    # 1. Hyphenated compounds
    # European Hunspell dictionaries validate each hyphen-delimited segment
    # independently.  A hyphenated form is valid when every non-empty segment
    # is either a known surface form or itself a valid compound.
    if '-' in token:
        parts = [p for p in token.split('-') if p]
        if len(parts) >= 2 and all(
            p in all_forms or
            is_valid_compound(p, begin_forms, middle_forms, end_forms,
                              flag_forms, all_forms, compound_min, _depth + 1)
            for p in parts
        ):
            return True
        # Fall through: also try concatenation-based checks in case the hyphen
        # is part of a fuge-element stem (German 'Arbeits-' etc.).

    # 2. COMPOUNDBEGIN / COMPOUNDEND  (2-part)
    if begin_forms and end_forms:
        for i in range(compound_min, n - compound_min + 1):
            left  = token[:i]
            right = token[i:]
            if right in end_forms and _is_valid_begin(left):
                return True

    # 3. COMPOUNDBEGIN / COMPOUNDMIDDLE / COMPOUNDEND  (3-part)
    if begin_forms and middle_forms and end_forms:
        for i in range(compound_min, n - 2 * compound_min + 1):
            left = token[:i]
            if not _is_valid_begin(left):
                continue
            for j in range(i + compound_min, n - compound_min + 1):
                mid   = token[i:j]
                right = token[j:]
                if mid in middle_forms and right in end_forms:
                    return True

    # 4. COMPOUNDFLAG  (simpler: any-position flag)
    if flag_forms:
        for i in range(compound_min, n - compound_min + 1):
            left  = token[:i]
            right = token[i:]
            if left in flag_forms and right in flag_forms:
                return True

    return False


# ==============================================================================
# 2.7  MAIN FILTER FUNCTION  (forward expansion, threaded)
# ==============================================================================

def filter_tokens_by_dictionary_with_affixes(txt_file_path: str, dic_file_path: str,
                                             aff_file_path: str, output_dic_path: str,
                                             num_threads: Optional[int] = None,
                                             audit_csv_path: Optional[str] = None) -> Dict[str, int]:
    """
    Filter tokens by removing those found in the Hunspell dictionary.

    Uses **forward expansion** (generate_word_forms) to build the full set of
    valid surface forms, then removes any input token that appears in that set.
    Compound words are handled via a second pass using is_valid_compound().

    Performance improvements:

    1. Pre-compiled regex conditions - parse_aff_file() stores a compiled
       re.Pattern in every rule so condition_matches() never calls re.compile()
       at match time.  With ~10 M condition checks this saves significant time.

    2. ThreadPoolExecutor parallelism - dictionary entries are split into
       chunks proportional to the number of logical CPU cores.  Each thread
       expands its chunk independently; results are unioned at the end.
       Because generate_word_forms() is mostly string/set operations and
       the regex search calls release the GIL, threading gives a real speedup
       (typically 2-4x on a quad-core machine).

    3. Compound-word detection - after the main expansion, a second parallel
       pass builds compound-role form sets (BEGIN / MIDDLE / END / FLAG) so
       that German-style concatenative compounds (e.g. 'Uebergang',
       'Zaubertraenke') and hyphenated compounds (e.g. 'Uebermagier-Rune')
       are correctly recognised as dictionary words and removed.

    4. Trailing-dot normalisation - Hunspell (pt_BR) stores abbreviations
       as 'bomb.' so that suffix rules append directly to the stem.

    5. BOM-aware file reading - the .dic file is opened with 'utf-8-sig'
       so the leading BOM that some distributions include is silently consumed.

    6. CHECKSHARPS support - when the .aff declares CHECKSHARPS (German),
       every generated form containing 'ß' is also added in its ss-variant
       so that game tokens written with 'ss' (e.g. from older encodings or
       all-caps text) are still matched.

    7. FORBIDDENWORD exclusion - .dic entries that carry the FORBIDDENWORD
       flag are completely excluded from the valid-forms set.  Hunspell itself
       rejects such words as spelling errors; we must not remove them from the
       token list either.

    Args:
        txt_file_path:  Path to the txt file with tokens (one per line)
        dic_file_path:  Path to the Hunspell .dic file
        aff_file_path:  Path to the Hunspell .aff file with affix rules
        output_dic_path: Path where the filtered .dic file will be saved
        num_threads:    Number of worker threads.  Defaults to os.cpu_count().

    Returns:
        Dict[str, int]: Statistics - original_txt_tokens, dictionary_base_words,
            generated_word_forms, removed_tokens, compound_removed_tokens,
            remaining_tokens
    """
    if not os.path.exists(txt_file_path):
        raise FileNotFoundError(f"Token file not found: {txt_file_path}")
    if not os.path.exists(dic_file_path):
        raise FileNotFoundError(f"Dictionary file not found: {dic_file_path}")
    if not os.path.exists(aff_file_path):
        raise FileNotFoundError(f"Affix file not found: {aff_file_path}")

    if num_threads is None:
        num_threads = os.cpu_count() or 4
    print(f"Using {num_threads} threads for parallel dictionary expansion")

    # Parse affix rules (with pre-compiled conditions)
    print(f"Parsing affix rules from: {aff_file_path}")
    affixes = parse_aff_file(aff_file_path)

    flag_mode = affixes.get('flag_mode', 'single')
    print(f"FLAG mode: {flag_mode}")
    for special in ('NEEDAFFIX', 'CIRCUMFIX', 'ONLYINCOMPOUND', 'FORBIDDENWORD', 'KEEPCASE'):
        if affixes.get(special):
            print(f"  {special} pseudo-flag: {affixes[special]!r}")
    if affixes.get('CHECKSHARPS'):
        print(f"  CHECKSHARPS: active (ß↔ss variants will be added)")
    for cdir in ('COMPOUNDBEGIN', 'COMPOUNDMIDDLE', 'COMPOUNDEND',
                 'COMPOUNDFLAG', 'COMPOUNDPERMITFLAG'):
        if affixes.get(cdir):
            print(f"  {cdir}: {affixes[cdir]!r}")
    if affixes.get('COMPOUNDBEGIN') or affixes.get('COMPOUNDFLAG'):
        print(f"  COMPOUNDMIN: {affixes['COMPOUNDMIN']}")

    prefix_count = sum(len(rs['rules']) for rs in affixes['PFX'].values())
    suffix_count = sum(len(rs['rules']) for rs in affixes['SFX'].values())
    print(f"Loaded {len(affixes['PFX'])} prefix flags ({prefix_count} rules) "
          f"and {len(affixes['SFX'])} suffix flags ({suffix_count} rules)")

    # Helper: split raw flag string from .dic entry
    def _parse_entry_flags(flag_str: str) -> List[str]:
        if not flag_str:
            return []
        if flag_mode == 'long':
            return [flag_str[i:i+2] for i in range(0, len(flag_str), 2)
                    if len(flag_str[i:i+2]) == 2]
        elif flag_mode == 'num':
            return [f.strip() for f in flag_str.split(',') if f.strip()]
        else:
            return list(flag_str)

    # Use the same encoding as the .aff file (stored in affixes['encoding'])
    # utf-8-sig also strips BOM (\ufeff) for UTF-8 dictionaries
    _dic_enc = affixes.get('encoding', 'utf-8')
    if _dic_enc == 'utf-8':
        _dic_enc = 'utf-8-sig'  # strip BOM if present
    with open(dic_file_path, 'r', encoding=_dic_enc) as f:
        dic_lines = f.readlines()

    if not dic_lines:
        raise ValueError("Dictionary file is empty")

    print(f"Dictionary declared entry count: {dic_lines[0].strip()}")

    # ── FORBIDDENWORD exclusion ──────────────────────────────────────────────
    # Entries carrying the FORBIDDENWORD flag (e.g. flag 'd' in de_DE_frami)
    # are treated as misspellings by Hunspell — they must NOT be added to the
    # valid-forms set, and we must NOT remove matching tokens from our output.
    _forbid = affixes.get('FORBIDDENWORD')
    forbidden_skipped = 0

    entries: List[Tuple[str, List[str]]] = []
    for line in dic_lines[1:]:
        line = line.strip()
        if not line:
            continue
        if '/' in line:
            raw_word, flags_str = line.split('/', 1)
            flags = _parse_entry_flags(flags_str)
        else:
            raw_word, flags = line, []

        # Skip FORBIDDENWORD entries entirely — Hunspell rejects these words
        # even though they appear in the .dic (old/invalid spelling variants).
        if _forbid and _forbid in flags:
            forbidden_skipped += 1
            continue

        # Trailing-dot normalisation:
        # pt_BR.dic stores abbreviations as "bomb." so that suffix rules append
        # directly to the stem.  Stripping the dot lets "+a" -> "bomba" work
        # instead of producing the spurious form "bomb.a".
        base_word = raw_word.rstrip('.').lower()
        if base_word:
            entries.append((base_word, flags))

    if forbidden_skipped:
        print(f"Skipped {forbidden_skipped:,} FORBIDDENWORD entries (flag {_forbid!r})")
    print(f"Parsed {len(entries):,} dictionary entries")

    # Parallel forward expansion
    chunk_size = max(1, len(entries) // (num_threads * 8))
    chunks = [entries[i:i + chunk_size] for i in range(0, len(entries), chunk_size)]
    print(f"Expanding word forms: {len(chunks)} chunks x ~{chunk_size} entries "
          f"across {num_threads} threads...")

    all_dictionary_forms: Set[str] = set()
    t0 = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(_expand_chunk, chunk, affixes): i
                   for i, chunk in enumerate(chunks)}
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            all_dictionary_forms.update(future.result())
            completed += 1
            if completed % max(1, len(chunks) // 10) == 0:
                elapsed = time.time() - t0
                print(
                    f"  {completed}/{len(chunks)} chunks done ({len(all_dictionary_forms):,} forms, {elapsed:.1f}s)",
                    end='\r',
                    flush=True,
                )

    print()
    print(f"Generated {len(all_dictionary_forms):,} unique word forms "
          f"from {len(entries):,} entries in {time.time()-t0:.1f}s")

    # ── CHECKSHARPS: add ß→ss variants ──────────────────────────────────────
    # When CHECKSHARPS is active (German), Hunspell equates ß with SS in
    # uppercase contexts.  Game texts often store these words with 'ss' instead
    # of 'ß' (e.g. from older Latin-1 sources or all-caps display rendering).
    # Adding ss-variants ensures tokens like 'strasse' match 'straße'.
    if affixes.get('CHECKSHARPS'):
        ss_variants: Set[str] = set()
        for form in all_dictionary_forms:
            if 'ß' in form:
                ss_variants.add(form.replace('ß', 'ss'))
        if ss_variants:
            all_dictionary_forms.update(ss_variants)
            print(f"CHECKSHARPS: added {len(ss_variants):,} ss-variants of ß forms")

    # Build compound form sets (if compound flags are present in this dictionary)
    compound_begin_forms:  Set[str] = set()
    compound_middle_forms: Set[str] = set()
    compound_end_forms:    Set[str] = set()
    compound_flag_forms:   Set[str] = set()

    _c_begin  = affixes.get('COMPOUNDBEGIN')
    _c_middle = affixes.get('COMPOUNDMIDDLE')
    _c_end    = affixes.get('COMPOUNDEND')
    _c_flag   = affixes.get('COMPOUNDFLAG')
    _c_oic    = affixes.get('ONLYINCOMPOUND')
    _compound_min = affixes.get('COMPOUNDMIN', 3)

    _any_compound = _c_begin or _c_middle or _c_end or _c_flag
    if _any_compound:
        print(f"Compound flags detected - building compound form sets "
              f"(COMPOUNDMIN={_compound_min})...")

        # German-style dictionaries (and others) assign compound-role flags
        # (COMPOUNDBEGIN x, COMPOUNDMIDDLE y, COMPOUNDEND z) only as add_flags
        # inside affix rules, not directly on .dic entries.  For example:
        #   SFX j 0 0/xoc .    <- SFX flag 'j' produces forms with flag 'x'
        # So we must discover which ENTRY-LEVEL affix flags transitively lead to
        # compound-role flags via their add_flags chains.
        def _entry_flags_for_role(role_flag: str) -> Set[str]:
            """
            Return all SFX/PFX flags whose rules (transitively) include
            role_flag in add_flags.  Uses BFS to follow the chain
            (flag A -> add_flag B -> add_flag C -> role_flag).
            """
            direct: Set[str] = set()
            for ft in ('SFX', 'PFX'):
                for aflag, rs in affixes[ft].items():
                    if any(role_flag in r.get('add_flags', [])
                           for r in rs['rules']):
                        direct.add(aflag)
            result = set(direct)
            changed = True
            while changed:
                changed = False
                for ft in ('SFX', 'PFX'):
                    for aflag, rs in affixes[ft].items():
                        if aflag in result:
                            continue
                        if any(af in result
                               for r in rs['rules']
                               for af in r.get('add_flags', [])):
                            result.add(aflag)
                            changed = True
            return result

        begin_eflag  = _entry_flags_for_role(_c_begin)  if _c_begin  else set()
        middle_eflag = _entry_flags_for_role(_c_middle) if _c_middle else set()
        end_eflag    = _entry_flags_for_role(_c_end)    if _c_end    else set()

        def _has_compound_role(flags: List[str]) -> bool:
            fset = set(flags)
            return bool(
                (_c_begin and ((fset & begin_eflag) or (_c_begin in fset)))
                or (_c_middle and ((fset & middle_eflag) or (_c_middle in fset)))
                or (_c_end and ((fset & end_eflag) or (_c_end in fset)))
                or (_c_flag and (_c_flag in fset))
                or (_c_oic and (_c_oic in fset))
            )

        compound_entries = [(w, f) for w, f in entries if _has_compound_role(f)]
        print(f"  {len(compound_entries):,} entries carry compound/OIC flags")
        print(f"  BEGIN via entry-flags: {begin_eflag or {'(direct only)'}}")
        print(f"  END via entry-flags:   {end_eflag   or {'(direct only)'}}")

        cchunks = [compound_entries[i:i + chunk_size]
                   for i in range(0, len(compound_entries), chunk_size)]

        # Map each base_word to its compound-expanded forms (includes OIC stems).
        # ── BUG FIX (thread-merge dict collision) ───────────────────────────
        # entry_forms_map.update(chunk_result) would silently overwrite forms
        # already accumulated for a base_word that appears in multiple chunks
        # (e.g. 'fluss' from Fluss/TpMmij AND fluss/TpMozm AND fluss/hke).
        # Fix: explicitly merge using set-union so no chunk's forms are lost.
        # ─────────────────────────────────────────────────────────────────────
        entry_forms_map: Dict[str, Set[str]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            cfutures = [executor.submit(_expand_chunk_compound, ch, affixes)
                        for ch in cchunks]
            for fut in concurrent.futures.as_completed(cfutures):
                for k, v in fut.result().items():
                    if k in entry_forms_map:
                        entry_forms_map[k].update(v)   # ← merge, never overwrite
                    else:
                        entry_forms_map[k] = v

        # Assign forms to role sets.  An entry contributes to a role if it
        # either (a) carries the compound-role flag directly, OR (b) carries
        # an entry-level flag that transitively produces the compound-role flag.
        for base_word, flags in compound_entries:
            forms = entry_forms_map.get(base_word, {base_word})
            fset  = set(flags)
            if _c_begin  and (fset & begin_eflag  or _c_begin  in fset):
                compound_begin_forms.update(forms)
            if _c_middle and (fset & middle_eflag or _c_middle in fset):
                compound_middle_forms.update(forms)
            if _c_end    and (fset & end_eflag    or _c_end    in fset):
                compound_end_forms.update(forms)
            if _c_flag   and _c_flag  in fset:
                compound_flag_forms.update(forms)

        print(f"  BEGIN:{len(compound_begin_forms):,}  "
              f"MIDDLE:{len(compound_middle_forms):,}  "
              f"END:{len(compound_end_forms):,}  "
              f"FLAG:{len(compound_flag_forms):,}")

    # Load tokens
    print(f"Reading tokens from: {txt_file_path}")
    original_txt_tokens: List[str] = []
    with open(txt_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            token = line.strip()
            if token:
                original_txt_tokens.append(token)
    print(f"Loaded {len(original_txt_tokens):,} tokens to check")

    # Filter
    _use_compound = bool(_any_compound)
    filtered_tokens: List[str] = []
    removed_count = 0
    compound_removed_count = 0
    sample_removals: List[str] = []
    token_audit_rows: List[Dict[str, str]] = []

    for original_token in original_txt_tokens:
        tok_lower = original_token.lower()
        known = tok_lower in all_dictionary_forms

        if not known and _use_compound:
            # Check whether the token is a valid compound word.  This handles:
            #   - Concatenative compounds (German): Uebergang, Zaubertrank, ...
            #   - Hyphenated compounds:             Uebermagier-Rune, Elfen-Klaeger, ...
            if is_valid_compound(tok_lower,
                                 compound_begin_forms, compound_middle_forms,
                                 compound_end_forms,   compound_flag_forms,
                                 all_dictionary_forms, _compound_min):
                known = True
                compound_removed_count += 1

        if known:
            removed_count += 1
            if len(sample_removals) < 10:
                sample_removals.append(original_token)
            if audit_csv_path:
                token_audit_rows.append({
                    'token': original_token,
                    'token_lower': tok_lower,
                    'status': 'removed_known_word',
                    'match_type': 'compound_word' if (not (tok_lower in all_dictionary_forms)) else 'dictionary_form',
                })
        else:
            filtered_tokens.append(original_token)
            if audit_csv_path:
                token_audit_rows.append({
                    'token': original_token,
                    'token_lower': tok_lower,
                    'status': 'kept_neologism',
                    'match_type': '',
                })

    if _use_compound and compound_removed_count:
        print(f"  (of which {compound_removed_count:,} removed via compound-word detection)")

    if sample_removals:
        print(f"Sample removed tokens: {', '.join(sample_removals[:5])}"
              f"{'...' if len(sample_removals) > 5 else ''}")

    print(f"Removed {removed_count:,} tokens that match dictionary word forms")
    print(f"Remaining tokens: {len(filtered_tokens):,}")

    os.makedirs(os.path.dirname(output_dic_path), exist_ok=True) if os.path.dirname(output_dic_path) else None
    with open(output_dic_path, 'w', encoding='utf-8') as f:
        f.write(str(len(filtered_tokens)) + '\n')
        for token in filtered_tokens:
            f.write(token + '\n')

    print(f"Filtered tokens saved as dictionary to: {output_dic_path}")

    if audit_csv_path:
        audit_dir = os.path.dirname(audit_csv_path)
        if audit_dir:
            os.makedirs(audit_dir, exist_ok=True)
        with open(audit_csv_path, 'w', newline='', encoding='utf-8-sig') as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=['token', 'token_lower', 'status', 'match_type']
            )
            writer.writeheader()
            writer.writerows(token_audit_rows)
        print(f"Filtering audit CSV saved to: {audit_csv_path}")

    return {
        'original_txt_tokens':    len(original_txt_tokens),
        'dictionary_base_words':  len(entries),
        'generated_word_forms':   len(all_dictionary_forms),
        'removed_tokens':         removed_count,
        'compound_removed_tokens':compound_removed_count,
        'remaining_tokens':       len(filtered_tokens)
    }
