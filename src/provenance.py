import csv
import json
import os
import re
from collections import Counter
from typing import Any, Dict, List, Set, Tuple, Union

from .dashbeautiful import build_dashboard_html


_REPORT_BASENAME_RE = re.compile(r"^(?P<game>.+?)_(?P<lang>[A-Za-z0-9-]+)_token_provenance_report\.(?P<ext>jsonl|csv)$")

_BOOL_FIELDS = {
    'in_tb_tokens',
    'in_filtered_dic_base',
    'is_corpus_form',
    'is_ghost_form',
    'is_casing_inferred',
}

_LIST_FIELDS = {
    'lineage_tags',
    'source_base_words',
    'assigned_flags',
    'mandatory_flags_assigned',
    'validated_flags_assigned',
    'validated_flags_candidates',
    'flag_generated_forms',
    'dropped_generated_by',
    'step4_related_bases',
    'source_games',
    'origin_classes',
    'filter_statuses',
    'filter_match_types',
}

_BASE_REPORT_FIELDNAMES = [
    'game', 'language', 'token', 'token_lower',
    'origin_class', 'lineage_tags',
    'in_tb_tokens', 'tb_key', 'tb_source_entity', 'in_filtered_dic_base',
    'is_corpus_form', 'is_ghost_form', 'is_casing_inferred',
    'source_base_words',
    'assigned_flags', 'mandatory_flags_assigned', 'validated_flags_assigned', 'validated_flags_candidates',
    'filter_status', 'filter_match_type',
    'flag_evidence_summary', 'step4_related_bases',
]

_ANK_EXTRA_FIELDNAMES = [
    'source_games',
    'source_reports_count',
    'source_rows_merged',
    'origin_classes',
    'filter_statuses',
    'filter_match_types',
]


def _read_jsonl_records(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return []
    rows: List[Dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8') as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _read_token_txt(path: str) -> Set[str]:
    if not path or not os.path.isfile(path):
        return set()
    out: Set[str] = set()
    with open(path, 'r', encoding='utf-8') as fh:
        for raw in fh:
            token = raw.strip()
            if token:
                out.add(token)
    return out


def _read_filtered_dic(path: str) -> Set[str]:
    if not path or not os.path.isfile(path):
        return set()
    out: Set[str] = set()
    with open(path, 'r', encoding='utf-8-sig') as fh:
        for idx, raw in enumerate(fh):
            if idx == 0:
                continue
            token = raw.strip()
            if not token:
                continue
            out.add(token.split('/', 1)[0].strip())
    return out


def _split_tb_keys(value: Any) -> List[str]:
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(_split_tb_keys(item))
        return out
    text = str(value or '').strip()
    if not text:
        return []
    return [part.strip() for part in text.split('|') if part.strip()]


def _unique_preserve_order(values: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for value in values:
        item = str(value or '').strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _merge_filter_audit_info(existing: Dict[str, str], incoming: Dict[str, str]) -> Dict[str, str]:
    if not existing:
        return dict(incoming)

    existing_status = str(existing.get('status', '')).strip()
    incoming_status = str(incoming.get('status', '')).strip()
    status = existing_status
    if incoming_status == 'kept_neologism' or not status:
        status = incoming_status

    match_type = str(existing.get('match_type', '')).strip() or str(incoming.get('match_type', '')).strip()
    merged_tb_keys = _unique_preserve_order(
        _split_tb_keys(existing.get('tb_key', '')) + _split_tb_keys(incoming.get('tb_key', ''))
    )

    return {
        'status': status,
        'match_type': match_type,
        'tb_key': ' | '.join(merged_tb_keys),
    }


def _tb_keys_from_filter_info(info: Dict[str, str]) -> List[str]:
    if not info:
        return []
    if str(info.get('status', '')).strip() != 'kept_neologism':
        return []
    return _unique_preserve_order(_split_tb_keys(info.get('tb_key', '')))


def _read_filter_audit(path: str) -> Dict[str, Dict[str, str]]:
    if not path or not os.path.isfile(path):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with open(path, 'r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            token = (row.get('token') or '').strip()
            if token:
                row_info = {
                    'status': (row.get('status') or '').strip(),
                    'match_type': (row.get('match_type') or '').strip(),
                    'tb_key': (row.get('tb_key') or '').strip(),
                }
                if token in out:
                    out[token] = _merge_filter_audit_info(out[token], row_info)
                else:
                    out[token] = row_info
    return out


def _extract_tb_source_entity(tb_key: str) -> str:
    if not tb_key:
        return ''
    first_key = str(tb_key).split('|', 1)[0].strip()
    parts = [part.strip() for part in first_key.split('.') if part.strip()]
    if len(parts) >= 2:
        return f"{parts[0]}:{parts[1]}"
    return first_key


def _split_pipe_list(value: Any) -> List[str]:
    text = str(value or '').strip()
    if not text:
        return []
    return [part.strip() for part in text.split('|') if part.strip()]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or '').strip().lower()
    return text in {'1', 'true', 'yes', 'y'}


def _normalize_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, tuple) or isinstance(value, set):
        return [str(v).strip() for v in value if str(v).strip()]
    return _split_pipe_list(value)


def _normalize_consolidated_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out['token'] = str(out.get('token', '')).strip()
    out['token_lower'] = str(out.get('token_lower', '')).strip().lower() or out['token'].lower()
    out['game'] = str(out.get('game', '')).strip()
    out['language'] = str(out.get('language', '')).strip()
    out['origin_class'] = str(out.get('origin_class', '')).strip() or 'generated_unknown'
    out['filter_status'] = str(out.get('filter_status', '')).strip()
    out['filter_match_type'] = str(out.get('filter_match_type', '')).strip()

    for key in _BOOL_FIELDS:
        out[key] = _as_bool(out.get(key, False))

    for key in _LIST_FIELDS:
        out[key] = _normalize_list(out.get(key, []))

    raw_flag_evidence = out.get('flag_evidence', [])
    if isinstance(raw_flag_evidence, list):
        clean_evidence: List[Dict[str, Any]] = []
        for ev in raw_flag_evidence:
            if isinstance(ev, dict):
                clean_evidence.append(dict(ev))
        out['flag_evidence'] = clean_evidence
    else:
        out['flag_evidence'] = []

    return out


def _read_consolidated_report_jsonl(path: str) -> List[Dict[str, Any]]:
    return [_normalize_consolidated_row(row) for row in _read_jsonl_records(path)]


def _read_consolidated_report_csv(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.isfile(path):
        return []

    rows: List[Dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            row = dict(raw)
            for key in _LIST_FIELDS:
                row[key] = _split_pipe_list(row.get(key, ''))
            for key in _BOOL_FIELDS:
                row[key] = _as_bool(row.get(key, False))
            row['flag_evidence'] = []
            rows.append(_normalize_consolidated_row(row))
    return rows


def _read_consolidated_report(path: str) -> Tuple[List[Dict[str, Any]], str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == '.jsonl':
        return _read_consolidated_report_jsonl(path), 'jsonl'
    if ext == '.csv':
        jsonl_peer = os.path.splitext(path)[0] + '.jsonl'
        if os.path.isfile(jsonl_peer):
            return _read_consolidated_report_jsonl(jsonl_peer), 'jsonl'
        return _read_consolidated_report_csv(path), 'csv'
    raise ValueError(f'Unsupported consolidated report format: {path}')


def _discover_consolidated_reports(work_dir: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    discovered: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not os.path.isdir(work_dir):
        return discovered

    for entry in sorted(os.listdir(work_dir)):
        match = _REPORT_BASENAME_RE.match(entry)
        if not match:
            continue

        game = match.group('game').upper()
        lang = match.group('lang').lower()
        ext = match.group('ext').lower()
        key = (game, lang)

        bucket = discovered.setdefault(
            key,
            {'game': game, 'language': lang, 'jsonl_path': '', 'csv_path': ''},
        )
        full_path = os.path.join(work_dir, entry)
        if ext == 'jsonl':
            bucket['jsonl_path'] = full_path
        elif ext == 'csv':
            bucket['csv_path'] = full_path

    for info in discovered.values():
        if info['jsonl_path']:
            info['preferred_path'] = info['jsonl_path']
            info['preferred_format'] = 'jsonl'
        elif info['csv_path']:
            info['preferred_path'] = info['csv_path']
            info['preferred_format'] = 'csv'
        else:
            info['preferred_path'] = ''
            info['preferred_format'] = ''

    return discovered


def _normalize_selector_games(games: Union[List[str], str], discovered_games: List[str]) -> List[str]:
    if isinstance(games, str):
        token = games.strip().lower()
        if token == 'all':
            return discovered_games
        if not token:
            return []
        return [games.strip().upper()]

    cleaned = [str(g).strip().upper() for g in games if str(g).strip()]
    return list(dict.fromkeys(cleaned))


def _normalize_selector_languages(languages: Union[List[str], str], discovered_languages: List[str]) -> List[str]:
    if isinstance(languages, str):
        token = languages.strip().lower()
        if token == 'all':
            return discovered_languages
        if not token:
            return []
        return [token]

    cleaned = [str(lang).strip().lower() for lang in languages if str(lang).strip()]
    return list(dict.fromkeys(cleaned))


def _row_to_csv_record(row: Dict[str, Any]) -> Dict[str, Any]:
    evidence_summary: List[str] = []
    for ev in row.get('flag_evidence', []):
        evidence_summary.append(
            f"{ev.get('base_word', '')}:{ev.get('flag', '')}:{ev.get('hit_count', 0)}/{ev.get('derived_count', 0)}"
        )

    record = {
        'game': row.get('game', ''),
        'language': row.get('language', ''),
        'token': row.get('token', ''),
        'token_lower': row.get('token_lower', ''),
        'origin_class': row.get('origin_class', ''),
        'lineage_tags': ' | '.join(_normalize_list(row.get('lineage_tags', []))),
        'in_tb_tokens': bool(row.get('in_tb_tokens', False)),
        'tb_key': row.get('tb_key', ''),
        'tb_source_entity': row.get('tb_source_entity', ''),
        'in_filtered_dic_base': bool(row.get('in_filtered_dic_base', False)),
        'is_corpus_form': bool(row.get('is_corpus_form', False)),
        'is_ghost_form': bool(row.get('is_ghost_form', False)),
        'is_casing_inferred': bool(row.get('is_casing_inferred', False)),
        'source_base_words': ' | '.join(_normalize_list(row.get('source_base_words', []))),
        'assigned_flags': ' | '.join(_normalize_list(row.get('assigned_flags', []))),
        'mandatory_flags_assigned': ' | '.join(_normalize_list(row.get('mandatory_flags_assigned', []))),
        'validated_flags_assigned': ' | '.join(_normalize_list(row.get('validated_flags_assigned', []))),
        'validated_flags_candidates': ' | '.join(_normalize_list(row.get('validated_flags_candidates', []))),
        'filter_status': row.get('filter_status', ''),
        'filter_match_type': row.get('filter_match_type', ''),
        'flag_evidence_summary': ' ; '.join(evidence_summary),
        'step4_related_bases': ' | '.join(_normalize_list(row.get('step4_related_bases', []))),
    }

    if row.get('source_games') is not None:
        record['source_games'] = ' | '.join(_normalize_list(row.get('source_games', [])))
        record['source_reports_count'] = int(row.get('source_reports_count', 0) or 0)
        record['source_rows_merged'] = int(row.get('source_rows_merged', 0) or 0)
        record['origin_classes'] = ' | '.join(_normalize_list(row.get('origin_classes', [])))
        record['filter_statuses'] = ' | '.join(_normalize_list(row.get('filter_statuses', [])))
        record['filter_match_types'] = ' | '.join(_normalize_list(row.get('filter_match_types', [])))

    return record


def _write_consolidated_rows(
    rows: List[Dict[str, Any]],
    base_name: str,
    work_dir: str,
    output_formats: List[str] | None,
    include_ank_extras: bool = False,
) -> Dict[str, Any]:
    formats = {f.strip().lower() for f in (output_formats or ['csv', 'jsonl']) if str(f).strip()}
    csv_path = os.path.join(work_dir, f"{base_name}.csv")
    jsonl_path = os.path.join(work_dir, f"{base_name}.jsonl")

    if 'jsonl' in formats:
        with open(jsonl_path, 'w', encoding='utf-8') as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + '\n')

    if 'csv' in formats:
        fieldnames = list(_BASE_REPORT_FIELDNAMES)
        if include_ank_extras:
            fieldnames.extend(_ANK_EXTRA_FIELDNAMES)

        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(_row_to_csv_record(row))

    return {
        'report_rows': len(rows),
        'csv_path': csv_path if 'csv' in formats else None,
        'jsonl_path': jsonl_path if 'jsonl' in formats else None,
    }


def build_consolidated_provenance_report(
    game: str,
    lang: str,
    work_dir: str,
    token_txt_path: str,
    filtered_dic_path: str,
    munch_provenance_jsonl_path: str,
    step4_provenance_jsonl_path: str = '',
    filter_audit_csv_path: str = '',
    output_formats: List[str] | None = None,
) -> Dict[str, Any]:
    """Build per-final-token consolidated explainability report for one pair."""
    tb_tokens = _read_token_txt(token_txt_path)
    tb_tokens_lower = {t.lower() for t in tb_tokens}
    filtered_bases = _read_filtered_dic(filtered_dic_path)
    filtered_bases_lower = {t.lower() for t in filtered_bases}

    step4_rows = _read_jsonl_records(step4_provenance_jsonl_path)
    step4_by_base: Dict[str, Dict[str, Any]] = {}
    step4_form_to_bases: Dict[str, Set[str]] = {}
    for row in step4_rows:
        bw = str(row.get('base_word', '')).strip()
        if not bw:
            continue
        step4_by_base[bw] = row
        for form in row.get('found_forms', []) + row.get('ghost_forms', []):
            key = str(form or '').strip().lower()
            if key:
                step4_form_to_bases.setdefault(key, set()).add(bw)

    filter_audit = _read_filter_audit(filter_audit_csv_path)
    filter_audit_lower: Dict[str, List[Dict[str, str]]] = {}
    for token, info in filter_audit.items():
        token_lower = token.lower()
        filter_audit_lower.setdefault(token_lower, []).append(info)

    munch_rows = _read_jsonl_records(munch_provenance_jsonl_path)
    final_rows: List[Dict[str, Any]] = []
    for row in munch_rows:
        if not row.get('in_compressed_output', False):
            continue

        token = str(row.get('token', '')).strip()
        if not token:
            continue

        token_lower = token.lower()
        source_bases = [str(b).strip() for b in row.get('source_base_words', []) if str(b).strip()]

        flag_evidence: List[Dict[str, Any]] = []
        for base in source_bases:
            base_step4 = step4_by_base.get(base)
            if not base_step4:
                continue
            for ev in base_step4.get('flag_evidence', []) or []:
                item = dict(ev)
                item['base_word'] = base
                flag_evidence.append(item)

        token_filter_candidates: List[Dict[str, str]] = []
        direct_filter_info = filter_audit.get(token)
        if direct_filter_info:
            token_filter_candidates.append(direct_filter_info)
        token_filter_candidates.extend(filter_audit_lower.get(token_lower, []))

        filter_info = token_filter_candidates[0] if token_filter_candidates else {}

        resolved_tb_keys: List[str] = []
        for info in token_filter_candidates:
            resolved_tb_keys.extend(_tb_keys_from_filter_info(info))

        # Propagate TB lineage from kept Step-2 base tokens into generated/corpus forms.
        for base in source_bases:
            base_candidates: List[Dict[str, str]] = []
            base_direct_info = filter_audit.get(base)
            if base_direct_info:
                base_candidates.append(base_direct_info)
            base_candidates.extend(filter_audit_lower.get(base.lower(), []))
            for info in base_candidates:
                resolved_tb_keys.extend(_tb_keys_from_filter_info(info))

        tb_keys = _unique_preserve_order(resolved_tb_keys)
        tb_key_value = ' | '.join(tb_keys)

        row_out = {
            'game': game,
            'language': lang,
            'token': token,
            'token_lower': token_lower,
            'origin_class': row.get('origin_class', 'generated_unknown'),
            'lineage_tags': row.get('lineage_tags', []),
            'in_tb_tokens': token in tb_tokens or token_lower in tb_tokens_lower,
            'tb_key': tb_key_value,
            'tb_source_entity': _extract_tb_source_entity(tb_key_value),
            'in_filtered_dic_base': token in filtered_bases or token_lower in filtered_bases_lower,
            'is_corpus_form': bool(row.get('is_corpus_form', False)),
            'is_ghost_form': bool(row.get('is_ghost_form', False)),
            'is_casing_inferred': bool(row.get('is_casing_inferred', False)),
            'source_base_words': source_bases,
            'assigned_flags': row.get('assigned_flags', []),
            'mandatory_flags_assigned': row.get('mandatory_flags_assigned', []),
            'validated_flags_assigned': row.get('validated_flags_assigned', []),
            'validated_flags_candidates': row.get('validated_flags_candidates', []),
            'flag_generated_forms': row.get('flag_generated_forms', []),
            'filter_status': filter_info.get('status', ''),
            'filter_match_type': filter_info.get('match_type', ''),
            'flag_evidence': flag_evidence,
            'dropped_generated_by': row.get('dropped_generated_by', []),
            'step4_related_bases': sorted(step4_form_to_bases.get(token_lower, set())),
        }
        final_rows.append(row_out)

    os.makedirs(work_dir, exist_ok=True)
    base_name = f"{game}_{lang}_token_provenance_report"
    return _write_consolidated_rows(
        rows=final_rows,
        base_name=base_name,
        work_dir=work_dir,
        output_formats=output_formats,
        include_ank_extras=False,
    )


def build_ank_superconsolidated_provenance_report(
        work_dir: str,
        games: Union[List[str], str] = 'all',
        languages: Union[List[str], str] = 'all',
        output_formats: List[str] | None = None,
        include_ank_sources: bool = False,
        strict_mode: bool = False,
) -> Dict[str, Any]:
        """Merge game-level consolidated reports into ANK per-language reports."""
        discovered = _discover_consolidated_reports(work_dir)
        discovered_games = sorted({game for game, _ in discovered.keys() if include_ank_sources or game != 'ANK'})
        discovered_languages = sorted({lang for _, lang in discovered.keys()})

        selected_games = _normalize_selector_games(games, discovered_games)
        selected_languages = _normalize_selector_languages(languages, discovered_languages)

        if not selected_games:
                raise ValueError('No source games selected/found in consolidated reports')
        if not selected_languages:
                raise ValueError('No languages selected/found in consolidated reports')

        runs: List[Dict[str, Any]] = []
        for lang in selected_languages:
                run: Dict[str, Any] = {
                        'language': lang,
                        'target_game': 'ANK',
                        'status': 'ok',
                        'error': '',
                        'source_games': selected_games,
                        'source_reports': [],
                        'artifacts': {},
                        'metrics': {},
                }

                merge_store: Dict[str, Dict[str, Any]] = {}
                for game in selected_games:
                        if not include_ank_sources and game == 'ANK':
                                continue

                        info = discovered.get((game, lang), {})
                        source_path = str(info.get('preferred_path', '')).strip()
                        if not source_path:
                                run['metrics'].setdefault('missing_reports', []).append(f'{game}/{lang}')
                                if strict_mode:
                                        run['status'] = 'failed'
                                        run['error'] = f'Missing source report for {game}/{lang}'
                                        break
                                continue

                        try:
                                source_rows, source_format = _read_consolidated_report(source_path)
                        except Exception as err:
                                if strict_mode:
                                        run['status'] = 'failed'
                                        run['error'] = f'{type(err).__name__}: {err}'
                                        break
                                run['metrics'].setdefault('failed_reports', []).append(f'{game}/{lang}')
                                continue

                        run['source_reports'].append({
                                'game': game,
                                'language': lang,
                                'path': source_path,
                                'format': source_format,
                                'rows': len(source_rows),
                        })

                        for row in source_rows:
                                token = str(row.get('token', '')).strip()
                                token_lower = str(row.get('token_lower', '')).strip().lower() or token.lower()
                                if not token_lower:
                                        continue

                                current = merge_store.get(token_lower)
                                if current is None:
                                        current = {
                                                'game': 'ANK',
                                                'language': lang,
                                                'token': token,
                                                'token_lower': token_lower,
                                                'origin_class': str(row.get('origin_class', 'generated_unknown') or 'generated_unknown'),
                                                'lineage_tags': set(_normalize_list(row.get('lineage_tags', []))),
                                                'in_tb_tokens': bool(row.get('in_tb_tokens', False)),
                                                'in_filtered_dic_base': bool(row.get('in_filtered_dic_base', False)),
                                                'is_corpus_form': bool(row.get('is_corpus_form', False)),
                                                'is_ghost_form': bool(row.get('is_ghost_form', False)),
                                                'is_casing_inferred': bool(row.get('is_casing_inferred', False)),
                                                'source_base_words': set(_normalize_list(row.get('source_base_words', []))),
                                                'assigned_flags': set(_normalize_list(row.get('assigned_flags', []))),
                                                'mandatory_flags_assigned': set(_normalize_list(row.get('mandatory_flags_assigned', []))),
                                                'validated_flags_assigned': set(_normalize_list(row.get('validated_flags_assigned', []))),
                                                'validated_flags_candidates': set(_normalize_list(row.get('validated_flags_candidates', []))),
                                                'flag_generated_forms': set(_normalize_list(row.get('flag_generated_forms', []))),
                                                'filter_status': str(row.get('filter_status', '')).strip(),
                                                'filter_match_type': str(row.get('filter_match_type', '')).strip(),
                                                'flag_evidence': [],
                                                'flag_evidence_seen': set(),
                                                'dropped_generated_by': set(_normalize_list(row.get('dropped_generated_by', []))),
                                                'step4_related_bases': set(_normalize_list(row.get('step4_related_bases', []))),
                                                'source_games': {game},
                                                'source_reports_count': 1,
                                                'source_rows_merged': 1,
                                                'origin_classes': {str(row.get('origin_class', 'generated_unknown') or 'generated_unknown')},
                                                'filter_statuses': {str(row.get('filter_status', '')).strip()} if str(row.get('filter_status', '')).strip() else set(),
                                                'filter_match_types': {str(row.get('filter_match_type', '')).strip()} if str(row.get('filter_match_type', '')).strip() else set(),
                                                'source_report_keys': {(game, lang)},
                                        }
                                        for ev in row.get('flag_evidence', []):
                                                if isinstance(ev, dict):
                                                        ev_key = json.dumps(ev, sort_keys=True, ensure_ascii=False)
                                                        if ev_key not in current['flag_evidence_seen']:
                                                                current['flag_evidence_seen'].add(ev_key)
                                                                current['flag_evidence'].append(dict(ev))
                                        merge_store[token_lower] = current
                                        continue

                                if token and (not current['token'] or (len(token), token.casefold(), token) < (len(current['token']), current['token'].casefold(), current['token'])):
                                        current['token'] = token

                                current['in_tb_tokens'] = current['in_tb_tokens'] or bool(row.get('in_tb_tokens', False))
                                current['in_filtered_dic_base'] = current['in_filtered_dic_base'] or bool(row.get('in_filtered_dic_base', False))
                                current['is_corpus_form'] = current['is_corpus_form'] or bool(row.get('is_corpus_form', False))
                                current['is_ghost_form'] = current['is_ghost_form'] or bool(row.get('is_ghost_form', False))
                                current['is_casing_inferred'] = current['is_casing_inferred'] or bool(row.get('is_casing_inferred', False))

                                current['lineage_tags'].update(_normalize_list(row.get('lineage_tags', [])))
                                current['source_base_words'].update(_normalize_list(row.get('source_base_words', [])))
                                current['assigned_flags'].update(_normalize_list(row.get('assigned_flags', [])))
                                current['mandatory_flags_assigned'].update(_normalize_list(row.get('mandatory_flags_assigned', [])))
                                current['validated_flags_assigned'].update(_normalize_list(row.get('validated_flags_assigned', [])))
                                current['validated_flags_candidates'].update(_normalize_list(row.get('validated_flags_candidates', [])))
                                current['flag_generated_forms'].update(_normalize_list(row.get('flag_generated_forms', [])))
                                current['dropped_generated_by'].update(_normalize_list(row.get('dropped_generated_by', [])))
                                current['step4_related_bases'].update(_normalize_list(row.get('step4_related_bases', [])))

                                row_origin = str(row.get('origin_class', 'generated_unknown') or 'generated_unknown')
                                current['origin_classes'].add(row_origin)
                                if len(current['origin_classes']) > 1:
                                        current['origin_class'] = 'mixed'
                                else:
                                        current['origin_class'] = row_origin

                                row_status = str(row.get('filter_status', '')).strip()
                                if row_status:
                                        current['filter_statuses'].add(row_status)
                                row_match = str(row.get('filter_match_type', '')).strip()
                                if row_match:
                                        current['filter_match_types'].add(row_match)

                                if current['filter_statuses']:
                                        current['filter_status'] = ' | '.join(sorted(current['filter_statuses']))
                                if current['filter_match_types']:
                                        current['filter_match_type'] = ' | '.join(sorted(current['filter_match_types']))

                                current['source_games'].add(game)
                                current['source_rows_merged'] += 1
                                current['source_report_keys'].add((game, lang))
                                current['source_reports_count'] = len(current['source_report_keys'])

                                for ev in row.get('flag_evidence', []):
                                        if isinstance(ev, dict):
                                                ev_key = json.dumps(ev, sort_keys=True, ensure_ascii=False)
                                                if ev_key not in current['flag_evidence_seen']:
                                                        current['flag_evidence_seen'].add(ev_key)
                                                        current['flag_evidence'].append(dict(ev))

                if run['status'] != 'failed':
                        merged_rows: List[Dict[str, Any]] = []
                        for token_lower, row in sorted(merge_store.items(), key=lambda kv: kv[0]):
                                _ = token_lower
                                merged_rows.append({
                                    'game': ' | '.join(sorted(row['source_games'])),
                                        'language': lang,
                                        'token': row['token'],
                                        'token_lower': row['token_lower'],
                                        'origin_class': row['origin_class'],
                                        'lineage_tags': sorted(row['lineage_tags']),
                                        'in_tb_tokens': row['in_tb_tokens'],
                                        'in_filtered_dic_base': row['in_filtered_dic_base'],
                                        'is_corpus_form': row['is_corpus_form'],
                                        'is_ghost_form': row['is_ghost_form'],
                                        'is_casing_inferred': row['is_casing_inferred'],
                                        'source_base_words': sorted(row['source_base_words']),
                                        'assigned_flags': sorted(row['assigned_flags']),
                                        'mandatory_flags_assigned': sorted(row['mandatory_flags_assigned']),
                                        'validated_flags_assigned': sorted(row['validated_flags_assigned']),
                                        'validated_flags_candidates': sorted(row['validated_flags_candidates']),
                                        'flag_generated_forms': sorted(row['flag_generated_forms']),
                                        'filter_status': row.get('filter_status', ''),
                                        'filter_match_type': row.get('filter_match_type', ''),
                                        'flag_evidence': row.get('flag_evidence', []),
                                        'dropped_generated_by': sorted(row['dropped_generated_by']),
                                        'step4_related_bases': sorted(row['step4_related_bases']),
                                        'source_games': sorted(row['source_games']),
                                        'source_reports_count': int(row['source_reports_count']),
                                        'source_rows_merged': int(row['source_rows_merged']),
                                        'origin_classes': sorted(row['origin_classes']),
                                        'filter_statuses': sorted(row['filter_statuses']),
                                        'filter_match_types': sorted(row['filter_match_types']),
                                })

                        if not merged_rows:
                                run['status'] = 'failed' if strict_mode else 'skipped'
                                run['error'] = f'No source rows available for language {lang}'
                        else:
                                base_name = f'ANK_{lang}_token_provenance_report'
                                write_result = _write_consolidated_rows(
                                        rows=merged_rows,
                                        base_name=base_name,
                                        work_dir=work_dir,
                                        output_formats=output_formats,
                                        include_ank_extras=True,
                                )
                                run['artifacts']['token_provenance_csv'] = write_result.get('csv_path')
                                run['artifacts']['token_provenance_jsonl'] = write_result.get('jsonl_path')
                                run['metrics']['report_rows'] = write_result.get('report_rows', 0)
                                run['metrics']['source_reports_count'] = len(run['source_reports'])
                                run['metrics']['source_games_covered'] = sorted({r['game'] for r in run['source_reports']})
                                source_rows_before_merge = sum(int(r.get('rows', 0) or 0) for r in run['source_reports'])
                                run['metrics']['source_rows_before_merge'] = source_rows_before_merge
                                run['metrics']['deduplicated_rows_removed'] = max(0, source_rows_before_merge - int(write_result.get('report_rows', 0) or 0))

                runs.append(run)
                if strict_mode and run['status'] == 'failed':
                        break

        summary = {
                'status': 'ok' if all(r['status'] == 'ok' for r in runs) else 'partial',
                'work_dir': work_dir,
                'selected_games': selected_games,
                'selected_languages': selected_languages,
                'processed_languages': len(runs),
                'ok_languages': sum(1 for r in runs if r['status'] == 'ok'),
                'skipped_languages': sum(1 for r in runs if r['status'] == 'skipped'),
                'failed_languages': sum(1 for r in runs if r['status'] == 'failed'),
        }

        return {
                'runs': runs,
                'summary': summary,
        }


def _build_dashboard_analytics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_rows = len(rows)
        origin_counter: Counter[str] = Counter()
        status_counter: Counter[str] = Counter()
        match_counter: Counter[str] = Counter()
        lineage_counter: Counter[str] = Counter()
        flag_counter: Counter[str] = Counter()
        source_game_counter: Counter[str] = Counter()
        token_length_counter: Counter[str] = Counter()

        booleans = {
                'in_tb_tokens': 0,
                'in_filtered_dic_base': 0,
                'is_corpus_form': 0,
                'is_ghost_form': 0,
                'is_casing_inferred': 0,
        }

        for row in rows:
                origin = str(row.get('origin_class', 'generated_unknown') or 'generated_unknown')
                origin_counter[origin] += 1

                status = str(row.get('filter_status', '')).strip() or '(empty)'
                status_counter[status] += 1
                match_type = str(row.get('filter_match_type', '')).strip() or '(empty)'
                match_counter[match_type] += 1

                for field in booleans:
                        if bool(row.get(field, False)):
                                booleans[field] += 1

                for tag in _normalize_list(row.get('lineage_tags', [])):
                        lineage_counter[tag] += 1
                for flag in _normalize_list(row.get('assigned_flags', [])):
                        flag_counter[flag] += 1
                for game in _normalize_list(row.get('source_games', [])):
                        source_game_counter[game] += 1

                token_len = len(str(row.get('token', '')).strip())
                if token_len <= 4:
                        token_length_counter['1-4'] += 1
                elif token_len <= 8:
                        token_length_counter['5-8'] += 1
                elif token_len <= 12:
                        token_length_counter['9-12'] += 1
                else:
                        token_length_counter['13+'] += 1

        return {
                'totals': {
                        'rows': total_rows,
                        'distinct_token_lower': len({str(r.get('token_lower', '')).strip().lower() for r in rows if str(r.get('token_lower', '')).strip()}),
                        'distinct_tokens_surface': len({str(r.get('token', '')).strip() for r in rows if str(r.get('token', '')).strip()}),
                },
                'booleans': booleans,
                'origin_class': dict(origin_counter.most_common()),
                'filter_status': dict(status_counter.most_common()),
                'filter_match_type': dict(match_counter.most_common()),
                'top_lineage_tags': dict(lineage_counter.most_common(20)),
                'top_assigned_flags': dict(flag_counter.most_common(20)),
                'source_games': dict(source_game_counter.most_common()),
                'token_length_buckets': dict(token_length_counter),
        }


def generate_consolidated_report_dashboard(
    report_path: str,
    output_html_path: str = '',
    dashboard_title: str = '',
) -> Dict[str, Any]:
    """Generate a self-contained interactive HTML dashboard from a consolidated report."""
    source_path = str(report_path or '').strip()
    if not source_path:
        raise ValueError('report_path is required')
    if not os.path.isfile(source_path):
        raise FileNotFoundError(f'Report file not found: {source_path}')

    rows, used_format = _read_consolidated_report(source_path)
    analytics = _build_dashboard_analytics(rows)

    title = str(dashboard_title or '').strip()
    if not title:
        title = f"Consolidated Provenance Dashboard - {os.path.basename(source_path)}"

    if not output_html_path:
        stem, _ = os.path.splitext(source_path)
        output_html_path = stem + '_dashboard.html'

    payload = {
        'source_path': source_path,
        'source_format': used_format,
        'row_count': len(rows),
        'rows': rows,
        'analytics': analytics,
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    html_doc = build_dashboard_html(
        title=title,
        source_path=source_path,
        payload_json=payload_json,
    )

    with open(output_html_path, 'w', encoding='utf-8') as fh:
        fh.write(html_doc)

    return {
        'status': 'ok',
        'source_path': source_path,
        'source_format': used_format,
        'output_html_path': output_html_path,
        'rows': len(rows),
    }

