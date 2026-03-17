import csv
import json
import os
from typing import Any, Dict, List, Set


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


def _read_filter_audit(path: str) -> Dict[str, Dict[str, str]]:
    if not path or not os.path.isfile(path):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with open(path, 'r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            token = (row.get('token') or '').strip()
            if token:
                out[token] = {
                    'status': (row.get('status') or '').strip(),
                    'match_type': (row.get('match_type') or '').strip(),
                }
    return out


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
    formats = {f.strip().lower() for f in (output_formats or ['csv', 'jsonl']) if str(f).strip()}

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
    filter_audit_lower: Dict[str, Dict[str, str]] = {}
    for token, info in filter_audit.items():
        filter_audit_lower[token.lower()] = info

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

        filter_info = filter_audit.get(token) or filter_audit_lower.get(token_lower) or {}

        row_out = {
            'game': game,
            'language': lang,
            'token': token,
            'token_lower': token_lower,
            'origin_class': row.get('origin_class', 'generated_unknown'),
            'lineage_tags': row.get('lineage_tags', []),
            'in_tb_tokens': token in tb_tokens or token_lower in tb_tokens_lower,
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
    csv_path = os.path.join(work_dir, f"{base_name}.csv")
    jsonl_path = os.path.join(work_dir, f"{base_name}.jsonl")

    if 'jsonl' in formats:
        with open(jsonl_path, 'w', encoding='utf-8') as fh:
            for row in final_rows:
                fh.write(json.dumps(row, ensure_ascii=False) + '\n')

    if 'csv' in formats:
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as fh:
            fieldnames = [
                'game', 'language', 'token', 'token_lower',
                'origin_class', 'lineage_tags',
                'in_tb_tokens', 'in_filtered_dic_base',
                'is_corpus_form', 'is_ghost_form', 'is_casing_inferred',
                'source_base_words',
                'assigned_flags', 'mandatory_flags_assigned', 'validated_flags_assigned', 'validated_flags_candidates',
                'filter_status', 'filter_match_type',
                'flag_evidence_summary', 'step4_related_bases',
            ]
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in final_rows:
                evidence_summary: List[str] = []
                for ev in row.get('flag_evidence', []):
                    evidence_summary.append(
                        f"{ev.get('base_word', '')}:{ev.get('flag', '')}:{ev.get('hit_count', 0)}/{ev.get('derived_count', 0)}"
                    )
                writer.writerow({
                    'game': row['game'],
                    'language': row['language'],
                    'token': row['token'],
                    'token_lower': row['token_lower'],
                    'origin_class': row['origin_class'],
                    'lineage_tags': ' | '.join(row.get('lineage_tags', [])),
                    'in_tb_tokens': row['in_tb_tokens'],
                    'in_filtered_dic_base': row['in_filtered_dic_base'],
                    'is_corpus_form': row['is_corpus_form'],
                    'is_ghost_form': row['is_ghost_form'],
                    'is_casing_inferred': row['is_casing_inferred'],
                    'source_base_words': ' | '.join(row.get('source_base_words', [])),
                    'assigned_flags': ' | '.join(row.get('assigned_flags', [])),
                    'mandatory_flags_assigned': ' | '.join(row.get('mandatory_flags_assigned', [])),
                    'validated_flags_assigned': ' | '.join(row.get('validated_flags_assigned', [])),
                    'validated_flags_candidates': ' | '.join(row.get('validated_flags_candidates', [])),
                    'filter_status': row.get('filter_status', ''),
                    'filter_match_type': row.get('filter_match_type', ''),
                    'flag_evidence_summary': ' ; '.join(evidence_summary),
                    'step4_related_bases': ' | '.join(row.get('step4_related_bases', [])),
                })

    return {
        'report_rows': len(final_rows),
        'csv_path': csv_path if 'csv' in formats else None,
        'jsonl_path': jsonl_path if 'jsonl' in formats else None,
    }
