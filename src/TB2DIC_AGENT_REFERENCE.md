# TB2dic Agent Reference

This document is the agent-facing reference for the modular `src` pipeline.

## Canonical Scope
- Active implementation is the `src` package.
- Notebook files are not the source of truth for behavior changes.

## Main Entry Points
- `run_pipeline_batch` in `src/tb2dic.py`: runs Steps 1-5 for one or many `(game, language)` pairs.
- `run_pipeline_single` in `src/tb2dic.py`: convenience wrapper for a single pair.
- Package exports are available in `src/__init__.py`.

## Pipeline Stages
1. Step 1 Tokenization (`src/prepro.py`):
- Load TB source, detect language column, normalize/tokenize terms.
- Build optional proper-noun sidecar used later for ghost generation.

2. Steps 2-3 Dictionary Filtering (`src/batchfiltering.py`, `src/filtering.py`):
- Resolve Hunspell `.dic/.aff` pair for target language.
- Expand and validate dictionary forms with affix rules.
- Remove known dictionary words from token list.

3. Step 4 Corpus Matching (`src/findincorpus.py`):
- Load/filter i18n corpus.
- Generate/validate candidate forms against corpus evidence.
- Emit `*_missing_wordforms.csv` for munch stage.

4. Step 5 Munch/Assembly (`src/munching.py`):
- Build flat and compressed dictionary outputs.
- Preserve mandatory flags and casing decisions.
- Export final dic/aff artifacts.

## Key Policies To Preserve
- Casing policy: only infer lowercase insertion from exact lowercase corpus evidence.
- Hunspell fallback: keep robust language-specific fallback path resolution, especially FR variants.
- Strict mode in orchestrator: fail fast, deterministic processing order.

## Concurrency Model
- `workers`: inner parallelism for Step 4 corpus matching.
- `pair_workers`: outer parallelism for `(game, language)` pair execution.
- When `strict_mode=True`, pair parallelism is forced to 1 for deterministic fail-fast behavior.

## Expected Result Schema
`run_pipeline_batch` returns:
- `runs`: list of per-pair dictionaries with `status`, `timings`, `metrics`, `artifacts`, and error metadata.
- `summary`: aggregate totals (`processed_pairs`, `ok_pairs`, `failed_pairs`, elapsed seconds).

## Debugging Checklist
- Verify dictionary preflight (`dic` + `aff`) resolves for each language.
- Confirm intermediary files exist after each stage (`tokens`, `filtered dic`, `wordforms csv`).
- Check `metrics.i18n_status` for prewarm state and failures.
- Run `strict_mode=True` + low `sample` for deterministic minimal repro.
- Use test scripts under `testings/` for smoke, parity, and benchmark validation.
