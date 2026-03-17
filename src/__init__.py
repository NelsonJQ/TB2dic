"""TB2dic modular package API."""

from .tb2dic import run_pipeline_batch, run_pipeline_single
from .batchfiltering import batch_filter_tokens_by_dictionary, resolve_hunspell_paths
from .findincorpus import find_corpus_wordforms, load_i18n_corpus
from .munching import munch_to_compressed_dic
from .provenance import build_consolidated_provenance_report
from .prepro import load_and_tokenize_terminology_base, normalize_language_code
from .utils import erase_intermediary_and_output_dirs, prewarm_all_available_i18n

__all__ = [
    "run_pipeline_batch",
    "run_pipeline_single",
    "batch_filter_tokens_by_dictionary",
    "resolve_hunspell_paths",
    "find_corpus_wordforms",
    "load_i18n_corpus",
    "munch_to_compressed_dic",
    "build_consolidated_provenance_report",
    "load_and_tokenize_terminology_base",
    "normalize_language_code",
    "erase_intermediary_and_output_dirs",
    "prewarm_all_available_i18n",
]
