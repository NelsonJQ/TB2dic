import os
import glob
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .filtering import filter_tokens_by_dictionary_with_affixes
from .params import DIC_FOLDER, HUNSPELL_PATHS, OUTPUT_DIR
from .prepro import normalize_language_code

def _build_hunspell_dic_candidates(lang_prefix: str, dic_folder: str = "dics") -> List[str]:
    """Return ordered candidate .dic paths for a normalized language code."""
    candidates: List[str] = []

    configured = HUNSPELL_PATHS.get(lang_prefix, "")
    if configured:
        candidates.append(configured)

        # Retarget paths rooted at DIC_FOLDER when dic_folder is overridden.
        norm_cfg = os.path.normpath(configured)
        cfg_parts = norm_cfg.split(os.sep)
        dic_root = os.path.normpath(DIC_FOLDER)
        if cfg_parts and cfg_parts[0].lower() == dic_root.lower():
            candidates.append(os.path.join(dic_folder, *cfg_parts[1:]))

    # Fallback candidates for common naming/layout variants.
    fallback_map = {
        "fr": [
            os.path.join(dic_folder, "fr_dic", "fr_FR.dic"),
            os.path.join(dic_folder, "fr_dic", "fr.dic"),
        ],
        "en": [
            os.path.join(dic_folder, "en_dic", "en_GB.dic"),
            os.path.join(dic_folder, "en_dic", "en_US.dic"),
            os.path.join(dic_folder, "en_dic", "en.dic"),
        ],
        "es": [
            os.path.join(dic_folder, "es_dic", "es", "es_ES.dic"),
            os.path.join(dic_folder, "es_dic", "es_ES.dic"),
            os.path.join(dic_folder, "es_dic", "es.dic"),
        ],
        "pt": [
            os.path.join(dic_folder, "pt_dic", "pt_BR", "pt_BR.dic"),
            os.path.join(dic_folder, "pt_dic", "pt_PT", "pt_PT.dic"),
            os.path.join(dic_folder, "pt_dic", "pt.dic"),
        ],
        "de": [
            os.path.join(dic_folder, "de_dic", "de_DE_frami.dic"),
            os.path.join(dic_folder, "de_dic", "de_DE.dic"),
            os.path.join(dic_folder, "de_dic", "de.dic"),
        ],
    }
    candidates.extend(fallback_map.get(lang_prefix, []))

    # De-duplicate while preserving order.
    deduped: List[str] = []
    seen = set()
    for p in candidates:
        key = os.path.normcase(os.path.normpath(p))
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    return deduped


def resolve_hunspell_paths(lang_code: str, dic_folder: str = "dics") -> Dict[str, Any]:
    """Resolve the first existing (.dic, .aff) pair for a language."""
    lang_prefix = normalize_language_code(lang_code)
    checks: List[Dict[str, Any]] = []

    for dic_path in _build_hunspell_dic_candidates(lang_prefix, dic_folder=dic_folder):
        aff_path = os.path.splitext(dic_path)[0] + ".aff"
        dic_exists = os.path.exists(dic_path)
        aff_exists = os.path.exists(aff_path)
        checks.append({
            "dic": dic_path,
            "aff": aff_path,
            "dic_exists": dic_exists,
            "aff_exists": aff_exists,
        })
        if dic_exists and aff_exists:
            return {
                "language": lang_prefix,
                "ok": True,
                "dic": dic_path,
                "aff": aff_path,
                "checks": checks,
            }

    return {
        "language": lang_prefix,
        "ok": False,
        "dic": "",
        "aff": "",
        "checks": checks,
    }


def batch_filter_tokens_by_dictionary(input_folder: str, target_languages: List[str], 
                                     dic_folder: str = "dics", 
                                     output_folder: Optional[str] = None) -> List[Dict]:
    """
    Batch process all token files in a folder using dictionary filtering with affix rules.
    
    Processes token files for each target language, filters them against Hunspell dictionaries
    (using affix rules for morphological matching), and saves results to dic format.
    
    Args:
        input_folder: Folder containing token files to filter
        target_languages: List of language codes to process (e.g., ['es', 'pt', 'en'])
        dic_folder: Folder containing dictionary files (default: "dics")
        output_folder: Folder to save filtered results. If None, uses OUTPUT_DIR from hyperparameters
        
    Returns:
        List[Dict]: Processing summary for each file, containing:
            - language: Language code
            - input_file: Input filename
            - output_file: Output filename
            - original_tokens: Count of input tokens
            - removed_tokens: Count of tokens matching dictionary
            - remaining_tokens: Count of neologisms (new tokens)
            - processing_time: Seconds to process
            - removal_rate: Percentage of tokens removed
    """
    
    # Use hyperparameter if output_folder not specified
    if output_folder is None:
        output_folder = OUTPUT_DIR
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Track processing statistics
    total_processed = 0
    total_errors = 0
    total_skipped = 0
    processing_summary: List[Dict] = []
    
    print("="*80)
    print("BATCH DICTIONARY FILTERING WITH AFFIX RULES")
    print("="*80)
    print(f"Input folder: {input_folder}")
    print(f"Target languages: {target_languages}")
    print(f"Dictionary folder: {dic_folder}")
    print(f"Output folder: {output_folder}")
    print("="*80)
    
    # Process each target language
    for lang_code in target_languages:
        # Normalize language code to 2 letters
        try:
            lang_prefix = normalize_language_code(lang_code)
        except ValueError as e:
            print(f"❌ Invalid language code '{lang_code}': {e}")
            total_errors += 1
            continue
        
        print(f"\n🌐 Processing language: {lang_code} (normalized: {lang_prefix})")
        print("-" * 50)
        
        # Resolve dictionary/affix paths with fallback candidates.
        resolved_paths = resolve_hunspell_paths(lang_prefix, dic_folder=dic_folder)
        if not resolved_paths["ok"]:
            print(f"❌ No valid dictionary+affix pair found for language '{lang_code}'")
            for chk in resolved_paths["checks"]:
                print(
                    f"   - DIC {'OK' if chk['dic_exists'] else 'MISSING'} | "
                    f"AFF {'OK' if chk['aff_exists'] else 'MISSING'} :: {chk['dic']}"
                )
            total_errors += 1
            continue

        dic_file_path = resolved_paths["dic"]
        aff_file_path = resolved_paths["aff"]
        
        print(f"✅ Dictionary files found:")
        print(f"   DIC: {dic_file_path}")
        print(f"   AFF: {aff_file_path}")
        
        # Find all token files for this language
        # Pattern: *{lang_code}*tokens*.txt or *{lang_prefix}*tokens*.txt
        token_patterns = [
            os.path.join(input_folder, f"*{lang_code}*tokens*.txt"),
            os.path.join(input_folder, f"*{lang_prefix}*tokens*.txt")
        ]
        token_files = []
        for pattern in token_patterns:
            token_files.extend(glob.glob(pattern))
        
        # Remove duplicates while preserving order
        token_files = list(dict.fromkeys(token_files))
        
        if not token_files:
            print(f"⏭️  No token files found for pattern: {token_patterns}")
            total_skipped += 1
            continue
            
        print(f"📁 Found {len(token_files)} token file(s) for {lang_prefix}:")
        
        # Process each token file for this language
        for token_file in token_files:
            token_filename = os.path.basename(token_file)
            print(f"\n  📄 Processing: {token_filename}")
            
            try:
                # Generate output filename by replacing 'tokens' with 'filtered_tokens'
                if 'tokens' in token_filename:
                    filtered_filename = token_filename.replace('tokens', 'filtered_tokens')
                    filtered_filename = filtered_filename.replace('.txt', '.dic')
                else:
                    base_name = Path(token_filename).stem
                    filtered_filename = f"{base_name}_filtered_tokens.dic"
                
                output_path = os.path.join(output_folder, filtered_filename)
                audit_filename = filtered_filename.replace('.dic', '_filter_audit.csv')
                audit_path = os.path.join(output_folder, audit_filename)
                
                # Check if output already exists
                if os.path.exists(output_path):
                    print(f"  ⏭️  Output already exists: {filtered_filename} - skipping")
                    total_skipped += 1
                    continue
                
                # Perform filtering
                start_time = time.time()
                result = filter_tokens_by_dictionary_with_affixes(
                    token_file,      # Input token file
                    dic_file_path,   # Dictionary file
                    aff_file_path,   # Affix file
                    output_path,     # Output file
                    audit_csv_path=audit_path,
                )
                end_time = time.time()
                
                # Calculate statistics
                processing_time = end_time - start_time
                removal_rate = (result['removed_tokens'] / result['original_txt_tokens'] * 100) if result['original_txt_tokens'] > 0 else 0
                
                print(f"  ✅ Successfully processed in {processing_time:.2f}s:")
                print(f"     Original tokens: {result['original_txt_tokens']:,}")
                print(f"     Removed tokens: {result['removed_tokens']:,} ({removal_rate:.1f}%)")
                print(f"     Remaining tokens: {result['remaining_tokens']:,}")
                print(f"     Output: {filtered_filename}")
                
                # Store summary for final report
                processing_summary.append({
                    'language': lang_code,
                    'input_file': token_filename,
                    'output_file': filtered_filename,
                    'original_tokens': result['original_txt_tokens'],
                    'removed_tokens': result['removed_tokens'],
                    'remaining_tokens': result['remaining_tokens'],
                    'processing_time': processing_time,
                    'removal_rate': removal_rate,
                    'filter_audit_csv': audit_path,
                })
                
                total_processed += 1
                
            except Exception as e:
                print(f"  ❌ Error processing {token_filename}: {e}")
                total_errors += 1
    
    # Print final summary
    print("\n" + "="*80)
    print("📊 BATCH PROCESSING SUMMARY")
    print("="*80)
    print(f"Total files processed: {total_processed}")
    print(f"Total errors: {total_errors}")
    print(f"Total skipped: {total_skipped}")
    
    if processing_summary:
        print(f"\n📈 DETAILED RESULTS:")
        print("-" * 80)
        
        # Group by language for better organization
        by_language: Dict[str, List[Dict]] = {}
        for item in processing_summary:
            lang = item['language']
            if lang not in by_language:
                by_language[lang] = []
            by_language[lang].append(item)
        
        total_original = sum(item['original_tokens'] for item in processing_summary)
        total_removed = sum(item['removed_tokens'] for item in processing_summary)
        total_remaining = sum(item['remaining_tokens'] for item in processing_summary)
        total_time = sum(item['processing_time'] for item in processing_summary)
        
        for lang, items in by_language.items():
            print(f"\n🌐 {lang.upper()}:")
            for item in items:
                print(f"  📄 {item['input_file']}")
                print(f"     → {item['remaining_tokens']:,} tokens ({item['removal_rate']:.1f}% removed)")
        
        print(f"\n📊 OVERALL STATISTICS:")
        print(f"   Total original tokens: {total_original:,}")
        print(f"   Total removed tokens: {total_removed:,}")
        print(f"   Total remaining tokens: {total_remaining:,}")
        if total_original > 0:
            print(f"   Overall removal rate: {(total_removed/total_original*100):.1f}%")
        print(f"   Total processing time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
        
        if total_processed > 0:
            print(f"   Average processing time: {total_time/total_processed:.2f}s per file")
    
    print(f"\n🎯 Next steps:")
    print(f"   - Check filtered files in: {output_folder}/")
    print(f"   - Review remaining tokens for quality")
    print(f"   - Use filtered tokens for translation validation or dictionary assembly")
    
    return processing_summary


# ==============================================================================
# USAGE EXAMPLE - Configure with hyperparameters
# ==============================================================================

# Use INTERMEDIARY_DIR for testing before final assembly
# Use OUTPUT_DIR for production dictionary outputs

# Recommended workflow:
# 1. First run: batch_filter_tokens_by_dictionary(..., output_folder=INTERMEDIARY_DIR)
# 2. Review results and validate quality
# 3. Final run: batch_filter_tokens_by_dictionary(..., output_folder=OUTPUT_DIR)

# Example (uncomment to run):
# TARGET_LANGUAGES = ["es", "pt", "en", "fr", "de"]
# INPUT_FOLDER = os.path.join(INTERMEDIARY_DIR, "raw_tokens")
# 
# batch_results = batch_filter_tokens_by_dictionary(
#     input_folder=INPUT_FOLDER,
#     target_languages=TARGET_LANGUAGES,
#     dic_folder=DIC_FOLDER,
#     output_folder=INTERMEDIARY_DIR  # or OUTPUT_DIR for final output
# )
