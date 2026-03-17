
import glob
import html
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from .params import (
    INTERMEDIARY_DIR,
    PROPER_NOUN_KEY_PATTERNS,
    _COMPILED_PROPER_NOUN_PATTERNS,
    basic_punct,
    i18n_PATHS,
    unicode_dashes,
)

# ==============================================================================
# LANGUAGE UTILITIES
# ==============================================================================

def normalize_language_code(code: str) -> str:
    """
    Normalize language codes to 2-letter lowercase format.
    
    Converts 'es-es', 'ES-ES', 'es_ES' -> 'es'; 'en-gb' -> 'en', etc.
    
    Args:
        code: Language code in any format (e.g., 'es-es', 'pt-br', 'en')
        
    Returns:
        str: 2-letter lowercase language code (e.g., 'es', 'pt', 'en')
        
    Raises:
        ValueError: If code cannot be normalized to a 2-letter code
    """
    if not code or not isinstance(code, str):
        raise ValueError(f"Invalid language code: {code}")
    
    # Normalize: lowercase, remove hyphens, underscores; take first 2 chars
    normalized = code.lower().replace('-', '').replace('_', '')
    if len(normalized) < 2:
        raise ValueError(f"Language code too short: {code}")
    
    return normalized[:2]


# ==============================================================================
# PRE-PROCESSING FUNCTIONS
# ==============================================================================

def remove_html_tags(text: str) -> str:
    """Remove HTML tags and decode HTML entities, with space insertion for br/p tags"""
    if not text:
        return text
    
    # First, replace br and p tags with spaces to prevent word concatenation
    # Handle both self-closing and regular br tags
    text = re.sub(r'&lt;/?br\s*/?&gt;', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'&lt;/?p\s*/?&gt;', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'&lt;p\s+[^&]*&gt;', ' ', text, flags=re.IGNORECASE)  # p with attributes
    text = re.sub(r'&lt;/p&gt;', ' ', text, flags=re.IGNORECASE)
    
    # Remove other HTML tags (without space insertion)
    text = re.sub(r'&lt;[^&]*&gt;', '', text)
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def process_english_contractions(text: str) -> str:
    """Process English contractions while preserving case"""
    if not text:
        return text
    
    # Comprehensive English contractions mapping
    contractions = {
        "ain't": "am not", "aren't": "are not", "can't": "cannot", "could've": "could have",
        "couldn't": "could not", "didn't": "did not", "doesn't": "does not", "don't": "do not",
        "hadn't": "had not", "hasn't": "has not", "haven't": "have not", "he'd": "he would",
        "he'll": "he will", "he's": "he is", "i'd": "i would", "i'll": "i will", "i'm": "i am",
        "i've": "i have", "isn't": "is not", "it'd": "it would", "it'll": "it will", "it's": "it is",
        "let's": "let us", "mustn't": "must not", "shan't": "shall not", "she'd": "she would",
        "she'll": "she will", "she's": "she is", "shouldn't": "should not", "that's": "that is",
        "there's": "there is", "they'd": "they would", "they'll": "they will", "they're": "they are",
        "they've": "they have", "we'd": "we would", "we're": "we are", "we've": "we have",
        "weren't": "were not", "what's": "what is", "where's": "where is", "who's": "who is",
        "won't": "will not", "wouldn't": "would not", "you'd": "you would", "you'll": "you will",
        "you're": "you are", "you've": "you have", "'cause": "because", "how's": "how is",
        "when's": "when is", "why's": "why is", "y'all": "you all", "would've": "would have",
        "should've": "should have", "might've": "might have", "must've": "must have"
    }
    
    def replace_contraction(match):
        contraction = match.group(0)
        lower_contraction = contraction.lower()
        
        if lower_contraction in contractions:
            replacement = contractions[lower_contraction]
            
            # Preserve case: if original was capitalized, capitalize the replacement
            if contraction[0].isupper():
                replacement = replacement.capitalize()
            
            return replacement
        return contraction
    
    # Use word boundaries to match contractions
    pattern = r"\b(?:" + "|".join(re.escape(cont) for cont in contractions.keys()) + r")\b"
    result = re.sub(pattern, replace_contraction, text, flags=re.IGNORECASE)
    
    return result

def process_portuguese_contractions(text: str) -> str:
    """Process Portuguese contractions and apostrophe patterns"""
    if not text:
        return text
    
    # Handle apostrophe contractions like d'Água -> de Água
    text = re.sub(r"\bd'([A-ZÁÉÍÓÚÂÊÔÀÇ])", r"de \1", text)
    text = re.sub(r"\bl'([A-ZÁÉÍÓÚÂÊÔÀÇ])", r"le \1", text)
    
    # Handle hyphenated pronouns like amá-lo -> amar lo
    text = re.sub(r"([aeiouáéíóúâêôàç])-([lm][eoasá]s?)\b", r"\1r \2", text)
    
    return text

def process_french_elisions(text: str) -> str:
    """Strip common French elided clitic prefixes before tokenization."""
    if not text:
        return text

    # Keep lexical apostrophes like A'geuse, but normalize l', s', qu' prefixes.
    return re.sub(
    r"\b(?:[cdjlmnst]|qu)['’](?=[AEIOUYÀÂÄÆÉÈÊËÎÏÔÖÙÛÜŒHaeiouyàâäæéèêëîïôöùûüœh])",
    "",
    text,
    flags=re.IGNORECASE
)

def has_wip_markers(text: str) -> bool:
    """Check if text contains WIP/translation markers"""
    if not text:
        return False
    if "[!]" in text:
        return True
    # Pattern to match markers like {WIP}, [NOTRAD], [no trad], {no_trad}, etc.
    pattern = r'[\[\{].*(wip|notrad|no trad|no_trad|no-trad).*[\]\}]'
    return bool(re.search(pattern, text, re.IGNORECASE))

def demorph_string(input_string: str) -> str:
    """
    Expand morphological patterns in localization strings.
    
    Supports two pattern types:
    1. Tilde patterns: {~X...} where X is a letter and ... is suffix
    2. Square bracket patterns: {[N*]?option1:option2} where N is a digit
    
    Args:
        input_string (str): String containing morphological patterns
        
    Returns:
        str: String with all variations joined by spaces
    """
    
    def extract_tilde_patterns(text: str) -> List[tuple]:
        """Extract all tilde morphological patterns from a word."""
        pattern_regex = r'\{~([^}]+)\}'
        matches = re.findall(pattern_regex, text)
        parsed_patterns = []
        for match in matches:
            # Split by ~ to handle multiple patterns in the same braces
            sub_patterns = match.split('~')
            for sub_pattern in sub_patterns:
                if len(sub_pattern) >= 1:
                    letter = sub_pattern[0]
                    suffix = sub_pattern[1:] if len(sub_pattern) > 1 else ""
                    parsed_patterns.append((letter, suffix))
        return parsed_patterns
    
    def extract_bracket_patterns(text: str) -> List[tuple]:
        """Extract all bracket patterns from a word."""
        # Pattern: {[N*]?option1:option2} — N may carry a comparison operator:
        #   {[>1]?a:o}   {[<1]?s:}   {[=1]?:s}   {[1*]?...}   {[~1]?...}
        pattern_regex = r'\{\[([~]?[<>=]?\d+\*?)\]\?([^:}]*):([^}]*)\}'
        matches = re.findall(pattern_regex, text)
        return matches
    
    def generate_tilde_variations(base_word: str, patterns: List[tuple]) -> List[str]:
        """Generate variations for tilde patterns."""
        # Remove patterns from base word to get the root
        root = re.sub(r'\{~[^}]+\}', '', base_word)
        
        # Check if root should be excluded (if 's' or 'm' patterns present)
        pattern_letters = [p[0] for p in patterns]
        exclude_root = 's' in pattern_letters or 'm' in pattern_letters
        
        # If no patterns, return the original word
        if not patterns:
            return [base_word]
        
        variations = []
        
        # Group patterns by type
        gender_patterns = [(letter, suffix) for letter, suffix in patterns if letter in 'mf']
        number_patterns = [(letter, suffix) for letter, suffix in patterns if letter in 'sp']
        
        # Handle gender+number combinations
        if gender_patterns and number_patterns:
            # We need all 4 combinations: masc sing, fem sing, masc plural, fem plural
            
            # 1. Masculine singular (root) - only if not excluded
            if not exclude_root:
                variations.append(root)

            # 2. Masculine singular with masculine suffix
            for g_letter, g_suffix in gender_patterns:
                if g_letter == 'm':
                    male_root = root + g_suffix
                    variations.append(male_root)

            # 3. Feminine singular (root + feminine suffix)
            for g_letter, g_suffix in gender_patterns:
                if g_letter == 'f':
                    variations.append(root + g_suffix)
            
            # 4. Masculine plural (root + plural suffix)  
            for n_letter, n_suffix in number_patterns:
                if n_letter == 'p':
                    variations.append(root + n_suffix)
            
            # 5. Feminine plural (root + feminine suffix + plural suffix)
            for (g_letter, g_suffix), (n_letter, n_suffix) in product(gender_patterns, number_patterns):
                if g_letter == 'f' and n_letter == 'p':
                    variations.append(root + g_suffix + n_suffix)
                    
        else:
            # Handle simple cases (no combinations needed)
            
            # If root should be included, add it first
            if not exclude_root:
                variations.append(root)
            
            # Add individual pattern variations
            for letter, suffix in patterns:
                variation = root + suffix
                variations.append(variation)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variations = []
        for var in variations:
            if var not in seen:
                seen.add(var)
                unique_variations.append(var)
        
        return unique_variations
    
    def generate_bracket_variations(base_word: str, bracket_patterns: List[tuple]) -> List[str]:
        """Generate variations for bracket patterns."""
        if not bracket_patterns:
            return [base_word]
        
        current_variations = [base_word]
        
        for pattern_match, option1, option2 in bracket_patterns:
            new_variations = []
            
            # Build the regex pattern correctly
            pattern_to_replace = r'\{\['  # {[
            pattern_to_replace += re.escape(pattern_match)  # pattern (escaped)
            pattern_to_replace += r'\]\?'  # ]?
            pattern_to_replace += re.escape(option1)  # option1 (escaped)
            pattern_to_replace += ':'  # :
            pattern_to_replace += re.escape(option2)  # option2 (escaped)
            pattern_to_replace += r'\}'  # }
            
            for current_var in current_variations:
                # For the pattern {[N*]?option1:option2}:
                # Generate variation 1: condition true -> use option1 (usually the base/unmarked form)
                # Use lambda replacement to avoid re.sub treating backslashes in option1/option2
                # as regex escape sequences (e.g. trailing \ in WAKFU strings causes "bad escape")
                var1 = re.sub(pattern_to_replace, lambda m, r=option1: r, current_var, count=1)
                if var1 not in new_variations:
                    new_variations.append(var1)
                
                # Generate variation 2: condition false -> use option2 (usually the marked form)
                var2 = re.sub(pattern_to_replace, lambda m, r=option2: r, current_var, count=1)
                if var2 not in new_variations:
                    new_variations.append(var2)
            
            current_variations = new_variations
        

        return current_variations
        
    # Find all words with patterns (both types); \S* at both ends to capture
    # any prefix/suffix chars like the trailing 's' in Ocult{[>1]?a:o}s
    word_pattern_regex = r'\S*\{[~\[][^}]+\}(?:\{[~\[][^}]+\})*\S*'
    
    def replace_word_patterns(match) -> str:
        word_with_patterns = match.group(0)
        
        # Check what type of patterns we have
        bracket_patterns = extract_bracket_patterns(word_with_patterns)
        tilde_patterns = extract_tilde_patterns(word_with_patterns)
        
        if bracket_patterns and not tilde_patterns:
            # Only bracket patterns
            variations = generate_bracket_variations(word_with_patterns, bracket_patterns)
        elif tilde_patterns and not bracket_patterns:
            # Only tilde patterns
            variations = generate_tilde_variations(word_with_patterns, tilde_patterns)
        elif bracket_patterns and tilde_patterns:
            # Both types - handle bracket first, then tilde
            bracket_variations = generate_bracket_variations(word_with_patterns, bracket_patterns)
            final_variations = []
            for var in bracket_variations:
                if extract_tilde_patterns(var):
                    tilde_vars = generate_tilde_variations(var, extract_tilde_patterns(var))
                    final_variations.extend(tilde_vars)
                else:
                    final_variations.append(var)
            variations = final_variations
        else:
            # No patterns found (shouldn't happen with our regex)
            variations = [word_with_patterns]
        
        return ' '.join(variations)
    
    # Replace all pattern words with their variations
    result = re.sub(word_pattern_regex, replace_word_patterns, input_string)
    
    return result

def tokenize_text(text: str, language: str = "default") -> Set[str]:
    """
    Enhanced tokenize function with language-specific processing and comprehensive filtering
    
    Args:
        text: Input text to tokenize
        language: Language for processing ("english", "portuguese", "french", or "default")
    
    Returns:
        Set[str]: Set of filtered tokens (deduplicated, no particular order)
    """
    if not text or not isinstance(text, str):
        return set()
    
    # Step 1: Remove HTML tags and decode entities
    text = remove_html_tags(text)

    # Step 1.25: Normalize escaped control sequences from i18n/properties sources
    # Example: "...\nUNA..." stored as literal backslash+n becomes "... nUNA ..."
    # and creates bad tokens like "nUNA". Convert to whitespace first.
    text = re.sub(r'\\+[nrt]', ' ', text)

    # Step 1.5: Expand morphological patterns if { or [ detected
    if '{' in text or '[' in text:
        text = demorph_string(text)
    
    # Step 2: Remove URLs and email addresses
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
    
    # Step 3: Language-specific contraction processing
    if language.lower() == "english":
        text = process_english_contractions(text)
    elif language.lower() == "portuguese":
        text = process_portuguese_contractions(text)
    elif language.lower() == "french":
        text = process_french_elisions(text)
    # For "default" or other languages, skip contraction processing
    
    # Step 4: Enhanced punctuation (including º character)
    punctuation = basic_punct + unicode_dashes
    
    # Step 5: Tokenize by whitespace and punctuation, preserving internal hyphens and apostrophes
    tokens = re.findall(r"[^\s" + re.escape(punctuation) + r"]+(?:[-'][^\s" + re.escape(punctuation) + r"]+)*", text)
    
    # Step 6: Clean and filter tokens
    filtered_tokens = set()
    for token in tokens:
        # Remove leading/trailing apostrophes and hyphens
        cleaned_token = token.strip("'-")

        # Remove English possessive 's (e.g., "Anear's" → "Anear")
        if language.lower() == "english":
            cleaned_token = re.sub(r"'[sS]$", "", cleaned_token)
        elif language.lower() == "french":
            cleaned_token = process_french_elisions(cleaned_token)
        
        # Skip if empty after cleaning
        if not cleaned_token:
            continue
        
        # Skip short tokens (< 3 characters)
        if len(cleaned_token) < 3:
            continue
        
        # Skip tokens that are chains of the same character
        if len(set(cleaned_token.lower())) == 1:
            continue
        
        # Skip tokens that are only digits
        if cleaned_token.isdigit():
            continue
        
        # Skip time patterns (e.g., "3PM", "10AM", "5PA", "12AL") - inlined for efficiency
        if re.match(r'^\d+(PM|AM|PA|AL|MP|AP|RW|BP|KT)$', cleaned_token, re.IGNORECASE):
            continue
        
        # Skip digit-word patterns (e.g., "123-neutral") - inlined for efficiency
        if re.match(r'^\d+-\w+$', cleaned_token):
            continue
        
        filtered_tokens.add(cleaned_token)
    
    return filtered_tokens

def detect_file_type(file_path: str) -> str:
    """
    Detect if file is Excel, CSV, or XLIFF based on extension.
    
    Args:
        file_path: Path to the file
        
    Returns:
        str: 'excel', 'csv', or 'xliff'
        
    Raises:
        ValueError: If file type is unsupported
    """
    file_path_lower = file_path.lower()
    if file_path_lower.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif file_path_lower.endswith('.csv'):
        return 'csv'
    elif file_path_lower.endswith(('.tsv', '.txt')):
        return 'tsv'
    elif file_path_lower.endswith(('.xliff', '.xlf', '.xml')):
        return 'xliff'
    else:
        raise ValueError(f"Unsupported file type for: {file_path}")

def load_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a tabular file into a DataFrame, auto-detecting format (Excel, CSV, TSV).
    
    Args:
        file_path: Path to the file (.xlsx, .xls, .csv, .tsv, .txt)
        
    Returns:
        pd.DataFrame: Loaded data
        
    Raises:
        ValueError: If the file cannot be read or is unsupported
    """
    file_type = detect_file_type(file_path)
    
    if file_type == 'excel':
        return pd.read_excel(file_path)
    elif file_type == 'csv':
        # utf-8-sig strips BOM (\ufeff) that Excel adds when saving UTF-8 CSVs
        try:
            return pd.read_csv(file_path, encoding='utf-8-sig', sep=None, engine='python')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin-1', sep=None, engine='python')
    elif file_type == 'tsv':
        try:
            return pd.read_csv(file_path, encoding='utf-8-sig', sep='\t')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin-1', sep='\t')
    else:
        raise ValueError(f"Cannot load unsupported file type: {file_path}")

def process_excel_file(file_path: str, language_code: str, ignore_identical_translation: bool, 
                      tokenize_language: str, skip_square_brackets: bool, skip_all_caps: bool, 
                      skip_wip_markers: bool) -> Tuple[Set[str], int, int]:
    """
    Process Excel/CSV file and extract tokens with configurable filtering.
    
    Args:
        file_path: Path to Excel or CSV file
        language_code: Target language code (e.g., 'es', 'en')
        ignore_identical_translation: Skip entries where source == target
        tokenize_language: Language for tokenization ("english", "portuguese", or "default")
        skip_square_brackets: Skip entries with [brackets] in source
        skip_all_caps: Skip entries with ALL_CAPS in target
        skip_wip_markers: Skip entries with WIP markers
        
    Returns:
        Tuple[Set[str], int, int]: (tokens_set, processed_count, skipped_count)
    """
    file_type = detect_file_type(file_path)
    
    if file_type == 'excel':
        # Try to find the sheet with actual data for the language
        xl_file = pd.ExcelFile(file_path)
        df = None
        sheet_used = None
        
        for sheet_name in xl_file.sheet_names:
            temp_df = pd.read_excel(file_path, sheet_name=sheet_name)
            if language_code in temp_df.columns:
                non_null_count = temp_df[language_code].notna().sum()
                if non_null_count > 0:
                    df = temp_df
                    sheet_used = sheet_name
                    print(f"Using sheet '{sheet_name}' with {non_null_count} {language_code} values")
                    break
        
        if df is None:
            df = pd.read_excel(file_path)
            sheet_used = "default"
    else:
        # CSV / TSV
        df = load_dataframe(file_path)
        sheet_used = "n/a"
    
    print(f"Columns: {list(df.columns)}")
    print(f"Sheet used: {sheet_used}")
    
    if language_code not in df.columns:
        raise ValueError(f"Language code '{language_code}' not found in columns: {list(df.columns)}")
    # Initialize tracking
    print(f"Total rows to process: {len(df)}")
    
    # Initialize tracking
    tokens = set()
    processed_count = 0
    skipped_count = 0
    skip_reasons = {"identical": 0, "square_brackets": 0, "all_caps": 0, "wip_markers": 0, "empty_target": 0}
    
    for index, row in df.iterrows():
        source_text = str(row.iloc[1]) if len(row) > 1 else ""  # Assume source is second column
        
        # Check if target is NaN or empty BEFORE converting to string
        target_value = row[language_code]
        if pd.isna(target_value):
            skipped_count += 1
            skip_reasons["empty_target"] += 1
            continue
            
        target_text = str(target_value)
        
        # Skip if target is empty string after conversion
        if target_text.strip() == '':
            skipped_count += 1
            skip_reasons["empty_target"] += 1
            continue
        
        # Apply filters based on configuration
        should_skip = False
        skip_reason = None
        
        # Filter 1: Identical translation
        if ignore_identical_translation and source_text == target_text:
            should_skip = True
            skip_reason = "identical"
        
        # Filter 2: Square brackets in source
        elif skip_square_brackets and re.search(r'\[.+\]', source_text):
            should_skip = True
            skip_reason = "square_brackets"
        
        # Filter 3: All caps target
        elif skip_all_caps and target_text.isupper() and len(target_text) > 2:
            should_skip = True
            skip_reason = "all_caps"
        
        # Filter 4: WIP markers
        elif skip_wip_markers and has_wip_markers(target_text):
            should_skip = True
            skip_reason = "wip_markers"
        
        if should_skip:
            skipped_count += 1
            if skip_reason is not None:
                skip_reasons[skip_reason] += 1
            continue
        
        # Process the target text
        processed_count += 1
        text_tokens = tokenize_text(target_text, tokenize_language)
        tokens.update(text_tokens)
    
    # Print skip statistics
    print(f"Skip reasons breakdown:")
    for reason, count in skip_reasons.items():
        if count > 0:
            print(f"  - {reason}: {count}")
    
    return tokens, processed_count, skipped_count

def process_xliff_file(file_path: str, language_code: str, ignore_identical_translation: bool,
                      tokenize_language: str, skip_square_brackets: bool, skip_all_caps: bool,
                      skip_wip_markers: bool) -> Tuple[Set[str], int, int]:
    """
    Process XLIFF file and extract tokens with configurable filtering.
    
    Args:
        file_path: Path to XLIFF file
        language_code: Target language code (e.g., 'es', 'en')
        ignore_identical_translation: Skip entries where source == target
        tokenize_language: Language for tokenization ("english", "portuguese", or "default")
        skip_square_brackets: Skip entries with [brackets] in source
        skip_all_caps: Skip entries with ALL_CAPS in target
        skip_wip_markers: Skip entries with WIP markers
    
    Returns:
        Tuple[Set[str], int, int]: (tokens_set, processed_count, skipped_count)
    """
    
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    # Find the namespace
    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag.split('}')[0] + '}'
    
    # Find file element and check language attributes
    file_elem = root.find(f'.//{namespace}file')
    if file_elem is None:
        raise ValueError("No file element found in XLIFF")
    
    source_lang = file_elem.get('source-language', '')
    target_lang = file_elem.get('target-language', '')
    
    print(f"XLIFF source language: {source_lang}")
    print(f"XLIFF target language: {target_lang}")
    
    # Determine if we should extract from source or target elements
    use_source = (language_code == source_lang)
    use_target = (language_code == target_lang)
    
    if not (use_source or use_target):
        raise ValueError(f"Language code '{language_code}' not found in XLIFF languages: {source_lang}, {target_lang}")
    
    # Find all trans-unit elements
    trans_units = root.findall(f'.//{namespace}trans-unit')
    print(f"Total XLIFF segments to process: {len(trans_units)}")
    
    # Initialize tracking
    tokens = set()
    processed_count = 0
    skipped_count = 0
    skip_reasons = {"identical": 0, "square_brackets": 0, "all_caps": 0, "wip_markers": 0}
    
    for trans_unit in trans_units:
        source_elem = trans_unit.find(f'{namespace}source')
        target_elem = trans_unit.find(f'{namespace}target')
        
        source_text = source_elem.text if source_elem is not None and source_elem.text else ""
        target_text = target_elem.text if target_elem is not None and target_elem.text else ""
        
        # Determine which text to process
        text_to_process = source_text if use_source else target_text
        
        # Skip if text is empty
        if not text_to_process:
            skipped_count += 1
            continue
        
        # Apply filters based on configuration
        should_skip = False
        skip_reason = None
        
        # Filter 1: Identical translation (only relevant for target)
        if ignore_identical_translation and use_target and source_text == target_text:
            should_skip = True
            skip_reason = "identical"
        
        # Filter 2: Square brackets in source
        elif skip_square_brackets and re.search(r'\[.+\]', source_text):
            should_skip = True
            skip_reason = "square_brackets"
        
        # Filter 3: All caps target (only relevant for target)
        elif skip_all_caps and use_target and target_text.isupper() and len(target_text) > 2:
            should_skip = True
            skip_reason = "all_caps"
        
        # Filter 4: WIP markers (consolidated - removed duplicate check)
        elif skip_wip_markers and has_wip_markers(target_text):
            should_skip = True
            skip_reason = "wip_markers"
        
        if should_skip:
            skipped_count += 1
            if skip_reason is not None:
                skip_reasons[skip_reason] += 1
            continue
        
        # Process the text
        processed_count += 1
        text_tokens = tokenize_text(text_to_process, tokenize_language)
        tokens.update(text_tokens)
    
    # Print skip statistics
    print(f"Skip reasons breakdown:")
    for reason, count in skip_reasons.items():
        if count > 0:
            print(f"  - {reason}: {count}")
    
    return tokens, processed_count, skipped_count

def export_tokens_to_txt(tokens: Set[str], output_path: str) -> None:
    """
    Export tokens to a text file, one per line, sorted alphabetically.
    
    Args:
        tokens: Set of tokens to export
        output_path: Path where to save the file
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        for token in sorted(tokens):
            f.write(token + '\n')
    print(f"Exported {len(tokens)} unique tokens to: {output_path}")

def create_sample_xliff() -> None:
    """Create a sample XLIFF file for testing"""
    sample_xliff_content = """<?xml version="1.0" encoding="UTF-8"?>
<xliff version="1.2" xmlns="urn:oasis:names:tc:xliff:document:1.2">
    <file datatype="plaintext" original="sample" source-language="fr-fr" target-language="es-es">
        <body>
            <trans-unit id="sample.1">
                <source>Votre alignement est probablement au sommet, vos ennemis n'existent plus à l'Apogée.</source>
                <target>Tu alineamiento está probablemente en la cumbre, tus enemigos no existen en el Apogeo.</target>
            </trans-unit>
            <trans-unit id="sample.2">
                <source>Test avec des crochets [DEBUG] dans le source</source>
                <target>Prueba con corchetes en el origen</target>
            </trans-unit>
        </body>
    </file>
</xliff>"""
    
    with open("sample.xliff", "w", encoding="utf-8") as f:
        f.write(sample_xliff_content)
    print("Sample XLIFF file created!")
    print("Sample XLIFF file created!")


# LOAD AND TOKENIZE TERMINOLOGY BASE
# ==============================================================================

# Regex that matches typical language-code column headers:
#   "es", "es-ES", "es_ES", "pt-BR", "fr-FR", "de-DE (info)", etc.
# Two lowercase letters, optionally followed by [-_] + two letters, optionally " (info)".
_LANG_COL_RE = re.compile(
    r'^[a-z]{2}(?:[-_][a-zA-Z]{2})?(?:\s+\(info\))?$',
    re.IGNORECASE
)

def _is_lang_col(col: str) -> bool:
    """Return True if the column name looks like a language-code column."""
    return bool(_LANG_COL_RE.match(col.strip().lstrip('\ufeff')))
    

# Cache: (game, lang, filtered_path, mtime, size) -> token evidence map
_ES_I18N_CASE_EVIDENCE_CACHE: Dict[Tuple[str, str, str, float, int], Dict[str, Dict[str, int]]] = {}
_ES_WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿÑñÜü][A-Za-zÀ-ÖØ-öø-ÿÑñÜü'’-]*")

def _is_sentence_start_boundary(text: str, pos: int) -> bool:
    """True when token at pos starts a sentence-like segment."""
    i = pos - 1
    while i >= 0 and text[i].isspace():
        i -= 1
    if i < 0:
        return True
    return text[i] in '.!?:;'

def _resolve_filtered_i18n_path(game: str, lang: str) -> str:
    """Return filtered i18n path for game/lang or empty string."""
    path = (i18n_PATHS.get(game) or {}).get(lang, '')
    if not path or not os.path.exists(path):
        return ''
    base = os.path.basename(path)
    if base in (f"{game}_{lang}_i18n_filtered.json", f"{game}_{lang}_i18n_filtered.properties"):
        return path
    return ''

def _build_es_i18n_case_evidence(game: str, lang: str = 'es') -> Dict[str, Dict[str, int]]:
    """Build lowercase/uppercase-mid-sentence evidence from filtered i18n only."""
    if lang != 'es':
        return {}
    filtered_path = _resolve_filtered_i18n_path(game, lang)
    if not filtered_path:
        return {}
    try:
        _st = os.stat(filtered_path)
    except OSError:
        return {}
    _cache_key = (game, lang, filtered_path, float(_st.st_mtime), int(_st.st_size))
    if _cache_key in _ES_I18N_CASE_EVIDENCE_CACHE:
        return _ES_I18N_CASE_EVIDENCE_CACHE[_cache_key]

    evidence: Dict[str, Dict[str, int]] = {}

    def _scan_text(raw_text: str) -> None:
        clean = demorph_string(remove_html_tags(str(raw_text)))
        for m in _ES_WORD_RE.finditer(clean):
            tok_raw = m.group(0).strip("'’-")
            if not tok_raw:
                continue
            tok_lower = tok_raw.lower()
            rec = evidence.setdefault(tok_lower, {
                'lowercase_count': 0,
                'uppercase_mid_sentence_count': 0,
            })
            if tok_raw == tok_raw.lower():
                rec['lowercase_count'] += 1
            if tok_raw[:1].isupper() and not _is_sentence_start_boundary(clean, m.start()):
                rec['uppercase_mid_sentence_count'] += 1

    try:
        if filtered_path.endswith('.json'):
            with open(filtered_path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            entries = data.get('entries', data)
            for raw_val in entries.values() if isinstance(entries, dict) else []:
                if isinstance(raw_val, str) and raw_val.strip():
                    _scan_text(raw_val)
        elif filtered_path.endswith('.properties'):
            with open(filtered_path, 'r', encoding='utf-8') as fh:
                for line_raw in fh:
                    line = line_raw.rstrip('\n')
                    if not line.strip() or line.lstrip().startswith('#') or '=' not in line:
                        continue
                    _, _, raw_val = line.partition('=')
                    if raw_val.strip():
                        _scan_text(raw_val)
    except Exception:
        return {}

    _ES_I18N_CASE_EVIDENCE_CACHE[_cache_key] = evidence
    return evidence
    

_ANK_PT_EXCEPTION = "[S'il s'agît d'un typo - Armand - son nom en portugais est ARMANDO]"
_ANK_WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿÑñÜü]+(?:[-'’][A-Za-zÀ-ÖØ-öø-ÿÑñÜü]+)*")

def _ank_title_case_words(text: str) -> str:
    """Title-case lexical words while preserving punctuation separators."""
    def _title_word(match: re.Match) -> str:
        word = match.group(0)
        return word[:1].upper() + word[1:].lower() if word else word
    return _ANK_WORD_RE.sub(_title_word, text)

def _preprocess_ankanimation_term(raw_value: Any, lang_prefix: str) -> str:
    """Normalize ANKANIMATION TB term comments/variants/casing."""
    if raw_value is None:
        return ''
    if isinstance(raw_value, float) and pd.isna(raw_value):
        return ''

    text = str(raw_value).strip()
    if not text:
        return ''

    # PT exception must remain untouched.
    if lang_prefix == 'pt' and text == _ANK_PT_EXCEPTION:
        # Keep only the intended Portuguese term, avoid noisy French sentence tokens.
        return 'Armando'

    # If term starts with a bracketed note, keep first lexical token from inside.
    stripped = text.lstrip()
    if stripped.startswith('['):
        m_first = re.match(r"^\s*\[([^\]]*)\]", text)
        if m_first:
            inner = m_first.group(1).strip()
            m_tok = _ANK_WORD_RE.search(inner)
            text = m_tok.group(0) if m_tok else ''

    # Remove all remaining [comments].
    text = re.sub(r"\[[^\]]*\]", " ", text)

    # Remove parenthetical variants/comments.
    text = re.sub(r"\([^)]*\)", " ", text)

    # Cleanup spacing and stray separator padding.
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*([,;:])\s*", r"\1 ", text).strip()
    text = text.rstrip(' ,;:')

    if not text:
        return ''

    # Normalize all-caps content to title case per lexical word.
    letters_only = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿÑñÜü]", "", text)
    if letters_only and letters_only.isupper():
        text = _ank_title_case_words(text)

    return text

def _preprocess_ankanimation_dataframe(df: pd.DataFrame, term_column: str, lang_prefix: str) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Apply ANKANIMATION text cleanup on a term column and report stats."""
    out_df = df.copy()
    before_series = out_df[term_column].astype(str)
    out_df[term_column] = out_df[term_column].apply(lambda v: _preprocess_ankanimation_term(v, lang_prefix))
    after_series = out_df[term_column].astype(str)

    stats = {
        'total_rows': int(len(out_df)),
        'changed_rows': int((before_series != after_series).sum()),
        'empty_after': int((after_series.str.strip() == '').sum()),
    }
    return out_df, stats

def _apply_ankanimation_token_overrides(tokens: Set[str], lang_prefix: str) -> Set[str]:
    """Apply ANKANIMATION token overrides before dictionary filtering/export."""
    out = {t for t in tokens if isinstance(t, str) and t}

    if lang_prefix == 'es':
        # 2) Force lowercase-only for this list (remove capitalized/other variants).
        force_lower_only = [
            "Brakmarianos", "Dragopavo", "Fab'huritus", "Núcrumos", "Selocosa", "Sadidas"
        ]
        for base in force_lower_only:
            base_cf = base.casefold()
            out = {tok for tok in out if tok.casefold() != base_cf}
            out.add(base.lower())

        # 2B) Keep capitalized and add lowercase.
        keep_both = ["Selacubo", "Selaculus", "Selasfera"]
        for base in keep_both:
            out.add(base)
            out.add(base.lower())

        # 3) Replace Mechasme -> Mecasma, and add mecasma/mecasmas if source had Mechasme.
        if any(tok.casefold() == 'mechasme' for tok in out):
            out = {tok for tok in out if tok.casefold() != 'mechasme'}
            out.update({'Mecasma', 'mecasma', 'mecasmas'})

    return out


def load_and_tokenize_terminology_base(excel_file_path: str, language_code: str,
                                      term_column: Optional[str] = None, tokenize_language: str = "default",
                                      output_file_path: Optional[str] = None,
                                      save_propernoun_sidecar: bool = True,
                                      game_tag: str = "",
                                      allow_language_fallback: bool = True) -> Tuple[List[str], str]:
    """
    Load terminology base from Excel/CSV/TSV, tokenize it, and optionally export to file.

    This unified function:
    1. Loads a terminology file (Excel, CSV, or TSV)
    2. Pre-filters rows: drops emote shortcut entries and entries flagged as morse
    3. Extracts terms from the specified language column
    4. For WAVEN-style files, also extracts plural forms from the matching "(info)" column
    5. Tokenizes each term using the custom tokenize_text() function
    6. Deduplicates and returns as a sorted list
    7. (Optional) Exports to a text file
    8. (Optional) Writes a proper-noun sidecar JSON for gender ghost generation

    Args:
        excel_file_path: Path to terminology file (.xlsx, .xls, .csv, .tsv)
        language_code: Language code (e.g., "es-es", "pt-br"). Auto-normalized to 2 letters
        term_column: Column name containing terms. If None, auto-detects based on language_code
        tokenize_language: Language for tokenization ("english", "portuguese", or "default")
        output_file_path: Path to save the tokenized list. If None, no export occurs
        save_propernoun_sidecar: If True, writes a JSON mapping key-types in
            PROPER_NOUN_KEY_PATTERNS to their token sets, for use by find_corpus_wordforms().
        game_tag: Game name tag (e.g. "DOFUS") used in the sidecar filename.
        allow_language_fallback: If True, fallback to last language column when
            exact language column is missing. If False, raises ValueError.

    Returns:
        Tuple[List[str], str]: (tokenized_list_sorted, output_file_path_or_empty)

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If language code or term column cannot be detected

    Example:
        >>> tokens, output_path = load_and_tokenize_terminology_base(
        ...     "terminology.csv",
        ...     "es-es",
        ...     tokenize_language="default",
        ...     output_file_path="terminology_tokens_es.txt"
        ... )
        >>> len(tokens)
        245
    """

    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"File not found: {excel_file_path}")
    # Load file (auto-detects Excel vs CSV/TSV)
    print(f"📂 Loading terminology from: {excel_file_path}")

    # Load file (auto-detects Excel vs CSV/TSV)
    try:
        df = load_dataframe(excel_file_path)
    except Exception as e:
        raise ValueError(f"Error reading file: {e}")

    print(f"📊 Shape: {df.shape} (rows, columns)")
    print(f"📋 Columns: {list(df.columns)}")

    # Normalized language code reused across auto-detection and ES-specific
    # item/monster first-token lowercasing logic.
    lang_prefix = normalize_language_code(language_code)
    _is_spanish = (lang_prefix == 'es')

    # Prewarm filtered i18n so ES default-lowercasing can use corpus evidence.
    if _is_spanish and game_tag:
        try:
            from .findincorpus import load_i18n_corpus

            load_i18n_corpus(lang_prefix, [game_tag], source_type='i18n', lang_detect=True)
        except Exception as _e:
            print(f"⚠️  [es-first-lower] i18n prewarm skipped for {game_tag}/{lang_prefix}: {_e}")

    def _extract_first_token_lower(text: str) -> str:
        """Return first lexical token lowercased, preserving accents/apostrophes."""
        m = re.search(r"[A-Za-zÀ-ÖØ-öø-ÿÑñÜü][A-Za-zÀ-ÖØ-öø-ÿÑñÜü'’-]*", text)
        if not m:
            return ''
        tok = m.group(0).strip("'’-")
        return tok.lower() if tok else ''

    # ── Identify key/note column ───────────────────────────────────────────────
    # Priority 1: an explicit key/definition/id/note column name.
    # Priority 2: the first column that does NOT look like a language column.
    # Language columns are NEVER used as the key column so we never corrupt them.
    _KEY_COL_NAMES = {'key', 'definition', 'id', 'note'}
    _named_key = next(
        (col for col in df.columns if col.strip().lower().lstrip('\ufeff') in _KEY_COL_NAMES),
        None
    )
    if _named_key:
        key_col = _named_key
    else:
        # Fall back to the first column that is not a language-code column
        _non_lang_cols = [col for col in df.columns if not _is_lang_col(col)]
        key_col = _non_lang_cols[0] if _non_lang_cols else None

    if key_col:
        print(f"🔑 Key column detected: '{key_col}'")
    else:
        print("⚠️  No key column found — emote/morse pre-filters will be skipped")

    # ── Pre-filter: drop emote shortcut rows ──────────────────────────────────
    if key_col:
        _before = len(df)
        df = df[~df[key_col].astype(str).str.match(r'emoticon\.\d+\.shortcut', na=False)].reset_index(drop=True)
        _dropped = _before - len(df)
        if _dropped:
            print(f"🚫 Dropped {_dropped:,} emote shortcut rows")

    # ── Pre-filter: drop morse rows (key/note contains \bmorse\b) ─────────────
    if key_col:
        _before = len(df)
        df = df[~df[key_col].astype(str).str.contains(r'\bmorse\b', flags=re.IGNORECASE, na=False)].reset_index(drop=True)
        _dropped = _before - len(df)
        if _dropped:
            print(f"🚫 Dropped {_dropped:,} morse rows")

    # ── Auto-detect term column if not provided ───────────────────────────────
    if term_column is None:
        # Normalize to 2-letter prefix so "es" matches "es-es", "es-ES", "es_ES", etc.
        common_names = [language_code, language_code.upper(), language_code.replace('-', '_'),
                       'Term', 'Terminology', 'Translation', 'Target']

        def _col_matches(col: str) -> bool:
            col_l = col.lower().lstrip('\ufeff')  # also strip any residual BOM
            # Exact match against common names
            if col_l in [n.lower() for n in common_names]:
                return True
            # "es" matches "es-es", "es_es", "es-ES" ...
            if col_l == lang_prefix or col_l.startswith(lang_prefix + '-') or col_l.startswith(lang_prefix + '_'):
                return True
            return False
        matched_cols = [col for col in df.columns if _col_matches(col)]

        if matched_cols:
            term_column = matched_cols[0]
            print(f"🔍 Auto-detected term column: '{term_column}'")
        else:
            # Fall back to the last language-looking column (excluding (info) columns)
            lang_cols = [col for col in df.columns if _is_lang_col(col) and '(info)' not in col.lower()]
            if lang_cols and allow_language_fallback:
                term_column = lang_cols[-1]
                print(f"⚠️  No exact match found. Using last language column: '{term_column}'")
            else:
                raise ValueError(
                    f"Could not detect exact term column for language '{language_code}' (normalized='{lang_prefix}'). Available columns: {list(df.columns)}"
                )

    if term_column not in df.columns:
        raise ValueError(f"Column '{term_column}' not found. Available columns: {list(df.columns)}")

    # ── ANKANIMATION-specific term cleanup ────────────────────────────────
    if str(game_tag).strip().upper() == 'ANKANIMATION':
        df, _ank_stats = _preprocess_ankanimation_dataframe(df, term_column, lang_prefix)
        print(
            f"🧼 ANKANIMATION cleanup: changed={_ank_stats['changed_rows']:,}/"
            f"{_ank_stats['total_rows']:,} rows, empty_after={_ank_stats['empty_after']:,}"
        )

    # ── Detect optional plural-info column (WAVEN style) ─────────────────────
    # WAVEN files carry a "<lang> (info)" column with plural forms:
    #   One: "Crujidor Erosionado", Other: "Crujidores Erosionados"
    info_col = f"{term_column} (info)" if f"{term_column} (info)" in df.columns else None
    if info_col:
        print(f"ℹ️  Plural-info column detected: '{info_col}'")

    def _parse_plural_info(info_text) -> List[str]:
        """Parse WAVEN plural format: 'One: "X", Other: "Y"' → list of plain strings."""
        if pd.isna(info_text):
            return []
        return re.findall(r'(?:One|Other|Few|Many|Zero|Two):\s*"([^"]*)"', str(info_text))

    # ── Extract and tokenize terms ────────────────────────────────────────────
    print(f"\n🧹 Tokenizing {len(df):,} entries from column '{term_column}'...")

    all_tokens = set()
    skipped_empty = 0
    processed = 0
    info_series = df[info_col] if info_col is not None else None

    # ── Proper-noun tracking ──────────────────────────────────────────────────
    # propernoun_map: key_type -> set of lowercased tokens from that category
    propernoun_map: Dict[str, Set[str]] = {}
    _key_series = df[key_col].astype(str) if key_col else None

    # Spanish-only fast index for default lowercasing of item/monster first tokens.
    # We collect category token sets in one pass and resolve additions after the loop.
    es_npc_area_tokens_lower: Set[str] = set()
    es_item_monster_first_candidates: Set[str] = set()

    for idx, term in enumerate(df[term_column]):
        # Skip empty or null values
        if pd.isna(term) or not str(term).strip():
            skipped_empty += 1
            continue
        # Tokenize the term (applies all custom filters and morphological expansion)
        term_str = str(term).strip()

        # Tokenize the term (applies all custom filters and morphological expansion)
        tokens = tokenize_text(term_str, language=tokenize_language)

        if tokens:
            all_tokens.update(tokens)
            processed += 1

            # ── Track proper-noun tokens by key-type ──────────────────────
            if _key_series is not None:
                raw_key = _key_series.iloc[idx]
                # TB keys are comma-separated groups of dot-notation keys
                # e.g. "monster.123.name, NPC.456.name"
                # Each part is matched against PROPER_NOUN_KEY_PATTERNS using
                # re.search() so we can distinguish .name from .reply etc.
                matched_categories: Set[str] = set()
                for key_part in raw_key.split(','):
                    key_part_s = key_part.strip()
                    for category, compiled_pats in _COMPILED_PROPER_NOUN_PATTERNS.items():
                        if any(p.search(key_part_s) for p in compiled_pats):
                            matched_categories.add(category)
                            if save_propernoun_sidecar:
                                propernoun_map.setdefault(category, set()).update(
                                    tok.lower() for tok in tokens
                                )
                            break  # stop at first matching category for this key_part
                    # continue outer loop — each comma-part may be a diff category

                # Spanish convention: item/monster names default to lowercase
                # first token, except when that token collides with NPC/area
                # vocabulary (token-level match, including compound names).
                if _is_spanish and matched_categories:
                    first_tok = _extract_first_token_lower(term_str)
                    if first_tok:
                        if ('npc' in matched_categories) or ('area' in matched_categories):
                            es_npc_area_tokens_lower.update(tok.lower() for tok in tokens)
                            es_npc_area_tokens_lower.add(first_tok)
                        if ('item' in matched_categories) or ('monster' in matched_categories):
                            es_item_monster_first_candidates.add(first_tok)

        # Also tokenize plural-info column if present (WAVEN)
        if info_series is not None:
            for plural_form in _parse_plural_info(info_series.iloc[idx]):
                all_tokens.update(tokenize_text(plural_form, language=tokenize_language))

        # Progress indicator every 1000 entries (single-line refresh)
        if (idx + 1) % 1000 == 0:
            print(
                f"   ✓ Processed {idx+1:,} entries... ({len(all_tokens):,} unique tokens so far)",
                end='\r',
                flush=True,
            )

    # Ensure next log starts on a new line after single-line progress refresh
    print()

    # ── Spanish default lowercasing for item/monster first token ───────────
    if _is_spanish and es_item_monster_first_candidates:
        _evidence = _build_es_i18n_case_evidence(game_tag, lang_prefix) if game_tag else {}
        _cand = len(es_item_monster_first_candidates)
        _blocked_npc_area_set = {
            tok for tok in es_item_monster_first_candidates
            if tok in es_npc_area_tokens_lower
        }
        if _evidence:
            _blocked_corpus_set = {
                tok for tok in es_item_monster_first_candidates
                if _evidence.get(tok, {}).get('uppercase_mid_sentence_count', 0) > 0
                and _evidence.get(tok, {}).get('lowercase_count', 0) == 0
            }
            _evidence_note = 'ready'
        else:
            # Conservative fallback: without filtered i18n evidence, do not
            # auto-add ES default lowercase candidates.
            _blocked_corpus_set = set(es_item_monster_first_candidates)
            _evidence_note = 'missing'

        _to_add = es_item_monster_first_candidates - _blocked_npc_area_set - _blocked_corpus_set
        _before = len(all_tokens)
        all_tokens.update(_to_add)
        _added = len(all_tokens) - _before
        print(
            f"   [es-first-lower] candidates={_cand:,}  "
            f"blocked(npc/area)={len(_blocked_npc_area_set):,}  "
            f"blocked(corpus-rule)={len(_blocked_corpus_set):,}  "
            f"added={_added:,}  evidence={_evidence_note}"
        )

    if str(game_tag).strip().upper() == 'ANKANIMATION':
        _before_ank = len(all_tokens)
        all_tokens = _apply_ankanimation_token_overrides(all_tokens, lang_prefix)
        _after_ank = len(all_tokens)
        print(f"   [ank-token-overrides] before={_before_ank:,} after={_after_ank:,}")

    # Convert to sorted list
    result_tokens = sorted(list(all_tokens))

    print(f"\n✅ TOKENIZATION COMPLETE:")
    print(f"   📥 Total entries: {len(df):,}")
    print(f"   ✓ Processed: {processed:,}")
    print(f"   ⚠️  Skipped (empty): {skipped_empty:,}")
    print(f"   🎯 Unique tokens: {len(result_tokens):,}")

    if result_tokens:
        print(f"\n   📝 Sample tokens (first 10):")
        for token in result_tokens[:10]:
            print(f"      - {token}")

    # Export if output path provided
    result_output_path = ""
    if output_file_path:
        print(f"\n💾 Exporting {len(result_tokens):,} tokenized terms to: {output_file_path}")
        export_tokens_to_txt(set(result_tokens), output_file_path)
        result_output_path = output_file_path
    else:
        # Generate default output path if not provided but would be useful to know
        base_name = Path(excel_file_path).stem
        default_output = f"{base_name}_{language_code}_tokenized.txt"

    # ── Save proper-noun sidecar JSON ─────────────────────────────────────────
    if save_propernoun_sidecar and propernoun_map:
        import json as _json
        lang_prefix = normalize_language_code(language_code)
        _game = game_tag or Path(excel_file_path).stem.split('_')[0]
        os.makedirs(INTERMEDIARY_DIR, exist_ok=True)
        _sidecar_path = os.path.join(
            INTERMEDIARY_DIR, f"{_game}_{lang_prefix}_propernoun_tokens.json"
        )
        # Convert sets to sorted lists for JSON serialisation
        _sidecar_data = {
            kt: sorted(tokens_set) for kt, tokens_set in propernoun_map.items()
        }
        with open(_sidecar_path, 'w', encoding='utf-8') as _fh:
            _json.dump(_sidecar_data, _fh, ensure_ascii=False, indent=2)
        _total_pn = sum(len(v) for v in _sidecar_data.values())
        print(f"\n🔖 Proper-noun sidecar saved → {_sidecar_path}")
        print(f"   Key-types: {list(_sidecar_data.keys())}  |  Tokens: {_total_pn:,}")

    elif save_propernoun_sidecar:
        print(f"\n🔖 No proper-noun tokens found for key patterns ({list(PROPER_NOUN_KEY_PATTERNS.keys())})")

    return result_tokens, result_output_path