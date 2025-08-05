# Enhanced Language File Processor - Complete Summary

## Features

The script now includes **comprehensive filtering** with multiple advanced conditions to ensure high-quality token extraction.

### Supported File Types
- **Excel files** (`.xlsx`, `.xls`): Language code as column name
- **XLIFF files** (`.xliff`, `.xlf`, `.xml`): Language code in `source-language` or `target-language` attributes

### Key Functionality
1. **File Type Detection**: Automatically detects file type based on extension
2. **Language Matching**: 
   - Excel: Extracts from column matching the language code
   - XLIFF: Extracts from `<source>` or `<target>` elements based on language attributes

### **COMPREHENSIVE Filtering System**
3. **Square Bracket Filtering**: Ignores entries where source text contains `[.+]` pattern
4. **Target = Source Filtering**: Ignores entries where target text equals source text
5. **All-Caps Target Filtering**: **NEW** - Ignores entries where target text is entirely in uppercase
6. **HTML Tag Removal**: **NEW** - Removes HTML tags and decodes HTML entities before tokenization
7. **Hyperlink & Email Removal**: Removes URLs and email addresses before tokenization
8. **Token Edge Cleaning**: **NEW** - Removes leading/trailing apostrophes and hyphens from tokens
9. **Short Token Filtering**: Removes tokens with length < 3 characters
10. **Same Character Chain Filtering**: Removes tokens that are chains of the same character (e.g., "aaa", "zzZZzz")
11. **Number-Only Token Filtering**: **NEW** - Removes tokens that consist only of digits
12. **Time Pattern Filtering**: **NEW** - Removes tokens matching `\d+(PA|PM|AM|AL)` pattern
13. **Digit-Word Pattern Filtering**: **NEW** - Removes tokens matching `\d+-\w+` pattern (e.g., "123-neutral")
14. **Enhanced Punctuation**: **NEW** - Includes º character in punctuation list
15. **Tokenization**: Splits by whitespace and punctuation, preserving hyphens (`-`) and apostrophes (`'`)
16. **Export**: Saves unique tokens (case-sensitive) to text file, one per line

### Usage

#### Basic Token Extraction
```python
# Basic usage
tokens = process_file(file_path, language_code)

# With custom output path
tokens = process_file(file_path, language_code, output_path)

# With ignore_identical_translation parameter (default: True)
tokens = process_file(file_path, language_code, output_path, ignore_identical_translation=False)
```

#### Token File Merging
```python
# Merge two token files into one with unique tokens
merge_token_files(file1_path, file2_path, output_file_path)

# Example: Merge terminology base (TB) tokens with translation memory (TM) tokens
merge_token_files("spanish_TB_tokens.txt", "spanish_TM_tokens.txt", "merged_spanish_tokens.txt")
```

#### Dictionary Filtering
Remove common language words from your token list using Hunspell dictionaries:

```python
# Basic dictionary filtering (v1.0)
filter_tokens_by_dictionary(
    txt_file_path="merged_spanish_tokens.txt",
    dic_file_path="es_ES.dic", 
    output_dic_path="filtered_spanish_tokens.dic"
)

# Enhanced filtering with morphological rules (v2.0 - Recommended)
filter_tokens_by_dictionary_with_affixes(
    txt_file_path="merged_spanish_tokens.txt",
    dic_file_path="es_ES.dic",
    aff_file_path="es_ES.aff",
    output_dic_path="enhanced_filtered_tokens.dic"
)
```

## Advanced Features

### Complete Workflow
1. **Extract tokens** from terminology base (TB) and translation memory (TM) files
2. **Merge token lists** to combine curated terms with TM vocabulary
3. **Filter common words** using Hunspell dictionaries to isolate game-specific terms

### Token File Merging
The `merge_token_files()` function combines multiple token files while maintaining uniqueness:
- **Purpose**: Merge curated terminology base tokens with translation memory tokens
- **Benefit**: Avoid problematic non-translations in TM (e.g., `élément_FR` → `élément[WIP]_ES`) while preserving intentional non-translations from TB (e.g., `Wabbit_FR` = `Wabbit_ES`)
- **Output**: Single deduplicated list sorted alphabetically

### Dictionary Filtering
Remove common language words to isolate domain-specific terminology:

#### Version 1.0: Basic Filtering
- Case-insensitive matching against Hunspell `.dic` files
- Removes Hunspell metadata (e.g., `casa/S` → matches `casa`)
- Simple but effective for most use cases

#### Version 2.0: Enhanced Morphological Filtering (Recommended)
- **Affix rule processing**: Parses `.aff` files for morphological patterns
- **Expanded coverage**: Generates all possible word forms (plurals, conjugations, etc.)
- **Better accuracy**: Recognizes `casas`, `casita`, `casón` from base word `casa`
- **Significantly improved**: Typically 2-3x more word forms than basic version

### File Format Support
- **Input**: Excel (`.xlsx`, `.xls`) and XLIFF (`.xliff`, `.xlf`, `.xml`) files
- **Output**: Text files (`.txt`) with one token per line, or dictionary files (`.dic`) with token count

### Parameters
- `ignore_identical_translation` (bool, default: True): Skip entries where target text equals source text
- Useful for including/excluding identical translations in token extraction

### Example Advanced Filtering Results
**Input Processing:**
- ✅ **"Hola mundo"** → `['Hola', 'mundo']`
- ❌ **"[Debug] test"** → Skipped (square brackets in source)
- ❌ **"Same text"** → Skipped (target equals source)
- ❌ **"TODO EN MAYÚSCULAS"** → Skipped (all caps target)
- ✅ **HTML content** → Tags removed, entities decoded
- ✅ **"'Resistencia 'Robo'"** → `['Resistencia', 'Robo']` (edges cleaned)
- ❌ **Number tokens: "123", "456"** → Filtered out (numbers only)
- ❌ **Time patterns: "3PM", "10AM"** → Filtered out (time pattern)
- ❌ **Digit-word: "123-neutral"** → Filtered out (digit-word pattern)
- ✅ **"25º celsius"** → `['celsius']` (º treated as punctuation)

**Final Result:** Only meaningful, clean tokens ≥ 3 characters from appropriate entries

## Complete Workflow Example

```python
# Step 1: Extract tokens from terminology base
tb_tokens = process_file("terminology_base.xlsx", "es-es", "spanish_TB_tokens.txt")

# Step 2: Extract tokens from translation memory  
tm_tokens = process_file("translation_memory.xliff", "es-es", "spanish_TM_tokens.txt")

# Step 3: Merge both token lists
merge_token_files("spanish_TB_tokens.txt", "spanish_TM_tokens.txt", "merged_spanish_tokens.txt")

# Step 4: Filter out common language words (Enhanced version)
filter_tokens_by_dictionary_with_affixes(
    txt_file_path="merged_spanish_tokens.txt",
    dic_file_path="es_ES.dic",
    aff_file_path="es_ES.aff", 
    output_dic_path="final_game_terms.dic"
)
```

**Result**: A curated dictionary of game-specific terms with common language words removed, ready for translation quality assurance or terminology validation.