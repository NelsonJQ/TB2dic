import os
from typing import Dict, List, Set

GAMES = ["DOFUS", "WAKFU", "TOUCH", "WAVEN", "RETRO",
        "ONE_MORE_GATE", "ANKANIMATION"]
# Animation glossary not included by 1/2026

TB_PATHS = {
    "DOFUS" : "TB_ANK_202507/DOFUS_TB.csv",
    "WAKFU" : "TB_ANK_202507/WAKFU_TB.csv",
    "TOUCH" : "TB_ANK_202507/TOUCH_TB.csv",
    "WAVEN" : "TB_ANK_202507/WAVEN_TB.xlsx",
    "RETRO" : "TB_ANK_202507/Retro_TB.xlsx",
    "ONE_MORE_GATE" : "TB_ANK_202507/ONE_MORE_GATE_TB.xlsx",
    "ANKANIMATION": "TB_ANK_202507/Anim.xls"
}

# TM files have xliff extension, if not available or empty, let's use i18n_PATHS
TM_PATHS = {
    "DOFUS" : "",
    "WAKFU" : "",
    "TOUCH" : "",
    "WAVEN" : "",
    "RETRO" : "",
    "ONE_MORE_GATE" : "",
    "ANKANIMATION": ""
}

#In-game i18n strings https://github.com/dofusdude/dofus3-main/releases
i18n_PATHS = {
    "DOFUS" : {
        "fr": "i18n/DOFUS/fr.json",
        "en": "i18n/DOFUS/en.json",
        "es": "i18n/DOFUS/es.json",
        "pt": "i18n/DOFUS/pt.json",
        "de": "i18n/DOFUS/de.json"
    },
    "WAKFU" : {
        "fr": "i18n/WAKFU/texts_fr.properties",
        "en": "i18n/WAKFU/texts_en.properties",
        "es": "i18n/WAKFU/texts_es.properties",
        "pt": "i18n/WAKFU/texts_pt.properties"
    },
    "TOUCH" :  {
        "fr": "i18n/TOUCH/fr.json",
        "en": "i18n/TOUCH/en.json",
        "es": "i18n/TOUCH/es.json",
        "pt": "i18n/TOUCH/pt.json",
        "de": "i18n/TOUCH/de.json"
    },
    "WAVEN" : "",
    "RETRO" : "",
    "ONE_MORE_GATE" : "",
    "ANKANIMATION": ""
}

LANG_CODES = {
    "FRENCH" : "fr",
    "ENGLISH" : "en",
    "SPANISH" : "es",
    "PORTUGUESE" : "pt",
    "GERMAN" : "de"
}

DIC_FOLDER = "dics"
OUTPUT_DIR = "output_dics"          # for final dictionary folders
INTERMEDIARY_DIR = "processing_dics" # for intermediary files, cache and test outputs
OUTPUT_FLATLISTS_DIR = os.path.join(OUTPUT_DIR, "Flatlists")
OUTPUT_COMPRESSED_DIR = os.path.join(OUTPUT_DIR, "Compressed_dics")
OUTPUT_FULL_DIR = os.path.join(OUTPUT_DIR, "Full_dics")


HUNSPELL_PATHS = {
    "es": os.path.join(DIC_FOLDER, "es_dic", "es", "es_ES.dic"),
    "fr": os.path.join(DIC_FOLDER, "fr_dic", "fr.dic"),
    "pt": os.path.join(DIC_FOLDER, "pt_dic", "pt_BR", "pt_BR.dic"),
    "en": os.path.join(DIC_FOLDER, "en_dic", "en_GB.dic"),
    "de": os.path.join(DIC_FOLDER, "de_dic", "de_DE_frami.dic"),
}

# -- Proper-noun key patterns -> sidecar category mapping ---------------------
# Maps a semantic category label to one or more regex patterns matched with
# re.search() against each comma-split TB / i18n key string.
#
# Pattern notes:
#   * D3 / RETRO / TOUCH  dot-notation    : monster.123.name
#   * WKF                 mixed notation  : Item.name.42 / Monster.name
#   * WAVEN               SCREAMING_SNAKE : ZONE_WORLD_1_2_NAME
#   * (?i) makes the match case-insensitive
#   * Only keys matching a pattern end up in the propernoun sidecar JSON.
#     Keys like "NPC.#.reply" are intentionally NOT listed -> not tagged.
#
# Category labels consumed by:
#   * find_corpus_wordforms()    -> entity categories trigger gender ghost forms
#   * munch_to_compressed_dic()  -> category-specific mandatory flag assignment
#
# _ENTITY_CATEGORIES  : names of living entities -> gender ghosts + possessive
# _AREA_CATEGORIES    : place names              -> DE compound flags
PROPER_NOUN_KEY_PATTERNS: Dict[str, List[str]] = {
    "monster": [
        r"(?i)monster\.\d+\.name",                # D3, RETRO, TOUCH
        r"(?i)Monster\.name(?:\.\d+)?",           # WKF
    ],
    "npc": [
        r"(?i)NPC\.\d+\.name",                    # D3, TOUCH
        r"(?i)npc\.\d+\.name",                    # RETRO
        r"(?i)taxcollector\.\w+name\.\d+\.\w+",  # TOUCH tax collector
        r"(?i)document\.\d+\.author",             # D3 document authors
        r"(?i)Membre de clan\.name\.\d+",         # WKF clan members
        # WAVEN companions are NPCs
        r"COMPANION_\d+_NAME",                    # WAVEN
        r"GOD_\d+_NAME",                          # WAVEN
        r"CHARACTER_NAME_\d+_NAME",               # WAVEN
    ],

    "title": [
        r"(?i)title\.\d+\.(male|female)",         # D3 titles (gendered)
    ],
    "area": [
        r"(?i)area\.\d+\.name",                   # D3, RETRO
        r"(?i)subarea\.\d+\.name",                # D3, RETRO
        r"(?i)superarea\.\d+\.name",              # RETRO
        r"(?i)world\.subarea\.\d+\.name",         # D3
        r"(?i)world\.\w+\.\d+\.name",             # D3 (world.* catch-all)
        r"(?i)map\.name\.\d+\.name",              # D3
        #r"(?i)dungeon\.\d+\.name",                # RETRO
        r"(?i)hint\.\d+\.name",                   # D3, RETRO, TOUCH
        #r"(?i)interactive\.\d+\.name",            # D3
        r"(?i)Ambience Zone\.name\.\d+",          # WKF
        r"(?i)Territory_Zone\.name\.\d+",         # WKF
        r"(?i)Zaap\.\w+name\.\d+",               # WKF
        r"ZONE_\w+_\d+(?:_\d+)?_NAME",           # WAVEN
    ],

    "item": [
        r"(?i)Item\.name\.\d+",                   # WKF items
        r"(?i)object\.\d+\.name",                 # D3
    ],
}

# Semantic groupings used by munch_to_compressed_dic() for flag assignment
_ENTITY_CATEGORIES: Set[str] = {"monster", "npc", "title"}
_AREA_CATEGORIES: Set[str] = {"area"}

# Pre-compiled patterns (built once at import time for speed)
import re as _re
_COMPILED_PROPER_NOUN_PATTERNS: Dict[str, list] = {
    cat: [_re.compile(p) for p in patterns]
    for cat, patterns in PROPER_NOUN_KEY_PATTERNS.items()
}

# -- Munch flag strategy per language -----------------------------------------
# mandatory     : assigned to ALL words unconditionally
# area_extra    : additional mandatory flags for area-category proper nouns only
#                 (e.g. DE COMPOUNDBEGIN/MID/END so place names can head compounds)
# entity_extra  : additional mandatory flags for entity-category proper nouns only
#                 (e.g. EN possessive M -- not needed for place names)
# validation    : assigned when >= quorum fraction of flag-generated forms are
#                 in the confirmed corpus set (evaluated in _wordform_match_worker)
# verb          : like validation but only tested when add_verb_flags=True
MUNCH_FLAG_CONFIG: Dict = {
    "es": {
        "mandatory":    [],
        "area_extra":   [],
        "entity_extra": [],
        "validation":   ["S", "G"],
        "verb":         ["R", "E", "I"],
    },
    "de": {
        "mandatory":    ["S"],             # genitive -s for all DE proper nouns
        "area_extra":   ["x", "y", "z"],   # COMPOUNDBEGIN/MID/END only for place names
        "entity_extra": [],
        "validation":   ["F", "p", "P", "R", "N", "E"],
        "verb":         ["I", "X", "Y", "Z", "W"],
    },
    "en": {
        "mandatory":    [],
        "area_extra":   [],
        "entity_extra": ["M"],             # possessive 's only for entity names
        "validation":   ["S"],
        "verb":         ["D", "G", "d"],
    },
    "pt": {
        "mandatory":    [],
        "area_extra":   [],
        "entity_extra": [],
        "validation":   ["B", "D", "F"],
        "verb":         [],
    },
    "fr": {
        "mandatory":    [],
        "area_extra":   [],
        "entity_extra": [],
        "validation":   [],
        "verb":         [],
    },
}

basic_punct = (
    '.,;:¡!?"'           # standard punct (incl. straight double quote)
    '\u201c\u201d'       # " " (curly quotes)
    '\u2018\u2019'       # ' ' (single curly quotes)
    '()[]{}«»„\u201a'    # brackets and other punct
    '-+=*/@#$%^&|\\<>~`º°ª¿'
    '\u2026'             # ... (ellipsis as single char)
)

unicode_dashes = '\u2014\u2013'  # em-dash and en-dash

__all__ = [
    "GAMES",
    "TB_PATHS",
    "TM_PATHS",
    "i18n_PATHS",
    "LANG_CODES",
    "DIC_FOLDER",
    "OUTPUT_DIR",
    "INTERMEDIARY_DIR",
    "OUTPUT_FLATLISTS_DIR",
    "OUTPUT_COMPRESSED_DIR",
    "OUTPUT_FULL_DIR",
    "HUNSPELL_PATHS",
    "PROPER_NOUN_KEY_PATTERNS",
    "_ENTITY_CATEGORIES",
    "_AREA_CATEGORIES",
    "_COMPILED_PROPER_NOUN_PATTERNS",
    "MUNCH_FLAG_CONFIG",
    "basic_punct",
    "unicode_dashes",
]
