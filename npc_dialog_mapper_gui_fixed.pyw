#!/usr/bin/env python3
"""
RETRO NPC Dialog Mapper - Tkinter GUI Application
Responsive GUI for generating NPC dialogs HTML files from JSON or XLIFF sources.
Author: NelsonJQ
Last Updated: 2025-10-06
Usage: This tool can be used by anyone for personal, educational, or commercial purposes.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import os
import sys
from pathlib import Path
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from collections import defaultdict

# Import the core classes and functionality from the notebook
@dataclass
class NPCMessage:
    message_id: int
    text_fr: str
    text_en: Optional[str]
    text_es: Optional[str]
    text_pt: Optional[str]
    context: Optional[str]
    replies: List['NPCReply'] = field(default_factory=list)

@dataclass
class NPCReply:
    reply_id: int
    text_fr: str
    text_en: Optional[str]
    text_es: Optional[str]
    text_pt: Optional[str]
    context: Optional[str]
    parent_message_id: int
    leads_to_message_id: Optional[int]

@dataclass
class NPCDialog:
    dialog_id: int
    npc_id: int
    message_id: int
    check_order: int

@dataclass
class NPCMetadata:
    index_id: int
    gender: int  # 0 = MALE, 1 = FEMALE, 2 = UNDEFINED
    look: str
    img_url: str
    metadata_id: str
    name_en: str
    name_pt: str
    name_es: str
    name_fr: str
    name_de: str

@dataclass
class NPC:
    npc_id: int
    name_fr: str
    name_en: Optional[str]
    name_es: Optional[str]
    name_pt: Optional[str]
    context: Optional[str]
    dialogs: List[NPCDialog] = field(default_factory=list)
    genders: List[int] = field(default_factory=list)
    img_urls: List[str] = field(default_factory=list)

class NPCDialogMapper:
    """Core mapping class from the notebook - corrected version"""
    
    def __init__(self, folder_path: str, use_xliff: bool = False, xliff_folder: str = "", xliff_files: Optional[dict] = None):
        self.folder_path = folder_path
        self.use_xliff = use_xliff
        self.xliff_folder = xliff_folder
        self.xliff_files = xliff_files or {}
        self.npcs: Dict[int, NPC] = {}
        self.messages: Dict[int, NPCMessage] = {}
        self.replies: Dict[int, NPCReply] = {}
        self.dialogs: List[NPCDialog] = []
        self.metadata: List[NPCMetadata] = []
        
        # For XLIFF mode, we store translations separately
        self.xliff_translations = {
            'npc_names': {},    # npc_id -> {fr, en, es, pt}
            'messages': {},     # message_id -> {fr, en, es, pt}
            'replies': {}       # reply_id -> {fr, en, es, pt}
        }
    
    def load_data(self):
        """Load all data files"""
        print("Loading NPC metadata...")
        self._load_metadata()
        
        if self.use_xliff:
            print("Loading translations from XLIFF files...")
            self._load_xliff_translations()
            print("Loading structure from JSON files...")
            self._load_structure_from_json()
            print("Merging XLIFF translations with JSON structure...")
            self._merge_xliff_with_structure()
        else:
            print("Loading NPC data...")
            self._load_npcs()
            print("Loading messages...")
            self._load_messages()
            print("Loading replies...")
            self._load_replies()
            print("Loading dialogs...")
            self._load_dialogs()
            
        print("Building relationships...")
        self._build_relationships()
        print("Matching metadata with NPCs...")
        self._match_metadata_with_npcs()
    
    def _load_xliff_translations(self):
        """Load all XLIFF files and extract translations only"""
        for lang_pair, filename in self.xliff_files.items():
            file_path = os.path.join(self.xliff_folder, filename)
            print(f"  Loading {lang_pair}: {filename}")
            
            if not os.path.exists(file_path):
                print(f"    Warning: File not found: {file_path}")
                continue
                
            # Determine target language
            target_lang = lang_pair.split('-')[1]  # en, es, pt
            
            # Parse XLIFF file
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Define namespace
                ns = {'xliff': 'urn:oasis:names:tc:xliff:document:1.2'}
                
                # Extract all trans-units
                trans_units = root.findall('.//xliff:trans-unit', ns)
                
                for trans_unit in trans_units:
                    unit_id = trans_unit.get('id', '')
                    
                    # Check if this is an NPC-related entry
                    if unit_id.startswith('npc.'):
                        source_elem = trans_unit.find('.//xliff:source', ns)
                        target_elem = trans_unit.find('.//xliff:target', ns)
                        
                        if source_elem is not None and target_elem is not None:
                            source_text = source_elem.text or ""
                            target_text = target_elem.text or ""
                            
                            # Clean the source text (remove bracketed content like [wait_mod_Br9100])
                            source_text = self._clean_source_text(source_text)
                            
                            # Parse the ID to extract parts
                            parts = unit_id.split('.')
                            if len(parts) >= 3:
                                entity_id = parts[1]  # This is the ID of the name/message/reply
                                entry_type = parts[2]  # name, message, reply
                                
                                try:
                                    entity_id_int = int(entity_id)
                                except ValueError:
                                    continue  # Skip non-numeric IDs
                                
                                # Store the translations by type and ID
                                if entry_type == 'name':
                                    if entity_id_int not in self.xliff_translations['npc_names']:
                                        self.xliff_translations['npc_names'][entity_id_int] = {'fr': source_text}
                                    self.xliff_translations['npc_names'][entity_id_int][target_lang] = target_text
                                    
                                elif entry_type == 'message':
                                    if entity_id_int not in self.xliff_translations['messages']:
                                        self.xliff_translations['messages'][entity_id_int] = {'fr': source_text}
                                    self.xliff_translations['messages'][entity_id_int][target_lang] = target_text
                                    
                                elif entry_type == 'reply':
                                    if entity_id_int not in self.xliff_translations['replies']:
                                        self.xliff_translations['replies'][entity_id_int] = {'fr': source_text}
                                    self.xliff_translations['replies'][entity_id_int][target_lang] = target_text
                
                npc_count = len(self.xliff_translations['npc_names'])
                msg_count = len(self.xliff_translations['messages'])
                reply_count = len(self.xliff_translations['replies'])
                
                print(f"    Extracted {npc_count} NPC name translations")
                print(f"    Extracted {msg_count} message translations")
                print(f"    Extracted {reply_count} reply translations")
                
            except ET.ParseError as e:
                print(f"    Error parsing {filename}: {e}")
            except Exception as e:
                print(f"    Unexpected error processing {filename}: {e}")
    
    def _clean_source_text(self, text: str) -> str:
        """Clean source text by removing bracketed modifiers"""
        import re
        # Remove patterns like [wait_mod_Br9100] from the beginning
        cleaned = re.sub(r'^\[wait_mod_[^\]]+\]\s*', '', text)
        return cleaned.strip()
    
    def _load_structure_from_json(self):
        """Load the structural data from JSON files (dialogs and reply relationships)"""
        # Load dialogs to understand NPC -> Message relationships
        file_path = os.path.join(self.folder_path, "export_npc_dialog_json.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        dialog_data = list(data.values())[0]
        
        for dialog in dialog_data:
            dialog_obj = NPCDialog(
                dialog_id=dialog['dialog_id'],
                npc_id=dialog['dialog_npc_id'],
                message_id=dialog['dialog_message_id'],
                check_order=dialog['dialog_message_check_order']
            )
            self.dialogs.append(dialog_obj)
        
        # Load reply relationships
        file_path = os.path.join(self.folder_path, "export_npc_reply_json.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        reply_data = list(data.values())[0]
        
        # Store reply relationships temporarily for merging
        self._reply_relationships = {}
        for reply in reply_data:
            self._reply_relationships[reply['reply_id']] = {
                'parent_message_id': reply['reply_parent_id'],
                'leads_to_message_id': reply['reply_message_id'],
                'context': reply['reply_i18n_context']
            }
        
        print(f"Loaded {len(self.dialogs)} dialog structures")
        print(f"Loaded {len(self._reply_relationships)} reply relationships")
    
    def _merge_xliff_with_structure(self):
        """Merge XLIFF translations with JSON structure"""
        # Create NPCs with XLIFF translations
        for npc_id, translations in self.xliff_translations['npc_names'].items():
            npc = NPC(
                npc_id=npc_id,
                name_fr=translations.get('fr', ''),
                name_en=translations.get('en'),
                name_es=translations.get('es'),
                name_pt=translations.get('pt'),
                context=None  # Not available in XLIFF
            )
            self.npcs[npc_id] = npc
        
        # Create messages with XLIFF translations
        for msg_id, translations in self.xliff_translations['messages'].items():
            message = NPCMessage(
                message_id=msg_id,
                text_fr=translations.get('fr', ''),
                text_en=translations.get('en'),
                text_es=translations.get('es'),
                text_pt=translations.get('pt'),
                context=None  # Not available in XLIFF
            )
            self.messages[msg_id] = message
        
        # Create replies with XLIFF translations + JSON relationships
        for reply_id, translations in self.xliff_translations['replies'].items():
            # Get relationship data from JSON
            relationship = self._reply_relationships.get(reply_id, {})
            
            reply = NPCReply(
                reply_id=reply_id,
                text_fr=translations.get('fr', ''),
                text_en=translations.get('en'),
                text_es=translations.get('es'),
                text_pt=translations.get('pt'),
                context=relationship.get('context'),
                parent_message_id=relationship.get('parent_message_id', 0),
                leads_to_message_id=relationship.get('leads_to_message_id')
            )
            self.replies[reply_id] = reply
        
        # Clean up temporary data
        del self._reply_relationships
        
        print(f"Created {len(self.npcs)} NPCs with XLIFF translations")
        print(f"Created {len(self.messages)} messages with XLIFF translations")
        print(f"Created {len(self.replies)} replies with XLIFF translations")
    
    def _load_metadata(self):
        """Load NPC metadata from sister game"""
        file_path = os.path.join(self.folder_path, "npcs_Dofus3-03_202508.json")
        print(f"Loading metadata from: {file_path}")
        
        if not os.path.exists(file_path):
            print(f"Warning: Metadata file not found")
            return
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            for item in data:
                self.metadata.append(NPCMetadata(
                    index_id=item['indexID'],
                    gender=item['gender'],
                    look=item['look'],
                    img_url=item['img'],
                    metadata_id=item['id'],
                    name_en=item['en'],
                    name_pt=item['pt'],
                    name_es=item['es'],
                    name_fr=item['fr'],
                    name_de=item['de']
                ))
            
            print(f"Successfully loaded {len(self.metadata)} metadata entries")
        except Exception as e:
            print(f"Error loading metadata: {e}")
            print("Continuing without metadata...")
    
    def _load_npcs(self):
        """Load NPC data (JSON mode only)"""
        file_path = os.path.join(self.folder_path, "export_npc_json.json")
        print(f"Loading NPCs from: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Get the first (and only) key which contains the SQL query data
            npc_data = list(data.values())[0]
            print(f"Found {len(npc_data)} NPC entries")
            
            for npc in npc_data:
                self.npcs[npc['npc_id']] = NPC(
                    npc_id=npc['npc_id'],
                    name_fr=npc['npc_name_fr'],
                    name_en=npc['npc_name_en'],
                    name_es=npc['npc_name_es'],
                    name_pt=npc['npc_name_pt'],
                    context=npc['context']
                )
            
            print(f"Successfully loaded {len(self.npcs)} NPCs")
        except Exception as e:
            print(f"Error loading NPCs: {e}")
            raise
    
    def _load_messages(self):
        """Load message data (JSON mode only)"""
        file_path = os.path.join(self.folder_path, "export_npc_message_json.json")
        print(f"Loading messages from: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            message_data = list(data.values())[0]
            print(f"Found {len(message_data)} message entries")
            
            for msg in message_data:
                self.messages[msg['message_id']] = NPCMessage(
                    message_id=msg['message_id'],
                    text_fr=msg['message_text_fr'],
                    text_en=msg['message_text_en'],
                    text_es=msg['message_text_es'],
                    text_pt=msg['message_text_pt'],
                    context=msg['message_i18n_context']
                )
            
            print(f"Successfully loaded {len(self.messages)} messages")
        except Exception as e:
            print(f"Error loading messages: {e}")
            raise
    
    def _load_replies(self):
        """Load reply data (JSON mode only)"""
        file_path = os.path.join(self.folder_path, "export_npc_reply_json.json")
        print(f"Loading replies from: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            reply_data = list(data.values())[0]
            print(f"Found {len(reply_data)} reply entries")
            
            for reply in reply_data:
                self.replies[reply['reply_id']] = NPCReply(
                    reply_id=reply['reply_id'],
                    text_fr=reply['reply_text_fr'],
                    text_en=reply['reply_text_en'],
                    text_es=reply['reply_text_es'],
                    text_pt=reply['reply_text_pt'],
                    context=reply['reply_i18n_context'],
                    parent_message_id=reply['reply_parent_id'],
                    leads_to_message_id=reply['reply_message_id']
                )
            
            print(f"Successfully loaded {len(self.replies)} replies")
        except Exception as e:
            print(f"Error loading replies: {e}")
            raise
    
    def _load_dialogs(self):
        """Load dialog data (JSON mode only)"""
        file_path = os.path.join(self.folder_path, "export_npc_dialog_json.json")
        print(f"Loading dialogs from: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            dialog_data = list(data.values())[0]
            print(f"Found {len(dialog_data)} dialog entries")
            
            for dialog in dialog_data:
                dialog_obj = NPCDialog(
                    dialog_id=dialog['dialog_id'],
                    npc_id=dialog['dialog_npc_id'],
                    message_id=dialog['dialog_message_id'],
                    check_order=dialog['dialog_message_check_order']
                )
                self.dialogs.append(dialog_obj)
            
            print(f"Successfully loaded {len(self.dialogs)} dialogs")
        except Exception as e:
            print(f"Error loading dialogs: {e}")
            raise
    
    def _build_relationships(self):
        """Build relationships between NPCs, dialogs, messages and replies"""
        # Link dialogs to NPCs
        for dialog in self.dialogs:
            if dialog.npc_id in self.npcs:
                self.npcs[dialog.npc_id].dialogs.append(dialog)
        
        # Sort dialogs by check_order for each NPC
        for npc in self.npcs.values():
            npc.dialogs.sort(key=lambda d: d.check_order)
        
        # Link replies to messages
        for reply in self.replies.values():
            if reply.parent_message_id in self.messages:
                self.messages[reply.parent_message_id].replies.append(reply)
    
    def _match_metadata_with_npcs(self):
        """Match metadata with NPCs by French name"""
        metadata_by_name = defaultdict(list)
        
        # Group metadata by French name
        for meta in self.metadata:
            metadata_by_name[meta.name_fr].append(meta)
        
        # Match NPCs with metadata
        matched_count = 0
        for npc in self.npcs.values():
            if npc.name_fr in metadata_by_name:
                matches = metadata_by_name[npc.name_fr]
                
                # Extract unique genders and image URLs
                unique_genders = list(set(meta.gender for meta in matches))
                unique_img_urls = list(set(meta.img_url for meta in matches))
                
                npc.genders = unique_genders
                npc.img_urls = unique_img_urls
                matched_count += 1
        
        print(f"Matched {matched_count} NPCs with metadata from {len(self.npcs)} total NPCs")
    
    def generate_html(self, output_filename: str):
        """Generate HTML file with all dialog mappings"""
        print(f"Generating HTML file: {output_filename}")
        
        html_content = self._generate_html_content()
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML file generated successfully: {output_filename}")
    
    def _format_text_with_null_check(self, text: Optional[str]) -> str:
        """Format text with null checking"""
        if text is None or text == "":
            return '<span class="null-value">NULL ⚠️</span>'
        return text.replace('\n', '<br>').replace('\r', '')
    
    def analyze_statistics(self) -> Dict[str, Any]:
        """Analyze comprehensive statistics for the loaded data"""
        stats = {}
        
        # Basic counts
        total_npcs = len(self.npcs)
        npcs_with_dialogs = len([npc for npc in self.npcs.values() if npc.dialogs])
        total_messages = len(self.messages)
        total_replies = len(self.replies)
        total_dialogs = len(self.dialogs)
        total_metadata = len(self.metadata)
        
        stats['basic'] = {
            'total_npcs': total_npcs,
            'npcs_with_dialogs': npcs_with_dialogs,
            'total_messages': total_messages,
            'total_replies': total_replies,
            'total_dialogs': total_dialogs,
            'total_metadata': total_metadata,
            'data_source': 'XLIFF Bilingual Files' if self.use_xliff else 'JSON Database Export'
        }
        
        # Metadata matching statistics
        npcs_with_metadata = sum(1 for npc in self.npcs.values() if npc.genders or npc.img_urls)
        npcs_with_images = sum(1 for npc in self.npcs.values() if npc.img_urls)
        npcs_with_gender = sum(1 for npc in self.npcs.values() if npc.genders)
        
        stats['metadata'] = {
            'npcs_with_metadata': npcs_with_metadata,
            'npcs_with_images': npcs_with_images,
            'npcs_with_gender': npcs_with_gender,
            'metadata_match_percentage': (npcs_with_metadata / total_npcs * 100) if total_npcs > 0 else 0,
            'gender_match_percentage': (npcs_with_gender / total_npcs * 100) if total_npcs > 0 else 0,
            'images_match_percentage': (npcs_with_images / total_npcs * 100) if total_npcs > 0 else 0
        }
        
        # Translation quality analysis
        def count_null_and_x(text: Optional[str]) -> tuple:
            """Count if text is null/empty or just 'x'"""
            if text is None or text == "":
                return (True, False)  # is_null, is_x
            elif text.strip().lower() == "x":
                return (False, True)  # is_null, is_x
            else:
                return (False, False)  # is_null, is_x
        
        # Analyze NPC names
        npc_name_stats = {
            'en': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'es': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'pt': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0}
        }
        
        for npc in self.npcs.values():
            for lang, text in [('en', npc.name_en), ('es', npc.name_es), ('pt', npc.name_pt)]:
                is_null, is_x = count_null_and_x(text)
                if is_null:
                    npc_name_stats[lang]['null'] += 1
                elif is_x:
                    npc_name_stats[lang]['x'] += 1
                else:
                    npc_name_stats[lang]['valid'] += 1
        
        # Analyze messages
        message_stats = {
            'en': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'es': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'pt': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0}
        }
        
        for message in self.messages.values():
            for lang, text in [('en', message.text_en), ('es', message.text_es), ('pt', message.text_pt)]:
                is_null, is_x = count_null_and_x(text)
                if is_null:
                    message_stats[lang]['null'] += 1
                elif is_x:
                    message_stats[lang]['x'] += 1
                else:
                    message_stats[lang]['valid'] += 1
        
        # Analyze replies
        reply_stats = {
            'en': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'es': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0},
            'pt': {'null': 0, 'x': 0, 'valid': 0, 'null_percentage': 0.0, 'x_percentage': 0.0, 'valid_percentage': 0.0}
        }
        
        for reply in self.replies.values():
            for lang, text in [('en', reply.text_en), ('es', reply.text_es), ('pt', reply.text_pt)]:
                is_null, is_x = count_null_and_x(text)
                if is_null:
                    reply_stats[lang]['null'] += 1
                elif is_x:
                    reply_stats[lang]['x'] += 1
                else:
                    reply_stats[lang]['valid'] += 1
        
        # Calculate percentages relative to totals
        for lang in ['en', 'es', 'pt']:
            if total_npcs > 0:
                npc_name_stats[lang]['null_percentage'] = (npc_name_stats[lang]['null'] / total_npcs * 100)
                npc_name_stats[lang]['x_percentage'] = (npc_name_stats[lang]['x'] / total_npcs * 100)
                npc_name_stats[lang]['valid_percentage'] = (npc_name_stats[lang]['valid'] / total_npcs * 100)
            else:
                npc_name_stats[lang]['null_percentage'] = 0.0
                npc_name_stats[lang]['x_percentage'] = 0.0
                npc_name_stats[lang]['valid_percentage'] = 0.0
                
            if total_messages > 0:
                message_stats[lang]['null_percentage'] = (message_stats[lang]['null'] / total_messages * 100)
                message_stats[lang]['x_percentage'] = (message_stats[lang]['x'] / total_messages * 100)
                message_stats[lang]['valid_percentage'] = (message_stats[lang]['valid'] / total_messages * 100)
            else:
                message_stats[lang]['null_percentage'] = 0.0
                message_stats[lang]['x_percentage'] = 0.0
                message_stats[lang]['valid_percentage'] = 0.0
                
            if total_replies > 0:
                reply_stats[lang]['null_percentage'] = (reply_stats[lang]['null'] / total_replies * 100)
                reply_stats[lang]['x_percentage'] = (reply_stats[lang]['x'] / total_replies * 100)
                reply_stats[lang]['valid_percentage'] = (reply_stats[lang]['valid'] / total_replies * 100)
            else:
                reply_stats[lang]['null_percentage'] = 0.0
                reply_stats[lang]['x_percentage'] = 0.0
                reply_stats[lang]['valid_percentage'] = 0.0
        
        stats['translation_quality'] = {
            'npc_names': npc_name_stats,
            'messages': message_stats,
            'replies': reply_stats
        }
        
        # XLIFF-specific analysis: unmapped entries
        if self.use_xliff:
            # Count messages/replies in XLIFF that don't appear in dialogs
            used_message_ids = set()
            used_reply_ids = set()
            
            # Collect all message IDs used in dialogs
            for dialog in self.dialogs:
                used_message_ids.add(dialog.message_id)
            
            # Collect all reply IDs that have parent messages in use
            for reply in self.replies.values():
                if reply.parent_message_id in used_message_ids:
                    used_reply_ids.add(reply.reply_id)
            
            # Count unmapped entries
            unmapped_messages = len(self.xliff_translations['messages']) - len(used_message_ids.intersection(self.xliff_translations['messages'].keys()))
            unmapped_replies = len(self.xliff_translations['replies']) - len(used_reply_ids.intersection(self.xliff_translations['replies'].keys()))
            
            stats['xliff_mapping'] = {
                'total_xliff_messages': len(self.xliff_translations['messages']),
                'total_xliff_replies': len(self.xliff_translations['replies']),
                'mapped_messages': len(used_message_ids.intersection(self.xliff_translations['messages'].keys())),
                'mapped_replies': len(used_reply_ids.intersection(self.xliff_translations['replies'].keys())),
                'unmapped_messages': unmapped_messages,
                'unmapped_replies': unmapped_replies,
                'message_mapping_percentage': (len(used_message_ids.intersection(self.xliff_translations['messages'].keys())) / len(self.xliff_translations['messages']) * 100) if len(self.xliff_translations['messages']) > 0 else 0,
                'reply_mapping_percentage': (len(used_reply_ids.intersection(self.xliff_translations['replies'].keys())) / len(self.xliff_translations['replies']) * 100) if len(self.xliff_translations['replies']) > 0 else 0
            }
        
        return stats
    
    def _remove_diacritics(self, text: str) -> str:
        """Remove diacritics from text for search comparison"""
        import unicodedata
        return ''.join(c for c in unicodedata.normalize('NFD', text) 
                      if unicodedata.category(c) != 'Mn')
    
    def _wildcard_to_regex(self, pattern: str) -> str:
        """Convert wildcard pattern to regex"""
        import re
        # Escape special regex characters except *
        escaped = re.escape(pattern)
        # Replace escaped asterisks with regex pattern
        regex_pattern = escaped.replace(r'\*', r'[^\s]*')
        return regex_pattern
    
    def _highlight_matches(self, text: str, search_term: str, ignore_diacritics: bool = False, use_wildcards: bool = False) -> str:
        """Highlight search matches in text"""
        if not search_term or not text:
            return text
            
        import re
        
        # Prepare search term and text for comparison
        search_text = text
        pattern = search_term
        
        if ignore_diacritics:
            search_text = self._remove_diacritics(text)
            pattern = self._remove_diacritics(search_term)
        
        if use_wildcards and '*' in pattern:
            pattern = self._wildcard_to_regex(pattern)
            flags = re.IGNORECASE
        else:
            pattern = re.escape(pattern)
            flags = re.IGNORECASE
        
        try:
            # Find matches in the processed text
            matches = list(re.finditer(pattern, search_text, flags))
            
            if not matches:
                return text
            
            # Apply highlighting to original text
            result = text
            offset = 0
            
            for match in matches:
                start, end = match.span()
                # Adjust positions for original text
                start += offset
                end += offset
                
                original_match = result[start:end]
                highlighted = f'<span class="search-highlight">{original_match}</span>'
                result = result[:start] + highlighted + result[end:]
                
                # Update offset for next replacement
                offset += len('<span class="search-highlight">') + len('</span>')
            
            return result
        except re.error:
            # If regex fails, return original text
            return text
    
    def _generate_html_content(self) -> str:
        """Generate the complete HTML content"""
        
        # Generate NPC cards HTML
        npc_cards_html = ""
        
        for npc in sorted(self.npcs.values(), key=lambda x: x.name_fr.lower()):
            if not npc.dialogs:  # Skip NPCs without dialogs
                continue
                
            npc_card = self._generate_npc_card(npc)
            npc_cards_html += npc_card
        
        # Complete HTML template
        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NPC Dialog Mapping - Dofus Retro</title>
    <style>
        {self._get_css_styles()}
    </style>
</head>
<body>
    <header>
        <h1>🗣️ NPC Dialog Mapping - Dofus Retro</h1>
        <button id="menuToggle" class="menu-toggle" title="Help & Guide">☰</button>
        <div class="disclaimer">
            ⚠️ <strong>Disclaimer:</strong> Gender and images are retrieved from name matches with Dofus 3 (dofusdb fansite), they might differ from RETRO's NPCs.
        </div>
        <div class="search-container">
            <div class="search-box">
                <input type="text" id="searchInput" placeholder="Search NPCs, dialogs, or IDs... (use * for wildcards)">
                
                <div class="search-options-grid">
                    <!-- Search Type Options -->
                    <div class="option-group">
                        <h4>🔍 Search Type</h4>
                        <div class="option-table">
                            <label class="option-row"><input type="radio" name="searchType" value="text" checked><span class="option-text">📝 Search in Text</span></label>
                            <label class="option-row"><input type="radio" name="searchType" value="npc-name"><span class="option-text">🧙‍♂️ Search in NPC Name</span></label>
                            <label class="option-row"><input type="radio" name="searchType" value="npc-id"><span class="option-text">🆔 Search by NPC ID</span></label>
                            <label class="option-row"><input type="radio" name="searchType" value="message-id"><span class="option-text">💬 Search by Message ID</span></label>
                            <label class="option-row"><input type="radio" name="searchType" value="reply-id"><span class="option-text">↪️ Search by Reply ID</span></label>
                        </div>
                    </div>
                    
                    <!-- Text Search Options -->
                    <div class="option-group">
                        <h4>⚙️ Text Search Options</h4>
                        <div class="option-table">
                            <label class="option-row"><input type="checkbox" id="exactMatch"><span class="option-text">🎯 Exact match</span></label>
                            <label class="option-row"><input type="checkbox" id="ignoreDiacritics"><span class="option-text">📝 Ignore diacritics (é=e)</span></label>
                            <label class="option-row"><input type="checkbox" id="useWildcards"><span class="option-text">⭐ Use wildcards (*)</span></label>
                            <label class="option-row"><input type="checkbox" id="showOnlyMatchDialogs"><span class="option-text">🌳 Show only match's dialog tree</span></label>
                            <label class="option-row"><input type="checkbox" id="showOnlyMatchMsgReply"><span class="option-text">💬↪️ Show only match's msg&reply</span></label>
                        </div>
                    </div>
                    
                    <!-- Language Filters -->
                    <div class="option-group">
                        <h4>🔎 Search in Languages</h4>
                        <div class="option-table">
                            <label class="option-row"><input type="checkbox" name="searchLang" value="fr" checked><span class="option-text">FR</span></label>
                            <label class="option-row"><input type="checkbox" name="searchLang" value="en" checked><span class="option-text">EN</span></label>
                            <label class="option-row"><input type="checkbox" name="searchLang" value="es" checked><span class="option-text">ES</span></label>
                            <label class="option-row"><input type="checkbox" name="searchLang" value="pt" checked><span class="option-text">PT</span></label>
                        </div>
                    </div>
                    
                    <!-- Display Options -->
                    <div class="option-group">
                        <h4>🖥️ Display Languages</h4>
                        <div class="option-table">
                            <label class="option-row"><input type="checkbox" name="displayLang" value="fr" checked><span class="option-text">Show FR</span></label>
                            <label class="option-row"><input type="checkbox" name="displayLang" value="en" checked><span class="option-text">Show EN</span></label>
                            <label class="option-row"><input type="checkbox" name="displayLang" value="es" checked><span class="option-text">Show SP</span></label>
                            <label class="option-row"><input type="checkbox" name="displayLang" value="pt" checked><span class="option-text">Show PT</span></label>
                        </div>
                    </div>
                    
                    <!-- Other Options -->
                    <div class="option-group">
                        <h4>🔧 Other Options</h4>
                        <div class="option-table">
                            <label class="option-row"><input type="checkbox" id="showNullOnly"><span class="option-text">⚠️ Show NULL values only</span></label>
                            <label class="option-row"><input type="checkbox" id="enableImages"><span class="option-text">🖼️ Enable NPC images on hover</span></label>
                            <label class="option-row"><input type="checkbox" id="enableConsolas"><span class="option-text">📝 Use Consolas font</span></label>
                            <label class="option-row language-highlight-row">
                                <span class="option-text">🌟 Highlight my language:</span>
                                <select id="highlightLanguage" class="language-dropdown">
                                    <option value="">None</option>
                                    <option value="fr">FR</option>
                                    <option value="en">EN</option>
                                    <option value="es">ES</option>
                                    <option value="pt">PT</option>
                                </select>
                            </label>
                        </div>
                    </div>
                </div>
                
                <div class="search-controls">
                    <button id="applyFiltersBtn" class="apply-btn">
                        <span class="btn-text">Apply Search Filters</span>
                        <span class="btn-icon" style="display: none;">🔄</span>
                    </button>
                    <button id="collapseAllBtn" class="control-btn">Collapse All</button>
                    <button id="expandAllBtn" class="control-btn">Expand All</button>
                </div>
            </div>
        </div>
        <div class="stats">
            <span>Total NPCs: {len([npc for npc in self.npcs.values() if npc.dialogs])}</span>
            <span>Total Messages: {len(self.messages)}</span>
            <span>Total Replies: {len(self.replies)}</span>
        </div>
    </header>
    
    <main id="npcContainer">
        {npc_cards_html}
    </main>
    
    <!-- Floating Navigation -->
    <div id="floatingNav" style="display: none;">
        <button id="prevMatchBtn" class="nav-btn" title="Previous Match">↑</button>
        <span id="matchCounter">0/0</span>
        <button id="nextMatchBtn" class="nav-btn" title="Next Match">↓</button>
    </div>
    
    <!-- Side Menu -->
    <div id="sideMenu" class="side-menu">
        <div class="side-menu-header">
            <h2>❓ Help & User Guide</h2>
            <button id="closeSideMenu" class="close-menu-btn">✕</button>
        </div>
        <div class="side-menu-content">
            <div class="help-section">
                <h3>🔍 Search Types</h3>
                <ul>
                    <li><strong>📝 Search in Text:</strong> Search within message and reply content in all languages</li>
                    <li><strong>🧙‍♂️ Search in NPC Name:</strong> Search specifically in NPC names</li>
                    <li><strong>🆔 Search by NPC ID:</strong> Find NPCs by their unique ID number</li>
                    <li><strong>💬 Search by Message ID:</strong> Find specific messages by ID</li>
                    <li><strong>↪️ Search by Reply ID:</strong> Find specific replies by ID</li>
                </ul>
            </div>
            
            <div class="help-section">
                <h3>⚙️ Text Search Options</h3>
                <ul>
                    <li><strong>🎯 Exact match:</strong> Search for whole words only (e.g., "cat" won't match "category")</li>
                    <li><strong>📝 Ignore diacritics:</strong> "café" will match "cafe", "résumé" will match "resume"</li>
                    <li><strong>⭐ Use wildcards:</strong> Use * as wildcard (e.g., "hel*" matches "hello", "help", "helmet")</li>
                    <li><strong>🌳 Show only match's dialog tree:</strong> Show complete dialog trees that contain matches</li>
                    <li><strong>💬↪️ Show only match's msg&reply:</strong> Show only matching messages with their direct replies, or matching replies with their parent messages</li>
                </ul>
            </div>
            
            <div class="help-section">
                <h3>🌐 Language Options</h3>
                <ul>
                    <li><strong>🔎 Search in Languages:</strong> Choose which languages to search in</li>
                    <li><strong>🖥️ Display Languages:</strong> Choose which languages to show in results</li>
                    <li><strong>🌟 Highlight my language:</strong> Highlight your preferred language with a dotted border</li>
                </ul>
            </div>
            
            <div class="help-section">
                <h3>🔧 Other Features</h3>
                <ul>
                    <li><strong>⚠️ Show NULL values only:</strong> Display only entries with missing translations</li>
                    <li><strong>🖼️ Enable NPC images:</strong> Show character images when hovering over NPC cards</li>
                    <li><strong>Collapse/Expand All:</strong> Quickly hide or show all dialog content</li>
                    <li><strong>Navigation:</strong> Use the floating navigation to jump between search results</li>
                </ul>
            </div>
            
            <div class="help-section">
                <h3>💡 Tips & Examples</h3>
                <ul>
                    <li><strong>Wildcard search:</strong> "quest*" finds "quest", "question", "questionnaire"</li>
                    <li><strong>Exact match:</strong> "the" only matches whole word "the", not "them" or "theory"</li>
                    <li><strong>Diacritic search:</strong> "gele" will find "gelé" & "gèle" when enabled</li>
                    <li><strong>Multi-language search:</strong> Uncheck languages you don't want to search in for faster results</li>
                    <li><strong>NULL hunting:</strong> Use "Show NULL values only" to find missing translations</li>
                </ul>
            </div>
        </div>
    </div>
    
    <!-- Side menu overlay -->
    <div class="side-menu-overlay" id="sideMenuOverlay"></div>
    
    <script>
        {self._get_javascript()}
    </script>
</body>
</html>
        """
        
        return html_template.strip()
    
    def _generate_npc_card(self, npc: NPC) -> str:
        """Generate HTML card for a single NPC"""
        
        # Format gender information with pastel colors
        gender_info = ""
        if npc.genders:
            gender_items = []
            for gender in npc.genders:
                if gender == 0:
                    gender_items.append('<span class="gender-male">Male</span>')
                elif gender == 1:
                    gender_items.append('<span class="gender-female">Female</span>')
                elif gender == 2:
                    gender_items.append('<span class="gender-undefined">Undefined</span>')
            gender_info = f'<div class="npc-gender"><strong>Gender:</strong> {", ".join(gender_items)}</div>'
        
        # Format image URLs for hover display
        img_data_attr = ""
        if npc.img_urls:
            img_data_attr = f'data-img-urls="{"|".join(npc.img_urls)}"'
        
        dialogs_html = ""
        for dialog in npc.dialogs:
            if dialog.message_id in self.messages:
                message = self.messages[dialog.message_id]
                dialog_html = self._generate_dialog_tree(message, dialog, level=0)
                dialogs_html += f"""
                <div class="dialog-container collapsible">
                    <div class="dialog-header clickable-header">
                        <strong>Dialog ID: {dialog.dialog_id}</strong> 
                        (Check Order: {dialog.check_order})
                        <span class="collapse-indicator">−</span>
                    </div>
                    <div class="collapsible-content">
                        {dialog_html}
                    </div>
                </div>
                """
        
        return f"""
        <div class="npc-card collapsible" data-npc-id="{npc.npc_id}" {img_data_attr}>
            <div class="npc-header sticky-header">
                <div class="header-content">
                    <h2 class="clickable-header">🧙‍♂️ {npc.name_fr} <span class="npc-id">(ID: {npc.npc_id})</span> <span class="collapse-indicator">−</span></h2>
                    <div class="npc-info-grid collapsible-content">
                        <div class="npc-names">
                            <div class="lang-row">
                                <span class="lang-label">FR:</span> {self._format_text_with_null_check(npc.name_fr)}
                            </div>
                            <div class="lang-row">
                                <span class="lang-label">EN:</span> {self._format_text_with_null_check(npc.name_en)}
                            </div>
                            <div class="lang-row">
                                <span class="lang-label">ES:</span> {self._format_text_with_null_check(npc.name_es)}
                            </div>
                            <div class="lang-row">
                                <span class="lang-label">PT:</span> {self._format_text_with_null_check(npc.name_pt)}
                            </div>
                        </div>
                        <div class="npc-metadata">
                            {gender_info}
                            {f'<div class="npc-context"><strong>Context:</strong> {npc.context}</div>' if npc.context else ''}
                        </div>
                    </div>
                </div>
                <div class="npc-image-container" style="display: none;">
                    <img class="npc-image" src="" alt="NPC Image" />
                </div>
            </div>
            <div class="dialogs collapsible-content">
                {dialogs_html}
            </div>
        </div>
        """
    
    def _generate_dialog_tree(self, message: NPCMessage, dialog: NPCDialog, level: int = 0, visited: Optional[set] = None) -> str:
        """Generate HTML for dialog tree with message-reply hierarchy"""
        if visited is None:
            visited = set()
        
        # Prevent infinite loops
        if message.message_id in visited:
            return f'<div class="message-ref">↺ Reference to Message ID: {message.message_id}</div>'
        
        visited.add(message.message_id)
        
        # Cap the indentation at a reasonable level to prevent overflow
        max_level = 15  # Maximum visual indentation level
        visual_level = min(level, max_level)
        indent_style = f"margin-left: {visual_level * 10}px;"  # Reduced from 20px to 10px
        
        # Add depth indicator for very deep levels
        depth_indicator = f" (Depth: {level})" if level > max_level else ""
        
        message_html = f"""
        <div class="message collapsible depth-{visual_level}" data-message-id="{message.message_id}" data-depth="{level}">
            <div class="message-header clickable-header">
                {f'<span class="depth-badge">Depth: {level}</span>' if level > max_level else ''}
                <strong>💬 Message ID: {message.message_id}</strong>
                <span class="collapse-indicator">−</span>
            </div>
            <div class="message-content collapsible-content">
                <div class="lang-row">
                    <span class="lang-label">FR:</span> {self._format_text_with_null_check(message.text_fr)}
                </div>
                <div class="lang-row">
                    <span class="lang-label">EN:</span> {self._format_text_with_null_check(message.text_en)}
                </div>
                <div class="lang-row">
                    <span class="lang-label">ES:</span> {self._format_text_with_null_check(message.text_es)}
                </div>
                <div class="lang-row">
                    <span class="lang-label">PT:</span> {self._format_text_with_null_check(message.text_pt)}
                </div>
            </div>
        """
        
        # Add replies within the collapsible content so they get hidden when message is collapsed
        if message.replies:
            message_html += '<div class="replies collapsible-nested">'
            for reply in message.replies:
                # Cap reply visual level as well
                reply_visual_level = min(level + 1, max_level)
                reply_indent_style = f"margin-left: {reply_visual_level * 10}px;"  # Reduced from 20px to 10px
                reply_depth_indicator = f" (Depth: {level + 1})" if level + 1 > max_level else ""
                
                reply_html = f"""
                <div class="reply collapsible-nested depth-{reply_visual_level}" data-reply-id="{reply.reply_id}" data-depth="{level + 1}">
                    <div class="reply-header">
                        {f'<span class="depth-badge">Depth: {level + 1}</span>' if level + 1 > max_level else ''}
                        <strong>↪️ Reply ID: {reply.reply_id}</strong>
                        {f' → Message ID: {reply.leads_to_message_id}' if reply.leads_to_message_id else ''}
                    </div>
                    <div class="reply-content">
                        <div class="lang-row">
                            <span class="lang-label">FR:</span> {self._format_text_with_null_check(reply.text_fr)}
                        </div>
                        <div class="lang-row">
                            <span class="lang-label">EN:</span> {self._format_text_with_null_check(reply.text_en)}
                        </div>
                        <div class="lang-row">
                            <span class="lang-label">ES:</span> {self._format_text_with_null_check(reply.text_es)}
                        </div>
                        <div class="lang-row">
                            <span class="lang-label">PT:</span> {self._format_text_with_null_check(reply.text_pt)}
                        </div>
                    </div>
                """
                
                # If reply leads to another message, recursively add it within the reply
                if reply.leads_to_message_id and reply.leads_to_message_id in self.messages:
                    next_message = self.messages[reply.leads_to_message_id]
                    reply_html += '<div class="nested-messages">'
                    reply_html += self._generate_dialog_tree(next_message, dialog, level + 2, visited.copy())
                    reply_html += '</div>'
                
                reply_html += '</div>'
                message_html += reply_html
            
            message_html += '</div>'
        
        message_html += '</div>'
        visited.remove(message.message_id)
        
        return message_html
    
    def _get_css_styles(self) -> str:
        """Get CSS styles for the HTML"""
        return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        /* Consolas font override */
        body.consolas-font {
            font-family: 'Consolas', 'Courier New', monospace;
        }
        
        body.consolas-font * {
            font-family: 'Consolas', 'Courier New', monospace;
        }
        
        header {
            background: white;
            padding: 20px;
            border-radius: 15px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        h1 {
            color: #333;
            margin-bottom: 20px;
            text-align: center;
        }
        
        .disclaimer {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 20px;
            text-align: center;
            color: #856404;
        }
        
        .search-container {
            max-width: 800px;
            margin: 0 auto 20px;
        }
        
        .search-box input {
            width: 100%;
            padding: 12px;
            font-size: 16px;
            border: 2px solid #ddd;
            border-radius: 8px;
            margin-bottom: 15px;
        }
        
        .search-options-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 15px;
        }
        
        .option-group {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }
        
        .option-group h4 {
            margin: 0 0 15px 0;
            color: #495057;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        /* Table-like layout for options */
        .option-table {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .option-row {
            display: flex;
            align-items: flex-start;
            padding: 8px 12px;
            border-radius: 4px;
            transition: background-color 0.2s ease;
            cursor: pointer;
            margin: 0;
            min-height: 32px;
            gap: 8px;
        }
        
        .option-row:hover {
            background-color: rgba(76, 175, 80, 0.1);
        }
        
        .option-row input[type="radio"],
        .option-row input[type="checkbox"] {
            margin: 0;
            flex-shrink: 0;
            width: 16px;
            height: 16px;
            margin-top: 2px;
        }
        
        .option-text {
            flex: 1;
            font-size: 14px;
            line-height: 1.4;
            word-wrap: break-word;
            overflow-wrap: break-word;
            max-width: calc(100% - 32px);
        }
        
        .language-highlight-row {
            justify-content: space-between;
            align-items: center;
        }
        
        .language-highlight-row .option-text {
            flex: 0 0 auto;
            margin-right: 10px;
        }
        
        .search-controls {
            text-align: center;
            margin-top: 15px;
            display: flex;
            gap: 10px;
            justify-content: center;
            flex-wrap: wrap;
        }
        
        .apply-btn, .control-btn {
            background: linear-gradient(135deg, #4CAF50, #45a049);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s ease;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        
        .control-btn {
            background: linear-gradient(135deg, #2196F3, #1976D2);
            padding: 10px 16px;
            font-size: 14px;
        }
        
        .apply-btn:hover, .control-btn:hover {
            transform: translateY(-1px);
        }
        
        .apply-btn.changed {
            background: linear-gradient(135deg, #ff9800, #f57c00);
        }
        
        .apply-btn.changed .btn-icon {
            display: inline !important;
        }
        
        .stats {
            text-align: center;
            color: #666;
            font-size: 14px;
        }
        
        .stats span {
            margin: 0 15px;
        }
        
        .npc-card {
            background: white;
            border-radius: 15px;
            margin-bottom: 20px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s;
        }
        
        .npc-card:hover {
            transform: translateY(-2px);
        }
        
        .npc-header {
            background: linear-gradient(135deg, #ff6b6b, #ffa726);
            color: white;
            padding: 20px;
        }
        
        .npc-header h2 {
            margin-bottom: 15px;
        }
        
        .npc-id {
            font-size: 0.8em;
            opacity: 0.9;
        }
        
        .npc-names {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
            margin-bottom: 10px;
        }
        
        .npc-info-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 20px;
            margin-bottom: 10px;
        }
        
        .npc-metadata {
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        
        .npc-gender {
            background: rgba(255,255,255,0.2);
            padding: 8px;
            border-radius: 6px;
            font-size: 0.9em;
        }
        
        .gender-male {
            background: #b3d9ff;
            color: #0066cc;
            padding: 2px 8px;
            border-radius: 12px;
            font-weight: bold;
            font-size: 0.85em;
        }
        
        .gender-female {
            background: #ffb3d9;
            color: #cc0066;
            padding: 2px 8px;
            border-radius: 12px;
            font-weight: bold;
            font-size: 0.85em;
        }
        
        .gender-undefined {
            background: #e6e6e6;
            color: #666666;
            padding: 2px 8px;
            border-radius: 12px;
            font-weight: bold;
            font-size: 0.85em;
        }
        
        .search-highlight {
            background-color: #ffff99;
            padding: 1px 2px;
            border-radius: 2px;
        }
        
        .current-match {
            outline: 3px solid #ff6b6b;
            outline-offset: 2px;
            border-radius: 8px;
        }
        
        .npc-image-container {
            position: absolute;
            top: 10px;
            right: 10px;
            z-index: 10;
            background: rgba(255,255,255,0.9);
            border-radius: 8px;
            padding: 5px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        }
        
        .npc-image {
            width: 150px;
            height: 150px;
            border-radius: 4px;
        }
        
        .npc-header {
            position: relative;
        }
        
        .sticky-header {
            position: sticky;
            top: 0;
            z-index: 100;
            background: linear-gradient(135deg, #ff6b6b, #ffa726);
            border-radius: 15px 15px 0 0;
        }
        
        .clickable-header {
            cursor: pointer;
            user-select: none;
            transition: opacity 0.2s ease;
        }
        
        .clickable-header:hover {
            opacity: 0.8;
        }
        
        .collapse-indicator {
            float: right;
            font-weight: bold;
            transition: transform 0.3s ease;
        }
        
        .collapsible.collapsed .collapse-indicator {
            transform: rotate(-90deg);
        }
        
        .collapsible.collapsed .collapsible-content {
            display: none !important;
        }
        
        .collapsible.collapsed .collapsible-nested {
            display: none !important;
        }
        
        .collapsible.collapsed .nested-messages {
            display: none !important;
        }
        
        .collapsible-content {
            transition: all 0.3s ease;
        }
        
        .collapsible-nested {
            transition: all 0.3s ease;
        }
        
        .nested-messages {
            transition: all 0.3s ease;
        }
        
        #floatingNav {
            position: fixed;
            top: 50%;
            right: 20px;
            transform: translateY(-50%);
            background: rgba(0, 0, 0, 0.8);
            color: white;
            padding: 10px;
            border-radius: 25px;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 5px;
            z-index: 1000;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        }
        
        .nav-btn {
            background: #4CAF50;
            color: white;
            border: none;
            width: 40px;
            height: 40px;
            border-radius: 50%;
            cursor: pointer;
            font-size: 18px;
            transition: all 0.3s ease;
        }
        
        .nav-btn:hover {
            background: #45a049;
            transform: scale(1.1);
        }
        
        .nav-btn:disabled {
            background: #666;
            cursor: not-allowed;
            transform: none;
        }
        
        #matchCounter {
            font-size: 12px;
            font-weight: bold;
            color: white;
            text-align: center;
            min-width: 40px;
        }
        
        .lang-row {
            display: flex;
            align-items: center;
            gap: 8px;
            margin: 5px 0;
        }
        
        .lang-row.lang-hidden {
            display: none !important;
        }
        
        .lang-label {
            font-weight: bold;
            min-width: 30px;
            background: rgba(255,255,255,0.2);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.8em;
        }
        
        .npc-context {
            background: rgba(255,255,255,0.1);
            padding: 10px;
            border-radius: 8px;
            font-size: 0.9em;
        }
        
        .dialogs {
            padding: 20px;
        }
        
        .dialog-container {
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            margin: 15px 0;
            overflow: visible;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }
        
        .dialog-header {
            background: #f5f5f5;
            padding: 10px 15px;
            border-bottom: 1px solid #e0e0e0;
            font-weight: bold;
            color: #333;
        }
        
        .message {
            border-left: 4px solid #4CAF50;
            margin: 10px 0;
            padding: 15px;
            background: #f9f9f9;
            border-radius: 0 8px 8px 0;
            max-width: none; /* Allow unlimited width for deep nesting */
            overflow: visible;
            word-wrap: break-word;
            overflow-wrap: break-word;
            box-sizing: border-box;
            position: relative;
            width: fit-content; /* Size to content */
            min-width: 300px; /* Minimum readable width */
        }
        
        /* Add visual indicators for very deep nesting */
        .message[data-depth="16"], .reply[data-depth="17"] { border-left-color: #ff5722; }
        .message[data-depth="17"], .reply[data-depth="18"] { border-left-color: #e91e63; }
        .message[data-depth="18"], .reply[data-depth="19"] { border-left-color: #9c27b0; }
        .message[data-depth="19"], .reply[data-depth="20"] { border-left-color: #673ab7; }
        
        /* Add depth indicator badge for very deep levels */
        
        .message-header {
            color: #2E7D32;
            margin-bottom: 10px;
            font-size: 0.9em;
        }
        
        .message-content {
            background: white;
            padding: 10px;
            border-radius: 6px;
            max-width: none; /* Allow unlimited width */
            overflow: visible;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }
        
        .reply {
            border-left: 4px solid #2196F3;
            margin: 10px 0;
            padding: 15px;
            background: #e3f2fd;
            border-radius: 0 8px 8px 0;
            max-width: none; /* Allow unlimited width for deep nesting */
            overflow: visible;
            word-wrap: break-word;
            overflow-wrap: break-word;
            box-sizing: border-box;
            position: relative;
            width: fit-content; /* Size to content */
            min-width: 300px; /* Minimum readable width */
        }
        
        .reply-header {
            color: #1976D2;
            margin-bottom: 10px;
            font-size: 0.9em;
        }
        
        .reply-content {
            background: white;
            padding: 10px;
            border-radius: 6px;
            max-width: none; /* Allow unlimited width */
            overflow: visible;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }
        
        .null-value {
            color: #d32f2f;
            background: #ffebee;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 0.9em;
        }
        
        .message-ref {
            color: #FF9800;
            font-style: italic;
            padding: 5px;
            background: #FFF3E0;
            border-radius: 4px;
            margin: 5px 0;
        }
        
        .hidden {
            display: none !important;
        }
        
        /* Dialog container styles for better deep nesting support */
        .dialogs {
            padding: 20px;
            overflow-x: auto;
            max-width: none; /* Allow unlimited width for deep nesting */
            min-width: 100%;
            /* Add scrollbar styling for better visibility */
            scrollbar-width: thin;
            scrollbar-color: #888 #f1f1f1;
        }
        
        /* Webkit scrollbar styling */
        .dialogs::-webkit-scrollbar {
            height: 8px;
        }
        
        .dialogs::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 4px;
        }
        
        .dialogs::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 4px;
        }
        
        .dialogs::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        
        .dialog-container {
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            margin: 15px 0;
            overflow-x: auto;
            word-wrap: break-word;
            overflow-wrap: break-word;
            min-width: 0; /* Allows container to shrink */
            max-width: none; /* Allow unlimited width */
            width: fit-content; /* Size to content */
        }
        
        .dialog-header {
            background: #f5f5f5;
            padding: 10px 15px;
            border-bottom: 1px solid #e0e0e0;
            font-weight: bold;
            color: #333;
            position: sticky;
            left: 0;
            z-index: 2;
        }
        
        /* Ensure nested content can scroll horizontally */
        .message, .reply {
            min-width: 0; /* Allows containers to shrink */
        }
        
        /* Add visual guide for very deep nesting */
        .message[data-depth^="1"]:not([data-depth="1"]),
        .reply[data-depth^="1"]:not([data-depth="1"]) {
            border-left-width: 6px;
        }
        
        .message[data-depth^="2"],
        .reply[data-depth^="2"] {
            border-left-width: 8px;
            box-shadow: 2px 0 4px rgba(0,0,0,0.1);
        }
        
        /* Depth badge styling - positioned on the left */
        .depth-badge {
            background: #ff5722;
            color: white;
            padding: 2px 6px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: bold;
            margin-right: 8px;
            display: inline-block;
        }
        
        .reply .depth-badge {
            background: #e91e63;
        }
        
        /* Improved scrolling for very wide content */
        .message-content, .reply-content {
            overflow-x: auto;
            max-width: none; /* Allow unlimited width */
        }
        
        /* Depth-based indentation classes - more precise control */
        .depth-0 { margin-left: 0px; }
        .depth-1 { margin-left: 10px; }
        .depth-2 { margin-left: 20px; }
        .depth-3 { margin-left: 30px; }
        .depth-4 { margin-left: 40px; }
        .depth-5 { margin-left: 50px; }
        .depth-6 { margin-left: 60px; }
        .depth-7 { margin-left: 70px; }
        .depth-8 { margin-left: 80px; }
        .depth-9 { margin-left: 90px; }
        .depth-10 { margin-left: 100px; }
        .depth-11 { margin-left: 110px; }
        .depth-12 { margin-left: 120px; }
        .depth-13 { margin-left: 130px; }
        .depth-14 { margin-left: 140px; }
        .depth-15 { margin-left: 150px; } /* Max visual depth */
        
        /* Header improvements with help button */
        header {
            position: relative;
        }
        
        .help-btn {
            position: absolute;
            top: 20px;
            right: 20px;
            background: linear-gradient(135deg, #4CAF50, #45a049);
            color: white;
            border: none;
            width: 40px;
            height: 40px;
            border-radius: 50%;
            font-size: 18px;
            cursor: pointer;
            transition: all 0.3s ease;
            z-index: 10;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        
        .help-btn:hover {
            background: linear-gradient(135deg, #45a049, #4CAF50);
            transform: scale(1.1);
            box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        }
        
        /* Improved label alignment */
        .option-group label {
            display: flex;
            align-items: center;
            margin: 8px 0;
            cursor: pointer;
            font-size: 14px;
            line-height: 1.4;
        }
        
        .option-group label input[type="radio"],
        .option-group label input[type="checkbox"] {
            margin-right: 8px;
            margin-top: 0;
            flex-shrink: 0;
        }
        
        /* Language dropdown styling */
        .language-dropdown {
            margin-left: 8px;
            padding: 4px 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
            font-size: 14px;
            cursor: pointer;
        }
        
        .language-dropdown:focus {
            outline: none;
            border-color: #4CAF50;
            box-shadow: 0 0 0 2px rgba(76, 175, 80, 0.2);
        }
        
        /* Language highlighting */
        .lang-row.highlighted-fr {
            border: 2px dotted #3498db;
            border-radius: 6px;
            background-color: rgba(52, 152, 219, 0.05);
            padding: 4px;
            margin: 2px 0;
        }
        
        .lang-row.highlighted-en {
            border: 2px dotted #e74c3c;
            border-radius: 6px;
            background-color: rgba(231, 76, 60, 0.05);
            padding: 4px;
            margin: 2px 0;
        }
        
        .lang-row.highlighted-es {
            border: 2px dotted #f39c12;
            border-radius: 6px;
            background-color: rgba(243, 156, 18, 0.05);
            padding: 4px;
            margin: 2px 0;
        }
        
        .lang-row.highlighted-pt {
            border: 2px dotted #9b59b6;
            border-radius: 6px;
            background-color: rgba(155, 89, 182, 0.05);
            padding: 4px;
            margin: 2px 0;
        }
        
        /* Side Menu styling */
        .side-menu {
            position: fixed;
            top: 0;
            left: -70%;
            width: 70%;
            height: 100vh;
            background: white;
            box-shadow: 4px 0 20px rgba(0,0,0,0.3);
            z-index: 1001;
            transition: left 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);
            overflow-y: auto;
            display: flex;
            flex-direction: column;
        }
        
        .side-menu.show {
            left: 0;
        }
        
        .side-menu-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        
        .side-menu-header h2 {
            margin: 0;
            font-size: 24px;
        }
        
        .close-menu-btn {
            background: rgba(255,255,255,0.2);
            border: none;
            color: white;
            width: 40px;
            height: 40px;
            border-radius: 50%;
            font-size: 18px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        
        .close-menu-btn:hover {
            background: rgba(255,255,255,0.3);
            transform: scale(1.1);
        }
        
        .side-menu-content {
            padding: 30px;
            flex: 1;
            overflow-y: auto;
        }
        
        .help-section {
            margin-bottom: 30px;
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid #4CAF50;
        }
        
        .help-section h3 {
            margin: 0 0 15px 0;
            color: #333;
            font-size: 18px;
        }
        
        .help-section ul {
            margin: 0;
            padding-left: 20px;
        }
        
        .help-section li {
            margin-bottom: 10px;
            line-height: 1.6;
        }
        
        .help-section li strong {
            color: #2c3e50;
        }
        
        /* Side menu overlay */
        .side-menu-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.5);
            z-index: 999;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
        }
        
        .side-menu-overlay.show {
            opacity: 1;
            visibility: visible;
        }
        
        /* Menu toggle button */
        .menu-toggle {
            position: fixed;
            top: 20px;
            right: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border: none;
            color: white;
            width: 50px;
            height: 50px;
            border-radius: 50%;
            font-size: 20px;
            cursor: pointer;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .menu-toggle:hover {
            transform: scale(1.1);
            box-shadow: 0 6px 16px rgba(0,0,0,0.3);
        }
        
        @media (max-width: 768px) {
            body {
                padding: 10px;
            }
            
            .npc-names {
                grid-template-columns: 1fr;
            }
            
            .search-options {
                flex-direction: column;
                align-items: center;
            }
            
            .side-menu {
                width: 90%;
                left: -90%;
            }
            
            .side-menu-content {
                padding: 20px;
            }
            
            .menu-toggle {
                top: 15px;
                right: 15px;
                width: 40px;
                height: 40px;
                font-size: 16px;
            }
        }
        """
    
    def _get_javascript(self) -> str:
        """Get JavaScript for search functionality"""
        return """
        document.addEventListener('DOMContentLoaded', function() {
            const searchInput = document.getElementById('searchInput');
            const searchTypeRadios = document.querySelectorAll('input[name="searchType"]');
            const showNullOnlyCheckbox = document.getElementById('showNullOnly');
            const enableImagesCheckbox = document.getElementById('enableImages');
            const enableConsolasCheckbox = document.getElementById('enableConsolas');
            const exactMatchCheckbox = document.getElementById('exactMatch');
            const ignoreDiacriticsCheckbox = document.getElementById('ignoreDiacritics');
            const useWildcardsCheckbox = document.getElementById('useWildcards');
            const showOnlyMatchDialogsCheckbox = document.getElementById('showOnlyMatchDialogs');
            const showOnlyMatchMsgReplyCheckbox = document.getElementById('showOnlyMatchMsgReply');
            const searchLangCheckboxes = document.querySelectorAll('input[name="searchLang"]');
            const displayLangCheckboxes = document.querySelectorAll('input[name="displayLang"]');
            const applyFiltersBtn = document.getElementById('applyFiltersBtn');
            const collapseAllBtn = document.getElementById('collapseAllBtn');
            const expandAllBtn = document.getElementById('expandAllBtn');
            const floatingNav = document.getElementById('floatingNav');
            const prevMatchBtn = document.getElementById('prevMatchBtn');
            const nextMatchBtn = document.getElementById('nextMatchBtn');
            const matchCounter = document.getElementById('matchCounter');
            const npcCards = document.querySelectorAll('.npc-card');
            const highlightLanguageDropdown = document.getElementById('highlightLanguage');
            const menuToggle = document.getElementById('menuToggle');
            const sideMenu = document.getElementById('sideMenu');
            const closeSideMenu = document.getElementById('closeSideMenu');
            const sideMenuOverlay = document.getElementById('sideMenuOverlay');
            
            let currentSearchState = {
                term: '',
                type: 'text',
                nullOnly: false,
                imagesEnabled: false,
                exactMatch: false,
                ignoreDiacritics: false,
                useWildcards: false,
                showOnlyMatchDialogs: false,
                showOnlyMatchMsgReply: false,
                searchLangs: ['fr', 'en', 'es', 'pt'],
                displayLangs: ['fr', 'en', 'es', 'pt']
            };
            
            let searchMatches = [];
            let currentMatchIndex = -1;
            
            // Diacritic removal function
            function removeDiacritics(str) {
                return str.normalize('NFD').replace(/[\\u0300-\\u036f]/g, '');
            }
            
            // Wildcard to regex conversion
            function wildcardToRegex(pattern) {
                const escaped = pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
                return escaped.replace(/\\\\\\*/g, '[^\\\\s]*');
            }
            
            // Text highlighting function
            function highlightText(text, searchTerm, ignoreDiacritics, useWildcards, exactMatch) {
                if (!searchTerm || !text) return text;
                
                let searchText = text;
                let pattern = searchTerm;
                
                if (ignoreDiacritics) {
                    searchText = removeDiacritics(text);
                    pattern = removeDiacritics(searchTerm);
                }
                
                let regex;
                if (exactMatch) {
                    regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'gi');
                } else if (useWildcards && pattern.includes('*')) {
                    regex = new RegExp(wildcardToRegex(pattern), 'gi');
                } else {
                    regex = new RegExp(pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&'), 'gi');
                }
                
                try {
                    return text.replace(regex, '<span class="search-highlight">$&</span>');
                } catch (e) {
                    return text;
                }
            }
            
            // Collapse/Expand functionality
            function toggleCollapse(element) {
                element.classList.toggle('collapsed');
                
                // When expanding a message, also expand all nested content
                if (!element.classList.contains('collapsed') && element.classList.contains('message')) {
                    // Expand all nested messages and replies within this message
                    const nestedElements = element.querySelectorAll('.collapsible');
                    nestedElements.forEach(nested => {
                        nested.classList.remove('collapsed');
                    });
                }
            }
            
            function collapseAll() {
                document.querySelectorAll('.collapsible').forEach(el => {
                    el.classList.add('collapsed');
                });
            }
            
            function expandAll() {
                document.querySelectorAll('.collapsible').forEach(el => {
                    el.classList.remove('collapsed');
                });
            }
            
            // Add click handlers for collapsible elements
            function setupCollapsibleHandlers() {
                document.querySelectorAll('.clickable-header').forEach(header => {
                    header.addEventListener('click', function(e) {
                        e.stopPropagation();
                        const collapsible = this.closest('.collapsible');
                        if (collapsible) {
                            toggleCollapse(collapsible);
                        }
                    });
                });
            }
            
            // Track changes to show the update icon
            function updateButtonState() {
                const currentState = {
                    term: searchInput.value.toLowerCase().trim(),
                    type: document.querySelector('input[name="searchType"]:checked').value,
                    nullOnly: showNullOnlyCheckbox.checked,
                    imagesEnabled: enableImagesCheckbox.checked,
                    exactMatch: exactMatchCheckbox.checked,
                    ignoreDiacritics: ignoreDiacriticsCheckbox.checked,
                    useWildcards: useWildcardsCheckbox.checked,
                    searchLangs: Array.from(searchLangCheckboxes).filter(cb => cb.checked).map(cb => cb.value),
                    displayLangs: Array.from(displayLangCheckboxes).filter(cb => cb.checked).map(cb => cb.value)
                };
                
                const hasChanges = JSON.stringify(currentState) !== JSON.stringify(currentSearchState);
                applyFiltersBtn.classList.toggle('changed', hasChanges);
            }
            
            // Update language display visibility
            function updateLanguageDisplay() {
                const displayLangs = Array.from(displayLangCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
                
                document.querySelectorAll('.lang-row').forEach(row => {
                    const langLabel = row.querySelector('.lang-label');
                    if (langLabel) {
                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                        row.classList.toggle('lang-hidden', !displayLangs.includes(lang));
                    }
                });
            }
            
            // Update language highlighting
            function updateLanguageHighlighting() {
                const selectedLang = highlightLanguageDropdown.value;
                
                // Remove all existing highlighting
                document.querySelectorAll('.lang-row').forEach(row => {
                    row.classList.remove('highlighted-fr', 'highlighted-en', 'highlighted-es', 'highlighted-pt');
                });
                
                // Add highlighting for selected language
                if (selectedLang) {
                    document.querySelectorAll('.lang-row').forEach(row => {
                        const langLabel = row.querySelector('.lang-label');
                        if (langLabel) {
                            const lang = langLabel.textContent.toLowerCase().replace(':', '');
                            if (lang === selectedLang) {
                                row.classList.add(`highlighted-${selectedLang}`);
                            }
                        }
                    });
                }
            }
            
            // Toggle Consolas font
            function toggleConsolasFont() {
                const body = document.body;
                if (enableConsolasCheckbox.checked) {
                    body.classList.add('consolas-font');
                } else {
                    body.classList.remove('consolas-font');
                }
            }
            
            // Side menu functionality
            function toggleSideMenu() {
                const isVisible = sideMenu.classList.contains('show');
                if (isVisible) {
                    sideMenu.classList.remove('show');
                    sideMenuOverlay.classList.remove('show');
                    document.body.style.overflow = '';
                } else {
                    sideMenu.classList.add('show');
                    sideMenuOverlay.classList.add('show');
                    document.body.style.overflow = 'hidden';
                }
            }
            
            // Image loading functionality
            function setupImageHandlers() {
                if (!enableImagesCheckbox.checked) {
                    document.querySelectorAll('.npc-image-container').forEach(container => {
                        container.style.display = 'none';
                    });
                    return;
                }
                
                npcCards.forEach(card => {
                    const imgUrls = card.dataset.imgUrls;
                    if (!imgUrls) return;
                    
                    const urls = imgUrls.split('|');
                    const imageContainer = card.querySelector('.npc-image-container');
                    const image = card.querySelector('.npc-image');
                    
                    if (imageContainer && image) {
                        card.addEventListener('mouseenter', function() {
                            if (enableImagesCheckbox.checked && urls[0]) {
                                image.src = urls[0];
                                imageContainer.style.display = 'block';
                            }
                        });
                        
                        card.addEventListener('mouseleave', function() {
                            imageContainer.style.display = 'none';
                        });
                    }
                });
            }
            
            // Search in NPC names function
            function searchInNPCNames(card, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch) {
                const npcNames = card.querySelectorAll('.npc-names .lang-row');
                for (const nameRow of npcNames) {
                    const langLabel = nameRow.querySelector('.lang-label');
                    if (langLabel) {
                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                        if (searchLangs.includes(lang)) {
                            let textContent = nameRow.textContent.toLowerCase();
                            let pattern = searchTerm;
                            
                            if (ignoreDiacritics) {
                                textContent = removeDiacritics(textContent);
                                pattern = removeDiacritics(searchTerm);
                            }
                            
                            let matches = false;
                            if (exactMatch) {
                                const regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'i');
                                matches = regex.test(textContent);
                            } else if (useWildcards && pattern.includes('*')) {
                                const regex = new RegExp(wildcardToRegex(pattern), 'i');
                                matches = regex.test(textContent);
                            } else {
                                matches = textContent.includes(pattern);
                            }
                            
                            if (matches) return true;
                        }
                    }
                }
                return false;
            }
            
            // Check if a dialog tree contains matches
            function dialogTreeContainsMatch(dialogContainer, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch) {
                if (!searchTerm) return false;
                
                // Search in all text within the dialog container
                const langRows = dialogContainer.querySelectorAll('.lang-row');
                for (const row of langRows) {
                    const langLabel = row.querySelector('.lang-label');
                    if (langLabel) {
                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                        if (searchLangs.includes(lang)) {
                            let textContent = row.textContent.toLowerCase();
                            let pattern = searchTerm;
                            
                            if (ignoreDiacritics) {
                                textContent = removeDiacritics(textContent);
                                pattern = removeDiacritics(searchTerm);
                            }
                            
                            let matches = false;
                            if (exactMatch) {
                                const regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'i');
                                matches = regex.test(textContent);
                            } else if (useWildcards && pattern.includes('*')) {
                                const regex = new RegExp(wildcardToRegex(pattern), 'i');
                                matches = regex.test(textContent);
                            } else {
                                matches = textContent.includes(pattern);
                            }
                            
                            if (matches) return true;
                        }
                    }
                }
                
                return false;
            }
            
            // Text search function with language filtering
            function searchInText(card, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch) {
                // Search in NPC names
                if (searchInNPCNames(card, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch)) {
                    return true;
                }
                
                // Search in messages and replies
                const langRows = card.querySelectorAll('.message-content .lang-row, .reply-content .lang-row');
                for (const row of langRows) {
                    const langLabel = row.querySelector('.lang-label');
                    if (langLabel) {
                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                        if (searchLangs.includes(lang)) {
                            let textContent = row.textContent.toLowerCase();
                            let pattern = searchTerm;
                            
                            if (ignoreDiacritics) {
                                textContent = removeDiacritics(textContent);
                                pattern = removeDiacritics(searchTerm);
                            }
                            
                            let matches = false;
                            if (exactMatch) {
                                const regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'i');
                                matches = regex.test(textContent);
                            } else if (useWildcards && pattern.includes('*')) {
                                const regex = new RegExp(wildcardToRegex(pattern), 'i');
                                matches = regex.test(textContent);
                            } else {
                                matches = textContent.includes(pattern);
                            }
                            
                            if (matches) return true;
                        }
                    }
                }
                
                return false;
            }
            
            // Navigation functions
            function updateNavigationButtons() {
                prevMatchBtn.disabled = currentMatchIndex <= 0;
                nextMatchBtn.disabled = currentMatchIndex >= searchMatches.length - 1;
                
                if (searchMatches.length > 0) {
                    matchCounter.textContent = `${currentMatchIndex + 1}/${searchMatches.length}`;
                    floatingNav.style.display = 'flex';
                } else {
                    matchCounter.textContent = '0/0';
                    floatingNav.style.display = 'none';
                }
            }
            
            function scrollToMatch(index) {
                if (index >= 0 && index < searchMatches.length) {
                    currentMatchIndex = index;
                    const element = searchMatches[index];
                    element.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    
                    // Highlight current match
                    document.querySelectorAll('.current-match').forEach(el => {
                        el.classList.remove('current-match');
                    });
                    element.classList.add('current-match');
                    
                    updateNavigationButtons();
                }
            }
            
            function performSearch() {
                const searchTerm = searchInput.value.toLowerCase().trim();
                const searchType = document.querySelector('input[name="searchType"]:checked').value;
                const showNullOnly = showNullOnlyCheckbox.checked;
                const exactMatch = exactMatchCheckbox.checked;
                const ignoreDiacritics = ignoreDiacriticsCheckbox.checked;
                const useWildcards = useWildcardsCheckbox.checked;
                const showOnlyMatchDialogs = showOnlyMatchDialogsCheckbox.checked;
                const showOnlyMatchMsgReply = showOnlyMatchMsgReplyCheckbox.checked;
                const searchLangs = Array.from(searchLangCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
                
                // Update current state
                currentSearchState = {
                    term: searchTerm,
                    type: searchType,
                    nullOnly: showNullOnly,
                    imagesEnabled: enableImagesCheckbox.checked,
                    exactMatch: exactMatch,
                    ignoreDiacritics: ignoreDiacritics,
                    useWildcards: useWildcards,
                    showOnlyMatchDialogs: showOnlyMatchDialogs,
                    showOnlyMatchMsgReply: showOnlyMatchMsgReply,
                    searchLangs: searchLangs,
                    displayLangs: Array.from(displayLangCheckboxes).filter(cb => cb.checked).map(cb => cb.value)
                };
                
                // Clear previous highlights and matches
                document.querySelectorAll('.search-highlight').forEach(highlight => {
                    highlight.outerHTML = highlight.innerHTML;
                });
                document.querySelectorAll('.current-match').forEach(el => {
                    el.classList.remove('current-match');
                });
                searchMatches = [];
                currentMatchIndex = -1;
                
                npcCards.forEach(card => {
                    let shouldShow = false;
                    
                    if (showNullOnly) {
                        shouldShow = card.innerHTML.includes('null-value');
                    } else if (searchTerm === '') {
                        shouldShow = true;
                    } else {
                        if (searchType === 'npc-id') {
                            const npcId = card.dataset.npcId;
                            shouldShow = npcId === searchTerm;
                        } else if (searchType === 'message-id') {
                            const messageIds = Array.from(card.querySelectorAll('[data-message-id]')).map(el => el.dataset.messageId);
                            shouldShow = messageIds.includes(searchTerm);
                        } else if (searchType === 'reply-id') {
                            const replyIds = Array.from(card.querySelectorAll('[data-reply-id]')).map(el => el.dataset.replyId);
                            shouldShow = replyIds.includes(searchTerm);
                        } else if (searchType === 'npc-name') {
                            shouldShow = searchInNPCNames(card, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch);
                        } else {
                            shouldShow = searchInText(card, searchTerm, searchLangs, ignoreDiacritics, useWildcards, exactMatch);
                        }
                    }
                    
                    card.classList.toggle('hidden', !shouldShow);
                    
                    // Apply dialog tree filtering if enabled and card is shown
                    if (shouldShow && showOnlyMatchDialogs && searchTerm && 
                        (searchType === 'text' || searchType === 'npc-name')) {
                        
                        const dialogContainers = card.querySelectorAll('.dialog-container');
                        dialogContainers.forEach(dialogContainer => {
                            const hasMatch = dialogTreeContainsMatch(
                                dialogContainer, searchTerm, searchLangs, 
                                ignoreDiacritics, useWildcards, exactMatch
                            );
                            dialogContainer.classList.toggle('hidden', !hasMatch);
                        });
                    } else if (shouldShow && showOnlyMatchMsgReply && searchTerm && 
                        (searchType === 'text' || searchType === 'npc-name')) {
                        
                        // Show only matching messages and their direct replies, or matching replies and their parent messages
                        const dialogContainers = card.querySelectorAll('.dialog-container');
                        dialogContainers.forEach(dialogContainer => {
                            // Hide all messages and replies first
                            const messages = dialogContainer.querySelectorAll('.message');
                            const replies = dialogContainer.querySelectorAll('.reply');
                            
                            messages.forEach(msg => msg.classList.add('hidden'));
                            replies.forEach(reply => reply.classList.add('hidden'));
                            
                            let hasAnyMatch = false;
                            
                            // Check messages for matches and show them with direct replies
                            messages.forEach(message => {
                                const msgLangRows = message.querySelectorAll('.lang-row');
                                let messageHasMatch = false;
                                
                                for (const row of msgLangRows) {
                                    const langLabel = row.querySelector('.lang-label');
                                    if (langLabel) {
                                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                                        if (searchLangs.includes(lang)) {
                                            let textContent = row.textContent.toLowerCase();
                                            let pattern = searchTerm;
                                            
                                            if (ignoreDiacritics) {
                                                textContent = removeDiacritics(textContent);
                                                pattern = removeDiacritics(searchTerm);
                                            }
                                            
                                            let matches = false;
                                            if (exactMatch) {
                                                const regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'i');
                                                matches = regex.test(textContent);
                                            } else if (useWildcards && pattern.includes('*')) {
                                                const regex = new RegExp(wildcardToRegex(pattern), 'i');
                                                matches = regex.test(textContent);
                                            } else {
                                                matches = textContent.includes(pattern);
                                            }
                                            
                                            if (matches) {
                                                messageHasMatch = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                
                                if (messageHasMatch) {
                                    message.classList.remove('hidden');
                                    hasAnyMatch = true;
                                    
                                    // Show direct replies of this message
                                    const messageId = message.dataset.messageId;
                                    const directReplies = dialogContainer.querySelectorAll(`[data-reply-id]`);
                                    directReplies.forEach(reply => {
                                        const replyParent = reply.closest('.message');
                                        if (replyParent && replyParent.dataset.messageId === messageId) {
                                            reply.classList.remove('hidden');
                                        }
                                    });
                                }
                            });
                            
                            // Check replies for matches and show them with their parent messages
                            replies.forEach(reply => {
                                const replyLangRows = reply.querySelectorAll('.lang-row');
                                let replyHasMatch = false;
                                
                                for (const row of replyLangRows) {
                                    const langLabel = row.querySelector('.lang-label');
                                    if (langLabel) {
                                        const lang = langLabel.textContent.toLowerCase().replace(':', '');
                                        if (searchLangs.includes(lang)) {
                                            let textContent = row.textContent.toLowerCase();
                                            let pattern = searchTerm;
                                            
                                            if (ignoreDiacritics) {
                                                textContent = removeDiacritics(textContent);
                                                pattern = removeDiacritics(searchTerm);
                                            }
                                            
                                            let matches = false;
                                            if (exactMatch) {
                                                const regex = new RegExp(`\\\\b${pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\\\b`, 'i');
                                                matches = regex.test(textContent);
                                            } else if (useWildcards && pattern.includes('*')) {
                                                const regex = new RegExp(wildcardToRegex(pattern), 'i');
                                                matches = regex.test(textContent);
                                            } else {
                                                matches = textContent.includes(pattern);
                                            }
                                            
                                            if (matches) {
                                                replyHasMatch = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                
                                if (replyHasMatch) {
                                    reply.classList.remove('hidden');
                                    hasAnyMatch = true;
                                    
                                    // Show parent message of this reply
                                    const parentMessage = reply.closest('.message');
                                    if (parentMessage) {
                                        parentMessage.classList.remove('hidden');
                                    }
                                }
                            });
                            
                            // Hide the entire dialog container if no matches found
                            dialogContainer.classList.toggle('hidden', !hasAnyMatch);
                        });
                    } else if (shouldShow) {
                        // Show all dialog containers and their content if not filtering
                        const dialogContainers = card.querySelectorAll('.dialog-container');
                        dialogContainers.forEach(dialogContainer => {
                            dialogContainer.classList.remove('hidden');
                            // Show all messages and replies
                            const messages = dialogContainer.querySelectorAll('.message');
                            const replies = dialogContainer.querySelectorAll('.reply');
                            messages.forEach(msg => msg.classList.remove('hidden'));
                            replies.forEach(reply => reply.classList.remove('hidden'));
                        });
                    }
                    
                    if (shouldShow) {
                        searchMatches.push(card);
                        
                        // Apply highlighting if showing and text search
                        if ((searchType === 'text' || searchType === 'npc-name') && searchTerm) {
                            const langRows = card.querySelectorAll('.lang-row');
                            langRows.forEach(row => {
                                const langLabel = row.querySelector('.lang-label');
                                if (langLabel) {
                                    const lang = langLabel.textContent.toLowerCase().replace(':', '');
                                    if (searchLangs.includes(lang)) {
                                        const textNodes = Array.from(row.childNodes).filter(node => 
                                            node.nodeType === Node.TEXT_NODE || 
                                            (node.nodeType === Node.ELEMENT_NODE && !node.classList.contains('lang-label'))
                                        );
                                        
                                        textNodes.forEach(node => {
                                            if (node.nodeType === Node.TEXT_NODE) {
                                                const highlighted = highlightText(node.textContent, searchTerm, ignoreDiacritics, useWildcards, exactMatch);
                                                if (highlighted !== node.textContent) {
                                                    const span = document.createElement('span');
                                                    span.innerHTML = highlighted;
                                                    node.parentNode.replaceChild(span, node);
                                                }
                                            } else if (node.textContent) {
                                                const highlighted = highlightText(node.textContent, searchTerm, ignoreDiacritics, useWildcards, exactMatch);
                                                if (highlighted !== node.textContent) {
                                                    node.innerHTML = highlighted;
                                                }
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    }
                });
                
                // Update navigation
                updateNavigationButtons();
                
                // Focus on first match if any
                if (searchMatches.length > 0) {
                    scrollToMatch(0);
                }
                
                // Update language display
                updateLanguageDisplay();
                
                // Update button state after search
                updateButtonState();
                
                // Setup image handlers after search
                setupImageHandlers();
            }
            
            // Event listeners
            searchInput.addEventListener('input', updateButtonState);
            searchTypeRadios.forEach(radio => radio.addEventListener('change', updateButtonState));
            showNullOnlyCheckbox.addEventListener('change', updateButtonState);
            enableImagesCheckbox.addEventListener('change', function() {
                updateButtonState();
                setupImageHandlers();
            });
            enableConsolasCheckbox.addEventListener('change', toggleConsolasFont);
            exactMatchCheckbox.addEventListener('change', updateButtonState);
            ignoreDiacriticsCheckbox.addEventListener('change', updateButtonState);
            useWildcardsCheckbox.addEventListener('change', updateButtonState);
            showOnlyMatchDialogsCheckbox.addEventListener('change', updateButtonState);
            showOnlyMatchMsgReplyCheckbox.addEventListener('change', updateButtonState);
            searchLangCheckboxes.forEach(checkbox => checkbox.addEventListener('change', updateButtonState));
            displayLangCheckboxes.forEach(checkbox => checkbox.addEventListener('change', function() {
                updateButtonState();
                updateLanguageDisplay();
            }));
            
            // New feature event listeners
            highlightLanguageDropdown.addEventListener('change', function() {
                updateLanguageHighlighting();
                updateButtonState();
            });
            
            menuToggle.addEventListener('click', toggleSideMenu);
            closeSideMenu.addEventListener('click', toggleSideMenu);
            
            // Close side menu when clicking on overlay
            sideMenuOverlay.addEventListener('click', function(e) {
                if (e.target === sideMenuOverlay) {
                    toggleSideMenu();
                }
            });
            
            // Button handlers
            applyFiltersBtn.addEventListener('click', performSearch);
            collapseAllBtn.addEventListener('click', collapseAll);
            expandAllBtn.addEventListener('click', expandAll);
            
            // Navigation handlers
            prevMatchBtn.addEventListener('click', () => {
                if (currentMatchIndex > 0) {
                    scrollToMatch(currentMatchIndex - 1);
                }
            });
            
            nextMatchBtn.addEventListener('click', () => {
                if (currentMatchIndex < searchMatches.length - 1) {
                    scrollToMatch(currentMatchIndex + 1);
                }
            });
            
            // Allow Enter key to trigger search
            searchInput.addEventListener('keypress', function(e) {
                if (e.key === 'Enter') {
                    performSearch();
                }
            });
            
            // Initial setup
            updateLanguageDisplay();
            updateLanguageHighlighting();
            setupImageHandlers();
            setupCollapsibleHandlers();
            updateNavigationButtons();
        });
        """
    

class StatisticsWindow:
    """Statistics display window with proper tables"""
    
    def __init__(self, parent, stats: Dict[str, Any]):
        self.stats = stats
        self.window = tk.Toplevel(parent)
        self.window.title("Generation Statistics")
        self.window.geometry("1200x800")
        self.window.resizable(True, True)
        
        # Make window modal
        self.window.transient(parent)
        self.window.grab_set()
        
        self.setup_ui()
        
        # Center the window
        self.window.update_idletasks()
        width = self.window.winfo_width()
        height = self.window.winfo_height()
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f'{width}x{height}+{x}+{y}')
    
    def setup_ui(self):
        """Setup the statistics UI"""
        # Create main container with scrollable area
        main_canvas = tk.Canvas(self.window)
        scrollbar = ttk.Scrollbar(self.window, orient="vertical", command=main_canvas.yview)
        scrollable_main_frame = ttk.Frame(main_canvas)
        
        scrollable_main_frame.bind(
            "<Configure>",
            lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all"))
        )
        
        main_canvas.create_window((0, 0), window=scrollable_main_frame, anchor="nw")
        main_canvas.configure(yscrollcommand=scrollbar.set)
        
        # Title
        title_label = ttk.Label(scrollable_main_frame, text="📊 Comprehensive Statistics Report", 
                               font=("Arial", 14, "bold"))
        title_label.pack(pady=(10, 20))
        
        # Create horizontal layout for stats
        top_frame = ttk.Frame(scrollable_main_frame)
        top_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Left side - Basic Statistics and Metadata
        left_frame = ttk.Frame(top_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        self.create_basic_and_metadata_stats(left_frame)
        
        # Right side - XLIFF Mapping (if available)
        if 'xliff_mapping' in self.stats:
            right_frame = ttk.Frame(top_frame)
            right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
            self.create_xliff_mapping_section(right_frame)
        
        # Bottom - Translation Progress Summary
        bottom_frame = ttk.Frame(scrollable_main_frame)
        bottom_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.create_translation_summary(bottom_frame)
        
        # Button frame
        button_frame = ttk.Frame(self.window)
        button_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        ttk.Button(button_frame, text="Close", command=self.window.destroy).pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="Export to CSV", command=self.export_to_csv).pack(side=tk.RIGHT, padx=(0, 10))
        
        # Pack canvas and scrollbar
        main_canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Bind mousewheel to canvas
        def _on_mousewheel(event):
            main_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        main_canvas.bind_all("<MouseWheel>", _on_mousewheel)
    
    def create_basic_and_metadata_stats(self, parent):
        """Create basic statistics and metadata in a compact format"""
        # Basic Statistics
        basic_frame = ttk.LabelFrame(parent, text="� Basic Statistics", padding=10)
        basic_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 5))
        
        basic = self.stats['basic']
        metadata = self.stats['metadata']
        
        # Create a more compact table without scrollbars
        tree = ttk.Treeview(basic_frame, columns=("Value", "Percentage"), show="tree headings", height=14)
        tree.pack(fill=tk.BOTH, expand=True)
        
        tree.heading("#0", text="Metric")
        tree.heading("Value", text="Count")
        tree.heading("Percentage", text="Percentage")
        
        tree.column("#0", width=200)
        tree.column("Value", width=80, anchor=tk.CENTER)
        tree.column("Percentage", width=80, anchor=tk.CENTER)
        
        # Insert data
        tree.insert("", "end", text="Data Source", values=(basic['data_source'], ""))
        tree.insert("", "end", text="", values=("", ""))  # Separator
        
        tree.insert("", "end", text="📊 BASIC COUNTS", values=("", ""), tags=("header",))
        tree.insert("", "end", text="Total NPCs loaded", values=(basic['total_npcs'], "100.0%"))
        tree.insert("", "end", text="NPCs with dialogs", values=(basic['npcs_with_dialogs'], f"{(basic['npcs_with_dialogs']/basic['total_npcs']*100):.1f}%"))
        tree.insert("", "end", text="Total messages", values=(basic['total_messages'], ""))
        tree.insert("", "end", text="Total replies", values=(basic['total_replies'], ""))
        tree.insert("", "end", text="Total dialogs", values=(basic['total_dialogs'], ""))
        tree.insert("", "end", text="Total metadata entries", values=(basic['total_metadata'], ""))
        
        tree.insert("", "end", text="", values=("", ""))  # Separator
        tree.insert("", "end", text="🎭 METADATA MATCHING", values=("", ""), tags=("header",))
        tree.insert("", "end", text="NPCs matched with metadata", values=(metadata['npcs_with_metadata'], f"{metadata['metadata_match_percentage']:.1f}%"))
        tree.insert("", "end", text="NPCs with gender info", values=(metadata['npcs_with_gender'], f"{metadata['gender_match_percentage']:.1f}%"))
        tree.insert("", "end", text="NPCs with images", values=(metadata['npcs_with_images'], f"{metadata['images_match_percentage']:.1f}%"))
        
        # Configure tags
        tree.tag_configure("header", background="#e6f3ff", font=("Arial", 9, "bold"))
    
    def create_xliff_mapping_section(self, parent):
        """Create XLIFF mapping section"""
        xliff_stats = self.stats['xliff_mapping']
        
        frame = ttk.LabelFrame(parent, text="📄 XLIFF Translation Mapping", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, pady=(0, 5))
        
        # Create table
        tree = ttk.Treeview(frame, columns=("Total", "Mapped", "Unmapped", "Mapped_Pct"), show="tree headings", height=4)
        tree.pack(fill=tk.BOTH, expand=True)
        
        tree.heading("#0", text="Type")
        tree.heading("Total", text="Total in XLIFF")
        tree.heading("Mapped", text="Mapped to Dialogs")
        tree.heading("Unmapped", text="Unmapped")
        tree.heading("Mapped_Pct", text="Mapping %")
        
        tree.column("#0", width=100)
        for col in ["Total", "Mapped", "Unmapped", "Mapped_Pct"]:
            tree.column(col, width=90, anchor=tk.CENTER)
        
        # Insert data
        tree.insert("", "end", text="💬 Messages", values=(
            xliff_stats['total_xliff_messages'],
            xliff_stats['mapped_messages'],
            xliff_stats['unmapped_messages'],
            f"{xliff_stats['message_mapping_percentage']:.1f}%"
        ))
        
        tree.insert("", "end", text="↪️ Replies", values=(
            xliff_stats['total_xliff_replies'],
            xliff_stats['mapped_replies'],
            xliff_stats['unmapped_replies'],
            f"{xliff_stats['reply_mapping_percentage']:.1f}%"
        ))
        
        # Add explanation
        explanation = ttk.Label(frame, text=
            "Note: Unmapped entries are translations that don't appear\nin the dialog relationship mapping.",
            font=("Arial", 8), foreground="gray", justify=tk.CENTER)
        explanation.pack(pady=(10, 0))
    
    def create_translation_summary(self, parent):
        """Create translation quality summary table"""
        frame = ttk.LabelFrame(parent, text="🌐 Translation Progress Summary", padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Create a comprehensive table showing all translation types
        # Use a more compact format that fits in single lines
        tree = ttk.Treeview(frame, columns=("EN_Valid", "EN_Null", "EN_X", "ES_Valid", "ES_Null", "ES_X", "PT_Valid", "PT_Null", "PT_X"), 
                           show="tree headings", height=6)
        tree.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        tree.heading("#0", text="Type")
        tree.heading("EN_Valid", text="EN ✅")
        tree.heading("EN_Null", text="EN ❌")
        tree.heading("EN_X", text="EN ❔")
        tree.heading("ES_Valid", text="ES ✅")
        tree.heading("ES_Null", text="ES ❌")
        tree.heading("ES_X", text="ES ❔")
        tree.heading("PT_Valid", text="PT ✅")
        tree.heading("PT_Null", text="PT ❌")
        tree.heading("PT_X", text="PT ❔")
        
        tree.column("#0", width=120)
        for col in ["EN_Valid", "EN_Null", "EN_X", "ES_Valid", "ES_Null", "ES_X", "PT_Valid", "PT_Null", "PT_X"]:
            tree.column(col, width=70, anchor=tk.CENTER)
        
        # Insert data for each type
        translation_data = self.stats['translation_quality']
        
        for type_name, display_name in [('npc_names', '🧙‍♂️ NPC Names'), ('messages', '💬 Messages'), ('replies', '↪️ Replies')]:
            data = translation_data[type_name]
            tree.insert("", "end", text=display_name, values=(
                f"{data['en']['valid']} ({data['en']['valid_percentage']:.1f}%)",
                f"{data['en']['null']} ({data['en']['null_percentage']:.1f}%)",
                f"{data['en']['x']} ({data['en']['x_percentage']:.1f}%)",
                f"{data['es']['valid']} ({data['es']['valid_percentage']:.1f}%)",
                f"{data['es']['null']} ({data['es']['null_percentage']:.1f}%)",
                f"{data['es']['x']} ({data['es']['x_percentage']:.1f}%)",
                f"{data['pt']['valid']} ({data['pt']['valid_percentage']:.1f}%)",
                f"{data['pt']['null']} ({data['pt']['null_percentage']:.1f}%)",
                f"{data['pt']['x']} ({data['pt']['x_percentage']:.1f}%)"
            ), tags=(type_name,))
        
        # Configure row colors
        tree.tag_configure("npc_names", background="#f8f9fa")
        tree.tag_configure("messages", background="#e9ecef")
        tree.tag_configure("replies", background="#dee2e6")
        
        # Configure tag properties to increase row height if needed
        style = ttk.Style()
        style.configure("Treeview", rowheight=25)  # Increase row height slightly
        
        # Add legend
        legend_frame = ttk.Frame(frame)
        legend_frame.pack(fill=tk.X)
        
        ttk.Label(legend_frame, text="Legend:", font=("Arial", 9, "bold")).pack(side=tk.LEFT)
        ttk.Label(legend_frame, text="✅ Valid translations", foreground="green").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Label(legend_frame, text="❌ NULL/Empty", foreground="red").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Label(legend_frame, text="❔ Just 'x'", foreground="orange").pack(side=tk.LEFT, padx=(10, 0))
    
    def export_to_csv(self):
        """Export statistics to CSV file"""
        try:
            import csv
            from tkinter import filedialog
            
            file_path = filedialog.asksaveasfilename(
                title="Export Statistics to CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
            )
            
            if not file_path:
                return
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Basic statistics
                writer.writerow(["BASIC STATISTICS"])
                writer.writerow(["Metric", "Value", "Percentage"])
                basic = self.stats['basic']
                metadata = self.stats['metadata']
                
                writer.writerow(["Data Source", basic['data_source'], ""])
                writer.writerow(["Total NPCs loaded", basic['total_npcs'], "100.0%"])
                writer.writerow(["NPCs with dialogs", basic['npcs_with_dialogs'], f"{(basic['npcs_with_dialogs']/basic['total_npcs']*100):.1f}%"])
                writer.writerow(["Total messages", basic['total_messages'], ""])
                writer.writerow(["Total replies", basic['total_replies'], ""])
                writer.writerow(["Total dialogs", basic['total_dialogs'], ""])
                writer.writerow(["Total metadata entries", basic['total_metadata'], ""])
                writer.writerow(["NPCs matched with metadata", metadata['npcs_with_metadata'], f"{metadata['metadata_match_percentage']:.1f}%"])
                writer.writerow(["NPCs with gender info", metadata['npcs_with_gender'], f"{metadata['gender_match_percentage']:.1f}%"])
                writer.writerow(["NPCs with images", metadata['npcs_with_images'], f"{metadata['images_match_percentage']:.1f}%"])
                
                writer.writerow([])  # Empty row
                
                # Translation quality
                writer.writerow(["TRANSLATION PROGRESS - NPC NAMES"])
                writer.writerow(["Status", "EN Count", "EN %", "ES Count", "ES %", "PT Count", "PT %"])
                name_data = self.stats['translation_quality']['npc_names']
                writer.writerow(["Valid", name_data['en']['valid'], f"{name_data['en']['valid_percentage']:.1f}%",
                               name_data['es']['valid'], f"{name_data['es']['valid_percentage']:.1f}%",
                               name_data['pt']['valid'], f"{name_data['pt']['valid_percentage']:.1f}%"])
                writer.writerow(["NULL", name_data['en']['null'], f"{name_data['en']['null_percentage']:.1f}%",
                               name_data['es']['null'], f"{name_data['es']['null_percentage']:.1f}%",
                               name_data['pt']['null'], f"{name_data['pt']['null_percentage']:.1f}%"])
                writer.writerow(["Just 'x'", name_data['en']['x'], f"{name_data['en']['x_percentage']:.1f}%",
                               name_data['es']['x'], f"{name_data['es']['x_percentage']:.1f}%",
                               name_data['pt']['x'], f"{name_data['pt']['x_percentage']:.1f}%"])
                
                writer.writerow([])  # Empty row
                
                writer.writerow(["TRANSLATION PROGRESS - MESSAGES"])
                writer.writerow(["Status", "EN Count", "EN %", "ES Count", "ES %", "PT Count", "PT %"])
                msg_data = self.stats['translation_quality']['messages']
                writer.writerow(["Valid", msg_data['en']['valid'], f"{msg_data['en']['valid_percentage']:.1f}%",
                               msg_data['es']['valid'], f"{msg_data['es']['valid_percentage']:.1f}%",
                               msg_data['pt']['valid'], f"{msg_data['pt']['valid_percentage']:.1f}%"])
                writer.writerow(["NULL", msg_data['en']['null'], f"{msg_data['en']['null_percentage']:.1f}%",
                               msg_data['es']['null'], f"{msg_data['es']['null_percentage']:.1f}%",
                               msg_data['pt']['null'], f"{msg_data['pt']['null_percentage']:.1f}%"])
                writer.writerow(["Just 'x'", msg_data['en']['x'], f"{msg_data['en']['x_percentage']:.1f}%",
                               msg_data['es']['x'], f"{msg_data['es']['x_percentage']:.1f}%",
                               msg_data['pt']['x'], f"{msg_data['pt']['x_percentage']:.1f}%"])
                
                writer.writerow([])  # Empty row
                
                writer.writerow(["TRANSLATION PROGRESS - REPLIES"])
                writer.writerow(["Status", "EN Count", "EN %", "ES Count", "ES %", "PT Count", "PT %"])
                reply_data = self.stats['translation_quality']['replies']
                writer.writerow(["Valid", reply_data['en']['valid'], f"{reply_data['en']['valid_percentage']:.1f}%",
                               reply_data['es']['valid'], f"{reply_data['es']['valid_percentage']:.1f}%",
                               reply_data['pt']['valid'], f"{reply_data['pt']['valid_percentage']:.1f}%"])
                writer.writerow(["NULL", reply_data['en']['null'], f"{reply_data['en']['null_percentage']:.1f}%",
                               reply_data['es']['null'], f"{reply_data['es']['null_percentage']:.1f}%",
                               reply_data['pt']['null'], f"{reply_data['pt']['null_percentage']:.1f}%"])
                writer.writerow(["Just 'x'", reply_data['en']['x'], f"{reply_data['en']['x_percentage']:.1f}%",
                               reply_data['es']['x'], f"{reply_data['es']['x_percentage']:.1f}%",
                               reply_data['pt']['x'], f"{reply_data['pt']['x_percentage']:.1f}%"])
                
                # XLIFF mapping if available
                if 'xliff_mapping' in self.stats:
                    writer.writerow([])  # Empty row
                    writer.writerow(["XLIFF MAPPING STATISTICS"])
                    writer.writerow(["Type", "Total in XLIFF", "Mapped to Dialogs", "Unmapped", "Mapping %"])
                    xliff = self.stats['xliff_mapping']
                    writer.writerow(["Messages", xliff['total_xliff_messages'], xliff['mapped_messages'], 
                                   xliff['unmapped_messages'], f"{xliff['message_mapping_percentage']:.1f}%"])
                    writer.writerow(["Replies", xliff['total_xliff_replies'], xliff['mapped_replies'], 
                                   xliff['unmapped_replies'], f"{xliff['reply_mapping_percentage']:.1f}%"])
            
            messagebox.showinfo("Export Complete", f"Statistics exported successfully to:\n{file_path}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export statistics:\n{str(e)}")


class NPCDialogMapperGUI:
    """Main GUI Application"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("RETRO - NPC Dialog Mapper")
        self.root.geometry("800x700")
        self.root.minsize(600, 500)
        
        # Variables
        self.json_folder_var = tk.StringVar()
        self.xliff_folder_var = tk.StringVar()
        self.output_folder_var = tk.StringVar()
        self.data_source_var = tk.StringVar(value="json")
        
        # XLIFF file variables
        self.xliff_fr_en_var = tk.StringVar()
        self.xliff_fr_es_var = tk.StringVar()
        self.xliff_fr_pt_var = tk.StringVar()
        
        # Default values
        self.json_folder_var.set("Retro_json_dialog_data")
        self.output_folder_var.set("output")
        
        # Statistics storage
        self.last_stats = None
        
        self.setup_ui()
        
        # Set default XLIFF files
        self.xliff_fr_en_var.set("export.2025-09-29_12-25-52.fr-fr.en-gb.xliff")
        self.xliff_fr_es_var.set("export.2025-09-29_12-26-00.fr-fr.es-es.xliff")
        self.xliff_fr_pt_var.set("export.2025-09-29_12-26-05.fr-fr.pt-br.xliff")
        
        # Add trace to auto-load XLIFF files when folder path changes
        self.xliff_folder_var.trace('w', self._on_xliff_folder_changed)
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame with scrollbar
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Title
        title_label = ttk.Label(main_frame, text="RETRO NPC Dialog Mapper", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Data source selection
        source_frame = ttk.LabelFrame(main_frame, text="Data Source", padding=10)
        source_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Radiobutton(source_frame, text="JSON Database Export Files", 
                       variable=self.data_source_var, value="json",
                       command=self.on_data_source_changed).pack(anchor=tk.W)
        ttk.Radiobutton(source_frame, text="XLIFF Bilingual Files + JSON Structure", 
                       variable=self.data_source_var, value="xliff",
                       command=self.on_data_source_changed).pack(anchor=tk.W)
        
        # JSON folder selection
        self.json_frame = ttk.LabelFrame(main_frame, text="JSON Data Folder", padding=10)
        self.json_frame.pack(fill=tk.X, pady=(0, 10))
        
        json_folder_frame = ttk.Frame(self.json_frame)
        json_folder_frame.pack(fill=tk.X)
        
        ttk.Label(json_folder_frame, text="Folder:").pack(side=tk.LEFT)
        ttk.Entry(json_folder_frame, textvariable=self.json_folder_var, width=50).pack(side=tk.LEFT, padx=(5, 5), fill=tk.X, expand=True)
        ttk.Button(json_folder_frame, text="Browse", command=self.browse_json_folder).pack(side=tk.RIGHT)
        
        # JSON files info - will be updated dynamically
        self.json_info = ttk.Label(self.json_frame, 
                                  text="",
                                  font=("Arial", 8), foreground="gray")
        self.json_info.pack(anchor=tk.W, pady=(5, 0))
        
        # XLIFF folder and files selection
        self.xliff_frame = ttk.LabelFrame(main_frame, text="XLIFF Data Configuration", padding=10)
        self.xliff_frame.pack(fill=tk.X, pady=(0, 10))
        
        # XLIFF folder
        xliff_folder_frame = ttk.Frame(self.xliff_frame)
        xliff_folder_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(xliff_folder_frame, text="XLIFF Folder:").pack(side=tk.LEFT)
        ttk.Entry(xliff_folder_frame, textvariable=self.xliff_folder_var, width=40).pack(side=tk.LEFT, padx=(5, 5), fill=tk.X, expand=True)
        ttk.Button(xliff_folder_frame, text="Browse", command=self.browse_xliff_folder).pack(side=tk.RIGHT)
        
        # XLIFF files
        xliff_files_frame = ttk.Frame(self.xliff_frame)
        xliff_files_frame.pack(fill=tk.X)
        
        # Create grid for XLIFF files
        ttk.Label(xliff_files_frame, text="FR-EN File:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5), pady=2)
        ttk.Entry(xliff_files_frame, textvariable=self.xliff_fr_en_var, width=40).grid(row=0, column=1, sticky=tk.EW, padx=(0, 5), pady=2)
        ttk.Button(xliff_files_frame, text="Browse", command=lambda: self.browse_xliff_file(self.xliff_fr_en_var)).grid(row=0, column=2, pady=2)
        
        ttk.Label(xliff_files_frame, text="FR-ES File:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5), pady=2)
        ttk.Entry(xliff_files_frame, textvariable=self.xliff_fr_es_var, width=40).grid(row=1, column=1, sticky=tk.EW, padx=(0, 5), pady=2)
        ttk.Button(xliff_files_frame, text="Browse", command=lambda: self.browse_xliff_file(self.xliff_fr_es_var)).grid(row=1, column=2, pady=2)
        
        ttk.Label(xliff_files_frame, text="FR-PT File:").grid(row=2, column=0, sticky=tk.W, padx=(0, 5), pady=2)
        ttk.Entry(xliff_files_frame, textvariable=self.xliff_fr_pt_var, width=40).grid(row=2, column=1, sticky=tk.EW, padx=(0, 5), pady=2)
        ttk.Button(xliff_files_frame, text="Browse", command=lambda: self.browse_xliff_file(self.xliff_fr_pt_var)).grid(row=2, column=2, pady=2)
        
        # Configure grid weights
        xliff_files_frame.columnconfigure(1, weight=1)
        
        # Output folder selection
        output_frame = ttk.LabelFrame(main_frame, text="Output Configuration", padding=10)
        output_frame.pack(fill=tk.X, pady=(0, 10))
        
        output_folder_frame = ttk.Frame(output_frame)
        output_folder_frame.pack(fill=tk.X)
        
        ttk.Label(output_folder_frame, text="Output Folder:").pack(side=tk.LEFT)
        ttk.Entry(output_folder_frame, textvariable=self.output_folder_var, width=40).pack(side=tk.LEFT, padx=(5, 5), fill=tk.X, expand=True)
        ttk.Button(output_folder_frame, text="Browse", command=self.browse_output_folder).pack(side=tk.RIGHT)
        
        # Control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(20, 10))
        
        self.generate_button = ttk.Button(button_frame, text="Generate HTML", 
                                         command=self.generate_html_threaded, 
                                         style="Accent.TButton")
        self.generate_button.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="Open Output Folder", 
                  command=self.open_output_folder).pack(side=tk.LEFT, padx=(0, 10))
        
        # Statistics and export buttons (initially disabled)
        self.view_stats_button = ttk.Button(button_frame, text="View Statistics", 
                                           command=self.view_statistics, state=tk.DISABLED)
        self.view_stats_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.export_html_button = ttk.Button(button_frame, text="Export Stats HTML", 
                                            command=self.export_stats_html, state=tk.DISABLED)
        self.export_html_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.export_json_button = ttk.Button(button_frame, text="Export Stats JSON", 
                                            command=self.export_stats_json, state=tk.DISABLED)
        self.export_json_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.export_csv_button = ttk.Button(button_frame, text="Export Stats CSV", 
                                           command=self.export_stats_csv, state=tk.DISABLED)
        self.export_csv_button.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(button_frame, text="Exit", command=self.root.quit).pack(side=tk.RIGHT)
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=(10, 0))
        
        # Status and log area
        log_frame = ttk.LabelFrame(main_frame, text="Processing Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Initial state
        self.on_data_source_changed()
    
    def on_data_source_changed(self):
        """Handle data source selection change"""
        if self.data_source_var.get() == "json":
            self.json_frame.pack(fill=tk.X, pady=(0, 10), after=self.json_frame.master.children[list(self.json_frame.master.children.keys())[1]])
            self.xliff_frame.pack_forget()
            # Update required files message for JSON mode
            self.json_info.config(text="Required files: export_npc_dialog_json.json, export_npc_json.json,\n"
                                      "export_npc_message_json.json, export_npc_reply_json.json, npcs_Dofus3-03_202508.json")
        else:
            self.xliff_frame.pack(fill=tk.X, pady=(0, 10), after=self.json_frame)
            # Update required files message for XLIFF mode
            self.json_info.config(text="Required files: export_npc_dialog_json.json, export_npc_reply_json.json,\n"
                                      "npcs_Dofus3-03_202508.json + XLIFF translation files")
            # Don't hide JSON frame completely in XLIFF mode since we need structure files
    
    def browse_json_folder(self):
        """Browse for JSON data folder"""
        folder = filedialog.askdirectory(title="Select JSON Data Folder")
        if folder:
            self.json_folder_var.set(folder)
    
    def browse_xliff_folder(self):
        """Browse for XLIFF folder"""
        folder = filedialog.askdirectory(title="Select XLIFF Folder")
        if folder:
            self.xliff_folder_var.set(folder)
            # Auto-load XLIFF files from the selected folder
            self.auto_load_xliff_files(folder)
    
    def auto_load_xliff_files(self, folder_path):
        """Auto-load XLIFF files from the selected folder"""
        if not folder_path or not os.path.exists(folder_path):
            return
        
        # Common XLIFF file patterns to look for
        patterns = {
            'fr-en': ['.fr-fr.en-gb.xliff', '.fr.en.xliff', 'fr-en.xliff'],
            'fr-es': ['.fr-fr.es-es.xliff', '.fr.es.xliff', 'fr-es.xliff'], 
            'fr-pt': ['.fr-fr.pt-br.xliff', '.fr.pt.xliff', 'fr-pt.xliff']
        }
        
        # Get all XLIFF files in the folder
        xliff_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.xliff')]
        
        # Try to auto-detect and assign files
        for lang_pair, file_patterns in patterns.items():
            found_file = None
            
            # First, try exact pattern matches
            for pattern in file_patterns:
                matching_files = [f for f in xliff_files if pattern in f.lower()]
                if matching_files:
                    found_file = matching_files[0]  # Take the first match
                    break
            
            # If no exact match, try fuzzy matching
            if not found_file:
                lang_codes = lang_pair.split('-')
                for f in xliff_files:
                    f_lower = f.lower()
                    if all(code in f_lower for code in lang_codes):
                        found_file = f
                        break
            
            # Set the file if found
            if found_file:
                if lang_pair == 'fr-en':
                    self.xliff_fr_en_var.set(found_file)
                elif lang_pair == 'fr-es':
                    self.xliff_fr_es_var.set(found_file)
                elif lang_pair == 'fr-pt':
                    self.xliff_fr_pt_var.set(found_file)
        
        # Log what was found
        found_count = sum(1 for var in [self.xliff_fr_en_var, self.xliff_fr_es_var, self.xliff_fr_pt_var] 
                         if var.get())
        if found_count > 0:
            self.log_message(f"Auto-loaded {found_count} XLIFF files from folder")
        else:
            self.log_message("No XLIFF files auto-detected. Please select manually.")
    
    def _on_xliff_folder_changed(self, *args):
        """Callback when XLIFF folder path changes"""
        folder_path = self.xliff_folder_var.get()
        if folder_path and os.path.exists(folder_path):
            # Small delay to avoid rapid triggering during typing
            self.root.after(500, lambda: self.auto_load_xliff_files(folder_path))
    
    def browse_xliff_file(self, var):
        """Browse for individual XLIFF file"""
        file_path = filedialog.askopenfilename(
            title="Select XLIFF File",
            filetypes=[("XLIFF files", "*.xliff"), ("All files", "*.*")],
            initialdir=self.xliff_folder_var.get() if self.xliff_folder_var.get() else None
        )
        if file_path:
            var.set(os.path.basename(file_path))
            # Update folder if it wasn't set
            if not self.xliff_folder_var.get():
                self.xliff_folder_var.set(os.path.dirname(file_path))
    
    def browse_output_folder(self):
        """Browse for output folder"""
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder_var.set(folder)
    
    def open_output_folder(self):
        """Open the output folder in file explorer"""
        output_path = self.output_folder_var.get()
        if os.path.exists(output_path):
            os.startfile(output_path)  # Windows
        else:
            self.log_message("Output folder does not exist.")
    
    def view_statistics(self):
        """Open the statistics window"""
        if self.last_stats:
            StatisticsWindow(self.root, self.last_stats)
        else:
            messagebox.showwarning("No Statistics", "No statistics available. Please generate HTML first.")
    
    def export_stats_html(self):
        """Export statistics as HTML file"""
        if not self.last_stats:
            messagebox.showwarning("No Statistics", "No statistics available. Please generate HTML first.")
            return
            
        try:
            from tkinter import filedialog
            
            file_path = filedialog.asksaveasfilename(
                title="Export Statistics as HTML",
                defaultextension=".html",
                filetypes=[("HTML files", "*.html"), ("All files", "*.*")]
            )
            
            if not file_path:
                return
            
            html_content = self._generate_stats_html()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            messagebox.showinfo("Export Complete", f"Statistics exported successfully to:\n{file_path}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export statistics:\n{str(e)}")
    
    def export_stats_json(self):
        """Export statistics as JSON file"""
        if not self.last_stats:
            messagebox.showwarning("No Statistics", "No statistics available. Please generate HTML first.")
            return
            
        try:
            import json
            from tkinter import filedialog
            
            file_path = filedialog.asksaveasfilename(
                title="Export Statistics as JSON",
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            
            if not file_path:
                return
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.last_stats, f, indent=2, ensure_ascii=False)
            
            messagebox.showinfo("Export Complete", f"Statistics exported successfully to:\n{file_path}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export statistics:\n{str(e)}")
    
    def export_stats_csv(self):
        """Export statistics as CSV files"""
        if not self.last_stats:
            messagebox.showwarning("No Statistics", "No statistics available. Please generate HTML first.")
            return
            
        try:
            import csv
            from tkinter import filedialog
            import os
            
            # Ask for directory instead of file
            directory = filedialog.askdirectory(title="Select Directory for CSV Export")
            
            if not directory:
                return
            
            # Export basic statistics
            basic_file = os.path.join(directory, "basic_statistics.csv")
            with open(basic_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Metric", "Value", "Percentage"])
                
                basic = self.last_stats['basic']
                metadata = self.last_stats['metadata']
                
                writer.writerow(["Data Source", basic['data_source'], ""])
                writer.writerow(["Total NPCs loaded", basic['total_npcs'], "100.0%"])
                writer.writerow(["NPCs with dialogs", basic['npcs_with_dialogs'], f"{(basic['npcs_with_dialogs']/basic['total_npcs']*100):.1f}%"])
                writer.writerow(["Total messages", basic['total_messages'], ""])
                writer.writerow(["Total replies", basic['total_replies'], ""])
                writer.writerow(["Total dialogs", basic['total_dialogs'], ""])
                writer.writerow(["Total metadata entries", basic['total_metadata'], ""])
                writer.writerow(["NPCs matched with metadata", metadata['npcs_with_metadata'], f"{metadata['metadata_match_percentage']:.1f}%"])
                writer.writerow(["NPCs with gender info", metadata['npcs_with_gender'], f"{metadata['gender_match_percentage']:.1f}%"])
                writer.writerow(["NPCs with images", metadata['npcs_with_images'], f"{metadata['images_match_percentage']:.1f}%"])
            
            # Export translation quality for each type
            for trans_type in ['npc_names', 'messages', 'replies']:
                file_name = f"translation_quality_{trans_type}.csv"
                file_path = os.path.join(directory, file_name)
                
                with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["Status", "EN Count", "EN %", "ES Count", "ES %", "PT Count", "PT %"])
                    
                    data = self.last_stats['translation_quality'][trans_type]
                    writer.writerow(["Valid", data['en']['valid'], f"{data['en']['valid_percentage']:.1f}%",
                                   data['es']['valid'], f"{data['es']['valid_percentage']:.1f}%",
                                   data['pt']['valid'], f"{data['pt']['valid_percentage']:.1f}%"])
                    writer.writerow(["NULL", data['en']['null'], f"{data['en']['null_percentage']:.1f}%",
                                   data['es']['null'], f"{data['es']['null_percentage']:.1f}%",
                                   data['pt']['null'], f"{data['pt']['null_percentage']:.1f}%"])
                    writer.writerow(["Just 'x'", data['en']['x'], f"{data['en']['x_percentage']:.1f}%",
                                   data['es']['x'], f"{data['es']['x_percentage']:.1f}%",
                                   data['pt']['x'], f"{data['pt']['x_percentage']:.1f}%"])
            
            # Export XLIFF mapping if available
            if 'xliff_mapping' in self.last_stats:
                xliff_file = os.path.join(directory, "xliff_mapping.csv")
                with open(xliff_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["Type", "Total in XLIFF", "Mapped to Dialogs", "Unmapped", "Mapping %"])
                    
                    xliff = self.last_stats['xliff_mapping']
                    writer.writerow(["Messages", xliff['total_xliff_messages'], xliff['mapped_messages'], 
                                   xliff['unmapped_messages'], f"{xliff['message_mapping_percentage']:.1f}%"])
                    writer.writerow(["Replies", xliff['total_xliff_replies'], xliff['mapped_replies'], 
                                   xliff['unmapped_replies'], f"{xliff['reply_mapping_percentage']:.1f}%"])
            
            messagebox.showinfo("Export Complete", f"Statistics exported successfully to multiple CSV files in:\n{directory}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export statistics:\n{str(e)}")
    
    def _generate_stats_html(self):
        """Generate HTML representation of statistics"""
        if not self.last_stats:
            return ""
        
        basic = self.last_stats['basic']
        metadata = self.last_stats['metadata']
        translation = self.last_stats['translation_quality']
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NPC Dialog Mapper - Statistics Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        h1, h2 {{ color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .section {{ margin: 30px 0; }}
        .valid {{ background-color: #d4edda; }}
        .null {{ background-color: #f8d7da; }}
        .x-only {{ background-color: #fff3cd; }}
        .percentage {{ text-align: center; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 RETRO NPC Dialog Mapper - Statistics Report</h1>
        <p><strong>Generated:</strong> {self._get_current_datetime()}</p>
        <p><strong>Data Source:</strong> {basic['data_source']}</p>
        
        <div class="section">
            <h2>📋 Basic Statistics</h2>
            <table>
                <tr><th>Metric</th><th>Count</th><th>Percentage</th></tr>
                <tr><td>Total NPCs loaded</td><td>{basic['total_npcs']}</td><td class="percentage">100.0%</td></tr>
                <tr><td>NPCs with dialogs</td><td>{basic['npcs_with_dialogs']}</td><td class="percentage">{(basic['npcs_with_dialogs']/basic['total_npcs']*100):.1f}%</td></tr>
                <tr><td>Total messages</td><td>{basic['total_messages']}</td><td class="percentage">-</td></tr>
                <tr><td>Total replies</td><td>{basic['total_replies']}</td><td class="percentage">-</td></tr>
                <tr><td>Total dialogs</td><td>{basic['total_dialogs']}</td><td class="percentage">-</td></tr>
                <tr><td>Total metadata entries (Dofus3 NPC list for gender & picture)</td><td>{basic['total_metadata']}</td><td class="percentage">-</td></tr>
            </table>
        </div>
        
        <div class="section">
            <h2>🎭 Metadata Matching</h2>
            <table>
                <tr><th>Metric</th><th>Count</th><th>Percentage</th></tr>
                <tr><td>NPCs matched with metadata</td><td>{metadata['npcs_with_metadata']}</td><td class="percentage">{metadata['metadata_match_percentage']:.1f}%</td></tr>
                <tr><td>NPCs with gender info</td><td>{metadata['npcs_with_gender']}</td><td class="percentage">{metadata['gender_match_percentage']:.1f}%</td></tr>
                <tr><td>NPCs with images</td><td>{metadata['npcs_with_images']}</td><td class="percentage">{metadata['images_match_percentage']:.1f}%</td></tr>
            </table>
        </div>
        
        <div class="section">
            <h2>🌐 Translation Progress</h2>
            
            <h3>🧙‍♂️ NPC Names</h3>
            {self._generate_translation_table(translation['npc_names'])}
            
            <h3>💬 Messages</h3>
            {self._generate_translation_table(translation['messages'])}
            
            <h3>↪️ Replies</h3>
            {self._generate_translation_table(translation['replies'])}
        </div>
        
        {"" if 'xliff_mapping' not in self.last_stats else self._generate_xliff_section()}
    </div>
</body>
</html>
        """
        
        return html.strip()
    
    def _generate_translation_table(self, data):
        """Generate HTML table for translation quality data"""
        return f"""
            <table>
                <tr><th>Status</th><th>EN Count</th><th>EN %</th><th>ES Count</th><th>ES %</th><th>PT Count</th><th>PT %</th></tr>
                <tr class="valid"><td>✅ Valid</td><td>{data['en']['valid']}</td><td class="percentage">{data['en']['valid_percentage']:.1f}%</td><td>{data['es']['valid']}</td><td class="percentage">{data['es']['valid_percentage']:.1f}%</td><td>{data['pt']['valid']}</td><td class="percentage">{data['pt']['valid_percentage']:.1f}%</td></tr>
                <tr class="null"><td>❌ NULL</td><td>{data['en']['null']}</td><td class="percentage">{data['en']['null_percentage']:.1f}%</td><td>{data['es']['null']}</td><td class="percentage">{data['es']['null_percentage']:.1f}%</td><td>{data['pt']['null']}</td><td class="percentage">{data['pt']['null_percentage']:.1f}%</td></tr>
                <tr class="x-only"><td>❔ Just 'x'</td><td>{data['en']['x']}</td><td class="percentage">{data['en']['x_percentage']:.1f}%</td><td>{data['es']['x']}</td><td class="percentage">{data['es']['x_percentage']:.1f}%</td><td>{data['pt']['x']}</td><td class="percentage">{data['pt']['x_percentage']:.1f}%</td></tr>
            </table>
        """
    
    def _generate_xliff_section(self):
        """Generate XLIFF mapping section"""
        if not self.last_stats or 'xliff_mapping' not in self.last_stats:
            return ""
            
        xliff = self.last_stats['xliff_mapping']
        return f"""
        <div class="section">
            <h2>📄 XLIFF Mapping Analysis</h2>
            <table>
                <tr><th>Type</th><th>Total in XLIFF</th><th>Mapped to Dialogs</th><th>Unmapped</th><th>Mapping %</th></tr>
                <tr><td>💬 Messages</td><td>{xliff['total_xliff_messages']}</td><td>{xliff['mapped_messages']}</td><td>{xliff['unmapped_messages']}</td><td class="percentage">{xliff['message_mapping_percentage']:.1f}%</td></tr>
                <tr><td>↪️ Replies</td><td>{xliff['total_xliff_replies']}</td><td>{xliff['mapped_replies']}</td><td>{xliff['unmapped_replies']}</td><td class="percentage">{xliff['reply_mapping_percentage']:.1f}%</td></tr>
            </table>
            <p><em>Note: Unmapped entries are translations in XLIFF files that don't appear in the dialog relationship mapping.</em></p>
        </div>
        """
    
    def _get_current_datetime(self):
        """Get current date and time formatted"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def log_message(self, message):
        """Add message to log"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.config(state=tk.DISABLED)
        self.log_text.see(tk.END)
        self.root.update()
    
    def validate_inputs(self):
        """Validate user inputs"""
        use_xliff = self.data_source_var.get() == "xliff"
        
        self.log_message(f"Validating inputs for {'XLIFF' if use_xliff else 'JSON'} mode...")
        
        # Check JSON folder (always needed for structure)
        json_folder = self.json_folder_var.get()
        self.log_message(f"JSON folder: {json_folder}")
        
        if not json_folder or not os.path.exists(json_folder):
            raise ValueError(f"JSON data folder is required and must exist. Current path: {json_folder}")
        
        # Check JSON files
        required_json_files = [
            "export_npc_dialog_json.json",
            "npcs_Dofus3-03_202508.json"
        ]
        
        if not use_xliff:
            required_json_files.extend([
                "export_npc_json.json",
                "export_npc_message_json.json",
                "export_npc_reply_json.json"
            ])
        else:
            # For XLIFF mode, we still need reply structure
            required_json_files.append("export_npc_reply_json.json")
        
        self.log_message(f"Checking {len(required_json_files)} required JSON files...")
        for filename in required_json_files:
            file_path = os.path.join(json_folder, filename)
            self.log_message(f"  Checking: {filename}")
            if not os.path.exists(file_path):
                self.log_message(f"  ❌ NOT FOUND: {file_path}")
                raise ValueError(f"Required file not found: {filename}\nFull path: {file_path}")
            else:
                self.log_message(f"  ✅ Found: {filename}")
        
        # Check XLIFF configuration if using XLIFF mode
        if use_xliff:
            xliff_folder = self.xliff_folder_var.get()
            self.log_message(f"XLIFF folder: {xliff_folder}")
            
            if not xliff_folder or not os.path.exists(xliff_folder):
                raise ValueError(f"XLIFF folder is required and must exist when using XLIFF mode.\nCurrent path: {xliff_folder}")
            
            xliff_files = {
                'fr-en': self.xliff_fr_en_var.get(),
                'fr-es': self.xliff_fr_es_var.get(),
                'fr-pt': self.xliff_fr_pt_var.get()
            }
            
            # Remove empty entries
            xliff_files = {k: v for k, v in xliff_files.items() if v}
            
            if not xliff_files:
                self.log_message("Warning: No XLIFF files specified, continuing anyway...")
            else:
                self.log_message(f"Checking {len(xliff_files)} XLIFF files...")
                for lang_pair, filename in xliff_files.items():
                    file_path = os.path.join(xliff_folder, filename)
                    self.log_message(f"  Checking {lang_pair}: {filename}")
                    if not os.path.exists(file_path):
                        self.log_message(f"  ❌ NOT FOUND: {file_path}")
                        raise ValueError(f"XLIFF file not found: {filename}\nFull path: {file_path}")
                    else:
                        self.log_message(f"  ✅ Found: {filename}")
        
        # Check output folder
        output_folder = self.output_folder_var.get()
        self.log_message(f"Output folder: {output_folder}")
        
        if not output_folder:
            raise ValueError("Output folder is required.")
        
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        self.log_message("✅ All validation checks passed!")
        
        return True
    
    def generate_html_threaded(self):
        """Generate HTML in a separate thread"""
        def run_generation():
            try:
                self.validate_inputs()
                self.generate_button.config(state=tk.DISABLED)
                self.progress.start()
                
                use_xliff = self.data_source_var.get() == "xliff"
                
                self.log_message("="*50)
                self.log_message(f"Starting generation with {'XLIFF' if use_xliff else 'JSON'} data source...")
                self.log_message("="*50)
                
                # Prepare configuration
                json_folder = self.json_folder_var.get()
                output_folder = self.output_folder_var.get()
                
                xliff_folder = ""
                xliff_files = {}
                
                if use_xliff:
                    xliff_folder = self.xliff_folder_var.get()
                    xliff_files = {
                        'fr-en': self.xliff_fr_en_var.get(),
                        'fr-es': self.xliff_fr_es_var.get(),
                        'fr-pt': self.xliff_fr_pt_var.get()
                    }
                    # Remove empty entries
                    xliff_files = {k: v for k, v in xliff_files.items() if v}
                
                # Create mapper
                mapper = NPCDialogMapper(
                    folder_path=json_folder,
                    use_xliff=use_xliff,
                    xliff_folder=xliff_folder,
                    xliff_files=xliff_files
                )
                
                # Redirect print to log
                import sys
                import io
                
                old_stdout = sys.stdout
                sys.stdout = buffer = io.StringIO()
                
                try:
                    # Load data
                    mapper.load_data()
                    
                    # Get the output and log it
                    output = buffer.getvalue()
                    for line in output.split('\n'):
                        if line.strip():
                            self.log_message(line)
                    
                    # Generate HTML
                    suffix = "_xliff" if use_xliff else "_json"
                    output_filename = f"npc_dialog_mapping{suffix}.html"
                    output_path = os.path.join(output_folder, output_filename)
                    
                    self.log_message(f"Generating HTML file: {output_filename}")
                    mapper.generate_html(output_path)
                    
                finally:
                    sys.stdout = old_stdout
                
                # Generate comprehensive statistics
                self.log_message("Analyzing comprehensive statistics...")
                stats = mapper.analyze_statistics()
                
                # Log statistics
                self.log_message("\n" + "="*50)
                self.log_message("GENERATION COMPLETE - STATISTICS")
                self.log_message("="*50)
                
                basic = stats['basic']
                metadata = stats['metadata']
                
                self.log_message(f"Data Source: {basic['data_source']}")
                self.log_message(f"Total NPCs loaded: {basic['total_npcs']}")
                self.log_message(f"NPCs with dialogs: {basic['npcs_with_dialogs']} ({(basic['npcs_with_dialogs']/basic['total_npcs']*100):.1f}%)")
                self.log_message(f"Total messages: {basic['total_messages']}")
                self.log_message(f"Total replies: {basic['total_replies']}")
                self.log_message(f"Total dialogs: {basic['total_dialogs']}")
                self.log_message(f"Total metadata entries: {basic['total_metadata']}")
                self.log_message(f"NPCs matched with metadata: {metadata['npcs_with_metadata']} ({metadata['metadata_match_percentage']:.1f}%)")
                self.log_message(f"NPCs with gender info: {metadata['npcs_with_gender']} ({metadata['gender_match_percentage']:.1f}%)")
                self.log_message(f"NPCs with images: {metadata['npcs_with_images']} ({metadata['images_match_percentage']:.1f}%)")
                
                # Log translation quality summary
                msg_stats = stats['translation_quality']['messages']
                reply_stats = stats['translation_quality']['replies']
                
                self.log_message(f"\nTranslation Progress Summary:")
                self.log_message(f"Messages - EN: {msg_stats['en']['valid']} valid ({msg_stats['en']['valid_percentage']:.1f}%), {msg_stats['en']['null']} null ({msg_stats['en']['null_percentage']:.1f}%)")
                self.log_message(f"Messages - ES: {msg_stats['es']['valid']} valid ({msg_stats['es']['valid_percentage']:.1f}%), {msg_stats['es']['null']} null ({msg_stats['es']['null_percentage']:.1f}%)")
                self.log_message(f"Messages - PT: {msg_stats['pt']['valid']} valid ({msg_stats['pt']['valid_percentage']:.1f}%), {msg_stats['pt']['null']} null ({msg_stats['pt']['null_percentage']:.1f}%)")
                
                # Log XLIFF mapping if applicable
                if 'xliff_mapping' in stats:
                    xliff = stats['xliff_mapping']
                    self.log_message(f"\nXLIFF Mapping Analysis:")
                    self.log_message(f"Messages: {xliff['mapped_messages']}/{xliff['total_xliff_messages']} mapped ({xliff['message_mapping_percentage']:.1f}%), {xliff['unmapped_messages']} unmapped")
                    self.log_message(f"Replies: {xliff['mapped_replies']}/{xliff['total_xliff_replies']} mapped ({xliff['reply_mapping_percentage']:.1f}%), {xliff['unmapped_replies']} unmapped")
                
                self.log_message(f"\nHTML file generated successfully: {output_path}")
                self.log_message("You can open it in a web browser to view the mapped dialogs.")
                
                # Store statistics for later use
                self.last_stats = stats
                
                # Enable statistics buttons
                self.view_stats_button.config(state=tk.NORMAL)
                self.export_html_button.config(state=tk.NORMAL)
                self.export_json_button.config(state=tk.NORMAL)
                self.export_csv_button.config(state=tk.NORMAL)
                
                # Show success message with option to view detailed statistics
                result = messagebox.askyesno("Success", 
                    f"HTML file generated successfully!\n\n"
                    f"File: {output_filename}\n"
                    f"Location: {output_folder}\n\n"
                    f"Would you like to view detailed statistics?")
                
                if result:
                    # Show detailed statistics window
                    self.root.after(100, lambda: StatisticsWindow(self.root, stats))
                
            except Exception as e:
                self.log_message(f"ERROR: {str(e)}")
                messagebox.showerror("Error", f"An error occurred:\n\n{str(e)}")
            
            finally:
                self.progress.stop()
                self.generate_button.config(state=tk.NORMAL)
        
        # Run in thread
        thread = threading.Thread(target=run_generation, daemon=True)
        thread.start()


def main():
    """Main function"""
    root = tk.Tk()
    
    # Configure style
    style = ttk.Style()
    
    # Try to use a modern theme
    available_themes = style.theme_names()
    if 'vista' in available_themes:
        style.theme_use('vista')
    elif 'clam' in available_themes:
        style.theme_use('clam')
    
    # Configure custom styles
    style.configure("Accent.TButton", font=("Arial", 10, "bold"))
    
    # Create and run application
    app = NPCDialogMapperGUI(root)
    
    # Center window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')
    
    # Start the application
    root.mainloop()


if __name__ == "__main__":
    main()