#!/usr/bin/env python3
"""
NPC Dialog Mapper - Tkinter GUI Application
Responsive GUI for generating NPC dialogs HTML files from JSON or XLIFF sources.
Based on the retro_NPC_dialogs_mapping.ipynb notebook.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import os
import sys
from pathlib import Path
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
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
    """Core mapping class from the notebook"""
    
    def __init__(self, folder_path: str, use_xliff: bool = False, xliff_folder: str = "", xliff_files: dict = None):
        self.folder_path = folder_path
        self.use_xliff = use_xliff
        self.xliff_folder = xliff_folder
        self.xliff_files = xliff_files or {}
        self.npcs: Dict[int, NPC] = {}
        self.messages: Dict[int, NPCMessage] = {}
        self.replies: Dict[int, NPCReply] = {}
        self.dialogs: List[NPCDialog] = []
        self.metadata: List[NPCMetadata] = []
        
        # For XLIFF mode
        self.xliff_translations = {
            'npc_names': {},
            'messages': {},
            'replies': {}
        }
    
    def load_data(self):
        """Load all data files"""
        try:
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
            
        except Exception as e:
            print(f"Error in load_data: {e}")
            raise
    
    def _clean_source_text(self, text: str) -> str:
        """Clean source text by removing bracketed content"""
        import re
        # Remove content in square brackets like [wait_mod_Br9100]
        cleaned = re.sub(r'\[.*?\]', '', text)
        return cleaned.strip()
    
    def _load_xliff_translations(self):
        """Load all XLIFF files and extract translations only"""
        for lang_pair, filename in self.xliff_files.items():
            file_path = os.path.join(self.xliff_folder, filename)
            print(f"  Loading {lang_pair}: {filename}")
            
            if not os.path.exists(file_path):
                print(f"    Warning: File not found: {file_path}")
                continue
                
            target_lang = lang_pair.split('-')[1]  # en, es, pt
            
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                ns = {'xliff': 'urn:oasis:names:tc:xliff:document:1.2'}
                trans_units = root.findall('.//xliff:trans-unit', ns)
                
                for trans_unit in trans_units:
                    unit_id = trans_unit.get('id', '')
                    
                    if unit_id.startswith('npc.'):
                        source_elem = trans_unit.find('.//xliff:source', ns)
                        target_elem = trans_unit.find('.//xliff:target', ns)
                        
                        if source_elem is not None and target_elem is not None:
                            source_text = source_elem.text or ""
                            target_text = target_elem.text or ""
                            
                            source_text = self._clean_source_text(source_text)
                            
                            parts = unit_id.split('.')
                            if len(parts) >= 3:
                                entity_id = parts[1]
                                entry_type = parts[2]
                                
                                try:
                                    entity_id_int = int(entity_id)
                                except ValueError:
                                    continue
                                
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
                                    
            except Exception as e:
                print(f"    Error parsing {filename}: {e}")
                continue
    
    def _load_structure_from_json(self):
        """Load structural data from JSON files (dialog relationships)"""
        # Load dialogs (structure)
        dialog_path = os.path.join(self.folder_path, "export_npc_dialog_json.json")
        print(f"Loading dialog structure from: {dialog_path}")
        
        if os.path.exists(dialog_path):
            try:
                with open(dialog_path, 'r', encoding='utf-8') as f:
                    dialog_data = json.load(f)
                    print(f"Loaded dialog structure with {len(dialog_data)} entries")
                    
                    for item in dialog_data:
                        try:
                            dialog = NPCDialog(
                                dialog_id=item['dialog_id'],
                                npc_id=item['dialog_npc_id'],
                                message_id=item['dialog_message_id'],
                                check_order=item['dialog_message_check_order']
                            )
                            self.dialogs.append(dialog)
                        except Exception as e:
                            print(f"Error processing dialog: {e}")
                            continue
            except Exception as e:
                print(f"Error loading dialog structure: {e}")
                raise
        else:
            print(f"Error: Dialog structure file not found at {dialog_path}")
            raise FileNotFoundError("export_npc_dialog_json.json is required for XLIFF mode")
        
        # Load replies (for relationships)
        reply_path = os.path.join(self.folder_path, "export_npc_reply_json.json")
        print(f"Loading reply structure from: {reply_path}")
        
        if os.path.exists(reply_path):
            try:
                with open(reply_path, 'r', encoding='utf-8') as f:
                    reply_data = json.load(f)
                    print(f"Loaded reply structure with {len(reply_data)} entries")
                    
                    for item in reply_data:
                        try:
                            reply = NPCReply(
                                reply_id=item['reply_id'],
                                text_fr="",  # Will be filled from XLIFF
                                text_en=None,
                                text_es=None,
                                text_pt=None,
                                context=item.get('reply_criteria'),
                                parent_message_id=item['reply_parent_id'],
                                leads_to_message_id=item.get('reply_message_id')
                            )
                            self.replies[reply.reply_id] = reply
                        except Exception as e:
                            print(f"Error processing reply: {e}")
                            continue
            except Exception as e:
                print(f"Error loading reply structure: {e}")
                raise
        else:
            print(f"Error: Reply structure file not found at {reply_path}")
            raise FileNotFoundError("export_npc_reply_json.json is required for XLIFF mode")
    
    def _merge_xliff_with_structure(self):
        """Merge XLIFF translations with JSON structure"""
        # Create NPCs from XLIFF name translations
        for npc_id, translations in self.xliff_translations['npc_names'].items():
            npc = NPC(
                npc_id=npc_id,
                name_fr=translations.get('fr', ''),
                name_en=translations.get('en'),
                name_es=translations.get('es'),
                name_pt=translations.get('pt'),
                context=None
            )
            self.npcs[npc_id] = npc
        
        # Create messages from XLIFF translations
        for msg_id, translations in self.xliff_translations['messages'].items():
            message = NPCMessage(
                message_id=msg_id,
                text_fr=translations.get('fr', ''),
                text_en=translations.get('en'),
                text_es=translations.get('es'),
                text_pt=translations.get('pt'),
                context=None
            )
            self.messages[msg_id] = message
        
        # Update replies with XLIFF translations
        for reply_id, reply in self.replies.items():
            if reply_id in self.xliff_translations['replies']:
                translations = self.xliff_translations['replies'][reply_id]
                reply.text_fr = translations.get('fr', '')
                reply.text_en = translations.get('en')
                reply.text_es = translations.get('es')
                reply.text_pt = translations.get('pt')
    
    def _load_npcs(self):
        """Load NPCs from JSON file"""
        npc_path = os.path.join(self.folder_path, "export_npc_json.json")
        print(f"Loading NPCs from: {npc_path}")
        
        if not os.path.exists(npc_path):
            print(f"Warning: NPC file not found")
            return
            
        try:
            with open(npc_path, 'r', encoding='utf-8') as f:
                npc_data = json.load(f)
                print(f"NPC file loaded: {len(npc_data)} entries")
                    
                if not npc_data:
                    print("Warning: NPC file is empty")
                    return
                
                first_entry = npc_data[0]
                print(f"First entry type: {type(first_entry)}")
                
                if isinstance(first_entry, dict):
                    print(f"Available keys: {list(first_entry.keys())}")
                    
                    invalid_count = 0
                    for i, item in enumerate(npc_data):
                        if not isinstance(item, dict):
                            invalid_count += 1
                            if invalid_count <= 3:
                                print(f"Invalid NPC item {i}: {type(item)}")
                            continue
                            
                        try:
                            npc = NPC(
                                npc_id=item['npc_id'],
                                name_fr=item['npc_name_fr'],
                                name_en=item.get('npc_name_en'),
                                name_es=item.get('npc_name_es'),
                                name_pt=item.get('npc_name_pt'),
                                context=item.get('npc_contextual_speech')
                            )
                            self.npcs[npc.npc_id] = npc
                        except Exception as e:
                            if len(self.npcs) < 3:
                                print(f"Error processing NPC item {i}: {e}")
                            continue
                    
                    if invalid_count > 3:
                        print(f"... and {invalid_count - 3} more invalid NPC items")
                        
                else:
                    print(f"ERROR: Expected list of dictionaries, got list of {type(first_entry)}")
                    print(f"First entry sample: {str(first_entry)[:200]}")
                    return
                        
                print(f"Successfully loaded {len(self.npcs)} NPCs")
                
        except Exception as e:
            print(f"Error loading NPC file: {e}")
    
    def _load_messages(self):
        """Load messages from JSON file"""
        message_path = os.path.join(self.folder_path, "export_npc_message_json.json")
        print(f"Loading messages from: {message_path}")
        
        if not os.path.exists(message_path):
            print(f"Warning: Message file not found")
            return
            
        try:
            with open(message_path, 'r', encoding='utf-8') as f:
                message_data = json.load(f)
                print(f"Message file loaded: {len(message_data)} entries")
                
                if not message_data:
                    print("Warning: Message file is empty")
                    return
                
                first_entry = message_data[0]
                print(f"First entry type: {type(first_entry)}")
                
                if isinstance(first_entry, dict):
                    print(f"Available keys: {list(first_entry.keys())}")
                    
                    invalid_count = 0
                    for i, item in enumerate(message_data):
                        if not isinstance(item, dict):
                            invalid_count += 1
                            if invalid_count <= 3:
                                print(f"Invalid message item {i}: {type(item)}")
                            continue
                            
                        try:
                            message = NPCMessage(
                                message_id=item['message_id'],
                                text_fr=item['message_fr'],
                                text_en=item.get('message_en'),
                                text_es=item.get('message_es'),
                                text_pt=item.get('message_pt'),
                                context=item.get('message_criteria')
                            )
                            self.messages[message.message_id] = message
                        except Exception as e:
                            if len(self.messages) < 3:
                                print(f"Error processing message item {i}: {e}")
                            continue
                    
                    if invalid_count > 3:
                        print(f"... and {invalid_count - 3} more invalid message items")
                        
                else:
                    print(f"ERROR: Expected list of dictionaries, got list of {type(first_entry)}")
                    print(f"First entry sample: {str(first_entry)[:200]}")
                    return
                        
                print(f"Successfully loaded {len(self.messages)} messages")
                
        except Exception as e:
            print(f"Error loading message file: {e}")
    
    def _load_replies(self):
        """Load replies from JSON file"""
        reply_path = os.path.join(self.folder_path, "export_npc_reply_json.json")
        print(f"Loading replies from: {reply_path}")
        
        if not os.path.exists(reply_path):
            print(f"Warning: Reply file not found")
            return
            
        try:
            with open(reply_path, 'r', encoding='utf-8') as f:
                reply_data = json.load(f)
                print(f"Reply file loaded: {len(reply_data)} entries")
                
                if not reply_data:
                    print("Warning: Reply file is empty")
                    return
                
                first_entry = reply_data[0]
                print(f"First entry type: {type(first_entry)}")
                
                if isinstance(first_entry, dict):
                    print(f"Available keys: {list(first_entry.keys())}")
                    
                    invalid_count = 0
                    for i, item in enumerate(reply_data):
                        if not isinstance(item, dict):
                            invalid_count += 1
                            if invalid_count <= 3:
                                print(f"Invalid reply item {i}: {type(item)}")
                            continue
                            
                        try:
                            reply = NPCReply(
                                reply_id=item['reply_id'],
                                text_fr=item['reply_fr'],
                                text_en=item.get('reply_en'),
                                text_es=item.get('reply_es'),
                                text_pt=item.get('reply_pt'),
                                context=item.get('reply_criteria'),
                                parent_message_id=item['reply_parent_id'],
                                leads_to_message_id=item.get('reply_message_id')
                            )
                            self.replies[reply.reply_id] = reply
                        except Exception as e:
                            if len(self.replies) < 3:
                                print(f"Error processing reply item {i}: {e}")
                            continue
                    
                    if invalid_count > 3:
                        print(f"... and {invalid_count - 3} more invalid reply items")
                        
                else:
                    print(f"ERROR: Expected list of dictionaries, got list of {type(first_entry)}")
                    print(f"First entry sample: {str(first_entry)[:200]}")
                    return
                        
                print(f"Successfully loaded {len(self.replies)} replies")
                
        except Exception as e:
            print(f"Error loading reply file: {e}")
    
    def _load_dialogs(self):
        """Load dialogs from JSON file"""
        dialog_path = os.path.join(self.folder_path, "export_npc_dialog_json.json")
        print(f"Looking for dialog file at: {dialog_path}")
        
        if not os.path.exists(dialog_path):
            print(f"Error: Dialog file not found at {dialog_path}")
            raise FileNotFoundError(f"Required file not found: export_npc_dialog_json.json")
            
        try:
            with open(dialog_path, 'r', encoding='utf-8') as f:
                dialog_data = json.load(f)
                print(f"Loaded dialog file with {len(dialog_data)} entries")
                
                for i, item in enumerate(dialog_data):
                    try:
                        dialog = NPCDialog(
                            dialog_id=item['dialog_id'],
                            npc_id=item['dialog_npc_id'],
                            message_id=item['dialog_message_id'],
                            check_order=item['dialog_message_check_order']
                        )
                        self.dialogs.append(dialog)
                    except Exception as e:
                        print(f"Error processing dialog item {i}: {e}")
                        continue
                        
                print(f"Successfully loaded {len(self.dialogs)} dialogs")
                
        except Exception as e:
            print(f"Error loading dialog file: {e}")
            raise
    
    def _load_metadata(self):
        """Load NPC metadata"""
        metadata_path = os.path.join(self.folder_path, "npcs_Dofus3-03_202508.json")
        print(f"Loading metadata from: {metadata_path}")
        
        if not os.path.exists(metadata_path):
            print(f"Warning: Metadata file not found")
            return
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata_data = json.load(f)
                print(f"Metadata file loaded: {len(metadata_data)} entries")
                
                # Check the structure of the data
                if not metadata_data:
                    print("Warning: Metadata file is empty")
                    return
                
                # Analyze first few entries to understand structure
                first_entry = metadata_data[0]
                print(f"First entry type: {type(first_entry)}")
                
                if isinstance(first_entry, dict):
                    print(f"Available keys: {list(first_entry.keys())}")
                    # Normal processing
                    invalid_count = 0
                    for i, item in enumerate(metadata_data):
                        if not isinstance(item, dict):
                            invalid_count += 1
                            if invalid_count <= 3:  # Only show first 3 invalid items
                                print(f"Invalid item {i}: {type(item)} - {str(item)[:100]}")
                            continue
                            
                        try:
                            required_fields = ['index_id', 'gender', 'look', 'img_url', 'metadata_id', 
                                             'name_en', 'name_pt', 'name_es', 'name_fr', 'name_de']
                            
                            missing_fields = [field for field in required_fields if field not in item]
                            if missing_fields:
                                if len(self.metadata) < 3:  # Only show first 3 missing field warnings
                                    print(f"Item {i} missing fields: {missing_fields}")
                                continue
                            
                            metadata = NPCMetadata(
                                index_id=item['index_id'],
                                gender=item['gender'],
                                look=item['look'],
                                img_url=item['img_url'],
                                metadata_id=item['metadata_id'],
                                name_en=item['name_en'],
                                name_pt=item['name_pt'],
                                name_es=item['name_es'],
                                name_fr=item['name_fr'],
                                name_de=item['name_de']
                            )
                            self.metadata.append(metadata)
                            
                        except Exception as e:
                            if len(self.metadata) < 3:  # Only show first 3 processing errors
                                print(f"Error processing item {i}: {e}")
                            continue
                    
                    if invalid_count > 3:
                        print(f"... and {invalid_count - 3} more invalid items")
                        
                else:
                    print(f"ERROR: Expected list of dictionaries, got list of {type(first_entry)}")
                    print(f"First entry sample: {str(first_entry)[:200]}")
                    print("Cannot process metadata file with this structure")
                    return
                        
                print(f"Successfully loaded {len(self.metadata)} metadata entries")
                
        except Exception as e:
            print(f"Error loading metadata file: {e}")
            print("Continuing without metadata...")
    
    def _build_relationships(self):
        """Build relationships between dialogs, messages, and replies"""
        # Assign dialogs to NPCs
        for dialog in self.dialogs:
            if dialog.npc_id in self.npcs:
                self.npcs[dialog.npc_id].dialogs.append(dialog)
        
        # Assign replies to messages
        for reply in self.replies.values():
            if reply.parent_message_id in self.messages:
                self.messages[reply.parent_message_id].replies.append(reply)
    
    def _match_metadata_with_npcs(self):
        """Match metadata with NPCs"""
        for npc in self.npcs.values():
            for metadata in self.metadata:
                if (npc.name_fr and metadata.name_fr and 
                    npc.name_fr.lower().strip() == metadata.name_fr.lower().strip()):
                    npc.genders.append(metadata.gender)
                    npc.img_urls.append(metadata.img_url)
    
    def generate_html(self, output_filename: str):
        """Generate HTML file with all dialog mappings"""
        # This is a simplified version - in the real implementation,
        # you would include the full HTML template from the notebook
        html_content = self._generate_html_content()
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_html_content(self) -> str:
        """Generate the actual HTML content"""
        # This would contain the full HTML template from the notebook
        # For brevity, returning a simplified version
        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NPC Dialog Mapping</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .npc-card {{ border: 1px solid #ccc; margin: 10px 0; padding: 15px; border-radius: 5px; }}
        .npc-name {{ font-weight: bold; font-size: 1.2em; margin-bottom: 10px; }}
        .lang-row {{ margin: 5px 0; }}
        .lang-label {{ font-weight: bold; color: #666; }}
    </style>
</head>
<body>
    <h1>NPC Dialog Mapping</h1>
    <p>Generated from {'XLIFF bilingual files' if self.use_xliff else 'JSON database exports'}</p>
    <p>Total NPCs: {len(self.npcs)} | Total Messages: {len(self.messages)} | Total Replies: {len(self.replies)}</p>
    
    <div class="npc-container">
        {self._generate_npc_cards()}
    </div>
</body>
</html>
"""
    
    def _generate_npc_cards(self) -> str:
        """Generate HTML for NPC cards"""
        cards = []
        
        for npc in sorted(self.npcs.values(), key=lambda x: x.name_fr):
            if not npc.dialogs:
                continue
                
            card_html = f"""
            <div class="npc-card">
                <div class="npc-name">{npc.name_fr} (ID: {npc.npc_id})</div>
                <div class="npc-names">
                    <div class="lang-row"><span class="lang-label">FR:</span> {npc.name_fr}</div>
                    <div class="lang-row"><span class="lang-label">EN:</span> {npc.name_en or 'N/A'}</div>
                    <div class="lang-row"><span class="lang-label">ES:</span> {npc.name_es or 'N/A'}</div>
                    <div class="lang-row"><span class="lang-label">PT:</span> {npc.name_pt or 'N/A'}</div>
                </div>
                
                <div class="dialogs">
                    {self._generate_dialog_tree(npc)}
                </div>
            </div>
            """
            cards.append(card_html)
        
        return ''.join(cards)
    
    def _generate_dialog_tree(self, npc: NPC) -> str:
        """Generate dialog tree HTML for an NPC"""
        dialogs_html = []
        
        for dialog in sorted(npc.dialogs, key=lambda x: x.check_order):
            if dialog.message_id in self.messages:
                message = self.messages[dialog.message_id]
                dialog_html = f"""
                <div class="dialog" style="margin: 10px 0; padding: 10px; border-left: 3px solid #007acc;">
                    <h4>Dialog {dialog.dialog_id} (Order: {dialog.check_order})</h4>
                    <div class="message">
                        <strong>Message {message.message_id}:</strong>
                        <div class="lang-row"><span class="lang-label">FR:</span> {message.text_fr}</div>
                        <div class="lang-row"><span class="lang-label">EN:</span> {message.text_en or 'N/A'}</div>
                        <div class="lang-row"><span class="lang-label">ES:</span> {message.text_es or 'N/A'}</div>
                        <div class="lang-row"><span class="lang-label">PT:</span> {message.text_pt or 'N/A'}</div>
                        
                        {self._generate_replies_html(message.replies)}
                    </div>
                </div>
                """
                dialogs_html.append(dialog_html)
        
        return ''.join(dialogs_html)
    
    def _generate_replies_html(self, replies: List[NPCReply]) -> str:
        """Generate HTML for replies"""
        if not replies:
            return ""
        
        replies_html = ["<div class='replies' style='margin-left: 20px; margin-top: 10px;'>"]
        replies_html.append("<strong>Replies:</strong>")
        
        for reply in sorted(replies, key=lambda x: x.reply_id):
            reply_html = f"""
            <div class="reply" style="margin: 5px 0; padding: 8px; border-left: 2px solid #28a745;">
                <strong>Reply {reply.reply_id}:</strong>
                <div class="lang-row"><span class="lang-label">FR:</span> {reply.text_fr}</div>
                <div class="lang-row"><span class="lang-label">EN:</span> {reply.text_en or 'N/A'}</div>
                <div class="lang-row"><span class="lang-label">ES:</span> {reply.text_es or 'N/A'}</div>
                <div class="lang-row"><span class="lang-label">PT:</span> {reply.text_pt or 'N/A'}</div>
                {f'<div><em>Leads to message: {reply.leads_to_message_id}</em></div>' if reply.leads_to_message_id else ''}
            </div>
            """
            replies_html.append(reply_html)
        
        replies_html.append("</div>")
        return ''.join(replies_html)


class NPCDialogMapperGUI:
    """Main GUI Application"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("NPC Dialog Mapper")
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
        title_label = ttk.Label(main_frame, text="NPC Dialog Mapper", font=("Arial", 16, "bold"))
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
                
                # Log statistics
                self.log_message("\n" + "="*50)
                self.log_message("GENERATION COMPLETE - STATISTICS")
                self.log_message("="*50)
                data_source = "XLIFF Bilingual Files" if use_xliff else "JSON Database Export"
                self.log_message(f"Data Source: {data_source}")
                self.log_message(f"Total NPCs loaded: {len(mapper.npcs)}")
                self.log_message(f"NPCs with dialogs: {len([npc for npc in mapper.npcs.values() if npc.dialogs])}")
                self.log_message(f"Total messages: {len(mapper.messages)}")
                self.log_message(f"Total replies: {len(mapper.replies)}")
                self.log_message(f"Total dialogs: {len(mapper.dialogs)}")
                self.log_message(f"Total metadata entries: {len(mapper.metadata)}")
                
                # Metadata statistics
                npcs_with_metadata = sum(1 for npc in mapper.npcs.values() if npc.genders or npc.img_urls)
                npcs_with_images = sum(1 for npc in mapper.npcs.values() if npc.img_urls)
                npcs_with_gender = sum(1 for npc in mapper.npcs.values() if npc.genders)
                
                self.log_message(f"NPCs matched with metadata: {npcs_with_metadata}")
                self.log_message(f"NPCs with gender info: {npcs_with_gender}")
                self.log_message(f"NPCs with images: {npcs_with_images}")
                
                self.log_message(f"\nHTML file generated successfully: {output_path}")
                self.log_message("You can open it in a web browser to view the mapped dialogs.")
                
                # Show success message
                messagebox.showinfo("Success", f"HTML file generated successfully!\n\nFile: {output_filename}\nLocation: {output_folder}")
                
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