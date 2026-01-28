import os
import sys
import json
import logging
import argparse
import requests
import subprocess
from pathlib import Path
from pymongo import MongoClient
from configparser import ConfigParser

AI_CONFIG_PATH = "/opt/sdna/nginx/ai-config.json"
NODE_EXTENDED_METADATA_URL = "http://127.0.0.1:5080/catalogs/extendedMetadata"
NODE_AI_ENRICH_URL = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
NORMALIZER_SCRIPT = "/opt/sdna/bin/azurevi_metadata_normalizer.py"

LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"

IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "ApiDNA"
MONGO_COLLECTION = "catalogExtendedMetadata"

SDNA_EVENT_MAP = {
    "azure_video_transcript": "transcript",
    "azure_video_ocr": "ocr",
    "azure_video_keywords": "keywords",
    "azure_video_topics": "topics",
    "azure_video_labels": "lables",
    "azure_video_brands": "brands",
    "azure_video_named_locations": "locations",
    "azure_video_named_people": "celebrities",
    "azure_video_audio_effects": "effects",
    "azure_video_detected_objects": "objects",
    "azure_video_sentiments": "sentiments",
    "azure_video_emotions": "emotions",
    "azure_video_visual_content_moderation": "moderation",
    "azure_video_frame_patterns": "patterns",
    "azure_video_speakers": "speakers"
}

LANGUAGE_CODE_MAP = {
    "": "Default",
    "ar-AE": "Arabic (United Arab Emirates)",
    "ar-BH": "Arabic Modern Standard (Bahrain)",
    "ar-EG": "Arabic Egypt",
    "ar-IL": "Arabic (Israel)",
    "ar-IQ": "Arabic (Iraq)",
    "ar-JO": "Arabic (Jordan)",
    "ar-KW": "Arabic (Kuwait)",
    "ar-LB": "Arabic (Lebanon)",
    "ar-OM": "Arabic (Oman)",
    "ar-PS": "Arabic (Palestinian Authority)",
    "ar-QA": "Arabic (Qatar)",
    "ar-SA": "Arabic (Saudi Arabia)",
    "ar-SY": "Arabic Syrian Arab Republic",
    "bg-BG": "Bulgarian",
    "ca-ES": "Catalan",
    "cs-CZ": "Czech",
    "da-DK": "Danish",
    "de-DE": "German",
    "el-GR": "Greek",
    "en-AU": "English Australia",
    "en-GB": "English United Kingdom",
    "en-US": "English United States",
    "es-ES": "Spanish",
    "es-MX": "Spanish (Mexico)",
    "et-EE": "Estonian",
    "fa-IR": "Persian",
    "fi-FI": "Finnish",
    "fil-PH": "Filipino",
    "fr-CA": "French (Canada)",
    "fr-FR": "French",
    "ga-IE": "Irish",
    "gu-IN": "Gujarati",
    "he-IL": "Hebrew",
    "hi-IN": "Hindi",
    "hr-HR": "Croatian",
    "hu-HU": "Hungarian",
    "hy-AM": "Armenian",
    "id-ID": "Indonesian",
    "is-IS": "Icelandic",
    "it-IT": "Italian",
    "ja-JP": "Japanese",
    "kn-IN": "Kannada",
    "ko-KR": "Korean",
    "lt-LT": "Lithuanian",
    "lv-LV": "Latvian",
    "ml-IN": "Malayalam",
    "ms-MY": "Malay",
    "nb-NO": "Norwegian",
    "nl-NL": "Dutch",
    "pl-PL": "Polish",
    "pt-BR": "Portuguese",
    "pt-PT": "Portuguese (Portugal)",
    "ro-RO": "Romanian",
    "ru-RU": "Russian",
    "sk-SK": "Slovak",
    "sl-SI": "Slovenian",
    "sv-SE": "Swedish",
    "ta-IN": "Tamil",
    "te-IN": "Telugu",
    "th-TH": "Thai",
    "tr-TR": "Turkish",
    "uk-UA": "Ukrainian",
    "vi-VN": "Vietnamese",
    "zh-Hans": "Chinese (Simplified)",
    "zh-HK": "Chinese (Cantonese, Traditional)"
}

EVENT_TYPE_PROXY_VALUE_MAP = {
    "video_proxy": "vid",
    "video_proxy_sample_scene_change": "frm",
    "video_sample_faces": "img",
    "video_sample_interval": "vid",
    "audio_proxy": "aud",
    "sprite_sheet": "img"
}

def load_ai_export_path():
    with open(AI_CONFIG_PATH, "r") as f:
        return json.load(f)["ai_export_shared_drive_path"]

def transform_normlized_to_enriched(norm_metadata_file_path, filetype_prefix):
    with open(norm_metadata_file_path, "r") as f:
        data = json.load(f)

    enriched = []

    for event_type, detections in data.items():
        sdna_event_type = SDNA_EVENT_MAP.get(event_type, "unknown")

        for detection in detections:
            occurrences = detection.get("occurrences", [])
            enriched.append({
                "eventType": event_type,
                "sdnaEventType": sdna_event_type,
                "sdnaEventTypePrefix": filetype_prefix,
                "eventValue": detection.get("value", ""),
                "totalOccurrences": len(occurrences),
                "eventOccurence": occurrences
            })

    return enriched

def get_node_api_key():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            return parser.get("General", "NodeAPIKey", fallback="")
        else:
            import plistlib
            with open(DNA_CLIENT_SERVICES, "rb") as fp:
                return plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

def resolve_physical_path(ai_export_base, logical_path):
    if "/./" in ai_export_base:
        meta_left, _ = ai_export_base.split("/./", 1)
    else:
        meta_left = ai_export_base
    return os.path.join(meta_left.rstrip("/"), logical_path.lstrip("/"))

def save_extended_metadata(repo_guid, file_path, raw_path=None, norm_path=None, 
                           language_code=None, api_key=None):
    headers = {"apikey": api_key, "Content-Type": "application/json"}
    
    payload = {
        "repoGuid": repo_guid,
        "providerName": "AZUREVI",
        "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default"),
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path)
        }]
    }
    
    if raw_path:
        payload["extendedMetadata"][0]["metadataRawJsonFilePath"] = raw_path
    if norm_path:
        payload["extendedMetadata"][0]["metadataFilePath"] = norm_path
    
    try:
        r = requests.post(NODE_EXTENDED_METADATA_URL, headers=headers, json=payload, timeout=30)
        if r.status_code in (200, 201):
            lang_label = language_code or "Default"
            logging.info(f"✔ Extended metadata record [{lang_label}]: {os.path.basename(file_path)}")
            return True
        logging.error(f"✖ Extended metadata failed [{r.status_code}]: {r.text}")
    except Exception as e:
        logging.error(f"✖ Extended metadata error: {e}")
    return False

def send_ai_enriched_metadata(repo_guid, file_path, norm_path, proxy_prefix, 
                               language_code=None, api_key=None):
    headers = {"apikey": api_key, "Content-Type": "application/json"}
    
    try:
        with open(norm_path, "r") as f:
            norm_data = json.load(f)
    except Exception as e:
        logging.error(f"✖ Failed to read normalized file: {e}")
        return False
    
    ai_enriched = transform_normlized_to_enriched(norm_path, proxy_prefix)
    
    if not ai_enriched:
        logging.warning(f"⚠ No enriched data: {os.path.basename(norm_path)}")
        return False
    
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "aiEnriched": ai_enriched,
        "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default") if language_code else "Default"
    }
    
    try:
        r = requests.post(NODE_AI_ENRICH_URL, headers=headers, json=payload, timeout=30)
        if r.status_code in (200, 201):
            lang_label = language_code or "Default"
            logging.info(f"✔ AI enriched [{lang_label}]: {os.path.basename(file_path)}")
            return True
        logging.error(f"✖ AI enriched failed [{r.status_code}]: {r.text}")
    except Exception as e:
        logging.error(f"✖ AI enriched error: {e}")
    return False

def normalize_raw_json(raw_path, norm_path):
    if not os.path.exists(raw_path):
        logging.error(f"Raw file not found: {raw_path}")
        return False
    
    if not os.path.exists(NORMALIZER_SCRIPT):
        logging.error(f"Normalizer script not found: {NORMALIZER_SCRIPT}")
        return False
    
    os.makedirs(os.path.dirname(norm_path), exist_ok=True)
    
    try:
        cmd = ["python3", NORMALIZER_SCRIPT, "-i", raw_path, "-o", norm_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            logging.info(f"✔ Normalized: {os.path.basename(norm_path)}")
            return True
        logging.error(f"✖ Normalizer failed: {proc.stderr.strip()}")
    except Exception as e:
        logging.error(f"✖ Normalization error: {e}")
    return False

def discover_language_files(base_path):
    base = Path(base_path)
    if not base.exists():
        return [(str(base), "")]
    
    results = [(str(base), "")]
    parent = base.parent
    stem = base.stem
    suffix = base.suffix
    
    for lang_code in LANGUAGE_CODE_MAP.keys():
        if not lang_code:
            continue
        lang_file = parent / f"{stem}_{lang_code}{suffix}"
        if lang_file.exists():
            results.append((str(lang_file), lang_code))
    
    return results

def process_record(record, ai_export_base, proxy_prefix, api_key, normalize_only=False, enrich_only=False):
    repo_guid = record.get("repoGuid")
    file_path = record.get("fullPath")
    raw_path_rel = record.get("metadataRawJsonFilePath")
    norm_path_rel_existing = record.get("metadataFilePath")
    provider = record.get("providerName")
    
    if provider != "AZUREVI":
        logging.debug(f"Skipping non-AZUREVI record: {provider}")
        return False
    
    if not raw_path_rel:
        logging.warning(f"⚠ No raw path for: {file_path}")
        return False
    
    raw_path = resolve_physical_path(ai_export_base, raw_path_rel)
    
    if not enrich_only and not os.path.exists(raw_path):
        logging.warning(f"⚠ Raw file missing: {raw_path}")
        return False
    
    norm_path_rel = raw_path_rel.replace("_raw.json", "_norm.json")
    norm_path = resolve_physical_path(ai_export_base, norm_path_rel)
    
    # Step 1: Normalize if needed (skip if enrich-only mode)
    if not enrich_only:
        if not os.path.exists(norm_path):
            logging.info(f"📝 Normalizing: {os.path.basename(raw_path)}")
            if not normalize_raw_json(raw_path, norm_path):
                return False
        else:
            logging.info(f"⏭ Normalized exists: {os.path.basename(norm_path)}")
    else:
        # In enrich-only mode, use existing normalized path or derive it
        if norm_path_rel_existing:
            norm_path = resolve_physical_path(ai_export_base, norm_path_rel_existing)
        if not os.path.exists(norm_path):
            logging.warning(f"⚠ Normalized file missing for enrich-only: {norm_path}")
            return False
    
    # Step 2: Discover all language files
    lang_files = discover_language_files(norm_path)
    logging.info(f"Found {len(lang_files)} language variant(s)")
    
    success_count = 0
    
    for norm_file, lang_code in lang_files:
        lang_label = lang_code or "Default"
        
        # Determine raw file for this language
        if lang_code:
            raw_base = Path(raw_path)
            raw_file_rel = str(Path(raw_path_rel).parent / f"{raw_base.stem}_{lang_code}{raw_base.suffix}")
        else:
            raw_file_rel = raw_path_rel
        
        norm_file_rel = str(Path(norm_path_rel).parent / Path(norm_file).name)
        
        # Step 3: Save normalized metadata record (skip if enrich-only mode)
        if not enrich_only:
            if not save_extended_metadata(repo_guid, file_path, raw_file_rel, norm_file_rel, lang_code, api_key):
                logging.warning(f"⚠ Failed to save extended metadata [{lang_label}]")
                continue
        
        # Step 4: Generate and save AI enriched (skip if normalize-only mode)
        if not normalize_only:
            if not send_ai_enriched_metadata(repo_guid, file_path, norm_file, proxy_prefix, lang_code, api_key):
                logging.warning(f"⚠ Failed to save AI enriched [{lang_label}]")
                continue
        
        success_count += 1
    
    logging.info(f"✅ Processed {success_count}/{len(lang_files)} languages for: {os.path.basename(file_path)}")
    return success_count > 0

def process_repo(repo_guid, proxy_type, normalize_only=False, enrich_only=False):
    if not normalize_only and proxy_type not in EVENT_TYPE_PROXY_VALUE_MAP:
        logging.error(f"Invalid proxy type: {proxy_type}")
        return
    
    proxy_prefix = EVENT_TYPE_PROXY_VALUE_MAP.get(proxy_type, "vid") if not normalize_only else None
    ai_export_base = load_ai_export_path()
    api_key = get_node_api_key()
    
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]
    
    query = {
        "repoGuid": repo_guid,
        "providerName": "AZUREVI",
        "isDeleted": False
    }
    
    if enrich_only:
        query["metadataFilePath"] = {"$exists": True, "$ne": None}
    else:
        query["metadataRawJsonFilePath"] = {"$exists": True, "$ne": None}
    
    cursor = col.find(query)
    total = col.count_documents(query)
    
    mode = "NORMALIZE ONLY" if normalize_only else ("ENRICH ONLY" if enrich_only else "NORMALIZE + ENRICH")
    logging.info(f"Mode: {mode}")
    logging.info(f"Found {total} AZUREVI records to process")
    
    processed = 0
    success = 0
    
    for record in cursor:
        processed += 1
        logging.info(f"[{processed}/{total}] Processing: {record.get('fileName')}")
        
        if process_record(record, ai_export_base, proxy_prefix, api_key, normalize_only, enrich_only):
            success += 1
    
    logging.info(f"✅ Complete: {success}/{processed} records processed successfully")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Azure Video Indexer Multi-Language Metadata Processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
        # Both normalize and enrich (default)
        %(prog)s --repo-guid REPO123

        # Only normalize raw JSON files
        %(prog)s --repo-guid REPO123 --normalize-only

        # Only enrich (assumes normalized files exist)
        %(prog)s --repo-guid REPO123 --enrich-only --proxy-type video_proxy
                """
    )
    
    parser.add_argument("--repo-guid", required=True, help="Repository GUID to process")
    parser.add_argument("--proxy-type", default="video_proxy", 
                       choices=EVENT_TYPE_PROXY_VALUE_MAP.keys(),
                       help="Proxy type for enrichment (required unless --normalize-only)")
    parser.add_argument("--normalize-only", action="store_true",
                       help="Only normalize raw JSON files, skip enrichment")
    parser.add_argument("--enrich-only", action="store_true",
                       help="Only perform enrichment, skip normalization")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    if args.normalize_only and args.enrich_only:
        logging.error("Cannot specify both --normalize-only and --enrich-only")
        sys.exit(1)
    
    logging.info("=" * 60)
    logging.info("Azure Video Indexer Multi-Language Processor")
    logging.info("=" * 60)
    
    process_repo(args.repo_guid, args.proxy_type, args.normalize_only, args.enrich_only)