import os
import sys
import json
import time
import random
import logging
import plistlib
import argparse
import requests
import subprocess
from configparser import ConfigParser
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------------------------------------
# CONSTANTS
# -------------------------------------------------------------------

AI_CONFIG_PATH = "/opt/sdna/nginx/ai-config.json"
NODE_AI_ENRICH_URL = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
NODE_EXTENDED_METADATA_URL = "http://127.0.0.1:5080/catalogs/extendedMetadata"

LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"

IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "ApiDNA"
MONGO_COLLECTION = "catalogExtendedMetadata"

BATCH_SIZE = 10
MAX_WORKERS = 5
MAX_RETRIES = 3

# -------------------------------------------------------------------
# NORMALIZER SCRIPTS MAP
# -------------------------------------------------------------------

NORMALIZER_SCRIPTS = {
    "azure_vision": "/opt/sdna/bin/azure_vision_metadata_normalizer.py",
    "AWS": "/opt/sdna/bin/rekognition_metadata_normalizer.py",
    "google_vision": "/opt/sdna/bin/google_vision_metadata_normalizer.py",
    "rubicx": "/opt/sdna/bin/rubicx_metadata_normalizer.py"
}

# -------------------------------------------------------------------
# EVENT MAPS
# -------------------------------------------------------------------

SDNA_EVENT_MAP = {
    # video indexer 
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
    "azure_video_speakers": "speakers",

    # twelvelabs
    "twelvelabs_labels": "labels",
    "twelvelabs_keywords": "keywords",
    "twelvelabs_summary": "summary",
    "twelvelabs_highlights": "highlights",
    "twelvelabs_transcript": "transcript",

    # Azure Vision
    "azure_vision_tags" : "tags",
    "azure_vision_objects" : "objects",
    "azure_vision_faces" : "faces",
    "azure_vision_categories" : "categories",
    "azure_vision_description" : "description",
    "azure_vision_brand" : "brands",
    "azure_vision_landmarks" : "landmarks",
   
    # AWS Rekognition
    "aws_rek_detect_labels" : "labels",
    "aws_rek_detect_faces" : "faces",
    "aws_rek_detect_moderation_labels" : "moderation",
    "aws_rek_detect_text" : "ocr",
    "aws_rek_detect_protective_equipment" : "protective_equipment",
    "aws_rek_recognize_celebrities" : "celebrities",
   
    # GCP vision
    "gcp_vision_label_annotations" : "labels",
    "gcp_vision_object_annotations" : "objects",
    "gcp_vision_text_annotations" : "ocr",
    "gcp_vision_face_annotations" : "faces",
    "gcp_vision_safe_search_annotation" : "safe_search",
    "gcp_vision_image_properties_annotation" : "image_color",
    "gcp_vision_web_detection" : "web_detection",    
}

EVENT_TYPE_PROXY_VALUE_MAP = {
    "video_proxy": "vid",
    "video_proxy_sample_scene_change": "frm",
    "video_sample_faces": "img",
    "video_sample_interval": "vid",
    "audio_proxy": "aud",
    "sprite_sheet": "img"
}

# -------------------------------------------------------------------
# CONFIG LOADERS
# -------------------------------------------------------------------

def load_ai_export_path():
    with open(AI_CONFIG_PATH, "r") as f:
        path = json.load(f)["ai_export_shared_drive_path"]
        return path

def resolve_physical_path(ai_export_base, logical_path):
    """
    ai_export_base: path containing /./
    logical_path: path stored in DB (post /./)
    """
    if "/./" in ai_export_base:
        meta_left, meta_right = ai_export_base.split("/./", 1)
    else:
        meta_left, meta_right = ai_export_base, ""

    meta_left = meta_left.rstrip("/")

    logical_path = logical_path.lstrip("/")

    if meta_right and logical_path.startswith(meta_right.lstrip("/") + "/"):
        return os.path.join(meta_left, logical_path)

    return os.path.join(meta_left, meta_right, logical_path)


def get_node_api_key():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            api_key = parser.get("General", "NodeAPIKey", fallback="")
        else:
            with open(DNA_CLIENT_SERVICES, "rb") as fp:
                api_key = plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

    if not api_key:
        logging.error("Node API key not found.")
        sys.exit(5)

    return api_key


NODE_API_KEY = get_node_api_key()

# -------------------------------------------------------------------
# NORMALIZATION LOGIC
# -------------------------------------------------------------------

def normalize_json(provider, raw_json_path, norm_json_path, azure_vision_version=None):
    """
    Converts raw JSON to normalized JSON using provider-specific normalizer scripts.
    """
    if not provider:
        logging.error("No provider specified for normalization.")
        return False
    
    normalizer = NORMALIZER_SCRIPTS.get(provider)
    if not normalizer:
        logging.error(f"No normalizer script for provider: {provider}")
        return False
    
    if not os.path.isfile(normalizer):
        logging.error(f"Normalizer script not found: {normalizer}")
        return False
    
    try:
        cmd = ["python3", normalizer, "-i", raw_json_path, "-o", norm_json_path]
        if provider == "azure_vision" and azure_vision_version:
            cmd.extend(["--version", azure_vision_version])
        
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            logging.info(f"✔ Normalized JSON saved to {norm_json_path}")
            return True
        
        logging.error(f"✖ Normalizer failed [{proc.returncode}]: {proc.stderr.strip()}")
    except Exception as e:
        logging.error(f"✖ Error normalizing JSON: {e}")
    
    return False


def save_extended_metadata(repo_guid, provider, file_path, raw_metadata_path=None, 
                          norm_metadata_path=None, max_attempts=3):
    """
    Saves/updates extended metadata record in MongoDB via Node API.
    """
    headers = {"apikey": NODE_API_KEY, "Content-Type": "application/json"}
    
    payload = {
        "repoGuid": repo_guid,
        "providerName": provider,
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path)
        }]
    }
    
    if raw_metadata_path is not None:
        payload["extendedMetadata"][0]["metadataRawJsonFilePath"] = raw_metadata_path
    if norm_metadata_path is not None:
        payload["extendedMetadata"][0]["metadataFilePath"] = norm_metadata_path
    
    for attempt in range(max_attempts):
        try:
            r = requests.post(NODE_EXTENDED_METADATA_URL, headers=headers, json=payload, timeout=30)
            if r and r.status_code in (200, 201):
                logging.info(f"✔ Updated metadata record for: {os.path.basename(file_path)}")
                return True
            
            if r.status_code >= 500:
                raise RuntimeError(f"Node 5xx: {r.status_code}")
                
        except Exception as e:
            if attempt == max_attempts - 1:
                logging.critical(f"✖ Failed to save metadata after {max_attempts} attempts: {e}")
                return False
            
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    
    return False

# -------------------------------------------------------------------
# ENRICHMENT LOGIC
# -------------------------------------------------------------------

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


def send_ai_enriched_metadata(record, ai_export_base, proxy_prefix):
    # Clean up path - remove leading ./ if present
    norm_path_relative = record["metadataFilePath"].lstrip('./')
    norm_path = resolve_physical_path(ai_export_base, norm_path_relative)

    if not os.path.exists(norm_path):
        logging.warning(f"⚠ Missing normalized JSON: {norm_path}")
        return False

    enriched = transform_normlized_to_enriched(norm_path, proxy_prefix)

    payload = {
        "repoGuid": record["repoGuid"],
        "fullPath": record["fullPath"],
        "fileName": record["fileName"],
        "providerName": record["providerName"],
        "normalizedMetadata": enriched
    }

    headers = {
        "apikey": NODE_API_KEY,
        "Content-Type": "application/json"
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                NODE_AI_ENRICH_URL,
                headers=headers,
                json=payload,
                timeout=30
            )

            if r.status_code in (200, 201):
                logging.info(f"✔ Enriched: {record['fileName']}")
                return True

            if r.status_code >= 500:
                raise RuntimeError(f"Node 5xx: {r.status_code}")

            logging.error(f"✖ Node rejected [{r.status_code}]: {r.text}")
            return False

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logging.critical(f"✖ Failed after retries: {record['fileName']} → {e}")
                return False

            time.sleep((2 ** attempt) + random.uniform(0, 1))

    return False

# -------------------------------------------------------------------
# DB + BATCH PROCESSING
# -------------------------------------------------------------------

def fetch_records_with_raw_metadata(repo_guid):
    """
    Fetches all records that have raw metadata files but may or may not have normalized ones.
    """
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    return col.find({
        "repoGuid": repo_guid,
        "isDeleted": False,
        "metadataRawJsonFilePath": {"$exists": True, "$ne": None}
    })


def chunked_cursor(cursor, size):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def process_normalization(repo_guid, azure_vision_version=None):
    """
    Step 1: Find all records with raw JSON, generate normalized JSON, and update MongoDB.
    """
    ai_export_base = load_ai_export_path()
    cursor = fetch_records_with_raw_metadata(repo_guid)
    
    processed_count = 0
    success_count = 0
    
    logging.info(f"Starting normalization for repo: {repo_guid}")
    
    for record in cursor:
        processed_count += 1
        
        raw_path_relative = record.get("metadataRawJsonFilePath")
        if not raw_path_relative:
            logging.warning(f"⚠ No raw path for record: {record.get('fileName')}")
            continue
        
        # Clean up path - remove leading ./ if present
        # Split ai_export_base at /./ to get the left part (physical path)
        if "/./" in ai_export_base:
            meta_left, meta_right = ai_export_base.split("/./", 1)
        else:
            meta_left, meta_right = ai_export_base, ""

        meta_left = meta_left.rstrip("/")

        # Build full physical path: meta_left/meta_right/raw_path_relative
        raw_path_full = resolve_physical_path(ai_export_base, raw_path_relative)
        
        if not os.path.exists(raw_path_full):
            logging.warning(f"⚠ Raw file not found: {raw_path_full}")
            continue
        
        # Generate normalized path (replace _raw.json with _norm.json)
        if raw_path_relative.endswith("_raw.json"):
            norm_path_relative = raw_path_relative.replace("_raw.json", "_norm.json")
        else:
            # Fallback if naming convention is different
            base, ext = os.path.splitext(raw_path_relative)
            norm_path_relative = f"{base}_norm{ext}"
        
        norm_path_full = resolve_physical_path(ai_export_base, norm_path_relative)
        
        # Skip if normalized file already exists
        if os.path.exists(norm_path_full):
            logging.info(f"⏭ Normalized file already exists: {norm_path_relative}")
            # Ensure it's recorded in MongoDB
            save_extended_metadata(
                repo_guid=repo_guid,
                provider=record.get("providerName"),
                file_path=record.get("fullPath"),
                norm_metadata_path=norm_path_relative
            )
            success_count += 1
            continue
        
        # Normalize the JSON
        provider = record.get("providerName")
        logging.info(f"📝 Normalizing [{provider}]: {os.path.basename(raw_path_full)}")
        
        if normalize_json(provider, raw_path_full, norm_path_full, azure_vision_version):
            # Update MongoDB with normalized path
            if save_extended_metadata(
                repo_guid=repo_guid,
                provider=provider,
                file_path=record.get("fullPath"),
                norm_metadata_path=norm_path_relative
            ):
                success_count += 1
        else:
            logging.error(f"✖ Failed to normalize: {raw_path_relative}")
    
    logging.info(f"Normalization complete: {success_count}/{processed_count} records processed successfully")
    return success_count, processed_count


def process_enrichment(repo_guid, proxy_type):
    """
    Step 2: Generate AI enriched metadata from normalized JSON files.
    """
    ai_export_base = load_ai_export_path()

    if proxy_type not in EVENT_TYPE_PROXY_VALUE_MAP:
        logging.error(f"Invalid proxy type: {proxy_type}")
        return

    proxy_prefix = EVENT_TYPE_PROXY_VALUE_MAP[proxy_type]

    # Fetch records that have normalized metadata
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]
    
    cursor = col.find({
        "repoGuid": repo_guid,
        "isDeleted": False,
        "metadataFilePath": {"$exists": True, "$ne": None}
    })

    processed_count = 0
    success_count = 0

    for batch in chunked_cursor(cursor, BATCH_SIZE):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    send_ai_enriched_metadata,
                    record,
                    ai_export_base,
                    proxy_prefix
                )
                for record in batch
            ]

            for f in as_completed(futures):
                processed_count += 1
                if f.result():
                    success_count += 1

    logging.info(f"Enrichment complete: {success_count}/{processed_count} records enriched successfully")

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="AI Metadata Normalization and Enrichment Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Only normalize raw JSON files
  %(prog)s --repo-guid REPO123 --normalize-only

  # Normalize and then enrich
  %(prog)s --repo-guid REPO123 --proxy-type video_proxy

  # Only enrich (assumes normalized files exist)
  %(prog)s --repo-guid REPO123 --enrich-only --proxy-type video_proxy
        """
    )

    parser.add_argument(
        "--repo-guid",
        required=True,
        help="Repository GUID to process"
    )

    parser.add_argument(
        "--normalize-only",
        action="store_true",
        help="Only normalize raw JSON files, skip enrichment"
    )

    parser.add_argument(
        "--enrich-only",
        action="store_true",
        help="Only perform enrichment, skip normalization"
    )

    parser.add_argument(
        "--proxy-type",
        default="video_proxy",
        choices=EVENT_TYPE_PROXY_VALUE_MAP.keys(),
        help="Proxy type for event prefix mapping (required for enrichment)"
    )

    parser.add_argument(
        "--azure-vision-version",
        help="Azure Vision API version (if using azure_vision provider)"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    return parser.parse_args()

# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    if args.normalize_only and args.enrich_only:
        logging.error("Cannot specify both --normalize-only and --enrich-only")
        sys.exit(1)

    # Step 1: Normalization
    if not args.enrich_only:
        logging.info("=" * 60)
        logging.info("STEP 1: NORMALIZATION")
        logging.info("=" * 60)
        process_normalization(args.repo_guid, args.azure_vision_version)

    # Step 2: Enrichment
    if not args.normalize_only:
        logging.info("=" * 60)
        logging.info("STEP 2: ENRICHMENT")
        logging.info("=" * 60)
        process_enrichment(args.repo_guid, args.proxy_type)

    logging.info("✅ Processing complete!")