import os
import sys
import json
import time
import random
import logging
import plistlib
import argparse
import requests
from configparser import ConfigParser
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------------------------------------
# CONSTANTS
# -------------------------------------------------------------------

AI_CONFIG_PATH = "/opt/sdna/nginx/ai-config.json"
NODE_AI_ENRICH_URL = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"

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
        return json.load(f)["ai_export_shared_drive_path"]


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
# CORE LOGIC
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
    norm_path = os.path.join(ai_export_base, record["metadataFilePath"])

    if not os.path.exists(norm_path):
        logging.warning(f"Missing normalized JSON: {norm_path}")
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

def fetch_catalog_records(repo_guid):
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    return col.find({
        "repoGuid": repo_guid,
        "isDeleted": False
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


def process_records(repo_guid, proxy_type):
    ai_export_base = load_ai_export_path()

    if proxy_type not in EVENT_TYPE_PROXY_VALUE_MAP:
        logging.error(f"Invalid proxy type: {proxy_type}")
        sys.exit(2)

    proxy_prefix = EVENT_TYPE_PROXY_VALUE_MAP[proxy_type]

    cursor = fetch_catalog_records(repo_guid)

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
                f.result()

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="AI Metadata Enrichment Runner")

    parser.add_argument(
        "--repo-guid",
        required=True,
        help="Repository GUID to process"
    )

    parser.add_argument(
        "--proxy-type",
        default="video_proxy",
        choices=EVENT_TYPE_PROXY_VALUE_MAP.keys(),
        help="Proxy type for event prefix mapping"
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

    process_records(args.repo_guid, args.proxy_type)
