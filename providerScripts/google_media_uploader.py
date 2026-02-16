import os
import subprocess
import sys
import json
import argparse
import logging
import random
import time
import uuid
from configparser import ConfigParser
import plistlib
import base64

# GCP Imports
from google.cloud import vision
from google.cloud import videointelligence_v1 as videointelligence
from google.api_core import operations_v1
from google.api_core import operation as gapic_operation
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
from google.protobuf.json_format import MessageToDict
from google.oauth2 import service_account
import requests

# ============================================================================
# CONSTANTS
# ============================================================================

VALID_MODES = ["proxy", "original", "get_base_target", "send_extracted_metadata", "analyze_and_embed"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/gcp_media_metadata_normalizer.py"

# GCP Video Intelligence feature mapping
VIDEO_FEATURE_MAP = {
    "LABEL_DETECTION": videointelligence.Feature.LABEL_DETECTION,
    "SHOT_CHANGE_DETECTION": videointelligence.Feature.SHOT_CHANGE_DETECTION,
    "EXPLICIT_CONTENT_DETECTION": videointelligence.Feature.EXPLICIT_CONTENT_DETECTION,
    "OBJECT_TRACKING": videointelligence.Feature.OBJECT_TRACKING,
    "TEXT_DETECTION": videointelligence.Feature.TEXT_DETECTION,
    "LOGO_RECOGNITION": videointelligence.Feature.LOGO_RECOGNITION,
    "FACE_DETECTION": videointelligence.Feature.FACE_DETECTION,
    "SPEECH_TRANSCRIPTION": videointelligence.Feature.SPEECH_TRANSCRIPTION,
    "PERSON_DETECTION": videointelligence.Feature.PERSON_DETECTION,
    "FEATURE_UNSPECIFIED": videointelligence.Feature.FEATURE_UNSPECIFIED
}

SDNA_EVENT_MAP = {
    "gcp_video_labels": "labels",
    "gcp_video_keywords": "keywords",
    "gcp_video_summary": "summary",
    "gcp_video_highlights": "highlights",
    "gcp_video_transcript": "transcript",
    "gcp_vision_labels": "labels",
    "gcp_vision_text": "ocr",
    "gcp_vision_faces": "faces",
}

# Multipart upload chunk size (32MB for optimal performance)
MULTIPART_CHUNK_SIZE = 32 * 1024 * 1024

logger = logging.getLogger()

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    
    # Reduce noise from Google libraries
    logging.getLogger('google').setLevel(logging.ERROR)
    logging.getLogger('google.api_core').setLevel(logging.ERROR)
    logging.getLogger('google.auth').setLevel(logging.ERROR)
    
    logging.info(f"Log level set to: {level.upper()}")

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def get_cloud_config_path():
    logging.debug("Determining cloud config path based on platform")
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            path = plistlib.load(fp)["CloudConfigFolder"] + "/cloud_targets.conf"
    logging.info(f"Using cloud config path: {path}")
    return path

def get_link_address_and_port():
    logging.debug(f"Reading server configuration from: {SERVERS_CONF_PATH}")
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
            logging.debug(f"Successfully read {len(lines)} lines from config")

        if IS_LINUX:
            for line in lines:
                if '=' in line:
                    key, value = map(str.strip, line.split('=', 1))
                    if key.endswith('link_address'):
                        ip = value
                    elif key.endswith('link_port'):
                        port = value
        else:
            for i, line in enumerate(lines):
                if "<key>link_address</key>" in line:
                    ip = lines[i + 1].split(">")[1].split("<")[0].strip()
                elif "<key>link_port</key>" in line:
                    port = lines[i + 1].split(">")[1].split("<")[0].strip()
    except Exception as e:
        logging.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)

    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json" if IS_LINUX else "/Library/Application Support/StorageDNA/nginx/ai-config.json"
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            metadata_store_path = config_data.get("ai_export_shared_drive_path", "")
            proxy_store_path = config_data.get("ai_proxy_shared_drive_path", "")
            logging.info(f"Metadata Store Path: {metadata_store_path}, Proxy Store Path: {proxy_store_path}")
        
    except Exception as e:
        logging.error(f"Error reading Metadata Store or Proxy Store settings: {e}")
        sys.exit(5)

    if not metadata_store_path or not proxy_store_path:
        logging.info("Store settings not found.")
        sys.exit(5)

    return metadata_store_path, proxy_store_path

def get_admin_dropbox_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            log_path = parser.get('General', 'LogPath', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                log_path = str(cfg.get("LogPath", "")).strip()
        return log_path or None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def get_retry_session(retries=3, backoff_factor_range=(1.0, 2.0)):
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exception = e
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. "
                                f"Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None

# ============================================================================
# CONFIGURATION LOADING
# ============================================================================

def get_advanced_ai_config(config_name, provider_name="gcp"):
    """
    Load unified AI configuration for both Google Vision and Video Intelligence.
    Returns normalized config dict with provider-specific settings.
    """
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return None

    provider_key = provider_name.lower()

    for src in [
        os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json"),
        os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    ]:
        if os.path.exists(src):
            try:
                with open(src, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                    if not isinstance(raw, dict):
                        continue

                    out = {}
                    
                    # Map input keys to canonical keys (strip optional provider_ prefix)
                    for key, val in raw.items():
                        clean_key = key
                        if key.startswith(f"{provider_key}_"):
                            clean_key = key[len(provider_key) + 1:]
                        
                        # Accept keys for both Vision AI and Video Intelligence
                        if clean_key.startswith("vision_") or clean_key.startswith("video_intelligence_") or \
                           clean_key.startswith("video_") or clean_key in {
                            "vision_ai_enabled", "video_intelligence_enabled", 
                            "vision_features", "video_intelligence_features",
                            "vision_max_results_per_feature", "video_gcs_upload_prefix",
                            "video_max_file_size_for_inline", "video_language_code",
                            "video_enable_automatic_punctuation", "video_enable_speaker_diarization",
                            "video_filter_profanity", "video_speech_contexts", "video_model"
                        }:
                            out[clean_key] = val

                    # Normalize boolean flags
                    for bool_key in ["vision_ai_enabled", "video_intelligence_enabled", 
                                     "video_enable_automatic_punctuation", "video_enable_speaker_diarization", 
                                     "video_filter_profanity"]:
                        if bool_key in out:
                            v = out[bool_key]
                            out[bool_key] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes", "on")

                    # Normalize Vision features
                    if "vision_features" in out:
                        feats = out["vision_features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        out["vision_features"] = [
                            f.upper() if isinstance(f, str) else f for f in feats
                        ]

                    # Normalize Video Intelligence features
                    if "video_intelligence_features" in out:
                        feats = out["video_intelligence_features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        out["video_intelligence_features"] = [
                            f.upper() if isinstance(f, str) else f for f in feats
                        ]

                    # Normalize speech_contexts
                    if "video_speech_contexts" in out and isinstance(out["video_speech_contexts"], str):
                        try:
                            out["video_speech_contexts"] = json.loads(out["video_speech_contexts"])
                        except:
                            out["video_speech_contexts"] = []

                    # Normalize numeric fields
                    for num_key in ["vision_max_results_per_feature", "video_max_file_size_for_inline"]:
                        if num_key in out and isinstance(out[num_key], str):
                            try:
                                out[num_key] = float(out[num_key]) if '.' in out[num_key] else int(out[num_key])
                            except ValueError:
                                del out[num_key]

                    logging.info(f"Loaded and normalized advanced AI config from: {src}")
                    return out

            except Exception as e:
                logging.error(f"Failed to load {src}: {e}")

    return None

def get_node_api_key():
    api_key = ""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            api_key = parser.get('General', 'NodeAPIKey', fallback='')
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                api_key = plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

    if not api_key:
        logging.error("Node API key not found in configuration.")
        sys.exit(5)

    logging.info("Successfully retrieved Node API key.")
    return api_key

def get_gcp_credentials(config):
    """Parse GCP credentials from config token field"""
    token_str = config.get("token", "").strip()
    if not token_str:
        logging.error("Missing 'token' in cloud config")
        return None

    try:
        if token_str.startswith("{"):
            credentials_info = json.loads(token_str)
        else:
            with open(token_str, "r") as f:
                credentials_info = json.load(f)
    except Exception as e:
        logging.error(f"Failed to parse GCP credentials: {e}")
        return None

    try:
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        return credentials
    except Exception as e:
        logging.error(f"Invalid service account info: {e}")
        return None

# ============================================================================
# GCS STORAGE FUNCTIONS
# ============================================================================

def upload_to_gcs(credentials, bucket_name, source_path, gcs_path, use_multipart=True):
    """
    Upload file to Google Cloud Storage with optional multipart upload for large files.
    """
    try:
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        file_size = os.path.getsize(source_path)
        logging.info(f"Uploading {source_path} ({file_size / 1024 / 1024:.2f} MB) to gs://{bucket_name}/{gcs_path}")
        
        # Use multipart upload for files larger than chunk size
        if use_multipart and file_size > MULTIPART_CHUNK_SIZE:
            blob.chunk_size = MULTIPART_CHUNK_SIZE
            logging.debug(f"Using multipart upload with chunk size: {MULTIPART_CHUNK_SIZE / 1024 / 1024:.2f} MB")
        
        blob.upload_from_filename(source_path)
        
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        logging.info(f"Upload successful: {gcs_uri}")
        return gcs_uri
        
    except google_exceptions.NotFound as e:
        logging.error(f"Bucket not found: {bucket_name}")
        return None
    except google_exceptions.Forbidden as e:
        logging.error(f"Access denied to bucket: {bucket_name}")
        return None
    except Exception as e:
        logging.error(f"GCS upload failed: {e}")
        return None

def delete_from_gcs(credentials, bucket_name, gcs_path):
    """Delete file from Google Cloud Storage"""
    try:
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.delete()
        logging.info(f"Deleted gs://{bucket_name}/{gcs_path}")
        return True
    except Exception as e:
        logging.warning(f"Failed to delete GCS file: {e}")
        return False

# ============================================================================
# CATALOG API FUNCTIONS
# ============================================================================

def update_catalog(repo_guid, file_path, operation_name_or_data):
    """Update catalog with provider data (operation name for video, or bucket info for original upload)"""
    url = "http://127.0.0.1:5080/catalogs/providerData"
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    
    # Handle both operation name (string) and full provider data (dict)
    if isinstance(operation_name_or_data, str):
        provider_data = {"operation_name": operation_name_or_data}
    else:
        provider_data = operation_name_or_data
    
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": "gcp_media",
        "providerData": provider_data
    }
    
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            logging.debug(f"[Attempt {attempt+1}/{max_attempts}] Starting catalog update request to {url}")
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response is None:
                logging.error(f"[Attempt {attempt+1}] Response is None!")
                time.sleep(5)
                continue
            
            try:
                resp_json = response.json() if response.text.strip() else {}
            except Exception as e:
                logging.warning(f"Failed to parse response JSON: {e}")
                resp_json = {}
            
            if response.status_code in (200, 201):
                logging.info("Catalog updated successfully.")
                return True
            
            if response.status_code == 404:
                logging.info("[404 DETECTED] Entering 404 handling block")
                message = resp_json.get('message', '')
                logging.debug(f"[404] Raw message from response: [{repr(message)}]")
                clean_message = message.strip().lower()
                logging.debug(f"[404] Cleaned message: [{repr(clean_message)}]")

                if clean_message == "catalog item not found":
                    wait_time = 60 + (attempt * 10)
                    logging.warning(f"[404] Known 'not found' case. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"[404] Unexpected message: {message}")
                    break
            else:
                logging.warning(f"[Attempt {attempt+1}] Non-404 error status: {response.status_code}")

        except Exception as e:
            logging.exception(f"[Attempt {attempt+1}] Unexpected exception in update_catalog: {e}")
        
        if attempt < max_attempts - 1:
            fallback_delay = 5 + attempt * 2
            logging.debug(f"Sleeping {fallback_delay}s before next attempt")
            time.sleep(fallback_delay)

    return False

def send_extracted_metadata(config, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, max_attempts=3):
    """Send extracted metadata paths to catalog API"""
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}

    metadata_array = []
    if normMetadataFilePath is not None:
        metadata_array.append({
            "type": "metadataFilePath",
            "path": normMetadataFilePath
        })
    if rawMetadataFilePath is not None:
        metadata_array.append({
            "type": "metadataRawJsonFilePath", 
            "path": rawMetadataFilePath
        })

    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "gcp_media"),
        "sourceLanguage": "Default",
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": metadata_array
        }]
    }
    
    for attempt in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to send metadata after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return False

def send_ai_enriched_metadata(config, repo_guid, file_path, enrichedMetadata, max_attempts=3):
    """Send AI enriched metadata to catalog API"""
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "gcp_media"),
        "sourceLanguage": "Default",
        "normalizedMetadata": enrichedMetadata
    }

    for attempt in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to send metadata after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return False

# ============================================================================
# METADATA PROCESSING
# ============================================================================

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    """Store metadata files (raw and normalized) in configured paths"""
    meta_path, proxy_path = get_store_paths()
    if not meta_path:
        logging.error("Metadata Store settings not found.")
        return None, None

    provider = config.get("provider", "gcp_media")
    base = os.path.splitext(os.path.basename(file_path))[0]
    repo_guid_str = str(repo_guid)

    # Split meta_path at /./
    if "/./" in meta_path:
        meta_left, meta_right = meta_path.split("/./", 1)
    else:
        meta_left, meta_right = meta_path, ""

    meta_left = meta_left.rstrip("/")

    # Build physical path
    metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)

    # Full local physical paths
    raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
    norm_json = os.path.join(metadata_dir, f"{base}_norm.json")

    # Returned paths (AFTER /./)
    raw_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_raw.json")
    norm_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_norm.json")

    raw_success = False
    norm_success = False

    # Write metadata with retry
    for attempt in range(max_attempts):
        try:
            # RAW write
            with open(raw_json, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2, default=str)
            raw_success = True

            # NORMALIZED write
            if not get_normalized_metadata(raw_json, norm_json):
                logging.error("Normalization failed.")
                norm_success = False
            else:
                norm_success = True

            break

        except Exception as e:
            logging.warning(f"Metadata write failed (Attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))

    return (
        raw_return if raw_success else None,
        norm_return if norm_success else None
    )

def get_normalized_metadata(raw_json_path, normalized_json_path):
    """Run normalizer script to convert raw metadata to normalized format"""
    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logging.error(f"Normalizer script not found: {NORMALIZER_SCRIPT_PATH}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, NORMALIZER_SCRIPT_PATH, raw_json_path, normalized_json_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            logging.info("Metadata normalization successful")
            return True
        else:
            logging.error(f"Normalizer failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error("Normalizer timed out")
        return False
    except Exception as e:
        logging.error(f"Error running normalizer: {e}")
        return False

def transform_normlized_to_enriched(normalized_json_path, enrich_prefix=None):
    """Transform normalized metadata into enriched format for AI services"""
    try:
        with open(normalized_json_path, 'r', encoding='utf-8') as f:
            normalized_data = json.load(f)
    except Exception as e:
        return f"Failed to read normalized metadata: {e}", False

    if not isinstance(normalized_data, dict):
        return "Normalized metadata is not a dictionary", False

    enriched_metadata = []
    prefix = enrich_prefix if enrich_prefix else "gcp_media"

    for event_type, event_key in SDNA_EVENT_MAP.items():
        if event_key in normalized_data and normalized_data[event_key]:
            enriched_metadata.append({
                "sdnaEventType": f"{prefix}_{event_key}",
                "data": normalized_data[event_key]
            })

    if not enriched_metadata:
        return "No enrichment data generated", False

    return enriched_metadata, True

# ============================================================================
# GOOGLE VISION AI (IMAGE ANALYSIS)
# ============================================================================

def get_vision_feature_list(ai_config):
    """Get list of Vision API features to request"""
    ai_config = ai_config or {}
    enabled = bool(ai_config.get("vision_ai_enabled", False))
    if not enabled:
        logging.info("GCP Vision AI not enabled in ai_config; skipping feature extraction")
        return []

    feature_map = {
        "LABEL_DETECTION": vision.Feature.Type.LABEL_DETECTION,
        "TEXT_DETECTION": vision.Feature.Type.TEXT_DETECTION,
        "LOGO_DETECTION": vision.Feature.Type.LOGO_DETECTION,
        "OBJECT_LOCALIZATION": vision.Feature.Type.OBJECT_LOCALIZATION,
        "FACE_DETECTION": vision.Feature.Type.FACE_DETECTION,
        "LANDMARK_DETECTION": vision.Feature.Type.LANDMARK_DETECTION,
        "SAFE_SEARCH_DETECTION": vision.Feature.Type.SAFE_SEARCH_DETECTION,
        "IMAGE_PROPERTIES": vision.Feature.Type.IMAGE_PROPERTIES,
        "CROP_HINTS": vision.Feature.Type.CROP_HINTS,
        "WEB_DETECTION": vision.Feature.Type.WEB_DETECTION,
        "TYPE_UNSPECIFIED": vision.Feature.Type.TYPE_UNSPECIFIED,
        "DOCUMENT_TEXT_DETECTION": vision.Feature.Type.DOCUMENT_TEXT_DETECTION,
        "PRODUCT_SEARCH": vision.Feature.Type.PRODUCT_SEARCH
    }

    requested_features = ai_config.get("vision_features", [])
    # Empty list → use all features
    if isinstance(requested_features, list) and len(requested_features) == 0:
        requested_features = list(feature_map.keys())

    features = []
    max_results = ai_config.get("vision_max_results_per_feature", 100)
    for feat_name in requested_features:
        if feat_name in feature_map:
            features.append(vision.Feature(type_=feature_map[feat_name], max_results=max_results))
        else:
            logging.warning(f"Ignoring unknown feature: {feat_name}")

    # Fallback to LABEL_DETECTION
    if not features:
        features.append(vision.Feature(type_=vision.Feature.Type.LABEL_DETECTION, max_results=max_results))

    return features

def merge_feature_results(per_feature_results):
    """Merge per-feature Vision API responses into flat structure"""
    annotation_fields = {
        "label_annotations", "text_annotations", "logo_annotations",
        "localized_object_annotations", "face_annotations", "landmark_annotations",
        "safe_search_annotation", "image_properties_annotation", "crop_hints_annotation",
        "web_detection", "full_text_annotation", "product_search_results"
    }
    
    # Pre-init list fields
    merged = {f: [] for f in annotation_fields if f.endswith("_annotations")}
    
    for feature_name, feature_response in per_feature_results.items():
        if "error" in feature_response:
            logging.warning(f"Skipping {feature_name}: {feature_response['error']}")
            continue
        
        for field in annotation_fields:
            value = feature_response.get(field)
            if value is None:
                continue
            
            if isinstance(value, list):
                merged[field].extend(value)
            elif field not in merged:  # Singular fields
                merged[field] = value
    
    return merged

def analyze_image_gcp(config, image_data, ai_config, max_retries=3):
    """Analyze image using Google Vision API"""
    credentials = get_gcp_credentials(config)
    if not credentials:
        logging.error("Failed to get GCP credentials")
        return None

    client = vision.ImageAnnotatorClient(credentials=credentials)
    image = vision.Image(content=image_data)
    features = get_vision_feature_list(ai_config)

    if not features:
        logging.warning("No valid features to process")
        return {}

    # Feature enum → string mapping
    reverse_feature_map = {
        vision.Feature.Type.LABEL_DETECTION: "LABEL_DETECTION",
        vision.Feature.Type.TEXT_DETECTION: "TEXT_DETECTION",
        vision.Feature.Type.LOGO_DETECTION: "LOGO_DETECTION",
        vision.Feature.Type.OBJECT_LOCALIZATION: "OBJECT_LOCALIZATION",
        vision.Feature.Type.FACE_DETECTION: "FACE_DETECTION",
        vision.Feature.Type.LANDMARK_DETECTION: "LANDMARK_DETECTION",
        vision.Feature.Type.SAFE_SEARCH_DETECTION: "SAFE_SEARCH_DETECTION",
        vision.Feature.Type.IMAGE_PROPERTIES: "IMAGE_PROPERTIES",
        vision.Feature.Type.CROP_HINTS: "CROP_HINTS",
        vision.Feature.Type.WEB_DETECTION: "WEB_DETECTION",
        vision.Feature.Type.DOCUMENT_TEXT_DETECTION: "DOCUMENT_TEXT_DETECTION",
        vision.Feature.Type.PRODUCT_SEARCH: "PRODUCT_SEARCH",
    }

    per_feature_results = {}

    for feature in features:
        feature_name = reverse_feature_map.get(feature.type_, f"UNKNOWN_{feature.type_}")
        logging.debug(f"Processing: {feature_name}")

        for attempt in range(max_retries):
            try:
                response = client.annotate_image({"image": image, "features": [feature]})

                if response.error and response.error.message:
                    raise Exception(response.error.message)

                serialized = MessageToDict(
                    response._pb,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )

                per_feature_results[feature_name] = serialized
                break

            except (google_exceptions.ResourceExhausted, google_exceptions.RetryError) as e:
                if attempt == max_retries - 1:
                    per_feature_results[feature_name] = {"error": f"Quota exceeded: {str(e)}"}
                    logging.warning(f"{feature_name} failed after retries: {e}")
                else:
                    wait_time = (attempt + 1) * 2 + random.uniform(0, 2)
                    logging.debug(f"Retrying {feature_name} in {wait_time:.1f}s...")
                    time.sleep(wait_time)

            except Exception as e:
                if attempt == max_retries - 1:
                    per_feature_results[feature_name] = {"error": f"Processing failed: {str(e)}"}
                    logging.warning(f"{feature_name} failed: {e}")
                else:
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    logging.debug(f"Retrying {feature_name} in {delay:.1f}s...")
                    time.sleep(delay)

    merged_result = merge_feature_results(per_feature_results)
    logging.info("GCP Vision analysis completed and merged")
    return merged_result

# ============================================================================
# GOOGLE VIDEO INTELLIGENCE
# ============================================================================

def merge_annotation_results(annotation_results):
    """Merge multiple video annotation results into one"""
    if not annotation_results or len(annotation_results) == 0:
        return {}
    
    if len(annotation_results) == 1:
        return annotation_results[0]

    merged = dict(annotation_results[0])

    list_fields = [
        "segment_label_annotations",
        "shot_label_annotations", 
        "face_detection_annotations",
        "shot_annotations",
        "text_annotations",
        "object_annotations",
        "logo_recognition_annotations",
        "person_detection_annotations",
        "speech_transcriptions"
    ]
    object_fields = [
        "explicit_annotation"
    ]
    
    # Merge all subsequent results
    for result in annotation_results[1:]:
        # Merge list fields by extending
        for field in list_fields:
            if field in result and result[field]:
                if field not in merged:
                    merged[field] = []
                merged[field].extend(result[field])
        
        # Merge object fields (take non-empty one)
        for field in object_fields:
            if field in result and result[field]:
                if field not in merged or not merged[field]:
                    merged[field] = result[field]
        
        # Merge segment info if present
        if "segment" in result and result["segment"]:
            if "segment" not in merged or not merged["segment"]:
                merged["segment"] = result["segment"]
    
    # Clean up empty list fields
    for field in list_fields:
        if field in merged and not merged[field]:
            merged[field] = []
    
    logging.info(f"Merged {len(annotation_results)} annotation results into single result")
    return merged

def get_video_feature_list(ai_config):
    """Get list of Video Intelligence features to request"""
    ai_config = ai_config or {}
    enabled = bool(ai_config.get("video_intelligence_enabled", False))
    if not enabled:
        logging.info("GCP Video Intelligence not enabled in ai_config; skipping feature extraction")
        return []

    requested_features = ai_config.get("video_intelligence_features", [])
    
    # Empty list means use all features
    if isinstance(requested_features, list) and len(requested_features) == 0:
        requested_features = list(VIDEO_FEATURE_MAP.keys())

    features = []
    for feat_name in requested_features:
        if feat_name in VIDEO_FEATURE_MAP:
            features.append(VIDEO_FEATURE_MAP[feat_name])
        else:
            logging.warning(f"Ignoring unknown feature: {feat_name}")

    # Fallback to LABEL_DETECTION
    if not features:
        features.append(videointelligence.Feature.LABEL_DETECTION)

    return features

def build_video_context(ai_config, features):
    """Build VideoContext with feature-specific configurations"""
    video_context = videointelligence.VideoContext()
    
    # Speech transcription config
    if videointelligence.Feature.SPEECH_TRANSCRIPTION in features:
        speech_config = videointelligence.SpeechTranscriptionConfig()
        speech_config.language_code = ai_config.get("video_language_code", "en-US")
        
        # Set automatic punctuation if requested
        if ai_config.get("video_enable_automatic_punctuation", False):
            speech_config.enable_automatic_punctuation = True
        
        # Set speaker diarization if requested
        if ai_config.get("video_enable_speaker_diarization", False):
            speech_config.enable_speaker_diarization = True
            # Optionally set speaker count
            speaker_count = ai_config.get("video_diarization_speaker_count")
            if speaker_count:
                speech_config.diarization_speaker_count = int(speaker_count)
        
        # Set max alternatives if specified
        max_alternatives = ai_config.get("video_max_alternatives")
        if max_alternatives:
            speech_config.max_alternatives = int(max_alternatives)
        
        # Set filter profanity if requested
        if ai_config.get("video_filter_profanity", False):
            speech_config.filter_profanity = True
        
        # Add audio tracks if specified
        audio_tracks = ai_config.get("video_audio_tracks", [])
        if audio_tracks:
            for track in audio_tracks:
                speech_config.audio_tracks.append(int(track))
        
        # Add speech contexts if provided
        speech_contexts = ai_config.get("video_speech_contexts", [])
        if speech_contexts:
            for ctx in speech_contexts:
                speech_context = videointelligence.SpeechContext()
                if isinstance(ctx, dict):
                    speech_context.phrases.extend(ctx.get("phrases", []))
                elif isinstance(ctx, str):
                    speech_context.phrases.append(ctx)
                speech_config.speech_contexts.append(speech_context)
        
        video_context.speech_transcription_config = speech_config
    
    # Person detection config
    if videointelligence.Feature.PERSON_DETECTION in features:
        person_config = videointelligence.PersonDetectionConfig()
        person_config.include_bounding_boxes = True
        person_config.include_attributes = True
        person_config.include_pose_landmarks = True
        video_context.person_detection_config = person_config
    
    # Face detection config
    if videointelligence.Feature.FACE_DETECTION in features:
        face_config = videointelligence.FaceDetectionConfig()
        face_config.include_bounding_boxes = True
        face_config.include_attributes = True
        video_context.face_detection_config = face_config
    
    # Text detection config
    if videointelligence.Feature.TEXT_DETECTION in features:
        text_config = videointelligence.TextDetectionConfig()
        text_config.language_hints.append(ai_config.get("video_language_code", "en-US"))
        video_context.text_detection_config = text_config
    
    # Object tracking config
    if videointelligence.Feature.OBJECT_TRACKING in features:
        object_config = videointelligence.ObjectTrackingConfig()
        object_config.model = ai_config.get("video_model", "builtin/stable")
        video_context.object_tracking_config = object_config
    
    return video_context

def start_video_analysis(config, video_path_or_uri, ai_config, use_gcs_uri=False, max_retries=3):
    """Start video analysis job and return operation handle"""
    credentials = get_gcp_credentials(config)
    if not credentials:
        logging.error("Failed to get GCP credentials")
        sys.exit(5)
    
    video_client = videointelligence.VideoIntelligenceServiceClient(credentials=credentials)
    features = get_video_feature_list(ai_config)
    
    if not features:
        logging.error("No valid features configured for analysis")
        sys.exit(1)
    
    # Prepare input
    request = {
        "features": features,
        "video_context": build_video_context(ai_config, features)
    }
    
    if use_gcs_uri:
        request["input_uri"] = video_path_or_uri
        logging.info(f"Using GCS URI: {video_path_or_uri}")
    else:
        file_size = os.path.getsize(video_path_or_uri)
        max_inline = ai_config.get("video_max_file_size_for_inline", 10 * 1024 * 1024)
        
        if file_size > max_inline:
            logging.error(f"File too large ({file_size} bytes). Upload to GCS first.")
            sys.exit(4)
        
        with open(video_path_or_uri, "rb") as f:
            request["input_content"] = f.read()
        logging.info(f"Using inline upload ({file_size} bytes)")
    
    # Start operation with retries
    for attempt in range(max_retries):
        try:
            operation = video_client.annotate_video(request=request)
            logging.info(f"Operation started: {operation.operation.name}")
            return operation
            
        except (google_exceptions.ResourceExhausted, google_exceptions.RetryError) as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed after {max_retries} attempts: {e}")
                sys.exit(10)
            wait = (attempt + 1) * 10 + random.uniform(0, 5)
            logging.warning(f"Quota error, retrying in {wait:.1f}s...")
            time.sleep(wait)
            
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to start analysis: {e}")
                sys.exit(10)
            delay = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Retrying in {delay:.1f}s...")
            time.sleep(delay)
    
    logging.error("Failed to start video analysis")
    sys.exit(10)

def poll_video_analysis(operation, timeout_minutes=15):
    """
    Poll operation until completion and return normalized results.
    If timeout is reached and job is still running, exit with code 7 and operation name.
    """
    max_time = timeout_minutes * 60
    poll_interval = 10
    elapsed = 0
    
    logging.info(f"Polling for completion (timeout: {timeout_minutes}m)...")
    
    while elapsed < max_time:
        try:
            if operation.done():
                result = operation.result()
                
                # Serialize to dict
                serialized = MessageToDict(
                    result._pb,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True
                )
                
                # Merge multiple annotation results if present
                annotations = serialized.get("annotation_results", [])
                if len(annotations) > 1:
                    logging.info(f"Merging {len(annotations)} annotation results...")
                    serialized["annotation_results"] = [merge_annotation_results(annotations)]
                
                logging.info(f"Analysis completed in {elapsed}s")
                return serialized
            
            # Continue polling
            time.sleep(poll_interval)
            elapsed += poll_interval
            
            if elapsed % 60 == 0:
                logging.info(f"Processing... ({elapsed // 60}m elapsed)")
            
            # Gradually increase poll interval: 10s -> 30s
            poll_interval = min(poll_interval + 5, 30)
            
        except Exception as e:
            logging.warning(f"Polling error: {e}")
            time.sleep(poll_interval)
            elapsed += poll_interval
    
    # Timeout reached - job still running, exit with code 7 and operation name
    operation_name = operation.operation.name
    logging.error(f"Timeout after {timeout_minutes}m - job still indexing")
    print(f"Metadata extraction failed for asset: {operation_name}")
    sys.exit(7)

def retrieve_operation_result(config, operation_name, timeout_minutes=15):
    """Retrieve and poll an existing operation by name"""
    credentials = get_gcp_credentials(config)
    if not credentials:
        logging.error("Failed to get GCP credentials")
        sys.exit(5)
    
    try:
        operations_client = operations_v1.OperationsClient(
            channel=videointelligence.VideoIntelligenceServiceClient(credentials=credentials).transport.operations_client.transport.channel
        )
        
        operation = gapic_operation.Operation(
            operation=operations_client.get_operation(name=operation_name),
            refresh=operations_client.get_operation,
            cancel=operations_client.cancel_operation,
            result_type=videointelligence.AnnotateVideoResponse
        )
        
        logging.info(f"Retrieved operation: {operation_name}")
        return poll_video_analysis(operation, timeout_minutes)
        
    except Exception as e:
        logging.error(f"Failed to retrieve operation: {e}")
        sys.exit(10)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation: proxy, original, get_base_target, send_extracted_metadata, analyze_and_embed")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("-b", "--bucket-name", help="Name of GCS Bucket")
    parser.add_argument("-id", "--asset-id", help="Asset ID or operation name for metadata operations")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for upload")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded to GCS")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("-o", "--output-path", help="Path where output will be saved")
    parser.add_argument("--enrich-prefix", help="Prefix to add for sdnaEventType of AI enrich data")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Only allowed modes: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    
    # Load cloud config
    cloud_config_data = None
    try:
        if IS_LINUX:
            parser_cfg = ConfigParser()
            parser_cfg.read(cloud_config_path)
            for section in parser_cfg.sections():
                if section.lower() == args.config_name.lower():
                    cloud_config_data = dict(parser_cfg.items(section))
                    cloud_config_data['name'] = section
                    break
        else:
            with open(cloud_config_path, 'rb') as fp:
                plist_data = plistlib.load(fp)
                for config in plist_data:
                    if config.get('name', '').lower() == args.config_name.lower():
                        cloud_config_data = config
                        break
        
        if not cloud_config_data:
            logging.error(f"Configuration '{args.config_name}' not found")
            sys.exit(5)
            
    except Exception as e:
        logging.error(f"Error loading cloud config: {e}")
        sys.exit(5)

    # Load advanced AI config
    ai_config = get_advanced_ai_config(args.config_name, "gcp") or {}
    logging.info(f"AI Config loaded: vision_enabled={ai_config.get('vision_ai_enabled')}, video_enabled={ai_config.get('video_intelligence_enabled')}")

    # ========================================================================
    # MODE: get_base_target (Retrieve existing operation result)
    # ========================================================================
    if mode == "get_base_target":
        if not args.asset_id:
            logging.error("Asset ID (operation name) required for get_base_target mode")
            sys.exit(1)
        
        if not args.repo_guid or not args.catalog_path:
            logging.error("Repository GUID and catalog path required for get_base_target mode")
            sys.exit(1)
        
        logging.info(f"Retrieving operation result: {args.asset_id}")
        
        try:
            serialized = retrieve_operation_result(cloud_config_data, args.asset_id, timeout_minutes=15)
        except SystemExit as e:
            if e.code == 7:
                logging.error("Polling failed or timed out")
            raise
        
        # Process and store metadata
        clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        
        raw_metadata_path, norm_metadata_path = store_metadata_file(
            cloud_config_data, args.repo_guid, clean_path, serialized
        )
        
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_path, 
                                       raw_metadata_path, norm_metadata_path):
            logging.error("Failed to send extracted metadata")
            sys.exit(7)
        
        logging.info("Extracted metadata sent successfully")
        
        # Generate and send AI enriched metadata if normalization succeeded
        if norm_metadata_path is not None:
            # Get physical root to construct full path
            meta_path, _ = get_store_paths()
            if "/./" in meta_path:
                physical_root = meta_path.split("/./")[0]
            else:
                physical_root = meta_path
            
            full_norm_path = os.path.join(physical_root, norm_metadata_path)
            enriched_metadata, success = transform_normlized_to_enriched(
                full_norm_path, args.enrich_prefix
            )
            if success:
                if send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_path, enriched_metadata):
                    logging.info("AI enriched metadata sent successfully")
                else:
                    logging.warning("Failed to send AI enriched metadata")
            else:
                logging.warning(f"Failed to transform normalized metadata: {enriched_metadata}")
        
        sys.exit(0)

    # ========================================================================
    # MODE: send_extracted_metadata (Send pre-existing metadata files)
    # ========================================================================
    if mode == "send_extracted_metadata":
        if not args.repo_guid or not args.catalog_path:
            logging.error("Repository GUID and catalog path required for send_extracted_metadata mode")
            sys.exit(1)
        
        # This mode assumes metadata files already exist
        logging.info("Sending previously generated metadata files")
        
        clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        
        # Construct expected metadata paths
        meta_path, _ = get_store_paths()
        if "/./" in meta_path:
            meta_left, meta_right = meta_path.split("/./", 1)
        else:
            meta_left, meta_right = meta_path, ""
        
        provider = cloud_config_data.get("provider", "gcp_media")
        base = os.path.splitext(os.path.basename(clean_path))[0]
        repo_guid_str = str(args.repo_guid)
        
        # Check if files exist
        metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, clean_path, provider)
        raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
        norm_json = os.path.join(metadata_dir, f"{base}_norm.json")
        
        raw_return = os.path.join(meta_right, repo_guid_str, clean_path, provider, f"{base}_raw.json") if os.path.exists(raw_json) else None
        norm_return = os.path.join(meta_right, repo_guid_str, clean_path, provider, f"{base}_norm.json") if os.path.exists(norm_json) else None
        
        if not raw_return and not norm_return:
            logging.error(f"No metadata files found in {metadata_dir}")
            sys.exit(7)
        
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_path, raw_return, norm_return):
            logging.error("Failed to send extracted metadata")
            sys.exit(7)
        
        logging.info("Extracted metadata sent successfully")
        
        # Try to send enriched metadata if normalized exists
        if norm_return and os.path.exists(norm_json):
            enriched_metadata, success = transform_normlized_to_enriched(norm_json, args.enrich_prefix)
            if success:
                if send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_path, enriched_metadata):
                    logging.info("AI enriched metadata sent successfully")
        
        sys.exit(0)

    # ========================================================================
    # MODE: analyze_and_embed (Google Vision - Image Analysis)
    # ========================================================================
    if mode == "analyze_and_embed":
        if not args.source_path:
            logging.error("Source path required for analyze_and_embed mode")
            sys.exit(1)
        
        if not os.path.isfile(args.source_path):
            logging.error(f"Image not found: {args.source_path}")
            sys.exit(4)

        if args.dry_run:
            logging.info(f"[DRY RUN] Config: {args.config_name}")
            logging.info(f"[DRY RUN] Would analyze image: {args.source_path}")
            sys.exit(0)

        # Load image
        with open(args.source_path, "rb") as f:
            image_data = f.read()
        logging.info(f"Loaded image: {len(image_data)} bytes")

        # Analyze with Google Vision
        analysis = analyze_image_gcp(cloud_config_data, image_data, ai_config)
        if not analysis:
            logging.error("GCP Vision analysis failed")
            sys.exit(10)

        # Prepare output metadata (keeping structure similar to S3 uploader)
        ai_metadata = {
            "vision_analysis": analysis,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Store metadata if repo_guid and catalog_path provided
        if args.repo_guid and args.catalog_path:
            clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
            raw_metadata_path, norm_metadata_path = store_metadata_file(
                cloud_config_data, args.repo_guid, clean_path, ai_metadata
            )
            
            if send_extracted_metadata(cloud_config_data, args.repo_guid, clean_path, 
                                       raw_metadata_path, norm_metadata_path):
                logging.info("Extracted metadata sent successfully")
                
                # Send enriched metadata
                if norm_metadata_path:
                    meta_path, _ = get_store_paths()
                    if "/./" in meta_path:
                        physical_root = meta_path.split("/./")[0]
                    else:
                        physical_root = meta_path
                    
                    full_norm_path = os.path.join(physical_root, norm_metadata_path)
                    enriched_metadata, success = transform_normlized_to_enriched(
                        full_norm_path, args.enrich_prefix
                    )
                    if success:
                        send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_path, enriched_metadata)

        # Save to output path if specified
        if args.output_path:
            try:
                with open(args.output_path, "w", encoding="utf-8") as f:
                    json.dump(ai_metadata, f, indent=2, ensure_ascii=False, default=str)
                logging.info(f"Output written to {args.output_path}")
            except Exception as e:
                logging.error(f"Failed to write output: {e}")
                sys.exit(15)

        logging.info("GCP Vision processing completed")
        sys.exit(0)

    # ========================================================================
    # MODE: proxy and original (Video Intelligence - Upload and Analyze)
    # ========================================================================
    if mode in ["proxy", "original"]:
        if not args.source_path:
            logging.error("Source path required for proxy/original mode")
            sys.exit(1)
        
        if not os.path.isfile(args.source_path):
            logging.error(f"Video file not found: {args.source_path}")
            sys.exit(4)
        
        # Check file size limit for original mode
        if mode == "original" and args.size_limit:
            try:
                limit_bytes = float(args.size_limit) * 1024 * 1024
                file_size = os.path.getsize(args.source_path)
                if file_size > limit_bytes:
                    logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                    sys.exit(4)
            except ValueError:
                logging.warning(f"Invalid size limit: {args.size_limit}")

        catalog_path = args.catalog_path or args.source_path
        clean_catalog_path = catalog_path.replace("\\", "/").split("/1/", 1)[-1]

        if args.dry_run:
            logging.info(f"[DRY RUN] Config: {args.config_name}")
            logging.info(f"[DRY RUN] Would upload: {args.source_path}")
            if args.bucket_name:
                logging.info(f"[DRY RUN] To bucket: {args.bucket_name}")
            sys.exit(0)

        # Upload to GCS
        if not args.bucket_name:
            logging.error("Bucket name required for GCP Video Intelligence analysis")
            sys.exit(1)
        
        credentials = get_gcp_credentials(cloud_config_data)
        if not credentials:
            logging.error("Failed to get GCP credentials")
            sys.exit(5)
        
        upload_prefix = ai_config.get("video_gcs_upload_prefix", "video-intelligence-temp")
        upload_path = args.upload_path.lstrip('/') if args.upload_path else f"{upload_prefix}/{uuid.uuid4()}/{os.path.basename(args.source_path)}"
        
        logging.info(f"Uploading video to gs://{args.bucket_name}/{upload_path}")
        
        # Use multipart upload for better performance on large files
        gcs_uri = upload_to_gcs(credentials, args.bucket_name, args.source_path, upload_path, use_multipart=True)
        if not gcs_uri:
            logging.error(f"Failed to upload video to GCS")
            sys.exit(10)
        
        logging.info(f"Video uploaded: {gcs_uri}")

        try:
            # Start video analysis
            operation = start_video_analysis(cloud_config_data, gcs_uri, ai_config, use_gcs_uri=True)
            operation_name = operation.operation.name
            logging.info(f"Analysis started with operation: {operation_name}")
            
            # Save operation name in catalog
            if args.repo_guid:
                update_catalog(args.repo_guid, clean_catalog_path, operation_name)
            
            # Poll for completion (15 minutes timeout)
            try:
                serialized = poll_video_analysis(operation, timeout_minutes=15)
            except SystemExit as e:
                # Clean up GCS file on failure
                delete_from_gcs(credentials, args.bucket_name, upload_path)
                if e.code == 7:
                    logging.error("Analysis polling timed out - job still indexing")
                raise
            
            # Clean up GCS file after successful analysis
            delete_from_gcs(credentials, args.bucket_name, upload_path)
            
            # Store metadata files
            raw_metadata_path, norm_metadata_path = store_metadata_file(
                cloud_config_data, args.repo_guid, clean_catalog_path, serialized
            )
            
            if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_metadata_path, norm_metadata_path):
                logging.error("Failed to send extracted metadata")
                sys.exit(7)
            
            logging.info("Extracted metadata sent successfully")
            
            # Generate and send AI enriched metadata if normalization succeeded
            if norm_metadata_path:
                meta_path, _ = get_store_paths()
                if "/./" in meta_path:
                    physical_root = meta_path.split("/./")[0]
                else:
                    physical_root = meta_path
                
                full_norm_path = os.path.join(physical_root, norm_metadata_path)
                enriched_metadata, success = transform_normlized_to_enriched(
                    full_norm_path, args.enrich_prefix
                )
                if success:
                    if send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched_metadata):
                        logging.info("AI enriched metadata sent successfully")
                    else:
                        logging.warning("Failed to send AI enriched metadata")
                else:
                    logging.warning(f"Failed to transform normalized metadata: {enriched_metadata}")
            
            logging.info("GCP Video Intelligence processing completed successfully")
            sys.exit(0)
            
        except Exception as e:
            # Clean up GCS file on any error
            delete_from_gcs(credentials, args.bucket_name, upload_path)
            logging.error(f"GCP Video Intelligence processing failed: {e}")
            sys.exit(10)

    logging.error(f"Unsupported mode: {mode}")
    sys.exit(1)