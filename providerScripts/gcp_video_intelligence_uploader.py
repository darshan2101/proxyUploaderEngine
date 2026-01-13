import os
import subprocess
import sys
import json
import argparse
import logging
import random
import time
from configparser import ConfigParser
import plistlib
import base64

# GCP Imports
from google.cloud import videointelligence_v1 as videointelligence
from google.api_core import operations_v1
from google.api_core import operation as gapic_operation
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
from google.protobuf.json_format import MessageToDict
from google.oauth2 import service_account
import requests

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "generate_video_proxy", 
               "generate_video_frame_proxy", "generate_intelligence_proxy", 
               "generate_video_to_spritesheet", "analyze_and_embed"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/gcp_video_intelligence_metadata_normalizer.py"

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

FEATURE_MAP = {
    "segment_label_annotations": "Labels",
    "shot_annotations": "Shots",
    "speech_transcriptions": "Transcriptions",
    "text_annotations": "Text",
    "object_annotations": "Objects",
    "logo_recognition_annotations": "Logos",
    "face_detection_annotations": "Faces",
    "person_detection_annotations": "Persons",
}

SDNA_EVENT_MAP = {
    "gcp_video_labels": "labels",
    "gcp_video_keywords": "keywords",
    "gcp_video_summary": "summary",
    "gcp_video_highlights": "highlights",
    "gcp_video_transcript": "transcript",
}

logger = logging.getLogger()

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper()), 
        format='%(asctime)s %(levelname)s: %(message)s'
    )

def fail(msg, code=7):
    """Exit with error message and code for coordinator script"""
    logger.error(msg)
    print(f"Metadata extraction failed for asset: {getattr(args, 'asset_id', 'unknown')}")
    sys.exit(code)

def get_retry_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, max_retries=3, **kwargs):
    session = get_retry_session()
    for attempt in range(max_retries):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                wait_time = retry_after + 5
                logging.warning(f"[429] Rate limited. Waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            if response.status_code < 500:
                return response
            logging.warning(f"[SERVER ERROR {response.status_code}] Retrying...")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                raise
            delay = [2, 5, 15][attempt] + random.uniform(0, 2)
            logging.warning(f"[{type(e).__name__}] Retrying in {delay:.1f}s...")
            time.sleep(delay)
    return None

def with_retry(fn, retries=3, base_delay=2):
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt == retries - 1:
                raise
            delay = base_delay ** attempt + random.uniform(0, 2)
            logging.warning(f"Retrying in {delay:.1f}s due to: {e}")
            time.sleep(delay)

def get_config_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            folder = parser.get('General', 'cloudconfigfolder', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                folder = plistlib.load(fp).get("CloudConfigFolder", "").strip()
        if not folder:
            logging.error("CloudConfigFolder not found")
            sys.exit(5)
        return os.path.join(folder, "cloud_targets.conf")
    except Exception as e:
        logging.error(f"Failed to read config: {e}")
        sys.exit(5)

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

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json" if IS_LINUX else \
                      "/Library/Application Support/StorageDNA/nginx/ai-config.json"
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

def get_advanced_ai_config(config_name, provider_name="google_video_intelligence"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return {}

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
                    for key, val in raw.items():
                        clean_key = key[len(provider_key) + 1:] if key.startswith(provider_key + "_") else key
                        logging.debug(f"DEBUG RAW: key='{key}', clean_key='{clean_key}', val={val}")
                        
                        if clean_key in {
                            "gcp_video_intelligence_enabled",
                            "features",
                            "gcs_bucket_name",
                            "gcs_upload_prefix",
                            "language_code",
                            "speech_contexts",
                            "enable_automatic_punctuation",
                            "enable_speaker_diarization",
                            "diarization_speaker_count",
                            "max_alternatives",
                            "filter_profanity",
                            "audio_tracks",
                            "model",
                            "max_file_size_for_inline"
                        }:
                            out[clean_key] = val
                    
                    logging.debug(f"Generated config dictionary: {out}")

                    # Normalize enabled flag
                    if "gcp_video_intelligence_enabled" in out:
                        v = out["gcp_video_intelligence_enabled"]
                        if isinstance(v, bool):
                            out["gcp_video_intelligence_enabled"] = v
                        else:
                            out["gcp_video_intelligence_enabled"] = str(v).lower() in ("true", "1", "yes", "on")
                    
                    logging.info(f"DEBUG: gcp_video_intelligence_enabled = {out.get('gcp_video_intelligence_enabled')}")

                    # Normalize features list
                    if "features" in out:
                        feats = out["features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        cleaned_features = []
                        for f in feats:
                            if isinstance(f, str):
                                f_lower = f.lower()
                                for prefix in ["google_video_intelligence_", "gcp_video_", "provider_", "video_"]:
                                    if f_lower.startswith(prefix):
                                        f = f[len(prefix):]
                                        break
                                cleaned_features.append(f.upper())
                            else:
                                cleaned_features.append(f)
                        out["features"] = cleaned_features

                    # Normalize numeric fields
                    if "max_file_size_for_inline" in out:
                        try:
                            out["max_file_size_for_inline"] = int(out["max_file_size_for_inline"])
                        except (ValueError, TypeError):
                            pass

                    logging.info(f"Loaded and normalized GCP Video Intelligence config from: {src}")
                    return out

            except Exception as e:
                logging.error(f"Failed to load {src}: {e}")

    return {}

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

def upload_to_gcs(credentials, bucket_name, source_path, gcs_path):
    """Upload file to Google Cloud Storage"""
    try:
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        logging.info(f"Uploading {source_path} to gs://{bucket_name}/{gcs_path}")
        blob.upload_from_filename(source_path)
        
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        logging.info(f"Successfully uploaded to {gcs_uri}")
        return gcs_uri
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {e}")
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
        logging.error(f"Failed to delete from GCS: {e}")
        return False

def merge_annotation_results(annotation_results):
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
                # Only add non-empty items
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

def get_feature_list(ai_config):
    ai_config = ai_config or {}
    enabled = bool(ai_config.get("gcp_video_intelligence_enabled", False))
    if not enabled:
        logging.info("GCP Video Intelligence not enabled in ai_config; skipping feature extraction")
        return []

    requested_features = ai_config.get("features", [])
    
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
        speech_config.language_code = ai_config.get("language_code", "en-US")
        
        # Set automatic punctuation if requested
        if ai_config.get("enable_automatic_punctuation", False):
            speech_config.enable_automatic_punctuation = True
        
        # Set speaker diarization if requested
        if ai_config.get("enable_speaker_diarization", False):
            speech_config.enable_speaker_diarization = True
            # Optionally set speaker count
            speaker_count = ai_config.get("diarization_speaker_count")
            if speaker_count:
                speech_config.diarization_speaker_count = int(speaker_count)
        
        # Set max alternatives if specified
        max_alternatives = ai_config.get("max_alternatives")
        if max_alternatives:
            speech_config.max_alternatives = int(max_alternatives)
        
        # Set filter profanity if requested
        if ai_config.get("filter_profanity", False):
            speech_config.filter_profanity = True
        
        # Add audio tracks if specified
        audio_tracks = ai_config.get("audio_tracks", [])
        if audio_tracks:
            for track in audio_tracks:
                speech_config.audio_tracks.append(int(track))
        
        # Add speech contexts if provided
        speech_contexts = ai_config.get("speech_contexts", [])
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
        text_config.language_hints.append(ai_config.get("language_code", "en-US"))
        video_context.text_detection_config = text_config
    
    # Object tracking config
    if videointelligence.Feature.OBJECT_TRACKING in features:
        object_config = videointelligence.ObjectTrackingConfig()
        object_config.model = ai_config.get("model", "builtin/stable")
        video_context.object_tracking_config = object_config
    
    return video_context

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    meta_path, proxy_path = get_store_paths()
    if not meta_path:
        logging.error("Metadata Store settings not found.")
        return None, None

    provider = config.get("provider", "gcp_video_intelligence")
    base = os.path.splitext(os.path.basename(file_path))[0]
    repo_guid_str = str(repo_guid)

    # -----------------------------------------
    # 1. Split meta_path at /./
    # -----------------------------------------
    if "/./" in meta_path:
        meta_left, meta_right = meta_path.split("/./", 1)
    else:
        meta_left, meta_right = meta_path, ""

    meta_left = meta_left.rstrip("/")

    # -----------------------------------------
    # 2. Build PHYSICAL PATH (local disk path) meta_left/meta_right/repo_guid/file_path/provider
    # -----------------------------------------
    metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)

    # Full local physical paths
    raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
    norm_json = os.path.join(metadata_dir, f"{base}_norm.json")

    # -----------------------------------------
    # 3. Returned paths (AFTER /./) meta_right/repo_guid/file_path/provider/file.json
    # -----------------------------------------
    raw_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_raw.json")
    norm_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_norm.json")

    raw_success = False
    norm_success = False

    # -----------------------------------------
    # 4. Write metadata with retry
    # -----------------------------------------
    for attempt in range(max_attempts):
        try:
            # RAW write
            with open(raw_json, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            raw_success = True

            # NORMALIZED write
            if not get_normalized_metadata(raw_json, norm_json):
                logging.error("Normalization failed.")
                norm_success = False
            else:
                norm_success = True

            break  # No need for more retries

        except Exception as e:
            logging.warning(f"Metadata write failed (Attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))

    # -----------------------------------------
    # 5. Return results
    # -----------------------------------------
    return (
        raw_return if raw_success else None,
        norm_return if norm_success else None
    )

def start_video_analysis(config, video_path_or_uri, ai_config, use_gcs_uri=False, max_retries=3):
    # Start video analysis job and return operation handle
    credentials = get_gcp_credentials(config)
    if not credentials:
        fail("Failed to get GCP credentials", 5)
    
    video_client = videointelligence.VideoIntelligenceServiceClient(credentials=credentials)
    features = get_feature_list(ai_config)
    
    if not features:
        fail("No valid features configured for analysis", 1)
    
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
        max_inline = ai_config.get("max_file_size_for_inline", 10 * 1024 * 1024)
        
        if file_size > max_inline:
            fail(f"File too large ({file_size} bytes). Upload to GCS first.", 4)
        
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
                fail(f"Failed after {max_retries} attempts: {e}", 10)
            wait = (attempt + 1) * 10 + random.uniform(0, 5)
            logging.warning(f"Quota error, retrying in {wait:.1f}s...")
            time.sleep(wait)
            
        except Exception as e:
            if attempt == max_retries - 1:
                fail(f"Failed to start analysis: {e}", 10)
            delay = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Retrying in {delay:.1f}s...")
            time.sleep(delay)
    
    fail("Failed to start video analysis", 10)

def poll_video_analysis(operation, timeout_minutes=15):
    # Poll operation until completion and return normalized results
    max_time = timeout_minutes * 60
    poll_interval = 10
    elapsed = 0
    
    logging.info("Polling for completion...")
    
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
                
                # Log detected features
                if annotations:
                    log_detected_features(annotations[0])
                
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
    
    # Timeout reached
    logging.error(f"Timeout after {timeout_minutes}m")
    logging.info(f"Operation: {operation.operation.name}")
    fail(f"Analysis timeout after {timeout_minutes}m. Operation may still be running.", 7)

def log_detected_features(video_result):
    # Log summary of detected features in video result
    features = []
    
    feature_map = {
        "segment_label_annotations": "Labels",
        "shot_annotations": "Shots",
        "explicit_annotation": "Explicit Content",
        "speech_transcriptions": "Transcriptions",
        "text_annotations": "Text",
        "object_annotations": "Objects",
        "logo_recognition_annotations": "Logos",
        "face_detection_annotations": "Faces",
        "person_detection_annotations": "Persons"
    }
    
    for key, label in feature_map.items():
        value = video_result.get(key)
        if value:
            count = len(value) if isinstance(value, list) else ""
            features.append(f"{label}: {count}".strip(": "))
    
    if features:
        logging.info(f"Detected: {', '.join(features)}")

# metadata conversion calls

def get_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path):
    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logging.error(f"Normalizer script not found: {NORMALIZER_SCRIPT_PATH}")
        return False
    
    try:
        process = subprocess.run(
            ["python3", NORMALIZER_SCRIPT_PATH, "-i", raw_metadata_file_path, "-o", norm_metadata_file_path],
            check=True
        )
        if process.returncode == 0:
            return True
        else:
            logging.error(f"Normalizer script failed with return code: {process.returncode}")
            return False
    except Exception as e:
        logging.error(f"Error normalizing metadata: {e}")
        return False
    
def transform_normlized_to_enriched(norm_metadata_file_path, filetype_prefix):
    try:
        with open(norm_metadata_file_path, "r") as f:
            data = json.load(f)
        result = []
        for event_type, detections in data.items():
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, 'unknown')
            for detection in detections:
                occurrences = detection.get("occurrences", [])
                event_obj = {
                    "eventType": event_type,
                    "sdnaEventType": sdna_event_type,
                    "sdnaEventTypePrefix": filetype_prefix,
                    "eventValue": detection.get("value", ""),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                }
                result.append(event_obj)
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False    
    
# Mongo update calls

def update_catalog(repo_guid, file_path, media_uri, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": cloud_config_data.get("provider", "gcp_video_intelligence"),
        "providerData": {
            "assetId": media_uri
        }
    }

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info(f"Catalog updated successfully: {response.text}")
                return True
            else:
                logging.warning(f"Catalog update failed (status {response.status_code if response else 'No response'}): {response.text if response else ''}")
        except Exception as e:
            logging.warning(f"Catalog update attempt {attempt + 1} failed: {e}")
        if attempt < max_attempts - 1:
            delay = [1, 3, 10][attempt] + random.uniform(0, 1)
            time.sleep(delay)
    logging.error("Catalog update failed after retries.")
    return False

def send_extracted_metadata(config, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "gcp_video_intelligence"),
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path)
        }]
    }
    if rawMetadataFilePath is not None:
        payload["extendedMetadata"][0]["metadataRawJsonFilePath"] = rawMetadataFilePath
    if normMetadataFilePath is not None:
        payload["extendedMetadata"][0]["metadataFilePath"] = normMetadataFilePath
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
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "gcp_video_intelligence"),
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

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 0.5))

    parser = argparse.ArgumentParser(description="GCP Video Intelligence - Analysis & Processing")
    parser.add_argument("-m", "--mode", required=True)
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-sp", "--source-path")
    parser.add_argument("-b", "--bucket-name", help="GCS bucket name for video upload")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded in GCS bucket")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-mp", "--metadata-file", help="Path where property bag for file resides")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-a", "--asset-id", help="Operation name for polling or asset ID for error tracking")
    parser.add_argument("-sl", "--size-limit", help="Source file size limit")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--controller-address", help="Link IP/Hostname Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata using Video Intelligence")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.mode not in VALID_MODES:
        fail(f"Invalid mode: {args.mode}. Allowed modes: {VALID_MODES}", 1)

    setup_logging(args.log_level)
    config_path = get_config_path()
    cloud_config = ConfigParser(interpolation=None)
    cloud_config.read(config_path)
    
    if args.config_name not in cloud_config:
        fail(f"Config '{args.config_name}' not found in {config_path}", 1)
    
    cloud_config_data = dict(cloud_config[args.config_name])

    if not cloud_config_data.get("token"):
        fail("Missing 'token' (GCP service account JSON) in cloud config", 5)

    # Load AI configuration
    ai_config = get_advanced_ai_config(
        args.config_name, 
        cloud_config_data.get("provider", "gcp_video_intelligence")
    )

    # Handle send_extracted_metadata mode separately
    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            fail("Asset ID (operation name), Repo GUID, and Catalog path required for send_extracted_metadata mode", 1)
        
        logging.info(f"Resuming analysis with operation name: {args.asset_id}")
        
        # Recreate operation object from name
        credentials = get_gcp_credentials(cloud_config_data)
        if not credentials:
            fail("Failed to get GCP credentials", 5)
        
        video_client = videointelligence.VideoIntelligenceServiceClient(credentials=credentials)
        
        # Get operation by name
        try:
            operations_client = operations_v1.OperationsClient(video_client.transport.channel)
            operation = operations_client.get_operation(args.asset_id)
            
            # Wrap in LRO operation object
            operation = gapic_operation.from_gapic(
                operation,
                operations_client,
                videointelligence.AnnotateVideoResponse,
                metadata_type=videointelligence.AnnotateVideoProgress
            )
        except Exception as e:
            fail(f"Failed to retrieve operation {args.asset_id}: {e}", 10)
        
        # Poll for completion
        try:
            serialized = poll_video_analysis(operation, timeout_minutes=15)
        except SystemExit as e:
            if e.code == 7:
                logging.error("Polling failed or timed out")
            raise
        
        # Process and store metadata
        clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        
        raw_metadata_path, norm_metadata_path = store_metadata_file(cloud_config_data, args.repo_guid, clean_path, serialized)
        
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_path, raw_metadata_path, norm_metadata_path):
            fail("Failed to send extracted metadata", 7)
        
        logging.info("Extracted metadata sent successfully")
        
        # Generate and send AI enriched metadata if normalization succeeded
        if norm_metadata_path is not None:
            enriched_metadata, success = transform_normlized_to_enriched(norm_metadata_path, "vid")
            if success:
                if send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_path, enriched_metadata):
                    logging.info("AI enriched metadata sent successfully")
                else:
                    logging.warning("Failed to send AI enriched metadata")
            else:
                logging.warning(f"Failed to transform normalized metadata: {enriched_metadata}")
        
        sys.exit(0)

    # Normal proxy/original mode
    if not args.source_path:
        fail("Source path required for proxy/original mode", 1)
    
    if not os.path.isfile(args.source_path):
        fail(f"Video file not found: {args.source_path}", 4)
    
    if args.mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(args.source_path)
            if file_size > limit_bytes:
                fail(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB", 4)
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
        fail("Bucket name required for GCP Video Intelligence analysis", 1)
    
    credentials = get_gcp_credentials(cloud_config_data)
    if not credentials:
        fail("Failed to get GCP credentials", 5)
    
    upload_path = args.upload_path.lstrip('/') if args.upload_path else f"temp/{os.path.basename(args.source_path)}"
    
    logging.info(f"Uploading video to gs://{args.bucket_name}/{upload_path}")
    gcs_uri = upload_to_gcs(credentials, args.bucket_name, args.source_path, upload_path)
    if not gcs_uri:
        fail(f"Failed to upload video to GCS", 10)
    
    logging.info(f"Video uploaded: {gcs_uri}")

    try:
        # Start video analysis
        operation = start_video_analysis(cloud_config_data, gcs_uri, ai_config, use_gcs_uri=True)
        operation_name = operation.operation.name
        logging.info(f"Analysis started with operation: {operation_name}")
        
        # Save operation name in catalog
        if args.repo_guid:
            update_catalog(args.repo_guid, clean_catalog_path, operation_name)
        
        # Poll for completion
        try:
            serialized = poll_video_analysis(operation, timeout_minutes=15)
        except SystemExit as e:
            # Clean up GCS file on failure
            delete_from_gcs(credentials, args.bucket_name, upload_path)
            if e.code == 7:
                logging.error("Analysis polling timed out")
                print(f"Metadata extraction failed for asset: {operation_name}")
                sys.exit(7)
            raise
        
        # Clean up GCS file after successful analysis
        delete_from_gcs(credentials, args.bucket_name, upload_path)
        
        # Store metadata files
        raw_metadata_path, norm_metadata_path = store_metadata_file(cloud_config_data, args.repo_guid, clean_catalog_path, serialized)
        
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_metadata_path, norm_metadata_path):
            fail("Failed to send extracted metadata", 7)
        
        logging.info("Extracted metadata sent successfully")
        
        # Generate and send AI enriched metadata if normalization succeeded
        if norm_metadata_path:
            enriched_metadata, success = transform_normlized_to_enriched(norm_metadata_path, "video")
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
        fail(f"GCP Video Intelligence processing failed: {e}", 10)