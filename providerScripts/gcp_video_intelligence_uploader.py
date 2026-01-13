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
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
from google.protobuf.json_format import MessageToDict
from google.oauth2 import service_account
import requests

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "generate_video_proxy", 
               "generate_video_frame_proxy", "generate_intelligence_proxy", 
               "generate_video_to_spritesheet", "analyze_and_embed"]
LINUX_CONFIG_PATH = "DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
IS_LINUX = True
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

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

logger = logging.getLogger()

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper()), 
        format='%(asctime)s %(levelname)s: %(message)s'
    )

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

def fail(msg, code=7):
    """Exit with error message and code for coordinator script"""
    logger.error(msg)
    print(f"Metadata extraction failed for asset: {getattr(args, 'asset_id', 'unknown')}")
    sys.exit(code)

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

def analyze_video_gcp(config, video_path_or_uri, ai_config, bucket_name=None, use_gcs_uri=False, max_retries=3):
    credentials = get_gcp_credentials(config)
    if not credentials:
        fail("Failed to get GCP credentials", 5)

    video_client = videointelligence.VideoIntelligenceServiceClient(credentials=credentials)
    features = get_feature_list(ai_config)
    
    if not features:
        fail("No valid features configured for analysis", 1)

    # Determine input method
    if use_gcs_uri:
        # Video already in GCS
        input_uri = video_path_or_uri
        input_content = None
        logging.info(f"Using pre-uploaded GCS URI: {input_uri}")
    else:
        # Local file - check size
        file_size = os.path.getsize(video_path_or_uri)
        max_inline_size = ai_config.get("max_file_size_for_inline", 10 * 1024 * 1024)  # Default 10MB
        
        if file_size <= max_inline_size:
            logging.info(f"File size {file_size} bytes is within inline limit, using direct upload")
            with open(video_path_or_uri, "rb") as f:
                input_content = f.read()
            input_uri = None
        else:
            fail(f"File too large ({file_size} bytes) for inline upload. Please upload to GCS first.", 4)

    # Build video context with feature-specific configs
    video_context = build_video_context(ai_config, features)

    # Start analysis with retry logic
    operation = None
    for attempt in range(max_retries):
        try:
            logging.info(f"Starting video analysis (attempt {attempt + 1}/{max_retries})")
            
            if input_uri:
                operation = video_client.annotate_video(
                    request={
                        "input_uri": input_uri,
                        "features": features,
                        "video_context": video_context
                    }
                )
            else:
                operation = video_client.annotate_video(
                    request={
                        "input_content": input_content,
                        "features": features,
                        "video_context": video_context
                    }
                )
            
            logging.info(f"Video analysis operation started: {operation.operation.name}")
            break

        except (google_exceptions.ResourceExhausted, google_exceptions.RetryError) as e:
            if attempt == max_retries - 1:
                fail(f"Failed to start video analysis after {max_retries} attempts: {e}", 10)
            else:
                wait_time = (attempt + 1) * 10 + random.uniform(0, 5)
                logging.warning(f"Quota error, retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)

        except Exception as e:
            if attempt == max_retries - 1:
                fail(f"Failed to start video analysis: {e}", 10)
            else:
                delay = (2 ** attempt) + random.uniform(0, 2)
                logging.warning(f"Retrying video analysis in {delay:.1f}s...")
                time.sleep(delay)

    if not operation:
        fail("Failed to start video analysis operation", 10)

    # Polling for completion with 15-minute timeout
    logging.info("Video analysis operation started, polling for completion...")
    max_poll_time = 15 * 60  # 15 minutes
    poll_interval = 10  # Starting with 10 seconds
    elapsed_time = 0
    
    while elapsed_time < max_poll_time:
        try:
            # Check if operation is done
            if operation.done():
                logging.info(f"Operation completed after {elapsed_time} seconds")
                result = operation.result()
                
                # Convert result to dict
                serialized = MessageToDict(
                    result._pb,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True
                )
                
                # Merge annotation results if there are multiple
                if "annotation_results" in serialized and len(serialized["annotation_results"]) > 1:
                    logging.info(f"Found {len(serialized['annotation_results'])} separate annotation results, merging...")
                    merged_result = merge_annotation_results(serialized["annotation_results"])
                    # Replace the array with single merged result
                    serialized["annotation_results"] = [merged_result]
                
                # Log detected features for this single video
                if "annotation_results" in serialized and len(serialized["annotation_results"]) > 0:
                    video_result = serialized["annotation_results"][0]
                    available_features = []
                    
                    if video_result.get("segment_label_annotations"):
                        available_features.append(f"Labels: {len(video_result['segment_label_annotations'])}")
                    if video_result.get("shot_annotations"):
                        available_features.append(f"Shots: {len(video_result['shot_annotations'])}")
                    if video_result.get("explicit_annotation"):
                        available_features.append("Explicit Content")
                    if video_result.get("speech_transcriptions"):
                        available_features.append(f"Transcriptions: {len(video_result['speech_transcriptions'])}")
                    if video_result.get("text_annotations"):
                        available_features.append(f"Text: {len(video_result['text_annotations'])}")
                    if video_result.get("object_annotations"):
                        available_features.append(f"Objects: {len(video_result['object_annotations'])}")
                    if video_result.get("logo_recognition_annotations"):
                        available_features.append(f"Logos: {len(video_result['logo_recognition_annotations'])}")
                    if video_result.get("face_detection_annotations"):
                        available_features.append(f"Faces: {len(video_result['face_detection_annotations'])}")
                    if video_result.get("person_detection_annotations"):
                        available_features.append(f"Persons: {len(video_result['person_detection_annotations'])}")
                    
                    if available_features:
                        logging.info(f"Detected features: {', '.join(available_features)}")
                
                logging.info("Video analysis completed successfully")
                return serialized
            
            # Not done yet, wait and check again
            time.sleep(poll_interval)
            elapsed_time += poll_interval
            
            # Log progress every minute
            if elapsed_time % 60 == 0:
                logging.info(f"Still processing... ({elapsed_time // 60} minutes elapsed)")
            
            # Increase poll interval gradually (10s -> 20s -> 30s max)
            poll_interval = min(poll_interval + 5, 30)
            
        except Exception as e:
            logging.warning(f"Error while polling operation: {e}")
            time.sleep(poll_interval)
            elapsed_time += poll_interval
    
    # 15 minutes elapsed without completion
    logging.error(f"Video analysis did not complete within {max_poll_time // 60} minutes")
    logging.info(f"Operation name: {operation.operation.name}")
    fail(f"Video analysis timeout after {max_poll_time // 60} minutes. Operation may still be running.", 7)

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

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 0.5))

    parser = argparse.ArgumentParser(description="GCP Video Intelligence - Analysis & Processing")
    parser.add_argument("-m", "--mode", required=True)
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-sp", "--source-path", required=True)
    parser.add_argument("-b", "--bucket-name", help="GCS bucket name for video upload")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded in GCS bucket")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-mp", "--metadata-file", help="Path where property bag for file resides")
    parser.add_argument("-o", "--output-path", help="Temporary output path for analysis JSON")
    parser.add_argument("-sl", "--size-limit", help="Source file size limit")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-a", "--asset-id", help="Asset ID for error tracking")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--controller-address", help="Link IP/Hostname Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata using Video Intelligence")
    parser.add_argument("--use-gcs-uri", action="store_true", help="Treat source-path as existing GCS URI")
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

    # Handle GCS upload if needed
    gcs_uri = None
    temp_gcs_path = None
    
    if args.use_gcs_uri:
        # Source path is already a GCS URI
        gcs_uri = args.source_path
        logging.info(f"Using existing GCS URI: {gcs_uri}")
    elif args.bucket_name and args.upload_path:
        # Need to upload to GCS first
        if not os.path.isfile(args.source_path):
            fail(f"Video file not found: {args.source_path}", 4)
        
        logging.info(f"Uploading video to GCS bucket: {args.bucket_name}")
        credentials = get_gcp_credentials(cloud_config_data)
        if not credentials:
            fail("Failed to get GCP credentials for upload", 5)
        
        # Clean up upload path
        upload_path = args.upload_path.lstrip('/')
        temp_gcs_path = upload_path
        
        gcs_uri = upload_to_gcs(credentials, args.bucket_name, args.source_path, upload_path)
        if not gcs_uri:
            fail(f"Failed to upload video to gs://{args.bucket_name}/{upload_path}", 10)
        
        logging.info(f"Video uploaded successfully to {gcs_uri}")
    else:
        # Use local file for inline analysis
        if not os.path.isfile(args.source_path):
            fail(f"Video file not found: {args.source_path}", 4)
        
        file_size = os.path.getsize(args.source_path)
        logging.info(f"Using local video file: {args.source_path} ({file_size} bytes)")

    if args.dry_run:
        logging.info(f"[DRY RUN] Config: {args.config_name}")
        if gcs_uri:
            logging.info(f"[DRY RUN] Would analyze GCS URI: {gcs_uri}")
        else:
            logging.info(f"[DRY RUN] Would analyze local file: {args.source_path}")
        sys.exit(0)

    # Load AI configuration
    ai_config = get_advanced_ai_config(
        args.config_name, 
        cloud_config_data.get("provider", "google_video_intelligence")
    )

    # Start video analysis
    if gcs_uri:
        analysis = analyze_video_gcp(
            cloud_config_data, 
            gcs_uri, 
            ai_config, 
            bucket_name=args.bucket_name,
            use_gcs_uri=True
        )
    else:
        analysis = analyze_video_gcp(
            cloud_config_data, 
            args.source_path, 
            ai_config,
            bucket_name=args.bucket_name
        )
    
    # Clean up temporary GCS file if we uploaded it
    if temp_gcs_path and args.bucket_name:
        credentials = get_gcp_credentials(cloud_config_data)
        if credentials:
            delete_from_gcs(credentials, args.bucket_name, temp_gcs_path)
    
    if not analysis or "error" in analysis:
        fail("GCP Video Intelligence analysis failed or returned errors", 10)

    # Prepare output metadata
    ai_metadata = {
        "analysis": analysis,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_file": os.path.basename(args.source_path) if not args.use_gcs_uri else args.source_path,
        "asset_id": args.asset_id if args.asset_id else None
    }

    if args.output_path:
        try:
            with open(args.output_path, "w", encoding="utf-8") as f:
                json.dump(ai_metadata, f, indent=2, ensure_ascii=False)
            logging.info(f"Output written to {args.output_path}")
        except Exception as e:
            fail(f"Failed to write output to {args.output_path}: {e}", 15)

    logging.info("GCP Video Intelligence processing completed successfully")
    sys.exit(0)