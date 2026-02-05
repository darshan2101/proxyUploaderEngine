#!/usr/bin/env python3
import os
import sys
import json
import uuid
import time
import random
import logging
import argparse
import subprocess
import urllib.parse
import urllib.request
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
import requests
import boto3
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from botocore.config import Config
from botocore.exceptions import ClientError

# ======================
# Constants
# ======================

VALID_MODES = ["proxy", "original", "get_base_target", "analyze_and_embed", "send_extracted_metadata"]
SDNA_ROOT = "/opt/sdna"
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir(SDNA_ROOT + "/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
NORMALIZER_SCRIPT_PATH = SDNA_ROOT + "/bin/rekognition_metadata_normalizer.py"
NGINX_CONFIG_PATH_LINUX = SDNA_ROOT + "/nginx/ai-config.json"
NGINX_CONFIG_PATH_MAC = "/Library/Application Support/StorageDNA/nginx/ai-config.json"
AI_EXPORT_KEY = "ai_export_shared_drive_path"
AI_PROXY_KEY = "ai_proxy_shared_drive_path"

S3_METADATA_MAX_SIZE = 2048

IS_LINUX = os.path.isdir(SDNA_ROOT + "/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

SDNA_EVENT_MAP = {
    "LABEL_DETECTION": "aws_vid_rekognition_label",
    "FACE_DETECTION": "aws_vid_rekognition_face",
    "PERSON_TRACKING": "aws_vid_rekognition_person",
    "TEXT_DETECTION": "aws_vid_rekognition_text",
    "CONTENT_MODERATION": "aws_vid_rekognition_moderation",
    "CELEBRITY_RECOGNITION": "aws_vid_rekognition_celebrity"
}

# ======================
# Logging & Utilities
# ======================

logger = logging.getLogger()

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

# ======================
# Configuration Readers
# ======================

def read_client_config(key: str):
    """Unified config reader for Linux (.conf) and macOS (.plist)."""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            return parser.get('General', key, fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                return plistlib.load(fp).get(key, "")
    except Exception as e:
        logging.error(f"Error reading client config key '{key}': {e}")
        return ""

def get_cloud_config_path():
    folder = read_client_config("CloudConfigFolder")
    path = os.path.join(folder, "cloud_targets.conf")
    logging.info(f"Using cloud config path: {path}")
    return path

def get_link_address_and_port():
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
        if IS_LINUX:
            for line in lines:
                if '=' in line:
                    k, v = map(str.strip, line.split('=', 1))
                    if k.endswith('link_address'):
                        ip = v
                    elif k.endswith('link_port'):
                        port = v
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
    config_path = NGINX_CONFIG_PATH_LINUX if IS_LINUX else NGINX_CONFIG_PATH_MAC
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        meta = config_data.get(AI_EXPORT_KEY, "").strip()
        proxy = config_data.get(AI_PROXY_KEY, "").strip()
        if not meta or not proxy:
            logging.error("Store paths missing in ai-config.json")
            sys.exit(5)
        logging.info(f"Metadata Store Path: {meta}, Proxy Store Path: {proxy}")
        return meta, proxy
    except Exception as e:
        logging.error(f"Failed to read store paths from {config_path}: {e}")
        sys.exit(5)

def get_admin_dropbox_path():
    return read_client_config("LogPath") or None

def get_node_api_key():
    key = read_client_config("NodeAPIKey")
    if not key:
        logging.error("Node API key not found in configuration.")
        sys.exit(5)
    logging.info("Successfully retrieved Node API key.")
    return key

# ======================
# HTTP & Retry Helpers
# ======================

def get_retry_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, retries=3, **kwargs):
    session = get_retry_session()
    last_exception = None
    for attempt in range(retries):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < retries - 1:
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, [1, 1, 5][attempt])
                delay = base_delay + jitter
                logging.warning(f"Attempt {attempt + 1} failed: {type(e).__name__}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise
    if last_exception:
        raise last_exception
    return None

# ======================
# AI Config Loader
# ======================

def normalize_feature_list(value):
    if isinstance(value, str):
        return [x.strip().upper() for x in value.split(",") if x.strip()]
    return [f.upper() if isinstance(f, str) else f for f in value]

def get_advanced_ai_config(config_name, provider_name="aws"):
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
                for key, val in raw.items():
                    clean_key = key[len(provider_key) + 1:] if key.startswith(f"{provider_key}_") else key
                    if clean_key.startswith("image_rekognition_") or clean_key.startswith("video_rekognition_") or clean_key in {
                        "rekognition_region", "video_rekognition_enabled", "image_rekognition_enabled"
                    }:
                        out[clean_key] = val
                # Normalize booleans
                for bool_key in ["image_rekognition_enabled", "video_rekognition_enabled"]:
                    if bool_key in out:
                        v = out[bool_key]
                        out[bool_key] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes", "on")
                # Normalize feature lists
                for feat_key in ["image_rekognition_features", "video_rekognition_features"]:
                    if feat_key in out:
                        out[feat_key] = normalize_feature_list(out[feat_key])
                # Normalize numbers
                for num_key in ["image_rekognition_labels_min_confidence", "image_rekognition_max_labels", "video_rekognition_min_confidence"]:
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

# ======================
# Metadata Handling
# ======================

def prepare_metadata_to_upload(backlink_url, properties_file=None):
    metadata = {"fabric URL": backlink_url}
    if not properties_file or not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return metadata
    try:
        if properties_file.lower().endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata = json.load(f)
        elif properties_file.lower().endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        metadata[key] = value
            else:
                logging.error("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',', 1)
                    if len(parts) == 2:
                        metadata[parts[0].strip()] = parts[1].strip()
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")
    return metadata

def truncate_metadata_to_fit(metadata, max_size=S3_METADATA_MAX_SIZE):
    essential_fields = ['fabric URL', 'fabric-url', 'filename', 'file-name']
    truncated = {}
    current_size = 0
    def field_size(k, v):
        key_norm = k.lower().replace(' ', '-')
        return len(key_norm) + 11 + len(str(v)) + 2
    for key in essential_fields:
        if key in metadata:
            v = str(metadata[key])
            if current_size + field_size(key, v) <= max_size:
                truncated[key] = v
                current_size += field_size(key, v)
    for key, value in metadata.items():
        if key not in essential_fields:
            v = str(value)
            if current_size + field_size(key, v) <= max_size:
                truncated[key] = v
                current_size += field_size(key, v)
    return truncated

def get_aws_video_rek_normalized_metadata(raw_path, norm_path):
    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logging.error(f"Normalizer script not found: {NORMALIZER_SCRIPT_PATH}")
        return False
    try:
        subprocess.run(["python3", NORMALIZER_SCRIPT_PATH, "-i", raw_path, "-o", norm_path], check=True)
        return True
    except Exception as e:
        logging.error(f"Normalization failed: {e}")
        return False

def transform_normlized_to_enriched(norm_path, filetype_prefix):
    try:
        with open(norm_path, "r") as f:
            data = json.load(f)
        result = []
        for event_type, detections in data.items():
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, 'unknown')
            for detection in detections:
                occurrences = detection.get("occurrences", [])
                result.append({
                    "eventType": event_type,
                    "sdnaEventType": sdna_event_type,
                    "sdnaEventTypePrefix": filetype_prefix,
                    "eventValue": detection.get("value", "Unidentified"),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                })
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    meta_path, _ = get_store_paths()
    if not meta_path:
        logging.error("Metadata Store settings not found.")
        return None, None
    provider = config.get("provider", "aws_rekognition")
    base = os.path.splitext(os.path.basename(file_path))[0]
    repo_guid_str = str(repo_guid)
    if "/./" in meta_path:
        meta_left, meta_right = meta_path.split("/./", 1)
    else:
        meta_left, meta_right = meta_path, ""
    meta_left = meta_left.rstrip("/")
    metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)
    raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
    norm_json = os.path.join(metadata_dir, f"{base}_norm.json")
    raw_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_raw.json")
    norm_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_norm.json")
    for attempt in range(max_attempts):
        try:
            with open(raw_json, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            if not get_aws_video_rek_normalized_metadata(raw_json, norm_json):
                logging.error("Normalization failed.")
                return raw_return, None
            return raw_return, norm_return
        except Exception as e:
            logging.warning(f"Metadata write failed (Attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))
    return None, None

# ======================
# Node API Calls
# ======================

def update_catalog(repo_guid, file_path, upload_path, bucket, max_attempts=5):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    headers = {"apikey": get_node_api_key(), "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": "s3",
        "providerData": {"bucket": bucket, "s3_key": upload_path}
    }
    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info("Catalog updated successfully.")
                return True
            if response and response.status_code == 404:
                message = response.json().get('message', '').strip().lower()
                if message == "catalog item not found":
                    wait_time = 60 + (attempt * 10)
                    logging.warning(f"[404] Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
            logging.warning(f"[Attempt {attempt+1}] Status: {response.status_code if response else 'None'}")
        except Exception as e:
            logging.exception(f"Unexpected error in update_catalog: {e}")
        if attempt < max_attempts - 1:
            time.sleep(5 + attempt * 2)
    return False

def send_extracted_metadata(config, repo_guid, file_path, raw_path, norm_path=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    headers = {"apikey": get_node_api_key(), "Content-Type": "application/json"}
    metadata_array = []
    if norm_path:
        metadata_array.append({"type": "metadataFilePath", "path": norm_path})
    if raw_path:
        metadata_array.append({"type": "metadataRawJsonFilePath", "path": raw_path})
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "AWS Rekognition"),
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
            if attempt == max_attempts - 1:
                logging.critical(f"Failed after {max_attempts} attempts: {e}")
                raise
        time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))
    return False

def send_ai_enriched_metadata(config, repo_guid, file_path, enriched, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
    headers = {"apikey": get_node_api_key(), "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "aws_rekognition"),
        "normalizedMetadata": enriched
    }
    for attempt in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            if attempt == max_attempts - 1:
                logging.critical(f"Failed after {max_attempts} attempts: {e}")
                raise
        time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))
    return False

# ======================
# S3 Operations
# ======================

def get_s3_bucket_region(bucket_name, config_data):
    retry_config = Config(retries={'max_attempts': 3, 'mode': 'adaptive'}, connect_timeout=10, read_timeout=10)
    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    try:
        s3 = boto3.client(**client_kwargs)
        s3.head_bucket(Bucket=bucket_name)
        return config_data.get('region', '')
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == '400':
            return e.response['Error'].get('Region')
        elif code in ('301', 'PermanentRedirect'):
            region = e.response.get('Region') or e.response.get('Endpoint', '').split('.')[1]
            return region
        logging.error(f"Client error getting bucket region: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return None

def upload_file_to_s3(config_data, bucket_name, source_path, upload_path, metadata=None):
    retry_config = Config(
        retries={'max_attempts': 5, 'mode': 'adaptive'},
        connect_timeout=15,
        read_timeout=30
    )
    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    if 'region' in config_data:
        client_kwargs['region_name'] = config_data['region']
    if 'endpoint_url' in config_data:
        client_kwargs['endpoint_url'] = config_data['endpoint_url']
    try:
        s3 = boto3.client(**client_kwargs)
        upload_path = upload_path.lstrip('/')
        sanitized_metadata = {k.lower().replace(' ', '-'): str(v) for k, v in metadata.items()} if metadata else None
        s3.upload_file(Filename=source_path, Bucket=bucket_name, Key=upload_path,
                       ExtraArgs={'Metadata': sanitized_metadata} if sanitized_metadata else {})
        logging.info(f"Uploaded {source_path} to s3://{bucket_name}/{upload_path}")
        return {"status": 200, "detail": "Success"}
    except ClientError as e:
        code = e.response['Error']['Code']
        if code.startswith('4'):
            logging.error(f"Client error: {e}")
            return {"status": 400, "detail": str(e)}
        logging.error(f"Transient S3 error: {e}")
        return {"status": 500, "detail": str(e)}
    except Exception as e:
        logging.error(f"Unexpected S3 error: {e}")
        return {"status": 500, "detail": str(e)}

def remove_file_from_s3(bucket_name, file_key, config_data):
    retry_config = Config(retries={'max_attempts': 5, 'mode': 'adaptive'}, connect_timeout=15, read_timeout=30)
    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    if 'region' in config_data:
        client_kwargs['region_name'] = config_data['region']
    if 'endpoint_url' in config_data:
        client_kwargs['endpoint_url'] = config_data['endpoint_url']
    try:
        s3 = boto3.client(**client_kwargs)
        s3.delete_object(Bucket=bucket_name, Key=file_key.lstrip("/").replace("\\", "/"))
        logging.info(f"Deleted s3://{bucket_name}/{file_key}")
        return True
    except Exception as e:
        logging.error(f"S3 deletion error: {e}")
        return False

# ======================
# Rekognition – Image
# ======================

def get_image_rek_commands_from_features(features):
    feature_map = {
        "LABEL_DETECTION": "detect_labels",
        "FACE_DETECTION": "detect_faces",
        "TEXT_DETECTION": "detect_text",
        "CONTENT_MODERATION": "detect_moderation_labels",
        "CELEBRITY_RECOGNITION": "recognize_celebrities",
        "SAFETY_EQUIPMENT_ANALYSIS": "detect_protective_equipment"
    }
    return [feature_map[f] for f in features if f in feature_map]

def get_image_rekogniton_data(image_input, aws_access_key, aws_secret_key, features, region="us-west-1", is_s3_uri=True):
    try:
        rekognition = boto3.client('rekognition', aws_access_key_id=aws_access_key,
                                   aws_secret_access_key=aws_secret_key, region_name=region)
        metadata = {}
        for command in get_image_rek_commands_from_features(features):
            func = getattr(rekognition, command)
            if is_s3_uri:
                bucket, key = image_input[5:].split('/', 1)
                param = {'S3Object': {'Bucket': bucket, 'Name': key}}
            else:
                param = {'Bytes': image_input}
            response = func(Image=param)
            metadata[command] = response
        return metadata
    except Exception as e:
        logging.error(f"Rekognition error: {e}")
        return {}

# ======================
# Rekognition – Video
# ======================

def start_video_rekognition_job(s3_uri, access_key, secret_key, features, region, min_confidence=80):
    client = boto3.client('rekognition', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
    job_ids = {}
    bucket = s3_uri.split('/')[2]
    key = '/'.join(s3_uri.split('/')[3:])
    video = {'S3Object': {'Bucket': bucket, 'Name': key}}
    mapping = {
        "LABEL_DETECTION": client.start_label_detection,
        "FACE_DETECTION": client.start_face_detection,
        "PERSON_TRACKING": client.start_person_tracking,
        "TEXT_DETECTION": client.start_text_detection,
        "CELEBRITY_RECOGNITION": client.start_celebrity_recognition,
        "CONTENT_MODERATION": client.start_content_moderation
    }
    for feat in features:
        try:
            func = mapping.get(feat)
            if not func:
                logging.warning(f"Unsupported feature: {feat}")
                continue
            kwargs = {'Video': video}
            if feat in ("LABEL_DETECTION", "CONTENT_MODERATION"):
                kwargs['MinConfidence'] = min_confidence
            resp = func(**kwargs)
            job_ids[feat] = resp['JobId']
            logging.info(f"Started {feat}: {resp['JobId']}")
        except Exception as e:
            logging.error(f"Failed to start {feat}: {e}")
    return job_ids

def get_rekognition_job_result(job_id, feature, access_key, secret_key, region):
    client = boto3.client('rekognition', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
    mapping = {
        "LABEL_DETECTION": client.get_label_detection,
        "FACE_DETECTION": client.get_face_detection,
        "PERSON_TRACKING": client.get_person_tracking,
        "TEXT_DETECTION": client.get_text_detection,
        "CELEBRITY_RECOGNITION": client.get_celebrity_recognition,
        "CONTENT_MODERATION": client.get_content_moderation
    }
    try:
        func = mapping.get(feature)
        if func:
            return func(JobId=job_id)
    except Exception as e:
        logging.error(f"Error fetching {feature} result: {e}")
    return None

# ======================
# Transcribe – Placeholder for Future Expansion
# ======================

# TODO: Integrate speaker diarization, PII redaction, custom vocabularies, etc.
# See AWS Transcribe docs for:
#   - Settings: ShowSpeakerLabels, MaxSpeakerLabels
#   - ContentRedactionType, PiiEntityTypes
#   - LanguageModelName, VocabularyName

def start_transcription_job(s3_uri, aws_access_key, aws_secret_key, region, language_code="en-US", job_name=None,
                           show_speaker_labels=False, max_speaker_labels=2,
                           enable_pii_redaction=False, pii_entity_types=None):
    transcribe = boto3.client('transcribe', aws_access_key_id=aws_access_key,
                              aws_secret_access_key=aws_secret_key, region_name=region)
    if not job_name:
        job_name = f"transcribe-{uuid.uuid4()}"
    ext = s3_uri.split('.')[-1].lower()
    media_format = {'mp3':'mp3','mp4':'mp4','wav':'wav','flac':'flac','ogg':'ogg','amr':'amr','webm':'webm','m4a':'mp4'}.get(ext, 'mp4')
    settings = {}
    if show_speaker_labels:
        settings['ShowSpeakerLabels'] = True
        settings['MaxSpeakerLabels'] = max_speaker_labels
    if enable_pii_redaction:
        settings['ContentRedaction'] = {
            'RedactionType': 'PII',
            'RedactionOutput': 'redacted'
        }
        if pii_entity_types:
            settings['ContentRedaction']['PiiEntityTypes'] = pii_entity_types
    try:
        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': s3_uri},
            MediaFormat=media_format,
            LanguageCode=language_code,
            **({'Settings': settings} if settings else {})
        )
        logging.info(f"Started transcription job: {job_name}")
        return job_name
    except Exception as e:
        logging.error(f"Transcribe start failed: {e}")
        return None

def wait_for_transcription_job(job_name, aws_access_key, aws_secret_key, region, max_wait_time=3600, poll_interval=30):
    transcribe = boto3.client('transcribe', aws_access_key_id=aws_access_key,
                              aws_secret_access_key=aws_secret_key, region_name=region)
    elapsed = 0
    while elapsed < max_wait_time:
        try:
            resp = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            status = resp['TranscriptionJob']['TranscriptionJobStatus']
            if status == 'COMPLETED':
                uri = resp['TranscriptionJob']['Transcript']['TranscriptFileUri']
                with urllib.request.urlopen(uri) as f:
                    return json.loads(f.read().decode())
            elif status == 'FAILED':
                reason = resp['TranscriptionJob'].get('FailureReason', 'Unknown')
                logging.error(f"Transcribe job failed: {reason}")
                return None
        except Exception as e:
            logging.error(f"Error polling transcribe job: {e}")
            return None
        time.sleep(poll_interval)
        elapsed += poll_interval
    logging.warning(f"Timeout waiting for transcribe job: {job_name}")
    return None

# ======================
# Mode Handlers
# ======================

def handle_analyze_and_embed(args, cloud_config_data):
    logging.info("Running in 'analyze_and_embed' mode")
    if not args.output_path:
        logging.error("--output-path is required")
        sys.exit(1)
    ai_config = get_advanced_ai_config(args.config_name, cloud_config_data.get("provider","rekognition")) or {}
    features = ai_config.get("rekognition_features", [
        "LABEL_DETECTION", "FACE_DETECTION", "CONTENT_MODERATION",
        "TEXT_DETECTION", "SAFETY_EQUIPMENT_ANALYSIS", "CELEBRITY_RECOGNITION"
    ])
    region = cloud_config_data.get("region", "us-east-1")
    MAX_DIRECT_SIZE = 4 * 1024 * 1024
    matched_file_size = os.stat(args.source_path).st_size
    if matched_file_size <= MAX_DIRECT_SIZE:
        logging.info("File ≤ 4 MB: using direct byte upload")
        try:
            with open(args.source_path, "rb") as f:
                image_bytes = f.read()
        except Exception as e:
            logging.error(f"Failed to read image: {e}")
            sys.exit(1)
        ai_metadata = get_image_rekogniton_data(
            image_input=image_bytes,
            aws_access_key=cloud_config_data['access_key_id'],
            aws_secret_key=cloud_config_data['secret_access_key'],
            features=features,
            region=region,
            is_s3_uri=False
        )
    else:
        logging.info("File > 4 MB: uploading to S3 first")
        response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, args.upload_path)
        if response["status"] != 200:
            logging.error(f"S3 upload failed: {response['detail']}")
            sys.exit(1)
        s3_uri = f"s3://{args.bucket_name}/{args.upload_path.lstrip('/').replace(chr(92), '/')}"
        ai_metadata = get_image_rekogniton_data(
            image_input=s3_uri,
            aws_access_key=cloud_config_data['access_key_id'],
            aws_secret_key=cloud_config_data['secret_access_key'],
            features=features,
            region=region,
            is_s3_uri=True
        )
        if ai_metadata:
            if not remove_file_from_s3(args.bucket_name, args.upload_path, cloud_config_data):
                logging.warning("Failed to delete temporary S3 object")
    if ai_metadata:
        try:
            with open(args.output_path, "w", encoding="utf-8") as f:
                json.dump(ai_metadata, f, indent=2, ensure_ascii=False)
            logging.info(f"AI metadata saved to {args.output_path}")
        except Exception as e:
            logging.error(f"Failed to write output: {e}")
            sys.exit(15)
    else:
        logging.error("No AI metadata generated")
        sys.exit(16)
    sys.exit(0)

def handle_send_extracted_metadata_video_resume(args, cloud_config_data, advanced_ai_settings, clean_catalog_path):
    if not (args.asset_id and args.repo_guid and args.catalog_path):
        logging.error("asset-id, repo-guid, catalog-path required")
        sys.exit(1)
    features = advanced_ai_settings.get("video_rekognition_features", [])
    if not features:
        logging.error("No video Rekognition features configured.")
        sys.exit(1)
    meta_path, _ = get_store_paths()
    if "/./" in meta_path:
        physical_root, logical_suffix = meta_path.split("/./", 1)
    else:
        physical_root, logical_suffix = meta_path, ""
    tracking_dir = os.path.join(physical_root.rstrip("/"), logical_suffix, str(args.repo_guid), clean_catalog_path, "aws_rekognition")
    analysis_id = args.asset_id
    tracker_path = os.path.join(tracking_dir, f"{analysis_id}_status.json")
    if not os.path.exists(tracker_path):
        logging.error(f"Tracker not found: {tracker_path}")
        sys.exit(1)
    with open(tracker_path, 'r') as f:
        tracker = json.load(f)
    s3_uri = tracker.get("asset_s3_uri")
    if not s3_uri:
        logging.error("asset_s3_uri missing in tracker.")
        sys.exit(1)
    region = cloud_config_data.get("rekognition_region", "us-east-1")
    min_conf = advanced_ai_settings.get("video_rekognition_min_confidence", 80)
    # Phase 1: Retry failed starts
    for feat, status in tracker["feature_status"].items():
        if not status["completed"] and status["job_id"] is None:
            single_job_ids = start_video_rekognition_job(s3_uri, cloud_config_data['access_key_id'],
                                                        cloud_config_data['secret_access_key'], [feat], region, min_conf)
            new_id = single_job_ids.get(feat)
            if new_id:
                status["job_id"] = new_id
                logging.info(f"Restarted {feat}: {new_id}")
            else:
                status["completed"] = True
                logging.warning(f"Permanently skipping {feat}")
    # Phase 2: Poll pending jobs
    pending_jobs = {f: s["job_id"] for f, s in tracker["feature_status"].items() if not s["completed"] and s["job_id"]}
    if pending_jobs:
        start_time = time.time()
        while pending_jobs and (time.time() - start_time) < 900:
            for feat in list(pending_jobs.keys()):
                job_id = pending_jobs[feat]
                result = get_rekognition_job_result(job_id, feat, cloud_config_data['access_key_id'],
                                                    cloud_config_data['secret_access_key'], region)
                if result and result.get('JobStatus') == 'SUCCEEDED':
                    raw_filename = f"{analysis_id}_{feat}_raw.json"
                    raw_path = os.path.join(tracking_dir, raw_filename)
                    with open(raw_path, 'w') as rf:
                        json.dump(result, rf, indent=2)
                    tracker["feature_status"][feat].update({"completed": True, "raw_file": raw_filename})
                    del pending_jobs[feat]
                    logging.info(f"{feat} completed on resume.")
                elif result and result.get('JobStatus') in ('FAILED', 'PARTIAL_SUCCESS'):
                    tracker["feature_status"][feat]["completed"] = True
                    del pending_jobs[feat]
                    reason = result.get('FailureReason', 'Unknown')
                    logging.error(f"{feat} failed: {reason}")
            if pending_jobs:
                time.sleep(30)
    # Phase 3: Update tracker
    all_done = all((s["completed"] if s.get("job_id") is not None else True) for s in tracker["feature_status"].values())
    tracker["all_done"] = all_done
    with open(tracker_path, 'w') as f:
        json.dump(tracker, f, indent=2)
    # Phase 4: Finalize
    if all_done:
        combined = {"rekognition_video_analysis": {}}
        for feat, status in tracker["feature_status"].items():
            if status["raw_file"]:
                with open(os.path.join(tracking_dir, status["raw_file"]), 'r') as f:
                    combined["rekognition_video_analysis"][feat] = json.load(f)
            else:
                combined["rekognition_video_analysis"][feat] = {}
        raw_ret, norm_ret = store_metadata_file(cloud_config_data, args.repo_guid, clean_catalog_path, combined)
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_ret, norm_ret):
            logging.error("Failed to send extracted metadata.")
            sys.exit(1)
        if norm_ret:
            try:
                full_norm = os.path.join(physical_root, norm_ret)
                enriched, ok = transform_normlized_to_enriched(full_norm, args.enrich_prefix)
                if ok and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                    logging.info("AI enriched metadata sent.")
                else:
                    logging.warning("AI enrichment send failed.")
            except Exception:
                logging.exception("Enrichment failed.")
        logging.info("Video analysis resume completed.")
    else:
        logging.info("Not all features done. Coordinator may retry.")
    sys.exit(0)

def handle_normal_upload(args, cloud_config_data, backlink_url, clean_catalog_path):
    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File: {args.source_path}, Upload path: {args.upload_path}")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata from: {args.metadata_file}")
        sys.exit(0)
    logging.info("Starting S3 upload")
    metadata_obj = prepare_metadata_to_upload(backlink_url, args.metadata_file)
    parsed = truncate_metadata_to_fit(metadata_obj) if metadata_obj else None
    if not args.bucket_name or not args.upload_path:
        logging.error("Bucket or upload path missing.")
        sys.exit(1)
    response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, args.upload_path, parsed)
    if response["status"] != 200:
        logging.error(f"Upload failed: {response['detail']}")
        sys.exit(1)
    if args.repo_guid and args.catalog_path:
        update_catalog(args.repo_guid, clean_catalog_path, args.upload_path, args.bucket_name)
    s3_uri = f"s3://{args.bucket_name}/{args.upload_path.lstrip('/').replace(chr(92), '/')}"
    # Video Rekognition (if enabled)
    if args.enrich_prefix == "vid" and cloud_config_data.get("export_ai_metadata") == "true":
        ai_config = get_advanced_ai_config(args.config_name, cloud_config_data.get("provider", "s3")) or {}
        features = ai_config.get("video_rekognition_features", [])
        if features:
            meta_path, _ = get_store_paths()
            if "/./" in meta_path:
                physical_root, logical_suffix = meta_path.split("/./", 1)
            else:
                physical_root, logical_suffix = meta_path, ""
            tracking_dir = os.path.join(physical_root.rstrip("/"), logical_suffix, str(args.repo_guid), clean_catalog_path, "aws_rekognition")
            analysis_id = str(uuid.uuid4())
            tracker_path = os.path.join(tracking_dir, f"{analysis_id}_status.json")
            region = cloud_config_data.get("rekognition_region", "us-east-1")
            min_conf = ai_config.get("video_rekognition_min_confidence", 80)
            job_ids = start_video_rekognition_job(s3_uri, cloud_config_data['access_key_id'], cloud_config_data['secret_access_key'], features, region, min_conf)
            feature_status = {
                feat: {
                    "job_id": job_ids.get(feat),
                    "completed": False,
                    "raw_file": None
                } for feat in features
            }
            tracker = {
                "analysis_id": analysis_id,
                "asset_file": os.path.basename(args.upload_path) or "unknown",
                "asset_s3_uri": s3_uri,
                "features_requested": features,
                "feature_status": feature_status,
                "all_done": False
            }
            os.makedirs(tracking_dir, exist_ok=True)
            with open(tracker_path, 'w') as f:
                json.dump(tracker, f, indent=2)
            logging.info(f"Tracker saved: {tracker_path}")
            # Poll up to 15 minutes
            start_time = time.time()
            while (time.time() - start_time) < 900:
                pending = False
                for feat in features:
                    status = tracker["feature_status"][feat]
                    if status["completed"] or not status["job_id"]:
                        continue
                    pending = True
                    result = get_rekognition_job_result(status["job_id"], feat, cloud_config_data['access_key_id'],
                                                        cloud_config_data['secret_access_key'], region)
                    if result and result.get('JobStatus') == 'SUCCEEDED':
                        raw_filename = f"{analysis_id}_{feat}_raw.json"
                        raw_path = os.path.join(tracking_dir, raw_filename)
                        with open(raw_path, 'w') as rf:
                            json.dump(result, rf, indent=2)
                        status.update({"completed": True, "raw_file": raw_filename})
                        logging.info(f"{feat} succeeded.")
                    elif result and result.get('JobStatus') in ('FAILED', 'PARTIAL_SUCCESS'):
                        status["completed"] = True
                        reason = result.get('FailureReason', 'Unknown')
                        logging.error(f"{feat} failed: {reason}")
                if not pending:
                    break
                time.sleep(30)
            all_done = all((s["completed"] if s.get("job_id") is not None else True) for s in tracker["feature_status"].values())
            tracker["all_done"] = all_done
            with open(tracker_path, 'w') as f:
                json.dump(tracker, f, indent=2)
            if all_done:
                combined = {"rekognition_video_analysis": {}}
                for feat in features:
                    raw_file = tracker["feature_status"][feat]["raw_file"]
                    if raw_file:
                        with open(os.path.join(tracking_dir, raw_file), 'r') as f:
                            combined["rekognition_video_analysis"][feat] = json.load(f)
                    else:
                        combined["rekognition_video_analysis"][feat] = {}
                raw_ret, norm_ret = store_metadata_file(cloud_config_data, args.repo_guid, clean_catalog_path, combined)
                if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_ret, norm_ret):
                    logging.error("Failed to send extracted metadata.")
                    sys.exit(1)
                if norm_ret:
                    try:
                        full_norm = os.path.join(physical_root, norm_ret)
                        enriched, ok = transform_normlized_to_enriched(full_norm, args.enrich_prefix)
                        if ok and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                            logging.info("AI enriched metadata sent.")
                    except Exception:
                        logging.exception("Enrichment failed.")
            else:
                logging.error("Not all video analysis jobs completed.")
                sys.exit(7)
    logging.info("All parts uploaded successfully to S3")
    sys.exit(0)

# ======================
# Main Entry Point
# ======================

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Operation mode")
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-j", "--job-guid")
    parser.add_argument("-b", "--bucket-name")
    parser.add_argument("-id", "--asset-id")
    parser.add_argument("-cp", "--catalog-path")
    parser.add_argument("-sp", "--source-path", required=True)
    parser.add_argument("-mp", "--metadata-file")
    parser.add_argument("-up", "--upload-path")
    parser.add_argument("-sl", "--size-limit")
    parser.add_argument("-r", "--repo-guid")
    parser.add_argument("-o", "--output-path")
    parser.add_argument("--enrich-prefix")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="debug")
    parser.add_argument("--controller-address")
    parser.add_argument("--export-ai-metadata")
    args = parser.parse_args()

    setup_logging(args.log_level)
    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode. Allowed: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Missing cloud config: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config section missing: {args.config_name}")
        sys.exit(1)

    cloud_config_data = dict(cloud_config[args.config_name])
    if args.export_ai_metadata:
        cloud_config_data["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"

    provider_name = cloud_config_data.get("provider", "s3")
    advanced_ai_settings = get_advanced_ai_config(args.config_name, provider_name)
    if not advanced_ai_settings:
        logging.warning("No advanced AI settings loaded.")

    # Validate source file
    if not os.path.exists(args.source_path):
        logging.error(f"File not found: {args.source_path}")
        sys.exit(4)

    matched_file_size = os.stat(args.source_path).st_size
    if mode == "original" and args.size_limit:
        try:
            limit_mb = float(args.size_limit)
            if matched_file_size > limit_mb * 1024 * 1024:
                logging.error(f"File too large: {matched_file_size / (1024*1024):.2f} MB > {limit_mb} MB")
                sys.exit(4)
        except Exception as e:
            logging.warning(f"Size limit validation failed: {e}")

    # Build backlink URL
    backlink_url = ""
    clean_catalog_path = ""
    if args.catalog_path:
        clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        normalized_path = args.catalog_path.replace("\\", "/")
        relative_path = normalized_path.split("/1/", 1)[-1] if "/1/" in normalized_path else normalized_path
        catalog_url = urllib.parse.quote(relative_path)
        filename_enc = urllib.parse.quote(extract_file_name(args.source_path if mode == "original" else args.catalog_path))
        job_guid = args.job_guid
        if args.controller_address and len(args.controller_address.split(":")) == 2:
            client_ip, client_port = args.controller_address.split(":")
        else:
            client_ip, client_port = get_link_address_and_port()
        backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"

    # Dispatch by mode
    if mode == "analyze_and_embed":
        handle_analyze_and_embed(args, cloud_config_data)
    elif mode == "send_extracted_metadata" and args.enrich_prefix == "vid":
        handle_send_extracted_metadata_video_resume(args, cloud_config_data, advanced_ai_settings, clean_catalog_path)
    else:
        handle_normal_upload(args, cloud_config_data, backlink_url, clean_catalog_path)