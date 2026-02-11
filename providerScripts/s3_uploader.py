import os
import subprocess
import uuid
import sys
import json
import boto3
import argparse
import logging
import urllib.parse
import urllib.request
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
import requests
import random
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError, EndpointConnectionError, ReadTimeoutError

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "analyze_and_embed"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/rekognition_metadata_normalizer.py"
S3_METADATA_MAX_SIZE = 2048

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

SDNA_EVENT_MAP = {
    "LABEL_DETECTION": "aws_vid_rekognition_label",
    "FACE_DETECTION": "aws_vid_rekognition_face",
    "PERSON_TRACKING": "aws_vid_rekognition_person",
    "TEXT_DETECTION": "aws_vid_rekognition_text",
    "CONTENT_MODERATION": "aws_vid_rekognition_moderation",
    "CELEBRITY_RECOGNITION": "aws_vid_rekognition_celebrity"
}

VIDEO_FEATURE_MAP = {
    "LABEL_DETECTION": {
        "start": "start_label_detection",
        "get": "get_label_detection",
        "supports_min_conf": True
    },
    "FACE_DETECTION": {
        "start": "start_face_detection",
        "get": "get_face_detection",
        "supports_min_conf": False
    },
    "CONTENT_MODERATION": {
        "start": "start_content_moderation",
        "get": "get_content_moderation",
        "supports_min_conf": True
    },
    "CELEBRITY_RECOGNITION": {
        "start": "start_celebrity_recognition",
        "get": "get_celebrity_recognition",
        "supports_min_conf": False
    },
    "TEXT_DETECTION": {
        "start": "start_text_detection",
        "get": "get_text_detection",
        "supports_min_conf": False
    },
    "PERSON_TRACKING": {
        "start": "start_person_tracking",
        "get": "get_person_tracking",
        "supports_min_conf": False
    },
    "FACE_SEARCH": {
        "start": "start_face_search",
        "get": "get_face_search",
        "supports_min_conf": True
    },
    "SEGMENT_DETECTION": {
        "start": "start_segment_detection",
        "get": "get_segment_detection",
        "supports_min_conf": False
    }
}

logger = logging.getLogger()

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('s3transfer').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    
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
    # handling retry delays manually via backoff in the range
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):  # Max 3 attempts
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:  # Only sleep if not last attempt
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])  # e.g., 1-2, 3-4, 10-15
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. "
                                f"Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None  # Should not reach here

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
                        clean_key = key
                        if key.startswith(f"{provider_key}_"):
                            clean_key = key[len(provider_key) + 1:]
                        if clean_key.startswith("image_rekognition_") or clean_key.startswith("video_rekognition_") or clean_key in {
                            "rekognition_region", "video_rekognition_enabled", "image_rekognition_enabled"
                        }:
                            out[clean_key] = val

                    for bool_key in ["image_rekognition_enabled", "video_rekognition_enabled"]:
                        if bool_key in out:
                            v = out[bool_key]
                            out[bool_key] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes", "on")

                    if "image_rekognition_features" in out:
                        feats = out["image_rekognition_features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        out["image_rekognition_features"] = [
                            f.upper() if isinstance(f, str) else f for f in feats
                        ]

                    if "video_rekognition_features" in out:
                        feats = out["video_rekognition_features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        out["video_rekognition_features"] = [
                            f.upper() if isinstance(f, str) else f for f in feats
                        ]

                    for num_key in ["image_rekognition_labels_min_confidence", "image_rekognition_max_labels"]:
                        if num_key in out and isinstance(out[num_key], str):
                            try:
                                out[num_key] = float(out[num_key]) if '.' in out[num_key] else int(out[num_key])
                            except ValueError:
                                del out[num_key]

                    if "video_rekognition_min_confidence" in out and isinstance(out["video_rekognition_min_confidence"], str):
                        try:
                            out["video_rekognition_min_confidence"] = float(out["video_rekognition_min_confidence"]) if '.' in out["video_rekognition_min_confidence"] else int(out["video_rekognition_min_confidence"])
                        except ValueError:
                            del out["video_rekognition_min_confidence"]

                    logging.info(f"Loaded and normalized advanced AI config from: {src}")
                    return out

            except Exception as e:
                logging.error(f"Failed to load {src}: {e}")

    return None

def estimate_metadata_size(metadata):
    total_size = 0
    for key, value in metadata.items():
        total_size += len(key.lower().replace(' ', '-')) + 11 + len(str(value)) + 2
    return total_size

def truncate_metadata_to_fit(metadata, max_size=S3_METADATA_MAX_SIZE):
    essential_fields = ['fabric URL', 'fabric-url', 'filename', 'file-name']
    truncated = {}
    current_size = 0
    
    for key in essential_fields:
        if key in metadata:
            value = str(metadata[key])
            field_size = len(key.lower().replace(' ', '-')) + 11 + len(value) + 2
            if current_size + field_size < max_size:
                truncated[key] = value
                current_size += field_size
    
    for key, value in metadata.items():
        if key not in essential_fields:
            value_str = str(value)
            field_size = len(key.lower().replace(' ', '-')) + 11 + len(value_str) + 2
            if current_size + field_size < max_size:
                truncated[key] = value_str
                current_size += field_size
    
    return truncated

def prepare_metadata_to_upload( backlink_url, properties_file = None):    
    metadata = {"fabric URL": backlink_url}
    
    if not properties_file or not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return metadata
    
    logging.debug(f"Reading properties from: {properties_file}")
    file_ext = properties_file.lower()
    try:
        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata = json.load(f)
                logging.debug(f"Loaded JSON properties: {metadata}")

        elif file_ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        metadata[key] = value
                logging.debug(f"Loaded XML properties: {metadata}")
            else:
                logging.error("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value
                logging.debug(f"Loaded CSV properties: {metadata}")
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")
    
    return metadata

def get_aws_video_rek_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path):
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
                    "eventValue": detection.get("value", "Unidentified"),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                }
                result.append(event_obj)
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    meta_path, proxy_path = get_store_paths()
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

    # Full local physical paths
    raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
    norm_json = os.path.join(metadata_dir, f"{base}_norm.json")

    raw_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_raw.json")
    norm_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_norm.json")

    raw_success = False
    norm_success = False

    for attempt in range(max_attempts):
        try:
            # RAW write
            with open(raw_json, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            raw_success = True

            # NORMALIZED write
            if not get_aws_video_rek_normalized_metadata(raw_json, norm_json):
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

def update_catalog(repo_guid, file_path, upload_path, bucket, max_attempts=5):
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
        "providerName": cloud_config_data.get("provider", "s3"),
        "providerData": {
            "bucket": bucket,
            "s3_key": upload_path,
        }
    }
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
            # --- Handle 404 explicitly ---
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
        # Default retry delay for non-404 or unhandled cases
        if attempt < max_attempts - 1:
            fallback_delay = 5 + attempt * 2
            logging.debug(f"Sleeping {fallback_delay}s before next attempt")
            time.sleep(fallback_delay)

    pass

def send_extracted_metadata(config, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, max_attempts=3):
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
                logger.info(f"Extended metadata save call response: {r.status_code} - {r.text}")
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
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "aws_rekognition"),
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

def get_s3_bucket_region(bucket_name, config_data):
    retry_config = Config(
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        connect_timeout=10,
        read_timeout=10
    )
    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    try:
        s3 = boto3.client(**client_kwargs)
        response = s3.head_bucket(Bucket=bucket_name)
        return config_data.get('region', '')
    except ClientError as e:
        if e.response['Error']['Code'] == '400':
            region = e.response['Error'].get('Region')
            if region:
                return region
        elif e.response['Error']['Code'] in ('301', 'PermanentRedirect'):
            region = e.response.get('Region') or e.response.get('Endpoint', '').split('.')[1]
            if region:
                return region
        logging.error(f"Failed to determine region for bucket {bucket_name}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting bucket region: {e}")
        return None

def upload_file_to_s3(config_data, bucket_name, source_path, upload_path, metadata=None):
    retry_config = Config(
        retries={
            'max_attempts': 5,
            'mode': 'adaptive'
        },
        connect_timeout=15,
        read_timeout=30,
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
        sanitized_metadata = None
        if metadata:
            sanitized_metadata = {k.lower().replace(' ', '-'): str(v) for k, v in metadata.items()}

        s3.upload_file(
            Filename=source_path,
            Bucket=bucket_name,
            Key=upload_path,
            ExtraArgs={'Metadata': sanitized_metadata} if sanitized_metadata else {}
        )

        logging.info(f"Successfully uploaded {source_path} to s3://{bucket_name}/{upload_path}")
        return {"status": 200, "detail": "Uploaded asset successfully"}
    except (ClientError, EndpointConnectionError, ReadTimeoutError, ConnectionError) as e:
        error_code = e.response['Error']['Code'] if isinstance(e, ClientError) else "Network/Timeout"
        if isinstance(e, ClientError) and e.response['Error']['Code'].startswith('4'):
            # Client error — don't retry
            logging.error(f"Client error during S3 upload: {e}")
            return {"status": 400, "detail": str(e)}
        logging.error(f"Transient S3 error: {e}")
        return {"status": 500, "detail": f"Transient S3 failure: {str(e)}"}

    except Exception as e:
        logging.error(f"Unexpected error during S3 upload: {e}")
        return {"status": 500, "detail": str(e)}

def remove_file_from_s3(bucket_name, file_key, config_data):
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectionError, EndpointConnectionError, ReadTimeoutError

    retry_config = Config(
        retries={
            'max_attempts': 5,
            'mode': 'adaptive'
        },
        connect_timeout=15,
        read_timeout=30,
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
    logging.debug(f"S3 client configuration for deletion: {client_kwargs}")
    try:
        s3 = boto3.client(**client_kwargs)
        file_key = file_key.lstrip("/").replace("\\", "/")
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        logging.info(f"Successfully deleted s3://{bucket_name}/{file_key}")
        return True

    except (ClientError, EndpointConnectionError, ReadTimeoutError, ConnectionError) as e:
        logging.error(f"Error during S3 file deletion: {e}")
        return False

    except Exception as e:
        logging.error(f"Unexpected error during S3 file deletion: {e}")
        return False

def get_image_rek_commands_from_features(features):
    commands = []
    feature_map = {
        "LABEL_DETECTION": "detect_labels",
        "FACE_DETECTION": "detect_faces",
        "TEXT_DETECTION": "detect_text",
        "CONTENT_MODERATION": "detect_moderation_labels",
        "CELEBRITY_RECOGNITION": "recognize_celebrities",
        "SAFETY_EQUIPMENT_ANALYSIS": "detect_protective_equipment"
    }
    for feature in features:
        if feature in feature_map:
            commands.append(feature_map[feature])
    return commands

def get_image_rekogniton_data(image_input, aws_access_key, aws_secret_key, features, region="us-west-1", is_s3_uri=True):
    try:
        logging.debug(f"Image Rekognition API call with features: {features}, mode: {'S3' if is_s3_uri else 'bytes'}")
        rekognition = boto3.client(
            'rekognition',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        rekogniton_metadata = {}
        for command in get_image_rek_commands_from_features(features):
            logging.debug(f"Executing Rekognition command: {command}")
            func = getattr(rekognition, command, None)
            if not func:
                logging.warning(f"Rekognition command {command} not found.")
                continue

            if is_s3_uri:
                if not isinstance(image_input, str) or not image_input.startswith("s3://"):
                    raise ValueError("Expected S3 URI but got invalid input")
                bucket_name, filepath = image_input[5:].split('/', 1)
                image_param = {'S3Object': {'Bucket': bucket_name, 'Name': filepath}}
            else:
                if not isinstance(image_input, bytes):
                    raise ValueError("Expected raw image bytes for direct analysis")
                image_param = {'Bytes': image_input}

            response = func(Image=image_param)
            rekogniton_metadata[command] = response

        return rekogniton_metadata

    except Exception as e:
        logging.error(f"Error in Rekognition API call: {e}")
        return {}

def start_video_rekognition_job(s3_uri,access_key,secret_key,features,region,min_confidence=80):
    client = boto3.client('rekognition',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name=region)

    job_ids = {}
    parsed = urllib.parse.urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    video = {'S3Object': {'Bucket': bucket, 'Name': key}}
    for feat in features:
        cfg = VIDEO_FEATURE_MAP.get(feat)
        if not cfg:
            logging.warning(f"Unsupported feature: {feat}")
            continue
        try:
            method = getattr(client, cfg["start"])
            kwargs = {"Video": video}
            if cfg.get("supports_min_conf"):
                kwargs["MinConfidence"] = min_confidence
            response = method(**kwargs)
            job_ids[feat] = response['JobId']
            logging.info(f"Started {feat} job: {response['JobId']}")
        except Exception as e:
            logging.error(f"Failed {feat}: {e}")
    return job_ids

def get_rekognition_job_result(job_id,feature,access_key,secret_key,region):
    client = boto3.client('rekognition',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name=region)
    cfg = VIDEO_FEATURE_MAP.get(feature)
    if not cfg:
        logging.error(f"Unsupported feature: {feature}")
        return None
    try:
        method = getattr(client, cfg["get"])
        return method(JobId=job_id)
    except Exception as e:
        logging.error(f"Error fetching {feature} result: {e}")
        return None

def start_transcription_job(s3_uri, aws_access_key, aws_secret_key, region, language_code="en-US", job_name=None):
    try:
        transcribe = boto3.client(
            'transcribe',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        if not job_name:
            import uuid
            job_name = f"transcribe-{uuid.uuid4()}"

        # Determine media format from file extension
        file_ext = s3_uri.split('.')[-1].lower()
        media_format_map = {
            'mp3': 'mp3',
            'mp4': 'mp4',
            'wav': 'wav',
            'flac': 'flac',
            'ogg': 'ogg',
            'amr': 'amr',
            'webm': 'webm',
            'm4a': 'mp4'
        }
        media_format = media_format_map.get(file_ext, 'mp4')

        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': s3_uri},
            MediaFormat=media_format,
            LanguageCode=language_code
        )

        logging.info(f"Started transcription job: {job_name}")
        return job_name

    except Exception as e:
        logging.error(f"Error starting transcription job: {e}")
        return None

def wait_for_transcription_job(job_name, aws_access_key, aws_secret_key, region, max_wait_time=3600, poll_interval=30):
    transcribe = boto3.client(
        'transcribe',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

    elapsed_time = 0

    while elapsed_time < max_wait_time:
        try:
            response = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            status = response['TranscriptionJob']['TranscriptionJobStatus']

            if status == 'COMPLETED':
                transcript_uri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
                
                # Download transcript
                with urllib.request.urlopen(transcript_uri) as url:
                    transcript_data = json.loads(url.read().decode())
                
                logging.info(f"Transcription job completed: {job_name}")
                return transcript_data

            elif status == 'FAILED':
                failure_reason = response['TranscriptionJob'].get('FailureReason', 'Unknown')
                logging.error(f"Transcription job failed: {failure_reason}")
                return None

            elif status == 'IN_PROGRESS':
                logging.debug(f"Transcription job in progress: {job_name}")

        except Exception as e:
            logging.error(f"Error checking transcription job status: {e}")
            return None

        time.sleep(poll_interval)
        elapsed_time += poll_interval

    logging.warning(f"Timeout waiting for transcription job: {job_name}")
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("-b", "--bucket-name", help= "Name of Bucket")
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata h6operations")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", required=True, help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("-o", "--output-path", help="Path where output will be saved")
    parser.add_argument("--enrich-prefix", help="Prefix to add for sdnaEventType of AI enrich data")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--controller-address",help="Link IP/Hostname Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata using Rekognition")
    args = parser.parse_args()

    setup_logging(args.log_level)

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Only allowed modes: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Missing cloud config: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    cloud_config_name = args.config_name
    if cloud_config_name not in cloud_config:
        logging.error(f"Missing cloud config section: {cloud_config_name}")
        sys.exit(1)

    cloud_config_data = cloud_config[cloud_config_name]
    
    if args.export_ai_metadata:
        cloud_config_data["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"

    # Load advanced AI settings
    provider_name = cloud_config_data.get("provider", "s3")
    advanced_ai_settings = get_advanced_ai_config(args.config_name, provider_name)
    if not advanced_ai_settings:
        logging.warning("No advanced AI settings available. AI metadata extraction will not happen.")

    logging.info(f"Starting S3 upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    matched_file = args.source_path
    catalog_path = args.catalog_path

    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    matched_file_size = os.stat(matched_file).st_size
    file_size_limit = args.size_limit
    if mode == "original" and file_size_limit:
        try:
            size_limit_bytes = float(file_size_limit) * 1024 * 1024
            if matched_file_size > size_limit_bytes:
                logging.error(f"File too large: {matched_file_size / (1024 * 1024):.2f} MB > limit of {file_size_limit} MB")
                sys.exit(4)
        except Exception as e:
            logging.warning(f"Could not validate size limit: {e}")

    backlink_url = ""
    clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1] if args.catalog_path else ""
    if args.upload_path and args.catalog_path:
        file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
        catalog_path = remove_file_name_from_path(args.catalog_path)
        normalized_path = catalog_path.replace("\\", "/")
        if "/1/" in normalized_path:
            relative_path = normalized_path.split("/1/", 1)[-1]
        else:
            relative_path = normalized_path
        catalog_url = urllib.parse.quote(relative_path)
        filename_enc = urllib.parse.quote(file_name_for_url)
        job_guid = args.job_guid

        if args.controller_address is not None and len(args.controller_address.split(":")) == 2:
            client_ip, client_port = args.controller_address.split(":")
        else:
            client_ip, client_port = get_link_address_and_port()

        backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
        logging.debug(f"Generated dashboard URL: {backlink_url}")

    if args.mode == "send_extracted_metadata" and args.enrich_prefix == "vid":
        # Validate required arguments (upload_path NOT required)
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("asset-id, repo-guid, and catalog-path required in send_extracted_metadata mode for video.")
            sys.exit(1)

        features = advanced_ai_settings.get("video_rekognition_features", [])
        if not features:
            logging.error("No video Rekognition features configured.")
            sys.exit(1)
        
        # Derive tracking directory (same as normal flow)
        meta_path, _ = get_store_paths()
        if not meta_path:
            logging.error("Metadata store path not configured.")
            sys.exit(5)

        if "/./" in meta_path:
            physical_root, logical_suffix = meta_path.split("/./", 1)
        else:
            physical_root, logical_suffix = meta_path, ""

        tracking_dir = os.path.join(
            physical_root.rstrip("/"),
            logical_suffix,
            str(args.repo_guid),
            clean_catalog_path,
            "aws_rekognition"
        )

        # Use asset_id as analysis_id
        analysis_id = args.asset_id
        tracker_path = os.path.join(tracking_dir, f"{analysis_id}_status.json")

        if not os.path.exists(tracker_path):
            logging.error(f"Tracker file not found: {tracker_path}")
            sys.exit(1)

        # Load existing tracker
        with open(tracker_path, 'r') as f:
            tracker = json.load(f)

        # Extract S3 URI from tracker (no CLI args needed)
        s3_uri = tracker.get("asset_s3_uri")
        if not s3_uri:
            logging.error("asset_s3_uri missing in tracker.")
            sys.exit(1)

        logging.info(f"Resuming video analysis: {analysis_id} for {s3_uri}")

        # AWS config
        region = cloud_config_data.get("rekognition_region", "us-east-1")
        min_conf = advanced_ai_settings.get("video_rekognition_min_confidence", 80)

        # --- Phase 1: Retry starting features that previously had job_id = None ---
        for feat, status in tracker["feature_status"].items():
            if not status["completed"] and status["job_id"] is None:
                logging.info(f"Retrying to start feature: {feat} (previously failed to launch)")
                single_job_ids = start_video_rekognition_job(
                    s3_uri,
                    cloud_config_data['access_key_id'],
                    cloud_config_data['secret_access_key'],
                    [feat],
                    region,
                    min_confidence=min_conf
                )
                new_job_id = single_job_ids.get(feat)
                if new_job_id:
                    status["job_id"] = new_job_id
                    logging.info(f"Restarted {feat} with JobId: {new_job_id}")
                else:
                    # Still can't start → mark as complete (skipped)
                    status["completed"] = True
                    logging.warning(f"Still unable to start {feat}. Permanently skipping.")

        # --- Phase 2: Poll all incomplete jobs ---
        pending_jobs = {}
        for feat, status in tracker["feature_status"].items():
            if not status["completed"] and status["job_id"] is not None:
                pending_jobs[feat] = status["job_id"]

        if pending_jobs:
            logging.info(f"Polling {len(pending_jobs)} pending jobs...")
            start_time = time.time()
            max_wait = 900  # 15 minutes
            poll_interval = 30

            while pending_jobs and (time.time() - start_time) < max_wait:
                for feat in list(pending_jobs.keys()):
                    job_id = pending_jobs[feat]
                    result = get_rekognition_job_result(
                        job_id, feat,
                        cloud_config_data['access_key_id'],
                        cloud_config_data['secret_access_key'],
                        region
                    )
                    if result:
                        job_status = result.get('JobStatus')
                        if job_status == 'SUCCEEDED':
                            raw_filename = f"{analysis_id}_{feat}_raw.json"
                            raw_path = os.path.join(tracking_dir, raw_filename)
                            with open(raw_path, 'w') as rf:
                                json.dump(result, rf, indent=2)
                            tracker["feature_status"][feat].update({
                                "completed": True,
                                "raw_file": raw_filename
                            })
                            del pending_jobs[feat]
                            logging.info(f"Feature {feat} completed on resume.")
                        elif job_status in ('FAILED', 'PARTIAL_SUCCESS'):
                            tracker["feature_status"][feat]["completed"] = True
                            del pending_jobs[feat]
                            reason = result.get('FailureReason', 'Unknown')
                            logging.error(f"Feature {feat} failed on resume: {reason}")
                    else:
                        logging.warning(f"Null result for {feat} (JobId: {job_id})")
                if pending_jobs:
                    time.sleep(poll_interval)

        # --- Phase 3: Update tracker ---
        all_done = all(
                (tracker["feature_status"][f]["completed"]
                 if tracker["feature_status"][f].get("job_id") is not None
                 else True)
                for f in features
            )
        tracker["all_done"] = all_done

        os.makedirs(tracking_dir, exist_ok=True)
        with open(tracker_path, 'w') as f:
            json.dump(tracker, f, indent=2)

        # --- Phase 4: Finalize if all done ---
        if all_done:
            logging.info("All features complete. Building combined metadata...")
            combined_metadata = {"rekognition_video_analysis": {}}
            for feat, status in tracker["feature_status"].items():
                if status["raw_file"]:
                    raw_path = os.path.join(tracking_dir, status["raw_file"])
                    with open(raw_path, 'r') as f:
                        combined_metadata["rekognition_video_analysis"][feat] = json.load(f)
                else:
                    combined_metadata["rekognition_video_analysis"][feat] = {}

            # Store via standard pipeline
            raw_return, norm_return = store_metadata_file(
                cloud_config_data, args.repo_guid, clean_catalog_path, combined_metadata
            )

            if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_return, norm_return):
                logging.error("Failed to send extracted metadata.")
                sys.exit(1)

            # Enrich if normalized metadata exists
            if norm_return:
                try:
                    full_norm_path = os.path.join(physical_root, norm_return)
                    enriched, success = transform_normlized_to_enriched(full_norm_path, args.enrich_prefix)
                    if success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                        logging.info("AI enriched metadata sent successfully.")
                    else:
                        logging.warning("AI enrichment send skipped or failed.")
                except Exception:
                    logging.exception("Enrichment failed during resume.")

            logging.info("Video analysis resume completed successfully.")
        else:
            logging.info("Not all features completed. Coordinator may retry later.")

        sys.exit(0)

    if mode == "analyze_and_embed":
        logging.info("Running in 'analyze_and_embed' mode")
        
        if not args.output_path:
            logging.error("--output-path is required in analyze_and_embed mode")
            sys.exit(1)

        # Load AI config
        ai_config = get_advanced_ai_config(args.config_name, cloud_config_data.get("provider","rekognition")) or {}
        features = ai_config.get("rekognition_features", [
            "LABEL_DETECTION", "FACE_DETECTION", "CONTENT_MODERATION",
            "TEXT_DETECTION", "SAFETY_EQUIPMENT_ANALYSIS", "CELEBRITY_RECOGNITION"
        ])
        region = cloud_config_data.get("region", "us-east-1")
        MAX_DIRECT_SIZE = 4 * 1024 * 1024  # 4 MB

        if matched_file_size <= MAX_DIRECT_SIZE:
            logging.info("File ≤ 4 MB: using direct byte upload to Rekognition")
            try:
                with open(args.source_path, "rb") as f:
                    image_bytes = f.read()
            except Exception as e:
                logging.error(f"Failed to read image file: {e}")
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
            # Large file: upload to S3 first
            logging.info("File > 4 MB: uploading to S3, then analyzing")
            upload_path = args.upload_path
            response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, upload_path)
            if response["status"] != 200:
                logging.error(f"Failed to upload large file to S3: {response['detail']}")
                sys.exit(1)

            s3_uri = f"s3://{args.bucket_name}/{upload_path.lstrip('/').replace(chr(92), '/')}"
            ai_metadata = get_image_rekogniton_data(
                image_input=s3_uri,
                aws_access_key=cloud_config_data['access_key_id'],
                aws_secret_key=cloud_config_data['secret_access_key'],
                features=features,
                region=region,
                is_s3_uri=True
            )

            # Clean up S3 object after successful analysis
            if ai_metadata:
                if remove_file_from_s3(args.bucket_name, upload_path, cloud_config_data):
                    logging.info("Temporary S3 object deleted after analysis")
                else:
                    logging.warning("Failed to delete temporary S3 object")

        # Save output
        if ai_metadata:
            try:
                with open(args.output_path, "w", encoding="utf-8") as f:
                    json.dump(ai_metadata, f, indent=2, ensure_ascii=False)
                logging.info(f"AI metadata saved to {args.output_path}")
            except Exception as e:
                logging.error(f"Failed to write output file: {e}")
                sys.exit(15)
        else:
            logging.error("No AI metadata generated")
            sys.exit(16)

        sys.exit(0)

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => S3")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to S3")
        
    matched_file = args.source_path
    catalog_path = args.catalog_path

    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)
    upload_path = args.upload_path

    meta_file = args.metadata_file
    logging.info("Preparing metadata to be uploaded ...")
    metadata_obj = prepare_metadata_to_upload(backlink_url, meta_file)
    parsed = truncate_metadata_to_fit(metadata_obj) if metadata_obj else None

    if not args.bucket_name or not args.upload_path:
        logging.error("Bucket name or upload path is missing or empty.")
        sys.exit(1)
    
    upload_path = os.path.join(remove_file_name_from_path(args.upload_path), extract_file_name(args.source_path))
    
    response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, upload_path, parsed)
    if response["status"] != 200:
        print(f"Failed to upload file parts: {response['detail']}")
        sys.exit(1)
    if args.repo_guid and args.catalog_path:
        logging.info("Updating catalog with new S3 file location...")
        update_catalog(args.repo_guid, clean_catalog_path, upload_path, args.bucket_name)

    s3_uri = f"s3://{args.bucket_name}/{upload_path.lstrip('/').replace(chr(92), '/')}"

    # ==============================================================================
    # AWS Rekognition Video Analysis – Normal Flow (with JobId persistence)
    # ==============================================================================
    if args.enrich_prefix == "vid" and cloud_config_data.get("export_ai_metadata") == "true":
        ai_config = advanced_ai_settings or {}
        features = ai_config.get("video_rekognition_features", [])
        
        if not features:
            logging.info("No video Rekognition features configured. Skipping.")
        else:
            clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1] if args.catalog_path else "unknown"
            
            # Derive tracking directory (physical path)
            meta_path, _ = get_store_paths()
            if not meta_path:
                logging.error("Metadata store path not configured.")
                sys.exit(5)
                
            if "/./" in meta_path:
                physical_root, logical_suffix = meta_path.split("/./", 1)
            else:
                physical_root, logical_suffix = meta_path, ""
                
            tracking_dir = os.path.join(
                physical_root.rstrip("/"),
                logical_suffix,
                str(args.repo_guid),
                clean_catalog_path,
                "aws_rekognition"
            )

            # Generate new analysis ID
            analysis_id = str(uuid.uuid4())
            tracker_path = os.path.join(tracking_dir, f"{analysis_id}_status.json")
            asset_file = os.path.basename(args.upload_path) if args.upload_path else "unknown"

            logging.info(f"Starting video analysis (ID: {analysis_id}) for features: {features}")

            # Start jobs
            region = cloud_config_data.get("rekognition_region", "us-east-1")
            min_conf = ai_config.get("video_rekognition_min_confidence", 80)
            job_ids = start_video_rekognition_job(
                s3_uri,
                cloud_config_data['access_key_id'],
                cloud_config_data['secret_access_key'],
                features,
                region,
                min_confidence=min_conf
            )

            if not job_ids:
                logging.error("No Rekognition jobs started.")
                print(f"Metadata extraction failed for asset: {analysis_id}")
                sys.exit(7)

            # Initialize tracker with job IDs (not yet completed)
            feature_status = {}
            for feat in features:
                job_id = job_ids.get(feat, None)  # May be None if start failed (e.g., IAM deny)
                feature_status[feat] = {
                    "job_id": job_id,
                    "completed": False,
                    "raw_file": None
                }

            tracker = {
                "analysis_id": analysis_id,
                "asset_file": asset_file,
                "asset_s3_uri": s3_uri,
                "features_requested": features,
                "feature_status": feature_status,
                "all_done": False
            }

            # Create directory and write tracker IMMEDIATELY (to persist JobIds)
            os.makedirs(tracking_dir, exist_ok=True)
            with open(tracker_path, 'w') as f:
                json.dump(tracker, f, indent=2)

            logging.info(f"Tracker saved with JobIds: {tracker_path}")

            # Poll for up to 15 minutes (900 seconds)
            start_time = time.time()
            max_wait = 900
            poll_interval = 30
            completed_results = {}

            while (time.time() - start_time) < max_wait:
                pending = False
                for feat in features:
                    status = tracker["feature_status"][feat]
                    
                    if status["completed"]:
                        continue
                    
                    if status["raw_file"]:
                        raw_path = os.path.join(tracking_dir, status["raw_file"])
                        if os.path.exists(raw_path):
                            status["completed"] = True
                            logging.info(f"Feature {feat} already extracted, skipping.")
                            continue
                    
                    if not status["job_id"]:
                        continue

                    pending = True
                    result = get_rekognition_job_result(
                        status["job_id"], feat,
                        cloud_config_data['access_key_id'],
                        cloud_config_data['secret_access_key'],
                        region
                    )
                    if result:
                        job_status = result.get('JobStatus')
                        if job_status == 'SUCCEEDED':
                            completed_results[feat] = result
                            status["completed"] = True
                            raw_filename = f"{analysis_id}_{feat}_raw.json"
                            status["raw_file"] = raw_filename
                            raw_path = os.path.join(tracking_dir, raw_filename)
                            with open(raw_path, 'w') as rf:
                                json.dump(result, rf, indent=2)
                            logging.info(f"Feature {feat} succeeded.")
                        elif job_status in ('FAILED', 'PARTIAL_SUCCESS'):
                            status["completed"] = True
                            reason = result.get('FailureReason', 'Unknown')
                            logging.error(f"Feature {feat} failed: {reason}")
                    else:
                        logging.warning(f"Null result for {feat} (JobId: {status['job_id']})")

                if not pending:
                    break

                time.sleep(poll_interval)

            # Update final completion status
            all_done = all(
                (tracker["feature_status"][f]["completed"]
                 if tracker["feature_status"][f].get("job_id") is not None
                 else True)
                for f in features
            )
            tracker["all_done"] = all_done

            with open(tracker_path, 'w') as f:
                json.dump(tracker, f, indent=2)

            succeeded_features = [f for f in features if tracker["feature_status"][f].get("raw_file") and 
                                  os.path.exists(os.path.join(tracking_dir, tracker["feature_status"][f]["raw_file"]))]
            
            if succeeded_features:
                combined_metadata = {"rekognition_video_analysis": {}}
                for feat in succeeded_features:
                    raw_file = tracker["feature_status"][feat]["raw_file"]
                    raw_path = os.path.join(tracking_dir, raw_file)
                    with open(raw_path, 'r') as f:
                        combined_metadata["rekognition_video_analysis"][feat] = json.load(f)

                raw_return, norm_return = store_metadata_file(
                    cloud_config_data, args.repo_guid, clean_catalog_path, combined_metadata
                )

                if raw_return:
                    send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_return, norm_return)

                if all_done and norm_return:
                    try:
                        full_norm_path = os.path.join(physical_root, norm_return)
                        enriched, success = transform_normlized_to_enriched(full_norm_path, args.enrich_prefix)
                        if success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                            logging.info("AI enriched metadata sent successfully.")
                        else:
                            logging.warning("AI enrichment send skipped or failed.")
                    except Exception:
                        logging.exception("Enrichment failed.")

            if not all_done:
                print(f"Metadata extraction failed for asset: {analysis_id}")
                sys.exit(7)

    logging.info("All parts uploaded successfully to S3")
    sys.exit(0)