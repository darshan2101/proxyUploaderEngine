import magic
import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
import xml.etree.ElementTree as ET
from configparser import ConfigParser
import random
import time
import subprocess
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "send_extracted_metadata"]
CHUNK_SIZE = 5 * 1024 * 1024
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
RUBICX_BASE_URL = "https://api.rubicx.ai"
NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/rubicx_metadata_normalizer.py"

# Exit codes
EXIT_SUCCESS = 0
EXIT_GENERAL_ERROR = 1
EXIT_CONFIG_ERROR = 5
EXIT_TIMEOUT_RETRYABLE = 7  # For timeouts that can be retried via send_extracted_metadata
EXIT_ANALYSIS_FAILED = 8     # For permanent analysis failures (cannot be retried)

SDNA_EVENT_MAP = {
    "rubicksx_scene_analysis" : "highlights",
    "rubicksx_summary" : "summary",
    "rubicksx_subjects" : "subjetcs",
    "rubicksx_objects" : "objects",
    "rubicksx_emotions" : "emotions",
}

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

logger = logging.getLogger()

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def fail(msg, code=EXIT_TIMEOUT_RETRYABLE):
    logger.error(msg)
    if code == 7:
        print(f"Metadata extraction failed for asset: {args.asset_id}")
    sys.exit(code)

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")
    
def parse_estimated_time(eta_str):
    if not eta_str or not isinstance(eta_str, str):
        return None
    total_seconds = 0
    parts = eta_str.strip().split()
    for part in parts:
        part = part.lower()
        if part.endswith('h'):
            try:
                hours = float(part[:-1])
                total_seconds += hours * 3600
            except ValueError:
                continue
        elif part.endswith('m'):
            try:
                minutes = float(part[:-1])
                total_seconds += minutes * 60
            except ValueError:
                continue
        elif part.endswith('s'):
            try:
                seconds = float(part[:-1])
                total_seconds += seconds
            except ValueError:
                continue
    return total_seconds if total_seconds > 0 else None

def get_link_address_and_port():
    logging.debug(f"Reading server configuration from: {SERVERS_CONF_PATH}")
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
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
        sys.exit(EXIT_CONFIG_ERROR)
    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json"
        if not os.path.exists(config_path):
            logger.error(f"AI config file not found: {config_path}")
            exit(EXIT_CONFIG_ERROR)
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            metadata_store_path = config_data.get("ai_export_shared_drive_path", "")
            proxy_store_path = config_data.get("ai_proxy_shared_drive_path", "")
            logging.info(f"Metadata Store Path: {metadata_store_path}, Proxy Store Path: {proxy_store_path}")
        
    except Exception as e:
        logging.error(f"Error reading Metadata Store or Proxy Store settings: {e}")
        sys.exit(EXIT_CONFIG_ERROR)

    if not metadata_store_path or not proxy_store_path:
        logging.info("Store settings not found.")
        sys.exit(EXIT_CONFIG_ERROR)

    return metadata_store_path, proxy_store_path

def get_rubicx_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path):

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
        
        if log_path:
            logging.info(f"Admin dropbox path from LogPath: {log_path}")
            return log_path
        else:
            logging.warning("LogPath not found in DNAClientServices config")
            return None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def get_advanced_ai_config(config_name):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        logging.debug("Admin dropbox path not available, skipping advanced AI config check")
        return None
    config_file_path = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    logging.debug(f"Checking for advanced AI config at: {config_file_path}")
    
    if not os.path.exists(config_file_path):
        logging.debug(f"Advanced AI config file not found: {config_file_path}")
        return None
    
    try:
        with open(config_file_path, 'r') as f:
            config_data = json.load(f)
        
        # Validate that config_data is a dictionary
        if not isinstance(config_data, dict):
            logging.error(f"Invalid JSON format in {config_file_path}: expected object/dict, got {type(config_data).__name__}")
            return None
        
        nth_frame_enabled = config_data.get("nth_frame_extraction_enabled", False)
        nth_frame = config_data.get("nth_frame")
        
        # Validate nth_frame is a positive integer if provided
        if nth_frame is not None:
            try:
                nth_frame = int(nth_frame)
                if nth_frame <= 0:
                    logging.error(f"Invalid nth_frame value in {config_file_path}: must be positive integer, got {nth_frame}")
                    return None
            except (ValueError, TypeError):
                logging.error(f"Invalid nth_frame value in {config_file_path}: must be integer, got {nth_frame}")
                return None
        
        if nth_frame_enabled and nth_frame:
            logging.info(f"Advanced AI config found: nth_frame_extraction_enabled=True, nth_frame={nth_frame}")
            return {
                "nth_frame_extraction_enabled": nth_frame_enabled,
                "nth_frame": nth_frame
            }
        else:
            logging.debug(f"Advanced AI config exists but nth_frame_extraction_enabled={nth_frame_enabled}, nth_frame={nth_frame}")
            return None
    
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in advanced AI config file {config_file_path}: {e}")
        return None
    except IOError as e:
        logging.error(f"Error reading advanced AI config file {config_file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error processing advanced AI config file {config_file_path}: {e}")
        return None

def get_cloud_config_path():
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            path = plistlib.load(fp)["CloudConfigFolder"] + "/cloud_targets.conf"
    logging.info(f"Using cloud config path: {path}")
    return path

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

def get_retry_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None
    for attempt in range(3):
        try:
            response = session.request(method, url, **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise
    if last_exception:
        raise last_exception
    return None

def parse_metadata_file(properties_file):
    metadata = { "fabric URL": backlink_url }

    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Metadata file not found: {properties_file}")
        return metadata

    try:
        file_ext = properties_file.lower()

        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata.update(json.load(f))

        elif file_ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        metadata[key] = value
            else:
                logging.warning("No <meta-data> section found in XML.")

        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value

    except Exception as e:
        logging.error(f"Error parsing metadata file: {e}")
    
    return metadata

def update_catalog(repo_guid, file_path, media_id, max_attempts=3):
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
        "providerName": cloud_config_data.get("provider", "rubicx"),
        "providerData": {
            "assetId": media_id
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

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    meta_path, proxy_path = get_store_paths()
    if not meta_path:
        logging.error("Metadata Store settings not found.")
        return None, None

    provider = config.get("provider", "rubicx")
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
            if not get_rubicx_normalized_metadata(raw_json, norm_json):
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

def send_extracted_metadata(config, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
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
            if attempt == 2:
                logging.critical(f"Failed to send metadata after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return False 

# --- Rubicx-specific functions with retry logic ---
def upload_asset(api_key, file_path, max_attempts=3):
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part

    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")

    file_size = os.path.getsize(file_path)
    logging.info(f"File size: {file_size} bytes")
    mime_type = magic.from_file(file_path, mime=True)
    if not mime_type:
        logging.critical(f"Could not detect MIME type for '{file_name}'")
        return None, 400    

    for attempt in range(max_attempts):
        try:
            url = f"{RUBICX_BASE_URL}/api/videos/upload"
            headers = {
                "X-API-Key": api_key,
                "Content-Type": "application/json"
            }
            params = {
                "api_key": api_key
            }
            json_data = {
                "filename": file_name,
                "content_type": mime_type
            }
            logging.info(f"Requesting presigned URL for '{file_name}' (attempt {attempt + 1}/{max_attempts})...")
            response = make_request_with_retries("POST", url, headers=headers, params=params, json=json_data, timeout=(10, 30))
            if not (response and response.status_code == 200):
                print(f"Response Text ----------------------> {response}")
                raise RuntimeError(f"Presigned URL request failed: {response.status_code if response else 'No response'} - {response.text if response else ''}")

            result = response.json()
            upload_url = result.get("upload_url")
            video_id = result.get("video_id")
            if not upload_url or not video_id:
                raise RuntimeError(f"Missing upload_url or video_id in response: {result}")

            logging.info(f"Presigned URL obtained. Video ID: {video_id}")

            with open(file_path, 'rb') as f:
                logging.info("Uploading file to presigned S3 URL...")
                upload_resp = requests.put(
                    upload_url,
                    data=f,
                    headers={"Content-Type": mime_type},
                    timeout=(10, 3600)
                )
                if upload_resp.status_code not in (200, 204):
                    raise RuntimeError(f"S3 upload failed: {upload_resp.status_code} - {upload_resp.text}")

            logging.info("File uploaded to S3 successfully.")
            return video_id

        except Exception as e:
            logging.warning(f"Upload attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                time.sleep(delay)
            else:
                raise RuntimeError(f"Upload failed after {max_attempts} attempts: {e}")

def start_analysis(api_key, video_id, max_attempts=3):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/analyze"
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, params=params, timeout=(10, 30))
            if response and response.status_code == 200:
                logging.info("Analysis started successfully.")
                return
            else:
                raise RuntimeError(f"Analysis start failed: {response.status_code if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Start analysis attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                time.sleep(delay)
            else:
                raise

def start_transcription(api_key, video_id, max_attempts=3):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/transcribe"
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}

    logging.info("Starting audio transcription...")
    
    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, params=params, timeout=(10, 30))
            if response and response.status_code == 200:
                logging.info("Transcription started successfully.")
                return True
            else:
                logging.warning(f"Transcription start returned status: {response.status_code if response else 'No response'}")
                if attempt < max_attempts - 1:
                    delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                    time.sleep(delay)
                else:
                    logging.error("Failed to start transcription after all attempts")
                    return False
        except Exception as e:
            logging.warning(f"Start transcription attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                time.sleep(delay)
            else:
                logging.error(f"Transcription start failed after {max_attempts} attempts")
                return False
    return False

def fetch_transcription(api_key, video_id, include_segments=True, max_attempts=3):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/transcription"
    headers = {"X-API-Key": api_key}
    params = {
        "api_key": api_key,
        "include_segments": str(include_segments).lower()
    }

    logging.info("Fetching transcription results...")
    
    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("GET", url, headers=headers, params=params, timeout=(10, 30))
            if response and response.status_code == 200:
                try:
                    data = response.json()
                    status = data.get("status")
                    
                    if status == "completed":
                        logging.info("Transcription completed successfully.")
                        return data
                    elif status == "processing":
                        logging.info("Transcription still processing...")
                        return None
                    elif status == "failed":
                        logging.error(f"Transcription failed: {data.get('error')}")
                        return None
                    else:
                        logging.warning(f"Unknown transcription status: {status}")
                        return None
                        
                except ValueError as e:
                    logging.error(f"Failed to parse transcription response: {e}")
                    return None
            elif response and response.status_code == 404:
                logging.info("Transcription not found or not started yet.")
                return None
            else:
                logging.warning(f"Transcription fetch returned status: {response.status_code if response else 'No response'}")
                if attempt < max_attempts - 1:
                    delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                    time.sleep(delay)
                else:
                    return None
        except Exception as e:
            logging.warning(f"Fetch transcription attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                time.sleep(delay)
            else:
                logging.error(f"Transcription fetch failed after {max_attempts} attempts")
                return None
    return None

def poll_analysis_progress(api_key, video_id, max_wait=1200, interval=5, max_attempts=3):
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}
    progress_url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/analysis"

    logging.info("Step 3: Polling analysis progress (20-min timeout)...")

    for outer_attempt in range(max_attempts):
        deadline = time.time() + max_wait
        try:
            while time.time() < deadline:
                resp = make_request_with_retries("GET", progress_url, headers=headers, params=params, timeout=(10, 30))

                if not resp or resp.status_code != 200:
                    logging.warning("Invalid progress response, retrying...")
                    time.sleep(interval)
                    continue

                data = resp.json()
                status = data.get("status")
                progress_pct = data.get("progress", 0)
                error_msg = data.get("error_message")

                logging.info(f"Analysis progress: {status} ({progress_pct}%)")

                if error_msg:
                    logging.error(f"Analysis error reported: {error_msg}")

                if status == "completed":
                    logging.info("Analysis completed successfully.")
                    return True

                if status == "failed":
                    logging.critical(f"Analysis permanently failed with response: {json.dumps(data, indent=2)}")

                    error_details = error_msg or "Unknown error"
                    current_stage = data.get("current_stage", "unknown")

                    logging.error(f"Analysis failed at stage '{current_stage}': {error_details}")
                    logging.error("This is a permanent failure and cannot be retried via send_extracted_metadata mode.")

                    print(f"Analysis failed permanently for asset: {video_id}")
                    print(f"Error: {error_details}")
                    print(f"Stage: {current_stage}")

                    # Exit with code 8 to report failure of analysis
                    sys.exit(EXIT_ANALYSIS_FAILED)
                time.sleep(interval)

            # Timeout reached
            logging.error(f"Analysis progress timed out after {max_wait} seconds.")
            logging.info("This may be retryable via send_extracted_metadata mode.")
            return False

        except Exception as e:
            logging.warning(f"Polling attempt {outer_attempt + 1} failed: {e}")
            if outer_attempt < max_attempts - 1:
                backoff = [5, 15, 30][outer_attempt] + random.uniform(0, 2)
                time.sleep(backoff)
            else:
                logging.error("Polling failed after all retry attempts.")
                return False

def fetch_batch_results(api_key, video_id, max_attempts=7, base_interval=60, max_allowed_eta_seconds=3600):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/batch-results"
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, params= params, timeout=(10, 30))
            if response and response.status_code == 200:
                try:
                    result = response.json()
                except ValueError:
                    logging.error("Batch results: Invalid JSON response")
                    result = {}

                success = result.get("success") is True
                status = result.get("status")
                if success and status == "completed":
                    logging.info("Batch results: Processing completed successfully.")
                    return True

                # Check ETA for early exit
                eta_str = result.get("estimated_time_remaining")
                eta_seconds = parse_estimated_time(eta_str) if eta_str else None
                if eta_seconds and eta_seconds > max_allowed_eta_seconds:
                    logging.warning(f"Batch ETA too high ({eta_str}), aborting early.")
                    return False

                # Determine wait interval
                if eta_seconds:
                    wait_time = min(eta_seconds, base_interval * (attempt + 1))
                else:
                    wait_time = base_interval * (attempt + 1)

                logging.info(f"Batch not ready. Status: {status}. ETA: {eta_str}. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.warning(f"Batch results request failed: {response.status_code if response else 'No response'}")
                time.sleep(base_interval * (attempt + 1))

        except Exception as e:
            logging.warning(f"Batch results attempt {attempt + 1} failed: {e}")
            time.sleep(base_interval * (attempt + 1))

    logging.error("Batch results did not complete within retry window.")
    return False

def fetch_metadata(api_key, video_id, n_th_frame=None, max_attempts=3, page_limit=1000):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/metadata"
    headers = {"X-API-Key": api_key}

    def req(offset):
        params = {
            "api_key": api_key,
            "include_frames": "true",
            "limit": page_limit,
            "offset": offset
        }
        for i in range(max_attempts):
            try:
                r = make_request_with_retries("GET", url, headers=headers, params=params, timeout=(10,30))
                if r and r.status_code == 200: return r.json()
                raise RuntimeError(f"HTTP {r.status_code if r else 'None'}")
            except Exception:
                if i == max_attempts - 1: raise
                time.sleep([1,3,10][min(i,2)] + random.random())

    # first page
    first = req(0)
    meta = {k:v for k,v in first.items() if k!="frames"}
    total = first.get("total_frames") or len(first.get("frames") or [])
    frames = first.get("frames") or []

    # sampling function
    filt = (lambda f: f.get("frame_number",0) % n_th_frame == 0) if n_th_frame else (lambda f: True)

    sampled = [f for f in frames if filt(f)]

    if len(frames) < page_limit:  # no pagination needed
        meta["frames"] = sampled
        meta["total_frames"] = total
        return meta

    # paginate
    offset = page_limit
    while offset < total:
        chunk = req(offset).get("frames") or []
        sampled.extend(f for f in chunk if filt(f))
        if len(chunk) < page_limit: break
        offset += page_limit

    meta["frames"] = sampled
    meta["total_frames"] = total
    return meta

def add_custom_metadata(api_key, asset_id, metadata, max_attempts=3):
    url = f"{RUBICX_BASE_URL}/v2/metadata/upload"
    headers = {
        "accept": "application/json",
        "X-API-Key": api_key,
        "Content-Type": "application/json"
    }
    params = {
        "video_id": asset_id
    }

    for attempt in range(max_attempts):
        try:
            resp = make_request_with_retries("POST", url, headers=headers, params=params, json=metadata, timeout=(10, 30))
            status = resp.status_code if resp else None
            detail = None
            if resp is not None:
                try:
                    resp_json = resp.json()
                    detail = resp_json
                except ValueError:
                    detail = resp.text

            if status in (200, 201):
                logging.info("Custom metadata uploaded successfully.")
                return {"status_code": status, "detail": detail, "json": resp_json}
            else:
                raise RuntimeError(f"Custom metadata upload failed: {status} - {detail}")
        except Exception as e:
            logging.warning(f"Custom metadata upload attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                delay = [1, 3, 10][attempt] + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return {"status_code": 500, "detail": str(e)}
    return {"status_code": 500, "detail": "Max retry attempts exhausted"}
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

def send_ai_enriched_metadata(config, repo_guid, file_path, enrichedMetadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "twelvelabs"),
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

# --- Main ---
if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 1.5))
    parser = argparse.ArgumentParser(description="Rubicx Video Uploader")
    parser.add_argument("--mode", required=True, choices=VALID_MODES, help="Operation mode")
    parser.add_argument("--source-path", help="Path to the video file")
    parser.add_argument("--size-limit", help="Max file size in MB for 'original' mode")
    parser.add_argument("--config-name", required=True, help="Cloud config name")
    parser.add_argument("--catalog-path", help="Catalog path of the asset")
    parser.add_argument("--repo-guid", help="Repository GUID")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded to frameIO")
    parser.add_argument("--asset-id", help="Asset ID for send_extracted_metadata mode")
    parser.add_argument("--metadata-file", help="Path to metadata XML/JSON file")
    parser.add_argument("--job-guid", help="Job GUID for backlink URL")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no upload/analysis)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--enrich-prefix", help="Prefix for enriched metadata filetypes")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Ignored")
    parser.add_argument("--controller-address", help="Controller IP:Port")
    parser.add_argument("--export-ai-metadata", help="Ignored flag (always true for Rubicx)")

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(EXIT_GENERAL_ERROR)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(EXIT_CONFIG_ERROR)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config '{args.config_name}' not found.")
        sys.exit(EXIT_CONFIG_ERROR)

    cloud_config_data = cloud_config[args.config_name]
    api_key = cloud_config_data.get("api_key")
    if not api_key:
        logging.error("API key not found in config.")
        sys.exit(EXIT_CONFIG_ERROR)
        
    filetype_prefix = args.enrich_prefix if args.enrich_prefix else "gen"

    # Check for advanced AI export configuration
    advanced_ai_config = get_advanced_ai_config(args.config_name)
    nth_frame_param = None
    if advanced_ai_config and advanced_ai_config.get("nth_frame_extraction_enabled"):
        nth_frame_param = advanced_ai_config.get("nth_frame")
        logging.info(f"Using nth_frame extraction with value: {nth_frame_param}")
    else:
        logging.debug("Using full frame extraction (no nth_frame filtering)")

    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required for send_extracted_metadata mode.")
            sys.exit(EXIT_GENERAL_ERROR)
        batch_completed = fetch_batch_results(api_key, args.asset_id)
        if not batch_completed:
            logging.error("Batch processing not completed. Cannot fetch metadata.")
            print(f"Metadata extraction failed for asset: {args.asset_id}")
            sys.exit(EXIT_TIMEOUT_RETRYABLE)
        
        # Fetch main metadata
        ai_metadata = fetch_metadata(api_key, args.asset_id, n_th_frame=nth_frame_param)
        if not ai_metadata:
            fail("Could Not get AI metadata", EXIT_TIMEOUT_RETRYABLE)

        # Fetch transcription data and add it to metadata
        transcription_data = fetch_transcription(api_key, args.asset_id, include_segments=True)
        if transcription_data:
            logging.info("Transcription data retrieved successfully.")
            ai_metadata["transcription"] = transcription_data
        else:
            logging.warning("Transcription data not available or still processing.")
            ai_metadata["transcription"] = None

        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        raw_path, norm_path = store_metadata_file(cloud_config_data, args.repo_guid, catalog_path_clean, ai_metadata)
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, raw_path, norm_path):
            fail("Failed to send extracted metadata.", EXIT_TIMEOUT_RETRYABLE)
        logger.info("Extracted metadata sent successfully.")

        if norm_path:
            try:
                meta_path, _ = get_store_paths()
                norm_metadata_file_path = os.path.join(meta_path.split("/./")[0] if "/./" in meta_path else meta_path.split("/$/")[0] if "/$/" in meta_path else meta_path, norm_path)
                enriched, success = transform_normlized_to_enriched(norm_metadata_file_path, filetype_prefix)
                if success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, enriched):
                    logger.info("AI enriched metadata sent successfully.")
                else:
                    logger.warning("AI enriched metadata send failed — skipping.")
            except Exception:
                logger.exception("AI enriched metadata transform failed — skipping.")
        else:
            logger.info("Normalized metadata not present — skipping AI enrichment.")

        sys.exit(EXIT_SUCCESS)

    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    if args.mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logging.warning(f"Invalid size limit: {args.size_limit}")

    catalog_path = args.catalog_path or matched_file
    file_name_for_url = extract_file_name(matched_file) if args.mode == "original" else extract_file_name(catalog_path)
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        ip, _ = get_link_address_and_port()
        client_ip = ip

    backlink_url = f"{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated backlink URL: {backlink_url}")

    clean_catalog_path = catalog_path.replace("\\", "/").split("/1/", 1)[-1]

    if args.dry_run:
        logging.info("[DRY RUN] Upload and analysis skipped.")
        logging.info(f"[DRY RUN] File: {matched_file}")
        sys.exit(EXIT_SUCCESS)

    try:
        try:
            video_id = upload_asset(api_key, matched_file)
            logging.info(f"Upload successful. Video ID: {video_id}")
            update_catalog(args.repo_guid, clean_catalog_path, video_id)
        except Exception as e:
            logging.critical(f"Upload failed: {e}")
            sys.exit(EXIT_GENERAL_ERROR)

        start_analysis(api_key, video_id)
        logging.info("Analysis started.")
        
        # Start transcription in parallel with analysis
        transcription_started = start_transcription(api_key, video_id)
        if transcription_started:
            logging.info("Transcription initiated.")
        else:
            logging.warning("Transcription could not be started, continuing without it.")
        
        try:
            meta_response = add_custom_metadata(
                api_key,
                video_id,
                parse_metadata_file(args.metadata_file)
            )
            if meta_response.get("status_code") in (200, 201):
                logging.info("Metadata uploaded successfully.")
            else:
                logging.error(f"Metadata upload failed: {meta_response.get('detail')}")
                print("File uploaded, but metadata failed.")
        except Exception as e:
            logging.warning(f"Metadata upload encountered error: {e}")        

        analysis_success = poll_analysis_progress(api_key, video_id)
        if not analysis_success:
            # poll_analysis_progress already exits with EXIT_ANALYSIS_FAILED if status is "failed"
            # If we reach here, it's a timeout scenario
            logging.error("Analysis timed out during processing.")
            logging.info("This can be retried using send_extracted_metadata mode.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(EXIT_TIMEOUT_RETRYABLE)

        if not fetch_batch_results(api_key, video_id):
            logging.error("Batch processing did not complete successfully.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(EXIT_TIMEOUT_RETRYABLE)
        
        # Fetch main metadata
        ai_metadata = fetch_metadata(api_key, video_id, n_th_frame=nth_frame_param)
        if not ai_metadata:
            fail("Could Not get AI metadata", EXIT_TIMEOUT_RETRYABLE)

        # Fetch transcription data and add it to combined metadata
        transcription_data = fetch_transcription(api_key, video_id, include_segments=True)
        if transcription_data:
            logging.info("Transcription data retrieved successfully.")
            ai_metadata["transcription"] = transcription_data
        else:
            logging.warning("Transcription data not available or still processing.")
            ai_metadata["transcription"] = None

        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        raw_path, norm_path = store_metadata_file(cloud_config_data, args.repo_guid, catalog_path_clean, ai_metadata)
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, raw_path, norm_path):
            fail("Failed to send extracted metadata.", EXIT_TIMEOUT_RETRYABLE)
        logger.info("Extracted metadata sent successfully.")

        if norm_path:
            try:
                meta_path, _ = get_store_paths()
                norm_metadata_file_path = os.path.join(meta_path.split("/./")[0] if "/./" in meta_path else meta_path.split("/$/")[0] if "/$/" in meta_path else meta_path, norm_path)
                enriched, success = transform_normlized_to_enriched(norm_metadata_file_path, filetype_prefix)
                if success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, enriched):
                    logger.info("AI enriched metadata sent successfully.")
                else:
                    logger.warning("AI enriched metadata send failed — skipping.")
            except Exception:
                logger.exception("AI enriched metadata transform failed — skipping.")
        else:
            logger.info("Normalized metadata not present — skipping AI enrichment.")

        sys.exit(EXIT_SUCCESS)

    except Exception as e:
        logging.critical(f"Rubicx processing failed: {e}")
        sys.exit(EXIT_GENERAL_ERROR)