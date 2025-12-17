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

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

logger = logging.getLogger()

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

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
        sys.exit(5)
    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json"
        if not os.path.exists(config_path):
            logger.error(f"AI config file not found: {config_path}")
            exit(5)
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

        else:  # assume CSV
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value

    except Exception as e:
        logging.error(f"Failed to parse {properties_file}: {e}")

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
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "rubicx"),
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
            headers = {"X-API-Key": api_key}
            data = {
                "filename": file_name,
                "content_type": mime_type,
                "api_key": api_key
            }
            logging.info(f"Requesting presigned URL for '{file_name}' (attempt {attempt + 1}/{max_attempts})...")
            response = make_request_with_retries("POST", url, headers=headers, data=data, timeout=(10, 30))
            if not (response and response.status_code == 200):
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

def poll_analysis_progress(api_key, video_id, max_wait=1200, interval=5, max_attempts=3):
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}
    progress_url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/analysis/progress"

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
                error_msg = data.get("error")

                logging.info(f"Analysis progress: {status} ({progress_pct}%)")

                if error_msg:
                    logging.error(f"Analysis error: {error_msg}")

                if status == "completed":
                    logging.info("Analysis completed successfully.")
                    return True

                if status == "failed":
                    logging.critical("Analysis permanently failed.")
                    return False

                time.sleep(interval)

            logging.error("Analysis progress timed out.")
            return False

        except Exception as e:
            logging.warning(f"Polling attempt {outer_attempt + 1} failed: {e}")
            if outer_attempt < max_attempts - 1:
                backoff = [5, 15, 30][outer_attempt] + random.uniform(0, 2)
                time.sleep(backoff)
            else:
                return False

def fetch_batch_results(api_key, video_id, max_attempts=7, base_interval=60, max_allowed_eta_seconds=3600):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/batch-results"
    headers = {"X-API-Key": api_key}
    data = {"api_key": api_key}

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, data=data, timeout=(10, 30))
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
                if eta_seconds is not None and eta_seconds > max_allowed_eta_seconds:
                    logging.warning(
                        f"Batch ETA ({eta_str} = {eta_seconds:.0f}s) exceeds {max_allowed_eta_seconds}s limit. Failing fast."
                    )
                    return False

                # Also check progress block
                progress_info = result.get("progress", {})
                eta_from_progress = progress_info.get("estimated_time_remaining")
                if eta_from_progress:
                    eta_sec2 = parse_estimated_time(eta_from_progress)
                    if eta_sec2 is not None and eta_sec2 > max_allowed_eta_seconds:
                        logging.warning(
                            f"Batch ETA from progress ({eta_from_progress} = {eta_sec2:.0f}s) exceeds limit. Failing fast."
                        )
                        return False

                logging.warning(f"Batch not ready: success={success}, status={status}, ETA={eta_str}")
            else:
                logging.warning(f"Batch results HTTP {response.status_code if response else 'None'}")

        except Exception as e:
            logging.warning(f"Batch results attempt {attempt + 1} failed: {e}")

        # Don't sleep after last attempt
        if attempt < max_attempts - 1:
            delay = base_interval * (2 ** attempt)
            logging.info(f"Waiting {delay}s before next batch check (attempt {attempt + 2}/{max_attempts})")
            time.sleep(delay)

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
    url = f"{RUBICX_BASE_URL}/v2/metadata/upload?video_id={asset_id}"
    headers = {
        "accept": "application/json",
        "X-API-Key": api_key,
        "Content-Type": "application/json"
    }

    for attempt in range(max_attempts):
        try:
            resp = make_request_with_retries("POST", url, headers=headers, json=metadata, timeout=(10, 30))
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

# --- Main ---
if __name__ == '__main__':
    time.sleep(random.uniform(0.0, 1.5))
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, send_extracted_metadata")
    parser.add_argument("-c", "--config-name", required=True, help="Name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-id", "--asset-id", help="Asset ID (video_id) for metadata operations")
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Path to metadata file (unused in Rubicx)")
    parser.add_argument("-up", "--upload-path", help="Ignored for Rubicx (no index concept)")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Ignored")
    parser.add_argument("--controller-address", help="Controller IP:Port")
    parser.add_argument("--export-ai-metadata", help="Ignored flag (always true for Rubicx)")

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config '{args.config_name}' not found.")
        sys.exit(1)

    cloud_config_data = cloud_config[args.config_name]
    api_key = cloud_config_data.get("api_key")
    if not api_key:
        logging.error("API key not found in config.")
        sys.exit(1)

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
            sys.exit(1)
        batch_completed = fetch_batch_results(api_key, args.asset_id)
        if not batch_completed:
            logging.error("Batch processing not completed. Cannot fetch metadata.")
            print(f"Metadata extraction failed for asset: {args.asset_id}")
            sys.exit(7)
        ai_metadata = fetch_metadata(api_key, args.asset_id, n_th_frame=nth_frame_param)
        clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if ai_metadata:
            raw_metadata_path, norm_metadata_path = store_metadata_file(cloud_config_data, args.repo_guid, clean_path, ai_metadata)
            if send_extracted_metadata(cloud_config_data, args.repo_guid, clean_path, raw_metadata_path, norm_metadata_path):
                logging.info("Extracted metadata sent successfully.")
                sys.exit(0)
            else:
                logging.error("Failed to send extracted metadata.")
                sys.exit(7)

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
        sys.exit(0)

    try:
        try:
            video_id = upload_asset(api_key, matched_file)
            logging.info(f"Upload successful. Video ID: {video_id}")
            update_catalog(args.repo_guid, clean_catalog_path, video_id)
        except Exception as e:
            logging.critical(f"Upload failed: {e}")
            sys.exit(1)

        start_analysis(api_key, video_id)
        logging.info("Analysis started.")
        
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
            logging.error("Analysis failed permanently during processing.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(7)

        if not fetch_batch_results(api_key, video_id):
            logging.error("Batch processing did not complete successfully.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(7)
        ai_metadata = fetch_metadata(api_key, video_id, n_th_frame=nth_frame_param)

        if ai_metadata:
            raw_metadata_path, norm_metadata_path = store_metadata_file(cloud_config_data,args.repo_guid, clean_path, ai_metadata)
            if send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_metadata_path, norm_metadata_path):
                logging.info("Extracted metadata sent successfully.")
                sys.exit(0)
            else:
                logging.error("Failed to send extracted metadata.")
                sys.exit(7)

    except Exception as e:
        logging.critical(f"Rubicx processing failed: {e}")
        sys.exit(1)