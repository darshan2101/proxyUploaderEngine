import magic
import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
from configparser import ConfigParser
import random
import time
from action_functions import flatten_dict
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "send_extracted_metadata"]
CHUNK_SIZE = 5 * 1024 * 1024
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
RUBICX_BASE_URL = "https://api.rubicx.ai"

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

def send_extracted_metadata(repo_guid, file_path, metadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": "rubicx",
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": flatten_dict(metadata)
        }]
    }
    for _ in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            logging.warning(f"Metadata send error: {e}")
    return False

# --- Rubicx-specific functions ---
def upload_asset(api_key, file_path):
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    mime_type = magic.from_file(file_path, mime=True)
    if not mime_type:
        logging.critical(f"Could not detect MIME type for '{file_name}'")
        return None, 400

    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")

    file_size = os.path.getsize(file_path)
    logging.info(f"File size: {file_size} bytes")

    url = f"{RUBICX_BASE_URL}/api/videos/upload/direct"
    logging.debug(f"Uploading to URL: {url}")
    headers = {"X-API-Key": api_key}
    logging.debug(f"headers: {headers}")
    data = {"api_key": api_key}  # Required by current Rubicx backend
    logging.debug(f"data: {data}")
    for attempt in range(3):
        try:
            with open(file_path, 'rb') as f:
                files = {"file": (file_name, f, mime_type)}
                logging.info(f"Uploading '{file_name}' to Rubicx... (Attempt {attempt + 1})")
                response = requests.post(
                    url,
                    headers=headers,
                    data=data,
                    files=files
                )

            if 400 <= response.status_code < 500:
                error_msg = f"Client error: {response.status_code} {response.text}"
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            if response.status_code in (200, 201):
                try:
                    result = response.json()
                    video_id = result.get("id")
                    if video_id:
                        logging.info(f"Upload successful. Video ID: {video_id}")
                        return video_id
                    else:
                        logging.error("Upload succeeded but no 'id' in response")
                        raise RuntimeError("Missing video ID in upload response")
                except ValueError:
                    logging.error(f"Invalid JSON response: {response.text}")
                    raise RuntimeError("Invalid response from Rubicx")

            logging.warning(f"Server error {response.status_code}: {response.text}. Retrying...")

        except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError) as e:
            logging.warning(f"Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            if "4xx" in str(e).lower():
                logging.error(f"Client error during upload: {e}")
                raise
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")

        if attempt < 2:
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        else:
            logging.critical(f"Upload failed after 3 attempts: {file_name}")
            raise RuntimeError("Upload failed after retries.")

    raise RuntimeError("Upload failed.")

def start_analysis(api_key, video_id):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/analyze"
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}
    response = make_request_with_retries("POST", url, headers=headers, params=params)
    if not (response and response.status_code == 200):
        raise RuntimeError(f"Analysis start failed: {response.status_code if response else 'No response'}")

def poll_analysis_progress(api_key, video_id, max_wait=300, interval=5):
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key}
    start_time = time.time()
    deadline = start_time + max_wait

    # === Step: Wait for analysis progress to complete ===
    progress_url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/analysis/progress"
    logging.info("Step 3: Polling analysis progress...")
    while time.time() < deadline:
        try:
            resp = make_request_with_retries("GET", progress_url, headers=headers, params=params)
            if resp and resp.status_code == 200:
                data = resp.json()
                status = data.get("status")
                progress_pct = data.get("progress", 0)
                logging.info(f"Analysis progress: {status} ({progress_pct}%)")
                if status == "completed":
                    logging.info("Step 3: Analysis progress completed.")
                    break
        except Exception as e:
            logging.debug(f"Progress polling error: {e}")
        time.sleep(interval)
    else:
        logging.warning("Step 3: Analysis progress did not complete within 5 minutes.")
        raise TimeoutError("Analysis progress timeout (5 min)")

    # === Step: Wait for batch status to fully complete ===
    batch_status_url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/batch-status"
    logging.info("Step 4: Polling batch status...")
    while time.time() < deadline:
        try:
            resp = make_request_with_retries("GET", batch_status_url, headers=headers, params=params)
            if resp and resp.status_code == 200:
                batch_data = resp.json()
                progress_info = batch_data.get("progress_info", {})
                
                total = progress_info.get("total_batches", 0)
                completed = progress_info.get("completed_batches", 0)
                pct = progress_info.get("completion_percentage", 0.0)
                overall_status = progress_info.get("overall_status", "")
                
                logging.info(
                    f"Batch status: {completed}/{total} batches, "
                    f"{pct}% complete, overall: '{overall_status}'"
                )
                
                if (
                    total > 0 and
                    completed == total and
                    abs(pct - 100.0) < 0.01 and
                    overall_status == "completed"
                ):
                    logging.info("Step 4: Batch processing fully completed.")
                    return True
        except Exception as e:
            logging.debug(f"Batch status polling error: {e}")
        time.sleep(interval)
    else:
        logging.warning("Step 4: Batch status did not reach full completion within 5 minutes.")
        raise TimeoutError("Batch status timeout (5 min)")

def fetch_metadata(api_key, video_id):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/metadata"
    headers = {"X-API-Key": api_key}
    params = {"api_key": api_key} 
    response = make_request_with_retries("GET", url, headers=headers, params=params)
    if response and response.status_code == 200:
        return response.json()
    return {}

def fetch_batch_results(api_key, video_id):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/batch-results"
    headers = {"X-API-Key": api_key}
    logging.debug(f"headers: {headers}")
    data = {"api_key": api_key}  # Required by current Rubicx backend
    logging.debug(f"data: {data}")
    response = make_request_with_retries("POST", url, headers=headers, data=data)
    response_data = response.json() if response else None
    if response and response.status_code == 200:
        if response_data.get("success") == True and response_data.get("status") == "completed":
            return True
        else:
            logging.warning("Batch results indicate failure or incomplete status.")
            return False
    logging.warning("Batch results not available or failed.")
    return False

def fetch_metadata(api_key, video_id):
    url = f"{RUBICX_BASE_URL}/api/videos/{video_id}/metadata"
    payload = {"api_key": api_key}
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    response = make_request_with_retries("POST", url, json=payload, headers=headers)
    if response and response.status_code == 200:
        return response.json()
    logging.error(f"Metadata POST failed: {response.status_code if response else 'No response'}")
    return {}

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

    # Load cloud config (to get API key)
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

    # Handle send_extracted_metadata mode
    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required for send_extracted_metadata mode.")
            sys.exit(1)
        batch_completed = fetch_batch_results(api_key, args.asset_id)
        if not batch_completed:
            logging.error("Batch processing not completed. Cannot fetch metadata.")
            print(f"Metadata extraction failed for asset: {args.asset_id}")
            sys.exit(7)
        metadata = fetch_metadata(api_key, args.asset_id)
        clean_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if send_extracted_metadata(args.repo_guid, clean_path, metadata):
            logging.info("Extracted metadata sent successfully.")
            sys.exit(0)
        else:
            logging.error("Failed to send extracted metadata.")
            sys.exit(1)

    # Main upload + analyze mode
    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    # Size limit check
    if args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logging.warning(f"Invalid size limit: {args.size_limit}")

    catalog_path = args.catalog_path or matched_file
    clean_catalog_path = catalog_path.replace("\\", "/").split("/1/", 1)[-1]

    if args.dry_run:
        logging.info("[DRY RUN] Upload and analysis skipped.")
        logging.info(f"[DRY RUN] File: {matched_file}")
        sys.exit(0)

    try:
        # Upload
        try:
            video_id = upload_asset(api_key, matched_file)
            logging.info(f"Upload successful. Video ID: {video_id}")
        except Exception as e:
            logging.critical(f"Upload failed: {e}")
            sys.exit(1)
        logging.info(f"Upload successful. Video ID: {video_id}")

        # Analyze
        start_analysis(api_key, video_id)
        logging.info("Analysis started.")

        # Poll until complete
        try:
            poll_analysis_progress(api_key, video_id, max_wait=300)
        except TimeoutError as e:
            logging.error(f"Analysis or batch timeout: {e}. Skipping metadata.")
            sys.exit(7)

        # Fetch metadata only if polling succeeded
        batch_results = fetch_batch_results(api_key, video_id)
        if not batch_results:
            logging.error("Batch results indicate failure or incomplete status. Skipping metadata.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(7)
        metadata = fetch_metadata(api_key, video_id)

        # Send to local controller
        if send_extracted_metadata(args.repo_guid, clean_catalog_path, metadata):
            logging.info("Rubicx metadata sent successfully.")
            sys.exit(0)
        else:
            logging.error("Failed to send metadata to controller.")
            print(f"Metadata extraction failed for asset: {video_id}")
            sys.exit(7)

    except Exception as e:
        logging.critical(f"Rubicx processing failed: {e}")
        sys.exit(1)