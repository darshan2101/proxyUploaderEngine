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
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 5 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
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

def prepare_metadata_to_upload(repo_guid ,relative_path, file_name, file_size, backlink_url, properties_file = None):    
    metadata = {
        "relativePath": relative_path if relative_path.startswith("/") else "/" + relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url,
        "fabric_size": file_size
    }
    
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

def file_exists_in_index(api_key, index_id, filename, file_size):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos"
    headers = {"x-api-key": api_key}
    params = {"filename": filename, "size": file_size}
    logging.info(f"Searching for existing video: '{filename}' (Size: {file_size} bytes)")
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            page = 1
            total_pages = 1
            video_id = None

            while page <= total_pages:
                params["page"] = page
                response = make_request_with_retries("GET", url, headers=headers, params=params)
                if not response or response.status_code != 200:
                    raise Exception(f"List videos failed: {response.status_code if response else 'No response'}")

                data = response.json()
                results = data.get("data", [])
                for video in results:
                    meta = video.get("system_metadata", {})
                    if meta.get("filename") == filename and meta.get("size") == file_size:
                        video_id = video["_id"]
                        logging.info(f"Matching video found: ID={video_id}")
                        return video_id

                page_info = data.get("page_info", {})
                total_pages = page_info.get("total_page", 1)
                page += 1

            return None  # No match

        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to check video existence after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def delete_video(api_key, index_id, video_id):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos/{video_id}"
    headers = {"x-api-key": api_key}
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("DELETE", url, headers=headers)
            if response and response.status_code in (200, 204):
                logging.info(f"Deleted existing video ID: {video_id}")
                return True
            elif response:
                logging.warning(f"Delete failed: {response.status_code} {response.text}")
            if attempt == 2:
                return False
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to delete video {video_id} after 3 attempts: {e}")
                return False
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return False

def upload_asset(api_key, index_id, file_path):
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    file_size = os.path.getsize(file_path)

    # Check for duplicates (with retry)
    conflict_resolution = cloud_config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    existing_video_id = file_exists_in_index(api_key, index_id, file_name, file_size)
    if existing_video_id:
        if conflict_resolution == "skip":
            logging.info(f"File '{file_name}' exists. Skipping upload.")
            print(f"File '{file_name}' already exists. Skipping upload.")
            sys.exit(0)
        elif conflict_resolution == "overwrite":
            if not delete_video(api_key, index_id, existing_video_id):
                logging.warning("Proceeding with upload despite failed deletion.")
    else:
        logging.info(f"No duplicate found for '{file_name}'.")

    url = "https://api.twelvelabs.io/v1.3/tasks"
    headers = {"x-api-key": api_key}
    payload = {"index_id": index_id}

    for attempt in range(3):
        try:
            # Open file on each retry
            with open(file_path, 'rb') as f:
                files = {"video_file": f}
                logging.info(f"Uploading '{file_name}' to Twelve Labs index {index_id}... (Attempt {attempt + 1})")
                response = requests.post(
                    url,
                    data=payload,
                    files=files,
                    headers=headers
                )

            # If 4xx, don't retry — it's a client error (e.g. bad index_id, auth)
            if 400 <= response.status_code < 500:
                error_msg = f"Client error: {response.status_code} {response.text}"
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            # If 2xx, success
            if response.status_code in (200, 201):
                logging.debug(f"Upload successful: {response.text}")
                return response.json()

            # If 5xx or unexpected, retry
            logging.warning(f"Server error {response.status_code}: {response.text}. Retrying...")

        except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError) as e:
            logging.warning(f"Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            if "4xx" in str(e).lower():
                logging.error(f"Client error during upload: {e}")
                raise  # Don't retry 4xx
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")

        # Apply jittered backoff
        if attempt < 2:
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        else:
            logging.critical(f"Upload failed after 3 attempts: {file_name}")
            raise RuntimeError("Upload failed after retries.")

    # Should not reach here
    raise RuntimeError("Upload failed.")

def poll_task_status(api_key, task_id, max_wait=10800, interval=10):
    url = f"https://api.twelvelabs.io/v1.3/tasks/{task_id}"
    headers = {"x-api-key": api_key}
    logger.debug(f"URL :------------------------------------------->  {url}")
    logging.info(f"Polling task {task_id} status. Max wait: {max_wait}s")

    start_time = time.time()
    attempt_counter = 0

    while (time.time() - start_time) < max_wait:
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if not response:
                raise ConnectionError("No response from server")

            if response.status_code == 404:
                logging.error(f"Task {task_id} not found (404)")
                return False
            elif response.status_code != 200:
                logging.warning(f"Status {response.status_code}: {response.text}")
                if attempt_counter < 2:
                    attempt_counter += 1
                    delay = [1, 3, 10][attempt_counter-1] + random.uniform(0, [1, 1, 5][attempt_counter-1])
                    time.sleep(delay)
                    continue
                else:
                    logging.critical(f"Failed to fetch task status after retries: {response.text}")
                    return False

            data = response.json()
            status = data.get("status")
            hls_status = data.get("hls", {}).get("status")

            logging.info(f"Task {task_id} | Status: {status} | HLS: {hls_status}")

            if status == "ready":
                logging.info(f"Task {task_id} completed successfully.")
                return data  # Success — return full task object

            elif status == "failed":
                reason = data.get("error", "No error message provided")
                logging.error(f"Task {task_id} failed: {reason}")
                return False

            # Reset retry counter on successful poll
            attempt_counter = 0

        except Exception as e:
            logging.debug(f"Exception during polling: {e}")
            if attempt_counter < 2:
                attempt_counter += 1
                delay = [1, 3, 10][attempt_counter-1] + random.uniform(0, [1, 1, 5][attempt_counter-1])
                time.sleep(delay)
                continue
            logging.warning(f"Polling transient error after retries: {e}")

        # Wait before next poll
        time.sleep(interval)

    # Timeout
    logging.critical(f" Polling timed out after {max_wait}s. Task {task_id} did not reach 'ready'.")
    return False

def add_metadata(api_key, index_id, video_id, metadata):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos/{video_id}"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    payload = {"user_metadata": metadata}
    logger.debug(f"URL :------------------------------------------->  {url}")
    logging.debug(f"Metadata payload -------------------------> {payload}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("PUT", url, json=payload, headers=headers)
            if not response:
                continue
            if response.status_code == 204:
                logging.info("Metadata updated successfully (204 No Content).")
                return None
            elif response.status_code == 200:
                logging.info("Metadata updated successfully.")
                return response.json()
            else:
                if attempt == 2:
                    raise RuntimeError(f"Metadata update failed: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to add metadata after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    raise RuntimeError("Metadata update failed after retries.")

def update_catalog(repo_guid, file_path, index_id, video_id, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/provideData"
    # Read NodeAPIKey from client services config
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": "twelvelabs",
        "providerData": {
            "assetId": video_id,
            "indexId": index_id,
            "providerUiLink": f"https://playground.twelvelabs.io/indexes/{index_id}/videos?page=1&task_id={video_id}"
        }
    }
    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info(f"Catalog updated successfully: {response.text}")
                return True
            if response and response.status_code == 404:
                try:
                    resp_json = response.json()
                    if resp_json.get("message", "").lower() == "catalog item not found":
                        wait_time = 60 + (attempt * 30)
                        logging.warning(
                            f"[Attempt {attempt+1}/{max_attempts}] Catalog item not found. "
                            f"Waiting {wait_time} seconds before retrying..."
                        )
                        time.sleep(wait_time)
                        continue  # retry outer loop
                except Exception:
                    logging.warning(f"Failed to parse JSON on 404 response: {response.text}")
                logging.warning(f"Catalog update failed (status 404): {response.text}")
                break
            logging.warning(
                f"Catalog update failed (status {response.status_code if response else 'No response'}): "
                f"{response.text if response else ''}"
            )
            break
        except Exception as e:
            logging.warning(f"Unexpected error in update_catalog attempt {attempt+1}: {e}")
            break
    pass

if __name__ == '__main__':
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target")
    parser.add_argument("-c", "--config-name", required=True, help="Name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Path to metadata file")
    parser.add_argument("-up", "--upload-path", required=True, help="Twelve Labs index ID or upload path")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Treat upload-path as index ID")
    parser.add_argument("--controller-address", help="Controller IP:Port")

    args = parser.parse_args()

    setup_logging(args.log_level)

    mode = args.mode
    if mode not in VALID_MODES:
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

    index_id = args.upload_path if args.resolved_upload_id else cloud_config_data.get('index_id')
    if not index_id:
        logging.error("Index ID not provided and not found in config.")
        sys.exit(1)

    logging.info(f"Starting Twelve Labs upload process in {mode} mode")
    logging.debug(f"Index ID: {index_id}")

    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    # Size limit
    if mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logging.warning(f"Invalid size limit: {args.size_limit}")

    # Backlink URL
    catalog_path = args.catalog_path or matched_file
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        ip, _ = get_link_address_and_port()
        client_ip = ip

    backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated backlink URL: {backlink_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File: {matched_file}")
        logging.info(f"[DRY RUN] Index ID: {index_id}")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata will be applied from: {args.metadata_file}")
        sys.exit(0)

    # Prepare metadata
    try:
        metadata_obj = prepare_metadata_to_upload(
            repo_guid=args.repo_guid,
            relative_path=catalog_url,
            file_name=file_name_for_url,
            file_size=os.path.getsize(matched_file),
            backlink_url=backlink_url,
            properties_file=args.metadata_file
        )
        # Rename forbidden metadata fields by prefixing with 'fabric-'
        forbidden_fields = {
            "duration", "filename", "fps", "height", "model_names", "size", "video_title", "width"
        }
        if isinstance(metadata_obj, dict):
            for key in list(metadata_obj.keys()):
                if key in forbidden_fields:
                    metadata_obj[f"fabric-{key}"] = metadata_obj.pop(key)
        logging.info("Metadata prepared successfully.")
    except Exception as e:
        logging.error(f"Failed to prepare metadata: {e}")
        metadata_obj = {}

    # Step 1: Upload
    try:
        result = upload_asset(api_key, index_id, matched_file)
        task_id = result.get("_id")
        if not task_id:
            logging.error("Upload succeeded but no 'task_id' in response.")
            sys.exit(1)
        logging.info(f"Upload successful. Task ID: {task_id}")

        # Step 2: Apply metadata BEFORE polling (as requested)
        video_id = result.get("video_id")  # may not exist yet
        if not video_id:
            logging.warning("video_id not in upload response. Will extract after ready.")
        else:
            try:
                add_metadata(api_key, index_id, video_id, metadata_obj)
                logging.info(f"Metadata applied to video {video_id}")
            except Exception as e:
                logging.warning(f"Metadata application failed: {e}")

        # Step 3: Poll for indexing completion
        poll_result = poll_task_status(api_key, task_id)
        if not poll_result:
            logging.error("Task polling failed or timed out.")
            sys.exit(1)

        final_video_id = poll_result.get("video_id")
        if not final_video_id:
            logging.error("Task completed but 'video_id' missing.")
            sys.exit(1)

        logging.info(f"Indexing complete. Final Video ID: {final_video_id}")
        update_catalog(args.repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], index_id, final_video_id)
        # Re-apply metadata if not done earlier (e.g., video_id wasn't available)
        if not video_id:
            try:
                add_metadata(api_key, index_id, final_video_id, metadata_obj)
                logging.info(f"Metadata applied after indexing: {final_video_id}")
            except Exception as e:
                logging.warning(f"Metadata application failed post-indexing: {e}")

        logging.info("Twelve Labs upload and indexing completed successfully.")
        sys.exit(0)

    except Exception as e:
        logging.critical(f"Upload or indexing failed: {e}")
        sys.exit(1)