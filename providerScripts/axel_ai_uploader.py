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
import magic
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 20 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
DOMAIN = "http://15.204.217.105:5000/api"

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

def get_access_token(config):
    url = f"{DOMAIN}/login-user/"
    payload = {
        "username": config["username"],
        "password": config["password"]
    }
    headers = {"Content-Type": "application/json"}

    logging.info("Fetching JWT token...")
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code == 200:
                logging.info("JWT token acquired successfully.")
                return response.json()['axleToken']
            else:
                logging.error(f"Failed to get JWT token: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get access token after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def upload_file(file_path,endpoint, token, metadata):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    url = f"{DOMAIN}/{endpoint}"
    payload = {
        "file": file_path,
        "fileName": file_name,
        "tags": metadata
        
    }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code == 200:
                logging.info("JWT token acquired successfully.")
                response_data = response.json()['response']
                return response_data[0]
            else:
                logging.error(f"Failed to get JWT token: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get access token after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def poll_task_status(token, task_id, max_wait=10800, interval=10):
    url = f"{DOMAIN}/video-status/{task_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
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

            status_dict = response.json()["response"]["status"]
            all_processed = True
            for key, obj in status_dict.items():
                if obj.get("status") == "failed":
                    reason = obj.get("error", "No error message provided")
                    logging.error(f"Task {task_id} failed for '{key}': {reason}")
                    return False
                if obj.get("status") != "processed":
                    all_processed = False

            if all_processed:
                logging.info(f"Task {task_id} completed successfully. All objects processed.")
                return status_dict  # Success — return status dict

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
    logging.critical(f"Polling timed out after {max_wait}s. Task {task_id} did not reach all 'processed'.")
    return False

def update_catalog(repo_guid, file_path, media_id, max_attempts=3):
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
        "providerName": "axel_ai",
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
        # External retry backoff
        if attempt < max_attempts - 1:
            delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    logging.error("Catalog update failed after retries.")
    pass

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 1.5))  # Avoid herd

    parser = argparse.ArgumentParser(description="Upload video to Azure Video Indexer")
    parser.add_argument("-m", "--mode", required=True, help="Operation mode")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata file path")
    parser.add_argument("-j", "--job-guid", help="SDNA Job GUID")
    parser.add_argument("--controller-address", help="Override Link IP:Port")
    parser.add_argument("--parent-id", help="Parent folder ID (unused)")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("-r", "--repo-guid", help="Override repo GUID")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--log-level", default="info", help="Log level")

    args = parser.parse_args()
    setup_logging(args.log_level)

    # Validate mode
    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(1)

    # Load cloud config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logger.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logger.error(f"Config section not found: {args.config_name}")
        sys.exit(1)

    section = cloud_config[args.config_name]
    
    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logger.error(f"Source file not found: {matched_file}")
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
        logging.info("Metadata prepared successfully.")
    except Exception as e:
        logging.error(f"Failed to prepare metadata: {e}")
        metadata_obj = {}

    try:
        # Get access token
        token = get_access_token(section)
        if not token:
            logger.critical("Cannot proceed without access token.")
            sys.exit(1)

        # Determine upload endpoint based on MIME type
        mime_type = magic.from_file(args.source_path, mime=True)
        if not mime_type:
            logging.critical(f"Could not detect MIME type for '{extract_file_name(args.source_path)}'")
        if "video" in mime_type:
            endpoint = "upload-video-by-url"
        elif "image" in mime_type:
            endpoint = "upload-image-by-url"

        # Upload file and get file_id from response
        response = upload_file(args.source_path, endpoint, token, metadata_obj)
        
        if response and response.status_code in (200, 201):
            task_id = response.get("_id")  # may not exist yet
            if not task_id:
                logging.warning("task_id not in upload response. Will extract after ready.")
        else:
            logger.error(f"Upload failed: {response.status_code} {response.text if response else 'No response'}")
            sys.exit(1)
            
        # Step 3: Poll for indexing completion
        poll_result = poll_task_status(task_id, token, max_wait=args.max_poll_time)
        if not poll_result:
            logging.error("Task polling failed or timed out.")
            sys.exit(1)
        update_catalog(args.repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], task_id) 
        logging.info("Operation completed successfully.")
        sys.exit(0)
            
    except Exception as e:
        logger.critical(f"Operation failed: {e}")
        print(f"Error: {str(e)}")
        sys.exit(1)   