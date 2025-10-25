import os
import sys
import json
import math
import argparse
import logging
import urllib.parse
import requests
import time
import random
from json import dumps
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from action_functions import flatten_dict

# Constants
VALID_MODES = ["proxy", "original", "get_base_target", "generate_video_proxy", "generate_video_frame_proxy", "generate_intelligence_proxy", "generate_video_to_spritesheet", "send_extracted_metadata"]
CATEGORIES = ["persons", "objects", "locations", "time_stamped_events", "emotions", "ocr_text", "brands", "keywords", "ad_recommendations", "scenes", "subtitle", "dialogues"]
CHUNK_SIZE = 20 * 1024 * 1024
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logger = logging.getLogger()

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    # Include PID for easier debugging in concurrent runs
    logging.basicConfig(level=numeric_level, format='%(asctime)s [PID %(process)d] %(levelname)s: %(message)s')
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
            pl = plistlib.load(fp)
            path = pl["CloudConfigFolder"] + "/cloud_targets.conf"
    logging.info(f"Using cloud config path: {path}")
    return path

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

def get_access_token(config):
    url = f"{config['base_url']}/auth/token/"
    payload = {
        "email": config["email"],
        "password": config["password"]
    }
    headers = {"Content-Type": "application/json"}

    logging.info("Fetching JWT token...")
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code == 200:
                logging.info("JWT token acquired successfully.")
                return response.json()['access']
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

def create_folder(base_url, token, name, workspace_id, parent_id=None):
    url = f"{base_url}/api/v1/library/folder/"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "name": name,
        "workspace": workspace_id,
    }
    if parent_id:
        payload["parent"] = parent_id

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                folder_id = response.json()["id"]
                logging.debug(f"Created folder '{name}' with ID: {folder_id}")
                return folder_id
            else:
                logging.warning(f"Failed to create folder '{name}' (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Exception during folder creation (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to create folder after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return None

def list_assets(config_data, token, workspace_id, parent_id, resource_type='Folder'):
    base_url = config_data['base_url']
    url = f"{base_url}/api/v1/library/"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "workspace": workspace_id,
        "parent": parent_id or "",
        "page_size": 100
    }
    all_assets = []

    while url:
        try:
            response = make_request_with_retries("GET", url, headers=headers, params=params)
            if not response or response.status_code not in (200, 201):
                logging.error(f"Failed to list assets: {response.text if response else 'No response'}")
                break

            data = response.json().get("data", {})
            results = data.get("results", [])
            if resource_type == 'File':
                assets = [item for item in results if item.get("resourcetype") != 'Folder']
            else:
                assets = [item for item in results if item.get("resourcetype") == resource_type]
            all_assets.extend(assets)

            meta = data.get("meta")
            if meta and meta.get("next"):
                next_url = meta.get("next")
                if next_url.startswith("http://"):
                    logging.warning(f"Insecure pagination URL: {next_url}, converting to HTTPS")
                    next_url = next_url.replace("http://", "https://")
                url = next_url
                params = None  # Only used on first request
            else:
                url = None
        except Exception as e:
            logging.error(f"Error fetching asset list: {e}")
            break

    return all_assets

def find_upload_id_tessact(upload_path, token, config_data, base_id = None):
    workspace_id = config_data['workspace_id']
    current_parent_id = base_id

    logging.info(f"Resolving folder path: '{upload_path}'")

    for segment in upload_path.strip("/").split("/"):
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")

        folders = list_assets(config_data, token, workspace_id, current_parent_id, 'Folder')
        matched = next((f for f in folders if f.get("name") == segment), None)

        if matched:
            current_parent_id = matched["id"]
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data['base_url'], token, segment, workspace_id, current_parent_id)
            if not current_parent_id:
                logging.error(f"Failed to create or find folder '{segment}'. Aborting.")
                sys.exit(1)
            logging.debug(f"Created folder '{segment}' (ID: {current_parent_id})")

    return current_parent_id

def get_file_parts(file_size, chunk_size):
    num_parts = math.ceil(file_size / chunk_size)
    parts = []
    for i in range(num_parts):
        part_size = min(chunk_size, file_size - i * chunk_size)
        parts.append({
            "number": i + 1,
            "size": part_size,
            "is_final": i == num_parts - 1
        })
    return parts


def find_existing_file(config_data, token, filename, filesize, parent_id=None):
    logging.info(f"Checking if file '{filename}' exists in folder ID: {parent_id}")
    all_assets = list_assets(config_data, token, config_data['workspace_id'], parent_id, 'File')
    file_match = next(
        (item for item in all_assets if item.get("name") == filename and int(item['size']) == int(filesize)),
        None
    )
    if file_match:
        logging.info(f"Existing file found: ID={file_match['id']}, Size={file_match['size']}")
    else:
        logging.info(f"No matching file found for '{filename}'")
    return file_match


def delete_existing_file(config_data, token, asset_id, filename=None):
    url = f"{config_data['base_url']}/api/v1/library/{asset_id}/"
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(3):
        try:
            response = make_request_with_retries("DELETE", url, headers=headers)
            if response and response.status_code in (200, 204):
                logging.info(f"Deleted existing file '{filename or asset_id}' (ID: {asset_id})")
                return True
            else:
                logging.warning(f"Delete failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Delete request failed (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error(f"Failed to delete file '{filename or asset_id}' after 3 attempts.")
                return False
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return False

def initiate_upload(file_path, config_data, token, parent_id=None):
    url = f"{config_data['base_url']}/api/v1/upload/initiate_upload/"
    workspace_id = config_data['workspace_id']
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    file_size = os.path.getsize(file_path)
    

    # Conflict resolution logic
    conflict_resolution = config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    existing_file = find_existing_file(config_data, token, file_name, file_size, parent_id)
    if existing_file:
        if conflict_resolution == "skip":
            logging.info(f"File '{file_name}' exists. Skipping upload.")
            update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], workspace_id, parent_id, existing_file["id"])
            print(f"File '{file_name}' already exists. Skipping upload.")
            sys.exit(0)
        elif conflict_resolution == "overwrite":
            if not delete_existing_file(config_data, token, existing_file["id"], file_name):
                logging.error("Aborting upload due to failed deletion.")
                sys.exit(1)

    parts = get_file_parts(file_size, CHUNK_SIZE)
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    payload = {
        "file_name": file_name,
        "file_size": file_size,
        "workspace": workspace_id,
        "parts": parts,
    }
    if parent_id:
        payload["parent"] = parent_id

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Upload initiated successfully.")
                return response, response.status_code
            else:
                logging.warning(f"Initiate upload failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Initiate upload error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to initiate upload after 3 attempts.")
                return None, 500
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return None, 500

def upload_parts(file_path, presigned_urls):
    etags = []
    logging.info("Uploading file parts using presigned URLs")
    with open(file_path, 'rb') as f:
        for i, url in enumerate(presigned_urls):
            part_data = f.read(CHUNK_SIZE)
            part_num = i + 1

            for attempt in range(3):
                try:
                    logging.info("Uploading part %d/%d (attempt %d)", part_num, len(presigned_urls), attempt + 1)
                    response = requests.put(url, data=part_data)
                    response.raise_for_status()
                    etag = response.headers.get("ETag")
                    etags.append({
                        "PartNumber": part_num,
                        "ETag": etag.strip('"') if etag else None
                    })
                    logging.info("Part %d uploaded, ETag: %s", part_num, etag)
                    break
                except RequestException as e:
                    logging.warning("Part %d upload failed (attempt %d): %s", part_num, attempt + 1, e)
                    if attempt == 2:
                        logging.error("Failed to upload part %d after 3 attempts.", part_num)
                        raise
                    time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    logging.info("All parts uploaded successfully")
    return etags

def finalize_upload(base_url, token, payload):
    url = f"{base_url}/api/v1/upload/finalize_upload/"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Upload finalized successfully.")
                return response.json(), response.status_code
            else:
                logging.warning(f"Finalize upload failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Finalize upload error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to finalize upload after 3 attempts.")
                return {}, 500
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return {}, 500

def update_catalog(repo_guid, file_path, workspace_id, folder_id, asset_id, max_attempts=5):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    # Read NodeAPIKey from client services config
    node_api_key = get_node_api_key()
    base_url = cloud_config_data["base_url"].strip()
    domain = "https://anything.tessact.com" if base_url == "https://dev-api.tessact.com" else "https://app.tessact.ai"
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": cloud_config_data.get("provider", "tessact"),
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "workspaceId": workspace_id,
            "providerUiLink": f"{domain}/library/asset/{asset_id}?workspace={workspace_id}"
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
                    break  # non-retryable 404
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

def parse_metadata_file(properties_file):
    props = {}
    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Metadata file not found: {properties_file}")
        return props
    try:
        ext = properties_file.lower()
        if ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                props = json.load(f)
        elif ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        props[key] = value
            else:
                logging.warning("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',', 1)
                    if len(parts) == 2:
                        props[parts[0].strip()] = parts[1].strip()
        logging.debug(f"Parsed metadata: {props}")
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")

    return props


def upload_metadata_to_asset(base_url, token, backlink_url, asset_id, properties_file = None):
    props = parse_metadata_file(properties_file)
    metadata = [{"field_name": "fabric URL", "field_type": "text", "value": backlink_url}]
    metadata.extend({"field_name": k, "field_type": "text", "value": v} for k, v in props.items())

    payload = {'file_id': asset_id, 'metadata': metadata}
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'}
    url = f'{base_url}/api/v1/value_instances/bulk_update/'

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Metadata uploaded successfully.")
                return response, response.status_code
            else:
                logging.warning(f"Metadata upload failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"Metadata upload error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to upload metadata after 3 attempts.")
                return response or requests.Response(), 500
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return requests.Response(), 500

def is_asset_indexed(config, token, asset_id):
    url = f"{config['base_url']}/api/v1/video_detection_metadata/get_sample_video_metadata/?video_id={asset_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = make_request_with_retries("GET", url, headers=headers)
        if resp and resp.status_code == 200:
            data = resp.json()
            return data.get("index_status") == "completed" and data.get("index_percentage") == 100.0
    except Exception as e:
        logging.warning(f"Failed to check indexing status for {asset_id}: {e}")
    return False

def get_video_metadata_by_categories(config, token, asset_id):
    base_url = config['base_url']
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {token}"
    }
    result = {}
    for category in CATEGORIES:
        url = f"{base_url}/api/v1/video_detection_metadata/get_video_metadata/?video_id={asset_id}&category={category}"
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code == 200:
                data = response.json()
                # If the API returns an array, store it, else store empty array
                result[category] = data if isinstance(data, list) else []
            else:
                logging.warning(f"Failed to fetch {category}: {response.status_code if response else 'No response'}")
                result[category] = []
        except Exception as e:
            logging.error(f"Error fetching {category}: {e}")
            result[category] = []
    return result

def send_extracted_metadata(config, repo_guid, file_path, metadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "tessact"),
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

if __name__ == '__main__':
    # Random delay to avoid thundering herd
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata operations")
    parser.add_argument("-j", "--job-guid", help="Job GUID")
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata file path")
    parser.add_argument("-up", "--upload-path", help="Upload path or ID")
    parser.add_argument("-sl", "--size-limit", help="Size limit in MB")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--log-level", default="debug", help="Log level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Upload path is already resolved ID")
    parser.add_argument("--controller-address", help="Controller IP:Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata")

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
    if not cloud_config_data.get("base_url"):
        cloud_config_data["base_url"] = cloud_config_data.get("domain", "https://dev-api.tessact.com").strip()

    workspace_id = cloud_config_data['workspace_id']
    if args.export_ai_metadata:
        cloud_config_data["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"

    logging.info(f"Starting Tessact upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")

    if mode == "get_base_target":
        if args.resolved_upload_id:
            print(args.upload_path)
            sys.exit(0)
        token = get_access_token(cloud_config_data)
        if not token:
            logging.error("Failed to get token.")
            sys.exit(1)
        base_id = args.parent_id or None
        up_id = find_upload_id_tessact(args.upload_path, token, cloud_config_data, base_id)
        print(up_id)
        sys.exit(0)

    if mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required.")
            sys.exit(1)
        token = get_access_token(cloud_config_data)
        if not token:
            logging.error("Failed to get token.")
            sys.exit(1)

        if is_asset_indexed(cloud_config_data, token, args.asset_id):
            metadata = get_video_metadata_by_categories(cloud_config_data, token, args.asset_id)
            catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
            if send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean,metadata):
                logging.info("Extracted metadata sent successfully.")
                sys.exit(0)
            else:
                logging.error("Failed to send extracted metadata.")
                sys.exit(1)
        else:
            logging.info(f"Asset {args.asset_id} not indexed yet.")
            sys.exit(1)
    
    matched_file = args.source_path
    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    catalog_path = args.catalog_path or matched_file
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
    matched_file_size = os.path.getsize(matched_file)

    if args.size_limit and mode == "original":
        try:
            limit_mb = float(args.size_limit)
            if matched_file_size > limit_mb * 1024 * 1024:
                logging.error(f"File too large: {matched_file_size / 1024 / 1024:.2f} MB > {limit_mb} MB")
                sys.exit(4)
        except ValueError:
            logging.warning("Invalid size limit.")

    # Generate backlink URL
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        client_ip, _ = get_link_address_and_port()

    backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Tessact")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    token = get_access_token(cloud_config_data)
    if not token:
        logging.error("Failed to get access token.")
        sys.exit(1)

    folder_id = args.upload_path if args.resolved_upload_id else find_upload_id_tessact(args.upload_path, token, cloud_config_data)
    if not folder_id:
        logging.error("Failed to resolve upload folder.")
        sys.exit(1)
    logging.info(f"Upload location ID: {folder_id}")
    response, code = initiate_upload(matched_file, cloud_config_data, token, folder_id)
    if code not in (200, 201):
        logging.error("Upload initiation failed.")
        sys.exit(1)

    meta = response.json()
    file_id = meta["data"]["id"]
    upload_id = meta["upload_id"]
    presigned_urls = meta["presigned_urls"]

    try:
        etags = upload_parts(matched_file, presigned_urls)
        finalize_payload = {"file": file_id, "upload_id": upload_id, "parts": etags}
        finalize_response, finalize_code = finalize_upload(cloud_config_data['base_url'], token, finalize_payload)
        if finalize_code not in (200, 201):
            logging.error("Finalize upload failed.")
            sys.exit(1)
        update_catalog(args.repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], workspace_id, folder_id, file_id)
        logging.info(f"Upload successful. Asset ID: {file_id}")

        meta_file = args.metadata_file
        meta_resp, meta_code = upload_metadata_to_asset(cloud_config_data['base_url'], token, backlink_url, file_id, meta_file)
        if meta_code not in (200, 201):
            print("File uploaded but basic metadata failed.")

        if cloud_config_data['export_ai_metadata'] == "true" and is_asset_indexed(cloud_config_data, token, file_id):
            logging.info("Asset is indexed. Fetching and sending AI metadata...")
            metadata = get_video_metadata_by_categories(cloud_config_data, token, file_id)
            catalog_path_clean = catalog_path.replace("\\", "/").split("/1/", 1)[-1]
            if send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, metadata):
                logging.info("AI metadata sent successfully.")
            else:
                logging.error("Failed to send AI metadata.")
                sys.exit(1)  # metadata ready but send failed
        else:
            print(f"Metadata extraction failed for asset: {file_id}")
            sys.exit(7)

    except Exception as e:
        logging.error(f"Upload failed: {e}")
        sys.exit(1)

    sys.exit(0)
