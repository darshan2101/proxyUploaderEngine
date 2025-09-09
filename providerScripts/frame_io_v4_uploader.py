import requests
import os
import sys
import argparse
import logging
import plistlib
import time
import random
import urllib.parse
from configparser import ConfigParser
from json import dumps
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 5 * 1024 * 1024
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
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
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

def get_root_asset_id(config_data):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/projects/{config_data['project_id']}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Root asset ID acquired Successfully.")
                return response.json()['data']['root_folder_id']
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to Root asset ID after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None 

def get_folders_list(config_data, folder_id):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{folder_id}/children"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Folder list acquired successfully.")
                items = response.json()['data']
                all_folders = []
                for item in items:
                    if item['type'] == 'folder':
                        all_folders.append(
                            {
                                "name": item['name'],
                                "id": item["id"]
                            }
                        )
                return all_folders
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get Folder list after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None 

def get_folders_content(config_data, asset_id, next_page_url = None, asset_data_list = None):
    if asset_data_list is None:
        asset_data_list = []
    if next_page_url is None:
        url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{asset_id}/children"
    else:
        url = f"{config_data['domain']}{next_page_url}"
    headers = { 'Accept': 'application/json', 'Authorization': f"Bearer {config_data['token']}" }
    logging.debug(f"URL: {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("contents of folder acquired successfully.")
                asset_data_list.extend(response.json()['data'])
                next_page = response.json()['links']['next']
                if not next_page is None:
                    get_folders_content(asset_id,next_page,asset_data_list)
                return asset_data_list
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get contents of folder after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def remove_file(config_data, asset_id):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/files/{asset_id}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    logging.debug(f"URL: {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("DELETE", url, headers=headers)
            if response and response.status_code == 204:
                logging.info("Asset removed successfully.")
                return True
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return False
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to remove asset after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None    

def create_folder(config_data,folder_name,parent_id):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{parent_id}/folders"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    payload = {
        "data": {
                "name": folder_name 
            }
        }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                folder_id = response.json()['data']['id']
                logging.debug(f"Created folder '{folder_name}' with ID: {folder_id}")
                return folder_id
            else:
                logging.warning(f"Failed to create folder '{folder_name}' (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None
        except Exception as e:
            logging.warning(f"Exception during folder creation (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to create folder after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def find_upload_id(upload_path, config_data, base_id=None):
    current_parent_id = base_id or get_root_asset_id(config_data)
    if not current_parent_id:
        logging.error("Unable to get base/root folder id.")
        return None
    folder_path = upload_path
    logging.info(f"Finding or creating folder path: '{folder_path}'")

    for segment in folder_path.strip("/").split("/"):
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")
        folders = get_folders_list(config_data, current_parent_id)
        if not isinstance(folders, list):
            logging.error(f"Failed to get folders list: {folders}")
            return None
        matched = next((f for f in folders if f.get("name") == segment), None)

        if matched:
            current_parent_id = matched["id"]
            logging.info(f"Folder '{segment}' already exists (ID: {matched['id']}) under parent ID {current_parent_id}")
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data, segment, current_parent_id)
            if not current_parent_id:
                logging.error(f"Failed to create folder '{segment}'")
                return None
            logging.info(f"Created new folder '{segment}' under parent ID {current_parent_id}, new ID: {current_parent_id}")
            logging.debug(f"Created folder '{segment}' (ID: {current_parent_id})")

    logging.info(f"Final destination folder ID for upload: {current_parent_id}")
    return current_parent_id

def create_asset(config_data,folder_id,file_path):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{folder_id}/files/local_upload"
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part

    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    file_size = os.path.getsize(file_path)
    # Conflict resolution logic
    conflict_resolution = config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    asset_list = get_folders_content(config_data, folder_id)
    existing_asset = next((asset for asset in asset_list if asset['name'] == file_name and int(asset['file_size']) == int(file_size) ), None)
    if existing_asset:
        if conflict_resolution == "skip":
            logging.info(f"File '{file_name}' exists. Skipping upload.")
            print(f"File '{file_name}' already exists. Skipping upload.")
            sys.exit(0)
        elif conflict_resolution == "overwrite":
            is_deleted = remove_file(config_data, existing_asset['id'])
            if is_deleted == True:
                logging.info(f"Removed existing asset: {existing_asset['name']} with ID: {existing_asset['id']}")
            else:
                logging.error(f"Failed to remove existing asset: {existing_asset['name']} with ID: {existing_asset['id']}")
    payload = {
        "data": {
            "name": file_name,
            "file_size": os.path.getsize(file_path)
        }
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
    }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info(f"Created asset '{file_name}' with size {os.path.getsize(file_path)} bytes in folder ID {folder_id}")
                return response.json(), response.status_code
            else:
                logging.error(f"Failed to create asset for file '{file_path}'. Status: {response.status_code}, Response: {response.text}")
        except Exception as e:
            logging.warning(f"Initiate upload error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to initiate upload after 3 attempts.")
                return None, 500
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    return None, 500

def update_catalog(repo_guid, file_path, project_id, folder_id, asset_id, max_attempts=5):
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
        "providerName": "frameio_v4",
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "projectId": project_id,
            "providerUiLink": f"https://next.frame.io/project/{project_id}/view/{asset_id}"
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
        
def upload_parts(file_path, asset_info):
    upload_urls = asset_info['data']['upload_urls']
    content_type = asset_info['data']['media_type']
    with open(file_path, "rb") as f:
        for i, part in enumerate(upload_urls):
            part_url = part["url"]
            part_size = part["size"]
            part_data = f.read(part_size)
            
            for attempt in range(3):
                try:
                    logging.info(f"Uploading part {i + 1}/{len(upload_urls)} of file '{file_path}' to Frame.io")
                    response = make_request_with_retries("PUT", part_url, data=part_data, headers={"Content-Type": content_type, "x-amz-acl": "private"})
                    if response.status_code == 200:
                        logging.debug(f"Part {i + 1} uploaded successfully.")
                        logging.info(f"Successfully uploaded part {i + 1}")
                    else:
                        logging.debug(f"Failed to upload part {i + 1}: {response.status_code} - {response.text}")
                        logging.error(f"Failed to upload part {i + 1}: HTTP {response.status_code} - {response.text}")
                        return {
                            "detail": response.text,
                            "status_code": response.status_code
                        }
                except RequestException as e:
                    logging.warning("Part %d upload failed (attempt %d): %s", i, attempt + 1, e)
                    if attempt == 2:
                        logging.error("Failed to upload part %d after 3 attempts.", i)
                        raise
                    time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))                    
    return {
        "detail": "uploaded all part",
        "status_code": 200
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Id of SDNA job")
    # parser.add_argument("-p", "--project-id", required=True, help="Project Id")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Pass if upload path is already resolved ID")
    parser.add_argument("--controller-address",help="Link IP/Hostname Port")
    
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
    if not cloud_config_data.get('domain'):
        cloud_config_data['domain'] = "https://api.frame.io"
        
    project_id = cloud_config_data['project_id']
    logging.info(f"project Id ----------------------->{project_id}")
    
    if mode == "get_base_target":
        upload_path = args.upload_path
        if not upload_path:
            logging.error("Upload path must be provided for get_base_target mode")
            sys.exit(1)

        logging.info(f"Fetching upload target ID for path: {upload_path}")
        
        if args.resolved_upload_id:
            print(args.upload_path)
            sys.exit(0)

        logging.info(f"Fetching upload target ID for path: {upload_path}")
        base_id = args.parent_id or None
        up_id = find_upload_id(upload_path, cloud_config_data, base_id) if '/' in upload_path else upload_path
        print(up_id)
        sys.exit(0)
    
    if mode == 'buckets':
        asset_id = get_root_asset_id(cloud_config_data)
        buckets = []
        response = get_folders_list(cloud_config_data, asset_id)

        if not response:
            logging.error(f"Failed to fetch Buckets.")
            sys.exit(1)
        print(f"Response of Buckets list -----> {response}")
        sys.exit(0)


    logging.info(f"Starting FrameIO v4 upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    matched_file = args.source_path
    catalog_path = args.catalog_path
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)

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

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Frame.io")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Frame.io")
    upload_path = args.upload_path
    
    if args.resolved_upload_id:
        folder_id = upload_path
    else:
        folder_id = find_upload_id(upload_path, cloud_config_data, args.parent_id)
    
    if not folder_id:
        logging.warning(f"failed to find location no upload asset")
        sys.exit(1)

    asset_info, creation_status = create_asset(cloud_config_data, folder_id, args.source_path)
    if creation_status not in (200, 201) or asset_info is None:
        logging.error(f"Failed to create asset: {getattr(asset_info, 'text', asset_info)}")
        sys.exit(1)

    parts_info = upload_parts(args.source_path, asset_info.json())
    if not parts_info or parts_info.get("status_code", 0) not in (200, 201):
        detail = parts_info['detail'] if parts_info else 'No detail available'
        logging.error(f"Failed to upload file parts: {detail}")
        sys.exit(1)
    else:
        update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], project_id, folder_id, asset_info['data']['id'])
        logging.info("All parts uploaded successfully to Frame.io")
        sys.exit(0)