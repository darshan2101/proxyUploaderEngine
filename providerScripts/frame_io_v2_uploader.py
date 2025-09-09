import os
import sys
import json
import argparse
import logging
import urllib.parse
import requests
import random
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from json import dumps
from requests import request
from configparser import ConfigParser
import xml.etree.ElementTree as ET
from frameioclient import FrameioClient
import plistlib
from pathlib import Path

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
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

def find_upload_id(path, project_id, token, base_id=None):
    logging.info(f"Finding upload location for path: {path}")
    client = FrameioClient(token)
    current_id = base_id or client.projects.get(project_id)['root_asset_id']
    logging.debug(f"Starting from root asset ID: {current_id}")

    for segment in path.strip('/').split('/'):
        logging.debug(f"Processing path segment: {segment}")
        children = client.assets.get_children(current_id, type='folder')
        next_id = next((child['id'] for child in children if child['name'] == segment), None)
        if not next_id:
            logging.info(f"Creating new folder: {segment}")
            next_id = client.assets.create(parent_asset_id=current_id, name=segment, type="folder", filesize=0)['id']
        current_id = next_id
        logging.debug(f"Current folder ID: {current_id}")
    return current_id

def remove_file(client, asset_id):
    try:
        remove = client.assets.delete(asset_id)
        return True
    except Exception as e:
        print("Error while Removing file :", e.text)
        return False

def upload_file(client, source_path, up_id, description):
    original_file_name = os.path.basename(source_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    file_size = os.path.getsize(source_path)
    
    # check if file exists
    asset_list = client.assets.get_children(up_id, type='file')
    conflict_resolution = cloud_config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    existing_asset = next((asset for asset in asset_list if asset['name'] == file_name and int(asset['filesize']) == int(file_size) ), None)
    if existing_asset:
        if conflict_resolution == "skip":
            logging.info(f"File '{file_name}' exists. Skipping upload.")
            print(f"File '{file_name}' already exists. Skipping upload.")
            sys.exit(0)
        elif conflict_resolution == "overwrite":     
            if remove_file(client, existing_asset['id']) == True:
                logging.info(f"Removed existing asset: {existing_asset['name']} with ID: {existing_asset['id']}")
            else:
                logging.error(f"Failed to remove existing asset: {existing_asset['name']} with ID: {existing_asset['id']}")
                return None

    logging.debug("Creating asset in Frame.io")
    asset = client.assets.create(
        parent_asset_id=up_id,
        name=extract_file_name(source_path),
        type="file",
        description=description,
        filesize=file_size
    )
    logging.debug(f"Asset created with ID: {asset['id']}")

    logging.info("Uploading file content...")
    with open(source_path, 'rb') as f:
        client.assets._upload(asset, f)
    logging.info("File upload completed successfully")
    return asset['id']

def update_catalog(repo_guid, file_path, folder_id, asset_id, max_attempts=5):
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
        "providerName": "frameio_v2",
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "providerUiLink": f"https://app.frame.io/player/{asset_id}"
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

def update_asset(token, asset_id, properties_file):
    logging.info(f"Updating asset {asset_id} with properties from {properties_file}")
    
    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return None, 204

    props = {}
    logging.debug(f"Reading properties from: {properties_file}")
    file_ext = properties_file.lower()
    try:
        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                props = json.load(f)
                logging.debug(f"Loaded JSON properties: {props}")

        elif file_ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        props[key] = value
                logging.debug(f"Loaded XML properties: {props}")
            else:
                logging.error("No <meta-data> section found in XML.")
                
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        props[key] = value
                logging.debug(f"Loaded CSV properties: {props}")
    except Exception as e:
        logging.error(f"Failed to parse metadata file {properties_file}: {e}")
        return None, 422  # Unprocessable Entity

    if not props:
        logging.warning(f"[Metadata Warning] No valid properties extracted from file: {properties_file}")
        return None, 204

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    logging.debug("Sending asset update request to Frame.io API")
    response = request('PUT', f'https://api.frame.io/v2/assets/{asset_id}', headers=headers, data=dumps({'properties': props}))
    if response.status_code in (200, 201):
        logging.info("Uploaded successfully")
    else:
        logging.error("Failed to upload Metadata file: %s", response.text)
    return response, response.status_code

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
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
    token = cloud_config_data['FrameIODevKey']
    project_id = cloud_config_data['project_id']
    logging.info(f"project Id ----------------------->{project_id}")

    logging.info(f"Starting Frame.io upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    logging.info(f"Initializing Frame.io client for project: {project_id}")
    client = FrameioClient(token)
    
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
        up_id = find_upload_id(upload_path, project_id, token, base_id) if '/' in upload_path else upload_path
        print(up_id)
        sys.exit(0)
        

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

    catalog_path = remove_file_name_from_path(catalog_path)
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

    url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated dashboard URL: {url}")

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
        up_id = upload_path
    else:
        up_id = find_upload_id(upload_path, project_id, token, args.parent_id)
    logging.info(f"Upload location ID: {up_id}")
    
    asset_id = upload_file(client, matched_file, up_id, url)

    if not asset_id:
        print("Failed to upload file to Frame.io")
        sys.exit(3)

    logging.info(f"File uploaded successfully. Asset ID: {asset_id}")
    update_catalog(args.repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], up_id, asset_id)
    meta_file = args.metadata_file
    if meta_file:
        logging.info("Applying metadata to uploaded asset...")
        response, metadata_code = update_asset(token, asset_id, meta_file)

        if metadata_code in (200, 201):
            logging.info("Metadata update confirmed by Frame.io.")
        elif metadata_code == 204:
            logging.info("Metadata skipped (either not provided or not parseable).")
        elif metadata_code == 422:
            logging.warning("Metadata file found but could not be parsed. Skipping update.")
        else:
            logging.warning(f"Asset uploaded but metadata update failed with status {metadata_code}. See logs for details.")

    sys.exit(0)
