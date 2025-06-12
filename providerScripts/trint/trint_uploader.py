import os
import sys
import json
import argparse
import logging
import urllib.parse
from urllib.parse import urlencode
import magic
import requests
from requests.auth import HTTPBasicAuth
from configparser import ConfigParser
import plistlib
from pathlib import Path

# Constants
VALID_MODES = ["proxy", "original"]
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

def create_folder(config_data, folder_name, parent_id=None):
    url = f"{config_data['base_url']}/folders/"
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    payload = {"name": folder_name}
    if parent_id:
        payload["parentId"] = parent_id

    logging.debug(f"Creating folder '{folder_name}' with parent ID: {parent_id}")
    logging.debug(f"Payload being sent: {json.dumps(payload)}")

    response = requests.post(
        url,
        auth=HTTPBasicAuth(config_data['api_key_id'], config_data['api_key_secret']),
        headers=headers,
        data=json.dumps(payload)
    )

    logging.debug(f"POST /folders/ response: {response.status_code} - {response.text}")

    if response.status_code not in [200, 201]:
        logging.error(f"Unexpected response when creating folder '{folder_name}': {response.text}")
        raise Exception(f"Failed to create folder '{folder_name}': {response.text}")

    folder_data = response.json()
    logging.info(f"Folder '{folder_name}' created or exists: {folder_data}")
    return folder_data

def list_all_folders(config_data):
    url = f"{config_data['base_url']}/folders/"
    headers = {"accept": "application/json"}

    response = requests.get(
        url,
        auth=HTTPBasicAuth(config_data['api_key_id'], config_data['api_key_secret']),
        headers=headers
    )

    if response.status_code not in [200, 201]:
        logging.error(f"Failed to fetch folders: {response.text}")
        raise Exception(f"Could not list folders: {response.text}")

    return response.json()


def get_folder_id(config_data, upload_path):
    """
    Resolves or creates the folder hierarchy based on upload_path and returns the final folder ID.
    """
    # Strip filename and leading/trailing slashes
    folder_path = os.path.dirname(upload_path).strip("/")
    folder_parts = folder_path.split("/")

    # Fetch all existing folders
    folders = list_all_folders(config_data)

    # Map full path -> folder metadata
    folder_map = {folder["name"]: folder for folder in folders}

    # Try to find the deepest existing folder path
    current_parent_id = None
    current_path_parts = []
    matched_index = -1

    for i, part in enumerate(folder_parts):
        current_path_parts.append(part)
        current_path = "/".join(current_path_parts)

        if current_path in folder_map:
            current_parent_id = folder_map[current_path]["_id"]
            matched_index = i
        else:
            break

    # Create folders from where match stopped
    for part in folder_parts[matched_index + 1:]:
        current_path_parts.append(part)
        folder_name = part
        current_path = "/".join(current_path_parts)

        folder_data = create_folder(config_data, folder_name, parent_id=current_parent_id)
        current_parent_id = folder_data["_id"]

        # Update folder_map with newly created folder
        folder_map[current_path] = folder_data

    logging.info(f"Resolved final folder ID for '{folder_path}': {current_parent_id}")
    return current_parent_id

def create_asset(config_data, file_path, backlink_url=None, folder_id=None, workspace_id=None, language="en"):
    key_id = config_data.get("api_key_id")
    key_secret = config_data.get("api_key_secret")
    base_upload_url = config_data.get("upload_url") or "https://upload.trint.com"

    file_name = os.path.basename(file_path)

    # Detect MIME type using python-magic
    mime_type = magic.from_file(file_path, mime=True)
    if not mime_type:
        raise ValueError("Could not detect MIME type of file")

    logger.info(f"Detected MIME type for '{file_name}': {mime_type}")

    # Prepare form fields
    data = {
        "filename": file_name,
        "language": language
    }

    if backlink_url:
        data["metadata"] = backlink_url
    if folder_id:
        data["folder-id"] = folder_id
    if workspace_id:
        data["workspace-id"] = workspace_id

    # Upload file using multipart/form-data
    with open(file_path, "rb") as file_data:
        files = {
            "file": (file_name, file_data, mime_type)
        }

        logger.info(f"Uploading file '{file_name}' to Trint...")
        logger.debug(f"Upload URL: {base_upload_url}")
        logger.debug(f"Form Data: {data}")

        response = requests.post(
            base_upload_url,
            auth=HTTPBasicAuth(key_id, key_secret),
            data=data,
            files=files
        )

    if response.status_code == 200:
        logger.info("Upload successful.")
        logger.debug(f"Response: {response.json()}")
        return response.json()
    else:
        logger.error(f"Upload failed with status: {response.status_code}")
        logger.error(f"Response: {response.text}")
        response.raise_for_status()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--jobId", help="Job Id of SDNA job")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded to Trint")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
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
    
    logging.info(f"Starting Trint upload process in {mode} mode")
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

    catalog_path = remove_file_name_from_path(matched_file)
    catalog_url = urllib.parse.quote(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    jobId = args.jobId
    client_ip, client_port = get_link_address_and_port()

    backlink_url = f"https://{client_ip}:{client_port}/dashboard/projects/{jobId}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated dashboard URL: {backlink_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Trint")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Trint")
    upload_path = args.upload_path

    
    # folder_id = create_folder(cloud_config_data, "Test2/v3")
    folder_id = get_folder_id(cloud_config_data, args.upload_path)
    print(f"Folder ID check -------------------------> {folder_id}")

    asset = create_asset(cloud_config_data, args.source_path, backlink_url, folder_id)    
    if asset and 'trintId' in asset:
        logging.info(f"asset upload completed ==============================> {asset['trintId']}")
        logging.debug(f"asset upload completed ==============================> {asset}")
    else:
        logging.error("Asset upload failed or 'trintId' not found in response.")
        
    sys.exit(0)