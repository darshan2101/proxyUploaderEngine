import os
import sys
import json
import math
import argparse
import logging
import urllib.parse
import requests
from json import dumps
from configparser import ConfigParser
import plistlib
from pathlib import Path

# Constants
VALID_MODES = ["proxy", "original"]
CHUNK_SIZE = 5 * 1024 * 1024 
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

def get_access_token(config):
    url = f"{config['base_url']}/auth/token/"
    logging.info(f"url check -------------------------------> {url}")
    payload = {
        "email": f"{config['email']}",
        "password": f"{config['password']}"
    }
    logging.info("Requesting JWT token for user %s", config['email'])
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        logging.info("JWT token received successfully")
        return response.json()['access']
    else:
        logging.error("Failed to get JWT token: %s", response.text)
        response.raise_for_status()

    
def create_folder(base_url, token, name, workspace_id, parent_id = None):
    headers = {"Content-Type": "application/json" ,"Authorization": f"Bearer {token}"}
    url = f"{base_url}/api/v1/library/folder/"
    payload = {
        "name": name,
        "workspace": workspace_id,
    }
    if parent_id is not None:
        payload["parent"] = parent_id
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Response for folder creation ------------------------------> {response.json()}")
    if response.status_code in (200, 201):
        logging.info("Token Aquired Successfully")
        return response.json()["id"]
    else:
        logging.error("Failed to create Folder")

def list_all_folders(config_data, token, workspace_id, parent_id):
    headers = {"Authorization": f"Bearer {token}"}
    base_url = config_data['base_url']
    all_folders = []

    url = f"{base_url}/api/v1/library/"
    params = {
        "workspace": workspace_id,
        "parent": parent_id or "",
        "page_size": 100
    }

    while url:
        logging.debug(f"Fetching folders from: {url}")
        response = requests.get( url, headers=headers, params=params)
        if response.status_code in (200, 201):
            logging.info("File Tree Successfully")
            data = response.json().get("data", {})
        else:
            logging.error("Failed to get File Tree")

        folders = [
            item for item in data.get("results", [])
            if item.get("resourcetype") == "Folder"
        ]
        all_folders.extend(folders)

        url = data.get("meta", {}).get("next")
        params = None  # Only used on first call
    return all_folders

def find_upload_id_tessact(path, token, config_data):
    workspace_id = config_data['workspace_id']
    current_parent_id = None

    logging.info(f"Finding or creating folder path: '{path}'")

    for segment in path.strip("/").split("/"):
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")

        folders = list_all_folders(config_data, token, workspace_id, current_parent_id)
        matched = next((f for f in folders if f.get("name") == segment), None)

        if matched:
            current_parent_id = matched["id"]
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data['base_url'], token, segment, workspace_id, current_parent_id)
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

def initiate_upload(file_path, config_data, token, parent_id=None):
    url = f"{config_data['base_url']}/api/v1/upload/initiate_upload/"
    workspace_id = config_data['workspace_id']
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    parts = get_file_parts(file_size, CHUNK_SIZE)
    headers = {"Content-Type": "application/json" ,"Authorization": f"Bearer {token}"}
    payload = {
        "file_name": file_name,
        "file_size": file_size,
        "workspace": workspace_id,
        "parts": parts,
    }
    if parent_id is not None:
        payload["parent"] = parent_id
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in (200, 201):
        logging.info("File Upload Initiated successfully")
        return response.json()
    else:
        logging.error("Failed to Initiate File Upload")

def upload_parts(file_path, presigned_urls):
    etags = []
    logging.info("Uploading file parts using presigned URLs")
    with open(file_path, 'rb') as f:
        for i, url in enumerate(presigned_urls):
            logging.info("Uploading part %d/%d", i + 1, len(presigned_urls))
            part_data = f.read(CHUNK_SIZE)
            put_resp = requests.put(url, data=part_data)
            put_resp.raise_for_status()
            etag = put_resp.headers.get("ETag")
            logging.info("Part %d uploaded, ETag: %s", i + 1, etag)
            etags.append({
                "PartNumber": i + 1,
                "ETag": etag.strip('"') if etag else None
            })
    logging.info("All parts uploaded successfully")
    return etags

def finalize_upload(base_url, token, payload):
    if not base_url:
        logging.error("Missing 'base_url' in payload for finalize_upload")
        return
    url = f"{base_url}/api/v1/upload/finalize_upload/"
    logging.info("Finalizing upload with payload: %s", payload)
    headers = {"Content-Type": "application/json" ,"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    logging.info("Finalize upload response: %s", response.text)

def upload_metadata_to_asset(base_url, token, asset_id, properties_file):
    logging.info(f"Updating asset {asset_id} with properties from {properties_file}")
    
    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        sys.exit(1)

    props = {}
    logging.debug(f"Reading properties from: {properties_file}")

    try:
        if properties_file.endswith(".json"):
            with open(properties_file, 'r') as f:
                props = json.load(f)
                logging.debug(f"Loaded JSON properties: {props}")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        props[key] = value
                logging.debug(f"Loaded CSV properties: {props}")
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")
        sys.exit(1)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    logging.debug("Sending asset update request to Tessact API")
    metadata = []
    for k, v in props.items():
        metadata.append(
            {
                "field_name": k,
                "field_type": "text",
                "value": v
            }
        )
    payload = { 
        'file_id': asset_id,
        'metadata': metadata
    }
    url =  f'{base_url}/api/v1/value_instances/bulk_update/'
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    logging.info(f"Asset update completed with status code: {response.status_code}")
    if response.status_code in (200, 201):
        logging.info(" uploaded successfully")
        return response.json()
    else:
        logging.error("Failed to upload Metadata file: %s",response.text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--jobId", help="Job Id of SDNA job")
    parser.add_argument("-cp", "--catalog-path", required=True, help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", required=True, help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to frameIO")
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

    workspace_id = cloud_config_data['workspace_id']
    print(f"Workspace Id ----------------------->{workspace_id}")

    logging.info(f"Starting Tessact upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    logging.info(f"Initializing Tessact client for workspace: {workspace_id}")
    token = get_access_token(cloud_config_data)
    if not token:
        logging.error("Failed to get Token. Exiting.")
        sys.exit(1)
    
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

    url = f"https://{client_ip}:{client_port}/dashboard/projects/{jobId}/browse&search?path={catalog_url}&filename={filename_enc}"
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
    
    up_id = find_upload_id_tessact(upload_path, token,cloud_config_data)
    logging.info(f"Upload location ID: {up_id}")
    
    # Initiate upload with chunks
    upload_meta = initiate_upload(args.source_path, cloud_config_data, token, up_id)
    if not upload_meta:
        logging.error("Failed to initiate upload. Exiting.")
        sys.exit(1)
    file_id = upload_meta["data"]["id"]
    upload_id = upload_meta["upload_id"]
    presigned_urls = upload_meta["presigned_urls"]
    logging.info("Received upload metadata: file_id=%s, upload_id=%s", file_id, upload_id)
    
    # Upload all parts to given urls and get etags to finalize upload
    part_etags = upload_parts(args.source_path, presigned_urls)
    
    finalize_payload = {
        "file": upload_meta["data"]["id"],
        "upload_id": upload_id,
        "parts": part_etags
    }
    finalize_upload(cloud_config_data['base_url'], token,finalize_payload)
    
    logging.info(f"File uploaded successfully. Asset ID: {file_id}")

    meta_file = args.metadata_file
    if meta_file:
        logging.info("Applying metadata to uploaded asset...")
        response = upload_metadata_to_asset(cloud_config_data['base_url'] ,token, file_id, meta_file)
        if response is not None:
            parsed = response
            print(json.dumps(parsed, indent=4))
        else:
            logging.error("Failed to upload metadata or no response received.")

    sys.exit(0)