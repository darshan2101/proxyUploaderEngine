import os
import sys
import json
import argparse
import logging
import urllib.parse
from json import dumps
from requests import request
from configparser import ConfigParser
import xml.etree.ElementTree as ET
from frameioclient import FrameioClient
import plistlib
from pathlib import Path

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
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

def upload_file(client, source_path, up_id, description):
    file_size = os.stat(source_path).st_size
    logging.info(f"Starting upload for file: {source_path} ({file_size/1024/1024:.2f} MB)")
    
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

def update_asset(token, asset_id, properties_file):
    logging.info(f"Updating asset {asset_id} with properties from {properties_file}")
    
    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        sys.exit(1)

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
                sys.exit(1)
                
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
    logging.debug("Sending asset update request to Frame.io API")
    response = request('PUT', f'https://api.frame.io/v2/assets/{asset_id}', headers=headers, data=dumps({'properties': props}))
    logging.info(f"Asset update completed with status code: {response.status_code}")
    return response

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--jobId", help="Job Id of SDNA job")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Pass if upload path is already resolved ID")

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
    if args.resolved_upload_id:
        up_id = upload_path
    else:
        up_id = find_upload_id(upload_path, project_id, token, args.parent_id)
    logging.info(f"Upload location ID: {up_id}")
    
    asset_id = upload_file(client, matched_file, up_id, url)
    logging.info(f"File uploaded successfully. Asset ID: {asset_id}")

    meta_file = args.metadata_file
    if meta_file:
        logging.info("Applying metadata to uploaded asset...")
        response = update_asset(token, asset_id, meta_file)
        parsed = response.json()
        logging.debug(json.dumps(parsed, indent=4))

    sys.exit(0)
