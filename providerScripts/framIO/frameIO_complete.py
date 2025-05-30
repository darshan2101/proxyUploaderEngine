import os
import sys
import json
import argparse
import logging
import urllib.parse
from json import dumps
from requests import request
from configparser import ConfigParser
from frameioclient import FrameioClient
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

# Util: find file by pattern and extension
def find_matching_file(directory, pattern, extensions):
    logging.debug(f"Searching for file using pattern in: {directory}")
    try:
        base_path = Path(directory)
        if not base_path.exists():
            logging.error(f"Directory does not exist: {directory}")
            return None
        
        if not base_path.is_dir() or not os.access(directory, os.R_OK):
            logging.error(f"No read permission for directory: {directory}")
            return None

        for matched_file in base_path.rglob(pattern):
            if matched_file.is_file():
                if extensions and not any(matched_file.name.lower().endswith(ext.lower()) for ext in extensions):
                    logging.debug(f"File {matched_file.name} does not match extensions")
                    continue
                logging.debug(f"Found matching file: {str(matched_file)}")
                return str(matched_file)

        return None
    except PermissionError as e:
        logging.error(f"Permission denied accessing directory: {e}")
        return None
    except Exception as e:
        logging.error(f"Error during file search: {e}")
        return None

# Util: find exact file name recursively
def find_exact_file(directory, target_name):
    logging.debug(f"Searching for exact match: {target_name} in {directory}")
    for root, _, files in os.walk(directory):
        if target_name in files:
            return os.path.join(root, target_name)
    return None

# Util: extract base file name
def extract_file_name(path):
    return os.path.basename(path)

# Util: remove file name to get directory path
def remove_file_name_from_path(path):
    return os.path.dirname(path)

# Reads platform-specific DNA client config and returns cloud config path
def get_cloud_config_path():
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        return parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            return plistlib.load(fp)["CloudConfigFolder"] + "/cloud_targets.conf"

# Reads IP and port from DNA Server config
def get_link_address_and_port():
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

    return ip, port

# Creates folder structure in Frame.io project and returns upload folder ID
def find_upload_id(path, project_id, token):
    client = FrameioClient(token)
    current_id = client.projects.get(project_id)['root_asset_id']
    for segment in path.strip('/').split('/'):
        children = client.assets.get_children(current_id, type='folder')
        next_id = next((child['id'] for child in children if child['name'] == segment), None)
        if not next_id:
            next_id = client.assets.create(parent_asset_id=current_id, name=segment, type="folder", filesize=0)['id']
        current_id = next_id
    return current_id

# Uploads file to Frame.io folder
def upload_file(client, source_path, up_id, description):
    asset = client.assets.create(
        parent_asset_id=up_id,
        name=extract_file_name(source_path),
        type="file",
        description=description,
        filesize=os.stat(source_path).st_size
    )
    # parsed = response.json()
    # print(json.dumps(parsed, indent=4))
    with open(source_path, 'rb') as f:
        client.assets._upload(asset, f)
    return asset['id']

# Updates asset metadata using key,value pairs from file
def update_asset(token, asset_id, properties_file):
    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        sys.exit(1)

    props = {}

    try:
        if properties_file.endswith(".json"):
            with open(properties_file, 'r') as f:
                props = json.load(f)
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        props[key] = value
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")
        sys.exit(1)


    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = request('PUT', f'https://api.frame.io/v2/assets/{asset_id}', headers=headers, data=dumps({'properties': props}))
    return response

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source-filename", required=True, help="Source filename to look for")
    parser.add_argument("-c", "--config-filename", required=True, help="JSON config file path")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)

    # Load file_config.json
    if not os.path.exists(args.config_filename):
        logging.error(f"Config file not found: {args.config_filename}")
        sys.exit(1)

    with open(args.config_filename) as f:
        file_config = json.load(f)

    mode = file_config.get("mode")
    if mode not in VALID_MODES:
        logging.error(f"Only allowed modes: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Missing cloud config: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    cloud_config_name = file_config.get("cloud_config_name")
    if cloud_config_name not in cloud_config:
        logging.error(f"Missing cloud config section: {cloud_config_name}")
        sys.exit(1)

    cloud_config_data = cloud_config[cloud_config_name]
    token = cloud_config_data['FrameIODevKey']
    project_id = file_config.get("frameio_project_id") or cloud_config_data['project_id']
    print(f"project Id ----------------------->{project_id}")

    if file_config.get("frameio_project_id") and file_config.get("frameio_project_id") != cloud_config_data['project_id']:
        logging.error(f"Project ID mismatch. Config: {cloud_config_data['project_id']} vs Input: {file_config.get('frameio_project_id')}")
        sys.exit(1)

    client = FrameioClient(token)

    matched_file = None
    file_name_for_url = ""

    if mode == "proxy":
        # -- File match via pattern for proxy
        search_dir = file_config.get("proxy_directory")
        source_name = os.path.splitext(args.source_filename)[0]
        pattern = file_config.get("search_pattern", f"{args.source_filename}*").replace("{orig}", source_name)
        file_name_for_url = extract_file_name(file_config.get("catalog_path"))
        matched_file = find_matching_file(search_dir, pattern, file_config.get("extensions"))

    elif mode == "original":
        # -- File match via exact file name for original
        search_dir = file_config.get("source_directory")
        if not search_dir:
            logging.error("No Source Directory provided.")
            sys.exit(4)
        file_name_for_url = extract_file_name(args.source_filename)
        matched_file = find_exact_file(search_dir, args.source_filename)

    if not matched_file:
        logging.error("No matching file found.")
        sys.exit(4)
    
    # Check for size limit for original file upload mode
    matched_file_size = os.stat(matched_file).st_size
    file_size_limit = file_config.get("original_file_size_limit")
    if mode == "original" and file_size_limit:
        try:
            size_limit_bytes = float(file_size_limit) * 1024 * 1024
            if matched_file_size > size_limit_bytes:
                logging.error(f"File too large: {matched_file_size / (1024 * 1024):.2f} MB > limit of {file_size_limit} MB")
                sys.exit(4)
        except Exception as e:
            logging.warning(f"Could not validate size limit: {e}")


    # Generate description link (StorageDNA URL)
    catalog_path = remove_file_name_from_path(matched_file)
    catalog_url = urllib.parse.quote(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    jobId = file_config.get("jobId")
    client_ip, client_port = (
        file_config.get("link_ip_port", "").split(":")
        if file_config.get("link_ip_port") else get_link_address_and_port()
    )
    url = f"https://{client_ip}:{client_port}/dashboard/projects/{jobId}/browse&search?path={catalog_url}&filename={filename_enc}"

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {file_config.get('upload_path')} => Frame.io")
        if file_config.get("upload_properties"):
            meta_file = file_config.get("metadata_file") or file_config.get("property_file_path")
            if meta_file:
                logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
            else:
                logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    # Find upload folder ID and upload file
    upload_path = file_config.get("upload_path")
    up_id = find_upload_id(upload_path, project_id, token) if '/' in upload_path else upload_path
    asset_id = upload_file(client, matched_file, up_id, url)
    print(f"uploaded Asset ------------------------------------> {asset_id}")

    # Upload associated metadata file (if applicable)
    if file_config.get("upload_properties"):
        meta_file = file_config.get("metadata_file") or file_config.get("property_file_path")
        if not meta_file:
            logging.error("Metadata file path not provided.")
            sys.exit(1)
        response = update_asset(token, asset_id, meta_file)
        parsed = response.json()
        print(json.dumps(parsed, indent=4))

    sys.exit(0)
