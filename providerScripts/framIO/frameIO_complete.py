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
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-p","--proxy_directory", help="Proxy location to look for matching proxy file")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j","--jobId", help="Job Id of SDNA job")
    parser.add_argument("-cp", "--catalog-path", required=True, help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", required=True, help="Source path of file to look for original upload")
    parser.add_argument("-mp","--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl","--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-exts", "--extensions", help="Extensions to look for searching file")
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
    token = cloud_config_data['FrameIODevKey']
    project_id = cloud_config_data['project_id']
    print(f"project Id ----------------------->{project_id}")

    client = FrameioClient(token)

    matched_file = None
    file_name_for_url = ""

    if mode == "proxy":
        # -- File match via pattern for proxy
        search_dir = args.proxy_directory
        # Extract base name without extension for pattern
        source_basename = os.path.basename(args.source_path)
        source_name_no_ext = os.path.splitext(source_basename)[0]
        pattern = f"{source_name_no_ext}*.*"
        extensions = args.extensions.split(',') if args.extensions else []
        file_name_for_url = extract_file_name(args.catalog_path)
        matched_file = find_matching_file(search_dir, pattern, extensions)

    elif mode == "original":
        # -- File match via exact file name for original
        matched_file = args.source_path
        source_filename= matched_file.split("/").pop()
        file_name_for_url = extract_file_name(source_filename)

    if not matched_file:
        logging.error("No matching file found.")
        sys.exit(4)
    
    # Check for size limit for original file upload mode
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


    # Generate description link (StorageDNA URL)
    catalog_path = remove_file_name_from_path(matched_file)
    catalog_url = urllib.parse.quote(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    jobId = args.jobId
    client_ip, client_port = get_link_address_and_port()
    
    url = f"https://{client_ip}:{client_port}/dashboard/projects/{jobId}/browse&search?path={catalog_url}&filename={filename_enc}"

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

    # Find upload folder ID and upload file
    upload_path = args.upload_path
    up_id = find_upload_id(upload_path, project_id, token) if '/' in upload_path else upload_path
    asset_id = upload_file(client, matched_file, up_id, url)
    print(f"uploaded Asset ------------------------------------> {asset_id}")

    # Upload associated metadata file (if provided)
    meta_file = args.metadata_file
    if meta_file:
        response = update_asset(token, asset_id, meta_file)
        parsed = response.json()
        print(json.dumps(parsed, indent=4))

    sys.exit(0)
