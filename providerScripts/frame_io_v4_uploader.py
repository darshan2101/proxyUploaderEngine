import requests
import os
import sys
import argparse
import logging
import plistlib
import urllib.parse
from configparser import ConfigParser

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
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

def get_root_asset_id(config_data):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/projects/{config_data['project_id']}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    response = requests.get(url, headers=headers)
    logger.debug(f"root asset fetch response :--------------------> {response.text}")
    if response.status_code not in (200, 201):
        logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    response = response.json()
    return response['data']['root_folder_id']

def get_folders_list(config_data, folder_id):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{folder_id}/children"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    logger.debug(f"URL :------------------------------------------->  {url}")
    response = requests.get(url, headers=headers)
    if response.status_code in (200, 201):
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
        return all_folders, response.status_code
    else:
        logger.error(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return response.text, response.status_code

def get_folders_content(config_data, asset_id, next_page_url = None, asset_data_list = None):
    if asset_data_list is None:
        asset_data_list = []
    if next_page_url is None:
        url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{asset_id}/children"
    else:
        url = f"{config_data['domain']}{next_page_url}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    logging.debug(f"URL: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200: 
        logging.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    response = response.json()
    asset_data_list.extend(response['data'])
    next_page = response['links']['next']
    if not next_page is None:
        get_folders_content(asset_id,next_page,asset_data_list)
    return asset_data_list

def remove_file(config_data, asset_id):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/files/{asset_id}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f"Bearer {config_data['token']}"
        }
    logging.debug(f"URL: {url}")
    response = requests.delete(url, headers=headers)
    if response.status_code != 204:
        logging.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
        return False
    return True

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
    response = requests.post(url, headers=headers,json=payload)
    logger.debug(f"RESPONSE: {response.text}")
    if response.status_code not in (200, 201):
        logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    response = response.json()
    return response['data']['id']

def find_upload_id(upload_path, config_data, base_id = None):
    current_parent_id = base_id or get_root_asset_id(config_data)

    folder_path = upload_path
    logging.info(f"Finding or creating folder path: '{folder_path}'")

    for segment in folder_path.strip("/").split("/"):
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")

        folders, response_code = get_folders_list(config_data, current_parent_id)
        if not isinstance(folders, list) or response_code not in (200, 201):
            print(f"Failed to get folders list: {folders}")
            sys.exit(1)
        matched = next((f for f in folders if f.get("name") == segment), None)

        if matched:
            current_parent_id = matched["id"]
            logging.info(f"Folder '{segment}' already exists (ID: {matched['id']}) under parent ID {current_parent_id}")
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data, segment, current_parent_id)
            logging.info(f"Created new folder '{segment}' under parent ID {current_parent_id}, new ID: {current_parent_id}")
            logging.debug(f"Created folder '{segment}' (ID: {current_parent_id})")

    logging.info(f"Final destination folder ID for upload: {current_parent_id}")
    return current_parent_id

def create_asset(config_data,folder_id,file_path):
    url = f"{config_data['domain']}/v4/accounts/{config_data['account_id']}/folders/{folder_id}/files/local_upload"
    file_name = extract_file_name(file_path)
    file_size = os.path.getsize(file_path)
    # remove if file already exists
    asset_list = get_folders_content(config_data, folder_id)
    existing_asset = next((asset for asset in asset_list if asset['name'] == file_name and int(asset['file_size']) == int(file_size) ), None)
    if existing_asset:
        logging.info(f"File '{file_name}' already exists in folder ID {folder_id}. Removing existing asset.")
        if remove_file(config_data, existing_asset['id']) == True:
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
        'Authorization': f"Bearer {cloud_config_data['token']}"
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in (200, 201):
        logging.info(f"Created asset '{file_name}' with size {os.path.getsize(file_path)} bytes in folder ID {folder_id}")
    else:
        logging.error(f"Failed to create asset for file '{file_path}'. Status: {response.status_code}, Response: {response.text}")
        logging.debug(f"Response error. Status - {response.status_code}, Error - {response.text}")
    return response, response.status_code
        
def upload_parts(file_path, asset_info):
    upload_urls = asset_info['data']['upload_urls']
    content_type = asset_info['data']['media_type']
    with open(file_path, "rb") as f:
        for i, part in enumerate(upload_urls):
            part_url = part["url"]
            part_size = part["size"]
            part_data = f.read(part_size)
            
            logging.debug(f"Uploading part {i + 1} to {part_url[:80]}...")
            logging.info(f"Uploading part {i + 1}/{len(upload_urls)} of file '{file_path}' to Frame.io")

            response = requests.put(
                part_url,
                data=part_data,
                headers={"Content-Type": content_type, "x-amz-acl": "private"}
            )

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
        # folders = get_folders_content(cloud_config_data, asset_id)
        # for folder in folders:
        #     if folder['type'] == 'folder':
        #         buckets.append(f"{folder['id']}:{folder['name']}")
        # print(','.join(buckets))
        response, response_code = get_folders_list(cloud_config_data, asset_id)

        if response_code not in (200, 201):
            logging.error(f"Failed to fetch folders. Status: {response_code}")
            sys.exit(1)
        print(f"Response of folder list -----> {response}")
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

    catalog_path = remove_file_name_from_path(matched_file)
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

    asset_info, creation_status = create_asset(cloud_config_data, folder_id, args.source_path)
    if creation_status not in (200, 201):
        print(f"Failed to create asset: {asset_info.text}")
        sys.exit(1)

    parts_info = upload_parts(args.source_path, asset_info.json())
    if parts_info["status_code"] not in (200, 201):
        print(f"Failed to upload file parts: {parts_info['detail']}")
        sys.exit(1)
    else:
        logging.info("All parts uploaded successfully to Frame.io")
        sys.exit(0)

