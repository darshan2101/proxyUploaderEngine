import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
from configparser import ConfigParser
import xml.etree.ElementTree as ET
from pathlib import Path

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

def prepare_metadata_to_upload(repo_guid ,relative_path, file_name, properties_file = None):    
    metadata = {
        "relativePath": relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url
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

def file_exists_in_index(api_key, index_id, filename, file_size):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos"
    headers = {"x-api-key": api_key}
    params = {
        "filename": filename,
        "size": file_size
    }
    video_id = None
    page = 1
    total_pages = 1

    logging.info(f"Searching for existing video: '{filename}' (Size: {file_size} bytes)")

    while page <= total_pages:
        params["page"] = page
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logging.error(f"Failed to list videos: {response.text}")
            return None

        data = response.json()
        results = data.get("data", [])
        for video in results:
            meta = video.get("system_metadata", {})
            if meta.get("filename") == filename and meta.get("size") == file_size:
                video_id = video["_id"]
                logging.info(f"Matching video found: ID={video_id}, filename='{filename}', size={file_size}")
                return video_id  # Return on first match

        page_info = data.get("page_info", {})
        total_pages = page_info.get("total_page", 1)
        page += 1

    return None  # No match found

def delete_video(api_key, index_id, video_id):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos/{video_id}"
    headers = {"x-api-key": api_key}
    response = requests.delete(url, headers=headers)
    if response.status_code in (200, 204):
        logging.info(f"✅ Deleted existing video ID: {video_id}")
        return True
    else:
        logging.error(f"❌ Failed to delete video {video_id}: {response.text}")
        return False

def upload_asset(api_key, index_id, file_path):
    try:
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        # Check and remove duplicate
        existing_video_id = file_exists_in_index(api_key, index_id, filename, file_size)
        if existing_video_id:
            logging.info(f"Duplicate video found for '{filename}'. Deleting...")
            if not delete_video(api_key, index_id, existing_video_id):
                logging.warning("Proceeding with upload despite failed deletion.")
        else:
            logging.info(f"No duplicate found for '{filename}'. Proceeding with upload.")

        url = "https://api.twelvelabs.io/v1.3/tasks"
        payload = {"index_id": index_id}
        headers = {"x-api-key": api_key}

        with open(file_path, 'rb') as f:
            files = {"video_file": f}
            logging.info(f"Uploading '{filename}' to Twelve Labs index {index_id}...")
            response = requests.post(url, data=payload, files=files, headers=headers)

        if response.status_code >= 400:
            logging.error(f"Upload failed with status {response.status_code}: {response.text}")
            raise RuntimeError(f"Upload failed: {response.text}")

        logging.debug(f"Upload response: {response.text}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error during upload: {e}")
        raise RuntimeError(f"Network error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in upload_asset: {e}")
        raise

def add_metadata(api_key, index_id, video_id, metadata):
    try:
        url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/videos/{video_id}"
        payload = {"user_metadata": metadata}
        headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }
        response = requests.put(url, json=payload, headers=headers)

        logging.debug(f"Metadata update status: {response.status_code}")

        if response.status_code == 204:
            logging.info("Metadata updated successfully (204 No Content).")
            return None  # No body to return
        elif response.status_code == 200:
            logging.info("Metadata updated successfully with response.")
            return response.json()
        else:
            raise RuntimeError(
                f"Metadata update failed with status {response.status_code}: {response.text}"
            )
    except Exception as e:
        raise RuntimeError(f"Failed to add metadata: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from") 
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded google Drive")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("-r", "--repo-guid",  help="repo GUID of provided file")  
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

    index_id = args.upload_path if args.resolved_upload_id else cloud_config_data.get('index_id', '')
    print(f"Index Id ----------------------->{index_id}")

    logging.info(f"Starting Twelve labs upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    
    logging.info(f"Initializing Twelve labs client")
    
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
        cache_path, relative_path = normalized_path.split("/1/")
        repo_guid = cache_path.split("/")[-1]        
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

    link_host = f"https://{client_ip}/dashboard/projects/{job_guid}"
    link_url = f"/browse&search?path={catalog_url}&filename={filename_enc}"
    backlink_url = f"{link_host}{link_url}"
    logging.debug(f"Generated dashboard URL: {backlink_url}")


    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")

        sys.exit(0)
        
    meta_file = args.metadata_file
    logging.info("Preparing metadata to be uploaded ...")
    metadata_obj = prepare_metadata_to_upload(args.repo_guid, catalog_url, filename_enc, backlink_url, meta_file)
    if metadata_obj is not None:
        parsed = metadata_obj
    else:
        parsed = None
        logging.error("Failed to find metadata .")

    logging.info(f"Starting upload process to Twelve labs")
    api_key = cloud_config_data.get("api_key")
    try:
        asset = upload_asset(api_key, index_id, args.source_path)
        logging.info(f"Asset uploaded to twelve labs. Asset ID: --> {asset['video_id']}")
        if "video_id" in asset:
            try:
                add_metadata(api_key, index_id, asset["video_id"], parsed)
                logging.info("File uploaded and metadata added successfully.")
            except Exception as e:
                logging.warning(f"Video uploaded, but metadata insertion failed: {e}")
                print("Video Uploaded, metadata insertion failed.")
            sys.exit(0)
        sys.exit(0)
    except Exception as e:
        print(str(e))
        sys.exit(1)
