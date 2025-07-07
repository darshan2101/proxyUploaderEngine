import requests
import argparse
import sys
import os
import json
import hashlib
import logging
import urllib.parse
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
from boxsdk import OAuth2, Client
from boxsdk.object.metadata import Metadata
import time

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 5 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
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

def calculate_sha1(file_path,chunk_size):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha1.update(chunk)

    return sha1.digest()

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

def prepare_metadata_to_upload( backlink_url, properties_file):    
    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        sys.exit(1)

    metadata = {
        "fabric URL": backlink_url
    }
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
                sys.exit(1)     
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
        sys.exit(1)
    
    return metadata

def get_or_create_folder(client, folder_name, parent_id):
    start_time = time.time()
    logging.debug(f"[get_or_create_folder] Start: folder_name='{folder_name}', parent_id='{parent_id}'")
    try:
        parent_folder = client.folder(folder_id=parent_id)
        items = parent_folder.get_items(limit=1000, offset=0)
        for item in items:
            if item.name == folder_name and item.type == 'folder':
                elapsed = time.time() - start_time
                return item.id

        created_folder = parent_folder.create_subfolder(folder_name)
        logging.debug(f"[get_or_create_folder] Created folder: {created_folder}")
        elapsed = time.time() - start_time
        return created_folder.id
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"[get_or_create_folder] Error after {elapsed:.3f}s in get_or_create_folder('{folder_name}', '{parent_id}'): {e}", exc_info=True)
        raise

def ensure_path(client, path, base_id="0"):
    start_time = time.time()
    logging.debug(f"[ensure_path] Start: path='{path}', base_id='{base_id}'")
    try:
        current_folder_id = base_id
        last_folder_id = None
        segments = path.strip("/").split("/")
        logging.debug(f"[ensure_path] Path segments: {segments}")
        for idx, segment in enumerate(segments):
            if segment:
                logging.debug(f"[ensure_path] ({idx+1}/{len(segments)}) Ensuring folder '{segment}' exists under parent ID '{current_folder_id}'")
                last_folder_id = get_or_create_folder(client, segment, current_folder_id)
                current_folder_id = last_folder_id
        return last_folder_id
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"[ensure_path] Error after {elapsed:.3f}s in ensure_path('{path}', base_id='{base_id}'): {e}", exc_info=True)
        raise

def upload_file(client, folder_id, file_path):
    logging.debug(f"Starting upload for file: {file_path} to folder ID: {folder_id}")
    try:
        file_name = extract_file_name(file_path)
        file_size = os.path.getsize(file_path)
        folder = client.folder(folder_id).get()

        # Box API: Use upload session only for files >= 20 MB
        min_upload_session_size = 20000000  # 20 MB

        if file_size < min_upload_session_size:
            # Use simple upload for small files
            logging.debug(f"File size {file_size} bytes is less than {min_upload_session_size} bytes, using simple upload")
            uploaded_file = folder.upload(file_path)
            return {
                "success": True,
                "file_id": uploaded_file.id,
                "file_name": uploaded_file.name,
                "size": uploaded_file.size
            }

        # Use chunked upload session for large files
        upload_session = folder.create_upload_session(
            file_name=file_name,
            file_size=file_size
        )
        logging.debug(f"Created upload session for file: {file_name}, size: {file_size} bytes")
        chunk_size = upload_session.part_size
        parts = []
        offset = 0
        logging.debug(f"Starting upload in chunks of size: {chunk_size} bytes")
        with open(file_path, 'rb') as file_stream:
            while offset < file_size:
                bytes_to_read = min(chunk_size, file_size - offset)
                part_data = file_stream.read(bytes_to_read)
                part = upload_session.upload_part_bytes(
                    part_data,
                    offset=offset,
                    total_size=file_size
                )
                parts.append(part)
                offset += bytes_to_read

        content_sha1 = calculate_sha1(file_path, chunk_size)
        logging.debug(f"Calculated SHA1 for file: {file_name}, SHA1: {content_sha1.hex()}")
        uploaded_file = upload_session.commit(
            parts=parts,
            content_sha1=content_sha1
        )

        return {
            "success": True,
            "file_id": uploaded_file.id,
            "file_name": uploaded_file.name,
            "size": uploaded_file.size
        }

    except Exception as e:
        logging.error(f"Failed to upload file: {file_path}, Error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to Box ")
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
    
    logging.info(f"Starting Box upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    parsed_token = json.loads(cloud_config_data.get('token')) if 'token' in cloud_config_data else None
    print(f"Parsed token: {parsed_token}")

    oauth = OAuth2(
        client_id=cloud_config_data['client_id'],
        client_secret=cloud_config_data['client_secret'],
        access_token= parsed_token["accessToken"] if parsed_token else cloud_config_data['access_token'],
        refresh_token=parsed_token["refreshToken"] if parsed_token else cloud_config_data.get('refresh_token', None),
    )
    client = Client(oauth)
    
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
        up_id = ensure_path(client, args.upload_path) if '/' in upload_path else upload_path
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
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Box")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Box in {mode} mode")
    upload_path = args.upload_path
    
    #  upload file along with meatadata
    meta_file = args.metadata_file
    if meta_file:
        logging.info("Preparing metadata to be uploaded ...")
        metadata_obj = prepare_metadata_to_upload(backlink_url, meta_file)
        if metadata_obj is not None:
            parsed = metadata_obj
        else:
            parsed = None
            logging.error("Failed to find metadata .")

    # upload file
    if args.resolved_upload_id:
        folder_id = upload_path
    else:
        folder_id = ensure_path(client, args.upload_path)
    print(f"Resolved upload path to folder ID: {folder_id}")
      
    asset = upload_file(client, folder_id, args.source_path)
    if asset.get("success"):
        logging.info(f"File uploaded successfully: {args.source_path}")
        if parsed is not None:
            applied_metadata = client.file(asset.get("file_id")).metadata().create(parsed)
            print(f'Applied metadata in instance ID {applied_metadata["$id"]}')
            sys.exit(0)
        print(f"File uploaded successfully: {args.source_path} to folder ID: {folder_id}")
        sys.exit(0)
    else:
        logging.error(f"Failed to upload file: {args.source_path}, Error: {asset.get('error', 'Unknown error')}")
        sys.exit(1)