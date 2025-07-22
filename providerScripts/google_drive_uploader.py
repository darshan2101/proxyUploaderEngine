import argparse
import sys
import os
import json
# from action_functions import *
import logging
import urllib.parse
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/drive']
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

def prepare_metadata_to_upload(link_host, link_url, properties_file=None):
    metadata = {
        "fabric_host": link_host,
        "filename": os.path.basename(link_url.split("filename=")[-1])
    }

    if not properties_file:
        logging.debug("No properties file provided, using default metadata.")
        return metadata

    if not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return metadata

    logging.debug(f"Reading properties from: {properties_file}")
    file_ext = properties_file.lower()
    try:
        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                loaded = json.load(f)
                metadata.update(loaded)
                logging.debug(f"Loaded JSON properties: {loaded}")

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
                return metadata
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

    return metadata

def get_or_create_folder(service, folder_name, parent_id):
    try:
        logging.debug(f"Searching for folder '{folder_name}' under parent ID '{parent_id}'")
        query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        logging.debug(f"Drive API list response: {results}")
        items = results.get('files', [])
        if items:
            logging.info(f"Found existing folder '{folder_name}' with ID '{items[0]['id']}'")
            return items[0]['id']
        # Create folder if not found
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        logging.info(f"Creating folder '{folder_name}' under parent ID '{parent_id}'")
        folder = service.files().create(body=file_metadata, fields='id').execute()
        logging.debug(f"Drive API create response: {folder}")
        folder_id = folder.get('id')
        if folder_id:
            logging.info(f"Created folder '{folder_name}' with ID '{folder_id}'")
        else:
            logging.error(f"Failed to get folder ID after creation for '{folder_name}'")
        return folder_id
    except Exception as e:
        logging.error(f"Error in get_or_create_folder('{folder_name}', '{parent_id}'): {e}", exc_info=True)
        raise

def ensure_path(token, path, base_id=None):
    try:
        if base_id is None:
            logging.warning("No base_id provided, defaulting to 'root'")
            base_id = "root"
        token_info = json.loads(token)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        service = build('drive', 'v3', credentials=creds)
        current_parent_id = base_id
        folder_id = None
        for folder in path.split('/'):
            if folder:
                logging.debug(f"Ensuring folder '{folder}' exists under parent ID '{current_parent_id}'")
                folder_id = get_or_create_folder(service, folder, current_parent_id)
                current_parent_id = folder_id
        logging.info(f"Resolved folder path '{path}' to folder ID '{folder_id}'")
        return folder_id
    except Exception as e:
        logging.error(f"Error in ensure_path('{path}', base_id='{base_id}'): {e}", exc_info=True)
        raise

def get_drive_service(token):
    creds = None
    token_info = json.loads(token)
    creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("Provided credentials are not valid and can't be refreshed.")
            exit(1)
 
    return build('drive', 'v3', credentials=creds)

def upload_file(token, file_path, folder_id,  metadata=None):
    try:
        service = get_drive_service(token)
        # Only replace spaces with underscores in metadata keys
        sanitized_metadata = None
        if metadata:
            sanitized_metadata = {str(k).replace(' ', '_'): str(v) for k, v in metadata.items()}

        file_metadata = {
            'name': os.path.basename(os.path.normpath(file_path)),
            'parents': [folder_id],
            'Source': sanitized_metadata['fabric_host'],
        }

        media = MediaFileUpload(file_path, resumable=True)

        file = service.files().create(body=file_metadata,media_body=media).execute()
        print(f"File uploaded successfully: {file}")
        return True
    except Exception as e:
        return str(e)



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
    # root_id = cloud_config_data.get("root_id", "root")
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
        folder_id = ensure_path(cloud_config_data["token"],  args.upload_path, base_id) if '/' in upload_path else upload_path
        print(folder_id)
        sys.exit(0)

    logging.info(f"Starting S3 upload process in {mode} mode")
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

    link_host = f"https://{client_ip}/dashboard/projects/{job_guid}"
    link_url = f"/browse&search?path={catalog_url}&filename={filename_enc}"
    backlink_url = f"{link_host}{link_url}"
    logging.debug(f"Generated dashboard URL: {backlink_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => google Drive")
        if "token" in cloud_config_data:
            resolved_folder_id = ensure_path(cloud_config_data["token"], args.upload_path, args.parent_id)
            logging.info(f"[DRY RUN] Resolved folder ID for upload path '{args.upload_path}': {resolved_folder_id}")
        else:
            logging.warning("[DRY RUN] Token missing from config, skipping folder resolution.")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Google Drive in {mode} mode")
    upload_path = args.upload_path
    
    #  upload file along with meatadata
    meta_file = args.metadata_file
    if meta_file:
        logging.info("Preparing metadata to be uploaded ...")
        metadata_obj = prepare_metadata_to_upload(link_host, link_url, meta_file)
        if metadata_obj is not None:
            parsed = metadata_obj
        else:
            parsed = None
            logging.error("Failed to find metadata .")
    else:
        metadata_obj = prepare_metadata_to_upload(link_host, link_url)
        parsed = metadata_obj
    
    #  upload file
    if args.resolved_upload_id:
        folder_id = upload_path
    else:
        folder_id = ensure_path(cloud_config_data["token"], args.upload_path)
    logging.info(f"Upload location ID: {folder_id}")
    
    response = upload_file(cloud_config_data["token"], args.source_path, folder_id, parsed)
    if response == True:
        print("File uploaded successfully")
        sys.exit(0)
    else:
        print(f"Upload failed, error details: {response}")
        sys.exit(1)
    