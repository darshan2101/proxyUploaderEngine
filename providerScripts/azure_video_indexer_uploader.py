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
LINUX_CONFIG_PATH = r"D:\Dev\(python +node) Wrappers\proxyUploaderEngine\extra\Test\DNAClientServices.conf" # "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"


# Detect platform
IS_LINUX = True # os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logger = logging.getLogger()

# Azure Video Indexer base URL
AZURE_API_URL = "https://api.videoindexer.ai"

# Helper functions
def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")

def get_link_address_and_port():
    """Read link_address and link_port from Servers.conf or plist"""
    logging.debug(f"Reading server configuration from: {SERVERS_CONF_PATH}")
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH, 'r') as f:
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
            # Simplified plist parsing (XML)
            content = ''.join(lines)
            if '<key>link_address</key>' in content:
                start = content.find('<key>link_address</key>') + len('<key>link_address</key>')
                next_key = content.find('<key>', start)
                ip = content[start:next_key].split('>')[1].split('<')[0].strip()
            if '<key>link_port</key>' in content:
                start = content.find('<key>link_port</key>') + len('<key>link_port</key>')
                next_key = content.find('<key>', start)
                port = content[start:next_key].split('>')[1].split('<')[0].strip()
    except Exception as e:
        logging.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)

    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_cloud_config_path():
    """Get path to cloud_targets.conf"""
    logging.debug("Determining cloud config path based on platform")
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            data = plistlib.load(fp)
            path = data.get("CloudConfigFolder", "") + "/cloud_targets.conf"
    logging.info(f"Using cloud config path: {path}")
    return path

def prepare_metadata_to_upload(repo_guid, relative_path, file_name, file_size, backlink_url, properties_file=None):
    """Prepare metadata dictionary from file or defaults"""
    metadata = {
        "relativePath": relative_path if relative_path.startswith("/") else "/" + relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url,
        "fabric_size": file_size
    }

    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Properties file not found or invalid: {properties_file}")
        return metadata

    logging.debug(f"Reading properties from: {properties_file}")
    ext = properties_file.lower()

    try:
        if ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                user_meta = json.load(f)
                metadata.update(user_meta)
        elif ext.endswith(".xml"):
            import xml.etree.ElementTree as ET
            tree = ET.parse(properties_file)
            root = tree.getroot()
            for data in root.findall(".//data"):
                key = data.get("name")
                value = data.text.strip() if data.text else ""
                if key:
                    metadata[key] = value
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        k, v = parts[0].strip(), parts[1].strip()
                        metadata[k] = v
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")

    return metadata

def get_azure_access_token(location, account_id, api_key):
    """Get OAuth token from Azure Video Indexer"""
    url = f"{AZURE_API_URL}/auth/{location}/Accounts/{account_id}/AccessToken"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to get access token: {response.text}")
    return response.json()

def upload_to_azure_video_indexer(access_token, account_id, video_path, video_name, metadata, location="trial"):
    """Upload video with metadata to Azure Video Indexer"""
    url = f"{AZURE_API_URL}/videos"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # Prepare metadata as JSON string
    metadata_str = json.dumps(metadata)

    # Prepare query parameters
    params = {
        "name": video_name,
        "privacy": "Private",  # or "Public"
        "partition": "default",
        "videoUrl": "",  # Not used when uploading file
        "description": f"Uploaded from SDNA: {video_name}",
        "metadata": metadata_str,
        "accountId": account_id,
        "location": location
    }

    # Read video file
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video file not found: {video_path}")

    with open(video_path, 'rb') as f:
        files = {'file': (video_name, f, 'video/mp4')}
        logging.info(f"Uploading '{video_name}' to Azure Video Indexer...")
        response = requests.post(url, params=params, headers=headers, files=files)

    if response.status_code != 200:
        raise RuntimeError(f"Upload failed: {response.status_code} - {response.text}")

    result = response.json()
    video_id = result.get("id")
    logging.info(f"✅ Video uploaded successfully. Video ID: {video_id}")
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Upload video to Azure Video Indexer with metadata")
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-j", "--job-guid", help="SDNA Job GUID")
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata properties file")
    parser.add_argument("-up", "--upload-path", required=True, help="Azure Account ID (used as index_id equivalent)")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Treat upload-path as Account ID")
    parser.add_argument("--controller-address", help="Link IP:Port override")

    args = parser.parse_args()
    setup_logging(args.log_level)

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(1)

    # Resolve cloud config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Cloud config section not found: {args.config_name}")
        sys.exit(1)

    config = cloud_config[args.config_name]
    azure_account_id = args.upload_path if args.resolved_upload_id else config.get('azure_account_id')
    azure_api_key = config.get('azure_api_key')
    azure_location = config.get('azure_location', 'trial')  # e.g., 'trial', 'westus2'

    if not azure_account_id or not azure_api_key:
        logging.error("Missing Azure credentials (azure_account_id or azure_api_key) in config")
        sys.exit(1)

    logging.info(f"Using Azure Account ID: {azure_account_id} at location: {azure_location}")

    # File and path setup
    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"Source file not found: {matched_file}")
        sys.exit(4)

    file_size = os.path.getsize(matched_file)
    if args.size_limit:
        limit_mb = float(args.size_limit)
        if file_size > limit_mb * 1024 * 1024:
            logging.error(f"File too large: {file_size / (1024*1024):.2f} MB > {limit_mb} MB")
            sys.exit(4)

    file_name = extract_file_name(matched_file)
    catalog_dir = remove_file_name_from_path(args.catalog_path or matched_file)
    normalized_path = catalog_dir.replace("\\", "/")

    # Extract relative path and repoGuid
    if "/1/" in normalized_path:
        cache_path, relative_path = normalized_path.split("/1/", 1)
        repo_guid = cache_path.split("/")[-1]
    else:
        relative_path = normalized_path
        repo_guid = args.repo_guid or "unknown"

    # Generate backlink URL
    job_guid = args.job_guid or "unknown"
    if args.controller_address and ":" in args.controller_address:
        client_ip, client_port = args.controller_address.split(":", 1)
    else:
        client_ip, client_port = get_link_address_and_port()

    link_host = f"https://{client_ip}/dashboard/projects/{job_guid}"
    catalog_url = urllib.parse.quote(relative_path)
    filename_enc = urllib.parse.quote(file_name)
    backlink_url = f"{link_host}/browse&search?path={catalog_url}&filename={filename_enc}"

    logging.debug(f"Generated backlink: {backlink_url}")

    # Prepare metadata
    metadata = prepare_metadata_to_upload(
        repo_guid=repo_guid,
        relative_path=relative_path,
        file_name=file_name,
        file_size=file_size,
        backlink_url=backlink_url,
        properties_file=args.metadata_file
    )

    logging.info("Prepared metadata:")
    for k, v in metadata.items():
        logging.debug(f"  {k}: {v}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] Would upload: {matched_file}")
        sys.exit(0)

    try:
        # Get access token
        logging.info("Fetching Azure Video Indexer access token...")
        token_response = get_azure_access_token(azure_location, azure_account_id, azure_api_key)
        access_token = token_response  # It's just a string token

        # Upload video
        result = upload_to_azure_video_indexer(
            access_token=access_token,
            account_id=azure_account_id,
            video_path=matched_file,
            video_name=file_name,
            metadata=metadata,
            location=azure_location
        )

        video_id = result.get("id")
        logging.info(f"🎉 Upload complete! Video ID: {video_id}")
        print(f"VideoID={video_id}")

        sys.exit(0)

    except Exception as e:
        logging.error(f"❌ Upload failed: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)