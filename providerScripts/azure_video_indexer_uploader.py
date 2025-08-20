import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
import requests
import plistlib
from configparser import ConfigParser
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

# =============================
# Constants
# =============================

VALID_MODES = [
    "proxy", "original", "get_base_target",
    "generate_video_proxy", "generate_video_frame_proxy",
    "generate_intelligence_proxy", "generate_video_to_spritesheet"
]

# Azure Video Indexer API Constants
API_VERSION = "2024-01-01"
API_ENDPOINT = "https://api.videoindexer.ai"
AZURE_RESOURCE_MANAGER = "https://management.azure.com"

# Platform detection
IS_LINUX = True  # Change if needed
LINUX_CONFIG_PATH = r"D:\Dev\(python +node) Wrappers\proxyUploaderEngine\extra\Test\DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if IS_LINUX else "/Library/Preferences/com.storagedna.Servers.plist"
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# =============================
# Helper Functions
# =============================

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def get_link_address_and_port():
    """Read link_address and link_port from Servers.conf or plist"""
    logger.debug(f"Reading server configuration from: {SERVERS_CONF_PATH}")
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
        logger.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)
    logger.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_cloud_config_path():
    """Get path to cloud_targets.conf"""
    logger.debug("Determining cloud config path based on platform")
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            data = plistlib.load(fp)
            path = data.get("CloudConfigFolder", "") + "/cloud_targets.conf"
    logger.info(f"Using cloud config path: {path}")
    return path

def parse_metadata_file(properties_file):
    """Parse metadata from .json, .xml, or .csv/.txt (key,value) file"""
    props = {}
    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Metadata file not found or invalid: {properties_file}")
        return props

    logging.debug(f"Parsing metadata from: {properties_file}")
    try:
        ext = properties_file.lower()
        if ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                props = json.load(f)
                logging.debug(f"Loaded JSON metadata: {props}")

        elif ext.endswith(".xml"):
            import xml.etree.ElementTree as ET
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        props[key] = value
                logging.debug(f"Loaded XML metadata: {props}")
            else:
                logging.warning("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',', 1)  # Split on first comma only
                    if len(parts) == 2:
                        k, v = parts[0].strip(), parts[1].strip()
                        props[k] = v
            logging.debug(f"Loaded CSV/TEXT metadata: {props}")

    except Exception as e:
        logging.error(f"Failed to parse metadata file '{properties_file}': {e}")
        return {}

    return props

# =============================
# Dataclass: Consts
# =============================

@dataclass
class Consts:
    SubscriptionId: str
    ResourceGroup: str
    AccountName: str
    tenant_id: str
    client_id: str
    client_secret: str
    location: str = "trial"

    def __post_init__(self):
        required = ["SubscriptionId", "ResourceGroup", "AccountName", "tenant_id", "client_id", "client_secret"]
        for field in required:
            if not getattr(self, field):
                raise ValueError(f"Missing required field: {field}")


# =============================
# Token Provider Functions
# =============================

def get_arm_access_token(consts: Consts) -> str:
    """Get ARM token using client credentials"""
    url = f"https://login.microsoftonline.com/{consts.tenant_id}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": consts.client_id,
        "client_secret": consts.client_secret,
        "scope": f"{AZURE_RESOURCE_MANAGER}/.default",
        "grant_type": "client_credentials"
    }
    resp = requests.post(url, data=data, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_account_access_token_async(consts: Consts, arm_access_token: str, permission_type='Contributor', scope='Account', video_id=None) -> str:
    """Get Video Indexer account access token"""
    url = (
        f"{AZURE_RESOURCE_MANAGER}/subscriptions/{consts.SubscriptionId}"
        f"/resourceGroups/{consts.ResourceGroup}/providers/Microsoft.VideoIndexer/accounts/{consts.AccountName}"
        f"/generateAccessToken?api-version={API_VERSION}"
    )
    headers = {
        "Authorization": f"Bearer {arm_access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "permissionType": permission_type,
        "scope": scope
    }
    if video_id:
        payload["videoId"] = video_id

    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    return resp.json()["accessToken"]


# =============================
# VideoIndexerClient Class
# =============================

class VideoIndexerClient:
    def __init__(self, consts: Consts):
        self.consts = consts
        self.arm_access_token = ""
        self.vi_access_token = ""
        self.account = None

    def authenticate(self):
        """Authenticate and get tokens"""
        logger.info("Authenticating with Azure...")
        self.arm_access_token = get_arm_access_token(self.consts)
        self.vi_access_token = get_account_access_token_async(self.consts, self.arm_access_token)
        logger.info("Authentication successful.")

    def get_account(self):
        """Get account details (accountId, location)"""
        if self.account:
            return self.account

        headers = {"Authorization": f"Bearer {self.arm_access_token}"}
        url = (
            f"{AZURE_RESOURCE_MANAGER}/subscriptions/{self.consts.SubscriptionId}"
            f"/resourceGroups/{self.consts.ResourceGroup}/providers/Microsoft.VideoIndexer/accounts/{self.consts.AccountName}"
            f"?api-version={API_VERSION}"
        )
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        self.account = resp.json()
        logger.info(f"[Account] ID: {self.account['properties']['accountId']}, Location: {self.account['location']}")
        return self.account

    def file_upload_async(
        self,
        media_path: str,
        video_name: Optional[str] = None,
        description: str = "",
        privacy: str = "Private",
        partition: str = "",
        excluded_ai: Optional[list] = None,
        metadata: Optional[dict] = None
    ) -> str:
        if excluded_ai is None:
            excluded_ai = []
        if metadata is None:
            metadata = {}

        if not os.path.exists(media_path):
            raise FileNotFoundError(f"Media file not found: {media_path}")

        if video_name is None:
            video_name = os.path.splitext(os.path.basename(media_path))[0]

        self.get_account()

        url = (
            f"{API_ENDPOINT}/{self.account['location']}/"
            f"Accounts/{self.account['properties']['accountId']}/Videos"
        )

        # query parameters
        params = {
            "accessToken": self.vi_access_token,
            "name": video_name[:80],
            "description": description,
            "privacy": privacy,
            "partition": partition
        }

        if excluded_ai:
            params["excludedAI"] = excluded_ai

        # Only add metadata if not empty
        if metadata:
            params["metadata"] = json.dumps(metadata, ensure_ascii=False)

        logger.info(f"Uploading '{video_name}'...")
        with open(media_path, 'rb') as f:
            files = {'file': (video_name, f, 'video/mp4')}
            response = requests.post(url, params=params, files=files)

        response.raise_for_status()
        video_id = response.json().get("id")
        logger.info(f"✅ Upload successful. Video ID: {video_id}")
        return video_id
    
    def wait_for_index_async(self, video_id: str, timeout_sec: int = 14400) -> str:
        """Poll until video is processed or failed"""
        self.get_account()

        url = (
            f"{API_ENDPOINT}/{self.account['location']}/Accounts/{self.account['properties']['accountId']}"
            f"/Videos/{video_id}/Index"
        )
        params = {"accessToken": self.vi_access_token}

        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < timeout_sec:
            try:
                resp = requests.get(url, params=params)
                if resp.status_code == 401:  # Token expired
                    logger.warning("Token expired. Refreshing...")
                    self.vi_access_token = get_account_access_token_async(self.consts, self.arm_access_token)
                    params["accessToken"] = self.vi_access_token
                    resp = requests.get(url, params=params)

                resp.raise_for_status()
                result = resp.json()
                state = result.get("state", "Unknown")

                if state == "Processed":
                    logger.info(f"✅ Video indexed successfully: {video_id}")
                    return "Processed"
                elif state == "Failed":
                    error = result.get("error", "No error message")
                    logger.error(f"❌ Indexing failed: {error}")
                    return "Failed"

                logger.info(f"🟡 Indexing... State: {state}")
                time.sleep(10)

            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                time.sleep(10)

        logger.error("⏰ Polling timeout reached.")
        return "Timeout"


# =============================
# Main Execution
# =============================

if __name__ == "__main__":
    import time  # Import here to avoid top-level conflict

    parser = argparse.ArgumentParser(description="Upload video to Azure Video Indexer")
    parser.add_argument("-m", "--mode", required=True, help="Operation mode")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata file path")
    parser.add_argument("-up", "--upload-path", help="Upload path (ignored if --resolved-upload-id)")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Use upload-path as account ID")
    parser.add_argument("-j", "--job-guid", help="SDNA Job GUID")
    parser.add_argument("--controller-address", help="Override Link IP:Port")
    parser.add_argument("--parent-id", help="Parent folder ID (unused)")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("-r", "--repo-guid", help="Override repo GUID")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--log-level", default="info", help="Log level")

    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    # Validate mode
    if args.mode not in VALID_MODES:
        logger.error(f"Invalid mode. Use: {VALID_MODES}")
        sys.exit(1)

    # Load cloud config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logger.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logger.error(f"Config section not found: {args.config_name}")
        sys.exit(1)

    section = cloud_config[args.config_name]
    consts = Consts(
        SubscriptionId=section.get("subscription_id"),
        ResourceGroup=section.get("resource_group"),
        AccountName=section.get("account_name"),
        tenant_id=section.get("tenant_id"),
        client_id=section.get("client_id"),
        client_secret=section.get("client_secret"),
        location=section.get("location", "trial")
    )

    # File and path setup
    if not args.source_path or not os.path.exists(args.source_path):
        logger.error(f"Source file not found: {args.source_path}")
        sys.exit(4)

    file_size = os.path.getsize(args.source_path)
    if args.size_limit:
        limit = float(args.size_limit) * 1024 * 1024
        if file_size > limit:
            logger.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
            sys.exit(4)

    file_name = extract_file_name(args.source_path) if args.mode == "original" else extract_file_name(args.catalog_path)
    catalog_dir = remove_file_name_from_path(args.catalog_path or args.source_path)
    normalized_path = catalog_dir.replace("\\", "/")
    relative_path = normalized_path.split("/1/", 1)[-1] if "/1/" in normalized_path else normalized_path
    repo_guid = args.repo_guid or (normalized_path.split("/1/")[0].split("/")[-1] if "/1/" in normalized_path else "unknown")

    # Backlink URL
    job_guid = args.job_guid or "unknown"
    client_ip, _ = (args.controller_address.split(":", 1) if args.controller_address and ":" in args.controller_address
                    else get_link_address_and_port())
    backlink_url = (
        f"https://{client_ip}/dashboard/projects/{job_guid}"
        f"/browse&search?path={urllib.parse.quote(relative_path)}"
        f"&filename={urllib.parse.quote(file_name)}"
    )

    # Prepare metadata
    metadata = {
        "relativePath": "/" + relative_path if not relative_path.startswith("/") else relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric_size": file_size
    }

    if args.metadata_file and os.path.exists(args.metadata_file):
        additional_metadata = parse_metadata_file(args.metadata_file)
        if additional_metadata:
            metadata.update(additional_metadata)

    if args.dry_run:
        logger.info("[DRY RUN] Upload skipped.")
        logger.info(f"[DRY RUN] File: {args.source_path}")
        logger.info(f"[DRY RUN] Metadata: {metadata}")
        sys.exit(0)

    try:
        # client Initialization
        client = VideoIndexerClient(consts)
        client.authenticate()

        # Upload
        partition = metadata.pop("partition", repo_guid)
        description = backlink_url

        video_id = client.file_upload_async(
            media_path=args.source_path,
            video_name=file_name,
            description=description,
            privacy="Private",
            partition=partition,
            metadata=metadata
        )

        # Poll
        status = client.wait_for_index_async(video_id)
        if status == "Processed":
            print(f"VideoID={video_id}")
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Upload failed: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)