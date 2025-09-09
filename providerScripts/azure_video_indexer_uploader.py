import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
import time
import random
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import plistlib
from configparser import ConfigParser
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Tuple

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
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite", "reindex"]
DEFAULT_CONFLICT_RESOLUTION = "skip"

# Detect platform
IS_LINUX = True # os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logger = logging.getLogger()
def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")


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

def get_retry_session(retries=3, backoff_factor_range=(1.0, 2.0)):
    session = requests.Session()
    # handling retry delays manually via backoff in the range
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):  # Max 3 attempts
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:  # Only sleep if not last attempt
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])  # e.g., 1-2, 3-4, 10-15
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. "
                                f"Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None  # Should not reach here

def get_node_api_key():
    api_key = ""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            api_key = parser.get('General', 'NodeAPIKey', fallback='')
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                api_key = plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

    if not api_key:
        logging.error("Node API key not found in configuration.")
        sys.exit(5)

    logging.info("Successfully retrieved Node API key.")
    return api_key

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
    url = f"https://login.microsoftonline.com/{consts.tenant_id}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": consts.client_id,
        "client_secret": consts.client_secret,
        "scope": f"{AZURE_RESOURCE_MANAGER}/.default",
        "grant_type": "client_credentials"
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, data=data)
            if response and response.status_code == 200:
                return response.json()["access_token"]
            else:
                if attempt == 2:
                    raise RuntimeError(f"ARM token fetch failed: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logger.critical(f"Failed to get ARM token after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    raise RuntimeError("get_arm_access_token failed")

def get_account_access_token_async(consts: Consts, arm_access_token: str, permission_type='Contributor', scope='Account', video_id=None) -> str:
    url = (
        f"{AZURE_RESOURCE_MANAGER}/subscriptions/{consts.SubscriptionId}"
        f"/resourceGroups/{consts.ResourceGroup}/providers/Microsoft.VideoIndexer/accounts/{consts.AccountName}"
        f"/generateAccessToken?api-version={API_VERSION}"
    )
    headers = {
        "Authorization": f"Bearer {arm_access_token}",
        "Content-Type": "application/json"
    }
    payload = {"permissionType": permission_type, "scope": scope}
    if video_id:
        payload["videoId"] = video_id

    logger.debug(f"URL :------------------------------------------->  {url}")
    logger.debug(f"Payload -------------------------> {payload}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers)
            if response and response.status_code == 200:
                return response.json()["accessToken"]
            else:
                if attempt == 2:
                    raise RuntimeError(f"VI token fetch failed: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logger.critical(f"Failed to get VI token after 3 attempts: {e}")
                raise
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    raise RuntimeError("get_account_access_token_async failed")

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
        if self.account:
            return self.account

        headers = {"Authorization": f"Bearer {self.arm_access_token}"}
        url = (
            f"{AZURE_RESOURCE_MANAGER}/subscriptions/{self.consts.SubscriptionId}"
            f"/resourceGroups/{self.consts.ResourceGroup}/providers/Microsoft.VideoIndexer/accounts/{self.consts.AccountName}"
            f"?api-version={API_VERSION}"
        )
        logger.debug(f"URL :------------------------------------------->  {url}")

        for attempt in range(3):
            try:
                response = make_request_with_retries("GET", url, headers=headers)
                if response and response.status_code == 200:
                    self.account = response.json()
                    logger.info(f"[Account] ID: {self.account['properties']['accountId']}, Location: {self.account['location']}")
                    return self.account
                else:
                    if attempt == 2:
                        raise RuntimeError(f"Account fetch failed: {response.status_code} {response.text}")
            except Exception as e:
                if attempt == 2:
                    logger.critical(f"Failed to get account after 3 attempts: {e}")
                    raise
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        raise RuntimeError("get_account failed")

    def list_videos(self, partition: str = None) -> list:
        self.get_account()

        videos = []
        skip = 0
        page_size = 1000  # Max allowed
        location = self.account['location']
        account_id = self.account['properties']['accountId']

        logger.info(f"Fetching videos from Azure Video Indexer (Partition: {partition or 'All'})")

        while True:
            url = f"{API_ENDPOINT}/{location}/Accounts/{account_id}/Videos"
            params = {
                "accessToken": self.vi_access_token,
                "pageSize": page_size,
                "skip": skip
            }
            if partition:
                params["partitions"] = json.dumps([partition])

            logger.debug(f"URL :------------------------------------------->  {url}")
            logger.debug(f"Query params -------------------------> {params}")

            # Outer retry loop (3 attempts per page)
            response = None
            for attempt in range(3):
                try:
                    response = make_request_with_retries("GET", url, params=params)
                    if response and response.status_code == 200:
                        break
                    else:
                        if attempt == 2:
                            if response:
                                raise RuntimeError(f"List videos failed: {response.status_code} {response.text}")
                            else:
                                raise RuntimeError("No response from server")
                except Exception as e:
                    if attempt == 2:
                        logger.critical(f"Failed to fetch page (skip={skip}) after 3 attempts: {e}")
                        raise
                    base_delay = [1, 3, 10][attempt]
                    delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                    time.sleep(delay)

            if not response or response.status_code != 200:
                logger.error("Final response failed")
                raise RuntimeError("Failed to fetch video list")

            data = response.json()
            page_results = data.get("results", [])
            videos.extend(page_results)
            logger.debug(f"Fetched {len(page_results)} videos (skip={skip}, total so far={len(videos)})")

            # Check for nextPage
            next_page = data.get("nextPage", {})
            if next_page.get("done", True):
                break

            skip = next_page.get("skip", skip + page_size)
            # Optional: Validate consistency
            if skip <= 0 or skip >= next_page.get("totalCount", 0):
                break

        return videos

    def delete_video(self, video_id: str) -> bool:
        self.get_account()
        url = (
            f"{API_ENDPOINT}/{self.account['location']}/"
            f"Accounts/{self.account['properties']['accountId']}/Videos/{video_id}"
        )
        params = {"accessToken": self.vi_access_token}
        logger.debug(f"URL :------------------------------------------->  {url}")

        for attempt in range(3):
            try:
                response = make_request_with_retries("DELETE", url, params=params)
                if response and response.status_code in (200, 204):
                    logger.info(f"✅ Deleted existing video: {video_id}")
                    return True
                elif response and response.status_code == 404:
                    logger.info(f"Video {video_id} not found (already deleted)")
                    return True
                else:
                    if attempt == 2:
                        logger.error(f"Delete failed: {response.status_code} {response.text}")
                        return False
            except Exception as e:
                if attempt == 2:
                    logger.critical(f"Failed to delete video {video_id}: {e}")
                    return False
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        return False

    def reindex_video(self, video_id: str) -> bool:
        self.get_account()
        url = (
            f"{API_ENDPOINT}/{self.account['location']}/"
            f"Accounts/{self.account['properties']['accountId']}/Videos/{video_id}/ReIndex"
        )
        params = {"accessToken": self.vi_access_token, "sendSuccessEmail": "false"}
        logger.debug(f"URL :------------------------------------------->  {url}")

        for attempt in range(3):
            try:
                response = make_request_with_retries("PUT", url, params=params)
                if response and response.status_code in (200, 204):
                    logger.info(f"🔄 Reindex triggered for video: {video_id}")
                    return True
                else:
                    if attempt == 2:
                        logger.error(f"Reindex failed: {response.status_code} {response.text}")
                        return False
            except Exception as e:
                if attempt == 2:
                    logger.critical(f"Failed to reindex video {video_id}: {e}")
                    return False
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        return False

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
            params["excludedAI"] = ",".join(excluded_ai)

        if metadata:
            params["metadata"] = json.dumps(metadata, ensure_ascii=False)

        logger.info(f"Uploading '{video_name}'...")
        logger.debug(f"URL :------------------------------------------->  {url}")
        logger.debug(f"Query params -------------------------> {params}")

        for attempt in range(3):
            try:
                with open(media_path, 'rb') as f:
                    files = {'file': (video_name, f, 'video/mp4')}
                    response = requests.post(url, params=params, files=files)

                if response.status_code in (200, 201):
                    video_id = response.json().get("id")
                    logger.info(f"✅ Upload successful. Video ID: {video_id}")
                    return video_id
                elif 400 <= response.status_code < 500:
                    error_msg = f"Client error: {response.status_code} {response.text}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                else:
                    logger.warning(f"Server error {response.status_code}: {response.text}")
                    if attempt == 2:
                        raise RuntimeError(f"Upload failed after 3 attempts: {response.text}")

            except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError, OSError) as e:
                logger.warning(f"Network error on attempt {attempt + 1}: {e}")
            except Exception as e:
                if "Client error" in str(e):
                    raise
                logger.warning(f"Transient error on attempt {attempt + 1}: {e}")

            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
            else:
                logger.critical(f"Upload failed after 3 attempts: {video_name}")
                raise RuntimeError("Upload failed after retries.")

        raise RuntimeError("file_upload_async failed")

    def wait_for_index_async(self, video_id: str, timeout_sec: int = 14400) -> str:
        self.get_account()

        url = (
            f"{API_ENDPOINT}/{self.account['location']}/Accounts/{self.account['properties']['accountId']}"
            f"/Videos/{video_id}/Index"
        )
        params = {"accessToken": self.vi_access_token}

        logger.debug(f"URL :------------------------------------------->  {url}")
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < timeout_sec:
            attempt_counter = 0
            try:
                response = make_request_with_retries("GET", url, params=params)
                if not response or response.status_code != 200:
                    if response and response.status_code == 401:
                        logger.warning("Token expired. Refreshing...")
                        self.vi_access_token = get_account_access_token_async(self.consts, self.arm_access_token)
                        params["accessToken"] = self.vi_access_token
                        continue
                    if attempt_counter < 2:
                        attempt_counter += 1
                        delay = [1, 3, 10][attempt_counter-1] + random.uniform(0, [1, 1, 5][attempt_counter-1])
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"Index check failed: {response.status_code} {response.text}")
                        return "Failed"

                result = response.json()
                state = result.get("state", "Unknown")
                video_data = result.get("videos", [{}])[0]
                progress = video_data.get("processingProgress", "0%")

                if state == "Processed":
                    logger.info(f"✅ Video indexed successfully: {video_id}")
                    return "Processed"
                elif state == "Failed":
                    error = result.get("error", "No error message")
                    logger.error(f"❌ Indexing failed: {error}")
                    return "Failed"

                logger.info(f"🟡 Indexing... State: {state}, progress: {progress}")
                time.sleep(10)

            except Exception as e:
                if attempt_counter < 2:
                    attempt_counter += 1
                    delay = [1, 3, 10][attempt_counter-1] + random.uniform(0, [1, 1, 5][attempt_counter-1])
                    time.sleep(delay)
                    continue
                logger.warning(f"Polling error: {e}")

        logger.error("⏰ Polling timeout reached.")
        return "Timeout"

    def resolve_conflict_and_upload(
        self,
        media_path: str,
        conflict_mode: str,
        video_name: Optional[str] = None,
        **upload_kwargs
    ) -> Tuple[str, bool]:

        if conflict_mode not in CONFLICT_RESOLUTION_MODES:
            raise ValueError(f"Invalid conflict_mode: {conflict_mode}. Use: {CONFLICT_RESOLUTION_MODES}")

        file_name = extract_file_name(media_path)
        base_name = os.path.splitext(file_name)[0]
        file_size = os.path.getsize(media_path)
        
        metadata = upload_kwargs.get("metadata", {})
        partition = metadata.get("repoGuid")
        if not partition:
            logger.warning("No 'repoGuid' found in metadata. Falling back to unfiltered search.")

        videos = self.list_videos(partition=partition)
        matching_video = next(
            (v for v in videos if v["name"] == base_name and v.get("sourceFiles", [{}])[0].get("size") == file_size),
            None
        )

        if not matching_video:
            video_id = self.file_upload_async(media_path, video_name=video_name, **upload_kwargs)
            return video_id, True

        video_id = matching_video["id"]
        logger.info(f"Found matching video: ID={video_id}, Name='{file_name}', Size={file_size}")

        if conflict_mode == "skip":
            logger.info("Conflict mode 'skip'. Skipping upload.")
            return video_id, False

        elif conflict_mode == "overwrite":
            logger.info("Conflict mode 'overwrite'. Deleting and re-uploading.")
            if not self.delete_video(video_id):
                raise RuntimeError("Failed to delete existing video for overwrite.")
            video_id = self.file_upload_async(media_path, video_name=video_name, **upload_kwargs)
            return video_id, True

        elif conflict_mode == "reindex":
            logger.info("Conflict mode 'reindex'. Triggering reindex.")
            if self.reindex_video(video_id):
                return video_id, False
            else:
                raise RuntimeError("Reindex failed.")

        # Should not reach
        raise RuntimeError(f"Unhandled conflict mode: {conflict_mode}")
    
    def update_catalog(self, repo_guid, file_path, video_id, max_attempts=5):
        url = "http://127.0.0.1:5080/catalogs/providerData"
        self.get_account()
        node_api_key = get_node_api_key()
        headers = {
            "apikey": node_api_key,
            "Content-Type": "application/json"
        }
        payload = {
            "repoGuid": repo_guid,
            "fileName": os.path.basename(file_path),
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "providerName": "azure_video_indexer",
            "providerData": {
                "assetId": video_id,
                "accountId": self.account['properties']['accountId'],
                "location": self.account['location'],
                "providerUiLink": f"https://www.videoindexer.ai/accounts/{self.account['properties']['accountId']}/videos/{video_id}?location=westus"
            }
        }
        for attempt in range(max_attempts):
            try:
                response = make_request_with_retries("POST", url, headers=headers, json=payload)
                if response and response.status_code in (200, 201):
                    logging.info(f"Catalog updated successfully: {response.text}")
                    return True
                if response and response.status_code == 404:
                    try:
                        resp_json = response.json()
                        if resp_json.get("message", "").lower() == "catalog item not found":
                            wait_time = 60 + (attempt * 30)
                            logging.warning(
                                f"[Attempt {attempt+1}/{max_attempts}] Catalog item not found. "
                                f"Waiting {wait_time} seconds before retrying..."
                            )
                            time.sleep(wait_time)
                            continue  # retry outer loop
                    except Exception:
                        logging.warning(f"Failed to parse JSON on 404 response: {response.text}")
                    logging.warning(f"Catalog update failed (status 404): {response.text}")
                    break
                logging.warning(
                    f"Catalog update failed (status {response.status_code if response else 'No response'}): "
                    f"{response.text if response else ''}"
                )
                break
            except Exception as e:
                logging.warning(f"Unexpected error in update_catalog attempt {attempt+1}: {e}")
                break
        pass

# =============================
# Main Execution
# =============================

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 1.5))  # Avoid herd

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
    setup_logging(args.log_level)

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
    try:
        consts = Consts(
            SubscriptionId=section.get("subscription_id"),
            ResourceGroup=section.get("resource_group"),
            AccountName=section.get("account_name"),
            tenant_id=section.get("tenant_id"),
            client_id=section.get("client_id"),
            client_secret=section.get("client_secret"),
            location=section.get("location", "trial")
        )
    except ValueError as e:
        logger.error(f"Missing config value: {e}")
        sys.exit(1)

    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logger.error(f"Source file not found: {matched_file}")
        sys.exit(4)

    if args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logger.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logger.warning(f"Invalid size limit: {args.size_limit}")

    # Paths and backlink
    file_name_for_url = extract_file_name(args.catalog_path)
    catalog_dir = remove_file_name_from_path(args.catalog_path or matched_file)
    normalized_path = catalog_dir.replace("\\", "/")
    relative_path = normalized_path.split("/1/", 1)[-1] if "/1/" in normalized_path else normalized_path
    repo_guid = args.repo_guid or (normalized_path.split("/1/")[0].split("/")[-1] if "/1/" in normalized_path else "")

    # Backlink URL
    job_guid = args.job_guid or ""
    client_ip, _ = (args.controller_address.split(":", 1) if args.controller_address and ":" in args.controller_address
                    else get_link_address_and_port())
    backlink_url = (
        f"https://{client_ip}/dashboard/projects/{job_guid}"
        f"/browse&search?path={urllib.parse.quote(relative_path)}"
        f"&filename={urllib.parse.quote(file_name_for_url)}"
    )

    # Metadata
    metadata = {
        "relativePath": "/" + relative_path if not relative_path.startswith("/") else relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name_for_url,
        "fabric_size": os.path.getsize(matched_file),
        "fabric-URL": backlink_url
    }

    if args.metadata_file and os.path.exists(args.metadata_file):
        additional_metadata = parse_metadata_file(args.metadata_file)
        if additional_metadata:
            metadata.update(additional_metadata)

    if args.dry_run:
        logger.info("[DRY RUN] Upload skipped.")
        logger.info(f"[DRY RUN] File: {matched_file}")
        logger.info(f"[DRY RUN] Metadata: {metadata}")
        sys.exit(0)

    try:
        # client Initialization
        client = VideoIndexerClient(consts)
        client.authenticate()

        # Conflict mode
        conflict_mode = (section.get("conflict_resolution") or DEFAULT_CONFLICT_RESOLUTION).lower()
        if conflict_mode not in CONFLICT_RESOLUTION_MODES:
            logger.error(f"Invalid conflict_resolution mode: {conflict_mode}")
            sys.exit(1)

        # Prepare metadata
        original_file_name = extract_file_name(matched_file)
        name_part, ext_part = os.path.splitext(original_file_name)
        sanitized_name_part = name_part.strip()
        file_name = sanitized_name_part + ext_part
        
        if file_name != original_file_name:
            logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
        partition = metadata.pop("partition", repo_guid)
        description = backlink_url

        # Upload or resolve conflict
        video_id, is_new = client.resolve_conflict_and_upload(
            media_path=matched_file,
            conflict_mode=conflict_mode,
            video_name=file_name,
            description=description,
            privacy="Private",
            partition=partition,
            metadata=metadata
        )

        # Only poll if it's a new upload or reindex was not enough
        # Note: Reindex triggers async processing — we still need to wait
        if conflict_mode != "skip" or is_new == True:  # skip doesn't need polling
            status = client.wait_for_index_async(video_id, timeout_sec=14400)
            if status != "Processed":
                logger.error(f"Indexing ended with status: {status}")
                sys.exit(1)

        client.update_catalog(repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], video_id)
        print(f"VideoID={video_id}")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Operation failed: {e}")
        print(f"Error: {str(e)}")
        sys.exit(1)