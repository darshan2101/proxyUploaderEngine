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
import subprocess
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

# =============================
# Constants
# =============================

VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet", "send_extracted_metadata"]

# Azure Video Indexer API Constants
API_VERSION = "2024-01-01"
API_ENDPOINT = "https://api.videoindexer.ai"
AZURE_RESOURCE_MANAGER = "https://management.azure.com"
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite", "reindex"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/azurevi_metadata_normalizer.py"

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

SDNA_EVENT_MAP = {    
    "azure_video_transcript": "transcript",
    "azure_video_ocr" : "ocr",
    "azure_video_keywords" : "keywords",
    "azure_video_topics" : "topics",
    "azure_video_labels" : "lables",
    "azure_video_brands" : "brands",
    "azure_video_named_locations" : "locations",
    "azure_video_named_people" : "celebrities",
    "azure_video_audio_effects" : "effects",
    "azure_video_detected_objects" : "objects",
    "azure_video_sentiments" : "sentiments",
    "azure_video_emotions" : "emotions",
    "azure_video_visual_content_moderation" : "moderation",
    "azure_video_frame_patterns" : "patterns",
    "azure_video_speakers" : "speakers"
}

LANGUAGE_CODE_MAP = {
  "": "default",
  "ar-AE": "Arabic (United Arab Emirates)",
  "ar-BH": "Arabic Modern Standard (Bahrain)",
  "ar-EG": "Arabic Egypt",
  "ar-IL": "Arabic (Israel)",
  "ar-IQ": "Arabic (Iraq)",
  "ar-JO": "Arabic (Jordan)",
  "ar-KW": "Arabic (Kuwait)",
  "ar-LB": "Arabic (Lebanon)",
  "ar-OM": "Arabic (Oman)",
  "ar-PS": "Arabic (Palestinian Authority)",
  "ar-QA": "Arabic (Qatar)",
  "ar-SA": "Arabic (Saudi Arabia)",
  "ar-SY": "Arabic Syrian Arab Republic",
  "bg-BG": "Bulgarian",
  "ca-ES": "Catalan",
  "cs-CZ": "Czech",
  "da-DK": "Danish",
  "de-DE": "German",
  "el-GR": "Greek",
  "en-AU": "English Australia",
  "en-GB": "English United Kingdom",
  "en-US": "English United States",
  "es-ES": "Spanish",
  "es-MX": "Spanish (Mexico)",
  "et-EE": "Estonian",
  "fa-IR": "Persian",
  "fi-FI": "Finnish",
  "fil-PH": "Filipino",
  "fr-CA": "French (Canada)",
  "fr-FR": "French",
  "ga-IE": "Irish",
  "gu-IN": "Gujarati",
  "he-IL": "Hebrew",
  "hi-IN": "Hindi",
  "hr-HR": "Croatian",
  "hu-HU": "Hungarian",
  "hy-AM": "Armenian",
  "id-ID": "Indonesian",
  "is-IS": "Icelandic",
  "it-IT": "Italian",
  "ja-JP": "Japanese",
  "kn-IN": "Kannada",
  "ko-KR": "Korean",
  "lt-LT": "Lithuanian",
  "lv-LV": "Latvian",
  "ml-IN": "Malayalam",
  "ms-MY": "Malay",
  "nb-NO": "Norwegian",
  "nl-NL": "Dutch",
  "pl-PL": "Polish",
  "pt-BR": "Portuguese",
  "pt-PT": "Portuguese (Portugal)",
  "ro-RO": "Romanian",
  "ru-RU": "Russian",
  "sk-SK": "Slovak",
  "sl-SI": "Slovenian",
  "sv-SE": "Swedish",
  "ta-IN": "Tamil",
  "te-IN": "Telugu",
  "th-TH": "Thai",
  "tr-TR": "Turkish",
  "uk-UA": "Ukrainian",
  "vi-VN": "Vietnamese",
  "zh-Hans": "Chinese (Simplified)",
  "zh-HK": "Chinese (Cantonese, Traditional)"
}

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

def fail(msg, code=7):
    logger.error(msg)
    print(f"Metadata extraction failed for asset: {args.asset_id}")
    sys.exit(code)

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

def get_admin_dropbox_path():
    """Get LogPath from DNAClientServices config (used as admin dropbox root)"""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            log_path = parser.get('General', 'LogPath', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                log_path = str(cfg.get("LogPath", "")).strip()
        return log_path or None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def get_advanced_ai_config(config_name, provider_name="azurevi"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return {}

    provider_key = provider_name.lower()  # "azurevi"

    for src in [
        os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json"),
        os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    ]:
        if os.path.exists(src):
            try:
                with open(src, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                    if not isinstance(raw, dict):
                        continue

                    out = {}
                    # Normalize top-level keys: strip optional "azurevi_" or "AZUREVI_" prefix
                    for key, val in raw.items():
                        key_lower = key.lower()
                        clean_key = key[len(provider_key) + 1:] if key_lower.startswith(provider_key + "_") else key
                        # Accept known Azure VI config keys
                        if clean_key in {
                            "features",
                            "indexingPreset",
                            "language",
                            "isSearchable",
                            "brandsCategories",
                            "customLanguages",
                            "preventDuplicates",
                            "retentionPeriod",
                            "translationLanguages",
                            "storageAccountName",
                            "storageAccountKey",
                            "storageContainer"
                        }:
                            out[clean_key] = val

                    # Normalize features: strip redundant provider prefixes
                    if "features" in out:
                        feats = out["features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        cleaned = []
                        for f in feats:
                            if isinstance(f, str):
                                f_lower = f.lower()
                                for prefix in ["azurevi_", "azurvi_", "provider_", "videoindexer_"]:
                                    if f_lower.startswith(prefix):
                                        f = f[len(prefix):]
                                        break
                                cleaned.append(f)
                            else:
                                cleaned.append(f)
                        out["features"] = cleaned

                    # Normalize brandsCategories: string → list → comma-joined string (as VI expects)
                    if "brandsCategories" in out:
                        bc = out["brandsCategories"]
                        if isinstance(bc, list):
                            out["brandsCategories"] = ",".join(str(x).strip() for x in bc if x)
                        elif isinstance(bc, str):
                            out["brandsCategories"] = ",".join(x.strip() for x in bc.split(",") if x.strip())

                    # Normalize translationLanguages: list or string → comma-joined string
                    if "translationLanguages" in out:
                        tl = out["translationLanguages"]
                        if isinstance(tl, list):
                            out["translationLanguages"] = [str(x).strip() for x in tl if x]
                        elif isinstance(tl, str):
                            out["translationLanguages"] = [x.strip() for x in tl.split(",") if x.strip()]
                        else:
                            out["translationLanguages"] = []

                    # Normalize booleans
                    for bool_key in ["isSearchable", "preventDuplicates"]:
                        if bool_key in out:
                            v = out[bool_key]
                            out[bool_key] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes", "on")

                    # Normalize retentionPeriod → int
                    if "retentionPeriod" in out:
                        try:
                            out["retentionPeriod"] = int(out["retentionPeriod"])
                        except (ValueError, TypeError):
                            del out["retentionPeriod"]

                    logging.info(f"Loaded and normalized Azure VI config from: {src}")
                    return out

            except Exception as e:
                logging.error(f"Failed to load {src}: {e}")

    return {}

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

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json" if IS_LINUX else "/Library/Application Support/StorageDNA/nginx/ai-config.json"
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            metadata_store_path = config_data.get("ai_export_shared_drive_path", "")
            proxy_store_path = config_data.get("ai_proxy_shared_drive_path", "")
            logging.info(f"Metadata Store Path: {metadata_store_path}, Proxy Store Path: {proxy_store_path}")
        
    except Exception as e:
        logging.error(f"Error reading Metadata Store or Proxy Store settings: {e}")
        sys.exit(5)

    if not metadata_store_path or not proxy_store_path:
        logging.info("Store settings not found.")
        sys.exit(5)

    return metadata_store_path, proxy_store_path

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
    return None

def add_metadata_directory(repo_guid: str, provider: str, file_path: str) -> tuple:
    meta_path, _ = get_store_paths()
    if not meta_path:
        logger.error("Metadata Store settings not found.")
        return None, None, None, None

    if "/./" in meta_path:
        meta_left, meta_right = meta_path.split("/./", 1)
    else:
        meta_left, meta_right = meta_path, ""
    meta_left = meta_left.rstrip("/")

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    repo_guid_str = str(repo_guid)

    metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)

    return metadata_dir, meta_right, base_name

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

def get_azurevi_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path):

    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logging.error(f"Normalizer script not found: {NORMALIZER_SCRIPT_PATH}")
        return False
    
    try:
        process = subprocess.run(
            ["python3", NORMALIZER_SCRIPT_PATH, "-i", raw_metadata_file_path, "-o", norm_metadata_file_path],
            check=True
        )
        if process.returncode == 0:
            return True
        else:
            logging.error(f"Normalizer script failed with return code: {process.returncode}")
            return False
    except Exception as e:
        logging.error(f"Error normalizing metadata: {e}")
        return False

def transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix, language_code):
    try:
        logging.debug(f"Transforming normalized metadata from: {norm_metadata_file_path}, Filetype Prefix: {filetype_prefix}, Language Code: {language_code}")
        with open(norm_metadata_file_path, "r") as f:
            data = json.load(f)
        result = []
        for event_type, detections in data.items():
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, 'unknown')
 
            for detection in detections:
                occurrences = detection.get("occurrences", [])
 
                event_obj = {
                    "eventType": event_type,
                    "sdnaEventTypePrefix": filetype_prefix,
                    "sdnaEventType": sdna_event_type,
                    "eventValue": detection.get("value", ""),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                }
                result.append(event_obj)
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False



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
# Azure Blob Storage Helper
# =============================

def get_blob_storage_config(section, advanced_ai_config):
    """Fetch blob storage credentials from section or advanced config."""
    # Try section first
    account_name = section.get("storage_account_name")
    account_key = section.get("storage_account_key")
    container_name = section.get("storage_container")
    if not (account_name and account_key and container_name):
        # Fall back to advanced config
        account_name = advanced_ai_config.get("storageAccountName")
        account_key = advanced_ai_config.get("storageAccountKey")
        container_name = advanced_ai_config.get("storageContainer")

    if not (account_name and account_key and container_name):
        return None

    return {
        "account_name": account_name,
        "account_key": account_key,
        "container_name": container_name
    }

def upload_to_blob_storage(file_path, blob_config):
    """Upload large file to Azure Blob Storage using parallel block upload."""
    account_name = blob_config["account_name"]
    account_key = blob_config["account_key"]
    container_name = blob_config["container_name"]

    blob_service_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )

    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    blob_name = os.path.basename(file_path)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Use high-performance parallel upload
    with open(file_path, "rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True,
            max_concurrency=8,
            validate_content=False,
            encoding=None
        )

    # Generate SAS URL (valid 48h for safety during VI processing)
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=48)
    )
    sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    return sas_url, blob_name

def refresh_sas_url(blob_config, blob_name, duration_hours=24):
    """Generate a fresh SAS URL for an existing blob."""
    sas_token = generate_blob_sas(
        account_name=blob_config["account_name"],
        container_name=blob_config["container_name"],
        blob_name=blob_name,
        account_key=blob_config["account_key"],
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=duration_hours)
    )
    return f"https://{blob_config['account_name']}.blob.core.windows.net/{blob_config['container_name']}/{blob_name}?{sas_token}"

def delete_blob(blob_config: dict, blob_name: str) -> bool:
    """Safely delete a blob from Azure Blob Storage."""
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{blob_config['account_name']}.blob.core.windows.net",
            credential=blob_config["account_key"]
        )
        blob_client = blob_service_client.get_blob_client(
            container=blob_config["container_name"],
            blob=blob_name
        )
        if blob_client.exists():
            blob_client.delete_blob()
            logger.info(f"Blob '{blob_name}' deleted successfully.")
            return True
        else:
            logger.warning(f"Blob '{blob_name}' not found — nothing to delete.")
            return True  # treat as success
    except Exception as e:
        logger.error(f"Failed to delete blob '{blob_name}': {e}")
        return False

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
                params["partitions"] = partition

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
        media_path: Optional[str] = None,
        video_url: Optional[str] = None,
        video_name: Optional[str] = None,
        description: str = "",
        privacy: str = "Private",
        partition: str = "",
        excluded_ai: Optional[list] = None,
        metadata: Optional[dict] = None,
        indexing_preset: Optional[str] = None,
        language: Optional[str] = None,
        retention_period: Optional[int] = None,
        is_searchable: Optional[bool] = None,
        brands_categories: Optional[str] = None,
        custom_languages: Optional[str] = None,
        prevent_duplicates: Optional[bool] = None
    ) -> str:
        if excluded_ai is None:
            excluded_ai = []
        if metadata is None:
            metadata = {}

        # Validate input
        if not media_path and not video_url:
            raise ValueError("Either media_path or video_url must be provided.")
        if media_path and video_url:
            raise ValueError("Provide only one of media_path or video_url.")

        if media_path and not os.path.exists(media_path):
            raise FileNotFoundError(f"Media file not found: {media_path}")

        if video_name is None:
            if media_path:
                video_name = os.path.splitext(os.path.basename(media_path))[0]
            else:
                video_name = "remote_video"

        self.get_account()
        url = (
            f"{API_ENDPOINT}/{self.account['location']}/"
            f"Accounts/{self.account['properties']['accountId']}/Videos"
        )

        # Build query parameters
        params = {
            "accessToken": self.vi_access_token,
            "name": video_name[:80],
            "privacy": privacy,
            "partition": partition
        }

        # Optional params
        if excluded_ai:
            params["excludedAI"] = ",".join(excluded_ai)
        if metadata:
            params["metadata"] = json.dumps(metadata, ensure_ascii=False)
        if indexing_preset:
            params["indexingPreset"] = indexing_preset
        if language:
            params["language"] = language
        if retention_period is not None:
            params["retentionPeriod"] = retention_period
        if is_searchable is not None:
            params["isSearchable"] = "true" if is_searchable else "false"
        if brands_categories:
            params["brandsCategories"] = brands_categories
        if custom_languages:
            params["customLanguages"] = custom_languages
        if prevent_duplicates is not None:
            params["preventDuplicates"] = "true" if prevent_duplicates else "false"

        logger.info(f"Uploading '{video_name}'...")
        logger.debug(f"URL: {url}")
        logger.debug(f"Query params: {params}")

        for attempt in range(3):
            try:
                if video_url:
                    # Remote upload via videoUrl
                    params["videoUrl"] = video_url
                    logger.info(f"Using remote video URL (SAS): {video_url[:60]}...")
                    response = requests.post(url, params=params)
                else:
                    # Local file upload
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
        logger.critical(f"Upload failed after 3 attempts: {video_name}")
        raise RuntimeError("Upload failed after retries.")

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
                    errorCode = video_data.get("failureCode", "Unknown")
                    errorMessage = video_data.get("failureMessage", "No failure message")
                    logger.debug(f"❌ Indexing failed for video {video_id}. Code: {errorCode}, Message: {errorMessage}")
                    print(f"Metadata extraction failed for asset: {video_id}")
                    sys.exit(7)

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
        conflict_mode: str,
        media_path: Optional[str] = None,
        video_url: Optional[str] = None,
        video_name: Optional[str] = None,
        **upload_kwargs
    ) -> Tuple[str, bool]:
        if conflict_mode not in CONFLICT_RESOLUTION_MODES:
            raise ValueError(f"Invalid conflict_mode: {conflict_mode}. Use: {CONFLICT_RESOLUTION_MODES}")

        # Determine file name
        if media_path:
            file_name = extract_file_name(media_path)
            file_size = os.path.getsize(media_path)
        elif video_url:
            file_name = video_name or "remote_video"
            file_size = "N/A (remote)"
        else:
            raise ValueError("Either media_path or video_url must be provided.")

        metadata = upload_kwargs.get("metadata", {})
        partition = metadata.get("repoGuid")
        if not partition:
            logger.warning("No 'repoGuid' found in metadata. Falling back to unfiltered search.")
        videos = self.list_videos(partition=partition)
        matching_video = next(
            (v for v in videos if v["name"] == file_name),
            None
        )
        if not matching_video:
            video_id = self.file_upload_async(
                media_path=media_path,
                video_url=video_url,
                video_name=video_name,
                **upload_kwargs
            )
            return video_id, True
        video_id = matching_video["id"]
        logger.info(f"Found matching video: ID={video_id}, Name='{file_name}', Size={file_size}")
        if conflict_mode == "skip":
            self.update_catalog(partition, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], video_id)
            logger.info("Conflict mode 'skip'. Skipping upload.")
            return video_id, False
        elif conflict_mode == "overwrite":
            logger.info("Conflict mode 'overwrite'. Deleting and re-uploading.")
            if not self.delete_video(video_id):
                raise RuntimeError("Failed to delete existing video for overwrite.")
            video_id = self.file_upload_async(
                media_path=media_path,
                video_url=video_url,
                video_name=video_name,
                **upload_kwargs
            )
            return video_id, True
        elif conflict_mode == "reindex":
            logger.info("Conflict mode 'reindex'. Triggering reindex.")
            if self.reindex_video(video_id):
                return video_id, False
            else:
                raise RuntimeError("Reindex failed.")
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
            "providerName": section.get("provider", "AZUREVI"),
            "providerData": {
                "assetId": video_id,
                "accountId": self.account['properties']['accountId'],
                "location": self.account['location'],
                "providerUiLink": f"https://www.videoindexer.ai/accounts/{self.account['properties']['accountId']}/videos/{video_id}?location=westus"
            }
        }
        for attempt in range(max_attempts):
            try:
                logging.debug(f"[Attempt {attempt+1}/{max_attempts}] Starting catalog update request to {url}")
                response = make_request_with_retries("POST", url, headers=headers, json=payload)
                if response is None:
                    logging.error(f"[Attempt {attempt+1}] Response is None!")
                    time.sleep(5)
                    continue
                try:
                    resp_json = response.json() if response.text.strip() else {}
                except Exception as e:
                    logging.warning(f"Failed to parse response JSON: {e}")
                    resp_json = {}
                if response.status_code in (200, 201):
                    logging.info("Catalog updated successfully.")
                    return True
                # --- Handle 404 explicitly ---
                if response.status_code == 404:
                    logging.info("[404 DETECTED] Entering 404 handling block")
                    message = resp_json.get('message', '')
                    logging.debug(f"[404] Raw message from response: [{repr(message)}]")
                    clean_message = message.strip().lower()
                    logging.debug(f"[404] Cleaned message: [{repr(clean_message)}]")

                    if clean_message == "catalog item not found":
                        wait_time = 60 + (attempt * 10)
                        logging.warning(f"[404] Known 'not found' case. Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"[404] Unexpected message: {message}")
                        break  # non-retryable 404
                else:
                    logging.warning(f"[Attempt {attempt+1}] Non-404 error status: {response.status_code}")

            except Exception as e:
                logging.exception(f"[Attempt {attempt+1}] Unexpected exception in update_catalog: {e}")
            # Default retry delay for non-404 or unhandled cases
            if attempt < max_attempts - 1:
                fallback_delay = 5 + attempt * 2
                logging.debug(f"Sleeping {fallback_delay}s before next attempt")
                time.sleep(fallback_delay)

        pass
    
    def get_ai_metadata(self, video_id, max_attempts=3):
        self.get_account()
        url = (
            f"{API_ENDPOINT}/{self.account['location']}/Accounts/"
            f"{self.account['properties']['accountId']}/Videos/{video_id}/Index"
        )
        params = {"accessToken": self.vi_access_token}
        for attempt in range(max_attempts):
            try:
                response = make_request_with_retries("GET", url, params=params)
                if response and response.status_code == 200:
                    data = response.json()
                    if data.get("state") != "Processed":
                        logger.warning(f"Video not processed yet. Current state: {data.get('state')}")
                        return None
                    videos = data.get("videos", [])
                    if videos and "insights" in videos[0]:
                        return videos[0]["insights"]
                    else:
                        logger.warning("No insights found in response.")
                        return None
                else:
                    logger.warning(f"Failed to fetch AI metadata (status {response.status_code if response else 'No response'}): {response.text if response else ''}")
            except Exception as e:
                logger.warning(f"Error fetching AI metadata (attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
        return None

    def get_translated_metadata(self, video_id: str, language_code: str, max_attempts=3):
        self.get_account()
        url = (
            f"{API_ENDPOINT}/{self.account['location']}/Accounts/"
            f"{self.account['properties']['accountId']}/Videos/{video_id}/Index"
        )
        params = {
            "accessToken": self.vi_access_token,
            "language": language_code  # This triggers translation
        }

        for attempt in range(max_attempts):
            try:
                response = make_request_with_retries("GET", url, params=params)
                if response and response.status_code == 200:
                    data = response.json()
                    if data.get("state") != "Processed":
                        logger.warning(f"Video not processed yet. Current state: {data.get('state')}")
                        return None
                    videos = data.get("videos", [])
                    if videos and "insights" in videos[0]:
                        logger.info(f"Retrieved translated metadata for language: {language_code}")
                        return videos[0]["insights"]
                    else:
                        logger.warning(f"No insights found for language: {language_code}")
                        return None
                else:
                    logger.warning(f"Failed to fetch translated metadata (status {response.status_code if response else 'No response'})")
            except Exception as e:
                logger.warning(f"Error fetching translated metadata (attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
        return None

    def save_metadata_to_file(self, file_path: str, metadata: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"Saved metadata to: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save metadata to {file_path}: {e}")
            return False

    def combine_multilanguage_metadata(self, base_metadata: dict, translated_data: dict) -> dict:
        combined = {
            "version": base_metadata.get("version", "1.0"),
            "id": base_metadata.get("id"),
            "name": base_metadata.get("name"),
            "description": base_metadata.get("description"),
            "insights": base_metadata
        }
        if translated_data:
            combined["multilanguage_insights"] = translated_data
        return combined

    def fetch_and_save_all_metadata(self, video_id: str, repo_guid: str, file_path: str, translation_languages: list = None) -> tuple:
        # Build metadata directory
        provider = section.get("provider", "AZUREVI")
        metadata_dir, meta_right, base_name = add_metadata_directory(repo_guid, provider, file_path)

        # 1. Fetch and save base metadata
        logger.info("Fetching base metadata...")
        base_metadata = self.get_ai_metadata(video_id)
        if not base_metadata:
            logger.error("Failed to fetch base metadata")
            return None

        base_json_path = os.path.join(metadata_dir, f"{base_name}.json")
        if not self.save_metadata_to_file(base_json_path, base_metadata):
            logger.error("Failed to save base metadata")
            return None

        # 2. Fetch and save translated metadata
        translated_data = {}
        if translation_languages:
            logger.info(f"Fetching translations for languages: {translation_languages}")
            for lang_code in translation_languages:
                logger.info(f"Fetching translation for: {lang_code}")
                translated_insights = self.get_translated_metadata(video_id, lang_code)
                
                if translated_insights:
                    # Save individual language file
                    lang_json_path = os.path.join(metadata_dir, f"{base_name}_{lang_code}.json")
                    if self.save_metadata_to_file(lang_json_path, translated_insights):
                        translated_data[lang_code] = translated_insights
                    else:
                        logger.warning(f"Failed to save translation for {lang_code}")
                else:
                    logger.warning(f"No translation data received for {lang_code}")

        # 3. Combine metadata
        combined_metadata = self.combine_multilanguage_metadata(base_metadata, translated_data)
        return combined_metadata

    def store_metadata_file(self, repo_guid, file_path, combined_metadata, max_attempts=3):
        # Build metadata directory
        provider = section.get("provider", "AZUREVI")
        metadata_dir, meta_right, base_name = add_metadata_directory(repo_guid, provider, file_path)
    
        # repo_guid string for path construction
        repo_guid_str = str(repo_guid)

        # Paths for default language
        raw_json_default = os.path.join(metadata_dir, f"{base_name}.json")
        norm_json_default = os.path.join(metadata_dir, f"{base_name}_norm.json")

        # Save default raw
        if not self.save_metadata_to_file(raw_json_default, combined_metadata):
            logger.error("Failed to save default raw metadata")
            return [], []

        # Normalize default
        if not get_azurevi_normalized_metadata(raw_json_default, norm_json_default):
            logger.error("Normalization failed for default language")
            return [], []

        # Collect all paths to return
        raw_paths = []
        norm_paths = []

        # Prepare paths for return to node API
        raw_return_default = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}.json")
        norm_return_default = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_norm.json")
        raw_paths.append((raw_return_default, None))  # (path, language_code)
        norm_paths.append((norm_return_default, None))

        # Handle multilanguage insights
        multilanguage_insights = combined_metadata.get("multilanguage_insights", {})
        for lang_code, lang_data in multilanguage_insights.items():
            # Save per-language raw
            raw_json_lang = os.path.join(metadata_dir, f"{base_name}_{lang_code}.json")
            if not self.save_metadata_to_file(raw_json_lang, lang_data):
                logger.warning(f"Failed to save raw metadata for {lang_code}")
                continue

            # Normalize per-language
            norm_json_lang = os.path.join(metadata_dir, f"{base_name}_norm_{lang_code}.json")
            if not get_azurevi_normalized_metadata(raw_json_lang, norm_json_lang):
                logger.warning(f"Normalization failed for {lang_code}")
                continue

            # Add to return lists - USE file_path not base_name
            raw_return_lang = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_{lang_code}.json")
            norm_return_lang = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_norm_{lang_code}.json")
            raw_paths.append((raw_return_lang, lang_code))
            norm_paths.append((norm_return_lang, lang_code))

        return raw_paths, norm_paths

    def send_extracted_metadata(self, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, language_code=None, max_attempts=3):
        url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
        node_api_key = get_node_api_key()
        headers = {"apikey": node_api_key, "Content-Type": "application/json"}
        payload = {
            "repoGuid": repo_guid,
            "providerName": section.get("provider", "AZUREVI"),
            "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default") if language_code is not None else "Default",
            "extendedMetadata": [{
                "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
                "fileName": os.path.basename(file_path)
            }]
        }
        if rawMetadataFilePath is not None:
            payload["extendedMetadata"][0]["metadataRawJsonFilePath"] = rawMetadataFilePath
        if normMetadataFilePath is not None:
            payload["extendedMetadata"][0]["metadataFilePath"] = normMetadataFilePath
        for attempt in range(max_attempts):
            try:
                r = make_request_with_retries("POST", url, headers=headers, json=payload)
                if r and r.status_code in (200, 201):
                    return True
            except Exception as e:
                if attempt == 2:
                    logging.critical(f"Failed to send metadata after 3 attempts: {e}")
                    raise
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        return False

    def send_ai_enriched_metadata(self, repo_guid, file_path, enrichedMetadata, language_code=None, max_attempts=3):
        url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
        node_api_key = get_node_api_key()
        headers = {"apikey": node_api_key, "Content-Type": "application/json"}
        payload = {
            "repoGuid": repo_guid,
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default") if language_code is not None else "Default",
            "providerName": section.get("provider", "AZUREVI"),
            "normalizedMetadata": enrichedMetadata
        }

        for attempt in range(max_attempts):
            try:
                r = make_request_with_retries("POST", url, headers=headers, json=payload)
                if r is not None and r.status_code in (200, 201):
                    return True
                else:
                    logging.debug(f"send_ai_enriched_metadata response: {r.text if r is not None else 'No response'}")
            except Exception as e:
                if attempt == 2:
                    logging.critical(f"Failed to send metadata after 3 attempts: {e}")
                    raise
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        return False

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
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata operations")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Use upload-path as account ID")
    parser.add_argument("-j", "--job-guid", help="SDNA Job GUID")
    parser.add_argument("--controller-address", help="Override Link IP:Port")
    parser.add_argument("--parent-id", help="Parent folder ID (unused)")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("-r", "--repo-guid", help="Override repo GUID")
    parser.add_argument("--enrich-prefix", help="Prefix to add for sdnaEventType of AI enrich data")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--log-level", default="info", help="Log level")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata")

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
    
    filetype_prefix = args.enrich_prefix if args.enrich_prefix else "gen"

    if args.export_ai_metadata:
        section["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"
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

    if args.mode == "send_extracted_metadata":
        if not all([args.asset_id, args.repo_guid, args.catalog_path]):
            fail("Asset ID, Repo GUID, and Catalog path required.", 1)

        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        advanced_ai_config = get_advanced_ai_config(args.config_name, "azurevi") or {}
        translation_languages = advanced_ai_config.get("translationLanguages", [])

        client = VideoIndexerClient(consts)
        client.authenticate()
        ai_metadata = client.fetch_and_save_all_metadata(
            args.asset_id, 
            args.repo_guid, 
            catalog_path_clean,
            translation_languages
        )
        if not ai_metadata:
            fail("No AI metadata returned.")
        raw_paths, norm_paths = client.store_metadata_file(args.repo_guid, catalog_path_clean, ai_metadata)
        for raw_return, lang_code in raw_paths:
            norm_return = next((p for p, lc in norm_paths if lc == lang_code), None)
            if not client.send_extracted_metadata(
                args.repo_guid,
                catalog_path_clean,
                raw_return,
                norm_return,
                language_code=lang_code
            ):
                fail("Failed to send extracted metadata.")

        logger.info("Extracted metadata sent successfully.")

        if norm_paths:
            for norm_return, lang_code in norm_paths:
                if not norm_return:
                    continue
                try:
                    meta_path, _ = get_store_paths()
                    norm_metadata_file_path = os.path.join(
                        meta_path.split("/./")[0] if "/./" in meta_path else meta_path,
                        norm_return
                    )
                    logger.debug(f"Attempting to transform: {norm_metadata_file_path}")
                    enriched, success = transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix, lang_code)
                    if not success:
                        logger.error(f"Transformation failed for {lang_code or 'default'}: {enriched}")
                        continue
                    if client.send_ai_enriched_metadata(args.repo_guid, catalog_path_clean, enriched, lang_code):
                        logger.info(f"AI enriched metadata sent successfully for {lang_code or 'default'}.")
                    else:
                        logger.warning(f"AI enriched metadata send failed for {lang_code or 'default'} — skipping.")
                except Exception as e:
                    logger.exception(f"AI enriched metadata transform/send failed for {lang_code or 'default'}: {e}")
        else:
            logger.info("Normalized metadata not present — skipping AI enrichment.")

        sys.exit(0)

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

    # Load Advanced AI Configuration
    advanced_ai_config = get_advanced_ai_config(args.config_name, "azurevi") or {}

    # Default Azure VI parameters
    indexing_preset = "Default"
    language = "auto"
    excluded_ai = []
    retention_period = None
    is_searchable = True
    brands_categories = None
    custom_languages = None
    prevent_duplicates = False

    # Parse advanced config if available
    if advanced_ai_config and isinstance(advanced_ai_config, dict):
        # --- Feature-based exclusion ---
        requested_features = advanced_ai_config.get("features", [])
        if requested_features:
            AZURE_EXCLUDABLE = {
                "Faces", "ObservedPeople", "Emotions", "Labels", "DetectedObjects",
                "Celebrities", "KnownPeople", "OCR", "Clapperboard", "Logos",
                "Speakers", "Topics", "Keywords", "Entities", "ShotType"
            }
            FEATURE_TO_AZURE = {
                "Faces": "Faces",
                "ObservedPeople": "ObservedPeople",
                "Emotions": "Emotions",
                "Labels": "Labels",
                "DetectedObjects": "DetectedObjects",
                "Ocr": "OCR",
                "Clapperboards": "Clapperboard",
                "Logos": "Logos",
                "Speakers": "Speakers",
                "Topics": "Topics",
                "Keywords": "Keywords",
                "NamedLocations": "Entities",
                "NamedPeople": "KnownPeople",
                "Shots": "ShotType"
            }
            requested_azure = {
                FEATURE_TO_AZURE[f] for f in requested_features
                if f in FEATURE_TO_AZURE
            }
            excluded_ai = list(AZURE_EXCLUDABLE - requested_azure)

        # --- Other parameters ---
        indexing_preset = advanced_ai_config.get("indexingPreset", indexing_preset)
        language = advanced_ai_config.get("language", language)
        retention_period = advanced_ai_config.get("retentionPeriod")
        
        # New: isSearchable (bool)
        is_searchable_raw = advanced_ai_config.get("isSearchable")
        if is_searchable_raw is not None:
            is_searchable = bool(is_searchable_raw)

        # New: brandsCategories (string, comma-separated)
        brands_categories = advanced_ai_config.get("brandsCategories")
        if brands_categories and isinstance(brands_categories, list):
            brands_categories = ",".join(brands_categories)
        elif brands_categories and not isinstance(brands_categories, str):
            brands_categories = None

        # New: customLanguages (string or list)
        custom_languages = advanced_ai_config.get("customLanguages")
        if custom_languages:
            if isinstance(custom_languages, list):
                if len(custom_languages) < 2 or len(custom_languages) > 10:
                    logger.warning("customLanguages must have 2–10 languages. Ignoring.")
                    custom_languages = None
                else:
                    custom_languages = ",".join(custom_languages)
            elif isinstance(custom_languages, str):
                parts = [x.strip() for x in custom_languages.split(",")]
                if len(parts) < 2 or len(parts) > 10:
                    logger.warning("customLanguages must have 2–10 languages. Ignoring.")
                    custom_languages = None
                else:
                    custom_languages = ",".join(parts)
            else:
                custom_languages = None

        # New: preventDuplicates (bool)
        prevent_duplicates_raw = advanced_ai_config.get("preventDuplicates")
        if prevent_duplicates_raw is not None:
            prevent_duplicates = bool(prevent_duplicates_raw)

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

    # if args.metadata_file and os.path.exists(args.metadata_file):
    #     additional_metadata = parse_metadata_file(args.metadata_file)
    #     if additional_metadata:
    #         metadata.update(additional_metadata)

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

        # Determine if we should use blob storage (file > 2 GB)
        USE_BLOB_THRESHOLD = 2 * 1024 * 1024 * 1024  # 2 GB
        file_size = os.path.getsize(matched_file)
        use_blob = file_size > USE_BLOB_THRESHOLD

        blob_config = None
        sas_url = None
        blob_name = None

        if use_blob:
            blob_config = get_blob_storage_config(section, advanced_ai_config)
            if not blob_config:
                logger.error("File > 2 GB but Azure Blob Storage credentials missing in config or advanced JSON.")
                sys.exit(6)
            logger.info("File exceeds 2 GB. Uploading to Azure Blob Storage...")
            sas_url, blob_name = upload_to_blob_storage(matched_file, blob_config)
            logger.info(f"Uploaded to blob. SAS URL generated (expires in 24h).")

        # Upload or resolve conflict
        if use_blob:
            video_id, is_new = client.resolve_conflict_and_upload(
            video_url=sas_url,
            conflict_mode=conflict_mode,
            video_name=file_name,
            description=description,
            privacy="Private",
            partition=partition,
            metadata=metadata,
            excluded_ai=excluded_ai,
            indexing_preset=indexing_preset,
            language=language,
            retention_period=retention_period,
            is_searchable=is_searchable,
            brands_categories=brands_categories,
            custom_languages=custom_languages,
            prevent_duplicates=prevent_duplicates
            )
        else:
            video_id, is_new = client.resolve_conflict_and_upload(
                media_path=matched_file,
                conflict_mode=conflict_mode,
                video_name=file_name,
                description=description,
                privacy="Private",
                partition=partition,
                metadata=metadata,
                excluded_ai=excluded_ai,
                indexing_preset=indexing_preset,
                language=language,
                retention_period=retention_period,
                is_searchable=is_searchable,
                brands_categories=brands_categories,
                custom_languages=custom_languages,
                prevent_duplicates=prevent_duplicates
            )
        status = "Processed" if conflict_mode == "skip" else "Unknown"
        # Only poll if it's a new upload or reindex was not enough
        if conflict_mode != "skip" or is_new == True:
            status = client.wait_for_index_async(video_id, timeout_sec=3600)
            if status != "Processed":
                logger.warning(f"Indexing did not complete within 1 hour. Status: {status}. Proceeding as if successful...")

        client.update_catalog(repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], video_id)
        print(f"VideoID={video_id}")

        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if section.get("export_ai_metadata") == "true" and status == "Processed" and is_new == True:
            try:
                translation_languages = advanced_ai_config.get("translationLanguages", [])
                ai_metadata = client.fetch_and_save_all_metadata(
                    video_id,
                    repo_guid, 
                    catalog_path_clean,
                    translation_languages
                )
                if not ai_metadata:
                    fail("No AI metadata returned.")

                raw_paths, norm_paths = client.store_metadata_file(args.repo_guid, catalog_path_clean, ai_metadata)
                for raw_return, lang_code in raw_paths:
                    norm_return = next((p for p, lc in norm_paths if lc == lang_code), None)
                    if not client.send_extracted_metadata(
                        args.repo_guid,
                        catalog_path_clean,
                        raw_return,
                        norm_return,
                        language_code=lang_code
                    ):
                        fail("Failed to send extracted metadata.")

                logger.info("Extracted metadata sent successfully.")

                if norm_paths:
                    for norm_return, lang_code in norm_paths:
                        if not norm_return:
                            continue
                        try:
                            meta_path, _ = get_store_paths()
                            norm_metadata_file_path = os.path.join(
                                meta_path.split("/./")[0] if "/./" in meta_path else meta_path,
                                norm_return
                            )
                            enriched, success = transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix, lang_code)
                            if success and client.send_ai_enriched_metadata(args.repo_guid, catalog_path_clean, enriched, lang_code):
                                logger.info(f"AI enriched metadata sent successfully for {lang_code or 'default'}.")
                            else:
                                logger.warning(f"AI enriched metadata send failed for {lang_code or 'default'} — skipping.")
                        except Exception:
                            logger.exception(f"AI enriched metadata transform failed for {lang_code or 'default'} — skipping.")
                else:
                    logger.info("Normalized metadata not present — skipping AI enrichment.")

            except Exception as e:
                fail(f"Error extracting/sending AI metadata: {e}")

        # Clean up blob if used
        if use_blob and blob_config and blob_name:
            if delete_blob(blob_config, blob_name):
                logger.info("Temporary blob cleaned up.")
            else:
                logger.warning("Blob cleanup failed — manual cleanup may be required.")

        sys.exit(0)

    except Exception as e:
        logger.critical(f"Operation failed: {e}")
        print(f"Error: {str(e)}")
        sys.exit(1)