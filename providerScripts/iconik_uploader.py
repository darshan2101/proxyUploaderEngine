import os
import sys
import json
import math
import hashlib
import argparse
import logging
import time
import random
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from json import dumps
from configparser import ConfigParser
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET
import plistlib
from pathlib import Path

DOMAIN = 'https://app.iconik.io'

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 5 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "overwrite"

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

    for attempt in range(4):  # Max 3 attempts
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:  # Only sleep if not last attempt
                base_delay = [1, 3, 10, 30][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5, 10][attempt])  # e.g., 1-2, 3-4, 10-15, 30-40
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

def prepare_metadata_to_upload( backlink_url, properties_file = None):    
    metadata = {
        "fabric URL": backlink_url
    }
    
    if not properties_file or not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return metadata

    logging.debug(f"Reading properties from: {properties_file}")
    try:
        file_ext = properties_file.lower()
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

def remove_file(config, asset_id):
    url = f'{DOMAIN}/API/assets/v1/assets/{asset_id}'
    headers = {"app-id":config["app-id"], "auth-token" : config["auth-token"]}
    logging.debug(f"URL: {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("DELETE", url, headers=headers)
            if response and response.status_code == 204:
                logging.info("Asset removed successfully.")
                return True
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return False
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to remove asset after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None  

def calculate_sha1(file_path):
    sha1 = hashlib.sha1()
    
    with open(file_path, 'rb') as file:
        while chunk := file.read(8192):
            sha1.update(chunk)
    
    return sha1.hexdigest()

def get_first_collection(config):
    url = f"{DOMAIN}/API/assets/v1/collections/"
    headers = {
        "accept": "application/json",
        "App-ID": config["app-id"],
        "Auth-Token": config["auth-token"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                collections = response.json().get('objects', [])
                if collections:    
                    logging.info(f"Using first collection ID: {collections[0].get('id')}")
                    return collections[0].get('id')
                else:
                    return None
            else:
                logging.error("No collections found in the response.")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to Root asset ID after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None 

def create_collection(config, collection_name, parent_id=None):
    url = f'{DOMAIN}/API/assets/v1/collections/'
    payload = {"title": collection_name}
    if parent_id:
        payload["parent_id"] = parent_id

    params = {"apply_default_acls": "true", "restrict_collection_acls": "true"} if parent_id else {"apply_default_acls": "true"}
    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:    
            response = make_request_with_retries("POST", url, headers=headers, json=payload, params=params)
            if response and response.status_code in (200, 201):
                return response.json()['id']
            else:
                logging.error(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            logging.warning(f"Exception during folder creation (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to create folder after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def find_or_create_collection_path(config, collection_path, base_collection_id=None):
    current_id = base_collection_id
    for folder in collection_path.strip('/').split('/'):
        if not folder:
            continue
        # Fetch contents of the current collection
        contents = get_call_of_collections_content(config, current_id) if current_id else []
        # Check if the folder exists as a collection
        matched = next((item for item in contents if 'files' not in item and item.get('title') == folder), None)
        if matched:
            current_id = matched['id']
        else:
            # Create collection under current_id
            current_id = create_collection(config, folder, current_id)
    return current_id


def get_call_of_collections_content(config, collection_id):
    url = f"{DOMAIN}/API/assets/v1/collections/{collection_id}/contents"
    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    params = {
        "per_page": 100,
        "page": 1,
        "index_immediately": "true"
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers, params=params)
            if response and response.status_code in (200, 201):
                logging.info("contents of folder acquired successfully.")                
                return response.json().get("objects", [])
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get contents of folder after 3 attempts: {e}")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def get_storage_id(config, storage_name,storage_method):
    url = f'{DOMAIN}/API/files/v1/storages/'
    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                logging.info("Storgae ID acquired successfully.")                
                response_data = response.json()
                storage_id = []
                for storage in response_data["objects"]:
                    if storage["name"] == storage_name and storage["method"] == storage_method:
                        storage_id.append(storage["id"])
                return storage_id[0]
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get Storgae ID after 3 attempts: {e}")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def create_asset_id(config, file_path, collection_id=None):
    url = f'{DOMAIN}/API/assets/v1/assets/'
    file_name = extract_file_name(file_path)
    file_size = os.path.getsize(file_path)
    payload = {"title": file_name, "type": "ASSET"}
    params = {"apply_default_acls": "true"}
    # check and remove file if is already present
    conflict_resolution = config.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    assets = get_call_of_collections_content(config, collection_id)
    for asset in assets:
        if 'files' in asset:
            for file in asset['files']:
                if file['original_name'] == file_name and int(file['size']) == int(file_size):
                    if conflict_resolution == "skip":
                        logging.info(f"File '{file_name}' exists. Skipping upload.")
                        print(f"File '{file_name}' already exists. Skipping upload.")
                        sys.exit(0)
                    elif conflict_resolution == "overwrite":
                        if remove_file(config, asset['id']) == True:
                            logging.info(f"Removed existing asset with path: {matched_file}")
                        else:
                            logging.error(f"Failed to remove existing asset: {file_name} with path: {matched_file}")                

    if collection_id:
        payload["collection_id"] = collection_id
        params = {"apply_default_acls": "false", "apply_collection_acls": "true"}

    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, params=params, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info("Upload initiated successfully.")
                response_data = response.json()
                return response_data['id'], response_data['created_by_user']
            else:
                logging.warning(f"create asset failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"create asset error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to create asset after 3 attempts.")
                return None, 500
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None, 500
    
def add_asset_in_collection(config, asset_id, collection_id):
    url = f'{DOMAIN}/API/assets/v1/collections/{collection_id}/contents'
    payload = {"object_id":asset_id,"object_type":"assets"}
    params = {"index_immediately":"true"}
    headers = {"app-id":config["app-id"],
            "auth-token" : config["auth-token"]
            }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, params=params, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info("Asset added in collection successfully.")
                return True
            else:
                logging.warning(f"adding asset to collection failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return False
        except Exception as e:
            logging.warning(f"asset addition to collection error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to add asset in collection after 3 attempts.")
                return False
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return False

def collection_fullpath(config,collection_id):
    url = f'{DOMAIN}/API/assets/v1/collections/{collection_id}/full/path'
    headers = {"app-id":config["app-id"],
        "auth-token" : config["auth-token"]
        }
    params = {"get_upload_path": "true"}
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers, params=params)
            if response and response.status_code in (200, 201):
                logging.info("Colection full path acquired successfully.")                
                return response.json()
            else:
                logger.info(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return ""
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get Collection full path after 3 attempts: {e}")
                return ""
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return ""

def create_format_id(config,asset_id, user_id):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/formats/'
    payload = {"user_id": user_id,"name": "ORIGINAL","metadata": [{"internet_media_type": "text/plain"}],"storage_methods": [config["method"]]}
    headers = {"app-id":config["app-id"],
               "auth-token" : config["auth-token"]
               }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info("format_id aquired successfully.")
                return response.json()['id']
            else:
                logging.warning(f"fetching of format_id failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None
        except Exception as e:
            logging.warning(f"format_id fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get format_id after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def create_fileset_id(config, asset_id, format_id, file_name, storage_id,upload_path):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/file_sets/'
    payload = {"format_id": format_id,"storage_id": storage_id,"base_dir": upload_path,"name": file_name,"component_ids": []}
    headers = {"app-id":config["app-id"],
               "auth-token" : config["auth-token"]
               }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info("fileset_id aquired successfully.")
                return response.json()['id']
            else:
                logging.warning(f"fetching of fileset_id failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None
        except Exception as e:
            logging.warning(f"fileset_id fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get fileset_id after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def get_upload_url(config, asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=file_info)
            if response and response.status_code in (200, 201):
                logging.info("create_format_id aquired successfully.")
                response_data = response.json()
                return response_data['upload_url'], response_data['id']
            else:
                logging.warning(f"fetching of create_format_id failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None, None
        except Exception as e:
            logging.warning(f"create_format_id fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get create_format_id after 3 attempts.")
                return None, None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None, None

def get_upload_url_s3(config, asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=file_info)
            if response and response.status_code in (200, 201):
                logging.info("upload_url for s3 aquired successfully.")
                response_data = response.json()
                return response_data['multipart_upload_url'], response_data['id']
            else:
                logging.warning(f"fetching of upload_url for S3 failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None, None
        except Exception as e:
            logging.warning(f"upload_url from S3 fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get upload_url from S3 after 3 attempts.")
                return None, None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None, None

def get_upload_url_b2(config, asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=file_info)
            if response and response.status_code in (200, 201):
                logging.info("upload_url for s3 aquired successfully.")
                response_data = response.json()
                return response_data['upload_url'], response_data['id'],response_data["upload_credentials"]["authorizationToken"],response_data["upload_filename"]
            else:
                logging.warning(f"fetching of upload_url for B2 failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None, None, None, None
        except Exception as e:
            logging.warning(f"upload_url from B2 fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get upload_url from B2 after 3 attempts.")
                return None, None, None, None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None, None, None, None

def get_upload_id_s3(upload_url):
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", upload_url)
            if response and response.status_code in (200, 201):
                logging.info("upload_id for s3 aquired successfully.")
                root = ET.fromstring(response.text)
                namespace = root.tag.split('}')[0] + '}'
                upload_id = root.find(f'{namespace}UploadId').text
                return upload_id
            else:
                logging.warning(f"fetching of upload_id for S3 failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None
        except Exception as e:
            logging.warning(f"upload_id from S3 fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get upload_id from S3 after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def get_part_url_s3(config, asset_id, file_id, upload_id):
    part_url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/part/'
    params = {"parts_num": "1", "upload_id": upload_id, "per_page": "100", "page": "1"}
    headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", part_url, headers=headers, params=params)
            if response and response.status_code in (200, 201):
                logging.info("part_url for s3 aquired successfully.")
                return response.json()["objects"][0]["url"]
            else:
                logging.warning(f"fetching of part_url for S3 failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
                return None
        except Exception as e:
            logging.warning(f"part_url from S3 fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get part_url from S3 after 3 attempts.")
                return None
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))
    return None

def upload_file_gcs(upload_url, file_path, file_size):
    google_headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7,nb;q=0.6,da;q=0.5',
        'content-length': '0',
        'dnt': '1',
        'origin': DOMAIN,
        'referer': f'{DOMAIN}/upload',
        'x-goog-resumable': 'start',
    }
    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", upload_url, headers=google_headers)
            if response and response.status_code in (200, 201):
                logging.info("upload_id for s3 aquired successfully.")
                upload_id = response.headers['X-GUploader-Uploadid']
            else:
                logging.warning(f"fetching of upload_id for S3 failed (attempt {attempt + 1}): {response.text if response else 'No response'}")
        except Exception as e:
            logging.warning(f"upload_id from S3 fetch error (attempt {attempt + 1}): {e}")
            if attempt == 2:
                logging.error("Failed to get upload_id from S3 after 3 attempts.")
            time.sleep(random.uniform(*([1, 2], [3, 4], [10, 15])[attempt]))

    google_headers = {
        'content-length': str(file_size),
        'x-goog-resumable': 'upload'
    }
    for attempt in range(3):
        try:
            # Open file on each retry
            with open(file_path, 'rb') as f:
                full_upload_url = f"{upload_url}&upload_id={upload_id}"
                upload_response = requests.put(full_upload_url, headers=google_headers, data=f)

                # If 4xx, don't retry — it's a client error (e.g. bad upload_id, data, or headers)
                if 400 <= upload_response.status_code < 500:
                    error_msg = f"Client error: {upload_response.status_code} {upload_response.text}"
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

                # If 2xx, success
                if upload_response.status_code in (200, 201):
                    logging.debug(f"Upload successful: {upload_response.text}")
                    return upload_response

                # If 5xx or unexpected, retry
                logging.warning(f"Server error {upload_response.status_code}: {upload_response.text}. Retrying...")

        except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError) as e:
            logging.warning(f"Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            if "4xx" in str(e).lower():
                logging.error(f"Client error during upload: {e}")
                raise  # Don't retry 4xx
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")

        # Apply jittered backoff
        if attempt < 2:
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        else:
            logging.critical(f"Upload failed after 3 attempts: {file_name}")
            raise RuntimeError("Upload failed after retries.")

    # Should not reach here
    raise RuntimeError("Upload failed for GCS.")         

def upload_file_s3(config, part_url, file_path, upload_id, asset_id, file_id):
    etag = None
    for attempt in range(3):
        try:
            with open(file_path, 'rb') as file:
                response = requests.put(part_url, data=file)
            if 400 <= response.status_code < 500:
                error_msg = f"Client error: {response.status_code} {response.text}"
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            if response.status_code in (200, 201):
                etag = response.headers.get('etag')
                if not etag:
                    logging.warning("No ETag returned from S3 part upload.")
                else:
                    logging.debug(f"Upload successful, ETag: {etag}")
                break

            logging.warning(f"Unexpected status code: {response.status_code}, response: {response.text}")
            if attempt < 2:
                continue
            else:
                raise RuntimeError(f"Upload failed with status {response.status_code}")

        except RuntimeError as e:
            if "Client error" in str(e):
                logging.error(f"Client error during upload: {e}")
                raise  # Don't retry 4xx
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
            else:
                logging.critical(f"Upload failed after 3 attempts: {file_path}")
                raise
        except Exception as e:
            logging.warning(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
            else:
                logging.critical(f"Upload failed after 3 attempts: {file_path}")
                raise

    if not etag:
        raise RuntimeError("Failed to retrieve ETag after successful upload.")

    logging.debug("Completing multipart upload...")
    multipart_url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/'
    try:
        complete_url_response = make_request_with_retries(
            method='GET',
            url=multipart_url,
            headers={
                "app-id": config["app-id"],
                "auth-token": config["auth-token"]
            },
            params={"upload_id": upload_id, "type": "complete_url"}
        )
        if complete_url_response.status_code != 200:
            error_msg = f"Failed to get complete_url: {complete_url_response.status_code}, {complete_url_response.text}"
            logging.error(error_msg)
            sys.exit(1)
        complete_url_data = complete_url_response.json()
        complete_url = complete_url_data.get('complete_url')
        if not complete_url:
            raise ValueError("No 'complete_url' in response")
        # Create XML payload
        xml_payload = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <CompleteMultipartUpload>
        <Part>
            <ETag>{etag}</ETag>
            <PartNumber>1</PartNumber>
        </Part>
        </CompleteMultipartUpload>'''
        headers = {'Content-Type': 'application/xml'}
        final_response = make_request_with_retries(
            method='POST',
            url=complete_url,
            data=xml_payload,
            headers=headers
        )
        if final_response.status_code != 200:
            error_msg = f"Failed to complete upload: {final_response.status_code}, {final_response.text}"
            logging.error(error_msg)
            sys.exit(1)
        logging.info("Multipart upload completed successfully.")
        return final_response

    except Exception as e:
        logging.critical(f"Failed to complete multipart upload: {e}")
        sys.exit(1)

def upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file):
    headers = {
               "Authorization" : authorizationToken,
               "X-Bz-File-Name" : upload_filename,
               "X-Bz-Content-Sha1" : sha1_of_file,
               "Content-Type" : "b2/x-auto"
               }
    for attempt in range(3):
        try:
            # Open file on each retry
            with open(file_path, 'rb') as f:
                response = requests.post(upload_url,headers=headers,data=f)

                # If 4xx, don't retry — it's a client error (e.g. bad index_id, auth)
                if 400 <= response.status_code < 500:
                    error_msg = f"Client error: {response.status_code} {response.text}"
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

                # If 2xx, success
                if response.status_code in (200, 201):
                    logging.debug(f"Upload successful for B2: {response.text}")
                    return response

                # If 5xx or unexpected, retry
                logging.warning(f"Server error {response.status_code}: {response.text}. Retrying...")

        except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError) as e:
            logging.warning(f"Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            if "4xx" in str(e).lower():
                logging.error(f"Client error during upload: {e}")
                raise  # Don't retry 4xx
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")

        # Apply jittered backoff
        if attempt < 2:
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        else:
            logging.critical(f"Upload failed after 3 attempts")
            raise RuntimeError("Upload failed after retries.")

    # Should not reach here
    raise RuntimeError("Upload failed for B2.")

def upload_file_azure(upload_url, file_path):
    headers = { "x-ms-blob-type" : "BlockBlob"}
    for attempt in range(3):
        try:
            # Open file on each retry
            with open(file_path, 'rb') as f:
                response = requests.put(upload_url, headers=headers, data=f)

                # If 4xx, don't retry — it's a client error (e.g. bad data, auth)
                if 400 <= response.status_code < 500:
                    error_msg = f"Client error: {response.status_code} {response.text}"
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

                # If 2xx, success
                if response.status_code in (200, 201):
                    logging.debug(f"Upload successful for Azure: {response.text}")
                    return response

                # If 5xx or unexpected, retry
                logging.warning(f"Server error {response.status_code}: {response.text}. Retrying...")

        except (ConnectionError, Timeout, requests.exceptions.ChunkedEncodingError) as e:
            logging.warning(f"Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            if "4xx" in str(e).lower():
                logging.error(f"Client error during upload: {e}")
                raise  # Don't retry 4xx
            logging.warning(f"Transient error on attempt {attempt + 1}: {e}")

        # Apply jittered backoff
        if attempt < 2:
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        else:
            logging.critical(f"Upload failed after 3 attempts")
            raise RuntimeError("Upload failed after retries.")

    # Should not reach here
    raise RuntimeError("Upload failed for Azure.")

def file_status_update(config, asset_id, file_id):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/'
    headers = {"app-id":config["app-id"],
            "auth-token" : config["auth-token"]
            }
    for attempt in range(3):
        try:
            response = make_request_with_retries("PATCH", url, headers=headers, json={"status": "CLOSED", "progress_processed": 100})
            if response and response.status_code == 200:
                logging.info("File status updated successfully.")
                return response
            else:
                logging.error(f"Failed to update File Status: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to update file status after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None



if __name__ == '__main__':
    # Random delay to avoid thundering herd
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-j", "--job-guid", help="Job GUID")
    parser.add_argument("--collection-id", help="Collection ID to resolve relative upload paths from") 
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata file path")
    parser.add_argument("-up", "--upload-path", required=True, help="Upload path or ID")
    parser.add_argument("-sl", "--size-limit", help="Size limit in MB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--log-level", default="debug", help="Log level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Upload path is already resolved ID")
    parser.add_argument("--controller-address", help="Controller IP:Port")

    args = parser.parse_args()

    setup_logging(args.log_level)

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config '{args.config_name}' not found.")
        sys.exit(1)

    cloud_config_data = cloud_config[args.config_name]
    
    # set collection ID if provided if not check from cloudconfig data block and if not present there need to use api of get collection and take the id of first collection we get
    if args.collection_id:
        collection_id = args.collection_id
    elif "collection_id" in cloud_config_data:
        collection_id = cloud_config_data["collection_id"]
    else:
        collection_id = get_first_collection(cloud_config_data)
        if not collection_id:
            logging.error("Could not retrieve collection_id from Iconik.")
            sys.exit(1)

    if collection_id is None:
        print(f'Collection id is required for upload')
        sys.exit(1)            

    logging.info(f"Starting S3 upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")

    if mode == "get_base_target":
        if args.resolved_upload_id:
            print(args.upload_path)
            sys.exit(0)

        try:
            base_id = args.parent_id or None
            folder_id = find_or_create_collection_path(cloud_config_data, args.upload_path, base_id)
            if not folder_id:
                logging.error("Failed to resolve folder ID for get_base_target.")
                sys.exit(1)
            print(folder_id)
            sys.exit(0)
        except Exception as e:
            logging.critical(f"Failed to resolve upload target: {e}")
            sys.exit(1)
    
    # Validate source file
    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"Source file not found: {matched_file}")
        sys.exit(4)

    matched_file_size = os.path.getsize(matched_file)
    # Size limit check
    if mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            if matched_file_size > limit_bytes:
                logging.error(f"File too large: {matched_file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except (ValueError, TypeError):
            logging.warning(f"Invalid size limit format: {args.size_limit}")

    # Prepare backlink URL
    catalog_path = args.catalog_path or matched_file
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

    # Resolve controller address
    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        ip, _ = get_link_address_and_port()
        client_ip = ip

    backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated backlink URL: {backlink_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Iconik")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata will be applied from: {args.metadata_file}")
        else:
            logging.warning("[DRY RUN] No metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Google Drive in {mode} mode")
 
    storage_name = cloud_config_data["name"] if not cloud_config_data["name"] is None and len(cloud_config_data["name"]) != 0 else "iconik-files-gcs"
    storage_method = cloud_config_data["method"] if not cloud_config_data["method"] is None and len(cloud_config_data["method"]) != 0 else "GCS"
    
    # Resolve Collection ID
    try:
        if args.upload_path and args.resolved_upload_id:
            collection_to_use = args.upload_path
        elif args.upload_path and not args.resolved_upload_id:
            collection_to_use = find_or_create_collection_path(cloud_config_data, args.upload_path, collection_id)
        else:
            collection_to_use = collection_id
    except Exception as e:
        logging.critical(f"Collection resolution failed: {e}")
        sys.exit(1)
    
    if storage_method and storage_name:
        collection = collection_to_use
        file_name = extract_file_name(matched_file)
        storage_id = get_storage_id(cloud_config_data, storage_name,storage_method)
        asset_id, user_id = create_asset_id(cloud_config_data, matched_file, collection)
        logging.debug(f"Adding asset to collection: asset_id={asset_id}, collection_id={collection_id}")
        add_asset_in_collection(cloud_config_data,asset_id,collection)
        upload_path = collection_fullpath(cloud_config_data, collection)
        format_id = create_format_id(cloud_config_data, asset_id, user_id)
        fileset_id = create_fileset_id(cloud_config_data,asset_id, format_id, file_name, storage_id,upload_path)
    else:
        print("Storage Name and Method is requird.")
        sys.exit(1)
    
    
    if storage_method == "GCS":
        upload_url, file_id = get_upload_url(cloud_config_data,asset_id, file_name, matched_file_size, fileset_id, storage_id, format_id,upload_path)
        upload_code = upload_file_gcs(upload_url, matched_file, matched_file_size)
        if upload_code.status_code == 200:
            response = file_status_update(cloud_config_data,asset_id, file_id)
            if response.status_code != 200:
                print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                sys.exit(1)
            print("File Upload Succesfully")
            sys.exit(0)
        else:
            print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
            sys.exit(1)

    elif storage_method == "S3":
        upload_url, file_id = get_upload_url_s3(cloud_config_data,asset_id, file_name, matched_file_size, fileset_id, storage_id, format_id,upload_path)
        upload_id = get_upload_id_s3(upload_url)
        part_url = get_part_url_s3(cloud_config_data,asset_id, file_id, upload_id)
        upload_code = upload_file_s3(cloud_config_data,part_url, extract_file_name(matched_file),upload_id, asset_id, file_id)
        if upload_code.status_code == 200:
            response = file_status_update(cloud_config_data,asset_id, file_id)
            if response.status_code != 200:
                print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                sys.exit(1)
            print("File Upload Succesfully")
            sys.exit(0)
        else:
            print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
            sys.exit(1)

    elif storage_method == "B2":
        upload_url, file_id,authorizationToken,upload_filename = get_upload_url_b2(cloud_config_data,asset_id, file_name, matched_file_size, fileset_id, storage_id, format_id,upload_path)
        sha1_of_file  = calculate_sha1(matched_file)
        upload_code = upload_file_b2(upload_url,authorizationToken,matched_file,upload_filename,sha1_of_file)
        if upload_code.status_code == 200:
            response = file_status_update(cloud_config_data,asset_id, file_id)
            if response.status_code != 200:
                print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                sys.exit(1)
            print("File Upload Succesfully")
            sys.exit(0)
        else:
            print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
            sys.exit(1)

    elif storage_method == "AZURE":
        upload_url, file_id = get_upload_url(cloud_config_data,asset_id, file_name, matched_file_size, fileset_id, storage_id, format_id,upload_path)
        upload_code = upload_file_azure(upload_url, matched_file)
        if upload_code is not None and upload_code.status_code == 200:
            response = file_status_update(cloud_config_data,asset_id, file_id)
            if response is None or response.status_code != 200:
                print(f"Response error. Status - {response.status_code if response else 'No response'}, Error - {response.text if response else 'No response'}")
                sys.exit(1)
            print("File Upload Succesfully")
            sys.exit(0)
        else:
            print(f"Response error. Status - {upload_code.status_code if upload_code else 'No response'}, Error - {upload_code.text if upload_code else 'No response'}")
            sys.exit(1)
            
    else:
        logging.error(f"Unsupported storage method: {storage_method}")
        sys.exit(1)
    
    # #  upload file along with meatadata
    # meta_file = args.metadata_file
    # if meta_file:
    #     logging.info("Preparing metadata to be uploaded ...")
    #     metadata_obj = prepare_metadata_to_upload(backlink_url, meta_file)
    #     if metadata_obj is not None:
    #         parsed = metadata_obj
    #     else:
    #         parsed = None
    #         logging.error("Failed to find metadata .")