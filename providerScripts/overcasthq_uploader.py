import os
import sys
import argparse
import json
from datetime import datetime
import logging
import urllib.parse
import requests
import time
import random
from json import dumps
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from configparser import ConfigParser
import plistlib
import hashlib
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import warnings
from action_functions import flatten_dict
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet", "send_extracted_metadata"]
CHUNK_SIZE = 5 * 1024 * 1024
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "overwrite"
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

def get_first_project(config_data):
    logging.debug("Fetching first project from OvercastHQ")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/projects"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code in (200, 201):
                result = response.json().get("result", {})
                items = result.get("items", [])
                if items:
                    logging.info(f"First project ID: {items[0].get('uuid')}")
                    return items[0].get("uuid")
                else:
                    return None
            else:
                logging.error(f"Failed to fetch projects: {response.status_code} {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to Root asset ID after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def create_folder(config_data, name, project_id, parent_id=None):
    logging.info(f"Creating folder: {name}, project: {project_id}, parent: {parent_id}")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/folders"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    payload = {
        "name": name,
        "project": project_id,
    }
    if parent_id is not None:
        payload["parent"] = parent_id
    logging.debug(f"data payload (form-data) -------------------------> {payload}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, data=payload)
            if response and response.status_code in (200, 201):
                resp_json = response.json()
                uuid = resp_json.get("uuid") or resp_json.get("result", {}).get("uuid")
                if uuid:
                    logging.debug(f"Created folder '{name}' with UUID: {uuid}")
                    return uuid
                else:
                    logging.error("UUID not found in folder creation response")
                    return None
            else:
                logging.error(f"Failed to create Folder: {response.status_code} {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to create folder after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None
    
def list_all_folders(config_data, project_id, parent_id):
    logging.debug(f"parameter to get folder tree =================> {parent_id}")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/folders"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    params = {
        "project": project_id
    }
    if parent_id is not None:
        params["parent"] = parent_id
    logging.debug(f"params payload -------------------------> {params}")
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers, params=params)
            if response and response.status_code in (200, 201):
                logging.info("File Tree Successfully")
                data = response.json().get("result", {})
                return data.get("items", [])
            else:
                logging.error(f"Failed to get File Tree: {response.status_code} {response.text}")
                return []
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to list folders after 3 attempts: {e}")
                return []
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return []

def get_folder_id(config_data, upload_path, base_id = None):
    project_id = config_data['project_id']
    current_parent_id = base_id

    # Remove the filename from the upload_path
    folder_path = upload_path
    logging.info(f"Finding or creating folder path: '{folder_path}'")

    for segment in folder_path.strip("/").split("/"):
        if not segment:
            continue
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")

        folders = list_all_folders(config_data, project_id, current_parent_id)

        matched = next(
            (
                f for f in folders
                if f.get("name") == segment and 
                   f.get("parent", {}).get("uuid") == current_parent_id
            ),
            None
        )

        if matched:
            current_parent_id = matched["uuid"]
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data, segment, project_id, current_parent_id)
            logging.debug(f"Created folder '{segment}' (ID: {current_parent_id})")
    logging.debug(f"UUID  after tree traversal/creation ---------------> {current_parent_id}")
    return current_parent_id

def get_assets_id_from_folder(config_data, folder_id):
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/folders/{folder_id}/asset"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to fetch assets from folder after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def remove_file(config_data, asset_id):
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets/{asset_id}"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("DELETE", url, headers=headers)
            if response and response.status_code == 200:
                logging.info(f"Successfully deleted existing asset {asset_id}")
                return True
            elif response:
                logging.warning(f"Delete failed: {response.status_code} {response.text}")
            return False
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to delete asset {asset_id} after 3 attempts: {e}")
                return False
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return False

def get_asset_details(config_data, asset_id):
    data_dict = {}
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets/{asset_id}"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response.status_code == 403:
                logging.warning(f"403 Forbidden: cannot fetch asset details for {asset_id}")
                return None
            elif response.status_code == 200:
                response_data = response.json()
                if not response_data.get("result"):
                    return None
                result = response_data["result"]
                folder_path_list = result.get("folder", {})
                folder_path = []
                if folder_path_list and "parent" in folder_path_list:
                    folder_path = [f.get("value") for f in folder_path_list["parent"] if f.get("value")]
                if "name" in folder_path_list:
                    folder_path.append(folder_path_list["name"])
                info = result.get("info", {})
                file_name = f"{result['name']}.{info.get('file_type', '')}" if 'name' in result else None
                if not file_name:
                    return None
                data_dict['path'] = f"{'/'.join(folder_path)}/{file_name}"
                data_dict['mtime'] = result.get('updated_at')
                data_dict['atime'] = result.get('created_at')
                data_dict['type'] = "file"
                data_dict['size'] = info.get('file_size')
                data_dict['asset_id'] = asset_id
                return response_data.get("result", {})
            else:
                logging.error(f"Response error. Status - {response.status_code}, Error - {response.text}")
                return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to get asset details after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def create_asset(config_data, project_id, folder_id, file_path):
    logging.debug(f"Folder ID check at asset create-------------------------> {folder_id}")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets"
    headers = {
        'x-api-key': config_data["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")
    original_file_name = extract_file_name(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    file_size = os.path.getsize(file_path)
    asset_list = get_assets_id_from_folder(config_data, folder_id)
    conflict_resolution = config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    for asset in asset_list['result']['items']:
        asset_info = get_asset_details(config_data, asset['uuid'])
        if not asset_info:
            continue
        name_part = asset_info.get('name', '')
        ext_part = asset_info.get('info', {}).get('file_type', '')
        orig_name = f"{name_part}.{ext_part}" if ext_part else name_part
        orig_size = asset_info.get("storages", {}).get("original", {}).get("file_size")

        try:
            if orig_name == file_name and int(orig_size) == file_size:
                logging.info(f"Duplicate asset found: {file_name} ({file_size} bytes)")
                if conflict_resolution == "skip":
                    update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], folder_id, asset_id)
                    logging.info(f"File '{file_name}' exists. Skipping upload.")
                    print(f"File '{file_name}' already exists. Skipping upload.")
                    sys.exit(0)
                elif conflict_resolution == "overwrite":
                    if remove_file(config_data, asset_info['asset_id']):
                        logging.info(f"Deleted existing asset for: {file_path}")
                    else:
                        logging.error(f"Failed to delete asset: {file_path}")
            break  # No need to check further
        except (TypeError, ValueError):
            logging.debug(f"Invalid original file size metadata on asset {asset['uuid']}: {orig_size}")
            continue

    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets"
    headers = {'x-api-key': config_data["api_key"]}
    payload = {
        "project": project_id,
        "file_name": file_name,
        "file_size": file_size,
        "folder": folder_id
    }
    logging.debug(f"data payload (form-data) -------------------------> {payload}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, data=payload)
            if response and response.status_code == 201:
                logging.debug(f"Asset creation response: {response.text}")
                return response, 201
            else:
                logging.error(f"Failed to create asset: {response.status_code} {response.text}")
                return None, response.status_code
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to create asset after 3 attempts: {e}")
                return None, 500
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None, 500

def multipart_upload_to_s3(asset_res, config_data,file_path):
    upload_id = upload_file_to_s3(asset_res,config_data)
    etags = []
    part_number = 1
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            etag = upload_part_to_s3(asset_res,part_number, upload_id,data, config_data)
            etags.append(etag)
            part_number += 1

    complete_multipart_upload(asset_res, upload_id, etags, config_data)

def upload_file_to_s3(asset_res, config_data):
    url = f"https://{asset_res['bucket']}.s3-accelerate.amazonaws.com/{asset_res['path_full']}?uploads"
    payload = b''
    payload_hash = hashlib.sha256(payload).hexdigest()
    d = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    date = d[:8]
    canonical_request = build_canonical_request(asset_res, d)
    string_to_sign = build_string_to_sign(canonical_request, d, date, asset_res['storage_region'])
    signature = get_sign(asset_res, d,string_to_sign, config_data)

    headers = {
        "Authorization": f"AWS4-HMAC-SHA256 Credential={asset_res['access_key']}/{date}/{asset_res['storage_region']}/s3/aws4_request, SignedHeaders=host;x-amz-date;x-amz-meta-asset-uuid, Signature={signature}",
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": d,
        "x-amz-meta-asset-uuid": asset_res['uuid']
    }

    res = requests.post(url, headers=headers)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    root = ET.fromstring(res.text)
    upload_id_elem = root.find('s3:UploadId', ns)
    if upload_id_elem is None or upload_id_elem.text is None:
        logging.error("UploadId not found in S3 response: %s", res.text)
        raise RuntimeError("UploadId not found in S3 response")
    upload_id = upload_id_elem.text
    logging.debug(upload_id)
    return upload_id

def build_canonical_request(asset_res, d):
    method = "POST"
    # The URI path is the asset path (URL-encoded)
    canonical_uri = "/" + urllib.parse.quote(asset_res['path_full'], safe='/')
    
    # Query string for initiating multipart upload
    canonical_querystring = "uploads="
    
    # Required headers
    canonical_headers = (
        f"host:{asset_res['bucket']}.s3-accelerate.amazonaws.com\n"
        f"x-amz-date:{d}\n"
        f"x-amz-meta-asset-uuid:{asset_res['uuid']}\n"
    )
    
    # Signed headers must be lowercase and sorted
    signed_headers = "host;x-amz-date;x-amz-meta-asset-uuid"
    
    # Payload hash for an empty payload POST
    payload_hash = hashlib.sha256(b'').hexdigest()
    
    # Construct canonical request string
    canonical_request = (
        f"{method}\n"
        f"{canonical_uri}\n"
        f"{canonical_querystring}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )
    return canonical_request

def build_put_canonical_request(asset_res, part_number, upload_id, d, payload_hash):
    uri = '/' + urllib.parse.quote(asset_res['path_full'], safe='/')
    qs = f"partNumber={part_number}&uploadId={upload_id}"
    headers = f"host:{asset_res['bucket']}.s3-accelerate.amazonaws.com\n" f"x-amz-date:{d}\n"
    return f"PUT\n{uri}\n{qs}\n{headers}\nhost;x-amz-date\n{payload_hash}"

def build_post_canonical_request(asset_res, upload_id, d, payload_hash):
    uri = '/' + urllib.parse.quote(asset_res['path_full'], safe='/')
    qs = f"uploadId={upload_id}"
    headers = f"host:{asset_res['bucket']}.s3-accelerate.amazonaws.com\n" f"x-amz-date:{d}\n"
    return f"POST\n{uri}\n{qs}\n{headers}\nhost;x-amz-date\n{payload_hash}"

def build_string_to_sign(canonical_request, d, date, region="us-east-1", service="s3"):
    # Hash canonical request
    hashed_canonical_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    scope = f"{date}/{region}/{service}/aws4_request"
    string_to_sign = (
        "AWS4-HMAC-SHA256\n"
        f"{d}\n"
        f"{scope}\n"
        f"{hashed_canonical_request}"
    )
    return string_to_sign

def get_sign(asset_res, d,string_to_sign, config_data):
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets/upload/sign"
    date = d[:8]
    
    # canonical_request = build_canonical_request(asset_res, d)
    # string_to_sign = build_string_to_sign(canonical_request, d, date, asset_res['storage_region'])
    
    headers = {
        'x-api-key': config_data["api_key"]
    }
    params = {
        "to_sign": string_to_sign,
        "datetime": d
    }
    logging.debug(f"Signing URL: {url} with to_sign={string_to_sign}")
    response = requests.get(url, headers=headers, params=params)
    logging.debug(f"Signing RESPONSE: {response.text}")
    if response.status_code != 200:
        logging.debug(f"Signature request error: {response.status_code} {response.text}")
        exit(1)
    return response.text.strip()

def upload_part_to_s3(asset_res, part_number, upload_id, part_data, config_data):
    d = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    date = d[:8]
    payload_hash = hashlib.sha256(part_data).hexdigest()
    canonical_request = build_put_canonical_request(asset_res, part_number, upload_id, d, payload_hash)
    string_to_sign = build_string_to_sign(canonical_request, d, date, asset_res['storage_region'])
    signature = get_sign(asset_res, d, string_to_sign, config_data)
    url = f"https://{asset_res['bucket']}.s3-accelerate.amazonaws.com/{asset_res['path_full']}?partNumber={part_number}&uploadId={upload_id}"
    headers = {
        "Authorization": f"AWS4-HMAC-SHA256 Credential={asset_res['access_key']}/{date}/{asset_res['storage_region']}/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={signature}",
        "x-amz-date": d,
        "x-amz-content-sha256": payload_hash,
        "Content-Length": str(len(part_data))
    }
    response = requests.put(url, headers=headers, data=part_data)
    response.raise_for_status()
    logging.debug(response.headers.get("ETag"))
    return response.headers.get("ETag")


def complete_multipart_upload(asset_res, upload_id, etags, config_data):
    d = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    date = d[:8]
    parts_xml = ''.join(f"<Part><PartNumber>{i+1}</PartNumber><ETag>{etag}</ETag></Part>" for i, etag in enumerate(etags))
    payload = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode('utf-8')
    payload_hash = hashlib.sha256(payload).hexdigest()
    canonical_request = build_post_canonical_request(asset_res, upload_id, d, payload_hash)
    string_to_sign = build_string_to_sign(canonical_request, d, date, asset_res['storage_region'])
    signature = get_sign(asset_res, d, string_to_sign, config_data)
    url = f"https://{asset_res['bucket']}.s3-accelerate.amazonaws.com/{asset_res['path_full']}?uploadId={upload_id}"
    headers = {
        "Authorization": f"AWS4-HMAC-SHA256 Credential={asset_res['access_key']}/{date}/{asset_res['storage_region']}/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature={signature}",
        "x-amz-date": d,
        "x-amz-content-sha256": payload_hash,
        "Content-Type": "application/xml"
    }
    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()
    logging.debug(response.status_code)
    logging.debug(response.text)

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


def parse_metadata_file(properties_file):
    metadata = {}

    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Metadata file not found: {properties_file}")
        return metadata

    try:
        file_ext = properties_file.lower()

        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata.update(json.load(f))

        elif file_ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        metadata[key] = value
            else:
                logging.warning("No <meta-data> section found in XML.")

        else:  # assume CSV
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value

    except Exception as e:
        logging.error(f"Failed to parse {properties_file}: {e}")

    return metadata

def update_catalog(repo_guid, file_path, folder_id, asset_id, max_attempts=5):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    # Read NodeAPIKey from client services config
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": cloud_config_data.get("provider", "overcasthq"),
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "providerUiLink": f"https://keycode.overcasthq.com/assets/{asset_id}"
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

def upload_metadata_to_asset(hostname, api_key, backlink_url, asset_id, properties_file=None):
    logging.info(f"Updating asset {asset_id} with properties from {properties_file}")
    url = f"https://api-{hostname}.overcasthq.com/v1/assets/{asset_id}/metadata"
    headers = {
        'x-api-key': api_key
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    metadata = {"fabric_URL": backlink_url}
    parsed_metadata = parse_metadata_file(properties_file)
    metadata.update(parsed_metadata)

    failed_keys = []
    for key, value in metadata.items():
        value = value if value not in (None, '', [], {}) else "N/A"
        payload = {"key": key, "value": value}
        logging.debug(f"Sending metadata: {payload}")

        success = False
        for attempt in range(3):
            try:
                response = make_request_with_retries("POST", url, headers=headers, data=payload)
                if response and response.status_code in (200, 201):
                    logging.info(f"Uploaded: {key} = {value}")
                    success = True
                    break
                else:
                    base_delay = [1, 3, 10][attempt]
                    delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                    time.sleep(delay)
            except Exception as e:
                base_delay = [1, 3, 10][attempt]
                delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        if not success:
            logging.error(f"Failed to upload metadata '{key}' after 3 attempts.")
            failed_keys.append(key)

    if failed_keys:
        return {
            "status_code": 207,
            "detail": f"Metadata uploaded with failures. Failed keys: {failed_keys}"
        }
    return {"status_code": 200, "detail": "Metadata uploaded successfully"}

def get_asset_ai_metadata(config, asset_id, max_attempts=3):
    url = f"https://api-{config['hostname']}.overcasthq.com/v1/assets/{asset_id}"
    headers = {
        'x-api-key': config["api_key"]
    }
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code == 200:
                response_data = response.json()
                ai_recognition = (
                    response_data.get("result", {})
                    .get("storages", {})
                    .get("ai_general_recognition", [])
                )
                if ai_recognition and isinstance(ai_recognition, list):
                    ai_file_url = ai_recognition[0].get("file", {}).get("url")
                    if ai_file_url:
                        try:
                            ai_resp = requests.get(ai_file_url, timeout=30)
                            ai_resp.raise_for_status()
                            labels = ai_resp.json()
                            return {"labels": labels}
                        except Exception as e:
                            logging.warning(f"Failed to fetch AI labels JSON: {e}")
                            return {}
                else:
                    logging.info("No AI general recognition data found for asset.")
                    return {}
            else:
                logging.warning(f"Metadata fetch failed: {response.status_code} {response.text}")
        except Exception as e:
            if attempt == max_attempts - 1:
                logging.warning(f"Skipping metadata fetch after {max_attempts} attempts: {e}")
                return {}
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return {}

def send_extracted_metadata(config, repo_guid, file_path, metadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "overcasthq"),
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": flatten_dict(metadata)
        }]
    }
    for _ in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            logging.debug(f"Metadata send response: {r.status_code} {r.text if r else 'No response'}")
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            logging.warning(f"Metadata send error: {r.text if r else str(e)}")
    return False

if __name__ == '__main__':
    # Random delay to avoid thundering herd
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata operations")
    parser.add_argument("--parent-id", help="Optional parent folder ID for path resolution")
    parser.add_argument("-p", "--project-id", help="Project ID for OvercastHQ")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source file path for upload")
    parser.add_argument("-mp", "--metadata-file", help="Path to metadata file (JSON/CSV/XML)")
    parser.add_argument("-up", "--upload-path", help="Target path or ID in Overcast HQ")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB for original upload")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("--dry-run", action="store_true", help="Perform dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level (debug, info, warning, error)")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Treat upload-path as resolved folder ID")
    parser.add_argument("--controller-address", help="Controller IP:Port override")

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
        logging.error(f"Cloud config section '{args.config_name}' not found.")
        sys.exit(1)

    cloud_config_data = cloud_config[args.config_name]

    # Resolve project ID
    if args.project_id:
        project_id = args.project_id
    elif "project_id" in cloud_config_data:
        project_id = cloud_config_data["project_id"]
    else:
        project_id = get_first_project(cloud_config_data)
        if not project_id:
            logging.error("Could not retrieve project ID from OvercastHQ.")
            sys.exit(1)

    if not cloud_config_data.get('hostname'):
        cloud_config_data['hostname'] = "keycode"

    logging.info(f"Starting OvercastHQ upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Project ID: {project_id}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")

    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required.")
            sys.exit(1)
        ai_metadata = get_asset_ai_metadata(cloud_config_data, args.asset_id)
        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if ai_metadata and "labels" in ai_metadata and ai_metadata["labels"]:
            if send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, ai_metadata):
                logging.info("Extracted metadata sent successfully.")
                sys.exit(0)
            else:
                logging.error("Failed to send extracted metadata.")
                sys.exit(1)
        else:
            print(f"Metadata extraction failed for asset: {args.asset_id}")
            sys.exit(7)

    if mode == "get_base_target":
        if args.resolved_upload_id:
            print(args.upload_path)
            sys.exit(0)

        try:
            base_id = args.parent_id or None
            folder_id = get_folder_id(cloud_config_data, args.upload_path, base_id)
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

    # Size limit check
    if mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
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
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Overcast HQ")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata will be applied from: {args.metadata_file}")
        else:
            logging.warning("[DRY RUN] No metadata file specified.")
        sys.exit(0)

    # Resolve folder ID
    try:
        folder_id = args.upload_path if args.resolved_upload_id else get_folder_id(cloud_config_data, args.upload_path)
        if not folder_id:
            logging.error("Failed to resolve upload folder ID.")
            sys.exit(1)
        logging.info(f"Resolved upload folder ID: {folder_id}")
    except Exception as e:
        logging.critical(f"Folder resolution failed: {e}")
        sys.exit(1)

    # Create asset
    try:
        response, status_code = create_asset(cloud_config_data, project_id, folder_id, matched_file)
        if status_code not in (200, 201):
            logging.error(f"Asset creation failed: {status_code} {response.text if response else 'No response'}")
            sys.exit(1)

        parsed = response.json()
        asset_id = parsed["result"]["uuid"]
        logging.info(f"Asset created successfully. Asset ID: {asset_id}")
    except Exception as e:
        logging.critical(f"Failed to create asset: {e}")
        sys.exit(1)

    # Upload file parts
    try:
        multipart_upload_to_s3(parsed["result"], cloud_config_data, matched_file)
        update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], folder_id, asset_id)
        logging.info("File upload completed successfully.")
    except Exception as e:
        logging.critical(f"File upload failed: {e}")
        sys.exit(1)
    # Upload metadata
    try:
        meta_response = upload_metadata_to_asset(
            hostname=cloud_config_data['hostname'],
            api_key=cloud_config_data['api_key'],
            backlink_url=backlink_url,
            asset_id=asset_id,
            properties_file=args.metadata_file
        )
        if meta_response["status_code"] == 207:
            logging.error(f"Metadata upload partial failure: {meta_response['detail']}")
            print("File uploaded, but some metadata failed.")
            # Don't exit with error — file is uploaded
        elif meta_response["status_code"] != 200:
            logging.error(f"Metadata upload failed: {meta_response['detail']}")
            print("File uploaded, but metadata failed.")
            # Don't exit with error — file is uploaded
        else:
            logging.info("Metadata uploaded successfully.")
    except Exception as e:
        logging.warning(f"Metadata upload encountered error: {e}")

    try:
        ai_metadata = get_asset_ai_metadata(cloud_config_data, asset_id)
        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if ai_metadata and "labels" in ai_metadata and ai_metadata["labels"]:
            if send_extracted_metadata(cloud_config_data, args.repo_guid, catalog_path_clean, ai_metadata):
                logging.info("AI metadata sent successfully.")
            else:
                logging.error("Failed to send AI metadata.")
                sys.exit(1)
        else:
            print(f"Metadata extraction failed for asset: {asset_id}")
            sys.exit(7)
    except Exception as e:
        logging.warning(f"AI metadata extraction/send error: {e}")
        print(f"Metadata extraction failed for asset: {asset_id}")
        sys.exit(7)

    logging.info("OvercastHQ upload process completed.")
    sys.exit(0)