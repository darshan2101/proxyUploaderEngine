import os
import sys
import argparse
import json
from datetime import datetime
import logging
import urllib.parse
import requests
from configparser import ConfigParser
import plistlib
import hashlib
import urllib.parse
import xml.etree.ElementTree as ET
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Constants
VALID_MODES = ["proxy", "original", "get_base_target"]
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
  
def create_folder(config_data, name, project_id, parent_id=None):
    logging.info(f"Creating folder: {name}, project: {project_id}, parent: {parent_id}")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/folders"

    headers = {
        'x-api-key': config_data["api_key"]
    }

    payload = {
        "name": name,
        "project": project_id,
    }
    if parent_id is not None:
        payload["parent"] = parent_id
    logging.debug(f"data payload (form-data) -------------------------> {payload}")
    response = requests.post(url, headers=headers, data=payload)
    logging.info(f"Response for folder creation: {response.text}")

    if response.status_code in (200, 201):
        resp_json = response.json()
        uuid = resp_json.get("uuid") or resp_json.get("result", {}).get("uuid")
        if uuid:
            return uuid
        else:
            logging.error("UUID not found in folder creation response")
            raise RuntimeError("UUID not found in folder creation response")
    else:
        logging.error(f"Failed to create Folder: {response.status_code} {response.text}")
        raise RuntimeError(f"Failed to create Folder: {response.status_code} {response.text}")

def list_all_folders(config_data, project_id, parent_id):
    logging.debug(f"parameter to get folder tree =================> {parent_id}")
    headers = { 'x-api-key': config_data["api_key"]}
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/folders"
    all_folders = []

    params = {
        "project": project_id
    }
    if parent_id is not None:
        params["parent"] = parent_id
    logging.debug(f"Fetching folders from: {url}")
    response = requests.get( url, headers=headers, params=params)
    # 
    if response.status_code in (200, 201):
        logging.info("File Tree Successfully")
        data = response.json().get("result", {})
    else:
        logging.error("Failed to get File Tree")

    all_folders.extend(data['items'])
    return all_folders

def get_folder_id(config_data, upload_path, base_id = None):
    project_id = config_data['project_id']
    current_parent_id = base_id

    # Remove the filename from the upload_path
    folder_path = os.path.dirname(upload_path)
    logging.info(f"Finding or creating folder path: '{folder_path}'")

    for segment in folder_path.strip("/").split("/"):
        if not segment:
            continue
        logging.debug(f"Looking for folder '{segment}' under parent '{current_parent_id}'")

        folders = list_all_folders(config_data, project_id, current_parent_id)
        matched = next((f for f in folders if f.get("name") == segment), None)

        if matched:
            current_parent_id = matched["uuid"]
            logging.debug(f"Found existing folder '{segment}' (ID: {current_parent_id})")
        else:
            current_parent_id = create_folder(config_data, segment, project_id, current_parent_id)
            logging.debug(f"Created folder '{segment}' (ID: {current_parent_id})")
    logging.debug(f"UUID  after tree traversal/creation ---------------> {current_parent_id}")
    return current_parent_id

def create_asset(config_data, project_id, folder_id, file_path):
    logging.debug(f"Folder ID check 2-------------------------> {folder_id}")
    url = f"https://api-{config_data['hostname']}.overcasthq.com/v1/assets"

    headers = {
    'x-api-key': config_data["api_key"]
    }
    payload = {
        "project" : project_id,
        "file_name" : extract_file_name(file_path),
        "file_size" : os.path.getsize(file_path),
        "folder" : folder_id
    }
    logging.debug(f"URL: {url}")
    response = requests.post(url, headers=headers,data=payload)
    logging.debug(f"response from API: {response.text}")
    if response.status_code != 201:
        logging.error("failed to create asset")       
        logging.debug(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    response = response.json()
    return response


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



def upload_metadata_to_asset(hostname, api_key, backlink_url, asset_id, properties_file):
    logging.info(f"Updating asset {asset_id} with properties from {properties_file}")
    
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

    headers = {
        'x-api-key': api_key
    }
    logging.debug("Sending asset update request to Overcast API (form-data)")

    url = f"https://api-{hostname}.overcasthq.com/v1/assets/{asset_id}/metadata"
    for key, value in metadata.items():
        payload = {
            "key": key,
            "value": value
        }
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code in (200, 201):
            response_json = response.json()
            logging.info(f"Added data successfully: {response_json}")
        else:
            logging.debug(f"Response error. Status - {response.status_code}, Error - {response.text}")
            logging.error("Failed to upload Metadata file: %s", response.text)

    logging.info("Asset metadata insertion completed")




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--jobId", help="Job Id of SDNA job")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from")
    parser.add_argument("-cp", "--catalog-path", required=True, help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", required=True, help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Pass if upload path is already resolved ID")
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

    project_id = cloud_config_data['project_id']
    logging.debug(f"Project id ----------------------->{project_id}")
    
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
        up_id = get_folder_id(upload_path, cloud_config_data, base_id) if '/' in upload_path else upload_path
        print(up_id)
        sys.exit(0)

    logging.info(f"Starting OvercastHQ upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    logging.info(f"Initialized OvercastHQ client for project: {project_id}")
    
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
    catalog_url = urllib.parse.quote(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    jobId = args.jobId
    client_ip, client_port = get_link_address_and_port()

    backlink_url = f"https://{client_ip}:{client_port}/dashboard/projects/{jobId}/browse&search?path={catalog_url}&filename={filename_enc}"
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

    folder_id = get_folder_id(cloud_config_data, upload_path)
    logging.debug(f"Folder ID check -------------------------> {folder_id}")

    asset = create_asset(cloud_config_data, project_id, folder_id, args.source_path)
    asset_id = asset['result']['uuid']
    logging.info(f"Asset upload Initiated. Asset id: {asset_id}")

    multipart_upload_to_s3(asset['result'], cloud_config_data, args.source_path)
    logging.info(f"asset upload completed")

    meta_file = args.metadata_file
    if meta_file:
        logging.info("Applying metadata to uploaded asset...")

        upload_metadata_to_asset(cloud_config_data['hostname'] ,cloud_config_data['api_key'], backlink_url, asset_id, meta_file)
        
    sys.exit(0)