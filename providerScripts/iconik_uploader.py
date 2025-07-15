import os
import sys
import json
import math
import hashlib
import argparse
import logging
import urllib.parse
import requests
from json import dumps
from configparser import ConfigParser
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
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        collections = response.json().get('objects', [])
        if collections:
            collection_id = collections[0].get('id')
            logging.info(f"Using first collection ID: {collection_id}")
            return collection_id
        else:
            logging.error("No collections found in the response.")
            sys.exit(1)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch collections: {e}")
        sys.exit(1)

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
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        params=params
    )
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    return response.json()['id']

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
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("objects", [])
    except requests.RequestException as e:
        logging.error(f"Failed to fetch collection contents for ID {collection_id}: {e}")
        return []

def get_storage_id(config, storage_name,storage_method):
    url = f'{DOMAIN}/API/files/v1/storages/'
    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    response = response.json()
    storage_id = []
    for storage in response["objects"]:
        if storage["name"] == storage_name and storage["method"] == storage_method:
            storage_id.append(storage["id"])
    return storage_id[0]

def create_asset_id(config, file_name, collection_id=None):
    url = f'{DOMAIN}/API/assets/v1/assets/'
    payload = {"title": file_name, "type": "ASSET"}
    params = {"apply_default_acls": "true"}

    if collection_id:
        payload["collection_id"] = collection_id
        params = {"apply_default_acls": "false", "apply_collection_acls": "true"}

    headers = {
        "app-id": config["app-id"],
        "auth-token": config["auth-token"]
    }
    try:
        response = requests.post(url, headers=headers, json=payload, params=params)
        response.raise_for_status()
        if response.status_code != 201:
            print(f"Response error. Status - {response.status_code}, Error - {response.text}")
            sys.exit(1)
        response = response.json()
        return response['id'], response['created_by_user']
    except requests.RequestException as e:
        logging.error(f"Failed to create asset: {e} - {getattr(e.response, 'text', '')}")
        return None, getattr(e.response, 'status_code', None)
    
def add_asset_in_collection(config, asset_id, collection_id):
    url = f'{DOMAIN}/API/assets/v1/collections/{collection_id}/contents'
    payload = {"object_id":asset_id,"object_type":"assets"}
    params = {"index_immediately":"true"}
    headers = {"app-id":config["app-id"],
            "auth-token" : config["auth-token"]
            }
    response = requests.post(url, headers=headers, json=payload,params=params)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    return True

def collection_fullpath(config,collection_id):
    url = f'{DOMAIN}/API/assets/v1/collections/{collection_id}/full/path'
    headers = {"app-id":config["app-id"],
        "auth-token" : config["auth-token"]
        }
    params = {"get_upload_path": "true"}
    response = requests.get(url, headers=headers,params=params)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return '' if "errors" in response else response

def create_format_id(config,asset_id, user_id):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/formats/'
    payload = {"user_id": user_id,"name": "ORIGINAL","metadata": [{"internet_media_type": "text/plain"}],"storage_methods": [config["method"]]}
    headers = {"app-id":config["app-id"],
               "auth-token" : config["auth-token"]
               }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response['id']

def create_fileset_id(config, asset_id, format_id, file_name, storage_id,upload_path):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/file_sets/'
    payload = {"format_id": format_id,"storage_id": storage_id,"base_dir": upload_path,"name": file_name,"component_ids": []}
    headers = {"app-id":config["app-id"],
               "auth-token" : config["auth-token"]
               }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response['id']

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
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response['upload_url'], response['id']

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
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response['multipart_upload_url'], response['id']

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
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response['upload_url'], response['id'],response["upload_credentials"]["authorizationToken"],response["upload_filename"]

def get_upload_id_s3(upload_url):
    response = requests.post(upload_url)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    root = ET.fromstring(response.text)
    namespace = root.tag.split('}')[0] + '}'
    upload_id = root.find(f'{namespace}UploadId').text
    return upload_id

def get_part_url_s3(config, asset_id, file_id, upload_id):
    part_url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/part/'
    params = {"parts_num": "1", "upload_id": upload_id, "per_page": "100", "page": "1"}
    headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] }
    response = requests.get(part_url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    response = response.json()
    return response["objects"][0]["url"]

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
    resp_JSON = requests.post(upload_url, headers=google_headers)
    if resp_JSON.status_code != 201:
        print(f"Response error. Status - {upload_url.status_code}, Error - {upload_url.text}")
        sys.exit(1)
    upload_id = resp_JSON.headers['X-GUploader-Uploadid']

    google_headers = {
        'content-length': str(file_size),
        'x-goog-resumable': 'upload'
    }

    with open(file_path, 'rb') as f:
        full_upload_url = f"{upload_url}&upload_id={upload_id}"
        x = requests.put(full_upload_url, headers=google_headers, data=f)
        if x.status_code != 200:
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            sys.exit(1)
        return x

def upload_file_s3(config, part_url, file_path, upload_id, asset_id, file_id):
    with open(file_path, 'rb') as file:
        response = requests.put(part_url, data=file)
        if response.status_code != 200:
            print(f"Response error. Status - {response.status_code}, Error - {response.text}")
            sys.exit(1)
        etag = response.headers['etag']
    multipart_url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/'
    complete_url_response = requests.get(
        multipart_url,
        headers = { "app-id":config["app-id"], "auth-token" : config["auth-token"] },
        params={"upload_id": upload_id, "type": "complete_url"}
    )
    if complete_url_response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    complete_url_response =complete_url_response.json()
    complete_url = complete_url_response['complete_url']
    xml_payload = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <CompleteMultipartUpload>
    <Part>
        <ETag>{etag}</ETag>
        <PartNumber>1</PartNumber>
    </Part>
    </CompleteMultipartUpload>'''.format(etag=etag)
    headers = {'Content-Type': 'application/xml'}
    response = requests.post(complete_url, data=xml_payload, headers=headers)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        sys.exit(1)
    return response

def upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file):
    headers = {
               "Authorization" : authorizationToken,
               "X-Bz-File-Name" : upload_filename,
               "X-Bz-Content-Sha1" : sha1_of_file,
               "Content-Type" : "b2/x-auto"
               }
    
    with open(file_path, 'rb') as f:
        x = requests.post(upload_url,headers=headers,data=f)
        if x.status_code != 200:
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            sys.exit(1)
        return x

def upload_file_azure(upload_url, file_path):
    headers = { "x-ms-blob-type" : "BlockBlob"}
    with open(file_path, 'rb') as file:
        x = requests.put(upload_url, data=file,headers=headers)
        if x.status_code != 200:
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            sys.exit(1)
        return x

def file_status_update(config, asset_id, file_id):
    url = f'{DOMAIN}/API/files/v1/assets/{asset_id}/files/{file_id}/'
    headers = {"app-id":config["app-id"],
            "auth-token" : config["auth-token"]
            }
    response = requests.patch(url, headers=headers, json={"status": "CLOSED", "progress_processed": 100})
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        exit(1)
    return response



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("--collection-id", help="Collection ID to resolve relative upload paths from") 
    parser.add_argument("--parent-id", help="Optional parent collection ID to resolve relative upload paths from") 
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded google Drive")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Pass if upload path is already resolved collection ID")
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
    
    # set collection ID if provided if not check from cloudconfig data block and if not present there need to use api of get collection and take the id of first collection we get
    if args.collection_id:
        collection_id = args.collection_id
    elif "collection_id" in cloud_config_data:
        collection_id = cloud_config_data["collection_id"]
    else:
        collection_id = get_first_collection(cloud_config_data)
        
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
        base_id = args.parent_id or collection_id
        up_id = find_or_create_collection_path(cloud_config_data, upload_path, base_id) if '/' in upload_path else upload_path
        print(up_id)
        sys.exit(0)
    
    matched_file = args.source_path
    catalog_path = args.catalog_path
    upload_path = remove_file_name_from_path(args.upload_path)

    if collection_id is None or matched_file is None:
        print(f'Collection id and source file both are required for upload')
        sys.exit(1)

    logging.info(f"Starting S3 upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")

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
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => google Drive")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Google Drive in {mode} mode")
 
    storage_name = cloud_config_data["name"] if not cloud_config_data["name"] is None and len(cloud_config_data["name"]) != 0 else "iconik-files-gcs"
    storage_method = cloud_config_data["method"] if not cloud_config_data["method"] is None and len(cloud_config_data["method"]) != 0 else "GCS"
    
    if args.upload_path and args.resolved_upload_id:
        collection_to_use = args.upload_path
    elif args.upload_path and not args.resolved_upload_id:
        collection_to_use = find_or_create_collection_path(cloud_config_data, args.upload_path, collection_id)
    else:
        collection_to_use = collection_id
    
    if storage_method and storage_name:
        collection = collection_to_use
        file_name = extract_file_name(matched_file)
        storage_id = get_storage_id(cloud_config_data, storage_name,storage_method)
        asset_id, user_id = create_asset_id(cloud_config_data, file_name,collection)
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