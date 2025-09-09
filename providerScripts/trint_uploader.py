import os
import sys
import json
import argparse
import logging
import plistlib
import xml.etree.ElementTree as ET
from configparser import ConfigParser
from urllib.parse import urlencode
import magic
import random
import time
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# Constants
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

def create_folder(config_data, folder_name, parent_id=None):
    url = f"{config_data['base_url']}/folders/"
    logger.debug(f"URL :------------------------------------------->  {url}")
    headers = {"accept": "application/json", "content-type": "application/json"}

    payload = {"name": folder_name}
    if parent_id:
        payload["parentId"] = parent_id
    if config_data.get("workspace_id"):
        payload["workspaceId"] = config_data["workspace_id"]

    logging.debug(f"data payload (form-data) -------------------------> {payload}")

    for attempt in range(3):
        try:
            response = requests.post(
                url,
                auth=HTTPBasicAuth(config_data['api_key_id'], config_data['api_key_secret']),
                headers=headers,
                data=json.dumps(payload),
                timeout=(10, 30)
            )
            if response.status_code in (200, 201):
                folder_data = response.json()
                logging.info(f"Created folder '{folder_name}': {folder_data['_id']}")
                return folder_data
            else:
                logging.error(f"Create folder failed '{folder_name}': {response.status_code} {response.text}")
                if attempt == 2:
                    return None
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to create folder '{folder_name}' after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def list_all_folders(config_data):
    base_url = f"{config_data['base_url']}/folders/"
    params = {"workspace-id": config_data["workspace_id"]} if config_data.get("workspace_id") else {}
    url = f"{base_url}?{urlencode(params)}" if params else base_url
    logger.debug(f"URL :------------------------------------------->  {url}")

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                auth=HTTPBasicAuth(config_data['api_key_id'], config_data['api_key_secret']),
                headers={"accept": "application/json"},
                timeout=(10, 30)
            )
            if response.status_code in (200, 201):
                return response.json()
            else:
                logging.error(f"List folders failed: {response.status_code} {response.text}")
                if attempt == 2:
                    return []
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to list folders after 3 attempts: {e}")
                return []
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return []

def get_folder_id(config_data, upload_path, base_id=None):
    upload_path = upload_path.strip("/")
    if not upload_path:
        return base_id or None

    parts = [p for p in upload_path.split("/") if p]
    folders = list_all_folders(config_data)
    has_workspace = bool(config_data.get("workspace_id"))

    if has_workspace:
        # Map by parent
        current_parent = base_id
        for part in parts:
            # Find child folder with given name and current parent
            found = next(
                (f for f in folders if f["name"] == part and f.get("parent") == current_parent),
                None
            )
            if not found:
                # Create folder under current parent
                found = create_folder(config_data, part, parent_id=current_parent)
                folders.append(found)
            current_parent = found["_id"]
        return current_parent

    else:
        # Names are full paths, so build map
        path_map = {f["name"]: f for f in folders}
        current_parent = base_id
        matched_index = -1

        # Match longest existing path segment
        for i in range(len(parts)):
            current_path = "/".join(parts[:i+1])
            if current_path in path_map:
                current_parent = path_map[current_path]["_id"]
                matched_index = i
            else:
                break

        # Create missing folders
        for part in parts[matched_index+1:]:
            new_folder = create_folder(config_data, part, parent_id=current_parent)
            current_parent = new_folder["_id"]
            path_map["/".join(parts[:matched_index+1])] = new_folder
            matched_index += 1

        return current_parent

def create_asset(config_data, file_path, folder_id=None, workspace_id=None, language="en"):
    key_id = config_data.get("api_key_id")
    key_secret = config_data.get("api_key_secret")
    base_upload_url = (config_data.get("upload_url") or "https://upload.trint.com").strip()

    file_name = os.path.basename(file_path)
    mime_type = magic.from_file(file_path, mime=True)
    if not mime_type:
        logging.critical(f"Could not detect MIME type for '{file_name}'")
        return None, 400

    url = base_upload_url
    logger.debug(f"URL :------------------------------------------->  {url}")

    data = {"filename": file_name, "language": language}
    if folder_id:
        data["folder-id"] = folder_id

    # workspace-id must be sent as a query parameter, not in the form data
    params = {}
    if workspace_id:
        params["workspace-id"] = workspace_id

    files = {"file": (file_name, open(file_path, "rb"), mime_type)}

    for attempt in range(3):
        try:
            with open(file_path, "rb") as f:
                files = {"file": (file_name, f, mime_type)}
                logging.info(f"Uploading file '{file_name}' to Trint...")
                response = requests.post(
                    url,
                    auth=HTTPBasicAuth(key_id, key_secret),
                    data=data,
                    files=files,
                    params=params,
                    timeout=(10, 60)
                )

            if response.status_code == 200:
                logging.info("Upload successful.")
                logging.debug(f"Response: {response.text}")
                return response, 200
            else:
                logging.error(f"Upload failed: {response.status_code} {response.text}")
                if attempt == 2:
                    return response, response.status_code
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Upload failed after 3 attempts: {e}")
                return None, 500
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
        finally:
            if 'f' in locals() and not f.closed:
                f.close()
    return None, 500

def update_catalog(repo_guid, file_path, workspace_id, folder_id, asset_id, max_attempts=5):
    url = "http://127.0.0.1:5080/catalogs/provideData"
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
        "providerName": "trint",
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "workspaceId": workspace_id,
            "providerUiLink": f"https://app.trint.com/editor/{asset_id}"
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

if __name__ == '__main__':
    # Random delay to avoid thundering herd
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Cloud config name")
    parser.add_argument("-j", "--job-guid", help="Job GUID")
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
    if not cloud_config_data.get("base_url"):
        cloud_config_data["base_url"] = cloud_config_data.get("domain", "https://api.trint.com").strip()

    logging.info(f"Starting Trint upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")

    if mode == "get_base_target":
        if args.resolved_upload_id:
            print(args.upload_path)
            sys.exit(0)

        folder_id = get_folder_id(cloud_config_data, args.upload_path, base_id=args.parent_id)
        if not folder_id:
            logging.error("Failed to resolve upload target folder ID.")
            sys.exit(1)

        print(folder_id)
        sys.exit(0)

    matched_file = args.source_path
    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    matched_file_size = os.path.getsize(matched_file)

    if args.size_limit and mode == "original":
        try:
            limit_mb = float(args.size_limit)
            if matched_file_size > limit_mb * 1024 * 1024:
                logging.error(f"File too large: {matched_file_size / 1024 / 1024:.2f} MB > {limit_mb} MB")
                sys.exit(4)
        except ValueError:
            logging.warning("Invalid size limit format.")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Trint")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    # Resolve upload folder
    try:
        folder_id = args.upload_path if args.resolved_upload_id else get_folder_id(cloud_config_data, args.upload_path, base_id=args.parent_id)
        if not folder_id:
            logging.error("Failed to resolve upload folder ID.")
            sys.exit(1)
        logging.info(f"Upload location ID: {folder_id}")
    except Exception as e:
        logging.critical(f"Folder resolution failed: {e}")
        sys.exit(1)

    # Upload asset
    try:
        workspace_id = cloud_config_data.get("workspace_id")
        response, status_code = create_asset(
            config_data=cloud_config_data,
            file_path=matched_file,
            folder_id=folder_id,
            workspace_id=workspace_id
        )
        if status_code == 200 and isinstance(response, requests.Response):
            resp_json = response.json()
            trint_id = resp_json.get("trintId")
            if trint_id:
                update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], workspace_id, folder_id, trint_id)
                logging.info(f"asset upload completed ==============================> {trint_id}")
                sys.exit(0) 
            else:
                logging.error("Upload succeeded but 'trintId' missing in response.")
                sys.exit(1)
        else:
            error_detail = response.text if isinstance(response, requests.Response) else "No response"
            logging.error(f"Upload failed: {error_detail}")
            sys.exit(1)
    except Exception as e:
        logging.critical(f"Upload failed due to unexpected error: {e}")
        sys.exit(1)

sys.exit(0)         