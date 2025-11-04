import requests
import argparse
import sys
import os
import json
import hashlib
import logging
import urllib.parse
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
import random
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 20 * 1024 * 1024
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
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

def calculate_sha1(file_path, chunk_size):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha1.update(chunk)
    # v4 requires SHA-1 as hex string, not bytes
    return sha1.hexdigest()

def get_cloud_config_path():
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

class ConfigTokenStore:
    def __init__(self, config_path, config_section):
        self.config_path = config_path
        self.config_section = config_section

    def read(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file {self.config_path} not found")
        config = ConfigParser()
        config.read(self.config_path)
        if self.config_section not in config:
            raise ValueError(f"Section '{self.config_section}' not found in config")
        token_info = {}
        section = config[self.config_section]

        if 'access_token' in section:
            token_info['access_token'] = section['access_token']
        if 'refresh_token' in section:
            token_info['refresh_token'] = section['refresh_token']

        if not token_info.get('access_token') or not token_info.get('refresh_token'):
             logging.warning("Access token or refresh token missing in individual keys.")
        return token_info

    def write(self, token_info):
        config = ConfigParser()
        config.read(self.config_path)
        if self.config_section not in config:
            config[self.config_section] = {}
        section = config[self.config_section]
        section['access_token'] = token_info.get('access_token', '')
        section['refresh_token'] = token_info.get('refresh_token', '')

        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as configfile:
            config.write(configfile)
        logging.debug(f"Tokens updated in {self.config_path} under [{self.config_section}]")
        logging.info("Tokens automatically refreshed and stored in config.")

    def clear(self):
        pass

class BoxTokenManager:
    def __init__(self, client_id, client_secret, config_path, config_section):
        self.client_id = client_id
        self.client_secret = client_secret
        self.config_path = config_path
        self.config_section = config_section
        self.token_store = ConfigTokenStore(config_path, config_section)
        self.oauth = None
        self.client = None

    def get_authenticated_client(self):
        if self.client:
            try:
                current_user = self.client.user().get()
                logging.debug(f"Token valid for user: {current_user.name} (ID: {current_user.id})")
                return self.client
            except BoxAPIException as e:
                if e.status == 401:
                    logging.info("Access token expired, attempting refresh...")
                    if self._refresh_tokens_internal():
                         return self.client
                    else:
                        logging.error("Automatic token refresh failed.")
                else:
                     logging.error(f"API error checking token validity: {e}")
        try:
            token_info = self.token_store.read()
            if not token_info.get('access_token') or not token_info.get('refresh_token'):
                raise ValueError("Valid tokens not found in configuration")

            # persistent OAuth2 with token refresh callback
            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret,
                access_token=token_info['access_token'],
                refresh_token=token_info['refresh_token'],
                store_tokens=self._store_tokens_callback
            )
            self.client = Client(self.oauth)
            current_user = self.client.user().get()
            logging.info(f"Authenticated with Box as {current_user.name} (ID: {current_user.id})")
            return self.client
        except (FileNotFoundError, ValueError) as e:
            logging.error(f"No stored tokens found or invalid tokens: {e}")
            return None
        except Exception as e:
            logging.error(f"Failed to create authenticated client: {e}", exc_info=True)
            return None

    def _store_tokens_callback(self, access_token, refresh_token):
        token_info = {
            'access_token': access_token,
            'refresh_token': refresh_token
        }
        self.token_store.write(token_info)

    def _refresh_tokens_internal(self):
        try:
            if not self.oauth:
                token_info = self.token_store.read()
                if not token_info.get('access_token') or not token_info.get('refresh_token'):
                    raise ValueError("Cannot refresh: tokens not found in configuration")
                self.oauth = OAuth2(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    access_token=token_info['access_token'],
                    refresh_token=token_info['refresh_token'],
                    store_tokens=self._store_tokens_callback
                )
            self.oauth.refresh()
            self.client = Client(self.oauth)
            logging.info("Tokens manually refreshed successfully.")
            return True
        except Exception as e:
            logging.error(f"Manual token refresh failed: {e}", exc_info=True)
            return False

def prepare_metadata_to_upload( backlink_url, properties_file):
    metadata = {
        "fabric URL": backlink_url
    }
    if not properties_file or not os.path.exists(properties_file):
        logging.warning(f"Metadata file not found: {properties_file}")
        return metadata
    file_ext = properties_file.lower()
    try:
        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata = json.load(f)
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
            else:
                logging.warning("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value
    except Exception as e:
        logging.error(f"Failed to parse {properties_file}: {e}")
    return metadata

def apply_metadata_with_upsert(file_obj, metadata_dict, template='properties', scope='global'):
    try:
        # v4: using add_metadata instead of .metadata().create()
        return file_obj.add_metadata(metadata_dict, scope=scope, template=template)
    except BoxAPIException as e:
        if e.status == 409 and 'conflict' in str(e).lower():
            # v4: update_metadata takes a dict directly, no MetadataUpdate object
            return file_obj.update_metadata(metadata_dict, scope=scope, template=template)
        else:
            logging.error(f"Failed to apply metadata: {e}", exc_info=True)

def get_or_create_folder(client, folder_name, parent_id):
    try:
        parent_folder = client.folder(folder_id=parent_id)
        items = parent_folder.get_items(limit=1000, offset=0)
        for item in items:
            if item.name == folder_name and item.type == 'folder':
                return item.id
        created_folder = parent_folder.create_subfolder(folder_name)
        return created_folder.id
    except Exception as e:
        logging.error(f"[get_or_create_folder] Error in get_or_create_folder('{folder_name}', '{parent_id}'): {e}", exc_info=True)
        raise

def ensure_path(client, path, base_id="0"):
    try:
        current_folder_id = base_id
        segments = path.strip("/").split("/")
        for segment in segments:
            if segment:
                current_folder_id = get_or_create_folder(client, segment, current_folder_id)
        return current_folder_id
    except Exception as e:
        logging.error(f"[ensure_path] Error in ensure_path('{path}', base_id='{base_id}'): {e}", exc_info=True)
        raise

def resolve_conflict_and_prepare_upload(folder, file_name, conflict_resolution):
    items = folder.get_items(limit=1000)
    existing_file = None
    existing_names = set()
    for item in items:
        if item.type == 'file':
            existing_names.add(item.name)
            if item.name == file_name:
                existing_file = item
    if existing_file:
        if conflict_resolution == "skip":
            return existing_file, file_name, True
        elif conflict_resolution == "rename":
            base, ext = os.path.splitext(file_name)
            counter = 1
            new_name = file_name
            while new_name in existing_names:
                new_name = f"{base} ({counter}){ext}"
                counter += 1
            return None, new_name, False
        else:
            return existing_file, file_name, False
    else:
        return None, file_name, False

def upload_file_with_conflict_resolution(client, folder_id, file_path, conflict_resolution):
    logging.debug(f"Starting upload for file: {file_path} to folder ID: {folder_id}")
    try:
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        folder = client.folder(folder_id).get()
        min_upload_session_size = 20000000  # 20 MB
        existing_file, upload_name, skip_upload = resolve_conflict_and_prepare_upload(
            folder, file_name, conflict_resolution
        )
        if skip_upload:
            logging.warning(f"File {file_name} exists and is being skipped (conflict_resolution=skip).")
            return {
                "success": True,
                "file_id": existing_file.id,
                "file_name": existing_file.name,
                "size": getattr(existing_file, 'size', None),
                "skipped": True
            }
        # Small file
        if file_size < min_upload_session_size:
            if existing_file and conflict_resolution in ("overwrite", "new_version"):
                file_obj = client.file(existing_file.id)
                uploaded_file = file_obj.update_contents(file_path)
            else:
                uploaded_file = folder.upload(file_path, file_name=upload_name, file_description=backlink_url)
            return {
                "success": True,
                "file_id": uploaded_file.id,
                "file_name": uploaded_file.name,
                "size": uploaded_file.size
            }
        # Large file (chunked upload)
        else:
            if existing_file and conflict_resolution in ("overwrite", "new_version"):
                # Box v4: For new version of large file, use file.get_chunked_uploader()
                file_obj = client.file(existing_file.id)
                with open(file_path, 'rb') as stream:
                    chunked_uploader = file_obj.get_chunked_uploader(stream, file_size)
                    uploaded_file = chunked_uploader.start()
            else:
                # Box v4: Create upload session via FOLDER
                folder = client.folder(folder_id)
                upload_session = folder.create_upload_session(file_size=file_size, file_name=upload_name)
                chunk_size = upload_session.part_size
                parts = []
                offset = 0
                sha1 = hashlib.sha1()
                with open(file_path, 'rb') as file_stream:
                    while offset < file_size:
                        bytes_to_read = min(chunk_size, file_size - offset)
                        part_data = file_stream.read(bytes_to_read)
                        sha1.update(part_data)
                        part = upload_session.upload_part_bytes(
                            part_data,
                            offset=offset,
                            total_size=file_size
                        )
                        parts.append(part)
                        offset += bytes_to_read
                uploaded_file = upload_session.commit(
                    sha1.digest(),
                    file_attributes={'name': upload_name, 'description': backlink_url},
                )
            return {
                "success": True,
                "file_id": uploaded_file.id,
                "file_name": uploaded_file.name,
                "size": uploaded_file.size
            }
    except BoxAPIException as e:
        logging.error(f"[BoxAPIException] Failed to upload file: {file_path} -> {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        logging.error(f"[Exception] Failed to upload file: {file_path} -> {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def get_shared_link_url(client, file_id, max_attempts=3):
    for attempt in range(max_attempts):
        try:
            file_obj = client.file(file_id)
            file_info = file_obj.get()
            # Check if shared link already exists
            if file_info.shared_link and file_info.shared_link.get('url'):
                return file_info.shared_link['url']

            # Create shared link using get_shared_link (v4 method)
            shared_link_url = file_obj.get_shared_link(
                access='open',
                allow_download=True,
                allow_edit=True
            )
            return shared_link_url

        except BoxAPIException as e:
            logging.error(f"Box API error while fetching shared link for {file_id}: {e}")
            return None
        except Exception as e:
            if attempt == max_attempts - 1:
                logging.critical(f"Failed to get embed url after {max_attempts} attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def update_catalog(repo_guid, file_path, asset_id, folder_id, embed_url=None, max_attempts=5):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": cloud_config_data.get("provider", "box"),
        "providerData": {
            "assetId": asset_id,
            "folderId": folder_id,
            "providerUiLink": f"https://app.box.com/file/{asset_id}",
            "embedURL": embed_url
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("--parent-id", help="Optional parent folder ID to resolve relative upload paths from")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", required=True, help="Path where file will be uploaded to Box ")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
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
    
    logging.info(f"Starting Box upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    logging.debug(f"Upload path: {args.upload_path}")

    token_manager = BoxTokenManager(
        client_id=cloud_config_data['client_id'],
        client_secret=cloud_config_data['client_secret'],
        config_path=cloud_config_path,
        config_section=cloud_config_name
    )

    client = token_manager.get_authenticated_client()
    if not client:
        logging.error("Failed to authenticate with Box. Please ensure tokens are valid or re-authenticate.")
        sys.exit(1)

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
        base_id = args.parent_id or "0"  # default to root only if not provided
        up_id = ensure_path(client, args.upload_path, base_id=base_id)
        print(up_id)
        sys.exit(0)
    
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

    catalog_path = remove_file_name_from_path(catalog_path)
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
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => Box")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to Box in {mode} mode")
    upload_path = args.upload_path
    
    #  upload file along with meatadata
    meta_file = args.metadata_file
    meta_file = args.metadata_file
    logging.info("Preparing metadata to be uploaded ...")
    metadata_obj = prepare_metadata_to_upload(backlink_url, meta_file)
    if metadata_obj is not None:
        parsed = metadata_obj
    else:
        parsed = None
    # upload file
    if args.resolved_upload_id:
        folder_id = upload_path
    else:
        folder_id = ensure_path(client, args.upload_path)
    print(f"Resolved upload path to folder ID: {folder_id}")
    
    conflict_resolution_mode = cloud_config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION).lower()

    asset = upload_file_with_conflict_resolution(client, folder_id, args.source_path, conflict_resolution_mode)
    if asset.get("success"):
        logging.info(f"File uploaded successfully: {args.source_path}")
        embed_url = get_shared_link_url(client, asset.get("file_id"))
        if not embed_url:
            logging.warning("Could not obtain embed URL. Proceeding without it.")
        update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1],asset.get("file_id"), folder_id, embed_url)
        if parsed is not None:
            file_obj = client.file(asset.get("file_id"))
            try:
                apply_metadata_with_upsert(file_obj, parsed)
                logging.info('Metadata applied successfully.')
            except Exception as e:
                logging.error(f"Failed to apply metadata: {e}", exc_info=True)
                logging.warning("File uploaded successfully, but metadata application failed. Continuing as success.")
            print(f"File uploaded successfully: {args.source_path} to folder ID: {folder_id} ")
            sys.exit(0)
        else:
            print(f"File uploaded successfully: {args.source_path} to folder ID: {folder_id}")
            sys.exit(0)
    else:
        logging.error(f"Failed to upload file: {args.source_path}, Error: {asset.get('error', 'Unknown error')}")
        sys.exit(1)
