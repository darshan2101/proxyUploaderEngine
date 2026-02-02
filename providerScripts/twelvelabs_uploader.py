import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
from configparser import ConfigParser
import random
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet", "send_extracted_metadata"]
CHUNK_SIZE = 5 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/twelvelabs_metadata_normalizer.py"

# SDNA Event Map for enriched metadata
SDNA_EVENT_MAP = {
    "twelvelabs_labels": "labels",
    "twelvelabs_keywords": "keywords",
    "twelvelabs_summary": "summary",
    "twelvelabs_highlights": "highlights",
    "twelvelabs_transcript": "transcript",
}

DEFAULT_TWELVELABS_CONFIG = {
    "fetch_transcript": True,
    "fetch_embeddings": True,
    "embedding_options": ["visual", "audio", "transcription"],
    "extraction_schemas": [
        {
            "name": "gist",
            "prompt": "Generate title, appropriate topics according to content, and suitable hashtags for this video.",
            "temperature": 0.3,
            "max_tokens": 150,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "topics": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "hashtags": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["title", "topic", "hashtags"]
                }
            }
        },
        {
            "name": "summary",
            "prompt": "Generate detailed summary of this video containing content and keypoints and give suitable title.",
            "temperature": 0.3,
            "max_tokens": 300,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                        "key_points": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["title", "content"]
                }
            }
        },
        {
            "name": "chapter",
            "prompt": "Generate a list of chapter titles and timestamps for this video.",
            "temperature": 0.3,
            "max_tokens": 400,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "chapters": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "title": {"type": "string"},
                                    "brief": {"type": "string"},
                                    "timestamp": {"type": "string"}
                                },
                                "required": ["title", "brief", "timestamp"]
                            }
                        }
                    },
                    "required": ["chapters"]
                }
            }
        },
        {
            "name": "highlights",
            "prompt": "Generate a list of key highlights from the video content.",
            "temperature": 0.3,
            "max_tokens": 700,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "highlights": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "start": {"type": "string"},
                                    "end": {"type": "string"},
                                    "highlight": {"type": "string"},
                                    "highlight_summary": {"type": "string"},
                                    "highlight_sentiment": {"type": "string"},
                                    "highlight_index": {"type": "integer"}
                                },
                                "required": ["highlight", "start", "end", "highlight_index"]
                            }
                        }
                    }
                },
            }
        }
    ],
}

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logger = logging.getLogger()

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")

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

def get_admin_dropbox_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            log_path = parser.get('General', 'LogPath', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                log_path = str(cfg.get("LogPath", "")).strip()
        if log_path:
            logging.info(f"Admin dropbox path from LogPath: {log_path}")
            return log_path
        else:
            logging.warning("LogPath not found in DNAClientServices config")
            return None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def get_advanced_ai_config(config_name, provider_name):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        logging.warning("Admin dropbox path not available, using default Twelve Labs config")
        return DEFAULT_TWELVELABS_CONFIG if provider_name.lower() == "twelvelabs" else None
    
    # Try config-specific file first
    config_file_path = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    logging.debug(f"Checking for config-specific AI settings at: {config_file_path}")
    
    if os.path.exists(config_file_path):
        try:
            with open(config_file_path, 'r') as f:
                config_data = json.load(f)
            
            if not isinstance(config_data, dict):
                logging.error(f"Invalid JSON format in {config_file_path}: expected object/dict, got {type(config_data).__name__}")
            else:
                logging.info(f"Loaded advanced AI config from: {config_file_path}")
                return config_data
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file {config_file_path}: {e}")
        except IOError as e:
            logging.error(f"Error reading config file {config_file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error reading config file {config_file_path}: {e}")
    else:
        logging.debug(f"Config-specific file not found: {config_file_path}")
    
    # Fallback to provider sample file
    sample_file_path = os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    logging.debug(f"Checking for provider sample at: {sample_file_path}")
    
    if os.path.exists(sample_file_path):
        try:
            with open(sample_file_path, 'r') as f:
                sample_data = json.load(f)
            
            if not isinstance(sample_data, dict):
                logging.error(f"Invalid JSON format in {sample_file_path}: expected object/dict, got {type(sample_data).__name__}")
            else:
                logging.info(f"Loaded advanced AI config from provider sample: {sample_file_path}")
                return sample_data
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in sample file {sample_file_path}: {e}")
        except IOError as e:
            logging.error(f"Error reading sample file {sample_file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error reading sample file {sample_file_path}: {e}")
    else:
        logging.debug(f"Provider sample file not found: {sample_file_path}")
    
    # Final fallback to hardcoded default for Twelve Labs
    if provider_name.lower() == "twelvelabs":
        logging.info("Using hardcoded default Twelve Labs AI config")
        return DEFAULT_TWELVELABS_CONFIG
    
    logging.warning(f"No advanced AI config found for provider '{provider_name}'")
    return None

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

def get_metadata_store_path():
    host, path = "", ""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            host = parser.get('General', 'MetaXtendHost', fallback='').strip()
            path = parser.get('General', 'MetaXtendPath', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                host = str(cfg.get("MetaXtendHost", "")).strip()
                path = str(cfg.get("MetaXtendPath", "")).strip()
    except Exception as e:
        logging.error(f"Error reading Metadata Store or MetaXtend settings: {e}")
        sys.exit(5)

    if not host or not path:
        logging.info("MetaXtend settings not found.")
        sys.exit(5)

    return host, path

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        # Read ai-config.json file
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
        logging.error("Store settings not found in ai-config.json")
        sys.exit(5)

    return metadata_store_path, proxy_store_path

def get_twelvelabs_normalized_metadata(raw_json_path, norm_json_path):
    try:
        with open(raw_json_path, 'r') as f:
            raw_data = json.load(f)
        
        # TODO: Implement your normalization logic here
        # This is a placeholder that just copies the raw data
        normalized_data = raw_data
        
        with open(norm_json_path, 'w') as f:
            json.dump(normalized_data, f, indent=4)
        
        return True
    except Exception as e:
        logging.error(f"Normalization failed: {e}")
        return False

def get_retry_session(retries=3, backoff_factor_range=(1.0, 2.0)):
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):
        try:
            response = session.request(method, url, **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None

def prepare_metadata_to_upload(repo_guid, relative_path, file_name, file_size, backlink_url, properties_file=None):    
    metadata = {
        "relativePath": relative_path if relative_path.startswith("/") else "/" + relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url,
        "fabric_size": file_size
    }
    
    if not properties_file or not os.path.exists(properties_file):
        if properties_file:
            logging.error(f"Properties file not found: {properties_file}")
        return metadata
    
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

# -----------------------------
# Multipart Upload Helpers
# -----------------------------
def create_multipart_upload_session(api_key, filename, file_size):
    url = "https://api.twelvelabs.io/v1.3/assets/multipart-uploads"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    payload = {"filename": filename, "type": "video", "total_size": file_size}

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if not response or response.status_code != 201:
                raise RuntimeError(f"Multipart session creation failed: {response.status_code if response else 'No response'}")
            data = response.json() or {}
            return data["upload_id"], data["asset_id"], data["chunk_size"], data.get("total_chunks"), data["upload_urls"]
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} to create multipart upload session failed: {e}")
            if attempt < 2:
                time.sleep([1, 3, 10][attempt])
            else:
                logging.error("All attempts to create multipart upload session have failed.")
                raise

def upload_single_chunk(chunk_file, presigned_url, chunk_index):
    headers = {'Content-Type': 'application/octet-stream'}
    for attempt in range(3):
        try:
            with open(chunk_file, 'rb') as f:
                resp = requests.put(presigned_url, data=f, headers=headers)
            resp.raise_for_status()
            etag = resp.headers.get('ETag', '').strip('"').strip()
            if not etag:
                raise ValueError(f"No ETag received for chunk {chunk_index}")
            return etag
        except (RequestException, Exception) as e:
            logging.warning(f"Attempt {attempt + 1} to upload chunk {chunk_index} failed: {e}")
            if attempt < 2:
                time.sleep([1, 3, 10][attempt])
            else:
                logging.error(f"All attempts to upload chunk {chunk_index} have failed.")
                raise

def get_additional_presigned_urls(api_key, upload_id, start, count):
    url = f"https://api.twelvelabs.io/v1.3/assets/multipart-uploads/{upload_id}/presigned-urls"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    payload = {"start": start, "count": count}

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if not response or response.status_code != 200:
                raise RuntimeError(f"Failed to get additional presigned URLs: {response.status_code if response else 'No response'}")
            
            data = response.json()
            upload_urls = data.get("upload_urls", [])
            logging.info(f"Retrieved {len(upload_urls)} presigned URLs starting from chunk {start}")
            return upload_urls
            
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} to get additional presigned URLs failed: {e}")
            if attempt < 2:
                time.sleep([1, 3, 10][attempt])
            else:
                logging.error("All attempts to get additional presigned URLs have failed.")
                raise

def report_completed_chunks(api_key, upload_id, completed_chunks, batch_size=20):
    url = f"https://api.twelvelabs.io/v1.3/assets/multipart-uploads/{upload_id}"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    
    for chunk in completed_chunks:
        if not isinstance(chunk.get('chunk_index'), int):
            raise ValueError(f"Invalid chunk_index: {chunk.get('chunk_index')}")
        if not chunk.get('proof'):
            raise ValueError(f"Missing proof for chunk {chunk.get('chunk_index')}")
        if chunk.get('proof_type') != 'etag':
            raise ValueError(f"Invalid proof_type for chunk {chunk.get('chunk_index')}: {chunk.get('proof_type')}")
    
    total_reported = 0
    for i in range(0, len(completed_chunks), batch_size):
        batch = completed_chunks[i:i+batch_size]
        payload = {"completed_chunks": batch}
        
        logging.info(f"Reporting batch of {len(batch)} chunks (total: {total_reported + len(batch)}/{len(completed_chunks)})")
        logging.debug(f"Batch range: chunks {batch[0]['chunk_index']} to {batch[-1]['chunk_index']}")

        for attempt in range(3):
            try:
                response = make_request_with_retries("POST", url, headers=headers, json=payload)
                
                if response is None:
                    raise RuntimeError("No response received from server")
                
                if response.status_code == 200:
                    data = response.json()
                    total_reported += data.get('processed_chunks', 0)
                    logging.info(f"Batch reported: processed={data.get('processed_chunks')}, duplicates={data.get('duplicate_chunks')}, total_completed={data.get('total_completed')}")
                    break
                else:
                    error_body = response.text
                    logging.error(f"Failed to report batch. Status: {response.status_code}, Response: {error_body}")
                    raise RuntimeError(f"Failed to report batch: HTTP {response.status_code} - {error_body}")
                    
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} to report batch failed: {e}")
                if attempt < 2:
                    time.sleep([1, 3, 10][attempt])
                else:
                    logging.error("All attempts to report batch have failed.")
                    raise
    
    logging.info(f"All chunks reported successfully. Total: {total_reported}")

def upload_file_chunks(api_key, file_path, upload_id, asset_id, chunk_size, upload_urls, max_workers=2):
    chunk_files = []
    chunk_dir = os.path.join(os.path.dirname(file_path), f"{os.path.basename(file_path)}.chunks")
    os.makedirs(chunk_dir, exist_ok=True)
    
    logging.debug(f"Splitting file into chunks of size {chunk_size} bytes")
    with open(file_path, 'rb') as f:
        i = 1
        while True:
            data = f.read(chunk_size)
            if not data: 
                break
            cf = os.path.join(chunk_dir, f"chunk_{i:04d}")
            with open(cf, 'wb') as out:
                out.write(data)
            chunk_files.append(cf)
            i += 1
    
    total_chunks = len(chunk_files)
    logging.info(f"File split into {total_chunks} chunks")
    
    url_map = {}
    for u in upload_urls:
        chunk_idx = u["chunk_index"]
        url_map[chunk_idx] = u["url"]
        logging.debug(f"Initial URL for chunk {chunk_idx}")
    
    logging.info(f"Received {len(url_map)} initial presigned URLs for chunks: {sorted(url_map.keys())}")
    
    missing_chunks = []
    for idx in range(1, total_chunks + 1):
        if idx not in url_map:
            missing_chunks.append(idx)
    
    if missing_chunks:
        logging.info(f"Need to fetch URLs for {len(missing_chunks)} additional chunks")
        
        batch_size = 50
        for i in range(0, len(missing_chunks), batch_size):
            batch = missing_chunks[i:i+batch_size]
            start_chunk = batch[0]
            count = len(batch)
            
            logging.info(f"Requesting {count} URLs starting from chunk {start_chunk}")
            extra_urls = get_additional_presigned_urls(api_key, upload_id, start_chunk, count)
            
            for u in extra_urls:
                chunk_idx = u["chunk_index"]
                url_map[chunk_idx] = u["url"]
                logging.debug(f"Added URL for chunk {chunk_idx}")
            
            logging.debug(f"URL map now has {len(url_map)} total URLs")
    
    missing = []
    for idx in range(1, total_chunks + 1):
        if idx not in url_map:
            missing.append(idx)
    
    if missing:
        raise RuntimeError(
            f"Missing presigned URLs for {len(missing)} chunks: {missing[:10]}{'...' if len(missing) > 10 else ''}. "
            f"Have URLs for: {sorted(url_map.keys())}"
        )
    
    logging.info(f"All {total_chunks} presigned URLs ready. Starting parallel upload.")
    
    completed_chunks = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {}
        for idx, cf in enumerate(chunk_files):
            chunk_index = idx + 1
            future = executor.submit(upload_single_chunk, cf, url_map[chunk_index], chunk_index)
            future_to_chunk[future] = (chunk_index, cf)
        
        for future in as_completed(future_to_chunk):
            chunk_index, cf = future_to_chunk[future]
            try:
                etag = future.result()
                completed_chunks.append({
                    "chunk_index": chunk_index,
                    "proof": etag,
                    "proof_type": "etag",
                    "chunk_size": os.path.getsize(cf)
                })
                logging.info(f"Chunk {chunk_index}/{total_chunks} uploaded successfully (ETag: {etag[:8]}...)")
            except Exception as e:
                logging.error(f"Failed to upload chunk {chunk_index}: {e}")
                raise
    
    logging.info(f"All {total_chunks} chunks uploaded. Reporting to Twelve Labs.")
    report_completed_chunks(api_key, upload_id, completed_chunks)
    
    for cf in chunk_files:
        try: 
            os.remove(cf)
            logging.debug(f"Removed chunk file: {cf}")
        except Exception as e:
            logging.warning(f"Could not remove chunk file {cf}: {e}")
    
    try: 
        os.rmdir(chunk_dir)
        logging.debug(f"Removed chunk directory: {chunk_dir}")
    except Exception as e:
        logging.warning(f"Could not remove chunk directory {chunk_dir}: {e}")
    
    logging.info("Chunk upload and cleanup completed successfully")

def poll_upload_session_status(api_key, upload_id, max_wait=7200):
    url = f"https://api.twelvelabs.io/v1.3/assets/multipart-uploads/{upload_id}"
    headers = {"x-api-key": api_key}
    start = time.time()
    
    while time.time() - start < max_wait:
        for attempt in range(3):
            try:
                resp = make_request_with_retries("GET", url, headers=headers)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status")
                    logging.debug(f"Upload session status: {status}")
                    
                    if status == "completed":
                        logging.info("Upload session completed successfully")
                        return True
                    elif status == "failed":
                        error_msg = data.get('error', 'Unknown error')
                        logging.error(f"Upload session failed: {error_msg}")
                        raise RuntimeError(f"Upload session failed: {error_msg}")
                    
                    # Still processing, wait and retry
                    time.sleep(5)
                    break
                else:
                    logging.warning(f"Failed to poll upload session: {resp.status_code if resp else 'No response'}")
                    
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} to poll upload session status failed: {e}")
                if attempt < 2:
                    time.sleep([1, 3, 10][attempt])
                else:
                    logging.error("All attempts to poll upload session status have failed.")
                    raise
    
    raise TimeoutError(f"Upload session polling timed out after {max_wait} seconds")

def index_asset(api_key, index_id, asset_id):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    payload = {"asset_id": asset_id}
    
    for attempt in range(3):
        try:
            resp = make_request_with_retries("POST", url, headers=headers, json=payload)
            if resp and resp.status_code == 202:
                indexed_asset_id = resp.json()["_id"]
                logging.info(f"Indexing started. Indexed asset ID: {indexed_asset_id}")
                return indexed_asset_id
            else:
                error_msg = resp.text if resp else "No response"
                logging.error(f"Indexing failed: status={resp.status_code if resp else 'N/A'}, error={error_msg}")
                raise RuntimeError(f"Indexing failed: {resp.status_code if resp else 'No response'}")
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} to index asset failed: {e}")
            if attempt < 2:
                time.sleep([1, 3, 10][attempt])
            else:
                logging.error("All attempts to index asset have failed.")
                raise

def poll_indexed_asset_status(api_key, index_id, indexed_asset_id, max_wait=1200):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets/{indexed_asset_id}"
    headers = {"x-api-key": api_key}
    start = time.time()
    
    while time.time() - start < max_wait:
        for attempt in range(3):
            try:
                resp = make_request_with_retries("GET", url, headers=headers)
                
                if resp is None:
                    logging.warning("No response received from server")
                    if attempt < 2:
                        time.sleep([1, 3, 10][attempt])
                        continue
                    else:
                        raise RuntimeError("Failed to get response after retries")
                
                if resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status")
                    logging.debug(f"Indexed asset status: {status}")
                    
                    if status == "ready":
                        logging.info("Indexing completed successfully")
                        return True
                    elif status == "failed":
                        error_msg = data.get('error', 'Unknown error')
                        logging.error(f"Indexing failed: {error_msg}")
                        return False
                    
                    time.sleep(10)
                    break
                    
                else:
                    try:
                        error_body = resp.text
                        error_json = resp.json() if error_body else {}
                        logging.error(f"Failed to poll indexed asset: status={resp.status_code}, response={error_body}")
                        logging.debug(f"Error details: {error_json}")
                    except:
                        logging.error(f"Failed to poll indexed asset: status={resp.status_code}, response={resp.text}")
                    
                    if attempt < 2:
                        time.sleep([1, 3, 10][attempt])
                    else:
                        raise RuntimeError(f"Polling failed with status {resp.status_code}: {resp.text}")
                    
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} to poll indexed asset status failed: {e}")
                if attempt < 2:
                    time.sleep([1, 3, 10][attempt])
                else:
                    logging.error("All attempts to poll indexed asset status have failed.")
                    raise
    
    logging.error(f"Indexed asset polling timed out after {max_wait} seconds")
    return False

# -----------------------------
# Core Functions
# -----------------------------

def file_exists_in_index(api_key, index_id, filename, file_size):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets"
    headers = {"x-api-key": api_key}
    params = {"filename": filename, "size": file_size}
    page = 1
    
    while True:
        params["page"] = page
        resp = make_request_with_retries("GET", url, headers=headers, params=params)
        if not resp or resp.status_code != 200:
            return None
        
        data = resp.json()
        for item in data.get("data", []):
            meta = item.get("system_metadata", {})
            if meta.get("filename") == filename and meta.get("size") == file_size:
                return item["_id"]
        
        if page >= data.get("page_info", {}).get("total_page", 1):
            break
        page += 1
    
    return None

def delete_video(api_key, index_id, indexed_asset_id):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets/{indexed_asset_id}"
    headers = {"x-api-key": api_key}
    
    for attempt in range(3):
        try:    
            resp = make_request_with_retries("DELETE", url, headers=headers)
            return resp and resp.status_code in (200, 204)
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} to delete video failed: {e}")
            if attempt < 2:
                time.sleep([1, 3, 10][attempt])
            else:
                logging.error("All attempts to delete video have failed.")
                raise

def upload_asset(api_key, index_id, file_path, cloud_config_data, repo_guid, catalog_path):
    original_file_name = os.path.basename(file_path)
    name_part, ext_part = os.path.splitext(original_file_name)
    sanitized_name_part = name_part.strip()
    file_name = sanitized_name_part + ext_part
    
    if file_name != original_file_name:
        logging.info(f"Filename sanitized from '{original_file_name}' to '{file_name}'")
    
    file_size = os.path.getsize(file_path)
    conflict_resolution = cloud_config_data.get('conflict_resolution', DEFAULT_CONFLICT_RESOLUTION)
    
    # Check if file already exists
    existing_id = file_exists_in_index(api_key, index_id, file_name, file_size)
    
    if existing_id:
        if conflict_resolution == "skip":
            logging.info(f"File '{file_name}' exists. Skipping upload.")
            update_catalog(repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], index_id, existing_id)
            logging.info("Catalog updated with existing asset. Exiting successfully.")
            print(f"File '{file_name}' already exists. Skipping upload.")
            sys.exit(0)
        elif conflict_resolution == "overwrite":
            logging.info(f"File '{file_name}' exists. Deleting before re-upload.")
            if not delete_video(api_key, index_id, existing_id):
                logging.warning("Proceeding despite failed deletion.")
    
    # Create multipart upload session
    logging.info(f"Creating multipart upload session for '{file_name}'")
    upload_id, asset_id, chunk_size, _, upload_urls = create_multipart_upload_session(api_key, file_name, file_size)
    logging.info(f"Upload session created. Upload ID: {upload_id}, Asset ID: {asset_id}")
    
    # Upload chunks
    logging.info("Starting chunk upload")
    upload_file_chunks(api_key, file_path, upload_id, asset_id, chunk_size, upload_urls)
    logging.info("All chunks uploaded successfully")
    
    # Poll for upload completion
    logging.info("Polling for upload session completion")
    if not poll_upload_session_status(api_key, upload_id):
        raise RuntimeError("Upload session did not complete successfully")
    
    logging.info(f"Asset uploaded successfully. Asset ID: {asset_id}")
    return asset_id, upload_id

def add_metadata(api_key, index_id, indexed_asset_id, metadata):
    url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets/{indexed_asset_id}"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}

    filtered_metadata = {}
    for key, value in metadata.items():
        if not isinstance(value, (str, int, float, bool)):
            logging.debug(f"Converting non-primitive metadata field '{key}' to string")
            filtered_metadata[key] = str(value)
        else:
            filtered_metadata[key] = value

    payload = {"user_metadata": filtered_metadata}

    for attempt in range(3):
        resp = make_request_with_retries("PATCH", url, headers=headers, json=payload)
        if resp is not None and resp.status_code == 204:
            logging.info("Metadata updated successfully.")
            return True
        elif resp is not None and resp.status_code >= 400:
            error_detail = resp.text or "No error body"
            logging.error(
                f"[add_metadata] [Attempt {attempt+1}] Request failed: status={resp.status_code}, response={error_detail}"
            )
            if attempt < 2:
                delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
        else:
            delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)

    logging.warning("Metadata update failed after retries - continuing anyway")
    return False

def update_catalog(repo_guid, file_path, index_id, indexed_asset_id, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": cloud_config_data.get("provider", "twelvelabs"),
        "providerData": {
            "assetId": indexed_asset_id,
            "indexId": index_id,
            "providerUiLink": f"https://playground.twelvelabs.io/indexes/{index_id}/analyze?v={indexed_asset_id}"
        }
    }
    
    for attempt in range(max_attempts):
        resp = make_request_with_retries("POST", url, headers=headers, json=payload)
        if resp and resp.status_code in (200, 201):
            logging.info("Catalog updated successfully.")
            return True
        time.sleep(5)
    
    return False

def get_ai_metadata(api_key, index_id, indexed_asset_id, advanced_settings=None, max_attempts=3):
    if not advanced_settings or not isinstance(advanced_settings, dict):
        logging.warning("No valid advanced_settings provided. Skipping all AI metadata extraction.")
        return {}

    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    metadata = {}
    url = "https://api.twelvelabs.io/v1.3/analyze"

    # Run each extraction schema via /analyze
    if "extraction_schemas" in advanced_settings:
        for schema in advanced_settings["extraction_schemas"]:
            name = schema["name"]
            if "prompt" not in schema:
                logging.warning(f"Schema '{name}' missing 'prompt'. Skipping.")
                continue

            payload = {
                "video_id": indexed_asset_id,
                "prompt": schema["prompt"],
                "stream": False
            }
            if "temperature" in schema:
                payload["temperature"] = float(schema["temperature"])
            if "max_tokens" in schema:
                payload["max_tokens"] = int(schema["max_tokens"])
            if "response_format" in schema:
                payload["response_format"] = schema["response_format"]

            for attempt in range(max_attempts):
                try:
                    response = make_request_with_retries("POST", url, headers=headers, json=payload)
                    if response and response.status_code == 200:
                        resp_json = response.json()
                        finish_reason = resp_json.get("finish_reason")
                        raw_data = resp_json.get("data")

                        if finish_reason == "length":
                            logging.warning(f"Response for '{name}' truncated due to token limit.")

                        # Parse data if it's a JSON string
                        if isinstance(raw_data, str):
                            try:
                                parsed = json.loads(raw_data)
                            except json.JSONDecodeError:
                                logging.error(f"Failed to parse 'data' as JSON for '{name}'. Keeping as string.")
                                parsed = raw_data
                        else:
                            parsed = raw_data

                        metadata[name] = parsed
                        logging.info(f"Successfully extracted '{name}'.")
                        break

                    else:
                        logging.warning(f"HTTP {response.status_code} for '{name}': {response.text}")

                except Exception as e:
                    logging.warning(f"Attempt {attempt+1} failed for '{name}': {e}")
                    if attempt == max_attempts - 1:
                        logging.error(f"All retries failed for '{name}'.")

    # Embeddings & Transcript
    get_embeddings = True if advanced_settings.get("fetch_embeddings").lower() in ("1","true","yes") else False
    get_transcript = True if advanced_settings.get("fetch_transcript").lower() in ("1","true","yes") else False
    logging.info(f"Fetching embeddings: {get_embeddings}, transcript: {get_transcript}")
    if get_embeddings or get_transcript:
        url = f"https://api.twelvelabs.io/v1.3/indexes/{index_id}/indexed-assets/{indexed_asset_id}"
        params = {}
        if get_embeddings:
            params["embedding_option"] = advanced_settings.get("embedding_options", [])
        if get_transcript:
            params["transcription"] = "true"

        for attempt in range(max_attempts):
            try:
                resp = make_request_with_retries("GET", url, headers=headers, params=params)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    if "embedding" in data:
                        metadata["embeddings"] = data["embedding"]
                    if "transcription" in data:
                        metadata["transcription"] = data["transcription"]
                    break
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} to fetch embeddings/transcript failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep([1, 3, 10][attempt])
                else:
                    logging.error("All attempts to fetch embeddings/transcript have failed.")

    return metadata

def store_metadata_file(config, repo_guid, file_path, metadata, max_attempts=3):
    meta_path, proxy_path = get_store_paths()
    if not meta_path:
        logging.error("Metadata Store settings not found.")
        return None, None

    provider = config.get("provider", "twelvelabs")
    base = os.path.splitext(os.path.basename(file_path))[0]
    repo_guid_str = str(repo_guid)

    # Split meta_path at /./
    if "/./" in meta_path:
        meta_left, meta_right = meta_path.split("/./", 1)
    else:
        meta_left, meta_right = meta_path, ""

    meta_left = meta_left.rstrip("/")

    # Build physical path
    metadata_dir = os.path.join(meta_left, meta_right, repo_guid_str, file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)

    # Full local physical paths
    raw_json = os.path.join(metadata_dir, f"{base}_raw.json")
    norm_json = os.path.join(metadata_dir, f"{base}_norm.json")

    # Returned paths (after /./)
    raw_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_raw.json")
    norm_return = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base}_norm.json")

    raw_success = False
    norm_success = False

    # Write metadata with retry
    for attempt in range(max_attempts):
        try:
            # RAW write
            with open(raw_json, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            raw_success = True

            # NORMALIZED write
            if not get_twelvelabs_normalized_metadata(raw_json, norm_json):
                logging.error("Normalization failed.")
                norm_success = False
            else:
                norm_success = True

            break

        except Exception as e:
            logging.warning(f"Metadata write failed (Attempt {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))

    return (
        raw_return if raw_success else None,
        norm_return if norm_success else None
    )

def send_extracted_metadata(config, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "twelvelabs"),
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

def transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix):
    try:
        with open(norm_metadata_file_path, "r") as f:
            data = json.load(f)
        result = []
        for event_type, detections in data.items():
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, 'unknown')
            for detection in detections:
                occurrences = detection.get("occurrences", [])
                event_obj = {
                    "eventType": event_type,
                    "sdnaEventType": sdna_event_type,
                    "sdnaEventTypePrefix": filetype_prefix,
                    "eventValue": detection.get("value", ""),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                }
                result.append(event_obj)
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False

def send_ai_enriched_metadata(config, repo_guid, file_path, enrichedMetadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "twelvelabs"),
        "normalizedMetadata": enrichedMetadata
    }

    for attempt in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            logging.debug(f"AI enrichment response: {r.text if r else 'No response'}")
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

# -----------------------------
# Main Execution
# -----------------------------

if __name__ == '__main__':
    time.sleep(random.uniform(0.0, 1.5))
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target")
    parser.add_argument("-c", "--config-name", required=True, help="Name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata operations")
    parser.add_argument("--parent-id", help="Parent folder ID")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-sp", "--source-path", help="Source file path")
    parser.add_argument("-mp", "--metadata-file", help="Path to metadata file")
    parser.add_argument("-up", "--upload-path", help="Twelve Labs index ID or upload path")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
    parser.add_argument("--enrich-prefix", help="Prefix to add for sdnaEventType of AI enrich data")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Treat upload-path as index ID")
    parser.add_argument("--controller-address", help="Controller IP:Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata")

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
    api_key = cloud_config_data.get("api_key")
    if not api_key:
        logging.error("API key not found in config.")
        sys.exit(1)
    if args.export_ai_metadata:
        cloud_config_data["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"

    filetype_prefix = args.enrich_prefix if args.enrich_prefix else "gen"

    # Load advanced AI settings
    provider_name = cloud_config_data.get("provider", "twelvelabs")
    advanced_ai_settings = get_advanced_ai_config(args.config_name, provider_name)
    if not advanced_ai_settings:
        logging.warning("No advanced AI settings available. AI metadata extraction may be limited.")

    # Handle send_extracted_metadata mode
    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required.")
            sys.exit(1)
        
        index_id = args.upload_path if args.resolved_upload_id else cloud_config_data.get('index_id')
        clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        
        if not advanced_ai_settings:
            logging.error("Advanced AI settings required for metadata extraction.")
            sys.exit(1)
        
        ai_metadata = get_ai_metadata(api_key, index_id, args.asset_id, advanced_settings=advanced_ai_settings)
        if not ai_metadata:
            logging.error("Could not get AI metadata")
            sys.exit(7)

        raw_path, norm_path = store_metadata_file(cloud_config_data, args.repo_guid, clean_catalog_path, ai_metadata)
        if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_path, norm_path):
            logging.error("Failed to send extracted metadata.")
            sys.exit(7)
        
        logger.info("Extracted metadata sent successfully.")

        if norm_path:
            try:
                meta_path, _ = get_store_paths()
                norm_metadata_file_path = os.path.join(
                    meta_path.split("/./")[0] if "/./" in meta_path else meta_path.split("/$/")[0] if "/$/" in meta_path else meta_path, 
                    norm_path
                )
                enriched, success = transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix)
                
                if success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                    logger.info("AI enriched metadata sent successfully.")
                else:
                    logger.warning("AI enriched metadata send failed – skipping.")
            except Exception as e:
                logger.exception(f"AI enriched metadata transform failed: {e}")
        else:
            logger.info("Normalized metadata not present – skipping AI enrichment.")

        sys.exit(0)

    # Main upload workflow
    index_id = args.upload_path if args.resolved_upload_id else cloud_config_data.get('index_id')
    if not index_id:
        logging.error("Index ID not provided and not found in config.")
        sys.exit(1)

    logging.info(f"Starting Twelve Labs upload process in {mode} mode")
    logging.debug(f"Index ID: {index_id}")

    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    # Size limit check
    if mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logging.warning(f"Invalid size limit: {args.size_limit}")

    # Generate backlink URL
    catalog_path = args.catalog_path or matched_file
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

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
        logging.info(f"[DRY RUN] Index ID: {index_id}")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata will be applied from: {args.metadata_file}")
        sys.exit(0)

    # Prepare metadata
    try:
        metadata_obj = prepare_metadata_to_upload(
            repo_guid=args.repo_guid,
            relative_path=catalog_url,
            file_name=file_name_for_url,
            file_size=os.path.getsize(matched_file),
            backlink_url=backlink_url,
            properties_file=args.metadata_file
        )
        # Rename forbidden metadata fields by prefixing with 'fabric-'
        RESERVED_METADATA_FIELDS = {
            "duration", "filename", "fps", "height", "model_names",
            "size", "video_title", "width", "id", "index_id", "video_id"
        }

        safe_metadata = {}
        for key, value in metadata_obj.items():
            if key.lower() in RESERVED_METADATA_FIELDS:
                new_key = f"fabric-{key}"
                safe_metadata[new_key] = value
                logger.debug(f"Renamed reserved metadata key: '{key}' → '{new_key}'")
            else:
                safe_metadata[key] = value

        metadata_obj = safe_metadata
        logging.info("Metadata prepared successfully.")
    except Exception as e:
        logging.error(f"Failed to prepare metadata: {e}")
        metadata_obj = {}

    # Execute upload workflow
    try:
        # Step 1: Upload asset and wait for completion
        logging.info("STEP 1: Uploading asset")
        asset_id, upload_id = upload_asset(api_key, index_id, matched_file, cloud_config_data, args.repo_guid, args.catalog_path)
        
        # Step 2: Index the asset
        logging.info("STEP 2: Indexing asset")
        indexed_asset_id = index_asset(api_key, index_id, asset_id)
        
        # Step 3: Poll for indexing completion FIRST
        logging.info("STEP 3: Waiting for indexing to complete")
        indexing_success = poll_indexed_asset_status(api_key, index_id, indexed_asset_id)

        if not indexing_success:
            logging.error("Indexing failed or timed out")
            sys.exit(1)
        
        # Step 4: Apply metadata AFTER indexing is ready
        logging.info("STEP 4: Applying metadata")
        try:
            add_metadata(api_key, index_id, indexed_asset_id, metadata_obj)
            logging.info(f"Metadata applied to indexed asset {indexed_asset_id}")
        except Exception as e:
            logging.warning(f"Metadata application failed: {e}")
    
        # Step 5: Update catalog
        logging.info("STEP 5: Updating catalog")
        if update_catalog(args.repo_guid, args.catalog_path.replace("\\", "/").split("/1/", 1)[-1], index_id, indexed_asset_id):
            logging.info("Catalog updated successfully")
        else:
            logging.warning("Catalog update failed but continuing")
        # Step 6: Export AI metadata if requested
        if cloud_config_data.get('export_ai_metadata') == "true":
            logging.info("STEP 6: Exporting AI metadata")
            try:
                ai_metadata = get_ai_metadata(api_key, index_id, indexed_asset_id, advanced_settings=advanced_ai_settings)
                clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]

                if not ai_metadata:
                    logging.error("Could not get AI metadata")
                    sys.exit(7)
                raw_path, norm_path = store_metadata_file(cloud_config_data, args.repo_guid, clean_catalog_path, ai_metadata)
                if not send_extracted_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, raw_path, norm_path):
                    logging.error("Failed to send extracted metadata.")
                    sys.exit(7)
                logger.info("Extracted metadata sent successfully.")
                if norm_path:
                    try:
                        meta_path, _ = get_store_paths()
                        norm_metadata_file_path = os.path.join(
                            meta_path.split("/./")[0] if "/./" in meta_path else meta_path.split("/$/")[0] if "/$/" in meta_path else meta_path, 
                            norm_path
                        )
                        enriched, enrich_success = transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix)
                        if enrich_success and send_ai_enriched_metadata(cloud_config_data, args.repo_guid, clean_catalog_path, enriched):
                            logger.info("AI enriched metadata sent successfully.")
                        else:
                            logger.warning("AI enriched metadata send failed – skipping.")
                    except Exception as e:
                        logger.exception(f"AI enriched metadata transform failed: {e}")
                else:
                    logger.info("Normalized metadata not present – skipping AI enrichment.")
            except Exception as e:
                logger.error(f"Exception while exporting AI metadata: {e}")
                print(f"Metadata extraction failed for asset: {indexed_asset_id}")
                sys.exit(7)

        logging.info("SUCCESS: Twelve Labs upload and indexing completed")
        logging.info(f"Indexed Asset ID: {indexed_asset_id}")
        sys.exit(0)

    except Exception as e:
        logging.critical(f"Upload or indexing failed: {e}")
        sys.exit(1)