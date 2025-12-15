import time
import csv
import os
import re
import posixpath
import subprocess
import logging
import time
import json
import sys
import random
import argparse
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from configparser import ConfigParser
import plistlib
import requests
from process_central_helper import ProcessCentralHelper


# Debug configuration
DEBUG_PRINT = True
DEBUG_TO_FILE = True

PROXY_GENERATION_HOST = "http://127.0.0.1:8000"
PROVIDERS_SUPPORTING_GET_BASE_TARGET = ["frameio" , "frameio_v2", "frameio_v4", "tessact", "overcasthq", "trint", "twelvelabs", "box", "box_v4", "googledrive", "iconik"]
PATH_BASED_PROVIDERS = ["cloud", "AWS", "axel_ai"]
ACCOUNT_BASED_PROVIDERS = ["azure_vision", "AZURE", "AZUREVI"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Mapping provider names to their respective upload scripts
PROVIDER_SCRIPTS = {
    "frameio": "frame_io_v2_uploader.py",
    "frameio_v2": "frame_io_v2_uploader.py",
    "frameio_v4": "frame_io_v4_uploader.py",
    "tessact": "tessact_uploader.py",
    "overcasthq": "overcasthq_uploader.py",
    "AWS": "s3_uploader.py",
    "trint": "trint_uploader.py",
    "twelvelabs": "twelvelabs_uploader.py",
    "box": "box_uploader.py",
    "box_v4": "box_uploader.py",
    "cloud": "dropbox_uploader.py",
    "googledrive": "google_drive_uploader.py",
    "iconik": "iconik_uploader.py",
    "AZURE": "azure_video_indexer_uploader.py",
    "AZUREVI": "azure_video_indexer_uploader.py",
    "axel_ai": "axel_ai_uploader.py",
    "momentslab": "momentslab_uploader.py",
    "rubicx": "rubicx_uploader.py",
    "azure_vision": "azure_vision_uploader.py",
    "google_vision": "google_vision_uploader.py"
}

NORMALIZER_SCRIPTS = {
    "azure_vision": "azure_vision_metadata_normalizer.py",
    "AWS": "rekognition_metadata_normalizer.py",
    "google_vision": "google_vision_metadata_normalizer.py"
}

# New upload modes
UPLOAD_TYPES = ["video_proxy", "video_proxy_sample_scene_change", "video_sample_faces", "video_sample_interval", "audio_proxy", "sprite_sheet"]

pc = ProcessCentralHelper()

def debug_print(log_path, text_string):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    output = f"{formatted_datetime} {text_string}"
    logging.info(output)

    if DEBUG_PRINT and DEBUG_TO_FILE:
        with open(log_path, "a") as debug_file:
            debug_file.write(f"{output}\n")
            
def get_advanced_ai_config(config_name, provider_name="gcp_vision"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        logging.info("Admin dropbox not available")
        return {}

    config_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded advanced config: {config_file}")
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load config: {e}")

    sample_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    if os.path.exists(sample_file):
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded sample config: {sample_file}")
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load sample: {e}")
    return {}            
            
def get_job_address():
    # Read Servers.conf to get job-address
    # [root@e0372a4af524 /]# cat /etc/StorageDNA/Servers.conf
    # [General]
    # 00000000-0000-0000-0000-0CC47AE523FC/job_address=dnaserver.storagedna.com
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
            logging.debug(f"Successfully read {len(lines)} lines from config")
        
        if os.path.isdir("/opt/sdna/bin"):
            # INI format
            for line in lines:
                match = re.match(r"([0-9a-fA-F-]{36})/job_address=(.+)", line.strip())
                if match:
                    job_address = match.group(2)
                    logging.debug(f"Found job_address: {job_address}")
                    return job_address
        else:
            # plist format
            plist = plistlib.load(open(SERVERS_CONF_PATH, 'rb'))
            for key, value in plist.items():
                match = re.match(r"([0-9a-fA-F-]{36})/job_address", key)
                if match:
                    job_address = value
                    logging.debug(f"Found job_address: {job_address}")
                    return job_address
            
    except Exception as e:
        logging.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)

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
        return log_path or None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def get_advanced_ai_config(config_name, provider):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        logging.info("Admin dropbox not available")
        return {}

    config_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded advanced config: {config_file}")
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load config: {e}")

    sample_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider}.json")
    if os.path.exists(sample_file):
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded sample config: {sample_file}")
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load sample: {e}")
    return {}
        
def get_retry_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, max_retries=3, **kwargs):
    session = get_retry_session()
    for attempt in range(max_retries):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                wait_time = retry_after + 5
                logging.warning(f"[429] Rate limited. Waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            if response.status_code < 500:
                return response
            logging.warning(f"[SERVER ERROR {response.status_code}] Retrying...")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                raise
            delay = [2, 5, 15][attempt] + random.uniform(0, 2)
            logging.warning(f"[{type(e).__name__}] Retrying in {delay:.1f}s...")
            time.sleep(delay)
    return None

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json" if os.path.isdir("/opt/sdna/bin") else "/Library/Application Support/StorageDNA/nginx/ai-config.json"
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

# Writes job progress details to an XML file at specified path
def send_progress(progressDetails, request_id):
    duration = (int(time.time()) - int(progressDetails["duration"])) * 1000
    run_guid = progressDetails["run_id"]
    job_id = progressDetails["job_id"]
    progress_path = progressDetails["progress_path"]
    num_files_scanned = progressDetails["totalFiles"]
    num_bytes_scanned = progressDetails["totalSize"]
    num_files_processed = progressDetails["processedFiles"]
    num_bytes_processed = progressDetails["processedBytes"]
    status = progressDetails["status"]

    avg_bandwidth = 232
    xml_str = f"""<update-job-progress duration=\"{duration}'\" avg_bandwidth=\"{avg_bandwidth}\">
    <progress jobid=\"{job_id}\" cur_bandwidth=\"0\" stguid=\"{run_guid}\" requestid=\"{request_id}\">
        <scanning>false</scanning>
        <scanned>{num_files_scanned}</scanned>
        <run-status>{status}</run-status>
        <quick-index>true</quick-index>
        <is_hyper>false</is_hyper>
        <session>
            <selected-files>{num_files_scanned}</selected-files>
            <selected-bytes>{num_bytes_scanned}</selected-bytes>
            <deleted-files>0</deleted-files>
            <processed-files>{num_files_processed}</processed-files>
            <processed-bytes>{num_bytes_processed}</processed-bytes>
        </session>
    </progress>
    <transfers>
    </transfers>
    <transferred/>
    <deleted/>
</update-job-progress>"""

    os.makedirs(os.path.dirname(progress_path), exist_ok=True)
    with open(progress_path, "w") as file:
        file.write(xml_str)

# get base upload path's id to optimize upload progress
def resolve_base_upload_id(logging_path,script_path, cloud_config_name, upload_path, parent_id=None):
    try:
        cmd = [
            "python3", script_path,
            "--mode", "get_base_target",
            "--config-name", cloud_config_name,
            "--upload-path", upload_path
        ]
        if parent_id:
            cmd.extend(["--parent-id", parent_id])
        debug_print(logging_path, f"Command block copy for resolve folder ---------------------> {' '.join(cmd)}")
        time.sleep(1 + 2 * random.random())  # 1–3 seconds
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return upload_path  # fallback
        resolved_id = result.stdout.strip()
        if resolved_id:
            return resolved_id
        return upload_path
    except Exception as e:
        return upload_path

def parse_scene_detection_filename(filename):
    print(f"Parsing filename: {filename}")

    # ---------- PRIMARY FORMAT ----------
    # Supports:
    #   Scene-001_Sub-01_00-00-00.000.jpg
    #   Scene-001_00-00-00.000.jpg
    #   Does NOT require anything before "Scene"
    primary_pattern = r"(?:^|.+?)Scene-(\d+)(?:_Sub-(\d+))?_(\d{2}-\d{2}-\d{2}\.\d{3})"
    match = re.search(primary_pattern, filename)
    if match:
        scene_no = match.group(1)
        subscene_no = match.group(2) if match.group(2) else "1"
        timestamp = match.group(3)
        print(f"Matched primary format: scene_no={scene_no}, subscene_no={subscene_no}, timestamp={timestamp}")
        return scene_no, subscene_no, timestamp

    # ---------- FALLBACK FORMAT ----------
    # Supports:
    #   prefix_001.jpg
    #   foo_023.png
    #   but NOT Scene-001_...
    #
    # Must have: underscore + number + extension
    fallback_pattern = r"_(\d+)\.[^.]+$"
    match = re.search(fallback_pattern, filename)
    
    if match:
        scene_no = match.group(1)
        return scene_no, "1", ""

    # ---------- NO MATCH ----------
    return None, None, None

def build_proxy_map(records, config, progressDetails):
    proxy_map = {}
    proxy_dir = config["proxy_directory"]
    log_path = config["logging_path"]
    upload_type = config.get("upload_type", "video_proxy")
    total_records = len(records)
    debug_print(log_path, f"[STEP] Building proxy map for {total_records} records using proxy directory: {proxy_dir} with mode {upload_type}")
    def resolve_proxy(record):
        original_source_path = record[0]
        base_source_path = os.path.join(*original_source_path.split("/./")) if "/./" in original_source_path else original_source_path
        base_name = os.path.splitext(os.path.basename(base_source_path))[0]
        
        if upload_type in ("sprite_sheet", "video_proxy_sample_scene_change"):
            # New behavior - folder with multiple files (search recursively)
            return base_source_path, find_folder_with_files(proxy_dir, base_name, upload_type, log_path)
        else:
            # Original behavior - single file proxy
            pattern = f"{base_name}*.*"
            return base_source_path, resolve_proxy_file(proxy_dir, base_source_path, pattern, log_path)

    # Initialize progress
    progressDetails["processedFiles"] = 0
    progressDetails["totalFiles"] = total_records
    progressDetails["status"] = "Resolving proxy files"
    progressDetails["duration"] = int(time.time())  # Initial timestamp

    last_send = time.time()
    SEND_INTERVAL = 30  # seconds

    with ThreadPoolExecutor(max_workers=config.get("thread_count", 2)) as executor:
        future_to_record = {executor.submit(resolve_proxy, r): r for r in records}
        for future in as_completed(future_to_record):
            base_source_path, proxy_result = future.result()
            proxy_map[base_source_path] = proxy_result

            # Update counter in main thread only
            progressDetails["processedFiles"] += 1
            now = time.time()

            # Send progress every 30 seconds OR on the last file
            if (now - last_send >= SEND_INTERVAL) or (progressDetails["processedFiles"] == total_records):
                progressDetails["duration"] = int(now)  # Unix timestamp
                send_progress(progressDetails, config["repo_guid"])
                last_send = now  # Reset timer

    debug_print(log_path, f"[PROXY MAP] Pre-resolved {len(proxy_map)} proxies.")
    return proxy_map

# Helper function to find folder with files recursively
def find_folder_with_files(proxy_dir, folder_name, upload_type, log_path):
    base_dir = Path(proxy_dir)
    
    debug_print(log_path, f"[FIND FOLDER] Searching for folder '{folder_name}' in {proxy_dir} with mode {upload_type}")
    
    # Use rglob to search recursively for directories with the specified name
    matching_folders = list(base_dir.rglob(folder_name))
    matching_folders = [f for f in matching_folders if f.is_dir()]
    
    debug_print(log_path, f"[FIND FOLDER] Found {len(matching_folders)} matching folders: {matching_folders}")
    
    if not matching_folders:
        debug_print(log_path, f"[FIND FOLDER] No folder found with name '{folder_name}'")
        return None
    proxy_folder = matching_folders[0]
    # For proxy_folder, collect files based on upload mode
    all_matching_files = []

    if upload_type == "sprite_sheet":
        for file_path in proxy_folder.iterdir():
            if file_path.is_file() and is_spreadsheet_file(str(file_path)):
                all_matching_files.append(str(file_path))
                debug_print(log_path, f"[FIND FOLDER] Found spreadsheet file: {file_path}")

    if upload_type == "video_proxy_sample_scene_change":
        scene_dir = proxy_folder / "scenes_images"
        search_dir = scene_dir if scene_dir.exists() and scene_dir.is_dir() else None

        # First attempt: search inside scenes_images
        if search_dir:
            debug_print(log_path, f"[FIND FOLDER] Trying scenes_images directory: {search_dir}")
            for file_path in search_dir.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in {'.jpg', '.jpeg', '.png', '.bmp', '.tiff'}:
                    all_matching_files.append(str(file_path))
                    debug_print(log_path, f"[FIND FOLDER] Found vision frame in scenes_images: {file_path}")

        # Fallback: if nothing found, search in proxy_folder itself
        if not all_matching_files:
            debug_print(log_path, f"[FIND FOLDER] scenes_images empty. Falling back to proxy folder: {proxy_folder}")
            for file_path in proxy_folder.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in {'.jpg', '.jpeg', '.png', '.bmp', '.tiff'}:
                    all_matching_files.append(str(file_path))
                    debug_print(log_path, f"[FIND FOLDER] Found fallback vision frame: {file_path}")
    
    debug_print(log_path, f"[FIND FOLDER] Total matching files found: {len(all_matching_files)}")
    return all_matching_files if all_matching_files else None

# Helper functions to identify file types
def is_spreadsheet_file(file_path):
    image_extensions = {'.jpg', '.jpeg', '.png'}
    return os.path.splitext(file_path)[1].lower() in image_extensions

# Locates proxy file by filename pattern inside a directory tree
def resolve_proxy_file(proxy_dir, original_source_path, pattern, log_path):
    base_dir = Path(proxy_dir)
    source_file = Path(original_source_path).resolve()

    debug_print(log_path,f"[resolve_proxy_file] proxy_dir={proxy_dir} | pattern={pattern} | original_source={original_source_path}")

    # 1. Flat search (non-recursive) in proxy_dir
    flat_matches = [f.resolve() for f in base_dir.glob(pattern) if f.is_file()]
    debug_print(log_path, f"[FLAT] Candidates: {flat_matches}")
    for candidate in flat_matches:
        if candidate != source_file:
            debug_print(log_path, f"[FLAT] Using proxy file for upload: {candidate}")
            return str(candidate)

    # 2. Mirror structure search
    try:
        relative_subpath = Path(*Path(original_source_path).parts[1:]).parent
        mirror_path = base_dir / relative_subpath
        if mirror_path.exists():
            mirror_matches = [f.resolve() for f in mirror_path.glob(pattern) if f.is_file()]
            debug_print(log_path, f"[MIRROR] Candidates in {mirror_path}: {mirror_matches}")
            for candidate in mirror_matches:
                if candidate != source_file:
                    debug_print(log_path, f"[MIRROR] Using proxy file for upload: {candidate}")
                    return str(candidate)
    except Exception as e:
        debug_print(log_path, f"[ERROR] Mirror structure exception: {e}")

    # 3. Segment-stripping search
    path_segments = Path(original_source_path).parts
    for i in range(1, len(path_segments) - 1):  # Exclude last (filename)
        sub_path = Path(*path_segments[i:-1])
        search_path = base_dir / sub_path
        if search_path.exists():
            strip_matches = [f.resolve() for f in search_path.glob(pattern) if f.is_file()]
            debug_print(log_path, f"[SEGMENT STRIP i={i}] Candidates in {search_path}: {strip_matches}")
            for candidate in strip_matches:
                if candidate != source_file:
                    debug_print(log_path, f"[SEGMENT STRIP] Using proxy file for upload: {candidate}")
                    return str(candidate)

    # 4. Fallback: rglob (recursive) inside proxy_dir only
    fallback_matches = sorted([
        f.resolve() for f in base_dir.rglob(pattern)
        if f.is_file() and f.resolve() != source_file
    ])
    debug_print(log_path, f"[FALLBACK RGLOB] Candidates: {fallback_matches}")
    if fallback_matches:
        resolved_path = fallback_matches[0]
        debug_print(log_path, f"[FALLBACK RGLOB] Using proxy file for upload: {resolved_path}")
        return str(resolved_path)

    # Nothing found
    debug_print(log_path, "[MISS] No proxy file found for this pattern.")
    return None

# Determine source path: override > proxy_map > base path (only for non-proxy modes) 
def resolve_source_path_for_upload_asset(record, config, override_source_path=None, proxy_map=None):
    original_source_path = record[0]

    if "/./" in original_source_path:
        base_source_path = os.path.join(*original_source_path.split("/./"))
    else:
        base_source_path = original_source_path

    # Proxy generation modes: always use override_source_path
    if config["mode"] in [
        "generate_video_proxy", "generate_video_frame_proxy",
        "generate_intelligence_proxy", "generate_video_to_spritesheet"
    ]:
        return override_source_path, None

    # Proxy mode: use proxy_map
    elif config["mode"] == "proxy":
        debug_print(config["logging_path"], f"[PROXY MODE] Resolving source path for {base_source_path}")
        if proxy_map and base_source_path in proxy_map and proxy_map[base_source_path]:
            return proxy_map[base_source_path], None
        else:
            error_msg = f"[STRICT PROXY MODE] Proxy missing for {base_source_path}"
            return None, error_msg

    # Default: original mode or others
    else:
        return override_source_path or base_source_path, None

# Launches the provider script with file arguments
def upload_asset(record, config, dry_run=False, upload_path_id=None, override_source_path=None, proxy_map=None, specific_file_path=None):
    original_source_path, catalog_path, metadata_path = record

    # Resolve base source path and upload path
    if "/./" in original_source_path:
        base_source_path = os.path.join(*original_source_path.split("/./"))
        relative_upload_path = original_source_path.split("/./", 1)[1]
    else:
        base_source_path = original_source_path
        relative_upload_path = os.path.basename(original_source_path)

    if "upload_path" in config and config["upload_path"]:
        upload_base = config["upload_path"].split(":")[-1]
        full_upload_path = os.path.join(upload_base, relative_upload_path)
    else:
        full_upload_path = relative_upload_path

    error = None
    # Determine the correct source path using the helper and for multiple files mode, use the specific file path
    if specific_file_path:
        debug_print(config["logging_path"], f"[SPECIFIC FILE] Using specific file path for upload: {specific_file_path}")
        source_path = specific_file_path
    else:
        source_path, error = resolve_source_path_for_upload_asset(record, config, override_source_path, proxy_map)

    if error:
        debug_print(config["logging_path"], error)
        logging.error(error)
        # try:
        #     if os.stat(base_source_path).st_size > 0 and os.path.exists(catalog_path):
        #         try:
        #             os.remove(catalog_path)
        #             logging.info(f"Deleted catalog file: {catalog_path}")
        #         except Exception as e:
        #             logging.error(f"Failed to delete catalog file {catalog_path}: {e}")
        # except Exception as e:
        #     logging.error(f"Failed to delete catalog file {catalog_path}: {e}")

        return {"success": False, "error": error}, base_source_path

    # Build command
    cmd = [
        "python3", config["script_path"],
        "--mode", config["mode"],
        "--source-path", source_path,
        "--catalog-path", catalog_path,
        "--config-name", config["cloud_config_name"],
        "--job-guid", config["job_guid"],
        "--repo-guid", config["repo_guid"],
        "--log-level", "debug",
        "--export-ai-metadata", "true"
    ]

    # Optional parameters
    if config["mode"] == "original" and "original_file_size_limit" in config:
        cmd += ["--size-limit", str(config["original_file_size_limit"])]
    if config["provider"] == "iconik" and "collection_id" in config:
        cmd += ["--collection-id", config["collection_id"]]
    if config["provider"] == "overcasthq" and "project_id" in config:
        cmd += ["--project-id", config["project_id"]]
    if metadata_path:
        cmd += ["--metadata-file", metadata_path]
    if config.get("controller_address"):
        cmd += ["--controller-address", config["controller_address"]]
    if config.get("provider") in PATH_BASED_PROVIDERS:
        cmd += ["--bucket-name", config["bucket"]]
    if dry_run:
        cmd.append("--dry-run")
    if upload_path_id:
        cmd.append("--resolved-upload-id")

    # Remove --upload-path for azure_video_indexer
    if config.get("provider") not in ACCOUNT_BASED_PROVIDERS:
        cmd += ["--upload-path", upload_path_id or full_upload_path]
    else:
        # Remove any accidental --upload-path argument
        cmd = [arg for arg in cmd if arg != "--upload-path"]

    debug_print(config["logging_path"], f"[COMMAND] {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result, source_path

# csv row writer helpeer
def write_csv_row(file_path, row):
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)
        
def write_ai_metadata_failure(file_path, asset_id, source_path, catalog_path, metadata_path):
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([asset_id, source_path, catalog_path, metadata_path or ""])    

def setup_log_and_progress_paths(config):
    for key in ["logging_path", "progress_path"]:
        path = config[key]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "a").close()

def prepare_log_files(config):
    log_prefix = config.get("log_prefix")
    if not log_prefix:
        return None, None, None

    debug_print(config['logging_path'], f"[STEP] Preparing log files with prefix: {log_prefix}")
    os.makedirs(os.path.dirname(log_prefix), exist_ok=True)
    transferred_log = f"{log_prefix}-uploader-transferred.csv"
    issues_log = f"{log_prefix}-uploader-issues.csv"
    client_log = f"{log_prefix}-uploader-client.csv"
    
    for log in [transferred_log, issues_log, client_log]:
        with open(log, "w", newline="") as f:
            csv.writer(f).writerow(["Status","Filename", "Size", "Detail"] if "issues" not in log else ["Status", "Filename","Timestamp", "Issue"])
    return transferred_log, issues_log, client_log

def read_csv_records(csv_path, logging_path, extensions = [], file_size_limit = None):
    debug_print(logging_path, "[STEP] Reading records from CSV...")
    records = []
    size_limit_bytes = float(file_size_limit) * 1024 * 1024 if file_size_limit else None

    # Normalizing extensions list for comparison if provided
    normalized_exts = [ext.lower().lstrip('.') for ext in extensions] if extensions else None

    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            if len(row) in (2, 3):
                first_value = row[0]

                if "/./" in first_value:
                    prefix, sub_path = first_value.split("/./", 1)
                    base_source_path = os.path.join(prefix, sub_path)
                else:
                    base_source_path = first_value

                debug_print(logging_path, f"[FILE] Checking file: {base_source_path}")

                try:
                    size = os.stat(base_source_path).st_size
                except Exception as e:
                    debug_print(logging_path, f"[ERROR] Could not stat file {base_source_path}: {e}")
                    continue

                # Only check extension if extensions list is not empty
                if normalized_exts is not None:
                    file_ext = os.path.splitext(base_source_path)[-1].lower().lstrip('.')
                    debug_print(logging_path, f"[EXTENSION CHECK] File: {base_source_path}, Extracted: '{file_ext}', Allowed: {extensions}")

                    if file_ext not in normalized_exts:
                        debug_print(logging_path, f"[SKIP] Extension '{file_ext}' not in allowed list {extensions}")
                        continue

                if size_limit_bytes is not None and size > size_limit_bytes:
                    debug_print(logging_path, f"[SKIP] File {base_source_path} exceeds size limit ({size} > {size_limit_bytes})")
                    continue

                # Always append a tuple of length 3
                if len(row) == 3:
                    records.append((row[0], row[1], row[2]))
                elif len(row) == 2:
                    records.append((row[0], row[1], None))

    debug_print(logging_path, f"[STEP] Total records loaded: {len(records)}")
    return records

def calculate_total_size(records, config, proxy_map=None):
    total_size = 0
    upload_type = config.get("upload_type", "video_proxy")

    for r in records:
        # Resolve actual source path
        if "/./" in r[0]:
            src = r[0].split("/./")[-1]
            full_path = os.path.join(r[0].split("/./")[0], src)
        else:
            full_path = r[0]

        # When in PROXY mode
        if config["mode"] == "proxy":
            proxy = proxy_map.get(full_path) if proxy_map else None

            # ----------------------------------------------------
            # CASE 1: upload types that produce MULTIPLE FILES
            # ----------------------------------------------------
            if upload_type in ("sprite_sheet", "video_proxy_sample_scene_change"):
                if proxy and isinstance(proxy, list):
                    logging.debug(f"[SIZE CALC] Counting files for {full_path}: {proxy}")
                    for file_path in proxy:
                        if file_path and os.path.exists(file_path):
                            total_size += os.path.getsize(file_path)
                # If proxy missing → count 0 (silent)
                continue

            # ----------------------------------------------------
            # CASE 2: Normal single-file proxy
            # ----------------------------------------------------
            if isinstance(proxy, list):
                # Safety fallback: treat multiple as sum
                for fp in proxy:
                    if fp and os.path.exists(fp):
                        total_size += os.path.getsize(fp)
                continue

            if proxy and isinstance(proxy, (str, bytes, os.PathLike)):
                if os.path.exists(proxy):
                    total_size += os.path.getsize(proxy)

        # --------------------------------------------------------
        # CASE 3: Local file upload mode (non-proxy)
        # --------------------------------------------------------
        elif os.path.exists(full_path):
            total_size += os.path.getsize(full_path)

    debug_print(config['logging_path'], f"[STEP] Total size to upload: {total_size} bytes")
    return total_size

def build_folder_id_map(records, config, log_path, progressDetails):
    resolved_ids = {}
    base_upload_path = "/"
    base_id = None

    # === Stage 0: Handle base upload path ===
    if "upload_path" in config and config["upload_path"]:
        upload_path = config["upload_path"]
        if ":" in upload_path:
            parent_id, base_upload_path = upload_path.split(":", 1)
            base_id = parent_id.strip("/")
            resolved_ids[base_upload_path] = base_id
            debug_print(log_path, f"[INIT] Mapping root path '{base_upload_path}' to ID '{base_id}'")
            if config.get("provider") == "twelvelabs":
                return resolved_ids
        else:
            base_upload_path = config["upload_path"]
            base_id = resolve_base_upload_id(
                log_path, config["script_path"], config["cloud_config_name"], f"/{base_upload_path}"
            )
            resolved_ids[base_upload_path] = base_id
            debug_print(log_path, f"[INIT] Resolved base path '{base_upload_path}' to ID '{base_id}'")
            if config.get("provider") == "twelvelabs":
                return resolved_ids
    else:
        # If upload_path is not provided, default to root "/" mapped to None
        resolved_ids["/"] = None
        debug_print(log_path, "[INIT] No upload_path in config. Using root '/' mapped to None.")
        if config.get("provider") == "twelvelabs":
            return resolved_ids

    # === Stage 1: Collect all unique paths ===
    progressDetails["status"] = "Collecting unique paths"
    progressDetails["processedFiles"] = 0
    progressDetails["duration"] = int(time.time())
    send_progress(progressDetails, config["repo_guid"])

    all_needed_paths = set()
    for idx, record in enumerate(records, start=1):
        progressDetails["processedFiles"] += 1
        original_source_path = record[0]
        if "/./" in original_source_path:
            _, sub_path = original_source_path.split("/./", 1)
            path_segments = [seg for seg in sub_path.split(os.sep) if seg]
            current_path = base_upload_path if config.get("upload_path") else "/"
            for seg_idx, segment in enumerate(path_segments):
                if seg_idx < len(path_segments):
                    is_last = seg_idx == len(path_segments) - 1
                    if is_last and config.get("mode") == "proxy" and config.get("upload_type") == "sprite_sheet":
                        segment = os.path.splitext(segment)[0]
                    elif is_last:
                        break
                    next_path = posixpath.join(current_path, segment)
                    all_needed_paths.add(next_path)
                    current_path = next_path
        # Send progress every 100 records
        if idx % 100 == 0:
            progressDetails["duration"] = int(time.time())
            send_progress(progressDetails, config["repo_guid"])

    sorted_paths = sorted(all_needed_paths, key=lambda p: p.count("/"))
    total_folders = len(sorted_paths)

    # === Stage 2: Create folders and resolve IDs ===
    progressDetails["status"] = "Creating unique Folders"
    progressDetails["processedFiles"] = 0
    progressDetails["totalFiles"] = total_folders
    progressDetails["duration"] = int(time.time())
    send_progress(progressDetails, config["repo_guid"])

    last_send = time.time()
    SEND_INTERVAL = 30  # seconds

    for p_idx, path in enumerate(sorted_paths, start=1):
        if path not in resolved_ids:
            parent_path = posixpath.dirname(path)
            parent_id = resolved_ids.get(parent_path)
            segment = posixpath.basename(path)
            debug_print(log_path, f"[STEP] Resolving segment: {segment} under {parent_path}")
            resolved_id = resolve_base_upload_id(
                log_path,
                config["script_path"],
                config["cloud_config_name"],
                f"/{segment}",
                parent_id=parent_id
            )
            resolved_ids[path] = resolved_id

        # Update progress
        progressDetails["processedFiles"] = p_idx
        now = time.time()

        # Send progress every 30 seconds OR on last folder
        if (now - last_send >= SEND_INTERVAL) or (p_idx == total_folders):
            progressDetails["duration"] = int(now)
            send_progress(progressDetails, config["repo_guid"])
            last_send = now

    return resolved_ids

def get_meta_pending_path(repo_guid):
    failed_dir = f"/sdna_fs/PROXIES/ARCHIVES/{repo_guid}/failed_uploads"
    return os.path.join(failed_dir, "meta-export-pending-list.csv")

def has_pending_metadata_work(repo_guid):
    meta_file = get_meta_pending_path(repo_guid)
    if not os.path.exists(meta_file):
        return False
    try:
        with open(meta_file, 'r', newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:  # valid record
                    return True
        return False
    except Exception:
        return False

def has_ai_proxy_lookup_pending_work(repo_guid):
    proxy_pending_file = get_ai_proxy_pending_path(repo_guid)
    return os.path.exists(proxy_pending_file) and os.path.getsize(proxy_pending_file) > 0

def load_ai_proxy_failures(repo_guid, logging_path):
    pending_file = get_ai_proxy_pending_path(repo_guid)
    failures = []
    if os.path.exists(pending_file):
        debug_print(logging_path, f"[AI PROXY RETRY] Loading failures from {pending_file}")
        with open(pending_file, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for row in reader:
                if len(row) >= 2:
                    failures.append((row[0], row[1], row[2] if len(row) > 2 else None))
    return failures

def save_ai_proxy_failures(repo_guid, failed_records, logging_path):
    pending_file = get_ai_proxy_pending_path(repo_guid)
    os.makedirs(os.path.dirname(pending_file), exist_ok=True)
    with open(pending_file + ".tmp", 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        for r in failed_records:
            writer.writerow([r[0], r[1], r[2] if r[2] else ""])
    os.replace(pending_file + ".tmp", pending_file)
    debug_print(logging_path, f"[AI PROXY] Saved {len(failed_records)} failures to {pending_file}")

def get_ai_proxy_pending_path(repo_guid):
    return f"/sdna_fs/PROXIES/ARCHIVES/{repo_guid}/failed_uploads/ai_proxy_pending.csv"

def load_previous_failures(repo_guid, logging_path):
    failed_dir = f"/sdna_fs/PROXIES/ARCHIVES/{repo_guid}/failed_uploads"
    failed_file_path = os.path.join(failed_dir, "failed_files.csv")

    previous_failures = []
    if os.path.exists(failed_file_path):
        debug_print(logging_path, f"[RETRY] Loading previous failures from: {failed_file_path}")
        with open(failed_file_path, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for row in reader:
                if len(row) >= 2:  # Must have at least source and catalog
                    source_path = row[0]
                    catalog_path = row[1]
                    metadata_path = row[2] if len(row) > 2 else None
                    previous_failures.append((source_path, catalog_path, metadata_path))
    else:
        debug_print(logging_path, "[RETRY] No previous failure file found.")

    return previous_failures

def save_current_failures(repo_guid, current_failures, logging_path):
    failed_dir = f"/sdna_fs/PROXIES/ARCHIVES/{repo_guid}/failed_uploads"
    failed_file_path = os.path.join(failed_dir, "failed_files.csv")
    temp_file_path = failed_file_path + ".tmp"  # Temporary file

    # Ensure the directory exists
    os.makedirs(failed_dir, exist_ok=True)

    debug_print(logging_path, f"[RETRY] Saving {len(current_failures)} current failures to: {failed_file_path}")

    # Write to temporary file first
    with open(temp_file_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')  # Use pipe delimiter
        for record in current_failures:
            # Ensure we write exactly 3 fields. If metadata is None, write empty string.
            source_path, catalog_path, metadata_path = record
            row = [source_path, catalog_path, metadata_path if metadata_path is not None else ""]
            writer.writerow(row)

    # Atomically replace the old file with the new one
    os.replace(temp_file_path, failed_file_path)
    debug_print(logging_path, f"[RETRY] Successfully updated failure list: {failed_file_path}")

def retry_ai_metadata_failures(config):
    meta_pending_file = get_meta_pending_path(config["repo_guid"])
    if not os.path.exists(meta_pending_file):
        debug_print(config["logging_path"], "[AI METADATA RETRY] No pending metadata exports.")
        return 0, 0  # success, failure

    debug_print(config["logging_path"], f"[AI METADATA RETRY] Processing {meta_pending_file}")

    # Read all pending records
    pending_records = []
    with open(meta_pending_file, 'r', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 3:
                pending_records.append(row)

    if not pending_records:
        debug_print(config["logging_path"], "[AI METADATA RETRY] No valid records found.")
        return 0, 0

    total_records = len(pending_records)
    debug_print(config["logging_path"], f"[AI METADATA RETRY] Total records to process: {total_records}")

    # Track which records succeeded (to exclude from next write)
    temp_file = meta_pending_file + ".tmp"
    success_count = 0
    failure_count = 0

    with open(temp_file, 'w', newline='') as out_f:
        writer = csv.writer(out_f)
        for idx, row in enumerate(pending_records):
            asset_id, source_path, catalog_path = row[0], row[1], row[2]
            metadata_path = row[3] if len(row) > 3 and row[3] else None
            upload_path = config["upload_path"]
            if ":" in upload_path:
                parent_id, _ = upload_path.split(":", 1)
                base_id = parent_id.strip("/")
            cmd = [
                "python3", config["script_path"],
                "--mode", "send_extracted_metadata",
                "--asset-id", asset_id,
                "--config-name", config["cloud_config_name"],
                "--catalog-path", catalog_path,
                "--repo-guid", config["repo_guid"],
            ]
            if config.get("provider") == "twelvelabs":
                cmd += ["--upload-path", base_id]
                cmd.append("--resolved-upload-id")
            if metadata_path and os.path.exists(metadata_path):
                cmd += ["--metadata-file", metadata_path]
            if config.get("export"):
                cmd += ["--export-ai-metadata", str(config["export"])]

            debug_print(config["logging_path"], f"[AI METADATA RETRY CMD] {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                debug_print(config["logging_path"], f"[AI METADATA RETRY SUCCESS] {asset_id} ({idx+1}/{total_records})")
                success_count += 1
            else:
                debug_print(config["logging_path"], f"[AI METADATA RETRY FAILED] {asset_id} ({idx+1}/{total_records}) | stderr: {result.stderr}")
                failure_count += 1
                writer.writerow(row)

    # Atomically replace original file
    os.replace(temp_file, meta_pending_file)

    debug_print(config["logging_path"], f"[AI METADATA RETRY SUMMARY] Total: {total_records}, Success: {success_count}, Failed: {failure_count}")
    return success_count, failure_count

def process_ai_proxy_lookup(config):
    if not config.get("ai_proxy_store_enabled"):
        return 0, 0

    repo_guid = config["repo_guid"]
    logging_path = config["logging_path"]
    _, proxy_store_base = get_store_paths()
    if not proxy_store_base:
        debug_print(logging_path, "[AI PROXY] Proxy store path not configured.")
        return 0, 0

    # Resolve physical proxy root: split on /./ and rejoin
    if "/./" in proxy_store_base:
        left, right = proxy_store_base.split("/./", 1)
        proxy_root = os.path.join(left.rstrip("/"), right, repo_guid)
    else:
        proxy_root = os.path.join(proxy_store_base.rstrip("/"), repo_guid)

    debug_print(logging_path, f"[AI PROXY] Using proxy root: {proxy_root}")

    # Determine records: retry only from pending file if exists
    retry_records = load_ai_proxy_failures(repo_guid, logging_path)
    current_records = []
    if not config.get("ai_proxy_lookup_only", False):
        exts = config.get("extensions", [])
        file_size_limit = config.get("original_file_size_limit") if config.get("mode") == "original" else None
        current_records = read_csv_records(config["files_list"], logging_path)

    # Deduplicate by base source path (normalize /./)
    seen = set()
    combined = []
    for rec in retry_records + current_records:
        base = os.path.join(*rec[0].split("/./")) if "/./" in rec[0] else rec[0]
        if base not in seen:
            seen.add(base)
            combined.append(rec)
    records = combined
    debug_print(logging_path, f"[AI PROXY] Processing {len(records)} combined records ({len(retry_records)} retries + {len(current_records)} new).")

    if not records:
        debug_print(logging_path, "[AI PROXY] No records to process.")
        return 0, 0

    # Reuse existing build_proxy_map
    proxy_config = {
        "proxy_directory": proxy_root,
        "logging_path": logging_path,
        "upload_type": "video_proxy",  # single-file lookup
        "thread_count": config.get("thread_count", 2),
        "repo_guid": repo_guid
    }
    progressDetails = {
        "run_id": config["run_id"],
        "job_id": config["job_id"],
        "progress_path": config["progress_path"],
        "totalFiles": len(records),
        "totalSize": 0,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "AI Proxy Lookup",
        "duration": int(time.time())
    }

    proxy_map = build_proxy_map(records, proxy_config, progressDetails)

    # Process results
    success_count = 0
    failed_records = []
    for record in records:
        original_source_path = record[0]
        base_source_path = os.path.join(*original_source_path.split("/./")) if "/./" in original_source_path else original_source_path
        catalog_path = record[1]
        catalog_clean = catalog_path.replace("\\", "/").split("/1/", 1)[-1]

        proxy_path = proxy_map.get(base_source_path)
        if proxy_path and os.path.exists(proxy_path):
            try:
                save_extended_metadata(config, catalog_clean, None, proxy_path, "Fabric AI-Proxy")
                success_count += 1
                debug_print(logging_path, f"[AI PROXY SUCCESS] {base_source_path} -> {proxy_path}")
            except Exception as e:
                debug_print(logging_path, f"[AI PROXY SAVE FAILED] {e}")
                failed_records.append(record)
        else:
            debug_print(logging_path, f"[AI PROXY MISSING] {base_source_path}")
            failed_records.append(record)

    if failed_records:
        save_ai_proxy_failures(repo_guid, failed_records, logging_path)

    debug_print(logging_path, f"[AI PROXY SUMMARY] Success: {success_count}, Failed: {len(failed_records)}")
    return success_count, len(failed_records)

def normalize_json(config, raw_json_path, norm_json_path):
    provider = config.get("provider")
    if not provider:
        logging.error("No provider specified for normalization.")
        return False
    normalizer = NORMALIZER_SCRIPTS.get(provider)
    if not normalizer:
        logging.error(f"No normalizer script for provider: {provider}")
        return False
    script_path = os.path.join(os.path.dirname(__file__), normalizer)
    if not os.path.isfile(script_path):
        logging.error(f"Normalizer script not found: {script_path}")
        return False
    try:
        proc = subprocess.run(
            ["python3", script_path, "-i", raw_json_path, "-o", norm_json_path],
            capture_output=True, text=True
        )
        if proc.returncode == 0:
            logging.info(f"Normalized JSON saved to {norm_json_path}")
            return True
        logging.error("Normalizer failed %s: %s", proc.returncode, proc.stderr.strip())
    except Exception as e:
        logging.error("Error normalizing JSON: %s", e)
    return False

def save_extended_metadata(config, file_path, rawMetadataFilePath, normMetadataFilePath=None, provider = None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": config["repo_guid"],
        "providerName": provider if provider is not None else config.get("provider"),
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
            r = requests.request("POST", url, headers=headers, json=payload)
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

def upload_worker(record, config, resolved_ids, progressDetails, transferred_log, issues_log, client_log, proxy_map=None):
    try:
        debug_print(config['logging_path'], f"[STEP] Processing record: {record}")
        original_source_path, catalog_path, metadata_path = record
        catalog_path_clean = catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        repo_guid = str(config["repo_guid"])
        upload_type = config.get("upload_type", "video_proxy")

        # Extract relative folder path for resolved_ids lookup
        if "/./" in original_source_path:
            _, sub_path = original_source_path.split("/./", 1)
            dir_path = os.path.dirname(sub_path)
        else:
            dir_path = ""

        # Compute resolved ID
        if upload_type == "video_proxy_sample_scene_change":
            upload_path_id = None
            normalized_folder_key = "/"
        else:
            # Extract relative folder path for resolved_ids lookup
            if "/./" in original_source_path:
                _, sub_path = original_source_path.split("/./", 1)
                dir_path = os.path.dirname(sub_path)
            else:
                dir_path = ""

            if "upload_path" in config and config["upload_path"]:
                base_path = config["upload_path"]
                logical_base = base_path.split(":", 1)[-1] if ":" in base_path else base_path
                normalized_folder_key = os.path.normpath(os.path.join(logical_base, dir_path))
            else:
                logical_base = "/"
                normalized_folder_key = os.path.join("/", dir_path) if dir_path else "/"

            if config.get("provider") == "twelvelabs":
                upload_path_id = resolved_ids.get(logical_base)
            elif config["provider"] not in PROVIDERS_SUPPORTING_GET_BASE_TARGET:
                upload_path_id = None
            else:
                upload_path_id = resolved_ids.get(normalized_folder_key, list(resolved_ids.values())[0])

        debug_print(config['logging_path'], f"[PATH-MATCH] {normalized_folder_key} -> using ID {upload_path_id}")

        # Proxy generation step if mode requires it
        override_source_path = None

        # Handle different upload modes
        if upload_type == "sprite_sheet" and config["mode"] == "proxy":
            # Multiple files upload mode (images/spreadsheets)
            base_source_path = os.path.join(*original_source_path.split("/./")) if "/./" in original_source_path else original_source_path
            proxy_files = proxy_map.get(base_source_path) if proxy_map else None
            logging.debug(f"proxy files for {base_source_path}: {proxy_files}")

            if not proxy_files or not isinstance(proxy_files, list):
                error_msg = f"[UPLOAD MODE {upload_type}] No files found for {base_source_path}"
                debug_print(config["logging_path"], error_msg)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                write_csv_row(issues_log, ["Error", base_source_path, timestamp, error_msg])
                # try:
                #     if os.path.exists(catalog_path):
                #         os.remove(catalog_path)
                #         logging.info(f"Deleted catalog file: {catalog_path}")
                # except Exception as e:
                #     logging.error(f"Failed to delete catalog file {catalog_path}: {e}")                
                return {"status": "failure", "file": base_source_path, "error": error_msg, "record": record}

            # Upload each file in the folder
            total_uploaded_size = 0
            success_count = 0
            failure_count = 0

            # Get the sub-folder ID for the source file name
            source_file_name_no_ext = os.path.splitext(os.path.basename(base_source_path))[0]
            sub_folder_key = os.path.join(normalized_folder_key, source_file_name_no_ext) if normalized_folder_key != "/" else f"/{source_file_name_no_ext}"
            specific_upload_path_id = resolved_ids.get(sub_folder_key, upload_path_id)

            for file_path in proxy_files:
                debug_print(config['logging_path'], f"[UPLOAD MODE {upload_type}] Uploading {file_path} to sub-folder {sub_folder_key}")

                # Create a temporary record for this specific file
                temp_record = (file_path, catalog_path, metadata_path)

                result, resolved_path = upload_asset(
                    temp_record, config, config.get("dry_run", False),
                    specific_upload_path_id, override_source_path, proxy_map, file_path
                )

                # Handle upload success
                if result and result.returncode == 0:
                    file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
                    total_uploaded_size += file_size
                    success_count += 1
                    debug_print(config["logging_path"], f"[UPLOAD SUCCESS] {resolved_path} | {file_size} bytes")

                    if transferred_log:
                        write_csv_row(transferred_log, ["Success", resolved_path, file_size, ""])
                    if client_log:
                        write_csv_row(client_log, ["Success", resolved_path, file_size, "Client"])
                else:
                    # Handle upload failure
                    stderr_cleaned = result.stderr.replace("\n", " ").replace("\r", " ").strip() if result and result.stderr else "Unknown Error"
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    debug_print(config["logging_path"], f"[UPLOAD FAILURE] {resolved_path}\n{stderr_cleaned}")
                    write_csv_row(issues_log, ["Error", resolved_path, timestamp, stderr_cleaned])
                    failure_count += 1

            # Update progress for the entire record (one increment for the whole folder)
            progressDetails["processedFiles"] += 1
            progressDetails["processedBytes"] += total_uploaded_size

            if failure_count == 0:
                return {"status": "success", "file": base_source_path, "size": total_uploaded_size, "sub_files": success_count}
            elif failure_count == len(proxy_files):
                # try:
                #     if os.path.exists(catalog_path):
                #         os.remove(catalog_path)
                #         logging.info(f"Deleted catalog file: {catalog_path}")
                # except Exception as e:
                #     logging.error(f"Failed to delete catalog file {catalog_path}: {e}")
                return {"status": "failure", "file": base_source_path, "error": "All sub-files in sprite sheet failed to upload.", "record": record}
            else:
                # return {"status": "partial", "file": base_source_path, "uploaded": success_count, "failed": failure_count}
                return {"status": "partial", "file": base_source_path, "uploaded": success_count, "failed": failure_count, "record": record}

        elif upload_type == "video_proxy_sample_scene_change" and config["mode"] == "proxy":
            base_source_path = os.path.join(*original_source_path.split("/./")) if "/./" in original_source_path else original_source_path
            advanced_config = get_advanced_ai_config(config.get('cloud_config_name'), config.get('provider'))
            # Safely get thread count; default to 2 if missing or invalid
            frame_extraction_thread_count = 2
            if isinstance(advanced_config, dict):
                proposed = advanced_config.get("frame_extraction_thread_count")
                if isinstance(proposed, int) and proposed > 0:
                    frame_extraction_thread_count = min(proposed, 16)  # cap to avoid overload

            proxy_files = proxy_map.get(base_source_path) if proxy_map else None
            if not proxy_files or not isinstance(proxy_files, list):
                msg = f"[Scene Detection] No scene frames for {base_source_path}"
                debug_print(config["logging_path"], msg)
                write_csv_row(issues_log, ["Error", base_source_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg])
                return {"status": "failure", "file": base_source_path, "error": msg, "record": record}

            metaxtend_base, _ = get_store_paths()
            if not metaxtend_base:
                msg = "[Scene Detection] MetaXtend path not configured"
                debug_print(config["logging_path"], msg)
                write_csv_row(issues_log, ["Error", base_source_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg])
                return {"status": "failure", "file": base_source_path, "error": msg, "record": record}
            
            if "/./" in metaxtend_base:
                meta_left, meta_right = metaxtend_base.split("/./", 1)
            else:
                meta_left, meta_right = metaxtend_base, ""

            meta_left = meta_left.rstrip("/")            
            # Build metadata directory as a Path
            metadata_dir = Path(os.path.join(meta_left, meta_right, repo_guid, catalog_path_clean, provider))
            metadata_dir.mkdir(parents=True, exist_ok=True)
            base = os.path.splitext(os.path.basename(base_source_path))[0]
            raw_json = metadata_dir / f"{base}_raw.json"
            norm_json = metadata_dir / f"{base}_norm.json"

            # return paths for saving to mongo
            raw_return = os.path.join(meta_right, repo_guid, catalog_path_clean, provider, f"{base}_raw.json")
            norm_return = os.path.join(meta_right, repo_guid, catalog_path_clean, provider, f"{base}_norm.json")

            raw_success = False
            norm_success = False            
            scene_groups = defaultdict(list)
            for fp in proxy_files:
                fn = Path(fp).name
                scene_no, subscene_no, ts = parse_scene_detection_filename(fn)
                if scene_no:
                    scene_groups[scene_no].append((ts, subscene_no, fp))
                else:
                    debug_print(config["logging_path"], f"[SKIP] Unrecognized: {fn}")

            all_scenes, processed, failed = [], 0, []
            dry_run = config.get("dry_run", False)
            max_retries = config.get("max_frame_retries", 3)
            timeout = config.get("vision_timeout", 120)

            # --- Begin multithreaded frame processing per scene ---
            for scene_no in sorted(scene_groups):
                frames = []
                frame_tasks = [(ts, subscene_no, fp) for ts, subscene_no, fp in sorted(scene_groups[scene_no])]

                # Local worker function (required for ThreadPoolExecutor clarity and correctness)
                def _process_frame_task(args):
                    ts, subscene_no, fp = args
                    if dry_run:
                        return {
                            "success": True,
                            "data": {
                                "timestamp": ts,
                                "subscene_no": subscene_no,
                                "analysis": {},
                                "embedding": [],
                                "embedding_length": 0,
                                "source_file": Path(fp).name
                            },
                            "key": (ts, subscene_no),
                            "fp": fp
                        }

                    temp_out = metadata_dir / f"temp_{scene_no}_{ts}_{subscene_no}.json"
                    success = False
                    result_data = None

                    for attempt in range(max_retries):
                        try:
                            cmd = ["python3", config["script_path"], "-m", "analyze_and_embed",
                                   "-c", config["cloud_config_name"],
                                   "-sp", fp.replace("\\", "/"),
                                   "-o", str(temp_out),
                                   "--log-level", "debug",
                                   "--export-ai-metadata", "True"]
                            if config.get("provider") in PATH_BASED_PROVIDERS:
                                cmd += ["--bucket-name", config["bucket"]]
                                cmd += ["--upload-path", fp.replace("\\", "/")]

                            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                            debug_print(config["logging_path"], f" [return_code] {result.returncode} [VISION CMD OUTPUT] {result.stdout.strip()}")
                            if result.returncode == 0 and temp_out.exists() and temp_out.stat().st_size > 0:
                                try:
                                    with open(temp_out, 'r', encoding='utf-8') as f:
                                        data = json.load(f)
                                    if config.get("provider") in ("google_vision", "azure_vision"):
                                        result_data = {
                                            "timestamp": ts,
                                            "subscene_no": subscene_no,
                                            "analysis": data.get("analysis"),
                                            "embedding": data.get("embedding"),
                                            "embedding_length": data.get("embedding_length", 0),
                                            "source_file": Path(fp).name
                                        }
                                    else:
                                        result_data = {
                                            "timestamp": ts,
                                            "subscene_no": subscene_no,
                                            "analysis": data,
                                            "source_file": Path(fp).name
                                        }
                                    success = True
                                    break
                                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                                    debug_print(config["logging_path"], f"[JSON/UTF8 ERROR] {temp_out}: {e}")
                            else:
                                debug_print(config["logging_path"], f"[VISION ERROR] Cmd failed with code {result.returncode} for {fp}: {result.stderr.strip()} with command {' '.join(cmd)}")
                        except (subprocess.TimeoutExpired, Exception) as e:
                            debug_print(config["logging_path"], f"[EXCEPTION] Frame task failed: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(5 + attempt * 2)
                    temp_out.unlink(missing_ok=True)
                    file_size = 0
                    if success:
                        try:
                            file_size = os.path.getsize(fp)
                        except (OSError, FileNotFoundError):
                            file_size = 0
                    return {
                        "success": success,
                        "data": result_data,
                        "key": (ts, subscene_no),
                        "fp": fp,
                        "file_size": file_size
                    }

                # Execute tasks in thread pool

                frame_results = []
                with ThreadPoolExecutor(max_workers=frame_extraction_thread_count) as executor:
                    futures = [executor.submit(_process_frame_task, task) for task in frame_tasks]
                    for future in as_completed(futures):
                        frame_results.append(future.result())
                processedBytes = 0
                # Restore original order
                frame_results.sort(key=lambda r: r["key"])
                for res in frame_results:
                    if res["success"]:
                        frames.append(res["data"])
                        processed += 1
                        processedBytes += res["file_size"]
                    else:
                        failed.append({
                            "scene": scene_no,
                            "subscene": res["key"][1],
                            "timestamp": res["key"][0],
                            "file": res["fp"]
                        })

                if frames:
                    all_scenes.append({"scene_no": scene_no, "frame_count": len(frames), "frames": frames})

            # --- End multithreaded block ---

            if not dry_run:
                output_data = {
                    "source": base_source_path,
                    "processed_at": datetime.now().isoformat(),
                    "total_scenes": len(all_scenes),
                    "total_frames_processed": processed,
                    "total_frames_failed": len(failed),
                    "scenes": all_scenes
                }
                if failed:
                    output_data["failed_frames"] = failed

                for attempt in range(3):
                    try:
                        # RAW write
                        with open(raw_json, "w", encoding="utf-8") as f:
                            json.dump(output_data, f, ensure_ascii=False, indent=2)
                        raw_success = True

                        # NORMALIZED write
                        if not normalize_json(config, raw_json, norm_json):
                            logging.error("Normalization failed.")
                            norm_success = False
                        else:
                            norm_success = True

                        break  # No need for more retries

                    except Exception as e:
                        logging.warning(f"Metadata write failed (Attempt {attempt+1}): {e}")
                        if attempt < 2:
                            time.sleep([1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt]))
                            
                # save data to metaxtend with save_extended_metadata call
                if raw_success or norm_success:
                    try:
                        save_extended_metadata(config,catalog_path_clean,raw_return if raw_success else None,norm_return if norm_success else None)
                    except Exception as e:
                        logging.error(f"Failed to save extended metadata for {base_source_path}: {e}")
            else:
                debug_print(config["logging_path"], f"[DRY RUN] Would write to {raw_json} and {norm_json}")

            progressDetails["processedFiles"] += 1
            progressDetails["processedBytes"] += processedBytes
            send_progress(progressDetails, config["repo_guid"])

            if processed == 0:
                debug_print(config["logging_path"], f"[SCENE BASED EXTRACTION FAILURE] {base_source_path} | No frames succeeded")
                return {"status": "failure", "file": base_source_path, "error": "No frames succeeded", "record": record}
            elif failed:
                debug_print(config["logging_path"], f"[SCENE BASED EXTRACTION PARTIAL SUCCESS] {base_source_path} | Failed frames: {len(failed)}")
                return {"status": "partial_success", "file": base_source_path, "output": str(raw_json), "processed": processed, "failed": len(failed), "record": record}
            else:
                debug_print(config["logging_path"], f"[SCENE BASED EXTRACTION SUCCESS] {base_source_path}")
                return {"status": "success", "file": base_source_path, "output": str(raw_json), "processed": processed, "record": record}

        else:
            # Original single file upload behavior
            result, resolved_path = upload_asset(
                record, config, config.get("dry_run", False),
                upload_path_id, override_source_path, proxy_map
            )

            # Handle proxy resolution failure
            if isinstance(result, dict) and not result.get("success", True):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                error_message = result.get("error", "Unknown error during proxy resolution or upload")
                debug_print(config["logging_path"], f"[PROXY FAILURE] {resolved_path}\n{error_message}")
                write_csv_row(issues_log, ["Error", resolved_path, timestamp, error_message])
                # try:
                #     if os.path.exists(catalog_path):
                #         os.remove(catalog_path)
                #         logging.info(f"Deleted catalog file: {catalog_path}")
                # except Exception as e:
                #     logging.error(f"Failed to delete catalog file {catalog_path}: {e}")                
                # return {"status": "failure", "file": resolved_path, "error": error_message}
                return {"status": "failure", "file": resolved_path, "error": error_message, "record": record}

            # Handle upload success
            if result and result.returncode == 0:
                file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
                progressDetails["processedFiles"] += 1
                progressDetails["processedBytes"] += file_size
                debug_print(config["logging_path"], f"[UPLOAD SUCCESS] {resolved_path} | {file_size} bytes")

                if transferred_log:
                    write_csv_row(transferred_log, ["Success", resolved_path, file_size, ""])
                if client_log:
                    write_csv_row(client_log, ["Success", resolved_path, file_size, "Client"])

                return {"status": "success", "file": resolved_path, "size": file_size}

            elif result and result.returncode == 7:
                # Special case: upload succeeded but AI metadata export failed
                try:
                    # Parse asset_id from stdout (assume it's on a line like "asset_id: abc123")
                    asset_id = None
                    if result.stdout:
                        for line in result.stdout.splitlines():
                            if line.startswith("Metadata extraction failed for asset:"):
                                asset_id = line.split(":", 1)[1].strip()
                                break
                    if not asset_id:
                        # Fallback: maybe it's just printed plainly as first token?
                        # Adjust based on actual uploader output format
                        asset_id = result.stdout.strip().split()[0] if result.stdout.strip() else None

                    if asset_id:
                        debug_print(config["logging_path"], f"[AI METADATA FAILED] asset_id={asset_id} for {resolved_path}")
                        meta_pending_file = get_meta_pending_path(config["repo_guid"])
                        os.makedirs(os.path.dirname(meta_pending_file), exist_ok=True)
                        write_ai_metadata_failure(meta_pending_file, asset_id, resolved_path, catalog_path, metadata_path)

                        # Count as upload success
                        file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
                        progressDetails["processedFiles"] += 1
                        progressDetails["processedBytes"] += file_size
                        if transferred_log:
                            write_csv_row(transferred_log, ["Success (AI metadata failed)", resolved_path, file_size, asset_id])
                        return {"status": "success_ai_metadata_failed", "file": resolved_path, "asset_id": asset_id}
                except Exception as e:
                    # If we can't extract asset_id, treat as full failure
                    stderr_cleaned = str(e)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    debug_print(config["logging_path"], f"[UPLOAD FAILURE - asset_id parse error] {resolved_path}\n{stderr_cleaned}")
                    write_csv_row(issues_log, ["Error", resolved_path, timestamp, stderr_cleaned])
                    return {"status": "failure", "file": resolved_path, "error": stderr_cleaned, "record": record}

            # Handle upload failure
            else:
                stderr_cleaned = result.stderr.replace("\n", " ").replace("\r", " ").strip() if result and result.stderr else "Unknown Error"
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                debug_print(config["logging_path"], f"[UPLOAD FAILURE] {resolved_path}\n{stderr_cleaned}")
                write_csv_row(issues_log, ["Error", resolved_path, timestamp, stderr_cleaned])
                # try:
                #     if os.path.exists(catalog_path):
                #         os.remove(catalog_path)
                #         logging.info(f"Deleted catalog file: {catalog_path}")
                # except Exception as e:
                #     logging.error(f"Failed to delete catalog file {catalog_path}: {e}")                
                # return {"status": "failure", "file": resolved_path, "error": stderr_cleaned}
                return {"status": "failure", "file": resolved_path, "error": stderr_cleaned, "record": record}

    except Exception as e:
        debug_print(config['logging_path'], f"[ERROR] upload_worker() failed: {e}")
        # return {"status": "failure", "file": record[0], "error": str(e)}
        return {"status": "failure", "file": record[0], "error": str(e), "record": record}

    finally:
        send_progress(progressDetails, config["repo_guid"])

def process_csv_and_upload(config, dry_run=False):
    config["dry_run"] = dry_run
    repo_guid = config["repo_guid"]
    logging_path = config["logging_path"]

    progressDetails = {
        "run_id": config["run_id"],
        "job_id": config["job_id"],
        "progress_path": config["progress_path"],
        "duration": int(time.time()),
        "totalFiles": 0,
        "totalSize": 0,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "Initializing",
    }

    setup_log_and_progress_paths(config)
    transferred_log, issues_log, client_log = prepare_log_files(config)

    pc.start_run("AI link process Initiated ")  # Started

    # Load previous failures
    previous_failures = load_previous_failures(repo_guid, logging_path)
    debug_print(logging_path, f"[RETRY] Loaded {len(previous_failures)} records from previous failure list.")
    pc.update_run(0,"Loaded previous failure records ", "Processing")

    # Determine extensions and size limit
    if config.get("mode") in ("original", "proxy"):
        exts = config.get("extensions", [])
    else:
        exts = []

    file_size_limit = (
        config.get("original_file_size_limit")
        if config.get("mode") == "original" and config.get("original_file_size_limit")
        else None
    )

    progressDetails["status"] = "Reading Files"
    send_progress(progressDetails, config["repo_guid"])

    # Read current batch
    current_records = read_csv_records(config["files_list"], config["logging_path"], exts, file_size_limit)
    debug_print(logging_path, f"[RETRY] Loaded {len(current_records)} records from current CSV.")
    pc.update_run(0,"Loaded fresh records.", "Processing")

    # Combine records
    current_record_base_paths = set()
    combined_records = []

    for record in current_records:
        base = os.path.join(*record[0].split("/./")) if "/./" in record[0] else record[0]
        current_record_base_paths.add(base)
        combined_records.append(record)

    for failed_record in previous_failures:
        base = os.path.join(*failed_record[0].split("/./")) if "/./" in failed_record[0] else failed_record[0]
        if base not in current_record_base_paths:
            combined_records.append(failed_record)
            debug_print(logging_path, f"[RETRY] Adding failed record for retry: {failed_record[0]}")

    debug_print(logging_path, f"[RETRY] Combined record list has {len(combined_records)} items.")
    pc.update_run(5,"Combined records for processing are loaded.", "Processing")

    # ================================
    # CHECK WORKLOAD FLAGS
    # ================================
    has_upload_work = len(combined_records) > 0
    has_metadata_work = has_pending_metadata_work(repo_guid)
    has_ai_proxy_lookup_work = has_ai_proxy_lookup_pending_work(repo_guid)

    # ================================
    # PROCESS AI PROXY LOOKUP (if enabled)
    # ================================
    ai_proxy_success = ai_proxy_failure = 0

    if config.get("ai_proxy_store_enabled", False) or has_ai_proxy_lookup_work:
        pc.update_run(7,"Starting AI Proxy Lookup phase.", "Processing")
        progressDetails["status"] = "AI Proxy Lookup"
        send_progress(progressDetails, config["repo_guid"])
        ai_proxy_success, ai_proxy_failure = process_ai_proxy_lookup(config)
        pc.update_run(10,"AI Proxy Lookup phase completed.", "Processing")

    # Early exit if ONLY proxy lookup is requested
    if config.get("ai_proxy_store_enabled") == True and config.get("provider") == "":
        debug_print(logging_path, "[AI PROXY ONLY MODE] Skipping upload and metadata stages.")
        progressDetails["status"] = "Complete"
        send_progress(progressDetails, repo_guid)
        debug_print(config["logging_path"], f"Upload summary: {ai_proxy_success} succeeded, {ai_proxy_failure} failed")
        pc.update_run(100, "AI Proxy Lookup complete", "Completed")
        sys.exit(0 if ai_proxy_failure == 0 else 3)

    if not has_upload_work and not has_metadata_work:
        debug_print(logging_path, "[EARLY EXIT] No upload work and no pending AI metadata exports.")
        progressDetails["status"] = "Complete"
        pc.update_run(100, "No work to process. Exiting.", "Completed")
        send_progress(progressDetails, config["repo_guid"])
        sys.exit(0)

    # ================================
    # HANDLE UPLOAD WORK
    # ================================
    if has_upload_work:
        progressDetails["totalFiles"] = len(combined_records)
        send_progress(progressDetails, config["repo_guid"])

        pc.update_run(5,"Starting uploads for combined records.", "Processing")

        proxy_map = {}

        # ----------------------------------------------------
        # STEP 1 — BUILD PROXY MAP
        # ----------------------------------------------------
        if config["mode"] == "proxy":
            progressDetails["status"] = "Building Proxy Map"
            send_progress(progressDetails, config["repo_guid"])

            pc.update_run(5,"Looking up for Proxy Files", "Waiting")

            proxy_map = build_proxy_map(combined_records, config, progressDetails)

            pc.update_run(10, "Proxy map resolved", "Processing")

        # ----------------------------------------------------
        # STEP 2 — CALCULATE TOTAL SIZE
        # ----------------------------------------------------
        progressDetails["status"] = "Calculating Total Size"
        pc.update_run(15, "Calculating total upload size", "Processing")

        total_size = calculate_total_size(
            combined_records,
            config,
            proxy_map if config["mode"] == "proxy" else None
        )

        progressDetails["totalSize"] = total_size
        send_progress(progressDetails, config["repo_guid"])
        pc.update_run(20, "Total size computed", "Processing")

        # ----------------------------------------------------
        # STEP 3 — RESOLVE UPLOAD FOLDER IDS
        # ----------------------------------------------------
        resolved_ids = {}
        if config["provider"] in PROVIDERS_SUPPORTING_GET_BASE_TARGET:
            progressDetails["status"] = "Building Folder Map"
            send_progress(progressDetails, config["repo_guid"])

            pc.update_run(22, "Resolving upload target folders", "Waiting")

            resolved_ids = build_folder_id_map(
                combined_records,
                config,
                config["logging_path"],
                progressDetails
            )

            pc.update_run(30, "Upload paths resolved", "Processing")

        # ----------------------------------------------------
        # STEP 4 — UPLOADING FILES
        # ----------------------------------------------------
        current_run_failures = []
        upload_results = []

        progressDetails["status"] = "Uploading Files"
        progressDetails["processedFiles"] = 0
        progressDetails["processedBytes"] = 0
        send_progress(progressDetails, config["repo_guid"])

        pc.update_run(35, "Starting upload phase", "Processing")
        total_records = len(combined_records)
        with ThreadPoolExecutor(max_workers=config["thread_count"]) as executor:
            futures = [
                executor.submit(
                    upload_worker,
                    record,
                    config,
                    resolved_ids,
                    progressDetails,
                    transferred_log,
                    issues_log,
                    client_log,
                    proxy_map,
                )
                for record in combined_records
            ]
            completed = 0

            for future in futures:
                result = future.result()
                upload_results.append(result)

                if result and result.get("status") in ("success", "success_ai_metadata_failed"):
                    completed += 1

                if result and result.get("status") in ["failure", "partial"]:
                    failed_record = result.get("record")
                    if failed_record:
                        current_run_failures.append(failed_record)
                        debug_print(
                            logging_path,
                            f"[RETRY] File failed/partial: {failed_record[0]} | Status: {result.get('status')}"
                        )

                # Update %
                percent = 35 + int((completed / total_records) * 65)
                pc.update_run(percent, f"Uploading files ({completed}/{total_records})", "Processing")

        # Finish upload
        pc.update_run(100, "Upload complete", "Completed")

        save_current_failures(repo_guid, current_run_failures, logging_path)

        success_count = sum(
            1 for r in upload_results if r and r.get("status") in ("success", "success_ai_metadata_failed")
        )
        partial_count = sum(1 for r in upload_results if r and r.get("status") == "partial")
        failure_results = [
            r for r in upload_results if r and r.get("status") == "failure"
        ]

        debug_print(
            config["logging_path"],
            f"Upload summary: {success_count} succeeded, {partial_count} partial, {len(failure_results)} failed"
        )

    else:
        success_count = partial_count = 0
        failure_results = []

    # ================================
    # PROCESS AI METADATA RETRIES
    # ================================
    if has_metadata_work:
        meta_success, meta_failure = retry_ai_metadata_failures(config)
    else:
        meta_success = meta_failure = 0

    debug_print(
        config["logging_path"],
        f"AI Metadata retry summary: {meta_success} succeeded, {meta_failure} failed"
    )

    # ================================
    # FINALIZE PROGRESS
    # ================================
    if not has_upload_work:
        progressDetails["totalFiles"] = 0
        progressDetails["totalSize"] = 0
        progressDetails["processedFiles"] = 0
        progressDetails["processedBytes"] = 0

    progressDetails["status"] = "Complete"
    send_progress(progressDetails, config["repo_guid"])

    # Exit code
    sys.exit(0 if (success_count + partial_count == len(combined_records)) else 2)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Uploader Script Backup")
    parser.add_argument("-c","--json-path", help="Path to JSON config file")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry mode without uploading")
    parser.add_argument("--log-prefix", help="Prefix path for transfer/client/issues CSV logs")
    parser.add_argument("--upload-type", choices=UPLOAD_TYPES, help="Upload mode spreadsheets,video or images")
    args = parser.parse_args()

    config_path = args.json_path

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        request_data = json.load(f)

    required_keys = [
        "provider", "progress_path", "logging_path", "thread_count", "files_list", "cloud_config_name", "job_id","mode", "run_id", "repo_guid","job_guid"
    ]
    optional_keys = ["proxy_output_base_path", "proxy_extra_params", "controller_address"]

    mode = request_data.get("mode")
    
    if not (request_data.get("thread_count") or "") or request_data["thread_count"] < 1:
        request_data["thread_count"] = 2
        
    ext = request_data.get("extensions")
    if not ext or not isinstance(ext, list) or not any(e.strip() for e in ext) or ext == ["*"] or ext == [""] or ext == "*" or ext == "":
        request_data["extensions"] = []
    
    if not (request_data.get("upload_path") or "").strip() or request_data["upload_path"] == "/":
        if "bucket" in request_data and ":" in request_data["bucket"]:
            bucket_id, bucket_name = request_data["bucket"].split(":", 1)
            request_data["upload_path"] = f"/{bucket_id}:/{bucket_name}"
            
    export_ai_metadata = request_data.get("export_ai_metadata")
    if export_ai_metadata and str(export_ai_metadata).lower() in ("1","true","yes"):
        request_data["export"] = True
        
    request_data["ai_proxy_store_enabled"] = True if str(request_data.get("ai_proxy_store_enabled","")).lower() in ("1","true","yes") else False

    if mode in ("original"):
        optional_keys.append("original_file_size_limit")

    # Conditionally require proxy_directory and original_file_size_limit
    if mode == "proxy":
        required_keys.append("proxy_directory")

    # Add upload_type from argument or config
    if args.upload_type and args.upload_type != "video_proxy":
        request_data["upload_type"] = args.upload_type
    elif request_data.get("proxy_type") in UPLOAD_TYPES:
        request_data["upload_type"] = request_data["proxy_type"]
    else:
        request_data["upload_type"] = "video_proxy"

    for key in required_keys:
        if key not in request_data:
            print(f"Missing required field in config: {key}")
            sys.exit(1)
    for key in optional_keys:
        if key in request_data:
            continue
        if key == "proxy_extra_params":
            request_data[key] = {}

    if args.log_prefix:
        request_data["log_prefix"] = args.log_prefix

    provider = request_data.get("provider")
    script_path = PROVIDER_SCRIPTS.get(provider)
    if provider and str(provider).strip() and not script_path:
        print(f"No script path found for provider: {provider}")
        sys.exit(1)

    
    request_data["script_path"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_path) if provider and str(provider).strip() else ""
    
    # crete process reporter instance
    pc.create_config(
        process_guid=request_data.get("job_guid"),     # Unique module identifier
        process_name=request_data.get("repo_name"),                          # Human-friendly name
        process_type="AI Link",                                # Category of process
        hostname=get_job_address(),                                # Optional hostname for tracking
        run_guid=request_data.get("run_id"),         # Predefined run GUID (or omit to auto-create)
        config_dict={
            "Mode": mode,
            "Provider": provider,
            "Remote CloudConfigName": request_data.get("cloud_config_name", ""),
            "Proxy Lookup Path": request_data.get("proxy_directory", ""),
            "Proxy Type": request_data.get("upload_type", "video_proxy"),
            "Threads": request_data.get("thread_count", 2),
        }
    )

    process_csv_and_upload(request_data, args.dry_run)