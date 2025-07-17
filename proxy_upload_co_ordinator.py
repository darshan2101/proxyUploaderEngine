import requests
import time
import csv
import os
import subprocess
import logging
import time
import json
import sys
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Debug configuration
DEBUG_PRINT = True
DEBUG_TO_FILE = True

PROXY_GENERATION_HOST = "http://127.0.0.1:8000"
PROVIDERS_SUPPORTING_GET_BASE_TARGET = {"frameio_v2", "frameio_v4", "tessact", "overcasthq", "trint"}
PATH_BASED_PROVIDERS = {"dropbox", "s3", "iconik"}

# Mapping provider names to their respective upload scripts
PROVIDER_SCRIPTS = {
    "frameio": "frame_io_v2_uploader.py",
    "frameio_v2": "frame_io_v2_uploader.py",
    "frameio_v4": "frame_io_v4_uploader.py",
    "tessact": "tessact_uploader.py",
    "overcasthq": "overcasthq_uploader.py",
    "s3": "s3_uploder.py",
    "trint": "trint_uploder.py",
    "twelvelabs": "twelvelabs_uploader.py",
    "box": "box_uploader.py",
    "dropbox": "dropbox_uploader.py",
    "google_drive": "google_drive_uploader.py",
    "iconik": "iconik_uploader.py",
}

def debug_print(log_path, text_string):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    output = f"{formatted_datetime} {text_string}"
    logging.info(output)

    if DEBUG_PRINT and DEBUG_TO_FILE:
        with open(log_path, "a") as debug_file:
            debug_file.write(f"{output}\n")

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
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return upload_path  # fallback
        resolved_id = result.stdout.strip()
        if resolved_id:
            return resolved_id
        return upload_path
    except Exception as e:
        return upload_path

# Locates proxy file by filename pattern inside a directory tree
def resolve_proxy_file(proxy_dir, original_source_path, pattern, log_path):
    base_dir = Path(proxy_dir)

    # Flat search (first-level, non-recursive)
    flat_matches = list(base_dir.glob(pattern))
    if flat_matches:
        resolved_path = str(flat_matches[0])
        debug_print(log_path, f"[FLAT] Using proxy file for upload: {resolved_path}")
        return resolved_path

    # Mirror structure search
    mirror_path = base_dir / Path(original_source_path).parent
    if mirror_path.exists():
        mirror_matches = list(mirror_path.glob(pattern))
        if mirror_matches:
            resolved_path = str(mirror_matches[0])
            debug_print(log_path, f"[MIRROR] Using proxy file for upload: {resolved_path}")
            return resolved_path

    # Segment-stripping search
    path_segments = Path(original_source_path).parts
    for i in range(1, len(path_segments)):
        sub_path = Path(*path_segments[i:-1])  # Exclude i leading segments + filename
        search_path = base_dir / sub_path
        if search_path.exists():
            strip_matches = list(search_path.glob(pattern))
            if strip_matches:
                resolved_path = str(strip_matches[0])
                debug_print(log_path, f"[SEGMENT STRIP] Using proxy file for upload: {resolved_path}")
                return resolved_path

    # Fallback to rglob (recursive, full-tree)
    fallback_matches = sorted([str(f) for f in base_dir.rglob(pattern) if f.is_file()])
    if fallback_matches:
        resolved_path = fallback_matches[0]
        debug_print(log_path, f"[FALLBACK RGLOB] Using proxy file for upload: {resolved_path}")
        return resolved_path

    # Nothing found
    debug_print(log_path, "[MISS] No proxy file found for this pattern.")
    return None

# validate params per mode for proxy_generation
def validate_proxy_params(mode, params):
    required_keys = {
        "generate_video_proxy": ["proxy_params"],
        "generate_video_frame_proxy": ["frame_formate", "proxy_params"],
        "generate_intelligence_proxy": [],
        "generate_video_to_spritesheet": ["frame_formate", "tile_layout", "image_geometry"]
    }
    missing = [k for k in required_keys.get(mode, []) if not params.get(k)]
    if missing:
        raise ValueError(f"Missing required proxy parameters for mode '{mode}': {', '.join(missing)}")

#payload generation for proxy job creation
def build_payload_for_proxy_mode(mode, input_path, output_path, config_params):
    payload = {
        "file_path": input_path,
        "output_path": output_path
    }
    if mode == "generate_video_proxy":
        required = ["proxy_params"]
        payload.update({k: config_params[k] for k in required if k in config_params})
    elif mode == "generate_video_frame_proxy":
        required = ["frame_formate", "proxy_params"]
        optional = ["frame_params"]
        payload.update({k: config_params[k] for k in required if k in config_params})
        payload.update({k: config_params[k] for k in optional if k in config_params})
    elif mode == "generate_intelligence_proxy":
        optional = ["proxy_params"]
        payload.update({k: config_params[k] for k in optional if k in config_params})
    elif mode == "generate_video_to_spritesheet":
        required = ["frame_formate", "tile_layout", "image_geometry"]
        optional = ["frame_params"]
        payload.update({k: config_params[k] for k in required if k in config_params})
        payload.update({k: config_params[k] for k in optional if k in config_params})
    return payload

# generate proxy asset according to options
def generate_proxy_asset(config_mode, input_path, output_path, extra_params, generator_tool = "ffmpeg"):
    base_url = f"{PROXY_GENERATION_HOST}/{generator_tool}/"
    mode_url_map = {
        "generate_video_proxy": "generate_video_proxy",
        "generate_video_frame_proxy": "generate_video_frame_proxy",
        "generate_intelligence_proxy": "generate_intelligence_video_proxy",
        "generate_video_to_spritesheet": "generate_video_to_sprite_sheet"
    }
    if config_mode not in mode_url_map:
        raise ValueError(f"Unsupported proxy generation mode: {config_mode}")

    url = base_url + mode_url_map[config_mode]
    validate_proxy_params(config_mode, extra_params)
    payload = build_payload_for_proxy_mode(config_mode, input_path, output_path, extra_params)

    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        if response.status_code != 200:
            raise Exception(f"API call failed with status {response.status_code}: {response.text}")

        jobid = response.json().get("jobid")
        if not jobid:
            raise Exception("No jobid returned from proxy generation call")

        # Poll job status
        job_status_url = f"{PROXY_GENERATION_HOST}/job_details/{jobid}"
        start_time = time.time()
        timeout = 600  # 10 minutes
        poll_interval = 3

        while time.time() - start_time < timeout:
            job_resp = requests.get(job_status_url)
            if job_resp.status_code != 200:
                raise Exception(f"Failed to fetch job status: {job_resp.text}")
            job_data = job_resp.json()
            status = job_data.get("jobstatus")

            if status == "Success":
                return
            elif status == "Failed":
                raise RuntimeError(f"Proxy generation job {jobid} failed: {job_data.get('description')}")

            time.sleep(poll_interval)

        raise TimeoutError(f"Proxy generation job {jobid} timed out after {timeout} seconds")

    except Exception as e:
        raise RuntimeError(f"Proxy generation failed: {e}")

# Launches the provider script with file arguments
def upload_asset(record, config, dry_run=False, upload_path_id=None, override_source_path=None):
    original_source_path, catalog_path, metadata_path = record

    if "/./" in original_source_path:
        base_source_path = os.path.join(*original_source_path.split("/./"))
        full_upload_path = os.path.join(config["upload_path"].split(":")[-1], original_source_path.split("/./", 1)[1])
    else:
        base_source_path = original_source_path
        full_upload_path = os.path.join(config["upload_path"].split(":")[-1], os.path.basename(original_source_path))

    # Use override proxy file if applicable
    source_path = override_source_path or base_source_path

    # Check if the file extension matches config['extensions']
    original_ext = os.path.splitext(original_source_path)[-1].lower().lstrip(".")
    extensions_lower = [ext.lower() for ext in config.get("extensions", [])]
    
    # Proxy mode: resolve proxy file if needed
    if config["mode"] == "proxy" and original_ext in extensions_lower:
        base_name = os.path.splitext(os.path.basename(base_source_path))[0]
        pattern = f"{base_name}*.*"
        resolved_path = resolve_proxy_file(config["proxy_directory"], base_source_path , pattern, config["logging_path"])

        if resolved_path:
            source_path = resolved_path
            debug_print(config["logging_path"], f"[Proxy Upload] Using proxy file: {source_path}")
        else:
            # Proxy not found for matching extension -> Fail the job & delete catalog
            error_message = f"Proxy file not found for: {base_name} in {config['proxy_directory']}"
            logging.error(error_message)
            debug_print(config["logging_path"], error_message)

            if os.path.exists(catalog_path):
                try:
                    os.remove(catalog_path)
                    logging.info(f"Deleted catalog file: {catalog_path}")
                except Exception as e:
                    logging.error(f"Failed to delete catalog file {catalog_path}: {e}")
            return {"success": False, "error": error_message}, source_path

    cmd = [
        "python3", config["script_path"],
        "--mode", config["mode"],
        "--source-path", source_path,
        "--catalog-path", catalog_path,
        "--config-name", config["cloud_config_name"],
        "--upload-path", upload_path_id or full_upload_path,
        "--job-guid", config["job_guid"],
        "--log-level", "debug"
    ]

    # Append optional args
    if config["mode"] == "original" and "original_file_size_limit" in config:
        cmd += ["--size-limit", str(config["original_file_size_limit"])]
    if config["provider"] == "iconik" and "collection_id" in config:
        cmd += ["--collection-id", config["collection_id"]]
    if config["provider"] == "overcasthq" and "project_id" in config:
        cmd += ["--project-id", config["project_id"]]
    if metadata_path:
        cmd += ["--metadata-file", metadata_path]
    if config["controller_address"]:
        cmd += ["--controller-address", config["controller_address"]]
    if dry_run:
        cmd.append("--dry-run")
    if upload_path_id:
        cmd.append("--resolved-upload-id")

    debug_print(config["logging_path"], f"Command block copy ---------------------> {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result, source_path

# csv row writer helpeer
def write_csv_row(file_path, row):
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

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
                try:
                    size = os.stat(base_source_path).st_size
                except Exception as e:
                    debug_print(logging_path, f"[ERROR] Could not stat file {base_source_path}: {e}")
                    continue
                # Check extension on the resolved base_source_path
                if extensions:
                    if not any(base_source_path.lower().endswith(ext.lower()) for ext in extensions):
                        continue
                if size_limit_bytes is not None and size > size_limit_bytes:
                    debug_print(logging_path, f"[SKIP] File {base_source_path} exceeds size limit ({size} > {size_limit_bytes})")
                    continue
                # Always append a tuple of length 3: (source, catalog, metadata)
                if len(row) == 3:
                    records.append((row[0], row[1], row[2]))
                elif len(row) == 2:
                    records.append((row[0], row[1], None))
    debug_print(logging_path, f"[STEP] Total records loaded: {len(records)}")
    return records

def calculate_total_size(records, config):
    total_size = 0
    for r in records:
        if "/./" in r[0]:
            src = r[0].split("/./")[-1]
            full_path = os.path.join(r[0].split("/./")[0], src)
        else:
            full_path = r[0]

        if config["mode"] == "proxy":
            base_name = os.path.splitext(os.path.basename(full_path))[0]
            pattern = f"{base_name}*.*"
            proxy = resolve_proxy_file(config["proxy_directory"], full_path, pattern, config["logging_path"])
            if proxy and os.path.exists(proxy):
                total_size += os.path.getsize(proxy)
            else:
                total_size += os.path.getsize(full_path)
        elif os.path.exists(full_path):
            total_size += os.path.getsize(full_path)

    debug_print(config['logging_path'], f"[STEP] Total size to upload: {total_size} bytes")
    return total_size

def build_folder_id_map(records, config, log_path):
    resolved_ids = {}
    # Determine base path and parent ID
    upload_path = config["upload_path"]
    if ":" in upload_path:
        parent_id, base_upload_path = upload_path.split(":", 1)
        base_id = parent_id.strip("/")
        resolved_ids[base_upload_path] = base_id
        debug_print(log_path, f"[INIT] Mapping root path '{base_upload_path}' to ID '{base_id}'")

    else:
        base_upload_path = config["upload_path"]
        base_id = resolve_base_upload_id(
            log_path, config["script_path"], config["cloud_config_name"], f"/{base_upload_path}"
        )
        resolved_ids[base_upload_path] = base_id
        debug_print(log_path, f"[INIT] Resolved base path '{base_upload_path}' to ID '{base_id}'")

    # Subtree creation for /./ based paths
    for record in records:
        original_source_path = record[0]
        if "/./" in original_source_path:
            _, sub_path = original_source_path.split("/./", 1)
            path_segments = [seg for seg in sub_path.split(os.sep) if seg]
            current_path = base_upload_path
            current_id = base_id

            for idx, segment in enumerate(path_segments):
                if idx < len(path_segments) - 1:
                    next_path = os.path.join(current_path, segment)
                    if next_path not in resolved_ids:
                        debug_print(log_path, f"[STEP] Resolving segment: {segment} under {current_path}")
                        resolved_id = resolve_base_upload_id(
                            config["logging_path"],
                            config["script_path"],
                            config["cloud_config_name"],
                            f"/{segment}",
                            parent_id=current_id
                        )
                        resolved_ids[next_path] = resolved_id
                    current_path = next_path
                    current_id = resolved_ids[next_path]

    return resolved_ids

def upload_worker(record, config, resolved_ids, progressDetails, transferred_log, issues_log, client_log):
    try:
        debug_print(config['logging_path'], f"[STEP] Processing record: {record}")
        original_source_path, catalog_path, metadata_path = record

        # Extract relative folder path for this record
        if "/./" in original_source_path:
            _, sub_path = original_source_path.split("/./", 1)
            dir_path = os.path.dirname(sub_path)
        else:
            dir_path = ""  # root upload

        # Compute key for resolved_ids lookup
        base_path = config["upload_path"]
        if ":" in base_path:
            _, logical_base = base_path.split(":", 1)
        else:
            logical_base = base_path
        normalized_folder_key = os.path.normpath(os.path.join(logical_base.strip("/"), dir_path))

        # Determine upload path ID
        if config["provider"] in PATH_BASED_PROVIDERS:
            upload_path_id = None
        else:
            upload_path_id = resolved_ids.get(normalized_folder_key, list(resolved_ids.values())[0])  # fallback to base_id

        debug_print(config['logging_path'], f"[PATH-MATCH] {normalized_folder_key} -> using ID {upload_path_id}")

        # --- Optional: proxy generation step ---
        override_source_path = None
        if config["mode"] in [
            "generate_video_proxy", "generate_video_frame_proxy",
            "generate_intelligence_proxy", "generate_video_to_spritesheet"
        ]:
            base_source_path = os.path.join(*original_source_path.split("/./")) if "/./" in original_source_path else original_source_path
            name_wo_ext, ext = os.path.splitext(os.path.basename(base_source_path))
            proxy_ext = ".png" if "spritesheet" in config["mode"] else ext
            proxy_output_path = os.path.join(config["proxy_output_base_path"], name_wo_ext + proxy_ext)
            try:
                generate_proxy_asset(config["mode"], base_source_path, proxy_output_path, config.get("proxy_extra_params", {}))
                override_source_path = proxy_output_path
            except Exception as e:
                msg = f"[ERROR] Proxy generation failed: {e}"
                debug_print(config["logging_path"], msg)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                write_csv_row(issues_log, ["Error", base_source_path, timestamp, msg])
                send_progress(progressDetails, config["repo_guid"])
                return

        result, resolved_path = upload_asset(record, config, config.get("dry_run", False), upload_path_id, override_source_path)
        
        # --- Handle proxy failure case ---
        if isinstance(result, dict) and not result.get("success", True):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_message = result.get("error", "Unknown error during proxy resolution or upload")
            debug_print(config["logging_path"], f"[PROXY FAILURE] {resolved_path}\n{error_message}")
            write_csv_row(issues_log, ["Error", resolved_path, timestamp, error_message])
            return {"status": "failure", "file": resolved_path, "error": error_message}
        
        # --- Handle normal subprocess result ---
        if result and result.returncode == 0:
            file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
            progressDetails["processedFiles"] += 1
            progressDetails["processedBytes"] += file_size
            debug_print(config["logging_path"], f"[UPLOAD SUCCESS] {resolved_path} | {file_size} bytes")
            if transferred_log: write_csv_row(transferred_log, ["Success", resolved_path, file_size, ""])
            if client_log: write_csv_row(client_log, ["Success", resolved_path, file_size, "Client"])
            return {"status": "success", "file": resolved_path, "size": file_size}
        else:
            stderr_cleaned = result.stderr.replace("\n", " ").replace("\r", " ").strip() if result and result.stderr else "Unknown Error"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            debug_print(config["logging_path"], f"[UPLOAD FAILURE] {resolved_path}\n{stderr_cleaned}")
            write_csv_row(issues_log, ["Error", resolved_path, timestamp, stderr_cleaned])
            return {"status": "failure", "file": resolved_path, "error": stderr_cleaned}
    except Exception as e:
        debug_print(config['logging_path'], f"[ERROR] upload_asset() failed: {e}")
    finally:
        send_progress(progressDetails, config["repo_guid"])

def process_csv_and_upload(config, dry_run=False):
    config["dry_run"] = dry_run
    setup_log_and_progress_paths(config)
    transferred_log, issues_log, client_log = prepare_log_files(config)
    
    exts = exts = config["extensions"] if config.get("mode") in ("original", "proxy") and config.get("extensions") else []
    file_size_limit = config.get("original_file_size_limit") if config.get("mode") == "original" and config.get("original_file_size_limit") else None
    records = read_csv_records(config["files_list"], config["logging_path"], exts, file_size_limit)
    if not records:
        size_limit_str = f"{file_size_limit} MB" if file_size_limit else "No Limit"
        extensions_str = ", ".join(exts) if exts else "All"
        print(f"No records found with size <= {size_limit_str} and extensions: {extensions_str}")
        sys.exit(0)    
    total_size = calculate_total_size(records, config)

    progressDetails = {
        "run_id": config["run_id"],
        "job_id": config["job_id"],
        "progress_path": config["progress_path"],
        "duration": int(time.time()),
        "totalFiles": len(records),
        "totalSize": total_size,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "Preparing to Upload"
    }

    send_progress(progressDetails, config["repo_guid"])
    if not config["provider"] in PATH_BASED_PROVIDERS:
        resolved_ids = build_folder_id_map(records, config, config["logging_path"])
    else:
        resolved_ids = []
    debug_print(config["logging_path"],f" Resolved ids directory ---------------------------> {resolved_ids}")
    upload_results = []
    progressDetails["status"] = "Uploading Files"
    send_progress(progressDetails, config["repo_guid"])
    with ThreadPoolExecutor(max_workers=config["thread_count"]) as executor:
        futures = [
            executor.submit(upload_worker, record, config, resolved_ids, progressDetails, transferred_log, issues_log, client_log)
            for record in records
        ]
        for future in futures:
            upload_results.append(future.result())
    success_count = sum(1 for r in upload_results if r and r.get("status") == "success")
    failure_results = [r for r in upload_results if r and r.get("status") == "failure"]

    debug_print(config["logging_path"], f"Upload summary: {success_count} succeeded, {len(failure_results)} failed")
    print(f"Successful Uploads:{success_count}")
    print(f"Failed Uploads:{len(failure_results)}")
            
    progressDetails["status"] = "complete"
    send_progress(progressDetails, config["repo_guid"])
    sys.exit(0 if success_count == len(records) else 2)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Uploader Script Backup")
    parser.add_argument("-c","--json-path", help="Path to JSON config file")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry mode without uploading")
    parser.add_argument("--log-prefix", help="Prefix path for transfer/client/issues CSV logs")
    args = parser.parse_args()

    config_path = args.json_path

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        request_data = json.load(f)

    required_keys = [
        "provider", "progress_path", "logging_path", "thread_count", "files_list", "cloud_config_name", "job_id","upload_path","mode", "run_id", "repo_guid","job_guid"
    ]
    optional_keys = ["proxy_output_base_path", "proxy_extra_params", "controller_address"]

    mode = request_data.get("mode")

    if mode in ("original"):
        optional_keys.append("original_file_size_limit")

    # Conditionally require proxy_directory and original_file_size_limit
    if mode == "proxy":
        required_keys.append("proxy_directory")
        required_keys.append("extensions")
    
    if "generate" in mode:
        required_keys.append("proxy_output_base_path")
        os.makedirs(request_data.get("proxy_output_base_path"), exist_ok=True)  # ✅ Ensure target dir exists


    for key in required_keys:
        if key not in request_data:
            print(f"Missing required field in config: {key}")
            sys.exit(1)
    for key in optional_keys:
        request_data.setdefault(key, {} if key == "proxy_extra_params" else None)

    if args.log_prefix:
        request_data["log_prefix"] = args.log_prefix

    provider = request_data.get("provider")
    script_path = PROVIDER_SCRIPTS.get(provider)
    if not script_path:
        print(f"No script path found for provider: {provider}")
        sys.exit(1)

    full_script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), script_path
    )
    request_data["script_path"] = full_script_path

    process_csv_and_upload(request_data, args.dry_run)