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

# Mapping provider names to their respective upload scripts
PROVIDER_SCRIPTS = {
    "frameio_v2": "providerScripts/frameIO/frameIO_v2.py",
    "frameio_v4": "providerScripts/frameIO/frameIO_v4.py",
    "tessact": "providerScripts/tessact/tessact_uploader.py",
    "overcast": "providerScripts/overcastHQ/overcast_uploader.py"
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

# Locates proxy file by filename pattern and extension inside a directory tree
def resolve_proxy_file(directory, pattern, extensions):
    logging.debug(f"Searching for file using pattern in: {directory}")
    try:
        base_path = Path(directory)
        if not base_path.exists():
            logging.error(f"Directory does not exist: {directory}")
            return None

        if not base_path.is_dir() or not os.access(directory, os.R_OK):
            logging.error(f"No read permission for directory: {directory}")
            return None

        for matched_file in base_path.rglob(pattern):
            if matched_file.is_file():
                if extensions and not any(matched_file.name.lower().endswith(ext.lower()) for ext in extensions):
                    logging.debug(f"File {matched_file.name} does not match extensions")
                    continue
                logging.debug(f"Found matching file: {str(matched_file)}")
                return str(matched_file)

        return None
    except PermissionError as e:
        logging.error(f"Permission denied accessing directory: {e}")
        return None
    except Exception as e:
        logging.error(f"Error during file search: {e}")
        return None

# Launches the provider script with file arguments
def upload_asset(record, config, dry_run=False):
    original_source_path, catalog_path, metadata_path = record

    if "/./" in original_source_path:
        prefix, sub_path = original_source_path.split("/./", 1)
        base_source_path = os.path.join(prefix, sub_path)
        upload_path = os.path.join(config["upload_path"], sub_path)
    else:
        base_source_path = original_source_path
        upload_path = config["upload_path"]

    if config["mode"] == "proxy":
        base_name = os.path.splitext(os.path.basename(base_source_path))[0]
        pattern = f"{base_name}*"
        resolved_path = resolve_proxy_file(config["proxy_directory"], pattern, config["extensions"])
        if not resolved_path:
            debug_print(config["logging_path"], f"Proxy not found for: {base_source_path}")
            return None, base_source_path
        source_path = resolved_path
    else:
        source_path = base_source_path

    cmd = [
        "python3", config["script_path"],
        "--mode", config["mode"],
        "--source-path", source_path,
        "--catalog-path", catalog_path,
        "--config-name", config["cloud_config_name"],
        "--upload-path", upload_path,
        "--jobId", config["jobId"],
        "--size-limit", str(config["original_file_size_limit"]),
        "--log-level", "error"
    ]
    if metadata_path:
        cmd.extend(["--metadata-file", metadata_path])
    if dry_run:
        cmd.append("--dry-run")
    if config['provider'] == "overcasthq":
        project_id = config.get("project_id")
        folder_id = config.get("folder_id")
        if project_id:
            cmd.extend(["-p", project_id])
        if folder_id:
            cmd.extend(["-f", folder_id])
        
    print(f" Command block copy ---------------------> {cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result, source_path

def write_csv_row(file_path, row):
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# Reads input CSV fileList, calculates size, triggers parallel upload
def process_csv_and_upload(config, dry_run=False):
    records = []
    with open(config["files_list"], 'r') as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            if len(row) == 3:
                records.append(tuple(row))

    total_files = len(records)
    total_size = 0
    for r in records:
        if "/./" in r[0]:
            src = r[0].split("/./")[-1]
            full_path = os.path.join(r[0].split("/./")[0], src)
        else:
            full_path = r[0]
        if config["mode"] == "proxy":
            base_name = os.path.splitext(os.path.basename(full_path))[0]
            pattern = f"{base_name}*"
            proxy = resolve_proxy_file(config["proxy_directory"], pattern, config["extensions"])
            if proxy and os.path.exists(proxy):
                total_size += os.path.getsize(proxy)
        elif os.path.exists(full_path):
            total_size += os.path.getsize(full_path)

    job_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    config["logging_path"] = os.path.join(config["logging_path"], f"{job_time_str}_{config['mode']}_log.txt")
    config["progress_path"] = os.path.join(config["progress_path"], f"{job_time_str}_{config['mode']}_progress.xml")

    log_prefix = config.get("log_prefix")
    if log_prefix:
        os.makedirs(os.path.dirname(log_prefix), exist_ok=True)
        transferred_log = f"{log_prefix}-uploader-transferred.csv"
        issues_log = f"{log_prefix}-uploader-issues.csv"
        client_log = f"{log_prefix}-uploader-client.csv"
        for log in [transferred_log, issues_log, client_log]:
            with open(log, "w", newline="") as f:
                csv.writer(f).writerow(["Status","Filename", "Size", "Detail"] if "issues" not in log else ["Status", "Filename","Timestamp", "Issue"])
    else:
        transferred_log = issues_log = client_log = None

    os.makedirs(os.path.dirname(config["logging_path"]), exist_ok=True)
    os.makedirs(os.path.dirname(config["progress_path"]), exist_ok=True)

    progressDetails = {
        "run_id": config["runId"],
        "job_id": config["jobId"],
        "progress_path": config["progress_path"],
        "duration": int(time.time()),
        "totalFiles": total_files,
        "totalSize": total_size,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "in-progress"
    }

    send_progress(progressDetails, config["repo_guid"])

    def task(record):
        result, resolved_path = upload_asset(record, config, dry_run)
        progressDetails["processedFiles"] += 1
        if result and result.returncode == 0:
            file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
            progressDetails["processedBytes"] += file_size
            debug_print(config["logging_path"], f"Upload success: {resolved_path} ({file_size} bytes)")
            if transferred_log:
                write_csv_row(transferred_log, ["Success" ,resolved_path, file_size, ""])
            if client_log:
                write_csv_row(client_log, ["Success", resolved_path, file_size, "Client"])
        else:
            debug_print(config["logging_path"], f"Upload failed: {resolved_path}\n{result.stderr if result else 'No result'}")
            if issues_log:
                stderr_cleaned = result.stderr.replace("\n", " ").replace("\r", " ").strip() if result and result.stderr else "No result"
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                write_csv_row(issues_log, ["Error", resolved_path, timestamp, stderr_cleaned])
            else:
                write_csv_row(issues_log, ["Error", resolved_path, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "No result"])
        send_progress(progressDetails, config["repo_guid"])

    with ThreadPoolExecutor(max_workers=config["thread_count"]) as executor:
        executor.map(task, records)

    progressDetails["status"] = "complete"
    send_progress(progressDetails, config["repo_guid"])

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
        "provider", "progress_path", "logging_path", "thread_count",
        "files_list", "cloud_config_name", "jobId",
        "proxy_directory", "original_file_size_limit", "upload_path",
        "extensions", "mode", "runId", "repo_guid"
    ]
    for key in required_keys:
        if key not in request_data:
            print(f"Missing required field in config: {key}")
            sys.exit(1)

    if args.log_prefix:
        request_data["log_prefix"] = args.log_prefix

    provider = request_data.get("provider")
    script_path = PROVIDER_SCRIPTS.get(provider)
    if not script_path:
        print(f"No script path found for provider: {provider}")
        sys.exit(1)

    request_data["script_path"] = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), script_path
    )

    process_csv_and_upload(request_data, args.dry_run)