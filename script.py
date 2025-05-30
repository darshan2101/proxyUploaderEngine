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

# Constants
DEBUG_PRINT = True
DEBUG_TO_FILE = True

PROVIDER_SCRIPTS = {
    "frameio": "providerScripts/framIO/frameIO_complete.py",
    "tessac": "providerScripts/tessac/tessac_uploader.py"
}

def debug_print(log_path, text_string):
    if not DEBUG_PRINT:
        return
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    output = f"{formatted_datetime} {text_string}"
    if DEBUG_TO_FILE:
        with open(log_path, "a") as debug_file:
            debug_file.write(f"{output}\n")
    else:
        print(output)

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

    with open(progress_path, "w") as file:
        file.write(xml_str)

def upload_asset(record, config, dry_run=False):
    source_path, catalog_path, metadata_path = record
    cmd = [
        "python3", config["script_path"],
        "--mode", config["mode"],
        "--source-path", source_path,
        "--catalog-path", catalog_path,
        "--config-name", config["cloud_config_name"],
        "--upload-path", config["upload_path"],
        "--jobId", config["jobId"],
        "--proxy_directory", config["proxy_directory"],
        "--size-limit", str(config["original_file_size_limit"]),
        "--extensions", ','.join(config["extensions"]),
        "--log-level", "info"
    ]
    if metadata_path:
        cmd.extend(["--metadata-file", metadata_path])
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

def process_csv_and_upload(config, dry_run=False):
    records = []
    with open(config["files_list"], 'r') as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            if len(row) == 3:
                records.append(tuple(row))

    total_files = len(records)
    progressDetails = {
        "run_id": config["job_guid"],
        "job_id": config["jobId"],
        "progress_path": config["progress_path"],
        "duration": int(time.time()),
        "totalFiles": total_files,
        "totalSize": 0,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "in-progress"
    }

    send_progress(progressDetails, config["repo_guid"])

    def task(record):
        result = upload_asset(record, config, dry_run)
        progressDetails["processedFiles"] += 1
        if result.returncode == 0:
            debug_print(config["logging_path"], f"Upload success: {record[0]}")
        else:
            debug_print(config["logging_path"], f"Upload failed: {record[0]}\n{result.stderr}")
        send_progress(progressDetails, config["repo_guid"])

    with ThreadPoolExecutor(max_workers=config["thread_count"]) as executor:
        executor.map(task, records)

    progressDetails["status"] = "complete"
    send_progress(progressDetails, config["repo_guid"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Uploader Script Backup")
    parser.add_argument("json_path", help="Path to JSON config file")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry mode without uploading")
    args = parser.parse_args()

    config_path = args.json_path

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        request_data = json.load(f)

    required_keys = [
        "provider", "progress_path", "logging_path", "thread_count", 
        "files_list", "config_file", "cloud_config_name", "jobId",
        "proxy_directory", "original_file_size_limit", "upload_path",
        "extensions", "mode", "job_guid", "repo_guid"
    ]
    for key in required_keys:
        if key not in request_data:
            print(f"Missing required field in config: {key}")
            sys.exit(1)

    provider = request_data.get("provider")
    script_path = PROVIDER_SCRIPTS.get(provider)
    if not script_path:
        print(f"No script path found for provider: {provider}")
        sys.exit(1)

    request_data["script_path"] = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), script_path
    )

    process_csv_and_upload(request_data, args.dry_run)