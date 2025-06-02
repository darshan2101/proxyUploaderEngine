import csv
import os
import subprocess
import logging
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
from pathlib import Path

app = Flask(__name__)

# Mapping provider names to their respective upload scripts
PROVIDER_SCRIPTS = {
    "frameio": "providerScripts/framIO/frameIO_complete.py",
    "tessac": "providerScripts/tessac/tessac_uploader.py"
}

# Debug configuration
DEBUG_PRINT = True
DEBUG_TO_FILE = True

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
def upload_asset(record, config):
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
        "--log-level", "debug"
    ]
    if metadata_path:
        cmd.extend(["--metadata-file", metadata_path])

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result, source_path

# Reads CSV, calculates size, triggers parallel upload
def process_csv_and_upload(config):
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
        result, resolved_path = upload_asset(record, config)
        progressDetails["processedFiles"] += 1
        if result and result.returncode == 0:
            file_size = os.path.getsize(resolved_path) if os.path.exists(resolved_path) else 0
            progressDetails["processedBytes"] += file_size
            debug_print(config["logging_path"], f"Upload success: {resolved_path} ({file_size} bytes)")
        else:
            debug_print(config["logging_path"], f"Upload failed: {resolved_path}\n{result.stderr if result else 'No result'})")
        send_progress(progressDetails, config["repo_guid"])

    with ThreadPoolExecutor(max_workers=config["thread_count"]) as executor:
        executor.map(task, records)

    progressDetails["status"] = "complete"
    send_progress(progressDetails, config["repo_guid"])

# API entrypoint for triggering upload from external clients
@app.route('/upload', methods=['POST'])
def handle_upload():
    try:
        request_data = request.get_json()

        provider = request_data.get("provider")
        script_path = PROVIDER_SCRIPTS.get(provider)
        if not script_path:
            return jsonify({"status": "error", "message": f"No script path found for provider: {provider}"}), 400

        request_data["script_path"] = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), script_path
        )

        process_csv_and_upload(request_data)
        return jsonify({"status": "success", "message": "Upload job started and processed."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)