import csv
import os
import subprocess
import logging
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify

app = Flask(__name__)

PROVIDER_SCRIPTS = {
    "frameio": "providerScripts/framIO/frameIO_complete.py",
    "tessac": "providerScripts/tessac/tessac_uploader.py"
}

# Constants
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

def upload_asset(record, config):
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

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

def process_csv_and_upload(config):
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
        result = upload_asset(record, config)
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