from flask import Flask, request, jsonify
import threading
import queue
import logging
import json
import os
import subprocess
from datetime import datetime
from typing import Optional

app = Flask(__name__)

class ProviderUploader:
    PROVIDER_SCRIPTS = {
        "frameio": "providerScripts/framIO/frameIO_complete.py",
        "tessac": "providerScripts/tessac/tessac_uploader.py"
    }

    def __init__(self, request_data: dict):
        self.request = request_data
        self.file_queue = queue.Queue()
        self.completed_files = 0
        self.total_files = 0
        self.lock = threading.Lock()
        
        provider_script = self.PROVIDER_SCRIPTS.get(request_data['provider'])
        if not provider_script:
            raise ValueError(f"No script path found for provider: {request_data['provider']}")
            
        self.script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            provider_script
        )
        
        # Setup logging
        logging.basicConfig(
            filename=request_data['logging_path'],
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def update_progress(self, message: str):
        try:
            with open(self.request['progress_path'], 'w') as f:
                progress = {
                    "message": message,
                    "timestamp": datetime.now().isoformat(),
                    "completed": self.completed_files,
                    "total": self.total_files,
                    "job_guid": self.request.get('job_guid'),
                    "repo_guid": self.request.get('repo_guid')
                }
                json.dump(progress, f)
        except Exception as e:
            logging.error(f"Error updating progress: {str(e)}")

    def upload_worker(self):
        while True:
            try:
                file_path = self.file_queue.get_nowait()
                
                if not os.path.exists(self.script_path):
                    raise Exception(f"Provider script not found: {self.script_path}")

                cmd = [
                    "python3",
                    self.script_path,
                    "-s", file_path,
                    "-c", self.request['config_file'],
                    "--log-level", "info"
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )

                logging.info(f"Upload output for {file_path}: {result.stdout}")
                
                if result.stderr:
                    logging.warning(f"Upload stderr for {file_path}: {result.stderr}")

                with self.lock:
                    self.completed_files += 1
                    self.update_progress(f"Uploading {self.completed_files} of {self.total_files} files...")
                
                self.file_queue.task_done()
                
            except queue.Empty:
                break
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {str(e)}")

    def process_uploads(self) -> bool:
        try:
            with open(self.request['files_list'], 'r') as f:
                files = [line.strip() for line in f.readlines()]
            
            self.total_files = len(files)
            self.update_progress("Uploading proxies...")
            
            for file_path in files:
                self.file_queue.put(file_path)
            
            threads = []
            thread_count = min(int(self.request['thread_count']), self.total_files)
            
            for _ in range(thread_count):
                thread = threading.Thread(target=self.upload_worker)
                thread.start()
                threads.append(thread)
            
            for thread in threads:
                thread.join()
            
            self.update_progress(f"Completed uploading {self.total_files} files")
            return True
            
        except Exception as e:
            logging.error(f"Error in process_uploads: {str(e)}")
            return False

@app.route('/upload', methods=['POST'])
def start_upload():
    try:
        request_data = request.get_json()
        
        # Validate required fields
        required_fields = ['provider', 'progress_path', 'logging_path', 
                         'thread_count', 'files_list', 'config_file']
        
        for field in required_fields:
            if field not in request_data:
                return jsonify({
                    "status": "error", 
                    "message": f"Missing required field: {field}"
                }), 400

        if request_data['provider'] not in ProviderUploader.PROVIDER_SCRIPTS:
            return jsonify({
                "status": "error",
                "message": f"Unsupported provider. Supported: {list(ProviderUploader.PROVIDER_SCRIPTS.keys())}"
            }), 400

        uploader = ProviderUploader(request_data)
        success = uploader.process_uploads()
        
        if success:
            return jsonify({
                "status": "success",
                "message": "Upload process completed"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Upload process failed"
            }), 500
            
    except Exception as e:
        logging.error(f"Error in upload endpoint: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)