from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import threading
import queue
import logging
import json
import os
import subprocess
from typing import List, Optional
from datetime import datetime

# FastAPI app
app = FastAPI()

class UploadRequest(BaseModel):
    provider: str
    progress_path: str
    logging_path: str
    thread_count: int
    files_list: str
    job_guid: Optional[str]
    repo_guid: Optional[str]
    config_file: str  # Config file for provider script

class ProviderUploader:
    PROVIDER_SCRIPTS = {
        "frameio": "providerScripts/framIO/frameIO_complete.py",
        "tessac": "providerScripts/tessac/tessac_uploader.py"  # for future implementation
    }

    def __init__(self, request: UploadRequest):
        self.request = request
        self.file_queue = queue.Queue()
        self.completed_files = 0
        self.total_files = 0
        self.lock = threading.Lock()
        self.script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.PROVIDER_SCRIPTS.get(request.provider)
        )
        
        # Setup logging
        logging.basicConfig(
            filename=request.logging_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def update_progress(self, message: str):
        """Update progress file with current status"""
        try:
            with open(self.request.progress_path, 'w') as f:
                progress = {
                    "message": message,
                    "timestamp": datetime.now().isoformat(),
                    "completed": self.completed_files,
                    "total": self.total_files,
                    "job_guid": self.request.job_guid,
                    "repo_guid": self.request.repo_guid
                }
                json.dump(progress, f)
        except Exception as e:
            logging.error(f"Error updating progress: {str(e)}")

    def upload_worker(self):
        """Worker thread function to process uploads"""
        while True:
            try:
                file_path = self.file_queue.get_nowait()
                
                if not os.path.exists(self.script_path):
                    raise Exception(f"Provider script not found: {self.script_path}")

                # Run provider script with parameters
                cmd = [
                    "python3",
                    self.script_path,
                    "-s", file_path,
                    "-c", self.request.config_file,
                    "--log-level", "info"
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )

                logging.info(f"Upload output for {file_path}:")
                logging.info(result.stdout)
                
                if result.stderr:
                    logging.warning(f"Upload stderr for {file_path}:")
                    logging.warning(result.stderr)

                with self.lock:
                    self.completed_files += 1
                    self.update_progress(f"Uploading {self.completed_files} of {self.total_files} files...")
                
                self.file_queue.task_done()
                
            except queue.Empty:
                break
            except subprocess.CalledProcessError as e:
                logging.error(f"Error running upload script for {file_path}:")
                logging.error(f"Exit code: {e.returncode}")
                logging.error(f"Output: {e.output}")
                logging.error(f"Stderr: {e.stderr}")
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {str(e)}")

    def process_uploads(self) -> bool:
        """Main method to process uploads using multiple threads"""
        try:
            # Read files list
            with open(self.request.files_list, 'r') as f:
                files = [line.strip() for line in f.readlines()]
            
            self.total_files = len(files)
            
            # Initial progress update
            self.update_progress("Uploading proxies...")
            
            # Add files to queue
            for file_path in files:
                self.file_queue.put(file_path)
            
            # Create and start worker threads
            threads = []
            for _ in range(min(self.request.thread_count, self.total_files)):
                thread = threading.Thread(target=self.upload_worker)
                thread.start()
                threads.append(thread)
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
            
            # Final progress update
            self.update_progress(f"Completed uploading {self.total_files} files")
            return True
            
        except Exception as e:
            logging.error(f"Error in process_uploads: {str(e)}")
            return False

@app.post("/upload")
async def start_upload(request: UploadRequest):
    try:
        if request.provider not in ProviderUploader.PROVIDER_SCRIPTS:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported provider. Supported: {list(ProviderUploader.PROVIDER_SCRIPTS.keys())}"
            )
            
        uploader = ProviderUploader(request)
        success = uploader.process_uploads()
        
        if success:
            return {"status": "success", "message": "Upload process completed"}
        else:
            raise HTTPException(status_code=500, detail="Upload process failed")
            
    except Exception as e:
        logging.error(f"Error in upload endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)