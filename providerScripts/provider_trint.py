# provider_trint.py
import argparse
import sys
import os
import json
import time
import logging
import magic # Ensure python-magic is installed
from configparser import ConfigParser
from datetime import datetime
from urllib.parse import urlencode
import requests
from requests.auth import HTTPBasicAuth
import plistlib

# --- Configuration & Platform Detection ---
IS_LINUX = os.path.isdir("/opt/sdna/bin/")
IS_WINDOWS = os.name == 'nt'

DNA_CLIENT_SERVICES_LINUX = '/etc/StorageDNA/DNAClientServices.conf'
DNA_CLIENT_SERVICES_MAC = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
DNA_CLIENT_SERVICES_WINDOWS = r'D:\etc\StorageDNA\DNAClientServices.conf'

SERVERS_CONF_LINUX = '/etc/StorageDNA/Servers.conf'
SERVERS_CONF_MAC = '/Library/Preferences/com.storagedna.Servers.plist'

DEBUG_FILEPATH = '/tmp/trint_debug.out'
DEBUG_PRINT = False
DEBUG_TO_FILE = False

def debug_print(filepath, to_stdout, to_file, message):
    """Unified debug print function"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    if to_stdout:
        print(log_msg)
    if to_file:
        try:
            with open(filepath, 'a') as f:
                f.write(log_msg + '\n')
        except Exception as e:
            print(f"Failed to write to debug file: {e}")

def get_dna_client_services_path():
    if IS_LINUX:
        return DNA_CLIENT_SERVICES_LINUX
    elif IS_WINDOWS:
        return DNA_CLIENT_SERVICES_WINDOWS
    else:
        return DNA_CLIENT_SERVICES_MAC

def get_servers_conf_path():
    return SERVERS_CONF_LINUX if IS_LINUX else SERVERS_CONF_MAC

def get_cloud_config_path():
    try:
        client_config_path = get_dna_client_services_path()
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Reading client config: {client_config_path}")

        if IS_LINUX or IS_WINDOWS:
            config = ConfigParser()
            config.read(client_config_path)
            if config.has_option('General', 'cloudconfigfolder'):
                config_path = os.path.join(config.get('General', 'cloudconfigfolder'), "cloud_targets.conf")
            else:
                raise KeyError("cloudconfigfolder not found in General section")
        else:
            with open(client_config_path, 'rb') as fp:
                plist_data = plistlib.load(fp)
                config_path = os.path.join(plist_data["CloudConfigFolder"], "cloud_targets.conf")

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Using cloud config: {config_path}")
        return config_path
    except Exception as e:
        fallback = r'D:\Dev\(python +node) Wrappers\proxyUploaderEngine\CloudConfig\cloud_targets.conf'
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                    f"Config path not found, using fallback: {fallback}\nException: {e}")
        return fallback

def get_link_address_and_port():
    path = get_servers_conf_path()
    ip, port = "", ""
    try:
        if IS_LINUX:
            config = ConfigParser()
            config.read(path)
            ip = config.get('General', 'link_address', fallback='')
            port = config.get('General', 'link_port', fallback='')
        else:
            with open(path, 'rb') as fp:
                plist_data = plistlib.load(fp)
                ip = plist_data.get('link_address', '')
                port = str(plist_data.get('link_port', ''))
    except Exception as e:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error reading Servers.conf: {e}")
        sys.exit(5)
    return ip, port

# --- Trint API Client ---
class TrintProvider:
    def __init__(self, config_map, config_name):
        self.api_key_id = config_map['api_key_id']
        self.api_key_secret = config_map['api_key_secret']
        self.base_url = config_map.get('base_url', 'https://api.trint.com')
        self.upload_url = config_map.get('upload_url', 'https://upload.trint.com')
        self.config_name = config_name
        self.auth = HTTPBasicAuth(self.api_key_id, self.api_key_secret)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.current_user = None

    def _request(self, method, url, **kwargs):
        try:
            response = self.session.request(method, url, **kwargs)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                        f"API {method} {url} -> {response.status_code}")
            if response.status_code >= 400:
                debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                            f"Error response: {response.text}")
            return response
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Request failed: {e}")
            return None

    def list_workspaces(self):
        """List all Shared Drives (Workspaces)"""
        url = f"{self.base_url}/workspaces/"
        response = self._request("GET", url, headers={"accept": "application/json"})
        if response and response.status_code == 200:
            return response.json().get("data", [])
        return []

    def list_folders(self, workspace_id=None):
        """List folders in a workspace or root"""
        url = f"{self.base_url}/folders/"
        params = {}
        if workspace_id:
            params["workspaceId"] = workspace_id
        response = self._request("GET", url, headers={"accept": "application/json"}, params=params)
        if response and response.status_code == 200:
            return response.json().get("data", [])
        return []

    def list_files(self, folder_id=None, workspace_id=None):
        """List files (transcripts) in a folder or workspace"""
        url = f"{self.base_url}/transcripts/"
        params = {}
        if folder_id:
            params["folderId"] = folder_id
        if workspace_id:
            params["sharedDriveId"] = workspace_id  # or workspaceId for backward compat
        response = self._request("GET", url, headers={"accept": "application/json"}, params=params)
        if response and response.status_code == 200:
            return response.json().get("data", [])
        return []

    def create_folder(self, folder_name, parent_id=None, workspace_id=None):
        """Create a folder"""
        url = f"{self.base_url}/folders/"
        payload = {"name": folder_name}
        if parent_id:
            payload["parentId"] = parent_id
        if workspace_id:
            payload["workspaceId"] = workspace_id
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        response = self._request("POST", url, headers=headers, json=payload)
        if response and response.status_code in [200, 201]:
            folder_data = response.json()
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                        f"Folder '{folder_name}' created with ID: {folder_data['_id']}")
            return folder_data
        return None

    def upload_file(self, file_path, folder_id=None, workspace_id=None, language="en", backlink_url=None):
        """Upload a file to Trint"""
        file_name = os.path.basename(file_path)
        mime_type = self._detect_mime(file_path)
        if not mime_type:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Cannot detect MIME type for {file_name}")
            return None

        data = {
            "filename": file_name,
            "language": language
        }
        if backlink_url:
            data["metadata"] = backlink_url
        if folder_id:
            data["folder-id"] = folder_id
        if workspace_id:
            data["workspace-id"] = workspace_id

        with open(file_path, 'rb') as f:
            files = {'file': (file_name, f, mime_type)}
            response = self._request("POST", self.upload_url, data=data, files=files)

        if response and response.status_code == 200:
            result = response.json()
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                        f"Upload successful: {result.get('trintId')}")
            return result
        else:
            error_msg = response.text if response else "No response"
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Upload failed: {error_msg}")
            return None

    def _detect_mime(self, file_path):
        try:
            return magic.from_file(file_path, mime=True)
        except ImportError:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "python-magic not available, using fallback")
            # Basic fallback
            if file_path.lower().endswith(('.mp3', '.wav', '.m4a')):
                return 'audio/mpeg'
            elif file_path.lower().endswith(('.mp4', '.mov', '.avi')):
                return 'video/mp4'
            else:
                return 'application/octet-stream'

# --- Helper Functions ---
def loadConfigurationMap(config_name):
    config_path = get_cloud_config_path()
    if not os.path.exists(config_path):
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Config file not found: {config_path}")
        return None
    config = ConfigParser()
    config.read(config_path)
    if config_name not in config:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Config section '{config_name}' not found")
        return None
    return dict(config[config_name])

def replace_file_path(path):
    """Sanitize path separators"""
    return path.replace("\\", "/")

def get_catalog_path(params):
    """Mock catalog path (as in Box provider)"""
    return f"{params.get('project_name', 'default')}/{params.get('label', 'default')}"

def check_if_catalog_file_exists(catalog_path, file_name, mtime):
    """Placeholder for dedup logic"""
    return False

def generate_xml_from_file_objects(obj_dict, target_path):
    """Generate XML output (simplified)"""
    try:
        with open(target_path, 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n<objects>\n')
            for obj in obj_dict.get("filelist", []):
                f.write(f'  <object name="{obj["name"]}" size="{obj["size"]}" />\n')
            f.write('</objects>\n')
        return True
    except Exception as e:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"XML generation failed: {e}")
        return False

def add_CDATA_tags_with_id_for_folder(folders):
    """Generate XML output for folder browse"""
    xml = '<?xml version="1.0" encoding="UTF-8"?><folders>'
    for folder in folders:
        name = folder["name"].replace("&", "&amp;").replace("<", "<").replace(">", ">")
        xml += f'<folder id="{folder["id"]}"><![CDATA[{name}]]></folder>'
    xml += '</folders>'
    return xml

def GetObjectDict(files_list, params, catalog_path):
    scanned_files = 0
    selected_count = 0
    total_size = 0
    file_object_list = []
    for file_data in files_list:
        try:
            file_name = file_data.get("filename", "Unknown")
            file_size = int(file_data.get("mediaSize", 0))
            file_id = file_data.get("_id", "")
            mtime_str = file_data.get("createdAt", "")
            mtime_epoch = 0
            if mtime_str:
                try:
                    dt = datetime.fromisoformat(mtime_str.replace("Z", "+00:00"))
                    mtime_epoch = int(dt.timestamp())
                except Exception:
                    pass
            if check_if_catalog_file_exists(catalog_path, file_name, mtime_epoch):
                continue
            file_object = {
                "name": replace_file_path(file_name),
                "size": str(file_size),
                "mode": "0",
                "tmpid": file_id,
                "type": "F_REG",
                "mtime": str(mtime_epoch),
                "atime": str(mtime_epoch),
                "owner": "0",
                "group": "0",
                "index": params.get("indexid", "0")
            }
            scanned_files += 1
            selected_count += 1
            total_size += file_size
            file_object_list.append(file_object)
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error processing file: {e}")
            continue
    return {
        "filelist": file_object_list,
        "scanned_count": scanned_files,
        "selected_count": selected_count,
        "total_size": total_size
    }

# --- Main CLI Handler ---
def main():
    parser = argparse.ArgumentParser(description="Trint Provider CLI")
    parser.add_argument('-c', '--config', required=True, help='Configuration name (e.g., trint-prod)')
    parser.add_argument('-m', '--mode', required=True, help='Mode: list, browse, upload, createfolder, actions, buckets, bucketsfolders')
    parser.add_argument('-s', '--source', help='Source file path (for upload)')
    parser.add_argument('-t', '--target', help='Target path (for list output XML)')
    parser.add_argument('-f', '--foldername', help='Folder name to create')
    parser.add_argument('-tid', '--tempid', help='File ID (unused in Trint)')
    parser.add_argument('-id', '--folder_id', help='Folder ID (for list, upload, browse)')
    parser.add_argument('-in', '--indexid', help='Index ID (for list)')
    parser.add_argument('-jg', '--jobguid', help='Job GUID')
    parser.add_argument('-r', '--runid', help='Run ID')
    parser.add_argument('-ji', '--jobid', help='Job ID')
    parser.add_argument('-p', '--projectname', help='Project name')
    parser.add_argument('-l', '--label', help='Label')
    parser.add_argument('--progress_path', help='Progress path')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite (placeholder)')
    parser.add_argument('--workspace_id', help='Optional workspace ID (Shared Drive)')
    parser.add_argument('--language', default='en', help='Transcription language code (default: en)')
    parser.add_argument('--controller-address', help='Link IP:Port override')

    args = parser.parse_args()
    mode = args.mode.lower()
    file_path = args.source
    folder_name = args.foldername
    target_path = args.target
    folder_id = args.folder_id
    workspace_id = args.workspace_id
    job_guid = args.jobguid

    # Load config
    config_map = loadConfigurationMap(args.config)
    if not config_map:
        print("Failed to load configuration")
        sys.exit(1)

    # Initialize provider
    trint = TrintProvider(config_map, args.config)

    progressDetails = {
        "duration": int(time.time()),
        "run_id": args.runid,
        "job_id": args.jobid,
        "progress_path": args.progress_path,
        "totalFiles": 0,
        "totalSize": 0,
        "processedFiles": 0,
        "processedBytes": 0,
        "status": "SCANNING"
    }

    # --- Mode Handling ---
    if mode == 'actions':
        print("list,browse,upload,createfolder,buckets,bucketsfolders")
        sys.exit(0)

    elif mode == 'buckets':
        # List top-level workspaces (Shared Drives)
        workspaces = trint.list_workspaces()
        buckets = [f"{w['_id']}:{w['name']}" for w in workspaces]
        print(','.join(buckets))
        sys.exit(0)

    elif mode == 'bucketsfolders':
        # List workspaces in XML
        workspaces = trint.list_workspaces()
        folders = [{"name": w["name"], "id": w["_id"]} for w in workspaces]
        xml_output = add_CDATA_tags_with_id_for_folder(folders)
        print(xml_output)
        sys.exit(0)

    elif mode == 'browse':
        # List folders in workspace or folder
        folders = trint.list_folders(workspace_id=workspace_id)
        if folder_id:
            folders = [f for f in folders if f.get("parentId") == folder_id]
        xml_output = add_CDATA_tags_with_id_for_folder(folders)
        print(xml_output)
        sys.exit(0)

    elif mode == 'list':
        if not target_path or not args.indexid:
            print("Target path (-t) and Index ID (-in) required for list mode.")
            sys.exit(1)
        files = trint.list_files(folder_id=folder_id, workspace_id=workspace_id)
        params = {
            "indexid": args.indexid,
            "project_name": args.projectname or "default",
            "label": args.label or ""
        }
        catalog_path = get_catalog_path(params)
        objects_dict = GetObjectDict(files, params, catalog_path)
        progressDetails.update({
            "totalFiles": objects_dict["scanned_count"],
            "totalSize": objects_dict["total_size"],
            "processedFiles": objects_dict["selected_count"],
            "processedBytes": 0,
            "status": "COMPLETED"
        })
        send_progress = lambda p, f: None  # Mock if not defined
        send_progress(progressDetails, progressDetails["processedFiles"])
        print(f"{progressDetails['totalFiles']}:{progressDetails['totalSize']}:{progressDetails['processedFiles']}:0")
        if generate_xml_from_file_objects(objects_dict, target_path):
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Generated XML: {target_path}")
        else:
            print("Failed to generate XML.")
            sys.exit(1)
        sys.exit(0)

    elif mode == 'upload':
        if not file_path or not folder_id:
            print("Source file (-s) and Folder ID (-id) required for upload.")
            sys.exit(1)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            sys.exit(1)

        # Generate backlink URL
        client_ip, client_port = args.controller_address.split(":") if args.controller_address and ":" in args.controller_address else get_link_address_and_port()
        backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path=/&filename={os.path.basename(file_path)}"
        result = trint.upload_file(
            file_path=file_path,
            folder_id=folder_id,
            workspace_id=workspace_id,
            language=args.language,
            backlink_url=backlink_url
        )
        if result and 'trintId' in result:
            print(f"File Upload successful: {result['trintId']}")
            sys.exit(0)
        else:
            print("Upload failed.")
            sys.exit(1)

    elif mode == 'createfolder':
        if not folder_name:
            print("Folder name (-f) required.")
            sys.exit(1)
        folder = trint.create_folder(folder_name, parent_id=folder_id, workspace_id=workspace_id)
        if folder:
            print(folder["_id"])
            sys.exit(0)
        else:
            print("Failed to create folder.")
            sys.exit(1)

    else:
        print(f"Unsupported mode: {mode}")
        sys.exit(1)

if __name__ == '__main__':
    main()