# provider_trint.py -working
import argparse
import sys
import os
import json
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from urllib.parse import urlencode
from action_functions import (
    loadConfigurationMap,
    get_catalog_path,
    check_if_catalog_file_exists,
    add_CDATA_tags_with_id_for_folder,
    generate_xml_from_file_objects,
    send_progress,
    debug_print
)

# --- Constants ---
DEBUG_FILEPATH = '/tmp/trint_debug.out'
DEBUG_PRINT = False
DEBUG_TO_FILE = False

DEFAULT_BASE_URL = 'https://api.trint.com'
DEFAULT_UPLOAD_URL = 'https://upload.trint.com'

# Global config map
config_map = None
api_key_id = None
api_key_secret = None
base_url = None
upload_url = None
auth = None
workspace_id = None  # Optional: from config

# --- Initialization ---
def init_config(args):
    global config_map, api_key_id, api_key_secret, base_url, upload_url, auth, workspace_id
    config_map = loadConfigurationMap(args.config)
    if not config_map:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Failed to load configuration.")
        print("Failed to load configuration.")
        sys.exit(1)

    api_key_id = config_map.get('api_key_id')
    api_key_secret = config_map.get('api_key_secret')
    base_url = config_map.get('base_url', DEFAULT_BASE_URL)
    upload_url = config_map.get('upload_url', DEFAULT_UPLOAD_URL)
    workspace_id = config_map.get('workspace_id')  # Optional

    if not api_key_id or not api_key_secret:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Missing API Key or Secret in config.")
        print("Missing API Key or Secret.")
        sys.exit(1)

    auth = HTTPBasicAuth(api_key_id, api_key_secret)

# --- Helper: Make API Request ---
def api_request(method, url, **kwargs):
    try:
        response = requests.request(method, url, auth=auth, **kwargs)
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                    f"API {method} {url} -> {response.status_code}")
        if response.status_code >= 400:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                        f"Error: {response.status_code} - {response.text}")
        return response
    except Exception as e:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Request failed: {e}")
        return None

# --- Mode: actions ---
def handle_actions():
    print("list,browse,upload,createfolder,buckets,bucketsfolders")
    sys.exit(0)

# --- Mode: auth ---
def handle_auth():
    print("Trint uses API Key/Secret. No interactive auth needed. Ensure keys are in cloud_targets.conf.")
    sys.exit(0)

# --- Mode: buckets (top-level folders as buckets) ---
def handle_buckets(args):
    url = f"{base_url}/folders/"
    params = {}
    if workspace_id:
        params["workspace-id"] = workspace_id

    response = api_request("GET", url, headers={"accept": "application/json"}, params=params)
    if not response or response.status_code != 200:
        print("")
        sys.exit(0)

    folders = response.json()
    root_folders = []

    if workspace_id:
        # Use parent field: root if no parent
        root_folders = [f for f in folders if f.get("parent") is None]
    else:
        # No workspace → name contains path; root = top-level only
        seen_roots = set()
        for f in folders:
            name = f.get("name", "")
            parts = name.split("/")
            if parts and parts[0] not in seen_roots:
                seen_roots.add(parts[0])
                # Fake a root folder with ID of first folder using that name
                root_folders.append({
                    "_id": f["_id"],
                    "name": parts[0]
                })

    buckets = [f"{f['_id']}:{f['name']}" for f in root_folders]
    print(','.join(buckets))
    sys.exit(0)

# --- Mode: bucketsfolders (XML list of top-level folders) ---
def handle_bucketsfolders(args):
    url = f"{base_url}/folders/"
    params = {}
    if workspace_id:
        params["workspace-id"] = workspace_id

    response = api_request("GET", url, headers={"accept": "application/json"}, params=params)
    if not response or response.status_code != 200:
        print('<?xml version="1.0" encoding="UTF-8"?><folders></folders>')
        sys.exit(0)

    folders = response.json()
    root_folders = []

    if workspace_id:
        root_folders = [{"name": f["name"], "id": f["_id"]} for f in folders if f.get("parent") is None]
    else:
        seen_roots = set()
        for f in folders:
            name = f.get("name", "")
            parts = name.split("/")
            if parts and parts[0] not in seen_roots:
                seen_roots.add(parts[0])
                root_folders.append({
                    "name": parts[0],
                    "id": f["_id"]
                })

    xml_output = add_CDATA_tags_with_id_for_folder(root_folders)
    print(xml_output)
    sys.exit(0)

# --- Mode: browse (list immediate children of a folder) ---
def handle_browse(args):
    parent_id = args.folder_id
    if not parent_id:
        print('Asset ID(-id <asset_id>) option required for browse.')
        sys.exit(1)

    params = {}
    if workspace_id:
        params["workspace-id"] = workspace_id

    url = f"{base_url}/folders/"
    response = api_request("GET", url, headers={"accept": "application/json"}, params=params)
    if not response or response.status_code != 200:
        print('<?xml version="1.0" encoding="UTF-8"?><folders></folders>')
        sys.exit(0)

    all_folders = response.json()
    children = []

    if workspace_id:
        # Use parent ID
        children = [f for f in all_folders if f.get("parent") == parent_id]
    else:
        # Find folder with this ID and get its name
        parent_folder = next((f for f in all_folders if f["_id"] == parent_id), None)
        if not parent_folder:
            print('<?xml version="1.0" encoding="UTF-8"?><folders></folders>')
            sys.exit(0)

        parent_path = parent_folder["name"]  # e.g., "Test-08-05"
        # Children: folders where name starts with parent_path/ and has one more segment
        for f in all_folders:
            name = f.get("name", "")
            if name.startswith(parent_path + "/"):
                relative = name[len(parent_path)+1:]
                if "/" not in relative:  # Direct child
                    children.append(f)

    children = [{"name": f["name"].split("/")[-1], "id": f["_id"]} for f in children]
    xml_output = add_CDATA_tags_with_id_for_folder(children)
    print(xml_output)
    sys.exit(0)

# --- Mode: list (list transcripts in folder or workspace) ---
def handle_list(args):
    if not args.target or not args.indexid:
        print("Target path (-t) and Index ID (-in) required for list mode.")
        sys.exit(1)

    folder_id = args.folder_id

    params = {}
    if folder_id:
        params["folderId"] = folder_id
    if workspace_id:
        params["sharedDriveId"] = workspace_id  # Trint supports both sharedDriveId and workspaceId

    url = f"{base_url}/transcripts/"
    response = api_request("GET", url, headers={"accept": "application/json"}, params=params)
    files_list = []
    if response and response.status_code == 200:
        files_list = response.json()

    params_map = {
        "indexid": args.indexid,
        "project_name": args.projectname or "default",
        "label": args.label or "",
        "source": args.source or ""
    }
    catalog_path = get_catalog_path(params_map)

    objects_dict = GetObjectDict(files_list, params_map, catalog_path)

    progressDetails = {
        "duration": int(time.time()),
        "run_id": args.runid,
        "job_id": args.jobid,
        "progress_path": args.progress_path,
        "totalFiles": objects_dict["scanned_count"],
        "totalSize": objects_dict["total_size"],
        "processedFiles": objects_dict["selected_count"],
        "processedBytes": 0,
        "status": "COMPLETED"
    }
    send_progress(progressDetails, progressDetails["processedFiles"])

    print(f"{objects_dict['scanned_count']}:{objects_dict['total_size']}:{objects_dict['selected_count']}:0")

    if generate_xml_from_file_objects(objects_dict, args.target):
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Generated XML: {args.target}")
    else:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Failed to generate XML.")
        print("Failed to generate XML.")
        sys.exit(1)
    sys.exit(0)

# --- Mode: createfolder ---
def handle_createfolder(args):
    if not args.foldername:
        print("Folder name (-f) required.")
        sys.exit(1)

    folder_name = args.foldername
    parent_id = args.folder_id

    url = f"{base_url}/folders/"
    payload = {"name": folder_name}
    if parent_id:
        payload["parentId"] = parent_id
    # Only include workspaceId if creating at root (i.e., no parent)
    if not parent_id and workspace_id:
        payload["workspaceId"] = workspace_id

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = api_request("POST", url, headers=headers, json=payload)
    if response and response.status_code in [200, 201]:
        folder_data = response.json()
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                    f"Folder '{folder_name}' created with ID: {folder_data['_id']}")
        print(folder_data["_id"])
        sys.exit(0)
    else:
        print("Failed to create folder.")
        sys.exit(1)

# --- Mode: upload ---
def handle_upload(args):
    if not args.source or not args.folder_id:
        print("Source file (-s) and Folder ID (-id) required for upload.")
        sys.exit(1)

    file_path = args.source
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)

    folder_id = args.folder_id
    language = args.language or "en"
    file_name = os.path.basename(file_path)

    # Detect MIME type
    try:
        import magic
        mime_type = magic.from_file(file_path, mime=True)
    except ImportError:
        mime_type = 'application/octet-stream'

    # Prepare upload
    data = {
        "filename": file_name,
        "language": language,
        "folder-id": folder_id
    }
    if workspace_id:
        data["workspace-id"] = workspace_id  # Scope upload to workspace

    with open(file_path, 'rb') as f:
        files = {'file': (file_name, f, mime_type)}
        response = api_request("POST", upload_url, data=data, files=files)

    if response and response.status_code == 200:
        result = response.json()
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE,
                    f"Upload successful: {result.get('trintId')}")
        print(f"File Upload successful: {result.get('trintId')}")
        sys.exit(0)
    else:
        error_msg = response.text if response else "No response"
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Upload failed: {error_msg}")
        print("Upload failed.")
        sys.exit(1)

# --- GetObjectDict ---
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
                except Exception as e:
                    debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Time parse error: {e}")

            if check_if_catalog_file_exists(catalog_path, file_name, mtime_epoch):
                continue

            file_object = {
                "name": file_name,
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

# --- get_folder_id: Resolve or create nested path ---
def get_folder_id(config_data, upload_path, base_id=None):
    """
    Resolves or creates the folder hierarchy based on upload_path.
    Starts from workspace root if no base_id.
    Uses parent-child relationships to avoid name collisions.
    """
    if not upload_path.strip("/"):
        return base_id  # Return base or None

    folder_parts = [part for part in upload_path.strip("/").split("/") if part]
    all_folders = list_all_folders(config_data)
    folder_by_id = {f["_id"]: f for f in all_folders}
    parent_map = {f["_id"]: f.get("parent") for f in all_folders}

    current_parent_id = base_id  # Can be None → means workspace root

    for folder_name in folder_parts:
        found_id = None

        # Search for folder with matching name and parent
        for f in all_folders:
            if f["name"] == folder_name:
                parent_id = f.get("parent")
                if current_parent_id is None:
                    if parent_id is None:  # Root folder
                        found_id = f["_id"]
                        break
                else:
                    if parent_id == current_parent_id:
                        found_id = f["_id"]
                        break

        if found_id is None:
            # Create folder
            payload = {"name": folder_name}
            if current_parent_id:
                payload["parentId"] = current_parent_id
            elif config_data.get("workspace_id"):
                payload["workspaceId"] = config_data["workspace_id"]

            url = f"{config_data['base_url']}/folders/"
            headers = {"accept": "application/json", "content-type": "application/json"}
            auth = HTTPBasicAuth(config_data['api_key_id'], config_data['api_key_secret'])
            response = requests.post(url, json=payload, headers=headers, auth=auth)

            if response.status_code not in [200, 201]:
                raise Exception(f"Create folder failed: {response.text}")
            new_folder = response.json()
            found_id = new_folder["_id"]
            all_folders.append(new_folder)
            folder_by_id[found_id] = new_folder
            if current_parent_id:
                parent_map[found_id] = current_parent_id

        current_parent_id = found_id

    return current_parent_id

# --- list_all_folders helper ---
def list_all_folders(config_data):
    """Fetch all folders, optionally scoped to workspace."""
    url = f"{config_data['base_url']}/folders/"
    params = {}
    if config_data.get("workspace_id"):
        params["workspace-id"] = config_data["workspace_id"]
    response = api_request("GET", url, headers={"accept": "application/json"}, params=params)
    if response and response.status_code == 200:
        return response.json().get("data", [])
    return []

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Trint Provider CLI")
    parser.add_argument('-c', '--config', required=True, help='Configuration name (e.g., trint-prod)')
    parser.add_argument('-m', '--mode', required=True, help='Mode: list, browse, upload, createfolder, actions, buckets, bucketsfolders')
    parser.add_argument('-s', '--source', help='Source file path (for upload)')
    parser.add_argument('-t', '--target', help='Target path (for list output XML)')
    parser.add_argument('-f', '--foldername', help='Folder name to create')
    parser.add_argument('-tid', '--tempid', help='File ID (unused)')
    parser.add_argument('-id', '--folder_id', help='Folder ID (for list, upload, browse)')
    parser.add_argument('-in', '--indexid', help='Index ID (for list)')
    parser.add_argument('-jg', '--jobguid', help='Job GUID')
    parser.add_argument('-r', '--runid', help='Run ID')
    parser.add_argument('-ji', '--jobid', help='Job ID')
    parser.add_argument('-p', '--projectname', help='Project name')
    parser.add_argument('-l', '--label', help='Label')
    parser.add_argument('--progress_path', help='Progress path')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite (placeholder)')
    parser.add_argument('--language', default='en', help='Transcription language code (default: en)')
    args = parser.parse_args()

    # Initialize config
    init_config(args)

    # --- Dispatch Mode ---
    mode = args.mode.lower()

    if mode == 'actions':
        handle_actions()
    elif mode == 'auth':
        handle_auth()
    elif mode == 'buckets':
        handle_buckets(args)
    elif mode == 'bucketsfolders':
        handle_bucketsfolders(args)
    elif mode == 'browse':
        handle_browse(args)
    elif mode == 'list':
        handle_list(args)
    elif mode == 'createfolder':
        handle_createfolder(args)
    elif mode == 'upload':
        handle_upload(args)
    else:
        print(f"Unsupported mode: {mode}")
        sys.exit(1)

if __name__ == '__main__':
    main()
