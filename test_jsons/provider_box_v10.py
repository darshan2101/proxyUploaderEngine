# provider_box_v10.py
import argparse
import sys
import os
import json
import plistlib
import time
from configparser import ConfigParser
from datetime import datetime
from box_sdk_gen import (
    BoxClient,
    BoxOAuth,
    BoxTokenStorage,
    CreateFolderRequestBodyParentField,
    UploadFileRequestBodyParentField,
)
from box_sdk_gen.managers.folders import GetFolderItemsOptions
from box_sdk_gen.managers.files import GetFileByIdOptions
from box_sdk_gen.auth import BoxOAuth as BoxOAuthAuth
from box_sdk_gen.token_storage import InMemoryTokenStorage
from box_sdk_gen.network import NetworkSession
from box_sdk_gen.utils import ByteStream

# Import your existing helpers (assumed unchanged)
from action_functions import *

DEBUG_FILEPATH = '/tmp/box_debug.out'
DEBUG_PRINT = False
DEBUG_TO_FILE = False

IS_LINUX = os.path.isdir("/opt/sdna/bin/")
IS_WINDOWS = os.name == 'nt'
DNA_CLIENT_SERVICES_LINUX = '/etc/StorageDNA/DNAClientServices.conf'
DNA_CLIENT_SERVICES_MAC = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
DNA_CLIENT_SERVICES_WINDOWS = r'D:\etc\StorageDNA\DNAClientServices.conf'

# --- Reuse your existing config logic (unchanged) ---
def get_dna_client_services_path():
    if IS_LINUX:
        return DNA_CLIENT_SERVICES_LINUX
    elif IS_WINDOWS:
        return DNA_CLIENT_SERVICES_WINDOWS
    else:
        return DNA_CLIENT_SERVICES_MAC

def get_cloud_config_path():
    try:
        client_config_path = get_dna_client_services_path()
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Using client config path: {client_config_path}")
        if IS_LINUX or IS_WINDOWS:
            config_parser = ConfigParser()
            config_parser.read(client_config_path)
            if config_parser.has_section('General') and config_parser.has_option('General', 'cloudconfigfolder'):
                section_info = config_parser['General']
                config_path = os.path.join(section_info['cloudconfigfolder'], "cloud_targets.conf")
            else:
                raise KeyError("cloudconfigfolder option not found in [General]")
        else:
            with open(client_config_path, 'rb') as fp:
                my_plist = plistlib.load(fp)
                config_path = os.path.join(my_plist["CloudConfigFolder"], "cloud_targets.conf")
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Using config path: {config_path}")
    except Exception as e:
        config_path = r'D:\Dev\(python +node) Wrappers\proxyUploaderEngine\CloudConfig\cloud_targets.conf'
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"CONFIG_PATH not found globally, using fallback: {config_path}\nException: {e}")
    return config_path

# --- Token storage bridge ---
class BoxV10TokenStore:
    def __init__(self, config_path, config_section):
        self.config_path = config_path
        self.config_section = config_section

    def read(self):
        if not os.path.exists(self.config_path):
            return {}
        config = ConfigParser()
        config.read(self.config_path)
        if self.config_section not in config:
            return {}
        section = config[self.config_section]
        return {
            'access_token': section.get('access_token', ''),
            'refresh_token': section.get('refresh_token', ''),
        }

    def write(self, access_token, refresh_token):
        config = ConfigParser()
        config.read(self.config_path)
        if self.config_section not in config:
            config[self.config_section] = {}
        config[self.config_section]['access_token'] = access_token or ''
        config[self.config_section]['refresh_token'] = refresh_token or ''
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            config.write(f)
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Tokens updated in {self.config_path} under [{self.config_section}]")

# --- V10 Provider ---
class BoxProviderV10:
    def __init__(self, config_map, config_name):
        self.client_id = config_map['client_id']
        self.client_secret = config_map['client_secret']
        self.config_name = config_name
        config_path = get_cloud_config_path()
        self.token_store = BoxV10TokenStore(config_path, config_name)
        self.client = None
        self._init_client()

    def _init_client(self):
        tokens = self.token_store.read()
        access_token = tokens.get('access_token')
        refresh_token = tokens.get('refresh_token')

        if not access_token or not refresh_token:
            return

        def token_storage_callback(access_token, refresh_token):
            self.token_store.write(access_token, refresh_token)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Tokens automatically refreshed and stored")

        auth = BoxOAuthAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            token_storage=BoxTokenStorage(token_storage_callback),
        )
        self.client = BoxClient(auth)

    def authenticate_with_auth_code(self, auth_code):
        try:
            redirect_uri = "http://localhost:3000/callback"
            auth = BoxOAuthAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            tokens = auth.get_tokens_from_code(auth_code, redirect_uri)
            self.token_store.write(tokens.access_token, tokens.refresh_token)
            self._init_client()
            return True
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Auth failed: {e}")
            return False

    def get_folder_contents(self, folder_id='0'):
        if not self.client:
            return []
        try:
            items = self.client.folders.get_folder_items(folder_id, options=GetFolderItemsOptions(limit=1000)).entries
            return items
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error listing folder: {e}")
            return []

    def create_folder(self, folder_name, parent_folder_id='0'):
        if not self.client:
            return None
        try:
            parent = CreateFolderRequestBodyParentField(id=parent_folder_id)
            folder = self.client.folders.create_folder(folder_name, parent)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Folder '{folder_name}' created: {folder.id}")
            return folder
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Create folder error: {e}")
            return None

    def download_file(self, file_id, target_path):
        if not self.client:
            return False
        try:
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            stream = self.client.files.download_file(file_id)
            with open(target_path, 'wb') as f:
                f.write(stream.content)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Downloaded: {target_path}")
            return True
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Download error: {e}")
            return False

    def upload_file(self, file_path, folder_id):
        if not self.client:
            return None
        try:
            parent = UploadFileRequestBodyParentField(id=folder_id)
            file_name = os.path.basename(file_path)
            with open(file_path, 'rb') as f:
                uploaded = self.client.files.upload_file(
                    parent=parent,
                    file_name=file_name,
                    file_stream=ByteStream(f.read())
                )
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Uploaded: {file_path} -> {uploaded.id}")
            return uploaded
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Upload error: {e}")
            return None

# --- Auth helper (v10 style) ---
def authenticate_first_time_v10(config_map, config_name):
    print("First-time setup required!")
    client_id = config_map['client_id']
    redirect_uri = "http://localhost:3000/callback"
    auth_url = f"https://account.box.com/api/oauth2/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
    print(f"\nVisit this URL:\n{auth_url}\n")
    auth_code = input("Enter authorization code: ").strip()

    config_path = get_cloud_config_path()
    provider = BoxProviderV10(config_map, config_name)
    if provider.authenticate_with_auth_code(auth_code):
        print("Authentication successful! Tokens saved.")
        return True
    else:
        print("Authentication failed!")
        return False

# --- Reuse your existing GetObjectDict logic (unchanged) ---
# (Assumes `replace_file_path`, `check_if_catalog_file_exists`, etc. are in action_functions)

def GetObjectDictV10(items, params, catalog_path):
    scanned_files = 0
    selected_count = 0
    output = {}
    file_object_list = []
    total_size = 0

    for item in items:
        if item.type != 'file':
            continue
        try:
            file_path = replace_file_path(item.name)
            file_size = int(item.size) if item.size else 0
            file_id = item.id

            mtime_epoch = int(datetime.fromisoformat(item.modified_at.replace('Z', '+00:00')).timestamp()) if item.modified_at else 0
            atime_epoch = int(datetime.fromisoformat(item.created_at.replace('Z', '+00:00')).timestamp()) if item.created_at else 0

            if check_if_catalog_file_exists(catalog_path, file_path, mtime_epoch):
                continue

            file_object = {
                "name": file_path,
                "size": str(file_size),
                "mode": "0",
                "tmpid": file_id,
                "type": "F_REG",
                "mtime": str(mtime_epoch),
                "atime": str(atime_epoch),
                "owner": "0",
                "group": "0",
                "index": params.get("indexid", "0")
            }

            scanned_files += 1
            selected_count += 1
            total_size += file_size
            file_object_list.append(file_object)
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error processing {item.name}: {e}")
            continue

    return {
        "filelist": file_object_list,
        "scanned_count": scanned_files,
        "selected_count": selected_count,
        "total_size": total_size,
    }

# --- Main CLI (mirrors original interface) ---
def main():
    parser = argparse.ArgumentParser(description="Box Provider CLI (v10)")
    parser.add_argument('-c', '--config', required=True)
    parser.add_argument('-m', '--mode', required=True)
    parser.add_argument('-s', '--source')
    parser.add_argument('-t', '--target')
    parser.add_argument('-f', '--foldername')
    parser.add_argument('-tid', '--tempid')
    parser.add_argument('-id', '--folder_id')
    parser.add_argument('-in', '--indexid')
    parser.add_argument('-jg', '--jobguid')
    parser.add_argument('-r', '--runid')
    parser.add_argument('-ji', '--jobid')
    parser.add_argument('-p', '--projectname')
    parser.add_argument('-l', '--label')
    parser.add_argument('--progress_path')
    parser.add_argument('-o', '--overwrite', action='store_true')

    args = parser.parse_args()
    config_map = loadConfigurationMap(args.config)
    if not config_map:
        print("Failed to load configuration")
        sys.exit(1)

    if args.mode == 'auth':
        if authenticate_first_time_v10(config_map, args.config):
            print("Authentication successful!")
        else:
            print("Authentication failed!")
        sys.exit(0)

    provider = BoxProviderV10(config_map, args.config)
    if not provider.client:
        print("Not authenticated. Run with --mode auth first.")
        sys.exit(1)

    # Modes: list, browse, download, upload, createfolder, actions, buckets, bucketsfolders
    if args.mode == 'actions':
        print("list,browse,download,upload,createfolder,buckets,bucketsfolders")
        sys.exit(0)

    elif args.mode == 'list':
        folder_id = args.folder_id or '0'
        if not args.target or not args.indexid:
            print('Target path (-t) and Index ID (-in) required for list')
            sys.exit(1)
        items = provider.get_folder_contents(folder_id)
        params_map = {"folder_id": folder_id, "indexid": args.indexid}
        catalog_path = get_catalog_path(params_map)
        obj_dict = GetObjectDictV10(items, params_map, catalog_path)
        # ... (reuse your progress + XML logic from original)
        if generate_xml_from_file_objects(obj_dict, args.target):
            print(f'{obj_dict["scanned_count"]}:{obj_dict["total_size"]}:{obj_dict["selected_count"]}:0')
        else:
            sys.exit(1)

    elif args.mode == 'browse':
        folder_id = args.folder_id or '0'
        items = provider.get_folder_contents(folder_id)
        folders = [{"name": i.name, "id": i.id} for i in items if i.type == 'folder']
        print(add_CDATA_tags_with_id_for_folder(folders))
        sys.exit(0)

    elif args.mode in ('buckets', 'bucketsfolders'):
        items = provider.get_folder_contents('0')
        folders = [{"name": i.name, "id": i.id} for i in items if i.type == 'folder']
        if args.mode == 'buckets':
            buckets = [f"{x['id']}:{replace_file_path(x['name'])}" for x in folders]
            print(','.join(buckets))
        else:
            print(add_CDATA_tags_with_id_for_folder(folders))
        sys.exit(0)

    elif args.mode == 'download':
        if not args.tempid or not args.target:
            print('File ID (-tid) and Target (-t) required')
            sys.exit(1)
        if provider.download_file(args.tempid, args.target):
            print(f"File download at {args.target}")
        else:
            sys.exit(1)

    elif args.mode == 'upload':
        if not args.source or not args.folder_id:
            print('Source (-s) and Folder ID (-id) required')
            sys.exit(1)
        if provider.upload_file(args.source, args.folder_id):
            print(f"File Upload successful: {args.source}")
        else:
            sys.exit(1)

    elif args.mode == 'createfolder':
        if not args.foldername or not args.folder_id:
            print('Folder name (-f) and Folder ID (-id) required')
            sys.exit(1)
        folder = provider.create_folder(args.foldername, args.folder_id)
        if folder:
            print(folder.id)
        else:
            sys.exit(1)

    else:
        print(f'Unsupported mode {args.mode}')
        sys.exit(1)

if __name__ == '__main__':
    main()