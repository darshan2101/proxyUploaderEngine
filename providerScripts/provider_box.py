# provider_box.py
# Updated for Box SDK v10.0.0+
import argparse
import sys
import os
import json
import plistlib
import time
from configparser import ConfigParser
from datetime import datetime
from boxsdk import OAuth2, Client
from boxsdk.object.folder import Folder
from boxsdk.object.file import File
from action_functions import *

DEBUG_FILEPATH = '/tmp/box_debug.out'
DEBUG_PRINT = False
DEBUG_TO_FILE = False

IS_LINUX = os.path.isdir("/opt/sdna/bin/")
IS_WINDOWS = os.name == 'nt'
DNA_CLIENT_SERVICES_LINUX = '/etc/StorageDNA/DNAClientServices.conf'
DNA_CLIENT_SERVICES_MAC = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
DNA_CLIENT_SERVICES_WINDOWS = r'D:\etc\StorageDNA\DNAClientServices.conf'

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

class ConfigTokenStore:
    def __init__(self, config_path, config_section):
        self.config_path = config_path
        self.config_section = config_section

    def read(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file {self.config_path} not found")

        config = ConfigParser()
        config.read(self.config_path)

        if self.config_section not in config:
            raise ValueError(f"Section '{self.config_section}' not found in config")

        token_info = {}
        section = config[self.config_section]

        if 'access_token' in section:
            token_info['access_token'] = section['access_token']
        if 'refresh_token' in section:
            token_info['refresh_token'] = section['refresh_token']

        return token_info

    def write(self, token_info):
        config = ConfigParser()
        config.read(self.config_path)

        if self.config_section not in config:
            config[self.config_section] = {}

        section = config[self.config_section]
        section['access_token'] = token_info.get('access_token', '')
        section['refresh_token'] = token_info.get('refresh_token', '')

        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as configfile:
            config.write(configfile)

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Tokens updated in {self.config_path} under [{self.config_section}]")

    def clear(self):
        if not os.path.exists(self.config_path):
             return

        config = ConfigParser()
        config.read(self.config_path)

        if self.config_section in config:
            section = config[self.config_section]
            changed = False
            if 'access_token' in section:
                del section['access_token']
                changed = True
            if 'refresh_token' in section:
                del section['refresh_token']
                changed = True

            if changed:
                with open(self.config_path, 'w') as configfile:
                    config.write(configfile)

class BoxTokenManager:
    def __init__(self, client_id, client_secret, config_path, config_section):
        self.client_id = client_id
        self.client_secret = client_secret
        self.config_path = config_path
        self.config_section = config_section
        self.token_store = ConfigTokenStore(config_path, config_section)
        self.oauth = None
        self.client = None

    def authenticate_with_auth_code(self, auth_code):
        try:
            temp_oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            access_token, refresh_token = temp_oauth.authenticate(auth_code)

            token_info = {
                'access_token': access_token,
                'refresh_token': refresh_token
            }
            self.token_store.write(token_info)

            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret,
                access_token=access_token,
                refresh_token=refresh_token,
                store_tokens=self._store_tokens_callback
            )

            self.client = Client(self.oauth)
            return True
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Authentication failed: {e}")
            return False

    def get_authenticated_client(self):
        if self.client:
            return self.client

        try:
            token_info = self.token_store.read()
            if 'access_token' not in token_info or 'refresh_token' not in token_info:
                raise ValueError("Tokens not found in configuration")

            self.oauth = OAuth2(
                client_id=self.client_id,
                client_secret=self.client_secret,
                access_token=token_info['access_token'],
                refresh_token=token_info['refresh_token'],
                store_tokens=self._store_tokens_callback
            )
            self.client = Client(self.oauth)
            return self.client

        except (FileNotFoundError, ValueError) as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"No stored tokens found: {e}")
            return None
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Failed to create authenticated client: {e}")
            return None

    def _store_tokens_callback(self, access_token, refresh_token):
        token_info = {
            'access_token': access_token,
            'refresh_token': refresh_token
        }
        self.token_store.write(token_info)
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Tokens automatically refreshed and stored in config")

    def refresh_tokens(self):
        try:
            if not self.oauth:
                token_info = self.token_store.read()
                self.oauth = OAuth2(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    access_token=token_info['access_token'],
                    refresh_token=token_info['refresh_token'],
                    store_tokens=self._store_tokens_callback
                )
            self.oauth.refresh()
            self.client = Client(self.oauth)
            return True
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Manual token refresh failed: {e}")
            return False

    def get_tokens_info(self):
        try:
            return self.token_store.read()
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Failed to read tokens: {e}")
            return None

class BoxProvider:
    def __init__(self, config_map, config_name):
        self.client_id = config_map['client_id']
        self.client_secret = config_map['client_secret']
        self.config_name = config_name
        config_path = get_cloud_config_path()

        self.token_manager = BoxTokenManager(
            client_id=self.client_id,
            client_secret=self.client_secret,
            config_path=config_path,
            config_section=self.config_name
        )

        self.client = self.token_manager.get_authenticated_client()
        self.current_user = None

    def get_folder_contents(self, folder_id='0'):
        if not self.client:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Not authenticated with Box")
            return []

        try:
            folder = self.client.folders.get_by_id(folder_id)
            items = folder.get_items()
            return list(items)
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error getting folder contents: {e}")
            return []

    def get_file_details(self, file_id):
        if not self.client:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Not authenticated with Box")
            return None
        try:
            file_item = self.client.files.get_by_id(file_id).get()
            return file_item
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error getting file details: {e}")
            return None

    def create_folder(self, folder_name, parent_folder_id='0'):
        if not self.client:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Not authenticated with Box")
            return None
        try:
            parent_folder = self.client.folders.get_by_id(parent_folder_id)
            folder = parent_folder.create_subfolder(folder_name)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Folder '{folder_name}' created successfully with ID: {folder.id}")
            return folder
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error creating folder '{folder_name}': {e}")
            return None

    def download_file(self, file_id, target_path):
        if not self.client:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Not authenticated with Box")
            return False
        try:
            file_item = self.client.files.get_by_id(file_id)
            download_parent_path = os.path.dirname(target_path)
            os.makedirs(download_parent_path, exist_ok=True)

            with open(target_path, 'wb') as file:
                file_item.download_to(file)

            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"File downloaded successfully: {target_path}")
            return True
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error downloading file: {e}")
            return False

    def upload_file(self, file_path, folder_id):
        if not self.client:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Not authenticated with Box")
            return None
        try:
            folder = self.client.folders.get_by_id(folder_id)
            uploaded_file = folder.upload(file_path)
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"File uploaded successfully: {file_path}")
            return uploaded_file
        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error uploading file: {e}")
            return None

def authenticate_first_time(config_map, config_name):
    try:
        config_path = get_cloud_config_path()
    except KeyError:
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "CONFIG_PATH not found")

    print("First-time setup required!")
    print("Please visit the Box authorization URL to get the authorization code.")

    client_id = config_map['client_id']
    redirect_uri = "http://localhost:3000/callback"

    auth_url = f"https://account.box.com/api/oauth2/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
    print(f"\nVisit this URL in your browser:")
    print(auth_url)
    print(f"\nAfter authorization, you'll get a code. Paste it here:")

    auth_code = input("Enter authorization code: ").strip()

    token_manager = BoxTokenManager(
        client_id=config_map['client_id'],
        client_secret=config_map['client_secret'],
        config_path=config_path,
        config_section=config_name
    )

    if token_manager.authenticate_with_auth_code(auth_code):
        print("Authentication successful! Tokens saved to config file.")
        return True
    else:
        print("Authentication failed!")
        return False


def GetObjectDict(files_list, params, catalog_path):
    scanned_files = 0
    selected_count = 0
    output = {}
    file_object_list = []
    total_size = 0

    for file_data in files_list:
        try:
            if file_data.type != 'file':
                continue

            file_path = replace_file_path(file_data.name)
            file_size = int(file_data.size) if file_data.size else 0
            file_id = file_data.id

            mtime_epoch_seconds = 0
            atime_epoch_seconds = 0

            if file_data.modified_at:
                try:
                    mtime_dt = datetime.fromisoformat(file_data.modified_at.replace('Z', '+00:00'))
                    mtime_epoch_seconds = int(mtime_dt.timestamp())
                except ValueError:
                    pass

            if file_data.created_at:
                try:
                    atime_dt = datetime.fromisoformat(file_data.created_at.replace('Z', '+00:00'))
                    atime_epoch_seconds = int(atime_dt.timestamp())
                except ValueError:
                    pass

            if check_if_catalog_file_exists(catalog_path, file_path, mtime_epoch_seconds):
                continue

            file_object = {
                "name": file_path,
                "size": str(file_size),
                "mode": "0",
                "tmpid": file_id,
                "type": "F_REG",
                "mtime": str(mtime_epoch_seconds),
                "atime": str(atime_epoch_seconds),
                "owner": "0",
                "group": "0",
                "index": params.get("indexid", "0")
            }

            scanned_files += 1
            selected_count += 1
            total_size += file_size
            file_object_list.append(file_object)

        except Exception as e:
            debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Error processing file {getattr(file_data, 'name', 'Unknown')}: {e}")
            continue

    output["filelist"] = file_object_list
    output["scanned_count"] = scanned_files
    output["selected_count"] = selected_count
    output["total_size"] = total_size

    return output

def main():
    parser = argparse.ArgumentParser(description="Box Provider CLI")
    parser.add_argument('-c', '--config', required=True, help='Configuration name (e.g., sdna-box)')
    parser.add_argument('-m', '--mode', required=True, help='Mode: list, browse, download, upload, createfolder, actions, auth, buckets, bucketsfolders')
    parser.add_argument('-s', '--source', help='Source file path (for upload)')
    parser.add_argument('-t', '--target', help='Target path (for list output XML or download destination)')
    parser.add_argument('-f', '--foldername', help='Folder name to create (for createfolder)')
    parser.add_argument('-tid', '--tempid', help='File ID (for download)')
    parser.add_argument('-id', '--folder_id', help='Folder ID (for list, upload, createfolder, browse)')
    parser.add_argument('-in', '--indexid', help='Index ID (for list)')
    parser.add_argument('-jg', '--jobguid', help='Job GUID')
    parser.add_argument('-r', '--runid', help='Run ID')
    parser.add_argument('-ji', '--jobid', help='Job ID')
    parser.add_argument('-p', '--projectname', help='Project name')
    parser.add_argument('-l', '--label', help='Label')
    parser.add_argument('--progress_path', help='Progress path')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite (placeholder)')

    args = parser.parse_args()
    mode = args.mode
    file_path = args.source # Alias for clarity
    folder_path = args.foldername # Alias for clarity
    target_path = args.target
    folder_id = args.folder_id
    job_guid = args.jobguid

    # Load configuration and logging using existing functions from action_functions
    # loadLoggingDict might require script name and job_guid
    # logging_dict = loadLoggingDict(os.path.basename(__file__), job_guid)
    config_map = loadConfigurationMap(args.config) # This should use CONFIG_PATH

    if not config_map:
        print("Failed to load configuration")
        sys.exit(1)

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

    if mode == 'auth':
        if authenticate_first_time(config_map, args.config):
            print("Authentication successful!")
        else:
            print("Authentication failed!")
        sys.exit(0)

    debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Initializing Box client...")
    box_provider = BoxProvider(config_map, args.config)

    if not box_provider.client:
        print("Not authenticated with Box. Please run with --mode auth first.")
        print("Example: python provider_box.py -m auth -c sdna-box")
        sys.exit(1)

    # --- Handle different operational modes ---

    if mode == 'actions':
        try:
            actions = "list,browse,download,upload,createfolder,buckets,bucketsfolders"
            print(actions)
            sys.exit(0)
        except Exception as e:
            print(f"An unexpected error occurred while getting actions: {e}")
            sys.exit(1)

    elif mode == 'list':
        if folder_id is None:
            folder_id = '0'

        if target_path is None or args.indexid is None:
            print('Target path (-t <targetpath>) and Index ID (-in <indexid>) options are required for list')
            sys.exit(1)

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Listing folder contents for ID: {folder_id}...")
        items = box_provider.get_folder_contents(folder_id)

        files_list = [item for item in items if item.type == 'file']

        params_map = {
            "folder_id": folder_id,
            "indexid": args.indexid,
            "project_name": args.projectname or "default",
            "source": args.source or "",
            "label": args.label or ""
        }

        catalog_path = get_catalog_path(params_map)
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Using catalog path: {catalog_path}")

        objects_dict = GetObjectDict(files_list, params_map, catalog_path)

        progressDetails.update({
            "totalFiles": objects_dict.get("scanned_count", 0),
            "totalSize": objects_dict.get("total_size", 0),
            "processedFiles": objects_dict.get("selected_count", 0),
            "processedBytes": 0,
            "status": "COMPLETED"
        })

        send_progress(progressDetails, progressDetails["processedFiles"])

        print(f'{progressDetails["totalFiles"]}:{progressDetails["totalSize"]}:{progressDetails["processedFiles"]}:{progressDetails["processedBytes"]}')

        if target_path:
            if generate_xml_from_file_objects(objects_dict, target_path):
                debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Generated XML file: {target_path}")
                pass
            else:
                print("Failed to generate XML file.")
                sys.exit(1)
        else:
            print("Target path is required for list mode output.")
            sys.exit(1)

    elif mode == 'browse':
        folder_id = folder_id or '0'

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Browsing folders in ID: {folder_id}...")
        items = box_provider.get_folder_contents(folder_id)

        folders = [{"name": item.name, "id": item.id} for item in items if item.type == 'folder']

        xml_output = add_CDATA_tags_with_id_for_folder(folders)
        print(xml_output)
        sys.exit(0)

    elif mode == 'buckets':
        folder_id = '0'
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Listing buckets (top-level folders)...")

        items = box_provider.get_folder_contents(folder_id)
        folders = [{"name": item.name, "id": item.id} for item in items if item.type == 'folder']
        buckets = [f"{x['id']}:{replace_file_path(x['name'])}" for x in folders]

        print(','.join(buckets))
        sys.exit(0)

    elif mode == 'bucketsfolders':
        folder_id = '0'
        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, "Listing buckets (top-level folders) in XML format...")
        items = box_provider.get_folder_contents(folder_id)

        folders = [{"name": item.name, "id": item.id} for item in items if item.type == 'folder']

        xml_output = add_CDATA_tags_with_id_for_folder(folders)
        print(xml_output)
        sys.exit(0)

    elif mode == 'download':
        if not (target_path and args.tempid):
            print('Target path (-t) and File ID (-tid) required for download')
            sys.exit(1)

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Downloading file ID: {args.tempid} to {target_path}...")
        if box_provider.download_file(args.tempid, target_path):
            print(f"File download at {target_path}")
            sys.exit(0)
        else:
            print("Error while downloading file")
            sys.exit(1)

    elif mode == 'upload':
        if not (file_path and folder_id):
            print('File path (-s) and Folder ID (-id) required for upload')
            sys.exit(1)

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Uploading file: {file_path} to folder ID: {folder_id}...")
        
        uploaded_file = box_provider.upload_file(file_path, folder_id)
        if uploaded_file:
            print(f"File Upload successful: {file_path}")
            sys.exit(0)
        else:
            print(f"Failed to Upload File: {file_path}")
            sys.exit(1)

    elif mode == 'createfolder':
        if not (folder_path and folder_id):
            print('Folder name (-f) and Folder ID (-id) required for createfolder')
            sys.exit(1)

        debug_print(DEBUG_FILEPATH, DEBUG_PRINT, DEBUG_TO_FILE, f"Creating folder '{folder_path}' in folder ID: {folder_id}...")
        folder = box_provider.create_folder(folder_path, folder_id)
        if folder:
            print(folder.id)
            sys.exit(0)
        else:
            print("Failed to create folder")
            sys.exit(1)

    else:
        # Unsupported mode
        print(f'Unsupported mode {mode}')
        sys.exit(1)

if __name__ == '__main__':
    main()
