import os
import sys
import argparse
import logging
import urllib.parse
from json import dumps
from requests import request
from configparser import ConfigParser
from frameioclient import FrameioClient
import plistlib

# Constants
VALID_MODES = ["upload", "uploadasset"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

# Environment detection
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH


def extract_file_name(path):
    return os.path.basename(path)


def remove_file_name_from_path(path):
    return os.path.dirname(path)


def get_cloud_config_path():
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        return parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            return plistlib.load(fp)["CloudConfigFolder"] + "/cloud_targets.conf"


def get_link_address_and_port():
    ip, port = "", ""

    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()

        if IS_LINUX:
            for line in lines:
                if '=' in line:
                    key, value = map(str.strip, line.split('=', 1))
                    if key.endswith('link_address'):
                        ip = value
                    elif key.endswith('link_port'):
                        port = value
        else:
            for i, line in enumerate(lines):
                if "<key>link_address</key>" in line:
                    ip = lines[i + 1].split(">")[1].split("<")[0].strip()
                elif "<key>link_port</key>" in line:
                    port = lines[i + 1].split(">")[1].split("<")[0].strip()

    except Exception as e:
        print(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)

    return ip, port


def find_upload_id(path, project_id, token):
    client = FrameioClient(token)
    current_id = client.projects.get(project_id)['root_asset_id']
    
    logging.debug(path.strip('/').split('/'))

    for segment in path.strip('/').split('/'):
        children = client.assets.get_children(current_id, type='folder')
        next_id = next((child['id'] for child in children if child['name'] == segment), None)
        if not next_id:
            next_id = client.assets.create(parent_asset_id=current_id, name=segment, type="folder", filesize=0)['id']
        current_id = next_id
    return current_id


def upload_file(client, source_path, up_id, description):
    asset = client.assets.create(
        parent_asset_id=up_id,
        name=extract_file_name(source_path),
        type="file",
        description=description,
        filesize=os.stat(source_path).st_size
    )
    with open(source_path, 'rb') as f:
        client.assets._upload(asset, f)
    return asset['id']


def update_asset(token, asset_id, properties_file):
    
    with open(properties_file) as f:
        props = {}
        for line in f:
            parts = line.strip().split(',')
            if len(parts) == 2:
                key, value = parts[0].strip(), parts[1].strip('\n').strip()
                props[key] = value
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    response = request('PUT', f'https://api.frame.io/v2/assets/{asset_id}', headers=headers, data=dumps({'properties': props}))
    
    return response

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--projectid')
    parser.add_argument('-c', '--config', required=True)
    parser.add_argument('-m', '--mode', required=True)
    parser.add_argument('-a', '--assetid')
    parser.add_argument('-s', '--sourcename')
    parser.add_argument('-u', '--uploadpath')
    parser.add_argument('-d', '--downloadpath')
    parser.add_argument('-cp', '--catalogpath')
    parser.add_argument('-an', '--archivename')
    parser.add_argument('-f', '--fileproperties')
    parser.add_argument('-j', '--jobid')
    parser.add_argument('-l', '--linkipaddrport')
    parser.add_argument('-pu', '--proxyupload') #-- Not necessary cause only proxies will get uploaded here

    
    args = parser.parse_args()

    if args.mode not in VALID_MODES:
        print(f"Error: Only allowed modes are {VALID_MODES}")
        sys.exit(1)

    config_path = get_cloud_config_path()
    if not os.path.exists(config_path):
        print(f"Unable to find cloud target file: {config_path}")
        sys.exit(1)

    config = ConfigParser()
    config.read(config_path)
    if args.config not in config:
        print(f"Unable to find cloud configuration: {args.config}")
        sys.exit(1)

    config_data = config[args.config]
    token = config_data['FrameIODevKey']
    project_id = args.projectid or config_data['project_id']

    if args.projectid and args.projectid != config_data['project_id']:
        print(f"Mismatch in project ID. Expected: {config_data['project_id']}, got: {args.projectid}")
        sys.exit(1)

    client = FrameioClient(token)

    if args.mode == 'upload_proxy':
        client_ip, client_port = (
            args.linkipaddrport.split(":") if args.linkipaddrport else get_link_address_and_port()
        )
        catalog_path = remove_file_name_from_path(args.catalogpath)
        
        catalog_url = urllib.parse.quote(catalog_path)
        # filename = extract_file_name(args.catalogpath) 
        filename = extract_file_name(args.catalogpath) if args.proxyupload == "true" else extract_file_name(args.sourcename) 

        filename_enc = urllib.parse.quote(filename)
        
        url = f"https://{client_ip}:{client_port}/dashboard/projects/{args.jobid}/browse&search?path={catalog_url}&filename={filename_enc}"
        
        up_id = find_upload_id(args.uploadpath, project_id, token) if '/' in args.uploadpath else args.uploadpath
        asset_id = upload_file(client, args.sourcename, up_id, url)
        
        print(asset_id)

    elif args.mode == 'uploadasset':
        if not args.assetid or not args.fileproperties:
            print("Both --assetid and --fileproperties are required for updateasset mode")
            sys.exit(1)
        update_asset(token, args.assetid, args.fileproperties)
        
    else:
        print(f'Error only modes allowed so far are {VALID_MODES}')

    sys.exit(0)
