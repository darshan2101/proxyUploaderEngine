from requests import request
from json import dumps
import argparse
import logging
from frameioclient import FrameioClient
import os
import time
import requests
import json
import subprocess
import urllib
import shutil
import urllib.parse
import urllib.request
from configparser import ConfigParser
import plistlib

VALID_MODES = ['folderlisting', 'listing', 'rootid', 'upload', 'foldercreate', 'updateasset', 'download']

useFrameIODownload = True

linux_dir = "/opt/sdna/bin"
is_linux = 0
if os.path.isdir(linux_dir):
    is_linux = 1
DNA_CLIENT_SERVICES = ''
if is_linux == 1:    
    DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    SERVERS_CONF_FILE = "/etc/StorageDNA/Servers.conf"
else:
    DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
    SERVERS_CONF_FILE = "/Library/Preferences/com.storagedna.Servers.plist"

def remove_file_name_from_path(file_path: str):
    parts = file_path.split("/")
    parts.pop()
    file_path = "/".join(parts)
    return file_path


'''
This is meant to extract a file name from a given full path
'''
def extract_file_name(full_path : str):

    #Meant to handle the case in which a / is not even in the string.
    if '/' not in full_path:
        return full_path
    
    #Meant for UNIX based paths
    try:
        return full_path[len(full_path) - full_path[::-1].index('/') : ]
    except ValueError:
        pass

    #Meant for Windows based paths
    try:
        return full_path[len(full_path) - full_path[::-1].index('\\') : ]
    except ValueError:
        pass
    
    return 'Error Extracting Name'


'''
Adds CDATA tags to a given string. 
'''
def add_CDATA_tags(text : str):
    return f'<![CDATA[{text}]]>'


def download_file(url, local_filename):
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1048576): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return True


def get_root_id(project_id : str, headers : dict) -> str:
    
    url = f'https://api.frame.io/v2/projects/{project_id}'
    response = request('GET', url, headers=headers).json()
    logging.debug(dumps(response, indent = 4))

    return response['root_asset_id']


def get_children(project_id : str, headers : dict, asset_id : str, folder : bool):
    
    root_asset_url = f'https://api.frame.io/v2/assets/{asset_id}/children?type=folder' if folder else f'https://api.frame.io/v2/assets/{asset_id}/children'
    children = request('GET', root_asset_url, headers = headers).json()
    return children


def find_upload_id(upload_path : str, project_id : str, bearer_token : str):
    client = FrameioClient(bearer_token)
    project = client.projects.get(project_id)
    current_id = project['root_asset_id']

    path_tokens = upload_path.strip('/').split('/')

    logging.debug(path_tokens)

    if path_tokens[0] == '':
        return current_id
    
    for path in path_tokens:
        children = client.assets.get_children(current_id, type = 'folder')
        prev_id = current_id
        
        #Loop through all of the children and if the child has the path we are looking for then we can 
        #move on. 
        for child in children:
            logging.debug(dumps(child, indent = 4))
            if child['name'] == path:
                current_id = child['id']
                break
        
        #If we did not find a child then we need to create a new one. 
        if prev_id == current_id:
            asset = client.assets.create(
                parent_asset_id = current_id,
                name = path,
                type = "folder",
                filesize = 0
            )
            current_id = asset['id']
    
    return current_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--projectid', help = 'project id that you would like to browse')
    parser.add_argument('-c', '--config', required = True, help = 'This is the name of the config that we want to look at.')
    parser.add_argument('-m', '--mode', required = True, 
    help = 'folderlisting, listing, rootid, upload, foldercreate, updateasset, download, deleteasset')
  
    parser.add_argument('-a', '--assetid', help = 'REQUIRED if folderlisting or listing or updateasset')
    
    parser.add_argument('-s', '--sourcename', help = 'REQUIRED if upload or download')
    parser.add_argument('-u', '--uploadpath', help = 'REQUIRED if upload or foldercreate')
    parser.add_argument('-d', '--downloadpath', help = 'REQUIRED if download')
    parser.add_argument('-cp','--catalogpath', help = 'Relative catalog file path with file name. REQUIRED if upload.')
    parser.add_argument('-an','--archivename', help = 'Current SDNA project name. REQUIRED if upload.')
    parser.add_argument('-pu','--proxyupload', help = 'Set to true if we are writing proxies to frame.io.')

    parser.add_argument('-media_res', '--mediaresolution', help = 'REQUIRED if download')
    parser.add_argument('-get_orig_media', '--getorigmedia', help = 'REQUIRED if download')

    parser.add_argument('-f', '--fileproperties', help = 'REQUIRED if updateasset')
    parser.add_argument('-j', '--jobid', help = 'Job identifier')
    parser.add_argument('-l', '--linkipaddrport', help = 'Link IP/Hostname Port')

    args = parser.parse_args()

    #Extract mode and project id
    mode = args.mode

    if args.projectid == None:
        project_id = ""
    else:
        project_id = args.projectid

    bearer_token = ''
    config_name = args.config
    
    if args.jobid == None:
        job_guid = ""
    else:
        job_guid = args.jobid

    cloud_config_path = ''
    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','cloudconfigfolder'):
            section_info = config_parser['General']
            cloud_config_path = section_info['cloudconfigfolder'] + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            cloud_config_path = my_plist["CloudConfigFolder"] + "/cloud_targets.conf"
            
    if not os.path.exists(cloud_config_path):
        err= "Unable to find cloud target file: " + cloud_config_path
        print(err)
        exit(1)

    config_parser = ConfigParser()
    config_parser.read(cloud_config_path)
    if not config_name in config_parser.sections():
        err = 'Unable to find cloud configuration: ' + config_name
        print(err)
        exit(1)
        
    cloud_config_info = config_parser[config_name]
    bearer_token = cloud_config_info["FrameIODevKey"]
    if project_id == "":
        project_id = cloud_config_info["project_id"]
    else:
        if not project_id == cloud_config_info["project_id"]:
            print(f'Project Id found({cloud_config_info["project_id"]}) under the config does not match the provided one({project_id})! Exiting...')
            exit(1)

    headers = {'authorization': f'Bearer {bearer_token}'}
    
    if mode == 'rootid':
        print(get_root_id(project_id, headers))
    
    elif mode == 'listing' or mode == 'folderlisting':
        
        folder = (mode == 'folderlisting')
        asset_id = args.assetid
        children = get_children(project_id, headers, asset_id, folder)

        #print(dumps(children, indent = 4))

        #logging.debug(children)

        result = '<FOLDERLIST>\n'
        
        for child in children:
            eye_dee, name, child_type = child['id'], child['name'], child['_type']
            result += f"<FOLDER ID='{eye_dee}' ACCESS='1' TYPE='NORMAL'>{add_CDATA_tags(name)}</FOLDER>\n" if child_type == 'folder' \
                else f"<FOLDERITEM ID='{eye_dee}' ACCESS='1' TYPE='NORMAL'>{add_CDATA_tags(name)}</FOLDERITEM>\n"
        
        result += '</FOLDERLIST>'
        print(result)
    
    elif mode == 'upload':
        source_name = args.sourcename
        up_path = args.uploadpath
        catalog_file_path = args.catalogpath
        archive_name = args.archivename
        is_proxy_upload = args.proxyupload

        #NOT USING THIS FOR NOW. We don't need clienthash as there is going to be a single entry only for link_address, link_port in Servers.conf
        """
        ##get cleinthash of machine
        if is_linux == 1:
            proc1 = subprocess.run(['/opt/sdna/bin/clienthash'], stdout=subprocess.PIPE, universal_newlines=True)
        else:
            proc1 = subprocess.run(['/Applications/StorageDNA/accessDNA.app/Contents/MacOS/clienthash'], stdout=subprocess.PIPE, universal_newlines=True)
        if proc1.returncode != 0:
            print ("Failed to get clienthash of the machine.")
            exit(4)
        clienthash = proc1.stdout
        clienthash = clienthash.strip()
        clienthash = clienthash.strip('\n')
        """
        
        client_ip = ""
        client_port = ""

        if not args.linkipaddrport is None:
            link_ip_port_parts = args.linkipaddrport.split(":")
            if len(link_ip_port_parts) == 2:
                client_ip = link_ip_port_parts[0]
                client_port = link_ip_port_parts[1]
       
        if len(client_ip) == 0 or len(client_port) == 0:
            ##get machine IP for that clienthash from Servers.conf file
            try:
                with open(SERVERS_CONF_FILE) as f:
                    linkAddrLine_found = False
                    linkPortLine_found = False
                    for line in f:
                        if is_linux == 1:
                            #if line.startswith(clienthash+'/link_address='):
                            if 'link_address' in line:
                                client_ip = line[line.index('=') + 1 : ].strip('\n')
                            #elif line.startswith(clienthash+'/link_port='):
                            elif 'link_port' in line:
                                client_port = line[line.index('=') + 1 : ].strip('\n')
                        else:
                            if "link_address" in line:
                                linkAddrLine_found = True
                                continue
                            if "link_port" in line:
                                linkPortLine_found = True
                                continue
                            if linkAddrLine_found:
                                linkAddrLine_found = False
                                client_ip = line[line.index(">") + 1 : line.index("</")].strip('\n')
                            if linkPortLine_found:
                                linkPortLine_found = False
                                client_port = line[line.index(">") + 1 : line.index("</")].strip('\n')
            except IOError:
                print(f'Failed to open {SERVERS_CONF_FILE}. Exiting...')
                exit(5)

        catalog_path = remove_file_name_from_path(catalog_file_path)
        encoded_catalog_path = urllib.parse.quote(catalog_path.encode('UTF-8'))

        ##get source file name and url encode it
        source_filename = ""
        if is_proxy_upload == "true":
            source_filename = extract_file_name(catalog_file_path)
        else:
            source_filename = os.path.basename(source_name)
        encoded_src_name = urllib.parse.quote(source_filename.encode('UTF-8'))

        ##URL is the description in frame.io
        '''
        url = ''
        if client_port != '443':
            url=(f"http://{client_ip}:{client_port}/dashboard/projects/{job_guid}/browse&search?path={encoded_catalog_path}&filename={encoded_src_name}")
        else:
        '''
        url=(f"https://{client_ip}:{client_port}/dashboard/projects/{job_guid}/browse&search?path={encoded_catalog_path}&filename={encoded_src_name}")
        
        ##If a path is supplied then we gotta find the upload id, otherwise we trust that the user gave us an up_id as up_path
        up_id = find_upload_id(up_path, project_id, bearer_token) if '/' in up_path else up_path

        client = FrameioClient(bearer_token)
        #project = client.get_project(project_id)

        #Old code to create and upload an asset
        asset = client.assets.create(
            parent_asset_id = up_id,
            name = extract_file_name(source_name),
            type = "file",
            description = url,
            filesize = os.stat(source_name).st_size
        )
        new_assetId = asset['id']
        
        file = open(source_name, 'rb')
        client.assets._upload(asset, file)
        file.close()

        ##This will be the only and final call to be used to create and upload asset in the future
        #asset = client.assets.upload(up_id, source_name)
        #new_assetId = asset['id']

        print (new_assetId)
        
    elif mode == 'download': 
        frame_file_asset_id = args.sourcename
        download_path = args.downloadpath
        media_res = args.mediaresolution
        is_get_orig_media = args.getorigmedia

        if media_res == "":
            media_res = "ORIG"
        
        client = FrameioClient(bearer_token)
        
        if media_res != "ORIG" and media_res != "":
            useFrameIODownload = False
 
        if useFrameIODownload == True:
            if os.path.exists(download_path):
                os.remove(download_path)
            download_path_only = os.path.dirname(download_path)
            asset = client.assets.get(frame_file_asset_id)
            if asset['filesize'] > 1048576:
                client.assets.download(asset, download_path_only, multi_part=True)
            else:
                client.assets.download(asset, download_path_only)
        else:
            response = client.assets.get(frame_file_asset_id)
        
            url = ""
        
            downloads_json = response['downloads']
            #print (downloads_json)      
            if media_res == "ORIG":
                url = response['original']
            else:
                val_found = 0
                count = 0
                for value in downloads_json.values(): 
                    count += 1
                    if count == 6:
                        break
                    if value != None:
                        val_found = 1
                        break
                
                if val_found == 0:
                    url = response['original']

                media_res_url = ""
                if url == "" or url == None:
                    media_res_url = response['downloads'][f'{media_res}']
            
                    if media_res_url == None and is_get_orig_media == "true":
                        url = response['original']
                    else:
                        url = media_res_url

            #print ("URL = "+url)
            if url == "" or url == None:
                print (f"Failed to obtain URL = {url}")
                exit (4)

            download_file(url, download_path)

    elif mode == 'foldercreate':
        up_path = args.uploadpath
        print(find_upload_id(up_path, project_id, bearer_token))

    elif mode == 'updateasset':
        asset_id = args.assetid
        prop_file = args.fileproperties

        client = FrameioClient(bearer_token)
        project = client.projects.get(project_id)

        prop_dict = dict()

        with open(prop_file) as f:
            for line in f:
                split_line = line.split(',')

                if len(split_line) != 2:
                    continue
                
                key, value = split_line[0].strip(), split_line[1].strip('\n').strip()
                prop_dict[key] = value

        props = {
            'properties' : prop_dict
        }

        headers = {
            'content-type': "application/json",
            'authorization': f"Bearer {bearer_token}"
        }

        request('PUT', f'https://api.frame.io/v2/assets/{asset_id}', data = dumps(props), headers = headers)
 
    elif mode == 'deleteasset':
        asset_id = args.assetid
        client = FrameioClient(bearer_token)
        client.assets.delete(asset_id)

    else:
        print(f'Error only modes allowed so far are {VALID_MODES}')

    exit(0)

