import json
import argparse
from action_functions import *
import requests
import logging

SCOPES = ['https://www.googleapis.com/auth/drive']

DEBUG_FILEPATH = '/tmp/googledrive_debug.out'
DEBUG_PRINT = False
DEBUG_TO_FILE = False
BASE_URL = "https://api.twelvelabs.io/v1.3"


def handle_buckets():
    url = f"{BASE_URL}/indexes"
    headers = {
        "x-api-key": config_map.get("api_key"),
        "Content-Type": "application/x-www-form-urlencoded"
    }

    folders = []
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        for item in result.get("data", []):
            folders.append({
                "id": item.get("_id"),
                "name": item.get("index_name")
            })
    else:
        logging.error(f"Failed to fetch folders: {response.status_code} {response.text}")

    buckets = [f"{f['id']}:{f['name']}" for f in folders]
    print(','.join(buckets))
    sys.exit(0)
    

# --- Mode: bucketsfolders (XML list of top-level folders in workspace) ---
def handle_bucketsfolders(args):
    url = f"{BASE_URL}/indexes"
    headers = {
        "x-api-key": config_map.get("api_key"),
        "Content-Type": "application/x-www-form-urlencoded"
    }

    folders = []
    response = requests.get(url, headers=headers)
    if response and response.status_code == 200:
        folders_data = response.json().get("data", [])
        for item in folders_data:
            folders.append({
                "id": item.get("_id"),
                "name": item.get("index_name")
            })
        xml_output = add_CDATA_tags_with_id_for_folder(folders)
        print(xml_output)
    else:
        print('<?xml version="1.0" encoding="UTF-8"?><folders></folders>')
    sys.exit(0)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,download,list,actions')
    parser.add_argument('-id','--folder_id',help='folder_id')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-b', '--bucket', required=False, help = 'Bucket for browse')
    parser.add_argument('-tmp','--tempid',help='tempid')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-r', '--runid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    parser.add_argument('-l', '--label', required=False, help = 'REQUIRED for list if a label is included.')
    parser.add_argument("--progress_path", required=False, help = 'progress_path')
    parser.add_argument('-o',"--overwrite", action="store_false", help = 'overwrite')


    args = parser.parse_args()
    mode = args.mode
    folder_id = args.folder_id
    file_path = args.source
    target_path = args.target
    tmp_id = args.tempid
    bucket = args.bucket
    folder_path = args.foldername
    progress_path = args.progress_path


    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)

    params_map = {}
    params_map["source"] = args.source
    params_map["target"] = args.target
    params_map["indexid" ] = args.indexid
    params_map["jobguid"] = args.jobguid
    params_map["jobid"] = args.jobid
    params_map["label"] = args.label
    params_map["project_name"] = args.projectname

    params_map["filtertype"] = filter_file_dict["type"]
    params_map["filterfile"] = filter_file_dict["filterfile"]
    params_map["policyfile"] = filter_file_dict["policyfile"]

    for key in config_map:
        if key in params_map:
            print(f'Skipping existing key {key}')
        else:
            params_map[key] = config_map[key]

    debug_print(DEBUG_FILEPATH,DEBUG_PRINT,DEBUG_TO_FILE,f"Parameters: {params_map}")

    progressDetails = {}
    progressDetails["duration"] = int(time.time())
    progressDetails["run_id"] = args.runid
    progressDetails["job_id"] = args.jobid
    progressDetails["progress_path"] = progress_path
    progressDetails["totalFiles"] = 0
    progressDetails["totalSize"] = 0
    progressDetails["processedFiles"] = 0
    progressDetails["processedBytes"] = 0
    progressDetails["status"] = "SCANNING"

    if mode == 'actions':
        try:
            if params_map["actions"]:
                print(params_map["actions"])
                exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            exit(1)

    if mode == "list":
        pass

    elif mode == 'browse':
        pass

    elif mode == 'buckets':
        buckets = []
        folders = handle_buckets(config_map)
        for folder in folders:
            buckets.append(f"{folder['id']}:{folder['name']}")
        print(','.join(buckets))
        exit(0)

    elif mode == 'bucketsfolders':
        buckets = []
        folders = handle_bucketsfolders()
        for folder in folders:
            buckets.append({"name" : folder['name'],
                            "id" : folder["id"]
                            })
        xml_output = add_CDATA_tags_with_id_for_folder(buckets)
        print(xml_output)
        exit(0)

    elif mode == 'upload':
        pass

    elif mode == 'download':
        pass

    elif mode == 'createfolder':
        pass

    else:
        print(f'Unsupported mode {mode}')
        exit(1)
