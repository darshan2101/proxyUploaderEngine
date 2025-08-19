import os
import shutil
import json
import sys
import time
import plistlib
import pathlib
import xml.etree.ElementTree as ET
import glob
from configparser import ConfigParser
import plistlib
import hashlib
from datetime import datetime
import csv

USER = "jboss"
GROUP = "jboss"

def generate_html(metadata, file_name):
    if isinstance(metadata, str):
        metadata = json.loads(metadata)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        pre {{
            white-space: pre-wrap;
        }}
    </style>
</head>
<body>
    <pre>
{json.dumps(metadata, indent=4)}
    </pre>
</body>
</html>"""

    with open(file_name, "w") as file:
        file.write(html_content)
    
    return file_name

def generate_json(metadata, file_name):
    if len(metadata) != 0:
        with open(file_name, "w") as file:
            json.dump(metadata,file)
        return file_name


def add_CDATA_tags_for_folder(folders : list):
    xml_output = '<FOLDERLIST>\n'
    for folder in folders:
        xml_output += f'<FOLDER><![CDATA[{folder}]]></FOLDER>\n'
    xml_output += '</FOLDERLIST>'
    return xml_output


def add_CDATA_tags_with_id_for_folder(folders : list):
    xml_output = '<FOLDERLIST>\n'
    for folder in folders:
        xml_output += f'<FOLDER ID="{folder["id"]}" ACCESS="1" TYPE="NORMAL"><![CDATA[{folder["name"]}]]></FOLDER>\n'
    xml_output += '</FOLDERLIST>'
    return xml_output

def add_CDATA_tags_for_buckets(folders : list):
    xml_output = '<BUCKETLIST>\n'
    for folder in folders:
        xml_output += f'<BUCKET><![CDATA[{folder}]]></BUCKET>\n'
    xml_output += '</BUCKETLIST>'
    return xml_output

def add_CDATA_tags_with_id_for_buckets(folders : list):
    xml_output = '<BUCKETLIST>\n'
    for folder in folders:
        xml_output += f'<BUCKET ID="{folder["id"]}" ACCESS="1" TYPE="NORMAL"><![CDATA[{folder["name"]}]]></BUCKET>\n'
    xml_output += '</BUCKETLIST>'
    return xml_output

def get_filename(filePath : str):
    if '/' in filePath:
        filename = filePath.split('/')[-1]
    elif '\\' in filePath:
        filename = filePath.split('\\')[-1]
    else:
        filename = filePath
    return filename


def isMatch(s, p):
    m, n = len(s), len(p)

    # Create a 2D DP table to store matching information
    dp = [[False] * (n + 1) for _ in range(m + 1)]

    # Empty pattern matches empty string
    dp[0][0] = True

    # Fill the first row of the DP table (when s is empty)
    for j in range(1, n + 1):
        if p[j - 1] == '*':
            dp[0][j] = dp[0][j - 1]

    # Fill the DP table using bottom-up dynamic programming
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if p[j - 1] == '*':
                dp[i][j] = dp[i - 1][j] or dp[i][j - 1]
            elif p[j - 1] == '?' or p[j - 1] == s[i - 1]:
                dp[i][j] = dp[i - 1][j - 1]

    return dp[m][n]


def isFilenameInFilterList(filename, filter_list):
    for filter in filter_list:
        if isMatch(filename, filter):
            return True
    return False


def get_projectid(filename):
    projectid  = filename.split("/")[-1]
    return projectid.split(".")[0]


def get_runid(filename):
    runid  = filename.split("/")[-1]
    return runid.split(".")[1]
    

def load_settings(provider):
    settings_file = f"{provider}.settings.json"
    if os.path.isfile(settings_file):
        with open(settings_file, 'r') as f:
            return json.load(f)
    else:
        print(f"Settings file {settings_file} not found.")
        return None


def send_response(handler, status, state, message):
    handler.send_response(status)
    handler.send_header('Content-type', 'application/json')
    handler.end_headers()
    response = {
        "status": status,
        "statusState": state,
        "message": message
    }
    handler.wfile.write(json.dumps(response).encode())

    
def load_policies_from_file(policy_filename):

    output = {}

    if len(policy_filename) == 0:
        output["type"] = "NOFILE"
        output["entries"] = []
        output["result"] = "SUCCESS"
        return output
    
    elif not os.path.isfile(policy_filename):
         output["type"] = "ERROR"
         output["entries"] = []
         output["result"] = "ERROR"
         return output

    policy_type = ""
    policy_entries = []

    try:
        policy_file = open(policy_filename)
        lines = policy_file.readlines()
        policy_file.close()

        for line in lines:
            if line.startswith("Type:"):
               policy_type = line.strip().split(":").pop()
               continue
            else:
               policy_parts = line.strip().split("|")

               if len(policy_parts) < 3:
                    continue  #invalid entry

               policy_entry = {}
               policy_entry["object"] = policy_parts.pop(0)
               policy_entry["verb"] = policy_parts.pop(0)
               policy_entry["value"] = "|".join(policy_parts)
               policy_entries.append(policy_entry)
    except:
        
        print("Unable to parse policies")
        policy_file.close()
        output["entries"] = []
        output["type"] = "ERROR"
        output["result"] = "ERROR"
        return output   
    
    output["entries"] =  policy_entries
    output["type"] = policy_type
    output["result"] = "SUCCESS"
    return output


def file_in_policy(policy_map, file_name, file_parent_path, file_size, mtime_epoch_seconds):

    policy_type = policy_map["type"]
    policy_entries = policy_map["entries"]

    # open the policy file and read in the contentws

    try:
        
        lastPolicyResult  = None

        for policy in policy_entries:

            if policy_type == 'ANY' and lastPolicyResult == True:
                return True
            elif policy_type == 'ALL' and lastPolicyResult == False:
                return False
            
            if policy["object"] == "filename" and policy["verb"] == "startwith":
                lastPolicyResult = file_name.startswith(policy["value"])
            elif policy["object"] == "filename" and policy["verb"] == "endswith":
                lastPolicyResult = file_name.endswith(policy["value"])
            elif policy["object"] == "filename" and policy["verb"] == "contains":
                lastPolicyResult = policy["value"] in file_name
            elif policy["object"] == "filename" and policy["verb"] == "matches":
                lastPolicyResult = isMatch(file_name, policy["value"])
            if policy["object"] == "filename" and policy["verb"] == "doesnotmatch":
                lastPolicyResult = not isMatch(file_name, policy["value"])
            if policy["object"] == "filepath" and policy["verb"] == "contains":
               lastPolicyResult = policy["value"] in file_parent_path
            if policy["object"] == "filepath" and policy["verb"] == "matches":
                lastPolicyResult = isMatch(file_parent_path, policy["value"])
            if policy["object"] == "filepath" and policy["verb"] == "doesnotmatch":
                lastPolicyResult = not isMatch(file_parent_path, policy["value"])
            if policy["object"] == "size" and policy["verb"] == "morethan":
                lastPolicyResult = int(file_size) > int(policy["value"])
            if policy["object"] == "size" and policy["verb"] == "lessthan":
                lastPolicyResult = int(file_size) < int(policy["value"])
            if policy["object"] == "mtime" and policy["verb"] == "morethan":
                lastPolicyResult = int(mtime_epoch_seconds) > int(policy["value"])
            if policy["object"] == "mtime" and policy["verb"] == "lessthan":
                lastPolicyResult = int(mtime_epoch_seconds) < int(policy["value"])

        return policy_type == 'ALL'

    except:
        
        return False


def generate_xml_from_file_objects(parse_result, xml_file):
    scanned_count = parse_result.get("scanned_count")
    selected_count = parse_result.get("selected_count")
    total_size = parse_result.get("total_size")
    root = ET.Element("files", scanned=str(scanned_count), selected=str(selected_count), size=str(total_size), bad_dir_count="0", delete_count="0", delete_folder_count="0")
    
    if "filelist" in parse_result:
        for file_info in parse_result["filelist"]:
            file_element = ET.SubElement(root, "file")
            for attr_name, attr_value in file_info.items():
                file_element.set(attr_name, str(attr_value))
    
    tree = ET.ElementTree(root)
    tree.write(xml_file)


def generate_xml(filename,xml_output):
    with open(filename, 'w') as f:
        f.write(xml_output)


def symbolic_to_hex(symbolic):
    permissions = {
        'r': 4,
        'w': 2,
        'x': 1,
        '-': 0
    }

    owner_perms = symbolic[1:4]
    group_perms = symbolic[4:7]
    other_perms = symbolic[7:10]

    def convert_to_hex(perm_str):
        val = 0
        for char in perm_str:
            val += permissions[char]
        return hex(val)[-1]

    return '0x' + ''.join([convert_to_hex(owner_perms), convert_to_hex(group_perms), convert_to_hex(other_perms)])


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

    avg_bandwidth=232
    file = open(progress_path, "w")

    xml_str = f"""<update-job-progress duration="{duration}'" avg_bandwidth="{avg_bandwidth}">
        <progress jobid="{job_id}" cur_bandwidth="0" stguid="{run_guid}" requestid="{request_id}">
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

    file.write(xml_str)
    file.close()
    return


def restore_ticket_to_csv(ticket_path, current_time, include_filename = False):
    total_size = 0
    total_files = 0
    csv_file = f'{ticket_path}/sdna-Restore-{current_time}.csv'
    file = open(csv_file, "w")
    for xml_ticket in glob.glob(f'{ticket_path}/*-converted.xml'): 
        filename = ""
        target_path = ""
        index_id = ""
        tree = ET.parse(xml_ticket)
        root = tree.getroot()
        ticket_attribs = root.attrib
        total_size += int(ticket_attribs["size"])
        total_files += int(ticket_attribs["file-count"])
        for child in root.iter():
            #print (f"I'm working with this tag - {child.tag}")
            if child.tag == "file":
                file_attributes = child.attrib 
                index_id = file_attributes["headers"]
            elif child.tag == "filename":
                filename = child.text
            elif child.tag == "target-path":
                target_path = child.text
            if include_filename:
                if index_id != "" and target_path != "" and filename != "":
                    #print (f"FILENAME = {filename}, TARGET = {target_path}, INDEX = {index_id}")
                    file.write(f"{index_id},/host_dir/{target_path}/{filename}\n")
                    filename = ""
                    target_path = ""
                    index_id = ""
            else:
                if index_id != "" and target_path != "":
                    #print (f"TARGET = {target_path}, INDEX = {index_id}")
                    file.write(f"{index_id},/host_dir/{target_path}\n")
                    filename = ""
                    target_path = ""
                    index_id = ""
    csv_dict = {}
    csv_dict["csvfile"] = csv_file
    csv_dict["totalfiles"] = total_files
    csv_dict["totalsize"] = total_size

    file.close()
    return csv_dict


def loadFilterPolicyFiles(job_guid):

    filtering_dict = {}
    filtering_dict["type"] = "none"
    filtering_dict["filterfile"] = ""
    filtering_dict["policyfile"] = ""
    
    attribute_file = f'/tmp/.attr-{job_guid}.out'
    include_file = f'/tmp/.include-{job_guid}.out'
    exclude_file = f'/tmp/.exclude-{job_guid}.out'

    if os.path.exists(include_file):
        filtering_dict["type"] = "include"
        filtering_dict["filterfile"] = include_file

    if os.path.exists(exclude_file):
        filtering_dict["type"] = "exclude"
        filtering_dict["filterfile"] = exclude_file
    
    if os.path.exists(attribute_file):
        filtering_dict["policyfile"] = attribute_file

    return filtering_dict


def loadLoggingDict(logging_suffix, job_guid):

    logging_dict = {}
    logging_dict["level"] = 0

    if os.path.isdir("/opt/sdna/bin/"):
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    else:
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    if not os.path.exists(DNA_CLIENT_SERVICES):
        print(f'Unable to find configuration file: {DNA_CLIENT_SERVICES}')
        return logging_dict;

    logging_folder = ""
    logging_level = 0
    logging_available = False
    logging_dict["logging_level"] = 0

    if os.path.isdir("/etc/StorageDNA"):
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General'):
            section_info = config_parser['General']
            if config_parser.has_option('General','CommandLoggingLevel') and config_parser.has_option('General','CommandLoggingPath'):
                logging_level = int(section_info['CommandLoggingLevel'])
                logging_folder = section_info['CommandLoggingPath']
                if logging_level > 0:
                    logging_available = True

    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            logging_level = int(my_plist["CommandLoggingLevel"])
            logging_folder = my_plist['CommandLoggingPath']
                
    if logging_available == True:
        logging_folder=f'{logging_folder}/{job_guid}'
        if not os.path.exists(logging_folder):
            pathlib.Path(logging_folder).mkdir(parents=True, exist_ok=True)
        current_date=int(time.time())
        logging_file_name = f'{logging_folder}/{current_date}.{logging_suffix}'
        logging_file_error_name = f'{logging_folder}/{current_date}_ERROR.{logging_suffix}'
        logging_dict["logging_level"] = logging_level
        logging_dict["logging_filename"] = logging_file_name
        logging_dict["logging_error_filename"] = logging_file_error_name

    return logging_dict

def loadConfigurationMap(config_name):
    config_map = {}

    # Detect platform
    is_linux = 0
    is_windows = 0
    if os.path.isdir("/opt/sdna/bin/"):
        is_linux = 1
    elif os.name == 'nt':  # Windows detection
        is_windows = 1
    
    # Set configuration file path based on platform
    if is_linux == 1:
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    elif is_windows == 1:
        # Direct path for Windows testing
        DNA_CLIENT_SERVICES = r'D:\Dev\(python +node) Wrappers\proxyUploaderEngine\CloudConfig\cloud_targets.conf'
    else:
        # Fallback to macOS
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    if not os.path.exists(DNA_CLIENT_SERVICES):
        print(f'Unable to find configuration file: {DNA_CLIENT_SERVICES}')
        return False

    # Handle configuration based on platform
    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','cloudconfigfolder'):
            section_info = config_parser['General']
            cloudTargetPath = section_info['cloudconfigfolder'] + "/cloud_targets.conf"
            tapeProxyPath = section_info.get('tapeproxypath', '/tmp')
        else:
            # Fallback for Linux if General section not found
            cloudTargetPath = DNA_CLIENT_SERVICES
            tapeProxyPath = "/tmp"
    elif is_windows == 1:
        # For Windows testing, use the same file for both
        cloudTargetPath = DNA_CLIENT_SERVICES
        tapeProxyPath = r"D:\Dev\(python +node) Wrappers\proxyUploaderEngine\tapeproxy"  # Adjust this path as needed
    else:
        # macOS/other platforms
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            cloudTargetPath = my_plist["CloudConfigFolder"] + "/cloud_targets.conf"
            tapeProxyPath = my_plist.get("TapeProxyPath", "/tmp")

    if not os.path.exists(cloudTargetPath):
        err = "Unable to find cloud target file: " + cloudTargetPath
        sys.exit(err)

    config_parser = ConfigParser()
    config_parser.read(cloudTargetPath)
    if not config_name in config_parser.sections():
        err = 'Unable to find cloud configuration: ' + config_name
        sys.exit(err)

    config_map["tapeproxypath"] = tapeProxyPath
    cloud_config_info = config_parser[config_name]
    for key in cloud_config_info:
        config_map[key] = cloud_config_info[key]

    return config_map

def get_meta_data_folder():
    is_linux = 0
    if os.path.isdir("/opt/sdna/bin/"):
        is_linux = 1
    if is_linux == 1:
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    else:
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    if not os.path.exists(DNA_CLIENT_SERVICES):
        print(f'Unable to find configuration file: {DNA_CLIENT_SERVICES}')
        return False

    workFolder = ""
    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','MetaDataWorkFolder'):
            section_info = config_parser['General']
            workFolder = section_info['MetaDataWorkFolder']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            workFolder  = my_plist["MetaDataWorkFolder"]

    if (len(workFolder) == 0):
        workFolder = "/tmp"
    return workFolder

def check_if_catalog_file_exists(catalog_path, file_name, file_time):
    if catalog_path.endswith("/"):
        catalog_file_path = catalog_path + file_name
    else:
        catalog_file_path = catalog_path + "/" + file_name

    #print (f'File - ({file_name}) time = {int(file_time)}') 
    ##Here we're replacing the folder paths with spaces before and after with $_ and _$ respectively to prevent catalog not found error.
    if not os.path.exists(catalog_file_path):
        #print ("CATALOG FILE NOT FOUND for = "+catalog_file_path)
        catalog_file_parts_list = catalog_file_path.split("/")
        for i, part in enumerate(catalog_file_parts_list):
            #print (f"OLD PART = {part}...")
            if part != "":
                if part.endswith(" ") and part.startswith(" "):
                    #print(f"1 HERE with part = {part}")
                    part = f"$_{part}_$"
                elif part.endswith(" "):
                    #print(f"2 HERE with part = {part}")
                    part = f"{part}_$"
                elif part.startswith(" "):
                    #print(f"3 HERE with part = {part}")
                    part = f"$_{part}"
                elif ":" in part:
                    #print(f"4 HERE with part = {part}")
                    part = part.replace(":","$;$")
            #print (f"NEW PART = {part}...")
            catalog_file_parts_list[i] = part
        catalog_file_path = ("/").join(catalog_file_parts_list)

    #print ("NEW CATALOG FILE built = "+catalog_file_path)   
    if not os.path.exists(catalog_file_path):
        #print ("Catalog file not found. Returning.")
        return False
    else:
        #print (f'M-time catalog file - ({file_name}) = {int(os.path.getmtime(catalog_file_path))}.')
        if int(os.path.getmtime(catalog_file_path)) == int(file_time):
            #print (f'SKIPPING FILE - {file_name}\n')
            return True     
        else:
            return False


def get_catalog_path(params_map):
    if params_map['source'] is not None:
        source_path = params_map['source']
    else:
        source_path = ""
    if params_map['label'] is not None:
        label = params_map['label']
    else:
        label = ""
    if params_map['project_name'] is not None:
        project_name = params_map['project_name']
    else:
        project_name = "" 
         
    strip_path = False
    keep_top = False  
    
    if source_path.endswith("/./"):
        strip_path = True
        keep_top = False
        source_path = source_path.replace("/./","")
    elif "/./" in source_path:
        source_path = source_path.replace("/./", "/")
        strip_path = True
        keep_top = True
        
    if source_path.endswith("/"):
        source_path = source_path[:-1]
    
    if len(label) == 0:
        baseCatalogPath = params_map['tapeproxypath'] + "/" + project_name + "/1"
    else:
        baseCatalogPath = params_map['tapeproxypath'] + "/" +project_name + "/1/" + label
    
    if not strip_path and not keep_top:
        catalogPath = baseCatalogPath + source_path + "/"
    elif strip_path and not keep_top:
        catalogPath = baseCatalogPath + "/"
    elif strip_path and keep_top:
        catalogPath = baseCatalogPath + "/" + source_path.split("/")[-1] + "/"
    
    return catalogPath


def replace_file_path(file_path):
    replace = {
        "\\": "/",
        "///": "/",
        "//": "/",
        "&": "&amp;",
        "\"": "&quot;"
    }

    for key, value in replace.items():
        if key == "&" and "&amp;" in file_path:
            continue
        if key == "\"" and "&quot;" in file_path:
            continue
        file_path = file_path.replace(key, value)
    
    return file_path


def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()


def aoc_command_to_str(command, filename):
    command_listed = '\" \"'.join(command)
    command_exec=f'\"{command_listed}\"'
    f = open(filename, "a")
    f.write(f'{command_listed}\n')
    f.close()

def generate_metadata_ele(metadata,run_date):
    xml_string = '<meta-data-elements>\n'
    for key, value in metadata.items():
        xml_string += f'<meta-data run="{run_date}" key="{key}"><![CDATA[{value}]]></meta-data>\n'
    xml_string += '</meta-data-elements>\n'
    return xml_string

def generate_metadata_ele_for_clip(metadata):
    xml_string = '<meta-data>\n'
    for key, value in metadata.items():
        xml_string += f'<data name="{key}"><![CDATA[{value}]]></data>\n'
    xml_string += '</meta-data>\n'
    return xml_string

def add_cdata(tag, text):
    return f"<{tag}><![CDATA[{text}]]></{tag}>"

def generate_clip_file(data_dict):
    clip_full_path = f"{data_dict['catalog_path']}/{data_dict['clip_path']}/{data_dict['clip_name']}.xml"
    file_entry = f'''<file type="audiovideo" file-id="{data_dict['fileid']}"><![CDATA[{data_dict['source_path']}/{data_dict['file_name']}]]></file>'''
    if not os.path.exists(clip_full_path):
        os.makedirs(f"{data_dict['catalog_path']}/{data_dict['clip_path']}",exist_ok=True)
        xml_string = ''
        xml_string += f'''<clip typeid="{data_dict['type_id']}" type="{data_dict['type']}" clipid="{data_dict['clip_id']}" name="{data_dict['clip_name']}" >\n'''
        xml_string += f"{add_cdata('media-path', data_dict['media_path'])}\n"
        xml_string += f'{generate_metadata_ele_for_clip(data_dict["metadata"])}'
        xml_string += f'<files>\n'
        xml_string += f'{file_entry}\n'
        xml_string += f'</files>\n'
        xml_string += f'</clip>\n'

        with open(clip_full_path, "w",encoding='utf-8') as f:
            f.write(xml_string)
        print(f"Generated Clip XML: {clip_full_path}")
        shutil.chown(clip_full_path,USER,GROUP)
        os.chmod(clip_full_path,0o777)
    
    else:
        with open(clip_full_path, "r", encoding='utf-8') as f:
            xml_str = f.read()

        if "</files>" in xml_str:
            updated_xml_file = xml_str.replace("</files>", f"{file_entry}\n</files>")
        else:
            print("<files> tag not found. skipping insert file entry.")
            return

        with open(clip_full_path, "w", encoding='utf-8') as f:
            f.write(updated_xml_file)

        print(f"Updated Clip XML: {clip_full_path}")

def generate_catalog_file_with_metadata(data_dict,secondary_archivename=False):
    source_path = data_dict['source_path'] if len(data_dict['source_path'])!=0 else '/'
    if not source_path.startswith('/'):
        source_path = '/' + source_path
    xml_string = ''
    xml_string += f'<file type="file">\n'
    xml_string += f"{add_cdata('filename', data_dict['file_name'])}\n"
    xml_string += f"{add_cdata('proxy-path', data_dict['proxy_path'] if secondary_archivename == False else data_dict['new_proxy_path'])}\n"
    xml_string += f"{add_cdata('source-path', source_path)}\n"
    xml_string += f"{add_cdata('archive-name', data_dict['archive_name'] if secondary_archivename == False else data_dict['new_proxy_path'])}\n"
    xml_string += f"<permissions/>\n"
    tape_path = data_dict['tape_prefix'] + '/' + source_path if len(data_dict['tape_prefix']) !=0 else source_path
    xml_string += f"{add_cdata('tape-path', tape_path)}\n"
    if 'checksum' in data_dict:
        xml_string += f'''<actions><action version="1" tier="" size="{data_dict['size']}" mod-time="{data_dict['mtime']}" run="{data_dict['run_date']}" checksum="{data_dict['checksum']}" write-time="{data_dict['write_time']}" write-index="{data_dict['write_index']}" type="upd" file-id="{data_dict['file_id']}" access-time="{data_dict['atime']}" tapeid="{data_dict['tapeid'] if 'tapeid' in data_dict else "INDEX"}"/></actions>\n'''
    else:
        xml_string += f'''<actions><action version="1" tier="" size="{data_dict['size']}" mod-time="{data_dict['mtime']}" run="{data_dict['run_date']}" write-time="{data_dict['write_time']}" write-index="{data_dict['write_index']}" type="upd" file-id="{data_dict['file_id']}" access-time="{data_dict['atime']}" tapeid="{data_dict['tapeid'] if 'tapeid' in data_dict else "INDEX"}"/></actions>\n'''
    xml_string += f'{generate_metadata_ele(data_dict["metadata"],data_dict["run_date"])}'
    xml_string += f'</file>'
    catalog_xml_file_path = ''
    if secondary_archivename == False:
        catalog_xml_file_path = f"{data_dict['catalog_path']}/{data_dict['proxy_path']}"
    else:
        catalog_xml_file_path = f"{data_dict['catalog_path']}/{data_dict['new_proxy_path']}"

    catalog_softlink_path = f"{data_dict['catalog_path']}/.mongo/{data_dict['archive_name']}/Folder.0"
    os.makedirs(catalog_xml_file_path,exist_ok=True)
    os.makedirs(catalog_softlink_path,exist_ok=True)

    xml_file = f"{catalog_xml_file_path}/{data_dict['file_name']}"
    path_hash = hashlib.md5(xml_file.encode()).hexdigest()
    soft_link_filename = f"{catalog_softlink_path}/{data_dict['archive_name']}_{data_dict['run_date']}_{path_hash}_{data_dict['archive_name']}.catalog"
    if not os.path.exists(xml_file):
        with open(xml_file, "w",encoding='utf-8') as f:
            f.write(xml_string)
      #  print(f"Generated XML: {xml_file}")
        if not os.path.islink(soft_link_filename):
            os.symlink(xml_file, soft_link_filename)
      #  print(f"Soft link created: {soft_link_filename} - {xml_file}")
        shutil.chown(xml_file,USER,GROUP)
        os.chmod(xml_file,0o777)

def generate_catalog_file_with_clips_and_metadata(data_dict,secondary_archivename=False):
    source_path = data_dict['source_path'] if len(data_dict['source_path'])!=0 else '/'
    if not source_path.startswith('/'):
        source_path = '/' + source_path
    xml_string = ''
    xml_string += f'<file type="file">\n'
    xml_string += f"{add_cdata('filename', data_dict['file_name'])}\n"
    xml_string += f"{add_cdata('proxy-path', data_dict['proxy_path'] if secondary_archivename == False else data_dict['new_proxy_path'])}\n"
    xml_string += f"{add_cdata('source-path', source_path)}\n"
    xml_string += f"{add_cdata('archive-name', data_dict['archive_name'] if secondary_archivename == False else data_dict['new_proxy_path'])}\n"
    xml_string += f"<permissions/>\n"
    tape_path = data_dict['tape_prefix'] + '/' + source_path if len(data_dict['tape_prefix']) !=0 else source_path
    xml_string += f"{add_cdata('tape-path', tape_path)}\n"
    xml_string += f"{add_cdata('clip-name', data_dict['clip_name'])}\n"
    if 'checksum' in data_dict:
        xml_string += f'''<actions><action version="1" tier="" size="{data_dict['size']}" mod-time="{data_dict['mtime']}" run="{data_dict['run_date']}" checksum="{data_dict['checksum']}" write-time="{data_dict['write_time']}" write-index="{data_dict['write_index']}" type="upd" file-id="{data_dict['file_id']}" access-time="{data_dict['atime']}" tapeid="{data_dict['tapeid'] if 'tapeid' in data_dict else "INDEX"}"/></actions>\n'''
    else:
        xml_string += f'''<actions><action version="1" tier="" size="{data_dict['size']}" mod-time="{data_dict['mtime']}" run="{data_dict['run_date']}" write-time="{data_dict['write_time']}" write-index="{data_dict['write_index']}" type="upd" file-id="{data_dict['file_id']}" access-time="{data_dict['atime']}" tapeid="{data_dict['tapeid'] if 'tapeid' in data_dict else "INDEX"}"/></actions>\n'''
    xml_string += f'</file>'

    generate_clip_file(data_dict)
    
    catalog_xml_file_path = ''
    if secondary_archivename == False:
        catalog_xml_file_path = f"{data_dict['catalog_path']}/{data_dict['proxy_path']}"
    else:
        catalog_xml_file_path = f"{data_dict['catalog_path']}/{data_dict['new_proxy_path']}"

    catalog_softlink_path = f"{data_dict['catalog_path']}/.mongo/{data_dict['archive_name']}/Folder.0"
    os.makedirs(catalog_xml_file_path,exist_ok=True)
    os.makedirs(catalog_softlink_path,exist_ok=True)

    xml_file = f"{catalog_xml_file_path}/{data_dict['file_name']}"
    path_hash = hashlib.md5(xml_file.encode()).hexdigest()
    soft_link_filename = f"{catalog_softlink_path}/{data_dict['archive_name']}_{data_dict['run_date']}_{path_hash}_{data_dict['archive_name']}.catalog"
    if not os.path.exists(xml_file):
        with open(xml_file, "w",encoding='utf-8') as f:
            f.write(xml_string)
        # print(f"Generated XML: {xml_file}")
        if not os.path.islink(soft_link_filename):
            os.symlink(xml_file, soft_link_filename)
        # print(f"Soft link created: {soft_link_filename} - {xml_file}")
        shutil.chown(xml_file,USER,GROUP)
        os.chmod(xml_file,0o777)

def debug_print(DEBUG_FILEPATH,DEBUG_PRINT,DEBUG_TO_FILE,text_string):
    if DEBUG_PRINT == False:
        return
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    str = f"{formatted_datetime} {text_string}"
    
    if DEBUG_TO_FILE == False:
        print(str)
    else:
        debug_file = open(DEBUG_FILEPATH, "a",encoding='utf-8')
        debug_file.write(f'{str}\n')
        debug_file.close()

def generate_log_csv_file(csv_file,file_details):
    file_exists = os.path.isfile(csv_file)
    file_dict = {
        "Status":"SUCCESS",
        "Filename":f"{file_details['name']}",
        "Size":f"{file_details['size']}",
        "Detail":""
    }
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=file_dict.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(file_dict)
