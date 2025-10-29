import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
import random
import time
import uuid
from configparser import ConfigParser
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET

# Constants
VALID_MODES = ["proxy", "original", "get_base_target"]
LINUX_CONFIG_PATH = "DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
DESCRIPT_API_URL = "https://descriptapi.com/v1/edit_in_descript/schema"

IS_LINUX = True
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

logger = logging.getLogger()


# ----------------------------
# Utility and Helper Functions
# ----------------------------

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")

def get_link_address_and_port():
    """Read IP/Port of local controller."""
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
        for line in lines:
            if '=' in line:
                key, value = map(str.strip, line.split('=', 1))
                if key.endswith('link_address'):
                    ip = value
                elif key.endswith('link_port'):
                    port = value
    except Exception as e:
        logging.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)
    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_cloud_config_path():
    """Get cloud_targets.conf path from client services config."""
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            path = plistlib.load(fp)["CloudConfigFolder"] + "/cloud_targets.conf"
    logging.info(f"Using cloud config path: {path}")
    return path

def get_node_api_key():
    """Get local Node API key for catalog updates."""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            return parser.get('General', 'NodeAPIKey', fallback='')
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                return plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

def get_retry_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    """Generic retry wrapper for requests."""
    session = get_retry_session()
    last_exception = None
    for attempt in range(3):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"{url} returned {response.status_code}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:
                delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
                logging.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {delay:.2f}s")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}: {e}")
        except RequestException as e:
            logging.error(f"Request exception: {e}")
            raise
    if last_exception:
        raise last_exception
    return None

def prepare_metadata_to_upload(repo_guid, relative_path, file_name, file_size, backlink_url, properties_file=None):
    """Load any metadata files and merge into a base structure."""
    metadata = {
        "relativePath": relative_path if relative_path.startswith("/") else "/" + relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url,
        "fabric_size": file_size
    }
    if properties_file and os.path.exists(properties_file):
        try:
            ext = properties_file.lower()
            if ext.endswith(".json"):
                with open(properties_file) as f:
                    metadata.update(json.load(f))
            elif ext.endswith(".xml"):
                tree = ET.parse(properties_file)
                root = tree.getroot()
                for data_node in root.findall(".//data"):
                    key = data_node.get("name")
                    val = data_node.text.strip() if data_node.text else ""
                    if key: metadata[key] = val
            else:
                with open(properties_file) as f:
                    for line in f:
                        if ',' in line:
                            key, val = [x.strip() for x in line.split(',', 1)]
                            metadata[key] = val
        except Exception as e:
            logging.warning(f"Failed to parse metadata file: {e}")
    return metadata


# ----------------------------
# Descript-specific functions
# ----------------------------

def build_import_schema(partner_drive_id, source_id, file_url, file_name):
    """Construct the schema JSON required by Descript."""
    return {
        "partner_drive_id": partner_drive_id,
        "project_schema": {
            "schema_version": "1.0.0",
            "source_id": source_id,
            "files": [{
                "name": file_name,
                "uri": file_url
            }]
        }
    }

def request_import_url(api_key, schema):
    """Send schema to Descript and return import URL."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    logging.info("Requesting import URL from Descript...")
    response = make_request_with_retries("POST", DESCRIPT_API_URL, headers=headers, json=schema)
    if not response:
        logging.critical("No response from Descript API.")
        sys.exit(1)
    if response.status_code == 201:
        data = response.json()
        import_url = data.get("url")
        logging.info(f"Descript Import URL generated: {import_url}")
        return import_url
    else:
        logging.error(f"Failed to generate import URL: {response.status_code} {response.text}")
        sys.exit(1)

def update_catalog(repo_guid, file_path, import_url, file_url, provider_name="descript"):
    """Send import URL to local catalog."""
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else "/" + file_path,
        "providerName": provider_name,
        "providerData": {
            "importUrl": import_url,
            "fileUrl": file_url
        }
    }
    url = "http://127.0.0.1:5080/catalogs/providerData"
    logging.info("Updating local catalog with Descript import URL...")
    response = make_request_with_retries("POST", url, headers=headers, json=payload)
    if response and response.status_code in (200, 201):
        logging.info("Catalog updated successfully.")
    else:
        logging.warning(f"Catalog update failed: {response.status_code if response else 'No response'}")


# ----------------------------
# Main Entry
# ----------------------------

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 1.5))  # jitter to avoid concurrency spikes

    parser = argparse.ArgumentParser(description="Descript Uploader - Generate Import URL")
    parser.add_argument("-m", "--mode", required=True, help="Operation mode (proxy/original)")
    parser.add_argument("-c", "--config-name", required=True, help="Config name from cloud_targets.conf")
    parser.add_argument("-sp", "--source-path", required=True, help="Source file path")
    parser.add_argument("-cp", "--catalog-path", help="Catalog path")
    parser.add_argument("-mp", "--metadata-file", help="Metadata file path")
    parser.add_argument("-j", "--job-guid", help="Job GUID")
    parser.add_argument("-r", "--repo-guid", help="Repo GUID")
    parser.add_argument("--controller-address", help="Controller IP:Port")
    parser.add_argument("--log-level", default="info", help="Logging level")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.mode not in VALID_MODES:
        logging.error(f"Invalid mode. Choose from: {VALID_MODES}")
        sys.exit(1)

    # Load cloud config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config section not found: {args.config_name}")
        sys.exit(1)
    config = cloud_config[args.config_name]

    api_key = config.get("api_key")
    partner_drive_id = config.get("partner_drive_id")
    if not api_key or not partner_drive_id:
        logging.critical("Missing 'api_key' or 'partner_drive_id' in config.")
        sys.exit(1)

    matched_file = args.source_path
    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    # Backlink + URL prep
    catalog_path = args.catalog_path or matched_file
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    file_name_for_url = extract_file_name(matched_file)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or str(uuid.uuid4())

    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        ip, _ = get_link_address_and_port()
        client_ip = ip

    file_path = matched_file
    proxy_prefix = "/sdna_fs/ADMINISTRATORDROPBOX/primary/proxy-link/"
    if file_path.startswith(proxy_prefix):
        file_path = file_path[len(proxy_prefix):]
    file_url = f"http://{client_ip}/proxy-link{file_path if file_path.startswith('/') else '/' + file_path}"

    backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"File proxy URL: {file_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Would send schema to Descript.")
        sys.exit(0)

    # Prepare metadata
    metadata_obj = prepare_metadata_to_upload(
        repo_guid=args.repo_guid,
        relative_path=catalog_url,
        file_name=file_name_for_url,
        file_size=os.path.getsize(matched_file),
        backlink_url=backlink_url,
        properties_file=args.metadata_file
    )

    # Build schema and send to Descript
    schema = build_import_schema(partner_drive_id, job_guid, file_url, file_name_for_url)
    import_url = request_import_url(api_key, schema)

    # Update local catalog
    update_catalog(args.repo_guid, catalog_path.replace("\\", "/").split("/1/", 1)[-1], import_url, file_url)

    logging.info("Descript uploader completed successfully.")
    sys.exit(0)
