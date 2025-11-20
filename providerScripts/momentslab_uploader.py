import requests
import os
import sys
import json
import argparse
import logging
import plistlib
import urllib.parse
from configparser import ConfigParser
import random
import magic
import time
import uuid
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
import xml.etree.ElementTree as ET
from action_functions import flatten_dict

# --- IMPORTS FOR S3 ---
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    logging.critical("boto3 is required for S3 upload. Install it via 'pip install boto3'.")
    sys.exit(1)

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet"]
CHUNK_SIZE = 20 * 1024 * 1024 
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
CONFLICT_RESOLUTION_MODES = ["skip", "overwrite"]
DEFAULT_CONFLICT_RESOLUTION = "skip"
DOMAIN = "https://api.momentslab.com"
UPLOAD_ENDPOINTS = ["/ingest-request", "/analysis-request"]

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Initialize logger
logger = logging.getLogger()

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')
    logging.info(f"Log level set to: {level.upper()}")

def get_link_address_and_port():
    logging.debug(f"Reading server configuration from: {SERVERS_CONF_PATH}")
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH) as f:
            lines = f.readlines()
            logging.debug(f"Successfully read {len(lines)} lines from config")

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
        logging.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)

    logging.info(f"Server connection details - Address: {ip}, Port: {port}")
    return ip, port

def get_cloud_config_path():
    logging.debug("Determining cloud config path based on platform")
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
    api_key = ""
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            api_key = parser.get('General', 'NodeAPIKey', fallback='')
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                api_key = plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as e:
        logging.error(f"Error reading Node API key: {e}")
        sys.exit(5)

    if not api_key:
        logging.error("Node API key not found in configuration.")
        sys.exit(5)

    logging.info("Successfully retrieved Node API key.")
    return api_key


def get_retry_session(retries=3, backoff_factor_range=(1.0, 2.0)):
    session = requests.Session()
    # handling retry delays manually via backoff in the range
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, timeout=(10, 30), **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):
        try:
            # If timeout passed in kwargs, use it, else default
            req_timeout = kwargs.pop("timeout", timeout)
            response = session.request(method, url, timeout=req_timeout, **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None

def upload_to_s3_and_get_presigned_url(
    file_path,
    bucket,
    region,
    aws_access_key_id,
    aws_secret_access_key,
    expiration=3600
):
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    file_name = os.path.basename(file_path)
    s3_key = f"uploads/{uuid.uuid4().hex}_{file_name}"

    try:
        logging.info(f"Uploading {file_path} to s3://{bucket}/{s3_key}")
        s3_client.upload_file(file_path, bucket, s3_key)
        logging.info("S3 upload successful.")

        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expiration
        )
        return presigned_url, bucket, s3_key  # <-- return key for cleanup

    except (ClientError, NoCredentialsError) as e:
        logging.error(f"S3 upload or URL generation failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during S3 upload: {e}")
        raise

def delete_s3_object(bucket, key, aws_access_key_id, aws_secret_access_key, region):
    """Delete an S3 object after successful processing."""
    try:
        s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logging.info(f"Cleaned up S3 object: s3://{bucket}/{key}")
    except Exception as e:
        logging.warning(f"Failed to delete S3 object s3://{bucket}/{key}: {e}")

def prepare_metadata_to_upload(repo_guid ,relative_path, file_name, file_size, backlink_url, properties_file = None):    
    metadata = {
        "relativePath": relative_path if relative_path.startswith("/") else "/" + relative_path,
        "repoGuid": repo_guid,
        "fileName": file_name,
        "fabric-URL": backlink_url,
        "fabric_size": file_size
    }
    
    if not properties_file or not os.path.exists(properties_file):
        logging.error(f"Properties file not found: {properties_file}")
        return metadata
    
    logging.debug(f"Reading properties from: {properties_file}")
    file_ext = properties_file.lower()
    try:
        if file_ext.endswith(".json"):
            with open(properties_file, 'r') as f:
                metadata = json.load(f)
                logging.debug(f"Loaded JSON properties: {metadata}")

        elif file_ext.endswith(".xml"):
            tree = ET.parse(properties_file)
            root = tree.getroot()
            metadata_node = root.find("meta-data")
            if metadata_node is not None:
                for data_node in metadata_node.findall("data"):
                    key = data_node.get("name")
                    value = data_node.text.strip() if data_node.text else ""
                    if key:
                        metadata[key] = value
                logging.debug(f"Loaded XML properties: {metadata}")
            else:
                logging.error("No <meta-data> section found in XML.")
        else:
            with open(properties_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        metadata[key] = value
                logging.debug(f"Loaded CSV properties: {metadata}")
    except Exception as e:
        logging.error(f"Failed to parse metadata file: {e}")
    
    return metadata

def upload_file(source_filename, presigned_url, type, token, metadata):
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Nb-Workspace": "ml-api-euw1"
    }
    logging.info(f"Uploading via presigned URL: {presigned_url}")

    original_file_name = metadata.get("fileName", "unknown")
    name_part, ext_part = os.path.splitext(source_filename)
    sanitized_name_part = name_part.strip().replace(" ", "_")
    file_name = sanitized_name_part + ext_part

    url = f"{DOMAIN}/analysis-request"
    payload = {
        "type": type,
        "external_id": str(uuid.uuid4()),
        "filename": file_name,
        "title": original_file_name,
        "source": {
            "type": "URL",
            "url": presigned_url
        },
        "analysis_parameters": {
            "tasks": ["mxt"]
        },
        "metadata": [
            {
                "field_uid": "text_field_1",
                "value": backlink_url
            }
        ]
    }
    logging.debug(f"Upload payload: {payload}")

    for attempt in range(3):
        try:
            response = make_request_with_retries("POST", url, json=payload, headers=headers, timeout=None)
            if response and response.status_code in (200, 201):
                logging.info("Video upload request to MomentsLab successful.")
                return response.json()
            else:
                logging.error(f"MomentsLab upload failed: {response.status_code} {response.text if response else 'No response'}")
        except Exception as e:
            if attempt == 2:
                logging.critical(f"Failed to upload to MomentsLab after 3 attempts: {e}")
                return None
            delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return None

def poll_task_status(token, task_id, max_wait=1200, interval=30):
    url = f"{DOMAIN.strip()}/analysis-request/{task_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Nb-Workspace": "ml-api-euw1"
    }
    logging.info(f"Polling task {task_id}. Max wait: {max_wait}s")
    start_time = time.time()
    attempt = 0

    while time.time() - start_time < max_wait:
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if not response:
                raise ConnectionError("No response")

            # Handle non-200
            if response.status_code != 200:
                if attempt < 2:
                    delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
                    time.sleep(delay)
                    attempt += 1
                    continue
                logging.error(f"Polling failed with HTTP {response.status_code}")
                return False

            data = response.json()
            media_id = data.get("media_id")
            error_msg = data.get("errorMessage")

            if media_id is not None:
                logging.info(f"Media ready. ID: {media_id}")
                return data

            if error_msg is not None or data.get("status") == "FAILED":
                logging.error(f"Analysis Job failed: {error_msg or 'Unknown error'}")
                return data

            attempt = 0
            time.sleep(interval)

        except Exception as e:
            logging.debug(f"Exception: {e}")
            if attempt < 2:
                delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
                time.sleep(delay)
                attempt += 1
            else:
                logging.error(f"Polling failed after retries: {e}")
                return False

    logging.critical(f"Polling timed out after {max_wait}s")
    return False

def update_catalog(repo_guid, file_path, media_id, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/providerData"
    # Read NodeAPIKey from client services config
    node_api_key = get_node_api_key()
    headers = {
        "apikey": node_api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "repoGuid": repo_guid,
        "fileName": os.path.basename(file_path),
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "providerName": "momentslab",
        "providerData": {
            "assetId": media_id
        }
    }

    for attempt in range(max_attempts):
        try:
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response and response.status_code in (200, 201):
                logging.info(f"Catalog updated successfully: {response.text}")
                return True
            else:
                logging.warning(f"Catalog update failed (status {response.status_code if response else 'No response'}): {response.text if response else ''}")
        except Exception as e:
            logging.warning(f"Catalog update attempt {attempt + 1} failed: {e}")
        # External retry backoff
        if attempt < max_attempts - 1:
            delay = [1, 3, 10][attempt] + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    logging.error("Catalog update failed after retries.")
    pass

def get_ai_metadata(token, media_id):
    url = f"{DOMAIN.strip()}/media/{media_id}/analysis"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Nb-Workspace": "ml-api-euw1"
    }
    for attempt in range(3):
        try:
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code == 200:
                data = response.json()
                logging.info(f"AI metadata retrieved for media ID {media_id}")
                return data
            else:
                logging.error(f"Failed to retrieve AI metadata: {response.status_code if response else 'No response'} {response.text if response else ''}")
        except Exception as e:
            logging.error(f"Error retrieving AI metadata: {e}")
            if attempt == 2:
                logging.critical(f"Failed to get access token after 3 attempts: {e}")
                return None
            base_delay = [1, 3, 10][attempt]
            delay = base_delay + random.uniform(0, [1, 1, 5][attempt])
            time.sleep(delay)
    return {}

def send_extracted_metadata(config, repo_guid, file_path, metadata, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}
    payload = {
        "repoGuid": repo_guid,
        "providerName": config.get("provider", "tessact"),
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": flatten_dict(metadata)
        }]
    }
    for _ in range(max_attempts):
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                return True
        except Exception as e:
            logging.warning(f"Metadata send error: {e}")
    return False

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 1.5))

    parser = argparse.ArgumentParser(description="Upload video to Momentslab via S3")
    parser.add_argument("-m", "--mode", required=True, help="Mode: proxy, original, get_base_target, etc.")
    parser.add_argument("-c", "--config-name", required=True, help="Name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job GUID of SDNA job")
    parser.add_argument("-id", "--asset-id", help="Asset ID for metadata operations")
    parser.add_argument("--parent-id", help="Optional parent folder ID for path resolution")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", help="Source file path for upload")
    parser.add_argument("-mp", "--metadata-file", help="Path to metadata file (JSON/CSV/XML)")
    parser.add_argument("-up", "--upload-path", help="Target path or ID in MomentsLab")
    parser.add_argument("-sl", "--size-limit", help="File size limit in MB for original upload")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("--dry-run", action="store_true", help="Perform dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level (debug, info, warning, error)")
    parser.add_argument("--resolved-upload-id", action="store_true", help="Treat upload-path as resolved folder ID")
    parser.add_argument("--controller-address", help="Controller IP:Port override")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata")

    args = parser.parse_args()
    setup_logging(args.log_level)

    # Validate mode
    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode. Use one of: {VALID_MODES}")
        sys.exit(1)

    # Load cloud config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logger.error(f"Cloud config not found: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        logger.error(f"Config section not found: {args.config_name}")
        sys.exit(1)

    section = cloud_config[args.config_name]
    
    token = section.get("token", "").strip()
    if not token:
        logger.critical("Cannot proceed without access token.")
        sys.exit(1)
        
    section = cloud_config[args.config_name]
    if args.export_ai_metadata:
        section["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"
    
    # Read and validate S3 configuration from config section
    s3_bucket = section.get("s3_bucket", "").strip()
    s3_region = section.get("s3_region", "us-east-1").strip()
    aws_access_key_id = section.get("aws_access_key_id", "").strip()
    aws_secret_access_key = section.get("aws_secret_access_key", "").strip()

    if not all([s3_bucket, aws_access_key_id, aws_secret_access_key]):
        logger.critical("Missing required S3 configuration in cloud_targets.conf section: "
                        "s3_bucket, aws_access_key_id, aws_secret_access_key")
        sys.exit(1)
    
    if args.mode == "send_extracted_metadata":
        if not (args.asset_id and args.repo_guid and args.catalog_path):
            logging.error("Asset ID, Repo GUID, and Catalog path required.")
            sys.exit(1)

        ai_metadata = get_ai_metadata(token, args.asset_id)
        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        if ai_metadata:
            if send_extracted_metadata(section, args.repo_guid, catalog_path_clean, ai_metadata):
                logger.info("Extracted metadata sent successfully.")
                sys.exit(0)
            else:
                logger.error("Failed to send extracted metadata.")
                sys.exit(7)
        else:
            print(f"Metadata extraction failed for asset: {args.asset_id}")
            sys.exit(7)
    
    matched_file = args.source_path
    if not matched_file or not os.path.exists(matched_file):
        logger.error(f"Source file not found: {matched_file}")
        sys.exit(4)

    # Size limit
    if mode == "original" and args.size_limit:
        try:
            limit_bytes = float(args.size_limit) * 1024 * 1024
            file_size = os.path.getsize(matched_file)
            if file_size > limit_bytes:
                logging.error(f"File too large: {file_size / 1024 / 1024:.2f} MB > {args.size_limit} MB")
                sys.exit(4)
        except ValueError:
            logging.warning(f"Invalid size limit: {args.size_limit}")

    # Backlink URL
    catalog_path = args.catalog_path or matched_file
    rel_path = remove_file_name_from_path(catalog_path).replace("\\", "/")
    rel_path = rel_path.split("/1/", 1)[-1] if "/1/" in rel_path else rel_path
    catalog_url = urllib.parse.quote(rel_path)
    file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
    filename_enc = urllib.parse.quote(file_name_for_url)
    job_guid = args.job_guid or ""

    if args.controller_address and ":" in args.controller_address:
        client_ip, _ = args.controller_address.split(":", 1)
    else:
        ip, _ = get_link_address_and_port()
        client_ip = ip

    backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
    logging.debug(f"Generated backlink URL: {backlink_url}")

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File: {matched_file}")
        if args.metadata_file:
            logging.info(f"[DRY RUN] Metadata will be applied from: {args.metadata_file}")
        sys.exit(0)
        
    # Prepare metadata
    try:
        metadata_obj = prepare_metadata_to_upload(
            repo_guid=args.repo_guid,
            relative_path=catalog_url,
            file_name=file_name_for_url,
            file_size=os.path.getsize(matched_file),
            backlink_url=backlink_url,
            properties_file=args.metadata_file
        )
        logging.info("Metadata prepared successfully.")
    except Exception as e:
        logging.error(f"Failed to prepare metadata: {e}")
        metadata_obj = {}

    try:
        # Determine upload type
        mime_type = magic.from_file(args.source_path, mime=True)
        if not mime_type:
            logging.critical(f"Could not detect MIME type for '{extract_file_name(args.source_path)}'")
            sys.exit(1)
        if "video" in mime_type:
            upload_type = "video"
        elif "image" in mime_type:
            upload_type = "image"
        else:
            logging.critical(f"Unsupported MIME type for upload: {mime_type}")
            sys.exit(1)

        # --- Upload to S3 and get presigned URL ---
        presigned_url, s3_bucket_used, s3_key_used = upload_to_s3_and_get_presigned_url(
            file_path=matched_file,
            bucket=s3_bucket,
            region=s3_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            expiration=3600
        )

        # Upload to MomentsLab using presigned URL
        response = upload_file(extract_file_name(args.source_path), presigned_url, upload_type, token, metadata_obj)
        logging.debug(f"Upload response: {response}")
        
        if not response:
            logger.error("Upload to MomentsLab failed.")
            sys.exit(1)

        task_id = response.get("analysis_request_id")
        if not task_id:
            logging.error("No analysis_request_id in response.")
            sys.exit(1)

        # Poll for completion
        poll_result = poll_task_status(token, task_id)
        if not poll_result or "media_id" not in poll_result:
            logging.error("Task polling failed or timed out.")
            sys.exit(1)
            
        media_id = poll_result.get("media_id")
        if media_id:
            delete_s3_object(
                bucket=s3_bucket_used,
                key=s3_key_used,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region=s3_region
            )
            # Update local catalog
            clean_catalog_path = catalog_path.replace("\\", "/").split("/1/", 1)[-1]
            catalog_success = update_catalog(args.repo_guid, clean_catalog_path, media_id)
            if not catalog_success:
                logger.error("Catalog update failed.")
        
        if section.get("export_ai_metadata") == "true" and media_id:
            ai_metadata = get_ai_metadata(token, media_id)

            if ai_metadata:
                if send_extracted_metadata(section, args.repo_guid, clean_catalog_path, ai_metadata):
                    logger.info("Extracted metadata sent successfully.")
                else:
                    logger.error("Failed to send extracted metadata.")
                    print(f"Metadata extraction failed for asset: {media_id}")
                    sys.exit(7)
            else:
                print(f"Metadata extraction failed for asset: {media_id}")
                sys.exit(7)
    except Exception as e:
        logger.critical(f"Operation failed: {e}")
        print(f"Error: {str(e)}")
        sys.exit(1)
