import os
import sys
import json
import boto3
import argparse
import logging
import urllib.parse
from configparser import ConfigParser
import xml.etree.ElementTree as ET
import plistlib
import requests
import random
import time
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout

# Constants
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet", "analyze_and_embed"]
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"

NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/rekognition_metadata_normalizer.py"

# Detect platform
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

def extract_file_name(path):
    return os.path.basename(path)

def remove_file_name_from_path(path):
    return os.path.dirname(path)

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

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        #read opt/sdna/nginx/ai-config.json file to get "ai_proxy_shared_drive_path" and ai_export_shared_drive_path":
        config_path = "/opt/sdna/nginx/ai-config.json" if IS_LINUX else "/Library/Application Support/StorageDNA/nginx/ai-config.json"
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            metadata_store_path = config_data.get("ai_export_shared_drive_path", "")
            proxy_store_path = config_data.get("ai_proxy_shared_drive_path", "")
            logging.info(f"Metadata Store Path: {metadata_store_path}, Proxy Store Path: {proxy_store_path}")
        
    except Exception as e:
        logging.error(f"Error reading Metadata Store or Proxy Store settings: {e}")
        sys.exit(5)

    if not metadata_store_path or not proxy_store_path:
        logging.info("Store settings not found.")
        sys.exit(5)

    return metadata_store_path, proxy_store_path

def get_admin_dropbox_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            log_path = parser.get('General', 'LogPath', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                log_path = str(cfg.get("LogPath", "")).strip()
        return log_path or None
    except Exception as e:
        logging.error(f"Error reading admin dropbox path: {e}")
        return None

def upload_file_to_s3(config_data, bucket_name, source_path, upload_path, metadata=None):
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectionError, EndpointConnectionError, ReadTimeoutError

    # Define retry logic via boto3 config
    retry_config = Config(
        retries={
            'max_attempts': 5,
            'mode': 'adaptive'  # or 'standard'
        },
        connect_timeout=15,
        read_timeout=30,
    )

    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    if 'region' in config_data:
        client_kwargs['region_name'] = config_data['region']
    if 'endpoint_url' in config_data:
        client_kwargs['endpoint_url'] = config_data['endpoint_url']

    try:
        s3 = boto3.client(**client_kwargs)

        upload_path = upload_path.lstrip('/')

        sanitized_metadata = None
        if metadata:
            sanitized_metadata = {k.lower().replace(' ', '-'): str(v) for k, v in metadata.items()}

        # Perform upload with extra retry resilience
        s3.upload_file(
            Filename=source_path,
            Bucket=bucket_name,
            Key=upload_path,
            ExtraArgs={'Metadata': sanitized_metadata} if sanitized_metadata else {}
        )

        logging.info(f"Successfully uploaded {source_path} to s3://{bucket_name}/{upload_path}")
        return {
            "status": 200,
            "detail": "Uploaded asset successfully"
        }

    except (ClientError, EndpointConnectionError, ReadTimeoutError, ConnectionError) as e:
        error_code = e.response['Error']['Code'] if isinstance(e, ClientError) else "Network/Timeout"
        if isinstance(e, ClientError) and e.response['Error']['Code'].startswith('4'):
            # Client error — don't retry
            logging.error(f"Client error during S3 upload: {e}")
            return {"status": 400, "detail": str(e)}
        logging.error(f"Transient S3 error: {e}")
        return {"status": 500, "detail": f"Transient S3 failure: {str(e)}"}

    except Exception as e:
        logging.error(f"Unexpected error during S3 upload: {e}")
        return {"status": 500, "detail": str(e)}

def remove_file_from_s3(bucket_name, file_key, config_data):
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectionError, EndpointConnectionError, ReadTimeoutError

    # Define retry logic via boto3 config
    retry_config = Config(
        retries={
            'max_attempts': 5,
            'mode': 'adaptive'  # or 'standard'
        },
        connect_timeout=15,
        read_timeout=30,
    )

    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': config_data['access_key_id'],
        'aws_secret_access_key': config_data['secret_access_key'],
        'config': retry_config
    }
    if 'region' in config_data:
        client_kwargs['region_name'] = config_data['region']
    if 'endpoint_url' in config_data:
        client_kwargs['endpoint_url'] = config_data['endpoint_url']
    logging.debug(f"S3 client configuration for deletion: {client_kwargs}")
    try:
        s3 = boto3.client(**client_kwargs)

        file_key = file_key.lstrip("/").replace("\\", "/")

        s3.delete_object(Bucket=bucket_name, Key=file_key)

        logging.info(f"Successfully deleted s3://{bucket_name}/{file_key}")
        return True

    except (ClientError, EndpointConnectionError, ReadTimeoutError, ConnectionError) as e:
        logging.error(f"Error during S3 file deletion: {e}")
        return False

    except Exception as e:
        logging.error(f"Unexpected error during S3 file deletion: {e}")
        return False

def get_retry_session(retries=3, backoff_factor_range=(1.0, 2.0)):
    session = requests.Session()
    # handling retry delays manually via backoff in the range
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, **kwargs):
    session = get_retry_session()
    last_exception = None

    for attempt in range(3):  # Max 3 attempts
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            else:
                logging.warning(f"Received {response.status_code} from {url}. Retrying...")
        except (SSLError, ConnectionError, Timeout) as e:
            last_exception = e
            if attempt < 2:  # Only sleep if not last attempt
                base_delay = [1, 3, 10][attempt]
                jitter = random.uniform(0, 1)
                delay = base_delay + jitter * ([1, 1, 5][attempt])  # e.g., 1-2, 3-4, 10-15
                logging.warning(f"Attempt {attempt + 1} failed due to {type(e).__name__}: {e}. "
                                f"Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {url}. Last error: {e}")
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

    if last_exception:
        raise last_exception
    return None  # Should not reach here

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

def update_catalog(repo_guid, file_path, upload_path, bucket, max_attempts=5):
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
        "providerName": cloud_config_data.get("provider", "s3"),
        "providerData": {
            "bucket": bucket,
            "s3_key": upload_path,
        }
    }
    for attempt in range(max_attempts):
        try:
            logging.debug(f"[Attempt {attempt+1}/{max_attempts}] Starting catalog update request to {url}")
            response = make_request_with_retries("POST", url, headers=headers, json=payload)
            if response is None:
                logging.error(f"[Attempt {attempt+1}] Response is None!")
                time.sleep(5)
                continue
            try:
                resp_json = response.json() if response.text.strip() else {}
            except Exception as e:
                logging.warning(f"Failed to parse response JSON: {e}")
                resp_json = {}
            if response.status_code in (200, 201):
                logging.info("Catalog updated successfully.")
                return True
            # --- Handle 404 explicitly ---
            if response.status_code == 404:
                logging.info("[404 DETECTED] Entering 404 handling block")
                message = resp_json.get('message', '')
                logging.debug(f"[404] Raw message from response: [{repr(message)}]")
                clean_message = message.strip().lower()
                logging.debug(f"[404] Cleaned message: [{repr(clean_message)}]")

                if clean_message == "catalog item not found":
                    wait_time = 60 + (attempt * 10)
                    logging.warning(f"[404] Known 'not found' case. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"[404] Unexpected message: {message}")
                    break  # non-retryable 404
            else:
                logging.warning(f"[Attempt {attempt+1}] Non-404 error status: {response.status_code}")

        except Exception as e:
            logging.exception(f"[Attempt {attempt+1}] Unexpected exception in update_catalog: {e}")
        # Default retry delay for non-404 or unhandled cases
        if attempt < max_attempts - 1:
            fallback_delay = 5 + attempt * 2
            logging.debug(f"Sleeping {fallback_delay}s before next attempt")
            time.sleep(fallback_delay)

    pass

def get_advanced_ai_config(config_name, provider_name="aws"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return None

    # Normalize provider name to lowercase for matching
    provider_key = provider_name.lower()

    # Try config file, then sample
    for src in [
        os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json"),
        os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    ]:
        if os.path.exists(src):
            try:
                with open(src, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                    if not isinstance(raw, dict):
                        continue

                    # Build normalized config
                    out = {}
                    for key, val in raw.items():
                        # Match keys with or without "provider_" prefix
                        clean_key = key
                        if key.startswith(f"{provider_key}_"):
                            clean_key = key[len(provider_key) + 1:]
                        # Also accept bare keys like "image_rekognition_features"
                        if clean_key.startswith("image_rekognition_") or clean_key in {
                            "rekognition_region", "video_rekognition_enabled"
                        }:
                            out[clean_key] = val

                    # Normalize boolean fields
                    for bool_key in ["image_rekognition_enabled", "video_rekognition_enabled"]:
                        if bool_key in out:
                            v = out[bool_key]
                            out[bool_key] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes", "on")

                    # Normalize features list
                    if "image_rekognition_features" in out:
                        feats = out["image_rekognition_features"]
                        if isinstance(feats, str):
                            feats = [x.strip() for x in feats.split(",") if x.strip()]
                        # Uppercase each feature (e.g., "label_detection" → "LABEL_DETECTION")
                        out["image_rekognition_features"] = [
                            f.upper() if isinstance(f, str) else f for f in feats
                        ]

                    # Normalize numeric fields
                    for num_key in ["image_rekognition_labels_min_confidence", "image_rekognition_max_labels"]:
                        if num_key in out and isinstance(out[num_key], str):
                            try:
                                out[num_key] = float(out[num_key]) if '.' in out[num_key] else int(out[num_key])
                            except ValueError:
                                del out[num_key]  # fallback to default

                    logging.info(f"Loaded and normalized advanced AI config from: {src}")
                    return out

            except Exception as e:
                logging.error(f"Failed to load {src}: {e}")

    return None

def prepare_metadata_to_upload( backlink_url, properties_file = None):    
    metadata = {
        "fabric URL": backlink_url
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

def get_commands_from_features(features):
    commands = []
    feature_map = {
        "LABEL_DETECTION": "detect_labels",
        "FACE_DETECTION": "detect_faces",
        "TEXT_DETECTION": "detect_text",
        "CONTENT_MODERATION": "detect_moderation_labels",
        "CELEBRITY_RECOGNITION": "recognize_celebrities",
        "SAFETY_EQUIPMENT_ANALYSIS": "detect_protective_equipment"
    }
    for feature in features:
        if feature in feature_map:
            commands.append(feature_map[feature])
    return commands

def get_image_rekogniton_data(image_input, aws_access_key, aws_secret_key, features, region="us-west-1", is_s3_uri=True):
    try:
        logging.debug(f"Image Rekognition API call with features: {features}, mode: {'S3' if is_s3_uri else 'bytes'}")
        rekognition = boto3.client(
            'rekognition',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        rekogniton_metadata = {}
        for command in get_commands_from_features(features):
            logging.debug(f"Executing Rekognition command: {command}")
            func = getattr(rekognition, command, None)
            if not func:
                logging.warning(f"Rekognition command {command} not found.")
                continue

            if is_s3_uri:
                if not isinstance(image_input, str) or not image_input.startswith("s3://"):
                    raise ValueError("Expected S3 URI but got invalid input")
                bucket_name, filepath = image_input[5:].split('/', 1)
                image_param = {'S3Object': {'Bucket': bucket_name, 'Name': filepath}}
            else:
                if not isinstance(image_input, bytes):
                    raise ValueError("Expected raw image bytes for direct analysis")
                image_param = {'Bytes': image_input}

            response = func(Image=image_param)
            rekogniton_metadata[command] = response

        return rekogniton_metadata

    except Exception as e:
        logging.error(f"Error in Rekognition API call: {e}")
        return {}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", required=True, help="mode of Operation proxy or original upload")
    parser.add_argument("-c", "--config-name", required=True, help="name of cloud configuration")
    parser.add_argument("-j", "--job-guid", help="Job Guid of SDNA job")
    parser.add_argument("-b", "--bucket-name", required=True, help= "Name of Bucket")
    parser.add_argument("-cp", "--catalog-path", help="Path where catalog resides")
    parser.add_argument("-sp", "--source-path", required=True, help="Source path of file to look for original upload")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-up", "--upload-path", help="Path where file will be uploaded to frameIO")
    parser.add_argument("-sl", "--size-limit", help="source file size limit for original file upload")
    parser.add_argument("-r", "--repo-guid", help="Repository GUID for catalog update")
    parser.add_argument("-o", "--output-path", help="Path where output will be saved")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--controller-address",help="Link IP/Hostname Port")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata using Rekognition")
    args = parser.parse_args()

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Only allowed modes: {VALID_MODES}")
        sys.exit(1)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        logging.error(f"Missing cloud config: {cloud_config_path}")
        sys.exit(1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    cloud_config_name = args.config_name
    if cloud_config_name not in cloud_config:
        logging.error(f"Missing cloud config section: {cloud_config_name}")
        sys.exit(1)

    cloud_config_data = cloud_config[cloud_config_name]
    
    if args.export_ai_metadata:
        cloud_config_data["export_ai_metadata"] = "true" if args.export_ai_metadata.lower() == "true" else "false"

    # Load advanced AI settings
    provider_name = cloud_config_data.get("provider", "s3")
    advanced_ai_settings = get_advanced_ai_config(args.config_name, provider_name)
    if not advanced_ai_settings:
        logging.warning("No advanced AI settings available. AI metadata extraction will not happen.")

    logging.info(f"Starting S3 upload process in {mode} mode")
    logging.debug(f"Using cloud config: {cloud_config_path}")
    logging.debug(f"Source path: {args.source_path}")
    logging.debug(f"Upload path: {args.upload_path}")
    
    matched_file = args.source_path
    catalog_path = args.catalog_path

    if not os.path.exists(matched_file):
        logging.error(f"File not found: {matched_file}")
        sys.exit(4)

    matched_file_size = os.stat(matched_file).st_size
    file_size_limit = args.size_limit
    if mode == "original" and file_size_limit:
        try:
            size_limit_bytes = float(file_size_limit) * 1024 * 1024
            if matched_file_size > size_limit_bytes:
                logging.error(f"File too large: {matched_file_size / (1024 * 1024):.2f} MB > limit of {file_size_limit} MB")
                sys.exit(4)
        except Exception as e:
            logging.warning(f"Could not validate size limit: {e}")

    backlink_url = ""
    clean_catalog_path = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1] if args.catalog_path else ""
    if args.upload_path and args.catalog_path:
        file_name_for_url = extract_file_name(matched_file) if mode == "original" else extract_file_name(catalog_path)
        catalog_path = remove_file_name_from_path(args.catalog_path)
        normalized_path = catalog_path.replace("\\", "/")
        if "/1/" in normalized_path:
            relative_path = normalized_path.split("/1/", 1)[-1]
        else:
            relative_path = normalized_path
        catalog_url = urllib.parse.quote(relative_path)
        filename_enc = urllib.parse.quote(file_name_for_url)
        job_guid = args.job_guid

        if args.controller_address is not None and len(args.controller_address.split(":")) == 2:
            client_ip, client_port = args.controller_address.split(":")
        else:
            client_ip, client_port = get_link_address_and_port()

        backlink_url = f"https://{client_ip}/dashboard/projects/{job_guid}/browse&search?path={catalog_url}&filename={filename_enc}"
        logging.debug(f"Generated dashboard URL: {backlink_url}")

    if mode == "analyze_and_embed":
        logging.info("Running in 'analyze_and_embed' mode")
        
        if not args.output_path:
            logging.error("--output-path is required in analyze_and_embed mode")
            sys.exit(1)

        # Load AI config
        ai_config = get_advanced_ai_config(args.config_name, cloud_config_data.get("provider","rekognition")) or {}
        features = ai_config.get("rekognition_features", [
            "LABEL_DETECTION", "FACE_DETECTION", "CONTENT_MODERATION",
            "TEXT_DETECTION", "SAFETY_EQUIPMENT_ANALYSIS", "CELEBRITY_RECOGNITION"
        ])
        region = cloud_config_data.get("region", "us-east-1")
        MAX_DIRECT_SIZE = 4 * 1024 * 1024  # 4 MB

        if matched_file_size <= MAX_DIRECT_SIZE:
            logging.info("File ≤ 4 MB: using direct byte upload to Rekognition")
            try:
                with open(args.source_path, "rb") as f:
                    image_bytes = f.read()
            except Exception as e:
                logging.error(f"Failed to read image file: {e}")
                sys.exit(1)

            ai_metadata = get_image_rekogniton_data(
                image_input=image_bytes,
                aws_access_key=cloud_config_data['access_key_id'],
                aws_secret_key=cloud_config_data['secret_access_key'],
                features=features,
                region=region,
                is_s3_uri=False
            )
        else:
            # Large file: upload to S3 first
            logging.info("File > 4 MB: uploading to S3, then analyzing")
            upload_path = args.upload_path
            response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, upload_path)
            if response["status"] != 200:
                logging.error(f"Failed to upload large file to S3: {response['detail']}")
                sys.exit(1)

            s3_uri = f"s3://{args.bucket_name}/{upload_path.lstrip('/').replace(chr(92), '/')}"
            ai_metadata = get_image_rekogniton_data(
                image_input=s3_uri,
                aws_access_key=cloud_config_data['access_key_id'],
                aws_secret_key=cloud_config_data['secret_access_key'],
                features=features,
                region=region,
                is_s3_uri=True
            )

            # Clean up S3 object after successful analysis
            if ai_metadata:
                if remove_file_from_s3(args.bucket_name, upload_path, cloud_config_data):
                    logging.info("Temporary S3 object deleted after analysis")
                else:
                    logging.warning("Failed to delete temporary S3 object")

        # Save output
        if ai_metadata:
            try:
                with open(args.output_path, "w", encoding="utf-8") as f:
                    json.dump(ai_metadata, f, indent=2, ensure_ascii=False)
                logging.info(f"AI metadata saved to {args.output_path}")
            except Exception as e:
                logging.error(f"Failed to write output file: {e}")
                sys.exit(15)
        else:
            logging.error("No AI metadata generated")
            sys.exit(16)

        sys.exit(0)

    if args.dry_run:
        logging.info("[DRY RUN] Upload skipped.")
        logging.info(f"[DRY RUN] File to upload: {matched_file}")
        logging.info(f"[DRY RUN] Upload path: {args.upload_path} => S3")
        meta_file = args.metadata_file
        if meta_file:
            logging.info(f"[DRY RUN] Metadata would be applied from: {meta_file}")
        else:
            logging.warning("[DRY RUN] Metadata upload enabled but no metadata file specified.")
        sys.exit(0)

    logging.info(f"Starting upload process to S3")
    upload_path = args.upload_path
    
    #  upload file along with meatadata
    meta_file = args.metadata_file
    logging.info("Preparing metadata to be uploaded ...")
    metadata_obj = prepare_metadata_to_upload(backlink_url, meta_file)
    if metadata_obj is not None:
        parsed = metadata_obj
    else:
        parsed = None
        logging.error("Failed to find metadata .")
    
    #  upload file
    response = upload_file_to_s3(cloud_config_data, args.bucket_name, args.source_path, args.upload_path, parsed)
    if response["status"] != 200:
        print(f"Failed to upload file parts: {response['detail']}")
        sys.exit(1)
    if args.repo_guid and args.catalog_path:
        logging.info("Updating catalog with new S3 file location...")
        update_catalog(args.repo_guid, clean_catalog_path, args.upload_path, args.bucket_name)

    logging.info("All parts uploaded successfully to S3")
    sys.exit(0)