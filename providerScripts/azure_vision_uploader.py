import os
import sys
import json
import argparse
import logging
import random
import time
import requests
from configparser import ConfigParser
import plistlib
from threading import Lock

# Constants
SUPPORTED_API_VERSIONS = ["3.2", "4.0"]
DEFAULT_API_VERSION = "4.0"
VISION_API_VERSION_V4 = "2024-02-01"
EMBEDDING_MODEL_VERSION_DEFAULT = "2023-04-15"
DEFAULT_VISUAL_FEATURES_V4 = ["TAGS", "CAPTION", "DENSE_CAPTIONS", "OBJECTS", "READ", "SMART_CROPS", "PEOPLE"]
DEFAULT_SMART_CROPS_RATIOS = [0.9, 1.33]
DEFAULT_LANGUAGE = "en"
DEFAULT_GENDER_NEUTRAL = False
DEFAULT_ANALYSIS_MODEL_VERSION_V4 = "latest"
DEFAULT_VISUAL_FEATURES_V32 = ["Tags", "Description", "Objects"]
DEFAULT_DETAILS_V32 = ["Celebrities", "Landmarks"]

# Platform
VALID_MODES = ["proxy", "original", "get_base_target","generate_video_proxy","generate_video_frame_proxy","generate_intelligence_proxy","generate_video_to_spritesheet", "analyze_and_embed"]
IS_LINUX = os.path.isdir("/opt/sdna/bin")
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH
NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/azure_vision_metadata_normalizer.py"

# Rate limiting
RATE_LIMIT_DELAY = 0.5
last_request_time = 0
rate_limit_lock = Lock()

logger = logging.getLogger()

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')

def wait_for_rate_limit():
    global last_request_time
    with rate_limit_lock:
        now = time.time()
        elapsed = now - last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        last_request_time = time.time()

def get_retry_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, max_retries=3, **kwargs):
    session = get_retry_session()
    for attempt in range(max_retries):
        try:
            wait_for_rate_limit()
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after + 5)
                continue
            if response.status_code < 500:
                return response
            if attempt == max_retries - 1:
                return response
            time.sleep(5)
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == max_retries - 1:
                raise
            time.sleep([2, 5, 15][attempt] + random.uniform(0, 2))
    return None

def get_config_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            folder = parser.get('General', 'cloudconfigfolder', fallback='').strip()
        else:
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                folder = plistlib.load(fp).get("CloudConfigFolder", "").strip()
        if not folder:
            logging.error("CloudConfigFolder not found in config")
            sys.exit(5)
        return os.path.join(folder, "cloud_targets.conf")
    except Exception as e:
        logging.error(f"Failed to read config: {e}")
        sys.exit(5)

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

def get_advanced_ai_config(config_name, provider_name="azure_vision"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return None

    config_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    api_ver = str(cfg.get("api_version", DEFAULT_API_VERSION)).strip()
                    if api_ver not in SUPPORTED_API_VERSIONS:
                        cfg["api_version"] = DEFAULT_API_VERSION
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load config file {config_file}: {e}")

    sample_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    if os.path.exists(sample_file):
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load sample file {sample_file}: {e}")

    return None

# --- v4.0 functions ---
def _get_v4_sdk():
    from azure.ai.vision.imageanalysis import ImageAnalysisClient
    from azure.ai.vision.imageanalysis.models import VisualFeatures
    from azure.core.credentials import AzureKeyCredential
    return ImageAnalysisClient, VisualFeatures, AzureKeyCredential

def _parse_visual_features_v4(feature_list):
    valid_features = {
        "TAGS": "TAGS",
        "CAPTION": "CAPTION",
        "DENSE_CAPTIONS": "DENSE_CAPTIONS",
        "OBJECTS": "OBJECTS",
        "READ": "READ",
        "SMART_CROPS": "SMART_CROPS",
        "PEOPLE": "PEOPLE"
    }
    ImageAnalysisClient, VisualFeatures, _ = _get_v4_sdk()
    mapping = {
        "TAGS": VisualFeatures.TAGS,
        "CAPTION": VisualFeatures.CAPTION,
        "DENSE_CAPTIONS": VisualFeatures.DENSE_CAPTIONS,
        "OBJECTS": VisualFeatures.OBJECTS,
        "READ": VisualFeatures.READ,
        "SMART_CROPS": VisualFeatures.SMART_CROPS,
        "PEOPLE": VisualFeatures.PEOPLE
    }
    features = []
    for f in feature_list:
        key = f.upper()
        if key in mapping:
            features.append(mapping[key])
    return features if features else [mapping[f] for f in DEFAULT_VISUAL_FEATURES_V4]

def _serialize_v4_result(result):
    output = {}
    if hasattr(result, 'tags') and result.tags:
        output['tags'] = [{"name": t.name, "confidence": t.confidence} for t in result.tags.list]
    if hasattr(result, 'objects') and result.objects:
        output['objects'] = [{
            "tags": [{"name": tag.name, "confidence": tag.confidence} for tag in obj.tags] if hasattr(obj, 'tags') else [],
            "bounding_box": {"x": obj.bounding_box.x, "y": obj.bounding_box.y, "w": obj.bounding_box.width, "h": obj.bounding_box.height}
        } for obj in result.objects.list]
    if hasattr(result, 'caption') and result.caption:
        output['caption'] = {"text": result.caption.text, "confidence": result.caption.confidence}
    if hasattr(result, 'dense_captions') and result.dense_captions:
        output['dense_captions'] = [{
            "text": dc.text,
            "confidence": dc.confidence,
            "bounding_box": {"x": dc.bounding_box.x, "y": dc.bounding_box.y, "w": dc.bounding_box.width, "h": dc.bounding_box.height}
        } for dc in result.dense_captions.list]
    if hasattr(result, 'people') and result.people:
        output['people'] = [{
            "confidence": p.confidence,
            "bounding_box": {"x": p.bounding_box.x, "y": p.bounding_box.y, "w": p.bounding_box.width, "h": p.bounding_box.height}
        } for p in result.people.list]
        output['people_count'] = len(output['people'])
    if hasattr(result, 'read') and result.read:
        output['read'] = [[{
            "text": line.text,
            "bounding_polygon": [{"x": pt.x, "y": pt.y} for pt in line.bounding_polygon]
        } for line in block.lines] for block in result.read.blocks]
    if hasattr(result, 'smart_crops') and result.smart_crops:
        output['smart_crops'] = [{
            "aspect_ratio": crop.aspect_ratio,
            "bounding_box": {"x": crop.bounding_box.x, "y": crop.bounding_box.y, "w": crop.bounding_box.width, "h": crop.bounding_box.height}
        } for crop in result.smart_crops.list]
    if hasattr(result, 'metadata'):
        output['metadata'] = {"width": result.metadata.width, "height": result.metadata.height}
    if hasattr(result, 'model_version'):
        output['model_version'] = result.model_version
    return output

def analyze_image_v4(config, image_data, ai_config):
    visual_features_raw = ai_config.get("visual_features", DEFAULT_VISUAL_FEATURES_V4)
    visual_features = _parse_visual_features_v4(visual_features_raw)
    smart_crops = ai_config.get("smart_crops_aspect_ratios", DEFAULT_SMART_CROPS_RATIOS)
    language = ai_config.get("language", DEFAULT_LANGUAGE)
    gender_neutral = ai_config.get("gender_neutral_caption", DEFAULT_GENDER_NEUTRAL)
    model_version = ai_config.get("analysis_model_version", DEFAULT_ANALYSIS_MODEL_VERSION_V4)
    max_image_size = 20*1024*1024 if model_version == "latest" else 4*1024*1024
    if len(image_data) > max_image_size:
        logger.error(f"Image size {len(image_data)} exceeds max for model {model_version} ({max_image_size})")
        sys.exit(8)

    ImageAnalysisClient, _, AzureKeyCredential = _get_v4_sdk()
    for attempt in range(10):
        try:
            wait_for_rate_limit()
            client = ImageAnalysisClient(endpoint=config.get("vision_endpoint"), credential=AzureKeyCredential(config["vision_key"]))
            result = client.analyze(
                image_data=image_data,
                visual_features=visual_features,
                smart_crops_aspect_ratios=smart_crops,
                language=language,
                gender_neutral_caption=gender_neutral,
                model_version=model_version
            )
            return _serialize_v4_result(result)
        except Exception as e:
            err = str(e).lower()
            if '429' in err or 'rate limit' in err:
                time.sleep(60 + attempt * 10)
            elif 'quota' in err:
                time.sleep(120)
            elif any(kw in err for kw in ['timeout', 'connection', 'network', 'ssl']):
                time.sleep(10 + attempt * 5)
            else:
                if attempt == 9:
                    return None
                time.sleep(5)
    return None

def get_embedding_v4(config, image_data, ai_config):
    emb_model = ai_config.get("embedding_model_version", EMBEDDING_MODEL_VERSION_DEFAULT)
    url = f"{config['vision_endpoint']}/computervision/retrieval:vectorizeImage?api-version={VISION_API_VERSION_V4}&model-version={emb_model}"
    headers = {
        "Content-Type": "application/octet-stream",
        "Ocp-Apim-Subscription-Key": config["vision_key"]
    }
    for attempt in range(10):
        try:
            response = make_request_with_retries("POST", url, data=image_data, headers=headers, max_retries=3)
            if not response:
                if attempt == 9:
                    return None
                time.sleep(10 + attempt * 5)
                continue
            if response.status_code == 200:
                vector = response.json().get("vector")
                if vector:
                    return vector
                if attempt == 9:
                    return None
                time.sleep(5)
                continue
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after + 10)
                continue
            elif response.status_code == 403 and 'quota' in response.text.lower():
                time.sleep(120)
                continue
            elif response.status_code in (400, 401, 404):
                return None
            elif response.status_code >= 500:
                if attempt == 9:
                    return None
                time.sleep(30 + attempt * 10)
                continue
            else:
                if attempt == 9:
                    return None
                time.sleep(20 + attempt * 5)
        except Exception:
            if attempt == 9:
                return None
            time.sleep(15 + attempt * 5)
    return None

# --- v3.2 functions ---
def _map_v4_to_v32_features(v4_features):
    mapping = {
        "TAGS": "Tags", "CAPTION": "Description", "DENSE_CAPTIONS": "Description",
        "OBJECTS": "Objects", "PEOPLE": "Faces", "FACES": "Faces", "BRANDS": "Brands",
        "IMAGETYPE": "ImageType", "COLOR": "Color", "ADULT": "Adult", "CATEGORIES": "Categories"
    }
    v32_set = set()
    for f in v4_features:
        key = f.upper()
        if key in mapping and mapping[key]:
            v32_set.add(mapping[key])
        elif f in ["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Brands", "Objects"]:
            v32_set.add(f)
    return list(v32_set) if v32_set else ["Tags", "Description", "Objects"]

def _build_v32_analyze_url(endpoint, visual_features, details, language):
    base = f"{endpoint}/vision/v3.2/analyze"
    params = [f"visualFeatures={','.join(visual_features)}", f"language={language}"]
    if details:
        params.append(f"details={','.join(details)}")
    return base + "?" + "&".join(params)

def analyze_image_v32(config, image_data, ai_config):
    visual_features_raw = ai_config.get("visual_features", DEFAULT_VISUAL_FEATURES_V32)
    if isinstance(visual_features_raw[0], str) and visual_features_raw[0].isupper():
        visual_features = _map_v4_to_v32_features(visual_features_raw)
    else:
        visual_features = visual_features_raw
    details = ai_config.get("details", DEFAULT_DETAILS_V32)
    language = ai_config.get("language", DEFAULT_LANGUAGE)
    if len(image_data) > 4*1024*1024:
        logger.error("Image size exceeds 4MB limit for v3.2")
        sys.exit(8)

    url = _build_v32_analyze_url(config["vision_endpoint"], visual_features, details, language)
    headers = {
        "Content-Type": "application/octet-stream",
        "Ocp-Apim-Subscription-Key": config["vision_key"]
    }

    for attempt in range(10):
        try:
            response = make_request_with_retries("POST", url, data=image_data, headers=headers, max_retries=3)
            if not response:
                if attempt == 9:
                    return None
                time.sleep(10 + attempt * 5)
                continue
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after + 10)
            elif response.status_code == 403 and 'quota' in response.text.lower():
                time.sleep(120)
            elif response.status_code in (400, 401, 404):
                return None
            elif response.status_code >= 500:
                if attempt == 9:
                    return None
                time.sleep(30 + attempt * 10)
            else:
                if attempt == 9:
                    return None
                time.sleep(20 + attempt * 5)
        except Exception:
            if attempt == 9:
                return None
            time.sleep(15 + attempt * 5)
    return None

# --- Main ---
if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 0.5))

    parser = argparse.ArgumentParser(description="Azure Vision - Unified v3.2 / v4.0")
    parser.add_argument("-m", "--mode", required=True)
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-sp", "--source-path", required=True)
    parser.add_argument("-o", "--output-path", help="Temporary output path for analysis and embedding JSON")
    parser.add_argument("--log-level", default="debug")
    parser.add_argument("--dry-run", action="store_true")
    args, _ = parser.parse_known_args()

    mode = args.mode
    if mode not in VALID_MODES:
        logging.error(f"Invalid mode: {mode}")
        sys.exit(1)

    setup_logging(args.log_level)
    config_path = get_config_path()
    cloud_config = ConfigParser()
    cloud_config.read(config_path)
    if args.config_name not in cloud_config:
        logging.error(f"Config '{args.config_name}' not found.")
        sys.exit(1)

    cloud_config_data = cloud_config[args.config_name]
    if not os.path.isfile(args.source_path):
        logging.error(f"Image not found: {args.source_path}")
        sys.exit(4)

    with open(args.source_path, "rb") as f:
        image_data = f.read()

    if args.dry_run:
        logging.info(f"[DRY RUN] Config: {args.config_name}, Endpoint: {cloud_config_data['vision_endpoint']}")
        sys.exit(0)

    ai_config = get_advanced_ai_config(args.config_name, "azure_vision") or {}
    api_version = ai_config.get("api_version", DEFAULT_API_VERSION)

    if mode == "analyze_and_embed" and api_version == "3.2":
        logging.error("Embedding not supported in API v3.2")
        sys.exit(13)

    if api_version == "4.0":
        analysis = analyze_image_v4(cloud_config_data, image_data, ai_config)
        if not analysis:
            logging.critical("v4.0 analysis failed")
            sys.exit(10)
        embedding = get_embedding_v4(cloud_config_data, image_data, ai_config) if mode == "analyze_and_embed" else None
        if mode == "analyze_and_embed" and not embedding:
            logging.critical("v4.0 embedding failed")
            sys.exit(12)
    else:
        analysis = analyze_image_v32(cloud_config_data, image_data, ai_config)
        if not analysis:
            logging.critical("v3.2 analysis failed")
            sys.exit(10)
        embedding = None

    output = {
        "api_version": api_version,
        "analysis": analysis,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    if embedding is not None:
        output["embedding"] = embedding
        output["embedding_length"] = len(embedding)

    if args.output_path:
        try:
            with open(args.output_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            logging.info(f"Output written to {args.output_path}")
        except Exception as e:
            logging.error(f"Failed to write output: {e}")
            sys.exit(15)

    logging.info("Completed successfully")
    sys.exit(0)