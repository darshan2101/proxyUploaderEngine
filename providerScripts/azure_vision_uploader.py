import os
import sys
import json
import argparse
import logging
import random
import time
import requests
from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from azure.core.credentials import AzureKeyCredential
from configparser import ConfigParser
import plistlib
from threading import Lock

# Constants (used as fallbacks and safety nets)
VISION_API_VERSION = "2024-02-01"
EMBEDDING_MODEL_VERSION = "2023-04-15"
DEFAULT_VISUAL_FEATURES = ["TAGS", "CAPTION", "DENSE_CAPTIONS", "OBJECTS", "READ", "SMART_CROPS", "PEOPLE"]
DEFAULT_SMART_CROPS_RATIOS = [0.9, 1.33]
DEFAULT_LANGUAGE = "en"
DEFAULT_GENDER_NEUTRAL = False
DEFAULT_ANALYSIS_MODEL_VERSION = "2024-02-01"

# Detect platform
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Rate limiting
RATE_LIMIT_DELAY = 3.5
last_request_time = 0
rate_limit_lock = Lock()

def setup_logging(level):
    logging.basicConfig(level=getattr(logging, level.upper()), format='%(asctime)s %(levelname)s: %(message)s')

def wait_for_rate_limit():
    global last_request_time
    with rate_limit_lock:
        now = time.time()
        elapsed = now - last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - elapsed
            logging.debug(f"Rate limit: waiting {sleep_time:.2f}s")
            time.sleep(sleep_time)
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
                wait_time = retry_after + 5
                logging.warning(f"[429] Rate limited. Waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            if response.status_code < 500:
                return response
            logging.warning(f"[SERVER ERROR {response.status_code}] Retrying...")
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                raise
            delay = [2, 5, 15][attempt] + random.uniform(0, 2)
            logging.warning(f"[{type(e).__name__}] Retrying in {delay:.1f}s...")
            time.sleep(delay)
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
        logging.info("Admin dropbox not available; using defaults")
        return None

    # Try config-specific file
    config_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded advanced config: {config_file}")
                    return cfg
                else:
                    logging.error(f"Config file {config_file} is not a JSON object")
        except Exception as e:
            logging.error(f"Failed to load config file {config_file}: {e}")

    # Fallback to provider sample
    sample_file = os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider_name}.json")
    if os.path.exists(sample_file):
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
                if isinstance(cfg, dict):
                    logging.info(f"Loaded sample config: {sample_file}")
                    return cfg
        except Exception as e:
            logging.error(f"Failed to load sample file {sample_file}: {e}")

    logging.info("No valid advanced config found; using defaults")
    return None

def load_config(section_name, config_path):
    if not os.path.isfile(config_path):
        logging.error(f"Config not found: {config_path}")
        sys.exit(1)
    parser = ConfigParser()
    parser.read(config_path)
    if section_name not in parser:
        logging.error(f"Section '{section_name}' not found")
        sys.exit(1)
    section = parser[section_name]
    endpoint = section.get("vision_endpoint", "").strip().rstrip('/')
    key = section.get("vision_key", "").strip()
    if not endpoint or not key:
        logging.error("Missing endpoint or key")
        sys.exit(5)
    return {"endpoint": endpoint, "key": key}

def parse_visual_features(feature_list):
    valid_features = {
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
        f = f.upper()
        if f in valid_features:
            features.append(valid_features[f])
        else:
            logging.warning(f"Ignoring invalid visual feature: {f}")
    return features if features else [valid_features[f] for f in DEFAULT_VISUAL_FEATURES]

def serialize_result(result):
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

def analyze_image(config, image_data, ai_config, max_retries=10):
    # Resolve analysis parameters
    visual_features_raw = ai_config.get("visual_features", DEFAULT_VISUAL_FEATURES)
    visual_features = parse_visual_features(visual_features_raw)
    smart_crops = ai_config.get("smart_crops_aspect_ratios", DEFAULT_SMART_CROPS_RATIOS)
    language = ai_config.get("language", DEFAULT_LANGUAGE)
    gender_neutral = ai_config.get("gender_neutral_caption", DEFAULT_GENDER_NEUTRAL)
    model_version = ai_config.get("analysis_model_version", DEFAULT_ANALYSIS_MODEL_VERSION)
    
    for attempt in range(max_retries):
        try:
            wait_for_rate_limit()
            client = ImageAnalysisClient(endpoint=config["endpoint"], credential=AzureKeyCredential(config["key"]))
            result = client.analyze(
                image_data=image_data,
                visual_features=visual_features,
                smart_crops_aspect_ratios=smart_crops,
                language=language,
                gender_neutral_caption=gender_neutral,
                model_version=model_version
            )
            logging.info("Image analysis completed")
            return serialize_result(result)
        except Exception as e:
            error_msg = str(e).lower()
            if '429' in error_msg or 'rate limit' in error_msg:
                wait_time = 60 + (attempt * 10)
                logging.warning(f"[ANALYSIS 429] Attempt {attempt+1}, waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            if 'quota' in error_msg:
                logging.warning(f"[ANALYSIS QUOTA] Waiting 120s")
                time.sleep(120)
                continue
            if any(kw in error_msg for kw in ['timeout', 'connection', 'network', 'ssl']):
                wait_time = 10 + (attempt * 5)
                logging.warning(f"[ANALYSIS NETWORK] {type(e).__name__}, retry in {wait_time}s")
                time.sleep(wait_time)
                continue
            logging.error(f"[ANALYSIS FAILED] {type(e).__name__}: {e}")
            return None
    logging.error("[ANALYSIS] All retries exhausted")
    return None

def get_embedding(config, image_data, ai_config, max_retries=10):
    # Use constant fallback for embedding model version
    emb_model = ai_config.get("embedding_model_version", EMBEDDING_MODEL_VERSION)
    url = f"{config['endpoint']}/computervision/retrieval:vectorizeImage?api-version={VISION_API_VERSION}&model-version={emb_model}"
    headers = {
        "Content-Type": "application/octet-stream",
        "Ocp-Apim-Subscription-Key": config["key"]
    }
    for attempt in range(max_retries):
        try:
            response = make_request_with_retries("POST", url, data=image_data, headers=headers, max_retries=3)
            if not response:
                if attempt < max_retries - 1:
                    time.sleep(10 + attempt * 5)
                    continue
                return None
            if response.status_code == 200:
                vector = response.json().get("vector")
                if vector:
                    logging.info(f"Embedding generated (length: {len(vector)})")
                    return vector
                logging.error("200 OK but no vector")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                return None
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                wait_time = retry_after + 10
                logging.warning(f"[EMBEDDING 429] Waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            elif response.status_code == 403 and 'quota' in response.text.lower():
                logging.warning(f"[EMBEDDING QUOTA] Waiting 120s")
                time.sleep(120)
                continue
            elif response.status_code in (400, 401, 404):
                logging.error(f"[EMBEDDING CLIENT ERROR {response.status_code}] {response.text[:200]}")
                return None
            elif response.status_code >= 500:
                if attempt < max_retries - 1:
                    wait_time = 30 + (attempt * 10)
                    logging.warning(f"[EMBEDDING SERVER ERROR {response.status_code}] Retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                return None
            else:
                logging.warning(f"[EMBEDDING UNKNOWN {response.status_code}] Retrying...")
                if attempt < max_retries - 1:
                    time.sleep(20 + attempt * 5)
                    continue
                return None
        except Exception as e:
            logging.error(f"[EMBEDDING EXCEPTION] {type(e).__name__}: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(15 + attempt * 5)
    return None

def main():
    parser = argparse.ArgumentParser(description="Azure Vision - Analysis & Embeddings")
    parser.add_argument("-m", "--mode", required=True, choices=["analyze_and_embed"])
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-i", "--image-path", required=True)
    parser.add_argument("-o", "--output-path", required=True)
    parser.add_argument("--log-level", default="info", choices=['debug', 'info', 'warning', 'error'])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    setup_logging(args.log_level)
    config_path = get_config_path()
    config = load_config(args.config_name, config_path)

    if not os.path.isfile(args.image_path):
        logging.error(f"Image not found: {args.image_path}")
        sys.exit(4)

    with open(args.image_path, "rb") as f:
        image_data = f.read()

    logging.info(f"Loaded image: {args.image_path} ({len(image_data)} bytes)")

    if args.dry_run:
        logging.info(f"[DRY RUN] Config: {args.config_name}, Endpoint: {config['endpoint']}")
        sys.exit(0)

    # Load advanced AI config or use defaults
    ai_config = get_advanced_ai_config(args.config_name, "azure_vision") or {}

    analysis = analyze_image(config, image_data, ai_config)
    if not analysis:
        logging.critical("Analysis failed")
        sys.exit(10)

    embedding = get_embedding(config, image_data, ai_config)
    if not embedding:
        logging.critical("Embedding failed")
        sys.exit(12)

    output = {
        "analysis": analysis,
        "embedding": embedding,
        "embedding_length": len(embedding),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(args.output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logging.info(f"Output saved: {args.output_path}")
    sys.exit(0)

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 0.5))
    main()