import os
import sys
import json
import ijson
import decimal
import time
import math
import random
import logging
import argparse
import subprocess
import urllib.parse
from pathlib import Path
from threading import Lock
from configparser import ConfigParser
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError, ConnectionError, Timeout
from typing import Dict, Any, List, Optional

# Constants
VALID_MODES = ["original", "proxy", "send_extracted_metadata"]
MAX_RETRIES = 3
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

# Resolve normalizer path
DEFAULT_NORMALIZER = "/opt/sdna/bin/assemblyai_metadata_normalizer.py"
if os.path.exists(DEFAULT_NORMALIZER):
    NORMALIZER_SCRIPT_PATH = DEFAULT_NORMALIZER
else:
    # Fallback to local directory
    NORMALIZER_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assemblyai_metadata_normalizer.py")

SDNA_EVENT_MAP = {
    "assembly_transcript": "transcript",
    "assembly_entities": "entities",
    "assembly_auto_chapters": "chapters",
    "assembly_auto_highlights": "highlights",
    "assembly_iab_categories": "iab_categories",
    "assembly_content_safety": "safety",
    "assembly_summary": "summary"
}

LANGUAGE_CODE_MAP = {
    "en": "Global English", "en_au": "Australian English", "en_uk": "British English",
    "en_us": "US English", "es": "Spanish", "fr": "French", "de": "German",
    "it": "Italian", "pt": "Portuguese", "nl": "Dutch", "hi": "Hindi",
    "ja": "Japanese", "zh": "Chinese", "fi": "Finnish", "ko": "Korean",
    "pl": "Polish", "ru": "Russian", "tr": "Turkish", "uk": "Ukrainian",
    "vi": "Vietnamese", "af": "Afrikaans", "sq": "Albanian",
    "am": "Amharic", "ar": "Arabic", "hy": "Armenian", "as": "Assamese",
    "az": "Azerbaijani", "eu": "Basque", "be": "Belarusian", "bn": "Bengali",
    "bs": "Bosnian", "bg": "Bulgarian", "ca": "Catalan", "hr": "Croatian",
    "cs": "Czech", "da": "Danish", "et": "Estonian", "gl": "Galician",
    "ka": "Georgian", "el": "Greek", "gu": "Gujarati",
    "ht": "Haitian", "ha": "Hausa", "haw": "Hawaiian", "he": "Hebrew",
    "hu": "Hungarian", "is": "Icelandic", "id": "Indonesian",
    "jw": "Javanese", "kn": "Kannada", "kk": "Kazakh",
    "lo": "Lao", "la": "Latin", "lv": "Latvian", "lt": "Lithuanian",
    "lb": "Luxembourgish", "mk": "Macedonian", "mg": "Malagasy",
    "ms": "Malay", "ml": "Malayalam", "mt": "Maltese",
    "mi": "Maori", "mr": "Marathi", "mn": "Mongolian",
    "ne": "Nepali", "no": "Norwegian", "pa": "Panjabi",
    "ps": "Pashto", "fa": "Persian", "ro": "Romanian",
    "sr": "Serbian", "sn": "Shona", "sd": "Sindhi",
    "si": "Sinhala", "sk": "Slovak", "sl": "Slovenian",
    "so": "Somali", "su": "Sundanese", "sw": "Swahili",
    "sv": "Swedish", "tl": "Tagalog", "tg": "Tajik",
    "ta": "Tamil", "te": "Telugu", "ur": "Urdu",
    "uz": "Uzbek", "cy": "Welsh", "yi": "Yiddish",
    "yo": "Yoruba",
}


logger = logging.getLogger()
_LOCAL_SESSION = None
_EXTERNAL_SESSION = None

UNIVERSAL_3_PRO = "universal-3-pro"
UNIVERSAL_2 = "universal-2"

class ValidationError(Exception):
    pass

def validate_transcribe_payload(payload: Dict[str, Any]) -> None:
    errors: List[str] = []

    # Helpers
    def enabled(flag: str) -> bool:
        return bool(payload.get(flag))

    def get_list(name: str) -> List[Any]:
        v = payload.get(name)
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return [v]

    # 1) Mutually exclusive: Summarization vs Auto Chapters
    if enabled("summarization") and enabled("auto_chapters"):
        errors.append("Enable only one of summarization or auto_chapters")

    # 2) Paired parameters: summary_model and summary_type
    has_summary_model = bool(payload.get("summary_model"))
    has_summary_type = bool(payload.get("summary_type"))
    if has_summary_model ^ has_summary_type:
        errors.append("Both summary_model and summary_type must be provided together")

    # 3) Model-gated parameters
    speech_models: List[str] = [m.lower() for m in get_list("speech_models")]
    uses_u3p = any(UNIVERSAL_3_PRO in m for m in speech_models)
    uses_u2 = any(UNIVERSAL_2 in m for m in speech_models)

    # prompt and temperature only if Universal-3-Pro is selected
    if ("prompt" in payload and payload["prompt"]) and not uses_u3p:
        errors.append("prompt is only supported with Universal-3-Pro (include it in speech_models)")
    if ("temperature" in payload and payload["temperature"] is not None) and not uses_u3p:
        errors.append("temperature is only supported with Universal-3-Pro (include it in speech_models)")
    
    # keyterms_prompt capacity differs by model
    if "keyterms_prompt" in payload and payload["keyterms_prompt"]:
        terms = payload["keyterms_prompt"]
        if not isinstance(terms, list):
            errors.append("keyterms_prompt must be a list of terms/phrases")
        else:
            max_terms = 1000 if uses_u3p else (200 if uses_u2 else None)
            if max_terms is not None and len(terms) > max_terms:
                errors.append(f"keyterms_prompt exceeds limit of {max_terms} terms for the selected model(s)")
                
    if payload.get("prompt") and payload.get("keyterms_prompt"):
        errors.append("Both prompt and keyterms_prompt can not be used in the same request")

    # 4) Value limits and validation
    if "content_safety_confidence" in payload and payload["content_safety_confidence"] is not None:
        c = payload["content_safety_confidence"]
        if not isinstance(c, int) or not (25 <= c <= 100):
            errors.append("content_safety_confidence must be an integer between 25 and 100")

    if "speech_threshold" in payload and payload["speech_threshold"] is not None:
        st = payload["speech_threshold"]
        try:
            st_f = float(st)
            if not (0.0 <= st_f <= 1.0):
                errors.append("speech_threshold must be between 0.0 and 1.0 inclusive")
        except (TypeError, ValueError):
            errors.append("speech_threshold must be a number between 0.0 and 1.0 inclusive")

    if "language_confidence_threshold" in payload and payload["language_confidence_threshold"] is not None:
        try:
            float(payload["language_confidence_threshold"])
        except (TypeError, ValueError):
            errors.append("language_confidence_threshold must be numeric")

    # language_codes for code-switching must include 'en'
    if "language_codes" in payload and payload["language_codes"] is not None:
        codes = payload["language_codes"]
        if not isinstance(codes, list) or not codes:
            errors.append("language_codes must be a non-empty list")
        elif "en" not in [str(c).lower() for c in codes]:
            errors.append("language_codes must include 'en'")

    # 5) Partial transcription offsets
    start = payload.get("audio_start_from")
    end = payload.get("audio_end_at")
    if start is not None or end is not None:
        if not (isinstance(start, int) and isinstance(end, int)):
            errors.append("audio_start_from and audio_end_at must both be provided together as integers (milliseconds)")
        else:
            if start < 0 or end < 0:
                errors.append("audio_start_from and audio_end_at must be >= 0")
            if start >= end:
                errors.append("audio_start_from must be less than audio_end_at")

    # 6) Redaction-related rules
    if enabled("redact_pii_audio"):
        quality = payload.get("redact_pii_audio_quality")
        if quality is not None and str(quality).lower() not in {"mp3", "wav"}:
            errors.append("redact_pii_audio_quality must be 'mp3' (default) or 'wav' when redact_pii_audio is enabled")

    # 7) Speech Understanding / Speaker ID
    if "speech_understanding" in payload:
        su = payload["speech_understanding"].get("request", {})
        if "speaker_identification" in su:
            ident = su["speaker_identification"]
            if ident.get("speaker_type") == "role":
                if not ident.get("known_values"):
                    errors.append("If speaker_type is 'role', must provide known_values array")
                else:
                    for val in ident["known_values"]:
                        if len(str(val)) > 35:
                            errors.append(f"known_values entry '{val}' exceeds 35 character limit")

    # 8) Deprecated/no-op safeguards
    for deprecated in ("custom_topics", "topics"):
        if deprecated in payload and payload[deprecated]:
            errors.append(f"{deprecated} has no effect and should not be used")
    if "speech_model" in payload and payload["speech_model"]:
        errors.append("speech_model is deprecated; use speech_models instead")

    if errors:
        raise ValidationError(" | ".join(errors))

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')

def fail(msg, code=7, asset_id=None):
    if code == 7 and asset_id:
        print(f"Metadata extraction failed for asset: {asset_id}")
    else:
        logger.error(msg)
    sys.exit(code)

def get_retry_session(url):
    global _LOCAL_SESSION, _EXTERNAL_SESSION
    is_local = "127.0.0.1" in url or "localhost" in url
    
    if is_local:
        if _LOCAL_SESSION is None:
            _LOCAL_SESSION = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
            _LOCAL_SESSION.mount("http://", adapter)
        return _LOCAL_SESSION
    else:
        if _EXTERNAL_SESSION is None:
            _EXTERNAL_SESSION = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
            _EXTERNAL_SESSION.mount("http://", adapter)
            _EXTERNAL_SESSION.mount("https://", adapter)
        return _EXTERNAL_SESSION

def make_request_with_retries(method, url, max_retries=MAX_RETRIES, stream=False, **kwargs):
    session = get_retry_session(url)
    last_error = None
    for attempt in range(max_retries):
        try:
            response = session.request(method, url, timeout=(10, 30), stream=stream, **kwargs)
            # 429 (Rate Limit) should be retried, 5xx (Server Error) should be retried
            if response.status_code < 500 and response.status_code != 429:
                return response
            
            logger.warning(f"Request to {url} returned {response.status_code}. Attempt {attempt + 1}/{max_retries}...")
            
            if attempt == max_retries - 1:
                return response
            
            # Backoff before retry
            wait_time = [5, 15, 30][attempt % 3] + random.uniform(0, 5)
            time.sleep(wait_time)
        except Exception as e:
            last_error = e
            if attempt == max_retries - 1:
                logger.error(f"Request failed after {max_retries} attempts: {e}")
                return None
            time.sleep([5, 15, 30][attempt % 3] + random.uniform(0, 5))
    return None


def get_link_address_and_port():
    logger.debug(f"Reading server config: {SERVERS_CONF_PATH}")
    ip, port = "", ""
    try:
        with open(SERVERS_CONF_PATH, 'r') as f:
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
            import plistlib
            with open(SERVERS_CONF_PATH, 'rb') as fp:
                data = plistlib.load(fp)
                ip = data.get('link_address', '')
                port = str(data.get('link_port', ''))
    except Exception as e:
        logger.error(f"Error reading {SERVERS_CONF_PATH}: {e}")
        sys.exit(5)
    return ip, port

def get_cloud_config_path():
    if IS_LINUX:
        parser = ConfigParser()
        parser.read(DNA_CLIENT_SERVICES)
        path = parser.get('General', 'cloudconfigfolder', fallback='') + "/cloud_targets.conf"
    else:
        import plistlib
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            data = plistlib.load(fp)
            path = data.get("CloudConfigFolder", "") + "/cloud_targets.conf"
    return path

def get_admin_dropbox_path():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            log_path = parser.get('General', 'LogPath', fallback='').strip()
        else:
            import plistlib
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                cfg = plistlib.load(fp) or {}
                log_path = str(cfg.get("LogPath", "")).strip()
        return log_path or None
    except Exception as e:
        logger.error(f"Error reading admin dropbox: {e}")
        return None

def get_node_api_key():
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            api_key = parser.get('General', 'NodeAPIKey', fallback='').strip()
        else:
            import plistlib
            with open(DNA_CLIENT_SERVICES, 'rb') as fp:
                data = plistlib.load(fp)
                api_key = data.get('NodeAPIKey', '').strip()
        return api_key
    except Exception as e:
        logger.error(f"Failed to read API key: {e}")
        sys.exit(5)

def get_store_paths():
    metadata_store_path, proxy_store_path = "", ""
    try:
        config_path = "/opt/sdna/nginx/ai-config.json" if IS_LINUX else "/Library/Application Support/StorageDNA/nginx/ai-config.json"
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            metadata_store_path = config_data.get("ai_export_shared_drive_path", "")
            proxy_store_path = config_data.get("ai_proxy_shared_drive_path", "")
    except Exception as e:
        logging.error(f"Error reading Metadata Store or Proxy Store settings: {e}")
        sys.exit(5)

    if not metadata_store_path or not proxy_store_path:
        logging.info("Store settings not found.")
        sys.exit(5)

    return metadata_store_path, proxy_store_path

def add_metadata_directory(repo_guid, provider, file_path):
    meta_path, _ = get_store_paths()
    
    # Ensure meta_path is treated as absolute if it looks like one
    if meta_path and not meta_path.startswith("/") and not meta_path.startswith("\\"):
        # If it starts with sdna_fs but no slash, it's likely intended to be /sdna_fs
        if meta_path.startswith("sdna_fs"):
            meta_path = "/" + meta_path
            
    base_meta = meta_path.split("/./")[0] if "/./" in meta_path else meta_path
    meta_right = meta_path.split("/./")[1] if "/./" in meta_path else "metadata"
    base_name = Path(file_path).stem
    metadata_dir = os.path.join(base_meta, meta_right, str(repo_guid), file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)
    return metadata_dir, meta_right, base_name

def get_advanced_ai_config(config_name):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return {}
    
    for src in [
        os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json"),
        os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", "assemblyai.json")
    ]:
        if os.path.exists(src):
            try:
                with open(src, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load {src}: {e}")
    return {}

def send_extracted_metadata_catalog(repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, language_code=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/extendedMetadata"
    node_api_key = get_node_api_key()
    headers = {"apikey": node_api_key, "Content-Type": "application/json"}

    metadata_array = []
    if normMetadataFilePath is not None:
        metadata_array.append({
            "type": "metadataFilePath",
            "path": normMetadataFilePath
        })
    if rawMetadataFilePath is not None:
        metadata_array.append({
            "type": "metadataRawJsonFilePath", 
            "path": rawMetadataFilePath
        })

    payload = {
        "repoGuid": repo_guid,
        "providerName": "ASSEMBLYAI",
        "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default"),
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": metadata_array
        }]
    }
    
    for attempt in range(max_attempts):
        r = None
        try:
            r = make_request_with_retries("POST", url, headers=headers, json=payload)
            if r and r.status_code in (200, 201):
                logger.debug(f"Metadata sent: {r.status_code}")
                return True
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_attempts} error: {e}")
        
        if attempt < max_attempts - 1:
            time.sleep([1, 3, 10][attempt] + random.uniform(0, 1))
    
    return False

def get_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path, language=None):
    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logger.error(f"Normalizer not found: {NORMALIZER_SCRIPT_PATH}")
        return False
    
    try:
        cmd = ["python3", NORMALIZER_SCRIPT_PATH, "-i", raw_metadata_file_path, "-o", norm_metadata_file_path]
        if language:
            cmd.extend(["-l", language])
            
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        if process.returncode == 0:
            return True
        else:
            logger.error(f"Normalizer failed with return code {process.returncode}")
            if process.stderr:
                logger.error(f"Normalizer Stderr: {process.stderr.strip()}")
            if process.stdout:
                logger.debug(f"Normalizer Stdout: {process.stdout.strip()}")
            return False
    except Exception as e:
        logger.error(f"Exception during normalization execution: {e}")
        return False

def transform_normlized_to_enriched(norm_metadata_file_path, filetype_prefix):
    try:
        if not os.path.exists(norm_metadata_file_path):
            logger.error(f"Normalized metadata file not found at {norm_metadata_file_path}")
            yield "File not found", False
            return
            
        file_size = os.path.getsize(norm_metadata_file_path)

        if file_size < 10 * 1024 * 1024:
            chunk_size_bytes = 1 * 1024 * 1024
        elif file_size < 100 * 1024 * 1024:
            chunk_size_bytes = 4 * 1024 * 1024
        elif file_size < 512 * 1024 * 1024:
            chunk_size_bytes = 8 * 1024 * 1024
        else:
            chunk_size_bytes = 16 * 1024 * 1024

        def build_event(event_type, sdna_event_type, event_value, occurrences):
            return {
                "eventType": event_type,
                "sdnaEventType": sdna_event_type,
                "sdnaEventTypePrefix": filetype_prefix,
                "eventValue": event_value,
                "totalOccurrences": len(occurrences),
                "eventOccurence": occurrences
            }

        with open(norm_metadata_file_path, "rb") as f:
            chunk = []
            chunk_bytes = 0
            for event_type, detections in ijson.kvitems(f, ""):
                sdna_event_type = SDNA_EVENT_MAP.get(event_type, "unknown")
                if isinstance(detections, list):
                    for d in detections:
                        record = build_event(
                            event_type,
                            sdna_event_type,
                            d.get("value", "Unidentified"),
                            d.get("occurrences", [])
                        )
                        chunk.append(record)
                        record_str = json.dumps(record, default=lambda x: float(x) if isinstance(x, decimal.Decimal) else str(x))
                        chunk_bytes += len(record_str.encode())
                        if chunk_bytes >= chunk_size_bytes:
                            yield chunk, True
                            chunk, chunk_bytes = [], 0
                else:
                    logger.warning(f"Skipping '{event_type}': unexpected type {type(detections).__name__}")

            if chunk:
                yield chunk, True

    except Exception as e:
        logger.error(f"Error transforming normalized metadata: {e}")
        yield f"Error during transformation: {e}", False

def send_ai_enriched_metadata(config, repo_guid, file_path, enriched_metadata_chunks, language_code=None, max_attempts=3):
    url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
    headers = {"apikey": get_node_api_key(), "Content-Type": "application/json"}
    base_payload = {
        "repoGuid": repo_guid,
        "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
        "fileName": os.path.basename(file_path),
        "providerName": config.get("provider", "ASSEMBLYAI"),
        "sourceLanguage": LANGUAGE_CODE_MAP.get(language_code, "Default"),
    }
    delays = [(1, 1), (3, 1), (10, 5)]
    all_succeeded = True

    for chunk, ok in enriched_metadata_chunks:
        if not ok:
            logger.error(f"Skipping bad chunk: {chunk}")
            all_succeeded = False
            continue
        for attempt in range(max_attempts):
            try:
                payload = {**base_payload, "normalizedMetadata": chunk}
                r = make_request_with_retries(
                    "POST", 
                    url, 
                    headers=headers, 
                    json=json.loads(json.dumps(payload, default=lambda x: float(x) if isinstance(x, decimal.Decimal) else str(x)))
                )
                if r and r.status_code in (200, 201):
                    break
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.critical(f"Failed to send chunk after {max_attempts} attempts: {e}")
                    all_succeeded = False
                    break
                base_delay, jitter = delays[attempt]
                time.sleep(base_delay + random.uniform(0, jitter))

    return all_succeeded

class AssemblyAiProcessor:
    def __init__(self, api_key, config=None):
        self.api_key = api_key
        self.config = config or {}
        self.base_url = "https://api.assemblyai.com/v2"
        self.llm_gateway_url = "https://llm-gateway.assemblyai.com/v1"

    def upload_file(self, file_path):
        url = f"{self.base_url}/upload"
        headers = {
            "Authorization": self.api_key
        }
        
        def read_file(filepath, chunk_size=5242880):
            with open(filepath, 'rb') as _file:
                while True:
                    data = _file.read(chunk_size)
                    if not data:
                        break
                    yield data

        try:
            logger.info("Uploading file to AssemblyAI proxy...")
            response = make_request_with_retries("POST", url, data=read_file(file_path), headers=headers, max_retries=1)
            response.raise_for_status()
            upload_url = response.json()["upload_url"]
            logger.info(f"File uploaded successfully.")
            return upload_url
        except Exception as e:
            logger.error(f"Failed to upload file to AssemblyAI: {e}")
            return None

    def build_transcript_payload(self, audio_url):
        payload = {"audio_url": audio_url}

        def get_lang_detection():
            if "language_detection" in self.config:
                if isinstance(self.config["language_detection"], dict):
                    return self.config["language_detection"].get("language_detection", False)
                return self.config.get("language_detection", False)
            return False
        
        payload["speech_models"] = self.config.get("speech_models", ["universal-3-pro", "universal-2"])
        
        if "language_code" in self.config and not get_lang_detection():
            payload["language_code"] = self.config["language_code"]

        if "language_detection" in self.config:
            lang_cfg = self.config["language_detection"]
            if isinstance(lang_cfg, dict):
                payload["language_detection"] = lang_cfg.get("language_detection", False)
                if "language_detection_options" in lang_cfg:
                    payload["language_detection_options"] = lang_cfg["language_detection_options"]
                elif payload["language_detection"]:
                    payload["language_detection_options"] = {
                        "code_switching": True,
                        "expected_languages": ["all"]
                    }
            else:
                payload["language_detection"] = bool(lang_cfg)
                if payload["language_detection"]:
                    payload["language_detection_options"] = {
                        "code_switching": True,
                        "expected_languages": ["all"]
                    }
        
        for param in ["audio_end_at", "audio_start_from", "speech_threshold", "temperature", "prompt"]:
            if param in self.config:
                payload[param] = self.config[param]
                
        for array_param in ["keyterms_prompt", "language_codes"]:
            if array_param in self.config:
                payload[array_param] = self.config[array_param]

        for wh_param in ["webhook_url", "webhook_auth_header_name", "webhook_auth_header_value"]:
             if wh_param in self.config:
                 payload[wh_param] = self.config[wh_param]

        if "formatting" in self.config:
            fmt = self.config["formatting"]
            if "format_text" in fmt: payload["format_text"] = fmt["format_text"]
            if "punctuate" in fmt: payload["punctuate"] = fmt["punctuate"]
            if "disfluencies" in fmt: payload["disfluencies"] = fmt["disfluencies"]
            if "filter_profanity" in fmt: payload["filter_profanity"] = fmt["filter_profanity"]
            if "custom_spelling" in fmt: payload["custom_spelling"] = fmt["custom_spelling"]
            if "multichannel" in fmt: payload["multichannel"] = fmt["multichannel"]
            
        if "redact_pii" in self.config:
            p_redact = self.config["redact_pii"]
            if isinstance(p_redact, dict):
                if "redact_pii" in p_redact: payload["redact_pii"] = p_redact["redact_pii"]
                if "redact_pii_audio" in p_redact: payload["redact_pii_audio"] = p_redact["redact_pii_audio"]
                if "redact_pii_audio_quality" in p_redact: payload["redact_pii_audio_quality"] = p_redact["redact_pii_audio_quality"]
                if "redact_pii_policies" in p_redact: payload["redact_pii_policies"] = p_redact["redact_pii_policies"]
                if "redact_pii_sub" in p_redact: payload["redact_pii_sub"] = p_redact["redact_pii_sub"]
                if "redact_pii_audio_options" in p_redact: payload["redact_pii_audio_options"] = p_redact["redact_pii_audio_options"]
            else:
                 payload["redact_pii"] = bool(p_redact)
                 
        if "audio_intelligence" in self.config:
            ai = self.config["audio_intelligence"]
            if "speaker_labels" in ai: payload["speaker_labels"] = ai["speaker_labels"]
            if "speakers_expected" in ai and ai.get("speakers_expected"): payload["speakers_expected"] = ai["speakers_expected"]
            if "speaker_options" in ai: payload["speaker_options"] = ai["speaker_options"]
            if "auto_chapters" in ai: payload["auto_chapters"] = ai["auto_chapters"]
            if "sentiment_analysis" in ai: payload["sentiment_analysis"] = ai["sentiment_analysis"]
            if "entity_detection" in ai: payload["entity_detection"] = ai["entity_detection"]
            if "iab_categories" in ai: payload["iab_categories"] = ai["iab_categories"]
            if "auto_highlights" in ai: payload["auto_highlights"] = ai["auto_highlights"]
            
        if "summarization" in self.config:
            summ = self.config["summarization"]
            if "summarization" in summ: payload["summarization"] = summ["summarization"]
            if "summary_model" in summ: payload["summary_model"] = summ["summary_model"]
            if "summary_type" in summ: payload["summary_type"] = summ["summary_type"]
            
        if "content_safety" in self.config:
            cs = self.config["content_safety"]
            if "content_safety" in cs: payload["content_safety"] = cs["content_safety"]
            if "content_safety_confidence" in cs: payload["content_safety_confidence"] = cs["content_safety_confidence"]

        # Handle Speech Understanding in the initial request
        if "speech_understanding" in self.config:
            su_cfg = self.config["speech_understanding"]
            su_request = {}
            
            if "translation" in su_cfg:
                trans = su_cfg["translation"].copy()
                trans["match_original_utterance"] = True
                su_request["translation"] = trans
            
            if "speaker_identification" in su_cfg:
                su_request["speaker_identification"] = su_cfg["speaker_identification"]
            
            if "custom_formatting" in su_cfg:
                su_request["custom_formatting"] = su_cfg["custom_formatting"]
            
            if su_request:
                payload["speech_understanding"] = {"request": su_request}

        return payload

    def start_transcription(self, audio_url):
        url = f"{self.base_url}/transcript"
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
        
        payload = self.build_transcript_payload(audio_url)

        try:
            validate_transcribe_payload(payload)
        except ValidationError as e:
            logger.error(f"Advanced Configuration Invalid: {str(e)}")
            return None

        response = make_request_with_retries("POST", url, headers=headers, json=payload)
        if response and response.status_code == 200:
            return response.json()["id"]
        logger.error(f"Failed to start transcription: {response.text if response else 'No response'}")
        return None

    def poll_transcript(self, transcript_id, timeout_minutes=15):
        url = f"{self.base_url}/transcript/{transcript_id}"
        headers = {"Authorization": self.api_key}
        
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        poll_interval = 10
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                logger.error("Polling timeout exceeded.")
                return "timeout", None
                
            response = make_request_with_retries("GET", url, headers=headers)
            if response and response.status_code == 200:
                data = response.json()
                print(f"Transcript {transcript_id} response: {data}")
                status = data.get("status")
                
                if status == "completed":
                    return "completed", data
                elif status == "error":
                    logger.error(f"Transcription failed: {data.get('error')}")
                    return "error", data
                elif status == "processing":
                    logger.debug(f"Transcript {transcript_id} status: {status}")
            
            time.sleep(poll_interval)
            
    def get_transcript(self, transcript_id):
        url = f"{self.base_url}/transcript/{transcript_id}"
        headers = {"Authorization": self.api_key}
        response = make_request_with_retries("GET", url, headers=headers)
        if response and response.status_code == 200:
            return response.json()
        return None

    def save_transcript_to_file(self, transcript_id, output_path):
        url = f"{self.base_url}/transcript/{transcript_id}"
        headers = {"Authorization": self.api_key}
        response = make_request_with_retries("GET", url, headers=headers, stream=True)
        if response and response.status_code == 200:
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        return False

    def split_multilingual_transcript_native(self, transcript_data, target_languages, metadata_dir, base_name):
        logger.info(f"Splitting native transcript for languages: {target_languages}")
        base_transcript = transcript_data.copy()
        base_transcript.pop('utterances', None)
        base_transcript.pop('text', None)
        base_transcript.pop('words', None)
        base_transcript.pop('translated_texts', None)

        try:
            for lang in target_languages:
                lang_raw_name = f"{base_name}_{lang}_raw.json"
                lang_raw_path = os.path.join(metadata_dir, lang_raw_name)
                virtual_transcript = base_transcript.copy()
                virtual_transcript['language_code'] = lang
                virtual_transcript['utterances'] = []
                virtual_transcript['text'] = ""
                
                all_translated = transcript_data.get('translated_texts', {})
                if isinstance(all_translated, dict):
                    virtual_transcript['text'] = all_translated.get(lang, "")
                
                utterances = transcript_data.get('utterances', []) or []
                for utt in utterances:
                    lang_utt = utt.copy()
                    lang_utt['original_text'] = utt.get('text', "")
                    translations = utt.get('translated_texts', {})
                    if lang in translations:
                        lang_utt['text'] = translations[lang]
                        lang_utt['language_code'] = lang
                    lang_utt.pop('translated_texts', None)
                    lang_utt.pop('words', None)
                    virtual_transcript['utterances'].append(lang_utt)
                
                with open(lang_raw_path, 'w', encoding='utf-8') as f:
                    json.dump(virtual_transcript, f, ensure_ascii=False, indent=4)
                logger.debug(f"Created virtual transcript for {lang} at {lang_raw_path}")
            return True
        except Exception as e:
            logger.error(f"Error splitting native transcript: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description="AssemblyAI Uploader")
    parser.add_argument("-m", "--mode", required=True, choices=VALID_MODES)
    parser.add_argument("-c", "--config-name", required=True, help="name of config")
    parser.add_argument("-sp", "--source-path", help="path to source file")
    parser.add_argument("-cp", "--catalog-path", required=True, help="path to catalog file")
    parser.add_argument("-mp", "--metadata-file", help="path where property bag for file resides")
    parser.add_argument("-r", "--repo-guid", required=True, help="repo guid")
    parser.add_argument("-j", "--job-guid", help="Job GUID")
    parser.add_argument("-id", "--asset-id", help="Asset ID passed from coordinator")
    parser.add_argument("--enrich-prefix", help="Prefix for sdnaEventType of AI enrich data")
    parser.add_argument("--export-ai-metadata", help="Export AI metadata using Rekognition")
    parser.add_argument("--controller-address", help="Override IP:Port")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.dry_run:
        logger.info("[DRY RUN] Skipped")
        sys.exit(0)

    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        fail(f"Cloud config not found: {cloud_config_path}", 1)

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        fail(f"Config section not found: {args.config_name}",1)

    api_key = cloud_config[args.config_name].get("api_key")
    if not api_key:
        fail("api_key not found in config",1)

    advanced_config = get_advanced_ai_config(args.config_name)
    processor = AssemblyAiProcessor(api_key, advanced_config)
    
    catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
    metadata_dir, meta_right, base_name = add_metadata_directory(args.repo_guid, "ASSEMBLYAI", catalog_path_clean)
    tracking_file_path = os.path.join(metadata_dir, f"{base_name}_tracking.json")

    job_tracker = {}

    if args.mode in ["original", "proxy"]:
        audio_url = processor.upload_file(args.source_path)
        if not audio_url:
            fail("Failed to upload audio to AssemblyAI proxy.", 1)

        logger.info("Starting transcription job...")
        master_job_id = processor.start_transcription(audio_url)
        if not master_job_id:
            fail("Failed to start master transcription job.", 1)
        
        job_tracker["default"] = {"id": master_job_id, "status": "queued", "target_language": "Default"}
        with open(tracking_file_path, "w") as f:
            json.dump(job_tracker, f, indent=4)
        logger.info(f"Tracking file saved: {tracking_file_path}")

    elif args.mode == "send_extracted_metadata":
        if not os.path.exists(tracking_file_path):
            fail(f"Tracking file not found: {tracking_file_path}", 1, asset_id=args.asset_id)
        with open(tracking_file_path, "r") as f:
            job_tracker = json.load(f)
            print(f"Loaded job tracker: {job_tracker}")
        
        master_info = job_tracker.get("default")
        if not master_info:
            fail("Master job is missing from tracker", 7, args.asset_id)
        master_job_id = master_info["id"]

    # Shared Logic: Poll and Process result if not already completed locally
    master_info = job_tracker.get("default")
    status = master_info.get("status")
    data = None

    if status != "completed":
        logger.info(f"Master job {master_job_id} status is '{status}'. Polling/Checking...")
        status, data = processor.poll_transcript(master_job_id, timeout_minutes=15)
        job_tracker["default"]["status"] = status
        
        if status == "timeout" or status == "error":
            with open(tracking_file_path, "w") as f:
                json.dump(job_tracker, f, indent=4)
            fail(f"Master job {status}", code=7, asset_id=args.asset_id or master_job_id)
        
        if status == "completed":
            raw_master_name = f"{base_name}_raw.json"
            raw_master_path = os.path.join(metadata_dir, raw_master_name)
            
            su_resp = data.get("speech_understanding", {}).get("response", {})
            mapping = su_resp.get("speaker_identification", {}).get("mapping", {})
            if mapping:
                logger.info(f"Applying speaker identification mapping: {mapping}")
                if "utterances" in data and data["utterances"]:
                    for utt in data["utterances"]:
                        orig_spk = utt.get("speaker")
                        if orig_spk in mapping:
                            utt["speaker"] = mapping[orig_spk]
            
            fmt_text = su_resp.get("custom_formatting", {}).get("formatted_text")
            if fmt_text:
                data["text"] = fmt_text

            with open(raw_master_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            job_tracker["default"]["raw_path"] = raw_master_path
            
            target_languages = []
            if "speech_understanding" in advanced_config:
                su_cfg = advanced_config["speech_understanding"]
                target_languages = su_cfg.get("translation", {}).get("target_languages", [])
                
            for lang in target_languages:
                job_tracker[lang] = {"id": f"native_{lang}_{master_job_id}", "status": "processing", "target_language": lang}

            if target_languages:
                if processor.split_multilingual_transcript_native(data, target_languages, metadata_dir, base_name):
                    for lang in target_languages:
                        job_tracker[lang].update({
                            "status": "completed",
                            "raw_path": os.path.join(metadata_dir, f"{base_name}_{lang}_raw.json")
                        })
                    logger.info("Native translations split successfully.")
                else:
                    for lang in target_languages: job_tracker[lang]["status"] = "error"
                    fail("Native transcript splitting failed", 7, args.asset_id)

            # Final tracker save for the processing run
            with open(tracking_file_path, "w") as f:
                save_tracker = {k: {
                    "id": v["id"], 
                    "status": v["status"], 
                    "target_language": v["target_language"],
                    "raw_path": v.get("raw_path")
                } for k, v in job_tracker.items()}
                json.dump(save_tracker, f, indent=4)


    combined_raw_data = {"insights": {}, "multilanguage_insights": {}}
    for key, job_info in list(job_tracker.items()):
        raw_json_path = job_info.get("raw_path")
        if not raw_json_path or not os.path.exists(raw_json_path):
            continue
        lang_code = job_info["target_language"]
        raw_base_name = os.path.basename(raw_json_path)
        norm_base_name = raw_base_name.replace("_raw.json", "_norm.json")
        norm_json_path = os.path.join(metadata_dir, norm_base_name)
        with open(raw_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if key == "default": combined_raw_data["insights"] = data
        else: combined_raw_data["multilanguage_insights"][key] = data
        
        if get_normalized_metadata(raw_json_path, norm_json_path, language=lang_code if key != "default" else None):
            norm_return_path = os.path.join(meta_right, str(args.repo_guid), catalog_path_clean, "ASSEMBLYAI", norm_base_name)
            raw_return_path = os.path.join(meta_right, str(args.repo_guid), catalog_path_clean, "ASSEMBLYAI", raw_base_name)
            send_extracted_metadata_catalog(args.repo_guid, catalog_path_clean, raw_return_path, norm_return_path, language_code=lang_code if key != "default" else None)
            if args.export_ai_metadata and args.export_ai_metadata.lower() == 'true':
                enrich_prefix = args.enrich_prefix if args.enrich_prefix else "aud"
                enriched_chunks_generator = transform_normlized_to_enriched(norm_json_path, enrich_prefix)
                send_ai_enriched_metadata({"provider": "ASSEMBLYAI"}, args.repo_guid, catalog_path_clean, enriched_chunks_generator, language_code=lang_code if key != "default" else None)
        else:
            fail("Failed to normalize metadata", 7, args.asset_id)

    combined_raw_json_path = os.path.join(metadata_dir, f"{base_name}_combined_raw.json")
    with open(combined_raw_json_path, "w", encoding="utf-8") as f:
        json.dump(combined_raw_data, f, ensure_ascii=False, indent=4)
    logger.info("AssemblyAI Processing completed successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()