import os
import sys
import json
import math
import time
import random
import logging
import argparse
import subprocess
import urllib.parse
from pathlib import Path
from threading import Lock
from configparser import ConfigParser
from datetime import datetime
from typing import Optional, Tuple, List
from openai import OpenAI
import requests
from requests.adapters import HTTPAdapter

# Constants
VALID_MODES = ["original", "proxy", "send_extracted_metadata"]
DEFAULT_CHUNK_DURATION = 300
RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 3
LINUX_CONFIG_PATH = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
SERVERS_CONF_PATH = "/etc/StorageDNA/Servers.conf" if os.path.isdir("/opt/sdna/bin") else "/Library/Preferences/com.storagedna.Servers.plist"
IS_LINUX = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH
NORMALIZER_SCRIPT_PATH = "/opt/sdna/bin/whisper_metadata_normalizer.py"

# SDNA Event Map
SDNA_EVENT_MAP = {
    "whisper_transcript": "transcript",
    "whisper_full_text": "full_text",
    "whisper_keywords": "keywords"
}

# Language support
WHISPER_LANGUAGES = {
    "en": "English", "zh": "Chinese", "de": "German", "es": "Spanish", 
    "ru": "Russian", "ko": "Korean", "fr": "French", "ja": "Japanese",
    "pt": "Portuguese", "tr": "Turkish", "pl": "Polish", "ca": "Catalan",
    "nl": "Dutch", "ar": "Arabic", "sv": "Swedish", "it": "Italian",
    "id": "Indonesian", "hi": "Hindi", "fi": "Finnish", "vi": "Vietnamese",
    "he": "Hebrew", "uk": "Ukrainian", "el": "Greek", "ms": "Malay",
    "cs": "Czech", "ro": "Romanian", "da": "Danish", "hu": "Hungarian",
    "ta": "Tamil", "no": "Norwegian", "th": "Thai", "ur": "Urdu",
    "hr": "Croatian", "bg": "Bulgarian", "lt": "Lithuanian", "la": "Latin",
    "mi": "Maori", "ml": "Malayalam", "cy": "Welsh", "sk": "Slovak",
    "te": "Telugu", "fa": "Persian", "lv": "Latvian", "bn": "Bengali",
    "sr": "Serbian", "az": "Azerbaijani", "sl": "Slovenian", "kn": "Kannada",
    "et": "Estonian", "mk": "Macedonian", "br": "Breton", "eu": "Basque",
    "is": "Icelandic", "hy": "Armenian", "ne": "Nepali", "mn": "Mongolian",
    "bs": "Bosnian", "kk": "Kazakh", "sq": "Albanian", "sw": "Swahili",
    "gl": "Galician", "mr": "Marathi", "pa": "Punjabi", "si": "Sinhala",
    "km": "Khmer", "sn": "Shona", "yo": "Yoruba", "so": "Somali",
    "af": "Afrikaans", "oc": "Occitan", "ka": "Georgian", "be": "Belarusian",
    "tg": "Tajik", "sd": "Sindhi", "gu": "Gujarati", "am": "Amharic",
    "yi": "Yiddish", "lo": "Lao", "uz": "Uzbek", "fo": "Faroese",
    "ht": "Haitian Creole", "ps": "Pashto", "tk": "Turkmen", "nn": "Nynorsk",
    "mt": "Maltese", "sa": "Sanskrit", "lb": "Luxembourgish", "my": "Myanmar",
    "bo": "Tibetan", "tl": "Tagalog", "mg": "Malagasy", "as": "Assamese",
    "tt": "Tatar", "haw": "Hawaiian", "ln": "Lingala", "ha": "Hausa",
    "ba": "Bashkir", "jw": "Javanese", "su": "Sundanese"
}

# Rate limiting
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

def fail(msg, code=7):
    logger.error(msg)
    sys.exit(code)

def get_retry_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def make_request_with_retries(method, url, max_retries=3, **kwargs):
    session = get_retry_session()
    last_error = None
    for attempt in range(max_retries):
        try:
            response = session.request(method, url, timeout=(10, 30), **kwargs)
            if response.status_code < 500:
                return response
            if attempt == max_retries - 1:
                return response
            time.sleep(5)
        except Exception as e:
            last_error = e
            if attempt == max_retries - 1:
                logger.error(f"Request failed after {max_retries} attempts: {e}")
                return None
            time.sleep([2, 5, 15][attempt] + random.uniform(0, 2))
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
    logger.info(f"Server: {ip}:{port}")
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

def add_metadata_directory(repo_guid, provider, file_path):
    meta_path, _ = get_store_paths()
    base_meta = meta_path.split("/./")[0] if "/./" in meta_path else meta_path
    meta_right = meta_path.split("/./")[1] if "/./" in meta_path else "metadata"
    base_name = Path(file_path).stem
    metadata_dir = os.path.join(base_meta, meta_right, str(repo_guid), file_path, provider)
    os.makedirs(metadata_dir, exist_ok=True)
    return metadata_dir, meta_right, base_name

def get_advanced_ai_config(config_name, provider="whisper_ai"):
    admin_dropbox = get_admin_dropbox_path()
    if not admin_dropbox:
        return {}
    
    for src in [
        os.path.join(admin_dropbox, "AdvancedAiExport", "Configs", f"{config_name}.json"),
        os.path.join(admin_dropbox, "AdvancedAiExport", "Samples", f"{provider}.json")
    ]:
        if os.path.exists(src):
            try:
                with open(src, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load {src}: {e}")
    return {}

def get_whisper_normalized_metadata(raw_metadata_file_path, norm_metadata_file_path):
    if not os.path.exists(NORMALIZER_SCRIPT_PATH):
        logger.error(f"Normalizer not found: {NORMALIZER_SCRIPT_PATH}")
        return False
    
    try:
        process = subprocess.run(
            ["python3", NORMALIZER_SCRIPT_PATH, "-i", raw_metadata_file_path, "-o", norm_metadata_file_path],
            check=True,
            capture_output=True
        )
        if process.returncode == 0:
            return True
        else:
            logger.error(f"Normalizer failed: {process.returncode}")
            return False
    except Exception as e:
        logger.error(f"Error normalizing: {e}")
        return False

def transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix, language_code):
    try:
        logger.debug(f"Transforming: {norm_metadata_file_path}, Prefix: {filetype_prefix}, Lang: {language_code}")
        with open(norm_metadata_file_path, "r") as f:
            data = json.load(f)
        result = []
        for event_type, detections in data.items():
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, 'unknown')
            
            for detection in detections:
                occurrences = detection.get("occurrences", [])
                
                event_obj = {
                    "eventType": event_type,
                    "sdnaEventTypePrefix": filetype_prefix,
                    "sdnaEventType": sdna_event_type,
                    "eventValue": detection.get("value", ""),
                    "totalOccurrences": len(occurrences),
                    "eventOccurence": occurrences
                }
                result.append(event_obj)
        return result, True
    except Exception as e:
        return f"Error during transformation: {e}", False

class WhisperProcessor:
    def __init__(self, api_key, config=None):
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key)
        self.config = config or {}
        self.model = self.config.get("model", "whisper-1")
        self.chunk_duration = self.config.get("chunk_duration", DEFAULT_CHUNK_DURATION)

    def _to_wav(self, input_path, wav_path):
        cmd = ["ffmpeg", "-y", "-i", input_path, "-ac", "1", "-ar", "16000", wav_path]
        try:
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg failed: {e}")
            raise

    def _get_duration(self, wav_path):
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
               "-of", "default=noprint_wrappers=1:nokey=1", wav_path]
        try:
            output = subprocess.check_output(cmd).decode().strip()
            return float(output)
        except (subprocess.CalledProcessError, ValueError) as e:
            logger.error(f"Failed to get duration: {e}")
            raise

    def _split_audio(self, wav_path, output_dir, duration):
        os.makedirs(output_dir, exist_ok=True)
        chunks = []
        num_chunks = math.ceil(duration / self.chunk_duration)

        for i in range(num_chunks):
            start = i * self.chunk_duration
            out_file = os.path.join(output_dir, f"chunk_{i}.wav")
            cmd = ["ffmpeg", "-y", "-i", wav_path, "-ss", str(start), "-t", str(self.chunk_duration), out_file]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            chunks.append((out_file, start))

        return chunks

    def _process_chunk_with_retry(self, wav_path, language, translate):
        for attempt in range(MAX_RETRIES):
            try:
                wait_for_rate_limit()
                with open(wav_path, "rb") as f:
                    kwargs = {"file": f, "model": self.model, "response_format": "verbose_json"}
                    if language:
                        kwargs["language"] = language
                    
                    if translate:
                        result = self.client.audio.translations.create(**kwargs)
                    else:
                        result = self.client.audio.transcriptions.create(**kwargs)
                    return result
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Failed after {MAX_RETRIES} attempts: {e}")
                    raise
                wait_time = (attempt + 1) * 5
                logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} in {wait_time}s: {e}")
                time.sleep(wait_time)

    def transcribe(self, input_path, language=None, translate=False):
        base = os.path.splitext(input_path)[0]
        wav_path = f"{base}_full.wav"
        chunk_dir = f"{base}_chunks"

        try:
            logger.info(f"Converting: {input_path}")
            self._to_wav(input_path, wav_path)
            
            duration = self._get_duration(wav_path)
            logger.info(f"Duration: {duration:.2f}s")

            final_segments = []
            full_text = []
            detected_language = language

            if duration <= self.chunk_duration:
                logger.info("Single chunk")
                result = self._process_chunk_with_retry(wav_path, language, translate)
                detected_language = "en" if translate else result.language
                full_text.append(result.text)

                for seg in result.segments:
                    final_segments.append({
                        "start": float(seg.start),
                        "end": float(seg.end),
                        "text": seg.text
                    })
            else:
                chunks = self._split_audio(wav_path, chunk_dir, duration)
                logger.info(f"Processing {len(chunks)} chunks")

                for idx, (chunk_path, offset) in enumerate(chunks, 1):
                    logger.info(f"Chunk {idx}/{len(chunks)}")
                    result = self._process_chunk_with_retry(chunk_path, language, translate)
                    detected_language = "en" if translate else result.language
                    full_text.append(result.text)

                    for seg in result.segments:
                        final_segments.append({
                            "start": float(seg.start + offset),
                            "end": float(seg.end + offset),
                            "text": seg.text
                        })

                    os.remove(chunk_path)

                if os.path.exists(chunk_dir):
                    os.rmdir(chunk_dir)

            return {
                "task": "translation" if translate else "transcription",
                "language": detected_language,
                "text": " ".join(full_text),
                "segments": final_segments,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "model": self.model,
                "duration": duration
            }

        finally:
            if os.path.exists(wav_path):
                os.remove(wav_path)
            if os.path.exists(chunk_dir):
                for f in os.listdir(chunk_dir):
                    os.remove(os.path.join(chunk_dir, f))
                os.rmdir(chunk_dir)

    def save_metadata_to_file(self, file_path: str, metadata: dict) -> bool:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"Saved: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Save failed: {e}")
            return False

    def fetch_and_save_all_metadata(self, input_path, repo_guid, file_path, translation_languages=None, force_refresh=False):
        provider = "WHISPER"
        metadata_dir, meta_right, base_name = add_metadata_directory(repo_guid, provider, file_path)

        # Base transcription
        base_json_path = os.path.join(metadata_dir, f"{base_name}.json")
        if os.path.exists(base_json_path) and not force_refresh:
            logger.info(f"Using cached base transcription: {base_json_path}")
            with open(base_json_path, 'r', encoding='utf-8') as f:
                base_metadata = json.load(f)
        else:
            if force_refresh:
                logger.info("Force refresh enabled - regenerating base transcription")
            else:
                logger.info("Fetching base transcription...")
            base_metadata = self.transcribe(input_path)
            if not base_metadata:
                fail("Transcription failed")
            if not self.save_metadata_to_file(base_json_path, base_metadata):
                fail("Failed to save base metadata")

        # Translations
        translated_data = {}
        if translation_languages:
            logger.info(f"Processing translations: {translation_languages}")
            for lang_code in translation_languages:
                if lang_code not in WHISPER_LANGUAGES:
                    logger.warning(f"Unsupported language: {lang_code}")
                    continue
                
                lang_json_path = os.path.join(metadata_dir, f"{base_name}_{lang_code}.json")
                if os.path.exists(lang_json_path) and not force_refresh:
                    logger.info(f"Using cached translation for {lang_code}: {lang_json_path}")
                    try:
                        with open(lang_json_path, 'r', encoding='utf-8') as f:
                            translated_data[lang_code] = json.load(f)
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to load cached {lang_code}, regenerating: {e}")
                
                if force_refresh:
                    logger.info(f"Force refresh - regenerating translation for: {lang_code}")
                else:
                    logger.info(f"Translating to: {lang_code}")
                translated_metadata = self.transcribe(input_path, language=lang_code)
                
                if translated_metadata:
                    if self.save_metadata_to_file(lang_json_path, translated_metadata):
                        translated_data[lang_code] = translated_metadata

        # Combine
        combined = {
            "base_transcription": base_metadata,
            "multilanguage_transcriptions": translated_data if translated_data else None
        }
        
        return combined

    def store_metadata_file(self, repo_guid, file_path, combined_metadata):
        provider = "WHISPER"
        metadata_dir, meta_right, base_name = add_metadata_directory(repo_guid, provider, file_path)
        repo_guid_str = str(repo_guid)

        # Save base raw
        raw_json_default = os.path.join(metadata_dir, f"{base_name}.json")
        if not self.save_metadata_to_file(raw_json_default, combined_metadata["base_transcription"]):
            fail("Failed to save base metadata")

        # Normalize base
        norm_json_default = os.path.join(metadata_dir, f"{base_name}_norm.json")
        if not get_whisper_normalized_metadata(raw_json_default, norm_json_default):
            logger.error("Normalization failed for default language")
            return [], []

        raw_paths = []
        norm_paths = []
        
        raw_return_default = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}.json")
        norm_return_default = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_norm.json")
        raw_paths.append((raw_return_default, None))
        norm_paths.append((norm_return_default, None))

        # Save translations
        multilang = combined_metadata.get("multilanguage_transcriptions") or {}
        for lang_code, lang_data in multilang.items():
            # Save raw
            raw_json_lang = os.path.join(metadata_dir, f"{base_name}_{lang_code}.json")
            if not self.save_metadata_to_file(raw_json_lang, lang_data):
                logger.warning(f"Failed to save raw for {lang_code}")
                continue
            
            # Normalize
            norm_json_lang = os.path.join(metadata_dir, f"{base_name}_norm_{lang_code}.json")
            if not get_whisper_normalized_metadata(raw_json_lang, norm_json_lang):
                logger.warning(f"Normalization failed for {lang_code}")
                continue
            
            raw_return_lang = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_{lang_code}.json")
            norm_return_lang = os.path.join(meta_right, repo_guid_str, file_path, provider, f"{base_name}_norm_{lang_code}.json")
            raw_paths.append((raw_return_lang, lang_code))
            norm_paths.append((norm_return_lang, lang_code))

        return raw_paths, norm_paths

    def send_extracted_metadata(self, repo_guid, file_path, rawMetadataFilePath, normMetadataFilePath=None, language_code=None, max_attempts=3):
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
            "providerName": "WHISPER_AI",
            "sourceLanguage": WHISPER_LANGUAGES.get(language_code, "Default") if language_code else "Default",
            "extendedMetadata": [{
                "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
                "fileName": os.path.basename(file_path),
                "metadata": metadata_array
            }]
        }
        
        logger.debug(f"Sending to {url}, lang={language_code}, raw={rawMetadataFilePath}, norm={normMetadataFilePath}")
        logger.debug(f" headers: {headers}, payload: {json.dumps(payload)[:500]}")
        for attempt in range(max_attempts):
            r = None
            error_detail = None
            try:
                r = make_request_with_retries("POST", url, headers=headers, json=payload)
                logger.debug(f"Response: {r.status_code} - {r.text if r is not None else 'No response'}")
                if r and r.status_code in (200, 201):
                    logger.debug(f"Metadata sent: {r.status_code}")
                    return True
                elif r:
                    logger.warning(f"Attempt {attempt+1}/{max_attempts} failed: {r.status_code} - {r.text[:500]}")
                    error_detail = f"HTTP {r.status_code}"
                else:
                    error_detail = "No response received"
            except Exception as e:
                error_detail = f"{type(e).__name__}: {e}"
                logger.warning(f"Attempt {attempt+1}/{max_attempts} error: {error_detail}")
            
            if r is None and attempt == 0:
                try:
                    import socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(2)
                    result = sock.connect_ex(('127.0.0.1', 5080))
                    sock.close()
                    if result != 0:
                        logger.error(f"Port 5080 not listening (connect returned {result}) - catalog service may not be running")
                    else:
                        logger.error(f"Port 5080 is listening but not responding - check catalog service logs")
                except Exception as sock_err:
                    logger.error(f"Socket check failed: {sock_err}")
            
            if attempt < max_attempts - 1:
                time.sleep([1, 3, 10][attempt] + random.uniform(0, 1))
        
        logger.error(f"Failed to send metadata after {max_attempts} attempts - last error: {error_detail}")
        return False

    def send_ai_enriched_metadata(self, repo_guid, file_path, enrichedMetadata, language_code=None, max_attempts=3):
        url = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
        node_api_key = get_node_api_key()
        headers = {"apikey": node_api_key, "Content-Type": "application/json"}
        payload = {
            "repoGuid": repo_guid,
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "sourceLanguage": WHISPER_LANGUAGES.get(language_code, "Default") if language_code else "Default",
            "providerName": "WHISPER",
            "normalizedMetadata": enrichedMetadata
        }
        
        for attempt in range(max_attempts):
            try:
                r = make_request_with_retries("POST", url, headers=headers, json=payload)
                if r is not None and r.status_code in (200, 201):
                    return True
                else:
                    logger.debug(f"AI enriched response: {r.text if r is not None else 'No response'}")
            except Exception as e:
                if attempt == 2:
                    logger.critical(f"Failed to send AI enriched: {e}")
                    raise
                time.sleep([1, 3, 10][attempt] + random.uniform(0, 1))
        return False

if __name__ == "__main__":
    time.sleep(random.uniform(0.0, 0.5))

    parser = argparse.ArgumentParser(description="Whisper AI Transcription Uploader")
    parser.add_argument("-m", "--mode", required=True, choices=VALID_MODES)
    parser.add_argument("-c", "--config-name", required=True)
    parser.add_argument("-sp", "--source-path", required=True)
    parser.add_argument("-cp", "--catalog-path", required=True)
    parser.add_argument("-r", "--repo-guid", required=True)
    parser.add_argument("-j", "--job-guid", help="Job GUID")
    parser.add_argument("--controller-address", help="Override IP:Port")
    parser.add_argument("--enrich-prefix", help="Prefix for sdnaEventType of AI enrich data")
    parser.add_argument("--log-level", default="debug", help="Logging level")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cached transcripts and regenerate all")
    args = parser.parse_args()

    setup_logging(args.log_level)

    if args.mode not in VALID_MODES:
        fail(f"Invalid mode: {args.mode}")

    # Load config
    cloud_config_path = get_cloud_config_path()
    if not os.path.exists(cloud_config_path):
        fail(f"Cloud config not found: {cloud_config_path}")

    cloud_config = ConfigParser()
    cloud_config.read(cloud_config_path)
    if args.config_name not in cloud_config:
        fail(f"Config section not found: {args.config_name}")

    section = cloud_config[args.config_name]
    api_key = section.get("api_key")
    if not api_key:
        fail("api_key not found in config")

    # Advanced config
    advanced_config = get_advanced_ai_config(args.config_name, "whisper_ai")
    
    # Paths
    file_name = os.path.basename(args.catalog_path)
    catalog_dir = os.path.dirname(args.catalog_path or args.source_path)
    normalized_path = catalog_dir.replace("\\", "/")
    relative_path = normalized_path.split("/1/", 1)[-1] if "/1/" in normalized_path else normalized_path

    # Backlink
    client_ip, _ = (args.controller_address.split(":", 1) if args.controller_address and ":" in args.controller_address
                    else get_link_address_and_port())
    job_guid = args.job_guid or ""
    backlink_url = (
        f"https://{client_ip}/dashboard/projects/{job_guid}"
        f"/browse&search?path={urllib.parse.quote(relative_path)}"
        f"&filename={urllib.parse.quote(file_name)}"
    )

    if args.dry_run:
        logger.info("[DRY RUN] Skipped")
        sys.exit(0)

    try:
        processor = WhisperProcessor(api_key, advanced_config)
        
        # Get languages and prefix
        translation_languages = advanced_config.get("translation_languages", [])
        catalog_path_clean = args.catalog_path.replace("\\", "/").split("/1/", 1)[-1]
        filetype_prefix = args.enrich_prefix or ""
        
        # Fetch metadata
        combined_metadata = processor.fetch_and_save_all_metadata(
            args.source_path,
            args.repo_guid,
            catalog_path_clean,
            translation_languages,
            force_refresh=args.force_refresh
        )
        
        if not combined_metadata:
            fail("No metadata returned")

        # Store files with normalization
        raw_paths, norm_paths = processor.store_metadata_file(args.repo_guid, catalog_path_clean, combined_metadata)
        
        # Send to API
        send_failures = []
        for raw_return, lang_code in raw_paths:
            norm_return = next((p for p, lc in norm_paths if lc == lang_code), None)
            if not processor.send_extracted_metadata(
                args.repo_guid,
                catalog_path_clean,
                raw_return,
                norm_return,
                language_code=lang_code
            ):
                send_failures.append(lang_code or "default")
        
        if send_failures:
            fail(f"Failed to send extracted metadata for: {', '.join(send_failures)}")

        logger.info("Extracted metadata sent successfully")

        # Transform and send enriched metadata
        if norm_paths:
            for norm_return, lang_code in norm_paths:
                if not norm_return:
                    continue
                try:
                    meta_path, _ = get_store_paths()
                    norm_metadata_file_path = os.path.join(
                        meta_path.split("/./")[0] if "/./" in meta_path else meta_path,
                        norm_return
                    )
                    enriched, success = transform_normalized_to_enriched(norm_metadata_file_path, filetype_prefix, lang_code)
                    if success and processor.send_ai_enriched_metadata(args.repo_guid, catalog_path_clean, enriched, lang_code):
                        logger.info(f"AI enriched metadata sent for {lang_code or 'default'}")
                    else:
                        logger.warning(f"AI enriched send failed for {lang_code or 'default'} — skipping")
                except Exception:
                    logger.exception(f"AI enriched transform failed for {lang_code or 'default'} — skipping")
        else:
            logger.info("Normalized metadata not present — skipping AI enrichment")

        logger.info("Metadata sent successfully")
        sys.exit(0)

    except Exception as e:
        fail(f"Operation failed: {e}", 1)