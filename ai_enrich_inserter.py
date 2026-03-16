#!/usr/bin/env python3
"""
ai_enrich_inserter.py
────────────────────────────────────────────────────────────────────────────────
Reads catalogExtendedMetadata documents from MongoDB, normalises raw JSON where
needed, then POSTs AI-enriched records to the local SDNA node API.

Compatible with the NEW metadata array schema:
  metadata: [
    { type: "metadataFilePath",         path: "…_norm.json" },
    { type: "metadataRawJsonFilePath",  path: "…_raw.json"  },
  ]

Usage
-----
  # Normalise only
  python3 ai_enrich_inserter.py --repo-guid <GUID> --normalize-only

  # Enrich only (norm files must already exist)
  python3 ai_enrich_inserter.py --repo-guid <GUID> --enrich-only --proxy-type video_proxy

  # Full pipeline
  python3 ai_enrich_inserter.py --repo-guid <GUID> --proxy-type video_proxy

Exit codes: 0 = success, 1 = bad args, 2 = partial/full failure, 5 = API key error
"""

import os
import sys
import json
import time
import random
import logging
import plistlib
import argparse
import subprocess
from configparser import ConfigParser
from decimal import Decimal

import ijson
import requests
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Constants ────────────────────────────────────────────────────────────────

AI_CONFIG_PATH             = "/opt/sdna/nginx/ai-config.json"
NODE_AI_ENRICH_URL         = "http://127.0.0.1:5080/catalogs/aiEnrichedMetadata/add"
NODE_EXTENDED_METADATA_URL = "http://127.0.0.1:5080/catalogs/extendedMetadata"

LINUX_CONFIG_PATH  = "/etc/StorageDNA/DNAClientServices.conf"
MAC_CONFIG_PATH    = "/Library/Preferences/com.storagedna.DNAClientServices.plist"
IS_LINUX           = os.path.isdir("/opt/sdna/bin")
DNA_CLIENT_SERVICES = LINUX_CONFIG_PATH if IS_LINUX else MAC_CONFIG_PATH

MONGO_URI        = "mongodb://127.0.0.1:27017"
MONGO_DB         = "ApiDNA"
MONGO_COLLECTION = "catalogExtendedMetadata"

BATCH_SIZE  = 10
MAX_WORKERS = 5
MAX_RETRIES = 3

# ── Normaliser scripts ────────────────────────────────────────────────────────

NORMALIZER_SCRIPTS = {
    "azure_vision":  "/opt/sdna/bin/azure_vision_metadata_normalizer.py",
    "AWS":           "/opt/sdna/bin/rekognition_metadata_normalizer.py",
    "google_vision": "/opt/sdna/bin/google_vision_metadata_normalizer.py",
    "rubicx":        "/opt/sdna/bin/rubicx_metadata_normalizer.py",
    "gcp":           "/opt/sdna/bin/google_media_metadata_normalizer.py",
}

# ── Event maps ────────────────────────────────────────────────────────────────

SDNA_EVENT_MAP = {
    # Azure Video Indexer
    "azure_video_transcript":                  "transcript",
    "azure_video_ocr":                         "ocr",
    "azure_video_keywords":                    "keywords",
    "azure_video_topics":                      "topics",
    "azure_video_labels":                      "labels",
    "azure_video_brands":                      "brands",
    "azure_video_named_locations":             "locations",
    "azure_video_named_people":                "celebrities",
    "azure_video_audio_effects":               "effects",
    "azure_video_detected_objects":            "objects",
    "azure_video_sentiments":                  "sentiments",
    "azure_video_emotions":                    "emotions",
    "azure_video_visual_content_moderation":   "moderation",
    "azure_video_frame_patterns":              "patterns",
    "azure_video_speakers":                    "speakers",
    # Twelve Labs
    "twelvelabs_labels":     "labels",
    "twelvelabs_keywords":   "keywords",
    "twelvelabs_summary":    "summary",
    "twelvelabs_highlights": "highlights",
    "twelvelabs_transcript": "transcript",
    # Azure Vision
    "azure_vision_tags":        "tags",
    "azure_vision_objects":     "objects",
    "azure_vision_faces":       "faces",
    "azure_vision_categories":  "categories",
    "azure_vision_description": "description",
    "azure_vision_brand":       "brands",
    "azure_vision_landmarks":   "landmarks",
    # AWS Rekognition + Transcribe
    "aws_rek_detect_labels":               "labels",
    "aws_rek_detect_faces":                "faces",
    "aws_rek_detect_moderation_labels":    "moderation",
    "aws_rek_detect_text":                 "ocr",
    "aws_rek_detect_protective_equipment": "protective_equipment",
    "aws_rek_recognize_celebrities":       "celebrities",
    "aws_transcript":                      "transcript",
    # GCP Video / Vision
    "gcp_video_labels":      "labels",
    "gcp_video_keywords":    "keywords",
    "gcp_video_summary":     "summary",
    "gcp_video_highlights":  "highlights",
    "gcp_video_transcript":  "transcript",
    "gcp_vision_label_annotations":              "labels",
    "gcp_vision_object_annotations":             "objects",
    "gcp_vision_text_annotations":               "ocr",
    "gcp_vision_face_annotations":               "faces",
    "gcp_vision_safe_search_annotation":         "safe_search",
    "gcp_vision_image_properties_annotation":    "image_color",
    "gcp_vision_web_detection":                  "web_detection",
    # Whisper / Assembly AI
    "whisper_transcript":   "transcript",
    "assembly_transcript":  "transcript",
}

EVENT_TYPE_PROXY_VALUE_MAP = {
    "video_proxy":                      "vid",
    "video_proxy_sample_scene_change":  "frm",
    "video_sample_faces":               "img",
    "video_sample_interval":            "vid",
    "audio_proxy":                      "aud",
    "sprite_sheet":                     "img",
}

# Transcript event types that arrive as a dict (not a list of detections)
TRANSCRIPT_EVENT_TYPES = {"aws_transcript", "whisper_transcript", "assembly_transcript"}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config helpers ────────────────────────────────────────────────────────────

def load_ai_export_path() -> str:
    try:
        with open(AI_CONFIG_PATH, "r") as f:
            path = json.load(f).get("ai_export_shared_drive_path", "")
        if not path:
            raise ValueError("ai_export_shared_drive_path is empty")
        return path
    except Exception as exc:
        logger.error("Cannot read AI export path from %s: %s", AI_CONFIG_PATH, exc)
        sys.exit(1)


def resolve_physical_path(ai_export_base: str, logical_path: str) -> str:
    """
    Build a full filesystem path from the export base and a relative logical path.
    Handles the /./  mount-point separator used by StorageDNA.
    """
    if "/./" in ai_export_base:
        meta_left, meta_right = ai_export_base.split("/./", 1)
    else:
        meta_left, meta_right = ai_export_base, ""

    meta_left    = meta_left.rstrip("/")
    logical_path = logical_path.lstrip("/")

    # Avoid doubling the meta_right prefix when it is already part of logical_path
    if meta_right and logical_path.startswith(meta_right.lstrip("/") + "/"):
        return os.path.join(meta_left, logical_path)

    return os.path.join(meta_left, meta_right, logical_path)


def get_node_api_key() -> str:
    try:
        if IS_LINUX:
            parser = ConfigParser()
            parser.read(DNA_CLIENT_SERVICES)
            key = parser.get("General", "NodeAPIKey", fallback="")
        else:
            with open(DNA_CLIENT_SERVICES, "rb") as fp:
                key = plistlib.load(fp).get("NodeAPIKey", "")
    except Exception as exc:
        logger.error("Error reading Node API key: %s", exc)
        sys.exit(5)

    if not key:
        logger.error("NodeAPIKey not found in %s", DNA_CLIENT_SERVICES)
        sys.exit(5)

    return key


NODE_API_KEY = get_node_api_key()

# ── New-schema helpers ────────────────────────────────────────────────────────

def extract_metadata_path(doc: dict, path_type: str) -> str | None:
    """
    Pull a path out of the new metadata array format:
      metadata: [ { type: "metadataFilePath", path: "…" }, … ]

    path_type is one of:
      "metadataFilePath"        → normalised JSON
      "metadataRawJsonFilePath" → raw JSON
    """
    for entry in doc.get("metadata", []):
        if isinstance(entry, dict) and entry.get("type") == path_type:
            return entry.get("path")
    return None


def has_norm_path(doc: dict) -> bool:
    return extract_metadata_path(doc, "metadataFilePath") is not None


def has_raw_path(doc: dict) -> bool:
    return extract_metadata_path(doc, "metadataRawJsonFilePath") is not None

# ── Normalisation ─────────────────────────────────────────────────────────────

def normalize_json(provider: str, raw_json_path: str, norm_json_path: str,
                   azure_vision_version: str | None = None) -> bool:
    if not provider:
        logger.error("No provider specified for normalisation")
        return False

    normalizer = NORMALIZER_SCRIPTS.get(provider)
    if not normalizer:
        logger.error("No normaliser script for provider: %s", provider)
        return False

    if not os.path.isfile(normalizer):
        logger.error("Normaliser script not found: %s", normalizer)
        return False

    try:
        cmd = ["python3", normalizer, "-i", raw_json_path, "-o", norm_json_path]
        if provider == "azure_vision" and azure_vision_version:
            cmd.extend(["--version", azure_vision_version])

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            logger.info("✔ Normalised JSON → %s", norm_json_path)
            return True

        logger.error("✖ Normaliser failed [%d]: %s", proc.returncode, proc.stderr.strip())
    except Exception as exc:
        logger.error("✖ Error running normaliser: %s", exc)

    return False


def save_extended_metadata(repo_guid: str, provider: str, file_path: str,
                            raw_metadata_path: str | None = None,
                            norm_metadata_path: str | None = None,
                            max_attempts: int = 3) -> bool:
    """
    POST to the Node API to create/update the catalogExtendedMetadata document.
    Builds the metadata array in the new format automatically.
    """
    headers = {"apikey": NODE_API_KEY, "Content-Type": "application/json"}

    metadata_array = []
    if norm_metadata_path:
        metadata_array.append({"type": "metadataFilePath",        "path": norm_metadata_path})
    if raw_metadata_path:
        metadata_array.append({"type": "metadataRawJsonFilePath", "path": raw_metadata_path})

    if not metadata_array:
        logger.warning("save_extended_metadata called with no paths for %s — skipping", file_path)
        return False

    payload = {
        "repoGuid":    repo_guid,
        "providerName": provider,
        "sourceLanguage": "Default",
        "extendedMetadata": [{
            "fullPath": file_path if file_path.startswith("/") else f"/{file_path}",
            "fileName": os.path.basename(file_path),
            "metadata": metadata_array,
        }],
    }

    delays = [(1, 1), (3, 1), (10, 5)]
    for attempt in range(max_attempts):
        try:
            r = requests.post(NODE_EXTENDED_METADATA_URL, headers=headers,
                              json=payload, timeout=30)
            if r.status_code in (200, 201):
                logger.info("✔ Extended metadata saved: %s", os.path.basename(file_path))
                return True
            if r.status_code >= 500:
                raise RuntimeError(f"Node 5xx: {r.status_code}")
            logger.error("✖ Node rejected [%d]: %s", r.status_code, r.text[:300])
            return False
        except Exception as exc:
            if attempt == max_attempts - 1:
                logger.critical("✖ Failed to save extended metadata after %d attempts: %s",
                                max_attempts, exc)
                return False
            base_d, jitter = delays[attempt]
            time.sleep(base_d + random.uniform(0, jitter))

    return False

# ── Transformer (streaming, Decimal-safe, transcript-aware) ──────────────────

def _sanitize(obj):
    """Recursively convert Decimal → int/float so json.dumps never raises."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(i) for i in obj]
    return obj


def transform_normalized_to_enriched(norm_metadata_file_path: str, filetype_prefix: str = "vid"):
    """
    Stream-parse a normalised JSON file and yield (chunk, True) tuples.

    Handles:
      • Transcript events (aws_transcript, whisper_transcript, assembly_transcript)
        whose value is a dict { "words": [...] }
      • All other events whose value is a list of detection dicts

    Never yields a (str, False) error tuple — raises on unexpected errors so the
    caller's try/except can handle cleanly.
    """
    # ── Pre-flight ────────────────────────────────────────────────────────────
    if not norm_metadata_file_path:
        logger.error("norm_metadata_file_path is None/empty — aborting")
        return

    if not os.path.isfile(norm_metadata_file_path):
        logger.error("File not found: %s", norm_metadata_file_path)
        return

    try:
        file_size = os.path.getsize(norm_metadata_file_path)
    except OSError as exc:
        logger.error("Cannot stat %s: %s", norm_metadata_file_path, exc)
        return

    if file_size == 0:
        logger.warning("File is empty: %s", norm_metadata_file_path)
        return

    # ── Chunk sizing ──────────────────────────────────────────────────────────
    if file_size < 10 * 1024 * 1024:
        chunk_size_bytes = 1 * 1024 * 1024
    elif file_size < 100 * 1024 * 1024:
        chunk_size_bytes = 4 * 1024 * 1024
    elif file_size < 512 * 1024 * 1024:
        chunk_size_bytes = 8 * 1024 * 1024
    else:
        chunk_size_bytes = 16 * 1024 * 1024

    # ── Helpers ───────────────────────────────────────────────────────────────
    def build_event(event_type, sdna_event_type, event_value, occurrences):
        if not isinstance(occurrences, list):
            logger.warning("occurrences not a list for '%s' — defaulting to []", event_type)
            occurrences = []
        return _sanitize({
            "eventType":           event_type,
            "sdnaEventType":       sdna_event_type,
            "sdnaEventTypePrefix": filetype_prefix,
            "eventValue":          event_value,
            "totalOccurrences":    len(occurrences),
            "eventOccurence":      occurrences,
        })

    def word_to_occ(w: dict) -> dict:
        return _sanitize({
            "confidence_score": float(w.get("confidence") or 0),
            "start":            w.get("start_time", 0),
            "end":              w.get("end_time", 0),
            "frame":            "",
            "positions":        [],
            "content":          w.get("content", ""),
            "speaker_label":    w.get("speaker_label", ""),
            "language_code":    w.get("language_code", ""),
        })

    def try_serialize_size(record, event_type) -> int:
        try:
            return len(json.dumps(record).encode("utf-8"))
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping non-serializable record for '%s': %s", event_type, exc)
            return 0

    # ── Streaming parse ───────────────────────────────────────────────────────
    f = None
    try:
        f = open(norm_metadata_file_path, "rb")
        chunk, chunk_bytes = [], 0

        for event_type, detections in ijson.kvitems(f, ""):
            sdna_event_type = SDNA_EVENT_MAP.get(event_type, "unknown")

            # ── Transcript block: dict with a "words" list ────────────────────
            if event_type in TRANSCRIPT_EVENT_TYPES and isinstance(detections, dict):
                for w in detections.get("words", []):
                    if not isinstance(w, dict) or w.get("type") == "punctuation":
                        continue
                    occ    = word_to_occ(w)
                    record = build_event(event_type, sdna_event_type, occ["content"], [occ])
                    size   = try_serialize_size(record, event_type)
                    if size == 0:
                        continue
                    chunk.append(record)
                    chunk_bytes += size
                    if chunk_bytes >= chunk_size_bytes:
                        yield chunk, True
                        chunk, chunk_bytes = [], 0

            # ── Generic detection list ────────────────────────────────────────
            elif isinstance(detections, list):
                for d in detections:
                    if not isinstance(d, dict):
                        logger.warning("Non-dict detection under '%s' — skipped", event_type)
                        continue
                    record = build_event(
                        event_type, sdna_event_type,
                        d.get("value", "Unidentified"),
                        d.get("occurrences", []),
                    )
                    size = try_serialize_size(record, event_type)
                    if size == 0:
                        continue
                    chunk.append(record)
                    chunk_bytes += size
                    if chunk_bytes >= chunk_size_bytes:
                        yield chunk, True
                        chunk, chunk_bytes = [], 0

            else:
                logger.warning("Skipping '%s': unexpected type %s",
                               event_type, type(detections).__name__)

        if chunk:
            yield chunk, True

    except ijson.common.IncompleteJSONError as exc:
        logger.error("Truncated/malformed JSON in %s: %s", norm_metadata_file_path, exc)
        if chunk:
            logger.warning("Yielding partial chunk (%d records) before parse failure", len(chunk))
            yield chunk, True

    except Exception:
        logger.exception("Unexpected error transforming %s", norm_metadata_file_path)
        raise

    finally:
        if f:
            f.close()

# ── Enrichment sender ─────────────────────────────────────────────────────────

def send_ai_enriched_for_record(record: dict, ai_export_base: str, proxy_prefix: str) -> bool:
    """
    Given a single catalogExtendedMetadata document, resolve its normalised JSON
    path, stream-transform it, and POST every chunk to the Node API.
    """
    # ── Resolve the normalised file path from the new metadata array ──────────
    norm_path_relative = extract_metadata_path(record, "metadataFilePath")
    if not norm_path_relative:
        logger.warning("⚠ No metadataFilePath for: %s", record.get("fileName"))
        return False

    norm_path = resolve_physical_path(ai_export_base, norm_path_relative)
    if not os.path.exists(norm_path):
        logger.warning("⚠ Normalised file not found on disk: %s", norm_path)
        return False

    # ── Build base POST payload ───────────────────────────────────────────────
    full_path = record.get("fullPath", "")
    base_payload = {
        "repoGuid":       record["repoGuid"],
        "fullPath":       full_path if full_path.startswith("/") else f"/{full_path}",
        "fileName":       record.get("fileName", os.path.basename(full_path)),
        "providerName":   record.get("providerName", ""),
        "sourceLanguage": record.get("sourceLanguage", "Default"),
    }

    headers = {"apikey": NODE_API_KEY, "Content-Type": "application/json"}
    delays  = [(1, 1), (3, 1), (10, 5)]
    all_ok  = True
    chunk_n = 0

    try:
        for chunk, ok in transform_normalized_to_enriched(norm_path, proxy_prefix):
            chunk_n += 1
            if not ok:
                logger.error("Bad chunk #%d for %s — skipping", chunk_n, record.get("fileName"))
                all_ok = False
                continue

            sent = False
            for attempt in range(MAX_RETRIES):
                try:
                    r = requests.post(
                        NODE_AI_ENRICH_URL,
                        headers=headers,
                        json={**base_payload, "normalizedMetadata": chunk},
                        timeout=(5, 120),
                    )
                    if r.status_code in (200, 201):
                        sent = True
                        break
                    if r.status_code >= 500:
                        raise RuntimeError(f"Node 5xx: {r.status_code}")
                    logger.error("✖ Node rejected [%d]: %s", r.status_code, r.text[:300])
                    break  # 4xx — not retryable
                except Exception as exc:
                    if attempt == MAX_RETRIES - 1:
                        logger.critical("✖ Chunk #%d failed after %d attempts for %s: %s",
                                        chunk_n, MAX_RETRIES, record.get("fileName"), exc)
                        all_ok = False
                        break
                    base_d, jitter = delays[attempt]
                    time.sleep(base_d + random.uniform(0, jitter))

            if not sent:
                all_ok = False

    except Exception:
        logger.exception("Unexpected error enriching %s", record.get("fileName"))
        return False

    if all_ok:
        logger.info("✔ Enriched (%d chunk(s)): %s", chunk_n, record.get("fileName"))
    return all_ok

# ── MongoDB queries ───────────────────────────────────────────────────────────

def _mongo_col():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][MONGO_COLLECTION]


def fetch_records_needing_normalisation(repo_guid: str):
    """
    Records that have a raw JSON path but are missing a normalised path.
    Uses the new metadata-array schema.
    """
    col = _mongo_col()
    # All non-deleted docs for this repo that have at least one metadata entry
    cursor = col.find({
        "repoGuid": repo_guid,
        "isDeleted": {"$ne": True},
        "metadata": {
            "$elemMatch": {"type": "metadataRawJsonFilePath"}
        },
    })
    # Filter in Python for docs that do NOT yet have a norm path
    # (avoids a complex $not/$elemMatch combo)
    return [doc for doc in cursor if not has_norm_path(doc)]


def fetch_records_with_norm_metadata(repo_guid: str):
    """
    Records that already have a normalised JSON path — ready for enrichment.
    """
    col = _mongo_col()
    return col.find({
        "repoGuid": repo_guid,
        "isDeleted": {"$ne": True},
        "metadata": {
            "$elemMatch": {"type": "metadataFilePath"}
        },
    })


def chunked_cursor(cursor, size: int):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch

# ── Step 1: Normalisation ─────────────────────────────────────────────────────

def process_normalization(repo_guid: str, azure_vision_version: str | None = None):
    ai_export_base = load_ai_export_path()
    records        = fetch_records_needing_normalisation(repo_guid)

    total   = len(records)
    success = 0

    logger.info("Normalisation: %d record(s) to process for repo %s", total, repo_guid)

    for record in records:
        raw_path_relative = extract_metadata_path(record, "metadataRawJsonFilePath")
        if not raw_path_relative:
            logger.warning("⚠ No raw path for: %s", record.get("fileName"))
            continue

        raw_path_full = resolve_physical_path(ai_export_base, raw_path_relative)
        if not os.path.exists(raw_path_full):
            logger.warning("⚠ Raw file not on disk: %s", raw_path_full)
            continue

        # Derive norm path from raw path
        if raw_path_relative.endswith("_raw.json"):
            norm_path_relative = raw_path_relative.replace("_raw.json", "_norm.json")
        else:
            base, ext = os.path.splitext(raw_path_relative)
            norm_path_relative = f"{base}_norm{ext}"

        norm_path_full = resolve_physical_path(ai_export_base, norm_path_relative)

        provider = record.get("providerName", "")

        # Already exists on disk — just make sure MongoDB knows about it
        if os.path.exists(norm_path_full):
            logger.info("⏭ Norm file already on disk: %s", norm_path_relative)
            if save_extended_metadata(
                repo_guid=repo_guid,
                provider=provider,
                file_path=record.get("fullPath", ""),
                raw_metadata_path=raw_path_relative,
                norm_metadata_path=norm_path_relative,
            ):
                success += 1
            continue

        logger.info("📝 Normalising [%s]: %s", provider, os.path.basename(raw_path_full))

        if normalize_json(provider, raw_path_full, norm_path_full, azure_vision_version):
            if save_extended_metadata(
                repo_guid=repo_guid,
                provider=provider,
                file_path=record.get("fullPath", ""),
                raw_metadata_path=raw_path_relative,
                norm_metadata_path=norm_path_relative,
            ):
                success += 1
        else:
            logger.error("✖ Failed to normalise: %s", raw_path_relative)

    logger.info("Normalisation complete: %d/%d succeeded", success, total)
    return success, total

# ── Step 2: Enrichment ────────────────────────────────────────────────────────

def process_enrichment(repo_guid: str, proxy_type: str):
    if proxy_type not in EVENT_TYPE_PROXY_VALUE_MAP:
        logger.error("Invalid proxy type: %s", proxy_type)
        return 0, 0

    proxy_prefix   = EVENT_TYPE_PROXY_VALUE_MAP[proxy_type]
    ai_export_base = load_ai_export_path()
    cursor         = fetch_records_with_norm_metadata(repo_guid)

    total   = 0
    success = 0

    for batch in chunked_cursor(cursor, BATCH_SIZE):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(send_ai_enriched_for_record, record, ai_export_base, proxy_prefix): record
                for record in batch
            }
            for future in as_completed(futures):
                total += 1
                try:
                    if future.result():
                        success += 1
                except Exception as exc:
                    record = futures[future]
                    logger.error("✖ Unhandled exception for %s: %s",
                                 record.get("fileName"), exc)

    logger.info("Enrichment complete: %d/%d succeeded", success, total)
    return success, total

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="AI Metadata Normalisation and Enrichment Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Normalise only
  %(prog)s --repo-guid REPO123 --normalize-only

  # Normalise then enrich
  %(prog)s --repo-guid REPO123 --proxy-type video_proxy

  # Enrich only (norm files must exist)
  %(prog)s --repo-guid REPO123 --enrich-only --proxy-type video_proxy
        """
    )
    parser.add_argument("--repo-guid",            required=True,
                        help="Repository GUID to process")
    parser.add_argument("--normalize-only",        action="store_true",
                        help="Only normalise raw JSON, skip enrichment")
    parser.add_argument("--enrich-only",           action="store_true",
                        help="Only enrich, skip normalisation")
    parser.add_argument("--proxy-type",            default="video_proxy",
                        choices=list(EVENT_TYPE_PROXY_VALUE_MAP.keys()),
                        help="Proxy type for sdnaEventTypePrefix mapping (default: video_proxy)")
    parser.add_argument("--azure-vision-version",
                        help="Azure Vision API version override (e.g. 4.0)")
    parser.add_argument("--workers",               type=int, default=MAX_WORKERS,
                        help=f"Thread pool size for enrichment (default: {MAX_WORKERS})")
    parser.add_argument("--batch-size",            type=int, default=BATCH_SIZE,
                        help=f"MongoDB batch size (default: {BATCH_SIZE})")
    parser.add_argument("--log-level",             default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging verbosity (default: INFO)")
    return parser.parse_args()


def main():
    args = parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    if args.normalize_only and args.enrich_only:
        logger.error("Cannot use --normalize-only and --enrich-only together")
        sys.exit(1)

    # Allow CLI overrides of module-level defaults
    global MAX_WORKERS, BATCH_SIZE
    MAX_WORKERS = args.workers
    BATCH_SIZE  = args.batch_size

    overall_ok = True

    # ── Step 1 ────────────────────────────────────────────────────────────────
    if not args.enrich_only:
        logger.info("=" * 60)
        logger.info("STEP 1: NORMALISATION")
        logger.info("=" * 60)
        norm_ok, norm_total = process_normalization(args.repo_guid, args.azure_vision_version)
        if norm_total > 0 and norm_ok < norm_total:
            overall_ok = False

    # ── Step 2 ────────────────────────────────────────────────────────────────
    if not args.normalize_only:
        logger.info("=" * 60)
        logger.info("STEP 2: ENRICHMENT")
        logger.info("=" * 60)
        enrich_ok, enrich_total = process_enrichment(args.repo_guid, args.proxy_type)
        if enrich_total > 0 and enrich_ok < enrich_total:
            overall_ok = False

    logger.info("✅ Processing complete!")
    sys.exit(0 if overall_ok else 2)


if __name__ == "__main__":
    main()