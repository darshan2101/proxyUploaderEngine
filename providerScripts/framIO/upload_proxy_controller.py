#!/usr/bin/env python3
import os
import sys
import json
import argparse
import subprocess
import fnmatch
import logging
from pathlib import Path

# --- Setup logging (updated below dynamically based on user input) ---
logger = logging.getLogger()


def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s: %(message)s')



def find_matching_file(directory, pattern, extensions):
    logging.debug(f"Searching for proxy pattern in: {directory}")
    
    try:
        base_path = Path(directory)
        if not base_path.exists():
            logging.error(f"Directory does not exist: {directory}")
            return None
            
        if not base_path.is_dir() or not os.access(directory, os.R_OK):
            logging.error(f"No read permission for directory: {directory}")
            return None

        # Direct pattern search using rglob
        for matched_file in base_path.rglob(pattern):
            if matched_file.is_file():
                if extensions:
                    if not any(matched_file.name.lower().endswith(ext.lower()) for ext in extensions):
                        logging.debug(f"File {matched_file.name} has wrong extension")
                        continue
                logging.debug(f"Found matching file: {str(matched_file)}")
                return str(matched_file)

        logging.debug(f"No files matching pattern '{pattern}' found in {directory}")
        return None

    except PermissionError as e:
        logging.error(f"Permission denied accessing directory: {e}")
        return None
    except Exception as e:
        logging.error(f"Error during file search: {e}")
        return None

def find_exact_file(directory, target_name):
    logging.debug(f"Searching for exact match: {target_name} in {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file == target_name:
                full_path = os.path.join(root, file)
                logging.debug(f"Matched original file: {full_path}")
                return full_path
    return None


def call_upload_script(script_path, source_file, config_name, dry_run):
    if dry_run:
        logging.info(f"[DRY-RUN] Would upload: {source_file} using config: {config_name}")
        return

    try:
        result = subprocess.run([
            sys.executable, script_path,
            "--mode", "upload",
            "--sourcename", source_file,
            "--config", config_name
        ], capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"Upload failed: {result.stderr.strip()}")
        else:
            logging.info(f"Upload successful: {result.stdout.strip()}")

    except Exception as e:
        logging.error(f"Exception during upload: {str(e)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source-filename", required=True, help="Source filename to look for")
    parser.add_argument("-c", "--config-filename", required=True, help="JSON config file path")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    parser.add_argument("--log-level", default="debug", help="Logging level (debug, info, warning, error)")
    args = parser.parse_args()

    setup_logging(args.log_level)

    source_filename = args.source_filename
    config_filename = args.config_filename
    dry_run = args.dry_run

    if not os.path.exists(config_filename):
        logging.error(f"Config file not found: {config_filename}")
        sys.exit(1)

    with open(config_filename) as f:
        config = json.load(f)

    mode = config.get("mode")
    if mode not in ["proxy", "original"]:
        logging.error(f"Invalid mode in config: {mode}")
        sys.exit(2)

    matched_file = None
    max_size = config.get("max_upload_size") if mode == "original" else None

    if mode == "original":
        search_dir = config.get("source_location")
        if not search_dir:
            logging.error("Missing 'source_location' for original mode.")
            sys.exit(3)
        matched_file = find_exact_file(search_dir, source_filename)
        if matched_file and max_size and os.path.getsize(matched_file) > max_size:
            logging.error(f"File exceeds max upload size ({max_size} bytes): {matched_file}")
            sys.exit(5)
    else:
        search_dir = config.get("proxy_directory")
        if not search_dir:
            logging.error("Missing 'proxy_directory' for proxy mode.")
            sys.exit(3)
            
        # Fix the pattern substitution
        source_name = os.path.splitext(source_filename)[0]  # Remove extension
        pattern = config.get("search_pattern", "").replace("{orig}", source_name)
        if not pattern:
            pattern = f"{source_filename}*"
            
        logging.debug(f"Using search directory: {search_dir}")
        logging.debug(f"Using search pattern: {pattern}")
        logging.debug(f'Using extensions filter: {config.get("extensions")}')  # Fixed quote usage
        
        matched_file = find_matching_file(search_dir, pattern, config.get("extensions"))

    if not matched_file:
        logging.error("No matching file found.")
        sys.exit(4)

    meta_file = config.get("metadata_file", "")
    if meta_file:
        os.environ["FRAMEIO_METADATA_FILE"] = meta_file
    call_upload_script("frameIO.py", matched_file, config_filename, dry_run)


if __name__ == '__main__':
    main()
