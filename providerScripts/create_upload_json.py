import csv
import os
import subprocess
import logging
import time
import json
import sys
import argparse
from datetime import datetime
import argparse
import configparser

def get_config_values(filename):

    config = configparser.ConfigParser(interpolation=None)

    try:
        config.read(filename)
    except configparser.Error as e:
        print(f"Error reading configuration file: {e}")
        return {}

    config_data = {}
    for section in config.sections():
        config_data[section] = {}
        for key, value in config.items(section):
            config_data[section][key] = value

    return config_data

def get_config_provider(config_name):

    config_values = get_config_values('/etc/StorageDNA/DNAClientServices.conf')
    if config_values:
        cloud_config_file = f"{config_values['General']['cloudconfigfolder']}/cloud_targets.conf"

    if cloud_config_file is None or len(cloud_config_file) == 0:
        return ''

    config_values = get_config_values(cloud_config_file)
    if config_values:
        if not config_name in config_values:
            return ''
        else:
            values = config_values[config_name]
            if 'provider' in values:
                return values['provider']
    return ''


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Uploader Script Json creator")

    parser.add_argument("-t","--target-json-path", help="Path to JSON config file", required=True)
    parser.add_argument("-c","--config-name", help="Path to JSON config file", required=True)
    parser.add_argument("-e","--extensions", help="Extension list")
    parser.add_argument("-m","--mode", choices=["original", "proxy"],  default='original', help="Extension list", required=True)
    parser.add_argument("-i","--input-csv-file", help="Extension list", required=True)
    parser.add_argument("-u", "--upload-path", help="upload path")
    parser.add_argument("-p", "--proxy-path", help = "proxy path")
    parser.add_argument('-j', '--job-id', help='job id')
    parser.add_argument('-r', '--run-id', help='run id')
    parser.add_argument('-g', '--repo-guid', help='repo guid')
    parser.add_argument('-v', '--provider', help='provider')
    parser.add_argument('-b', '--bucket', help='bucket')
    parser.add_argument('-l', '--log-path', help='log path')
    parser.add_argument('-s', '--progress-path', help='progress path')
    parser.add_argument('-z', '--orig-size-limit', help='progress path')
    parser.add_argument('--proxy-type', help='progress path')
    parser.add_argument(
        "--thread-count",
        type=int,
        default=2,
        help="Number of threds (default: 2)",
    )
    parser.add_argument('-d','--upload-properties', action="store_true", default=False, help="Uplaod property flag default is false")
    parser.add_argument('--controller-ip', help='controller_ip')
    parser.add_argument('--job-guid', help='job_guid')
    
    args = parser.parse_args()

    if args.provider is None:
        provider = get_config_provider(args.config_name)

    extention_list = []
    property_bag = {}

    if not args.extensions is None:
        extension_list = args.extensions.split(",")

    property_bag['mode'] = args.mode
    if args.provider is None:
        property_bag['provider'] = get_config_provider(args.config_name)
    else:
        property_bag['provider']  = args.provider
    property_bag['logging_path']  = args.log_path
    property_bag['proxy_directory'] = args.proxy_path
    property_bag['progress_path'] = args.progress_path
    property_bag['thread_count'] = args.thread_count
    property_bag['extensions'] = extension_list
    property_bag['controller_address'] = args.controller_ip
    if not args.orig_size_limit is None:
        property_bag['original_file_size_limit'] = args.orig_size_limit
    property_bag['upload_path'] = args.upload_path
    property_bag['cloud_config_name'] = args.config_name
    property_bag['job_id'] = args.job_id
    property_bag['upload_properties'] = args.upload_properties
    property_bag['files_list'] = args.input_csv_file 
    property_bag['run_id'] = args.run_id
    property_bag['repo_guid'] = args.repo_guid
    property_bag['job_guid'] = args.job_guid
    property_bag['bucket'] = args.bucket
    property_bag['proxy_type'] = args.proxy_type
    target_path = args.target_json_path

    json_string = json.dumps(property_bag, indent=4)
    with open(target_path, "w") as file:
        file.write(json_string)

    print(json_string)

        



