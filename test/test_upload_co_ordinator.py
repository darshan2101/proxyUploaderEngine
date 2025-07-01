import subprocess
import os

def run_config_test(config_path, label):
    print(f"\n=== Testing mode: {label} ===")
    result = subprocess.run([
        "python3", "/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/proxy_upload_co_ordinator.py",
        "--json-path", config_path,
        # "--dry-run",
        "--log-prefix", f"logs/tessact/{label}"
    ], capture_output=True, text=True)

    print(f"[{label}] Return Code: {result.returncode}")
    if result.stdout:
        print(f"[{label}] STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"[{label}] STDERR:\n{result.stderr}")

if __name__ == "__main__":
    # run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test/test_config_proxy.json", "proxy")
    # run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test/test_config_original.json", "original")

    run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test_jsons/tessact_gen_proxy_test_config.json", "generate_intelligence_proxy")

    run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test_jsons/test_gen_video_proxy_config.json", "generate_video_proxy")

    run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test_jsons/test_gen_video_frame_proxy_config.json", "generate_video_frame_proxy")