import subprocess
import os

def run_config_test(config_path, label):
    print(f"\n=== Testing mode: {label} ===")
    result = subprocess.run([
        "python3", "/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/proxy_upload_co_ordinator.py",
        "--json-path", config_path,
        # "--dry-run",
        "--log-prefix", f"extra/logs/{label}/{label}"
    ], capture_output=True, text=True)

    print(f"[{label}] Return Code: {result.returncode}")
    if result.stdout:
        print(f"[{label}] STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"[{label}] STDERR:\n{result.stderr}")

if __name__ == "__main__":
    os.makedirs("test_logs", exist_ok=True)
    run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test/test_config_proxy.json", "proxy")
    run_config_test("/home/darshan/Darshan/Dev/(python +node) Wrappers/proxyUploaderEngine/test/test_config_original.json", "original")
