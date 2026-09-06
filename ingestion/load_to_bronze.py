"""
Bronze Landing Script
Takes generated e-commerce data and writes raw JSON/JSONL artifacts to the
bronze landing zone (local filesystem ./data/bronze/ or MinIO S3 bucket).
"""

import os
import json
import yaml
from datetime import datetime, timezone
from ingestion.generate_data import generate_dataset, load_config

def save_to_filesystem(dataset, bronze_dir="data/bronze"):
    os.makedirs(bronze_dir, exist_ok=True)
    file_paths = {}

    for entity, records in dataset.items():
        file_path = os.path.join(bronze_dir, f"{entity}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)
        file_paths[entity] = file_path
        print(f"Bronze Landed: {file_path} ({len(records)} records)")

    # Save manifest with ingestion timestamp
    manifest_path = os.path.join(bronze_dir, "manifest.json")
    manifest = {
        "landed_at": datetime.now(timezone.utc).isoformat(),
        "entities": {k: len(v) for k, v in dataset.items()},
        "storage": "filesystem"
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return file_paths

def upload_to_minio(dataset, config):
    try:
        import boto3
        from botocore.client import Config

        minio_cfg = config.get("storage", {}).get("minio", {})
        endpoint = os.getenv("MINIO_ENDPOINT", minio_cfg.get("endpoint", "http://localhost:9000"))
        access_key = os.getenv("MINIO_ROOT_USER", minio_cfg.get("access_key", "minioadmin"))
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", minio_cfg.get("secret_key", "minioadmin"))
        bucket_name = os.getenv("MINIO_BUCKET", minio_cfg.get("bucket", "bronze"))

        if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
            endpoint = f"http://{endpoint}"

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4")
        )

        # Ensure bucket exists
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if bucket_name not in buckets:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created MinIO bucket '{bucket_name}'")

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        for entity, records in dataset.items():
            key = f"landing/{timestamp}/{entity}.json"
            body = json.dumps(records, default=str).encode("utf-8")
            s3.put_object(Bucket=bucket_name, Key=key, Body=body)
            # Also update latest
            latest_key = f"landing/latest/{entity}.json"
            s3.put_object(Bucket=bucket_name, Key=latest_key, Body=body)
            print(f"Uploaded to MinIO: s3://{bucket_name}/{key}")

        return True
    except Exception as e:
        print(f"Notice: MinIO upload skipped or not available ({e}). Local filesystem is active.")
        return False

def run_bronze_landing(config_path="ingestion/config.yaml"):
    config = load_config(config_path)
    dataset = generate_dataset(config)

    # 1. Always write to local bronze folder
    local_dir = config.get("storage", {}).get("local_bronze_dir", "data/bronze")
    file_paths = save_to_filesystem(dataset, bronze_dir=local_dir)

    # 2. Upload to MinIO if enabled or reachable
    if config.get("storage", {}).get("mode") == "minio" or os.getenv("MINIO_ENDPOINT"):
        upload_to_minio(dataset, config)

    print("Bronze landing completed successfully.")
    return file_paths

if __name__ == "__main__":
    run_bronze_landing()
