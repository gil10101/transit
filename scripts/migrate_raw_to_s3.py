"""Copy the local MinIO raw/ archive (replay asset) and static zips to the cloud
raw bucket. Bronze/silver Iceberg tables are NOT copied: their metadata embeds
absolute s3a://lakehouse/ paths; cloud tables start fresh and history can be
replayed from raw/ if ever needed.

Usage: uv run python scripts/migrate_raw_to_s3.py <target-raw-bucket>
"""

from __future__ import annotations

import sys

import boto3
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    target_bucket = sys.argv[1]
    src = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    dst = boto3.session.Session().client("s3")  # default AWS chain

    copied = 0
    for page in src.get_paginator("list_objects_v2").paginate(Bucket="raw"):
        for obj in page.get("Contents", []):
            body = src.get_object(Bucket="raw", Key=obj["Key"])["Body"].read()
            dst.put_object(Bucket=target_bucket, Key=obj["Key"], Body=body)
            copied += 1
            if copied % 200 == 0:
                print(f"{copied} objects copied", flush=True)
    print(f"done: {copied} objects -> s3://{target_bucket}")


if __name__ == "__main__":
    main()
