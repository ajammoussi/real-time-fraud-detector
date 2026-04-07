"""Wait for the S3-compatible data lake endpoint and optionally create the bucket."""

from __future__ import annotations

import argparse
import sys
import time

from botocore.exceptions import BotoCoreError, ClientError

from config.settings import get_settings
from datalake.storage import s3_client


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for S3-compatible endpoint to become ready.")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds")
    parser.add_argument(
        "--create-bucket",
        action="store_true",
        help="Create configured data lake bucket if it does not exist",
    )
    args = parser.parse_args()

    cfg = get_settings()
    client = s3_client()

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            names = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
            if args.create_bucket and cfg.datalake_bucket not in names:
                client.create_bucket(Bucket=cfg.datalake_bucket)
                print(f"Created bucket: {cfg.datalake_bucket}")

            print(f"S3 endpoint ready: {cfg.seaweed_endpoint}")
            return 0
        except (BotoCoreError, ClientError, OSError) as exc:
            print(f"Waiting for S3 endpoint ({exc})...")
            time.sleep(2)

    print(f"Timed out waiting for S3 endpoint: {cfg.seaweed_endpoint}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
