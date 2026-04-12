from __future__ import annotations

import argparse
import gzip
import io
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import requests
from minio import Minio

from jobs.common.config import (
    DEFAULT_EVENT_TYPES,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RAW_DATA_DIR,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download selected GH Archive files.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")
    parser.add_argument(
        "--hours",
        nargs="+",
        type=int,
        default=list(range(24)),
        help="Hours to download, for example 0 1 2 3.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(RAW_DATA_DIR),
        help="Directory for downloaded and filtered files.",
    )
    parser.add_argument(
        "--event-types",
        nargs="+",
        default=sorted(DEFAULT_EVENT_TYPES),
        help="Only keep these event types.",
    )
    parser.add_argument(
        "--upload-minio",
        action="store_true",
        help="Upload filtered output files to MinIO after download.",
    )
    parser.add_argument(
        "--minio-bucket",
        default=MINIO_BUCKET,
        help="MinIO bucket name used when --upload-minio is enabled.",
    )
    return parser


def build_url(target_date: date, hour: int) -> str:
    return (
        f"https://data.gharchive.org/"
        f"{target_date.year}-{target_date.month:02d}-{target_date.day:02d}-{hour}.json.gz"
    )


def filter_events(raw_bytes: bytes, event_types: set[str]) -> Iterable[dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz_file:
        for line in gz_file:
            if not line.strip():
                continue
            raw_event = json.loads(line)
            if raw_event.get("type") in event_types:
                yield raw_event


def download_hour(target_date: date, hour: int, output_dir: Path, event_types: set[str]) -> Path:
    url = build_url(target_date, hour)
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    output_path = output_dir / f"{target_date.isoformat()}-{hour:02d}.json"
    filtered_count = 0
    with output_path.open("w", encoding="utf-8") as file_obj:
        for event in filter_events(response.content, event_types):
            file_obj.write(json.dumps(event, ensure_ascii=False) + "\n")
            filtered_count += 1

    print(f"saved {output_path} with {filtered_count} filtered events")
    return output_path


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upload_to_minio(client: Minio, bucket: str, file_path: Path) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    object_name = f"raw/{file_path.name}"
    client.fput_object(bucket, object_name, str(file_path))
    print(f"uploaded {file_path.name} to minio bucket {bucket} as {object_name}")


def main() -> None:
    args = build_parser().parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    event_types = set(args.event_types)
    minio_client = get_minio_client() if args.upload_minio else None

    for hour in args.hours:
        output_path = download_hour(target_date, hour, output_dir, event_types)
        if minio_client is not None:
            upload_to_minio(minio_client, args.minio_bucket, output_path)


if __name__ == "__main__":
    main()
