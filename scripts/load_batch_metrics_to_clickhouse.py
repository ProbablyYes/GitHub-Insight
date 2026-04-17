from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from jobs.common.clickhouse_client import insert_records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load parquet outputs into ClickHouse.")
    parser.add_argument("--input", required=True, help="Directory containing parquet outputs.")
    return parser


def load_parquet_records(path: Path):
    if not path.exists():
        return []
    frame = pd.read_parquet(path)
    if frame.empty:
        return []
    return frame.to_dict(orient="records")


def main() -> None:
    args = build_parser().parse_args()
    input_dir = Path(args.input)

    daily_records = load_parquet_records(input_dir / "daily_metrics")
    pattern_records = load_parquet_records(input_dir / "activity_patterns")

    insert_records("batch_daily_metrics", daily_records)
    insert_records("batch_activity_patterns", pattern_records)
    print(
        "loaded records into ClickHouse: "
        f"{len(daily_records)} daily rows, {len(pattern_records)} activity rows"
    )


if __name__ == "__main__":
    main()
