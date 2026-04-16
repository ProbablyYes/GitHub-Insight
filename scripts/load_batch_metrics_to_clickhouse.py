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
    language_day_records = load_parquet_records(input_dir / "language_day_trend")
    top_users_records = load_parquet_records(input_dir / "top_users_day")
    top_repos_records = load_parquet_records(input_dir / "top_repos_day")
    event_type_records = load_parquet_records(input_dir / "event_type_day")

    insert_records("batch_daily_metrics", daily_records)
    insert_records("batch_activity_patterns", pattern_records)
    insert_records("batch_language_day_trend", language_day_records)
    insert_records("batch_top_users_day", top_users_records)
    insert_records("batch_top_repos_day", top_repos_records)
    insert_records("batch_event_type_day", event_type_records)
    print(
        "loaded records into ClickHouse: "
        f"{len(daily_records)} daily rows, "
        f"{len(pattern_records)} activity rows, "
        f"{len(language_day_records)} language/day rows, "
        f"{len(top_users_records)} top-user rows, "
        f"{len(top_repos_records)} top-repo rows, "
        f"{len(event_type_records)} event-type rows"
    )


if __name__ == "__main__":
    main()
