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
    repo_rank_daily_records = load_parquet_records(input_dir / "repo_rank_daily")
    repo_trend_forecast_records = load_parquet_records(input_dir / "repo_trend_forecast")
    repo_burst_stability_records = load_parquet_records(input_dir / "repo_burst_stability")
    developer_rhythm_heatmap_records = load_parquet_records(input_dir / "developer_rhythm_heatmap")
    repo_hotness_components_records = load_parquet_records(input_dir / "repo_hotness_components")
    event_complexity_day_records = load_parquet_records(input_dir / "event_complexity_day")

    insert_records("batch_daily_metrics", daily_records)
    insert_records("batch_activity_patterns", pattern_records)
    insert_records("batch_language_day_trend", language_day_records)
    insert_records("batch_top_users_day", top_users_records)
    insert_records("batch_top_repos_day", top_repos_records)
    insert_records("batch_event_type_day", event_type_records)
    insert_records("batch_repo_rank_daily", repo_rank_daily_records)
    insert_records("batch_repo_trend_forecast", repo_trend_forecast_records)
    insert_records("batch_repo_burst_stability", repo_burst_stability_records)
    insert_records("batch_developer_rhythm_heatmap", developer_rhythm_heatmap_records)
    insert_records("batch_repo_hotness_components", repo_hotness_components_records)
    insert_records("batch_event_complexity_day", event_complexity_day_records)
    print(
        "loaded records into ClickHouse: "
        f"{len(daily_records)} daily rows, "
        f"{len(pattern_records)} activity rows, "
        f"{len(language_day_records)} language/day rows, "
        f"{len(top_users_records)} top-user rows, "
        f"{len(top_repos_records)} top-repo rows, "
        f"{len(event_type_records)} event-type rows, "
        f"{len(repo_rank_daily_records)} rank rows, "
        f"{len(repo_trend_forecast_records)} trend rows, "
        f"{len(repo_burst_stability_records)} burst-stability rows, "
        f"{len(developer_rhythm_heatmap_records)} rhythm rows, "
        f"{len(repo_hotness_components_records)} hotness-component rows, "
        f"{len(event_complexity_day_records)} complexity rows"
    )


if __name__ == "__main__":
    main()
