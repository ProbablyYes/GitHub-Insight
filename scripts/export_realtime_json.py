from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from jobs.common.clickhouse_client import get_clickhouse_client


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export unified realtime JSON snapshot from ClickHouse."
    )
    parser.add_argument(
        "--output",
        default="data/realtime/realtime_snapshot.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=5,
        help="Export interval in seconds when running continuously.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Export once and exit.",
    )
    parser.add_argument(
        "--event-limit",
        type=int,
        default=50,
        help="Max rows for realtime_event_metrics section.",
    )
    parser.add_argument(
        "--repo-limit",
        type=int,
        default=20,
        help="Max rows for realtime_repo_scores section.",
    )
    parser.add_argument(
        "--alert-limit",
        type=int,
        default=20,
        help="Max rows for realtime_anomaly_alerts section.",
    )
    return parser


def query_rows(client, query: str) -> list[dict[str, Any]]:
    result = client.query(query)
    column_names = list(result.column_names)
    rows: list[dict[str, Any]] = []
    for row in result.result_rows:
        rows.append(dict(zip(column_names, row)))
    return rows


def build_snapshot(event_limit: int, repo_limit: int, alert_limit: int) -> dict[str, Any]:
    client = get_clickhouse_client()

    event_rows = query_rows(
        client,
        f"""
        SELECT
            toString(window_start) AS window_start,
            event_type,
            repo_name,
            actor_category,
            event_count
        FROM realtime_event_metrics
        ORDER BY window_start DESC, event_count DESC
        LIMIT {max(1, min(event_limit, 500))}
        """,
    )

    repo_rows = query_rows(
        client,
        f"""
        WITH latest_window AS (
            SELECT max(window_start) AS ws FROM realtime_repo_scores
        )
        SELECT
            toString((SELECT ws FROM latest_window)) AS window_start,
            repo_name,
            sum(hotness_score) AS hotness_score,
            sum(push_events) AS push_events,
            sum(watch_events) AS watch_events,
            sum(fork_events) AS fork_events
        FROM realtime_repo_scores
        WHERE window_start = (SELECT ws FROM latest_window)
        GROUP BY repo_name
        ORDER BY hotness_score DESC
        LIMIT {max(1, min(repo_limit, 500))}
        """,
    )

    alert_rows = query_rows(
        client,
        f"""
        SELECT
            toString(window_start) AS window_start,
            repo_name,
            max(current_events) AS current_events,
            max(baseline_events) AS baseline_events,
            round(max(anomaly_ratio), 2) AS anomaly_ratio,
            any(alert_level) AS alert_level
        FROM realtime_anomaly_alerts
        GROUP BY window_start, repo_name
        ORDER BY window_start DESC, anomaly_ratio DESC
        LIMIT {max(1, min(alert_limit, 500))}
        """,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "clickhouse",
        "realtime_event_metrics": event_rows,
        "realtime_repo_scores": repo_rows,
        "realtime_anomaly_alerts": alert_rows,
    }


def write_snapshot(snapshot: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output)
    interval_seconds = max(args.interval_seconds, 1)

    while True:
        snapshot = build_snapshot(args.event_limit, args.repo_limit, args.alert_limit)
        write_snapshot(snapshot, output_path)
        print(
            f"exported realtime snapshot to {output_path} at {snapshot['generated_at']}"
        )

        if args.once:
            break

        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
