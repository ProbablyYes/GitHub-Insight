from __future__ import annotations

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from jobs.common.clickhouse_client import get_clickhouse_client


def fetch_scalar(client, query: str) -> int:
    result = client.query(query)
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def main() -> None:
    client = get_clickhouse_client()

    event_rows = fetch_scalar(client, "SELECT count() FROM realtime_event_metrics")
    repo_rows = fetch_scalar(client, "SELECT count() FROM realtime_repo_scores")
    alert_rows = fetch_scalar(client, "SELECT count() FROM realtime_anomaly_alerts")

    print("=== Realtime Table Row Counts ===")
    print(f"realtime_event_metrics: {event_rows}")
    print(f"realtime_repo_scores: {repo_rows}")
    print(f"realtime_anomaly_alerts: {alert_rows}")
    print()

    print("=== Hot Repo Top 10 (latest window) ===")
    top_repos = client.query(
        """
        WITH latest_window AS (
            SELECT max(window_start) AS ws FROM realtime_repo_scores
        )
        SELECT
            repo_name,
            round(sum(hotness_score), 2) AS score,
            sum(push_events) AS push_events,
            sum(watch_events) AS watch_events,
            sum(fork_events) AS fork_events
        FROM realtime_repo_scores
        WHERE window_start = (SELECT ws FROM latest_window)
        GROUP BY repo_name
        ORDER BY score DESC
        LIMIT 10
        """
    )
    for idx, (repo_name, score, push_events, watch_events, fork_events) in enumerate(
        top_repos.result_rows, start=1
    ):
        print(
            f"{idx:02d}. {repo_name} | score={score} | "
            f"push={push_events} watch={watch_events} fork={fork_events}"
        )
    if not top_repos.result_rows:
        print("(empty)")
    print()

    print("=== Latest Anomaly Alerts (Top 10) ===")
    alerts = client.query(
        """
        SELECT
            toString(window_start) AS window_start,
            repo_name,
            current_events,
            round(baseline_events, 2) AS baseline_events,
            round(anomaly_ratio, 2) AS anomaly_ratio,
            alert_level
        FROM realtime_anomaly_alerts
        ORDER BY window_start DESC, anomaly_ratio DESC
        LIMIT 10
        """
    )
    for row in alerts.result_rows:
        print(
            f"{row[0]} | {row[1]} | current={row[2]} baseline={row[3]} "
            f"ratio={row[4]} level={row[5]}"
        )
    if not alerts.result_rows:
        print("(empty)")


if __name__ == "__main__":
    main()
