from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer

from jobs.common.clickhouse_client import insert_records
from jobs.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


HOTNESS_WEIGHTS = {
    "WatchEvent": 1.0,
    "ForkEvent": 3.0,
    "IssuesEvent": 2.0,
    "PullRequestEvent": 2.5,
    "PushEvent": 1.5,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lightweight Kafka consumer that materializes stream metrics into ClickHouse."
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--batch-size", type=int, default=1000)
    return parser


def flush_metrics(events: list[dict]) -> None:
    if not events:
        return

    event_metrics = []
    repo_scores = defaultdict(Counter)
    repo_totals = Counter()
    anomaly_rows = []

    for event in events:
        created_at = datetime.fromisoformat(event["created_at"])
        window_start = created_at.replace(second=0, microsecond=0)
        event_metrics.append(
            {
                "window_start": window_start,
                "event_type": event["event_type"],
                "repo_name": event["repo_name"],
                "actor_category": event["actor_category"],
                "event_count": 1,
            }
        )
        repo_scores[(window_start, event["repo_name"])][event["event_type"]] += 1
        repo_totals[event["repo_name"]] += 1

    repo_score_rows = []
    for (window_start, repo_name), counter in repo_scores.items():
        hotness_score = sum(
            counter[event_type] * HOTNESS_WEIGHTS.get(event_type, 1.0)
            for event_type in counter
        )
        repo_score_rows.append(
            {
                "window_start": window_start,
                "repo_name": repo_name,
                "hotness_score": hotness_score,
                "watch_events": counter.get("WatchEvent", 0),
                "fork_events": counter.get("ForkEvent", 0),
                "issue_events": counter.get("IssuesEvent", 0),
                "pull_request_events": counter.get("PullRequestEvent", 0),
                "push_events": counter.get("PushEvent", 0),
            }
        )

    average_events = sum(repo_totals.values()) / max(len(repo_totals), 1)
    for repo_name, current_events in repo_totals.items():
        anomaly_ratio = current_events / max(average_events, 1.0)
        if anomaly_ratio >= 2.0:
            anomaly_rows.append(
                {
                    "window_start": datetime.now(timezone.utc).replace(tzinfo=None),
                    "repo_name": repo_name,
                    "current_events": current_events,
                    "baseline_events": average_events,
                    "anomaly_ratio": anomaly_ratio,
                    "alert_level": "high" if anomaly_ratio >= 4.0 else "medium",
                }
            )

    insert_records("realtime_event_metrics", event_metrics)
    insert_records("realtime_repo_scores", repo_score_rows)
    insert_records("realtime_anomaly_alerts", anomaly_rows)


def main() -> None:
    args = build_parser().parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="github-stream-batch-analytics",
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )

    batch = []
    print("streaming consumer started")
    for message in consumer:
        batch.append(message.value)
        if len(batch) >= args.batch_size:
            flush_metrics(batch)
            print(f"materialized {len(batch)} events")
            batch = []


if __name__ == "__main__":
    main()
