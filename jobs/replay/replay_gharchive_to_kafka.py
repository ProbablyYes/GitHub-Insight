from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from kafka import KafkaProducer

from jobs.common.config import (
    DEFAULT_EVENT_TYPES,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    REALTIME_REPLAY_INPUT,
)
from jobs.common.event_schema import GithubEvent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay GH Archive events to Kafka.")
    parser.add_argument(
        "--input",
        default=str(REALTIME_REPLAY_INPUT),
        help="Input directory containing json files, or a single json replay file.",
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Kafka topic.")
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        default=300.0,
        help="Replay acceleration factor based on event timestamps.",
    )
    parser.add_argument(
        "--event-types",
        nargs="+",
        default=sorted(DEFAULT_EVENT_TYPES),
        help="Only replay selected event types.",
    )
    return parser


def iter_events(input_path: Path, event_types: set[str]):
    if input_path.is_file():
        files = [input_path]
    else:
        files = sorted(input_path.glob("*.json"))

    for path in files:
        with path.open("r", encoding="utf-8") as file_obj:
            for line in file_obj:
                if not line.strip():
                    continue
                raw_event = json.loads(line)
                if raw_event.get("type") not in event_types:
                    continue
                yield GithubEvent.from_raw_event(raw_event)


def replay_events(args: argparse.Namespace) -> None:
    input_dir = Path(args.input)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )

    previous_timestamp = None
    sent_count = 0
    for event in iter_events(input_dir, set(args.event_types)):
        current_timestamp = event.created_at.timestamp()
        if previous_timestamp is not None:
            gap_seconds = max(current_timestamp - previous_timestamp, 0.0)
            sleep_seconds = gap_seconds / max(args.speedup, 1.0)
            if sleep_seconds > 0:
                time.sleep(min(sleep_seconds, 2.0))

        producer.send(args.topic, value=event.to_record())
        previous_timestamp = current_timestamp
        sent_count += 1

        if sent_count % 1000 == 0:
            producer.flush()
            print(f"replayed {sent_count} events")

    producer.flush()
    print(f"finished replay, total events: {sent_count}")


def main() -> None:
    args = build_parser().parse_args()
    replay_events(args)


if __name__ == "__main__":
    main()
