from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CURATED_DATA_DIR = DATA_DIR / "curated"
SAMPLE_DATA_DIR = DATA_DIR / "sample"
REALTIME_REPLAY_INPUT = DATA_DIR / "raw_single" / "2024-01-03-05.json"

DEFAULT_EVENT_TYPES = {
    "PushEvent",
    "WatchEvent",
    "ForkEvent",
    "IssuesEvent",
    "PullRequestEvent",
}

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "github_events"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics"
CLICKHOUSE_DATABASE = "github_analytics"

MINIO_ENDPOINT = "localhost:9002"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "gharchive"
