CREATE TABLE github_events (
    event_id STRING,
    event_type STRING,
    created_at STRING,
    actor_login STRING,
    actor_category STRING,
    repo_id BIGINT,
    repo_name STRING,
    public BOOLEAN,
    event_time AS TO_TIMESTAMP(created_at),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'github_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'github-stream-batch-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE realtime_event_metrics (
    window_start TIMESTAMP(3),
    event_type STRING,
    repo_name STRING,
    actor_category STRING,
    event_count BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO realtime_event_metrics
SELECT
    window_start,
    event_type,
    repo_name,
    actor_category,
    COUNT(*) AS event_count
FROM TABLE(
    TUMBLE(TABLE github_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, event_type, repo_name, actor_category;
