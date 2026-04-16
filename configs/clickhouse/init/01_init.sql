CREATE DATABASE IF NOT EXISTS github_analytics;

CREATE TABLE IF NOT EXISTS github_analytics.realtime_event_metrics (
    window_start DateTime,
    event_type LowCardinality(String),
    repo_name String,
    actor_category LowCardinality(String),
    event_count UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (window_start, event_type, repo_name, actor_category);

CREATE TABLE IF NOT EXISTS github_analytics.realtime_repo_scores (
    window_start DateTime,
    repo_name String,
    hotness_score Float64,
    watch_events UInt64,
    fork_events UInt64,
    issue_events UInt64,
    pull_request_events UInt64,
    push_events UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (window_start, hotness_score, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.realtime_anomaly_alerts (
    window_start DateTime,
    repo_name String,
    current_events UInt64,
    baseline_events Float64,
    anomaly_ratio Float64,
    alert_level LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (window_start, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_daily_metrics (
    metric_date Date,
    repo_name String,
    event_type LowCardinality(String),
    language_guess String,
    actor_category LowCardinality(String),
    event_count UInt64,
    unique_actors UInt64,
    avg_hour Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, repo_name, event_type);

CREATE TABLE IF NOT EXISTS github_analytics.batch_activity_patterns (
    metric_date Date,
    hour_of_day UInt8,
    day_of_week UInt8,
    actor_category LowCardinality(String),
    event_count UInt64,
    unique_repos UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, hour_of_day, day_of_week, actor_category);

CREATE TABLE IF NOT EXISTS github_analytics.batch_language_day_trend (
    metric_date Date,
    language_guess String,
    event_count UInt64,
    unique_repos UInt64,
    unique_actors UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, language_guess);

CREATE TABLE IF NOT EXISTS github_analytics.batch_top_users_day (
    metric_date Date,
    actor_login String,
    event_count UInt64,
    unique_repos UInt64,
    rank UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank, actor_login);

CREATE TABLE IF NOT EXISTS github_analytics.batch_top_repos_day (
    metric_date Date,
    repo_name String,
    event_count UInt64,
    unique_actors UInt64,
    rank UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_event_type_day (
    metric_date Date,
    event_type LowCardinality(String),
    event_count UInt64,
    unique_actors UInt64,
    unique_repos UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, event_type);
