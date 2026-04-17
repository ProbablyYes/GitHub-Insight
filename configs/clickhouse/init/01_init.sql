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

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_rank_daily (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    hotness_raw_total Float64,
    hotness_decayed_total Float64,
    momentum_latest Float64,
    avg_daily_unique_actors Float64,
    stability_index Float64,
    bot_ratio Float64,
    trend_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_trend_forecast (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    total_events UInt64,
    hotness_raw Float64,
    hotness_decayed Float64,
    ma3 Float64,
    ma7 Float64,
    momentum Float64,
    slope7 Float64,
    forecast_next_day Float64,
    trend_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (repo_name, metric_date);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_burst_stability (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    burst_index Float64,
    stability_index Float64,
    short_term_pressure Float64,
    events_7d_sum UInt64,
    events_28d_avg Float64,
    quadrant LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, quadrant, burst_index, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_developer_rhythm_heatmap (
    metric_date Date,
    day_of_week UInt8,
    hour_of_day UInt8,
    actor_category LowCardinality(String),
    event_count UInt64,
    unique_actors UInt64,
    unique_repos UInt64,
    avg_daily_events Float64,
    intensity_score Float64,
    peak_flag Bool,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, actor_category, day_of_week, hour_of_day);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_hotness_components (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    watch_events UInt64,
    fork_events UInt64,
    issues_events UInt64,
    pull_request_events UInt64,
    push_events UInt64,
    hotness_raw Float64,
    watch_contribution Float64,
    fork_contribution Float64,
    issues_contribution Float64,
    pull_request_contribution Float64,
    push_contribution Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_event_complexity_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    total_events UInt64,
    active_event_types UInt64,
    event_entropy Float64,
    normalized_entropy Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, normalized_entropy, repo_name);

