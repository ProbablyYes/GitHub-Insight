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

CREATE TABLE IF NOT EXISTS github_analytics.batch_org_daily_metrics (
    metric_date Date,
    org_or_owner String,
    event_type LowCardinality(String),
    event_count UInt64,
    unique_actors UInt64,
    unique_repos UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, org_or_owner, event_type);

CREATE TABLE IF NOT EXISTS github_analytics.batch_org_rank_latest (
    metric_date Date,
    org_or_owner String,
    rank_no UInt64,
    hotness_score Float64,
    event_count UInt64,
    unique_repos UInt64,
    watch_events UInt64,
    fork_events UInt64,
    issues_events UInt64,
    pull_request_events UInt64,
    push_events UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, org_or_owner);

CREATE TABLE IF NOT EXISTS github_analytics.batch_event_action_day (
    metric_date Date,
    event_type LowCardinality(String),
    payload_action LowCardinality(String),
    event_count UInt64,
    unique_actors UInt64,
    unique_repos UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, event_type, payload_action);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_payload_size_day (
    metric_date Date,
    repo_name String,
    push_events UInt64,
    sum_payload_size UInt64,
    avg_payload_size Float64,
    p95_payload_size Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_payload_bucket_day (
    metric_date Date,
    size_bucket LowCardinality(String),
    event_count UInt64,
    avg_payload_size Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, size_bucket);

CREATE TABLE IF NOT EXISTS github_analytics.batch_user_segment_latest (
    metric_date Date,
    actor_login String,
    segment LowCardinality(String),
    event_count UInt64,
    active_days UInt64,
    unique_repos UInt64,
    favorite_event_type LowCardinality(String),
    favorite_repo String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, segment, event_count, actor_login);

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

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_health_latest (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    health_score Float64,
    total_events UInt64,
    unique_actors UInt64,
    push_events UInt64,
    watch_events UInt64,
    fork_events UInt64,
    issues_events UInt64,
    pull_request_events UInt64,
    bot_ratio Float64,
    dev_norm Float64,
    community_norm Float64,
    attention_norm Float64,
    diversity_norm Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, health_score, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_offline_anomaly_alerts (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    current_events UInt64,
    baseline_mean Float64,
    baseline_std Float64,
    z_score Float64,
    alert_level LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, alert_level, z_score, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_offline_decline_warnings (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    slope7 Float64,
    pct_change7 Float64,
    warning_level LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, warning_level, slope7, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_clusters (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    health_score Float64,
    cluster_id Int32,
    pca_x Float64,
    pca_y Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cluster_id, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_rank_explain_latest (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    hotness_part Float64,
    momentum_part Float64,
    engagement_part Float64,
    stability_part Float64,
    bot_penalty Float64,
    hotness_norm Float64,
    momentum_norm Float64,
    engagement_norm Float64,
    stability_norm Float64,
    bot_ratio_norm Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_concentration_day (
    metric_date Date,
    total_events UInt64,
    repo_count UInt64,
    top1_share Float64,
    top5_share Float64,
    hhi Float64,
    entropy Float64,
    normalized_entropy Float64,
    gini Float64 DEFAULT 0.0,
    events_delta_7d Float64 DEFAULT 0.0,
    repo_count_delta_7d Float64 DEFAULT 0.0,
    top5_share_delta_7d Float64 DEFAULT 0.0,
    gini_delta_7d Float64 DEFAULT 0.0,
    entropy_delta_7d Float64 DEFAULT 0.0,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date);

CREATE TABLE IF NOT EXISTS github_analytics.batch_ecosystem_changepoints (
    metric_date Date,
    kind LowCardinality(String),
    cusum Float64,
    z_score Float64,
    contribution_top_type LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, kind);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_cohort_day (
    metric_date Date,
    cohort LowCardinality(String),
    actors UInt64,
    events UInt64,
    avg_unique_repos Float64,
    avg_reactivation_gap_days Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cohort);

CREATE TABLE IF NOT EXISTS github_analytics.batch_event_type_share_shift_day (
    metric_date Date,
    event_type LowCardinality(String),
    event_count UInt64,
    share Float64,
    share_shift Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, event_type);

CREATE TABLE IF NOT EXISTS github_analytics.batch_offline_insights_latest (
    metric_date Date,
    insight_type LowCardinality(String),
    insight_text String,
    evidence_json String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, insight_type);

-- ── Repo deep-dive (v2): daily score history + drift + evidence chain ──

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_rank_score_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    hotness_part Float64,
    momentum_part Float64,
    engagement_part Float64,
    stability_part Float64,
    bot_penalty Float64,
    hotness_norm Float64,
    momentum_norm Float64,
    engagement_norm Float64,
    stability_norm Float64,
    bot_ratio_norm Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (repo_name, metric_date);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_rank_delta_explain_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    delta_rank_no Int32,
    delta_rank_score Float64,
    delta_hotness_part Float64,
    delta_momentum_part Float64,
    delta_engagement_part Float64,
    delta_stability_part Float64,
    delta_bot_penalty Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, delta_rank_score, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_contributor_concentration_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    total_events UInt64,
    active_actors UInt64,
    top1_actor_share Float64,
    top5_actor_share Float64,
    actor_hhi Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, top1_actor_share, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_top_actors_latest (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    actor_rank Int32,
    actor_login String,
    actor_category LowCardinality(String),
    actor_events UInt64,
    share Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, repo_name, actor_rank);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_event_type_share_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    event_type LowCardinality(String),
    event_count UInt64,
    total_events UInt64,
    share Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (repo_name, metric_date, event_type);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_event_mix_shift_day (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    js_divergence Float64,
    l1_distance Float64,
    total_abs_shift Float64,
    top_shift_event_type LowCardinality(String),
    top_shift_abs Float64,
    top_shift_signed Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (repo_name, metric_date);

-- ── L1 cluster interpretable profile ──
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_cluster_profile (
    metric_date Date,
    cluster_id Int32,
    cluster_label LowCardinality(String),
    members UInt64,
    share_of_repos Float64,
    avg_hotness Float64,
    avg_momentum Float64,
    avg_engagement Float64,
    avg_stability Float64,
    avg_bot_ratio Float64,
    avg_rank_score Float64,
    sample_repos String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cluster_id);

-- ── L2 hot-vs-cold attribution & repo DNA ──
CREATE TABLE IF NOT EXISTS github_analytics.batch_hot_vs_cold_attribution (
    metric_date Date,
    cohort_scope LowCardinality(String) DEFAULT 'all',
    feature_name LowCardinality(String),
    mean_hot Float64,
    mean_cold Float64,
    mean_diff Float64,
    cohen_d Float64,
    cohens_d_low Float64 DEFAULT 0.0,
    cohens_d_high Float64 DEFAULT 0.0,
    t_stat Float64,
    n_hot UInt64,
    n_cold UInt64,
    direction LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cohort_scope, feature_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_dna_outliers (
    metric_date Date,
    repo_name String,
    z_distance Float64,
    off_features String,
    watch_share Float64,
    pr_push_ratio Float64,
    bot_ratio Float64,
    active_days_ratio Float64,
    log_total_events Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, z_distance);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_dna (
    metric_date Date,
    repo_name String,
    cohort_group LowCardinality(String),
    watch_share Float64,
    fork_share Float64,
    issues_share Float64,
    pr_share Float64,
    push_share Float64,
    bot_ratio Float64,
    night_ratio Float64,
    weekend_ratio Float64,
    pr_push_ratio Float64,
    active_days_ratio Float64,
    actors_per_event Float64,
    event_entropy Float64,
    top1_actor_share Float64,
    log_total_events Float64,
    log_payload_p95 Float64,
    rank_score Float64,
    total_events UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cohort_group, repo_name);

-- ── L4 actor persona (GMM + PCA) ──
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_persona (
    metric_date Date,
    actor_login String,
    persona_id Int32,
    persona_label LowCardinality(String),
    is_bot Int32,
    event_count UInt64,
    active_days UInt64,
    unique_repos UInt64,
    night_ratio Float64,
    weekend_ratio Float64,
    push_share Float64,
    pr_share Float64,
    issues_share Float64,
    watch_share Float64,
    fork_share Float64,
    hour_entropy Float64,
    repo_entropy Float64,
    pca_x Float64,
    pca_y Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, persona_id, actor_login);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_persona_centroid (
    metric_date Date,
    persona_id Int32,
    persona_label LowCardinality(String),
    members UInt64,
    share Float64,
    log_event_count_avg Float64,
    active_days_f_avg Float64,
    log_unique_repos_avg Float64,
    night_ratio_avg Float64,
    weekend_ratio_avg Float64,
    hour_entropy_avg Float64,
    repo_entropy_avg Float64,
    push_share_avg Float64,
    pr_share_avg Float64,
    issues_share_avg Float64,
    watch_share_avg Float64,
    fork_share_avg Float64,
    is_bot_f_avg Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, persona_id);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_persona_bic (
    metric_date Date,
    k Int32,
    log_likelihood Float64,
    bic Float64,
    n_params Int32,
    n_samples UInt64,
    is_selected UInt8,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, k);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_persona_bot_validation (
    metric_date Date,
    persona_label LowCardinality(String),
    true_bots UInt64,
    false_bots UInt64,
    missed_bots UInt64,
    precision Float64,
    recall Float64,
    f1 Float64,
    total_bots UInt64,
    total_humans UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, persona_label);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_persona_transition (
    metric_date Date,
    persona_early LowCardinality(String),
    persona_late LowCardinality(String),
    actors UInt64,
    row_total UInt64,
    transition_prob Float64,
    is_stable UInt8,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, persona_early, persona_late);

-- ── L3 hot-repo watcher persona lift + profile ──
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_watcher_persona_lift (
    metric_date Date,
    repo_name String,
    persona_label LowCardinality(String),
    watchers UInt64,
    share_in_watchers Float64,
    share_in_global Float64,
    lift Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, repo_name, persona_label);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_watcher_profile (
    metric_date Date,
    repo_name String,
    rank_no UInt64,
    rank_score Float64,
    watchers UInt64,
    avg_night_ratio Float64,
    avg_pr_push_ratio Float64,
    avg_unique_repos Float64,
    bot_ratio Float64,
    dominant_persona LowCardinality(String),
    dominant_share Float64,
    dominant_lift Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no, repo_name);

-- ── L5 repo similarity / community / association rules ──
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_similarity_edges (
    metric_date Date,
    src_repo String,
    dst_repo String,
    shared_actors UInt64,
    jaccard Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, src_repo, jaccard);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_community (
    metric_date Date,
    repo_name String,
    community_id String,
    community_size UInt64,
    rank_no UInt64,
    rank_score Float64,
    sample_members String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, community_id, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_association_rules (
    metric_date Date,
    antecedent String,
    consequent String,
    antecedent_size Int32,
    support Float64,
    confidence Float64,
    lift Float64,
    is_frontier UInt8 DEFAULT 0,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, lift, confidence);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_community_profile (
    metric_date Date,
    community_id String,
    community_size UInt64,
    top_members String,
    avg_rank_score Float64,
    avg_active_days Float64,
    total_events UInt64,
    watch_share Float64,
    pr_push_ratio Float64,
    bot_ratio Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, community_id);


-- ═══════════════════════════════════════════════════════════════════════════
-- People-page depth analyses (batch, written by jobs/batch/people_depth.py)
-- ═══════════════════════════════════════════════════════════════════════════

-- Bucket A — collaboration network
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_collab_edge (
    metric_date Date,
    actor_a String,
    actor_b String,
    shared_repos UInt64,
    co_days UInt64,
    weight Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, actor_a, actor_b);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_graph_metrics (
    metric_date Date,
    actor_login String,
    pagerank Float64,
    betweenness Float64,
    community_id Int32,
    degree UInt64,
    persona_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, pagerank, actor_login);

-- Bucket B — retention & churn
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_burst_stability (
    metric_date Date,
    actor_login String,
    burst_index Float64,
    stability_index Float64,
    quadrant LowCardinality(String),
    rank_score Float64,
    total_events UInt64,
    persona_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, quadrant, rank_score);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_retention_curve (
    metric_date Date,
    cohort_week Date,
    days_since_first Int32,
    retained_count UInt64,
    cohort_size UInt64,
    retention_rate Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, cohort_week, days_since_first);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_churn_risk (
    metric_date Date,
    actor_login String,
    days_since_last Int32,
    decay_slope Float64,
    churn_prob Float64,
    risk_tier LowCardinality(String),
    persona_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, risk_tier, churn_prob);

-- Bucket C — individual influence
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_hotness (
    metric_date Date,
    actor_login String,
    event_count UInt64,
    unique_repos UInt64,
    avg_repo_rank_score Float64,
    hotness_score Float64,
    rank_no UInt64,
    persona_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_bus_factor (
    metric_date Date,
    repo_name String,
    top_actor String,
    top_actor_share Float64,
    contributor_count UInt64,
    bus_factor LowCardinality(String),
    repo_rank_score Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, bus_factor, top_actor_share);

-- Bucket D — authenticity / anomaly
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_bot_supervised (
    metric_date Date,
    actor_login String,
    xgb_prob_bot Float64,
    iforest_score Float64,
    combined_score Float64,
    is_bot_truth Int32,
    rank_no UInt64,
    persona_label LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no);

CREATE TABLE IF NOT EXISTS github_analytics.batch_bot_feature_importance (
    metric_date Date,
    feature String,
    importance Float64,
    rank_no Int32,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, rank_no);

CREATE TABLE IF NOT EXISTS github_analytics.batch_bot_classifier_meta (
    metric_date Date,
    metric String,
    value Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, metric);

CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_ring (
    metric_date Date,
    ring_id Int32,
    actor_count Int32,
    repos_shared Int32,
    sample_actors Array(String),
    avg_co_bursts Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, ring_id);

-- ====================================================================
-- Network-depth tables (7 directions across 3 phases)
-- ====================================================================

-- Phase 1 · Direction 2 · ALS matrix-factorization embedding
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_embedding (
    metric_date Date,
    repo_name String,
    rank_dim UInt16,
    factor_values Array(Float32),
    rank_score Float64,
    total_events UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_als_neighbor (
    metric_date Date,
    src_repo String,
    dst_repo String,
    cosine Float64,
    rank_no UInt16,
    is_jaccard_miss UInt8,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, src_repo, rank_no);

-- Phase 1 · Direction 1 · Multi-layer networks by event type
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_layer_edge (
    metric_date Date,
    layer LowCardinality(String),
    src_repo String,
    dst_repo String,
    jaccard Float64,
    shared_actors UInt64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, layer, src_repo);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_layer_community (
    metric_date Date,
    layer LowCardinality(String),
    repo_name String,
    community_id String,
    community_size UInt32,
    rank_score Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, layer, community_id);

-- Phase 1 · Direction 4 · k-core decomposition
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_coreness (
    metric_date Date,
    actor_login String,
    coreness UInt32,
    degree UInt32,
    persona_label LowCardinality(String),
    is_bot UInt8,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, coreness, actor_login);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_coreness (
    metric_date Date,
    repo_name String,
    coreness UInt32,
    degree UInt32,
    rank_score Float64,
    cohort_group LowCardinality(String),
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, coreness, repo_name);

-- Phase 1 · Direction 5 · Meta-path similarity
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_metapath_sim (
    metric_date Date,
    path_type LowCardinality(String),
    src_repo String,
    dst_repo String,
    sim Float64,
    shared_actors UInt64,
    rank_no UInt16,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, path_type, src_repo, rank_no);

-- Phase 1 · Direction 7 · Repo archetype (rule + GMM validation)
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_archetype (
    metric_date Date,
    repo_name String,
    archetype_rule LowCardinality(String),
    archetype_gmm_id UInt8,
    archetype_confidence Float64,
    pca_x Float64,
    pca_y Float64,
    rank_score Float64,
    total_events UInt64,
    watch_share Float64,
    pr_push_ratio Float64,
    bot_ratio Float64,
    active_days UInt32,
    contributor_count UInt32,
    sample_note String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, archetype_rule, repo_name);

CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_archetype_centroid (
    metric_date Date,
    archetype_rule LowCardinality(String),
    archetype_gmm_id UInt8,
    members UInt32,
    avg_watch_share Float64,
    avg_pr_push_ratio Float64,
    avg_bot_ratio Float64,
    avg_active_days Float64,
    avg_rank_score Float64,
    rule_gmm_overlap Float64,
    sample_repos String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, archetype_rule);

-- Phase 3 · Direction 3 · Temporal community evolution
CREATE TABLE IF NOT EXISTS github_analytics.batch_repo_community_weekly (
    metric_date Date,
    week_idx UInt8,
    week_start Date,
    week_end Date,
    repo_name String,
    community_id String,
    community_size UInt32,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, week_idx, community_id);

CREATE TABLE IF NOT EXISTS github_analytics.batch_community_lineage (
    metric_date Date,
    lineage_id UInt32,
    week_idx UInt8,
    community_id String,
    prev_community_id String,
    event_type LowCardinality(String),
    members_count UInt32,
    overlap_jaccard Float64,
    sample_members String,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, week_idx, lineage_id);

-- Phase 4 · Direction 6 · Influence-maximization (Independent Cascade)
CREATE TABLE IF NOT EXISTS github_analytics.batch_actor_ic_reach (
    metric_date Date,
    k UInt16,
    strategy LowCardinality(String),
    seeds String,
    expected_reach Float64,
    reach_stddev Float64,
    sim_runs UInt32,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, strategy, k);

CREATE TABLE IF NOT EXISTS github_analytics.batch_seed_greedy (
    metric_date Date,
    seed_rank UInt16,
    actor_login String,
    persona_label LowCardinality(String),
    is_bot UInt8,
    marginal_gain Float64,
    cumulative_reach Float64,
    degree UInt32,
    pagerank Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (metric_date, seed_rank);

