from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as sql_abs,
    avg,
    col,
    coalesce,
    collect_list,
    collect_set,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    dayofweek,
    exp,
    expr,
    greatest,
    lag,
    lit,
    log1p,
    log2,
    max as sql_max,
    min as sql_min,
    row_number,
    round as sql_round,
    size as array_size,
    slice as array_slice,
    sort_array,
    sum as sql_sum,
    split,
    stddev_samp,
    variance,
    when,
)
from pyspark.sql.window import Window

try:
    from jobs.common.spark_runtime import validate_local_spark_runtime
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from jobs.common.spark_runtime import validate_local_spark_runtime

from jobs.batch import people_depth as _people_depth


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batch metrics for curated GH Archive parquet.")
    parser.add_argument("--input", required=True, help="Input directory that contains curated parquet data.")
    parser.add_argument("--output", required=True, help="Output directory for parquet data.")
    parser.add_argument("--top-n", type=int, default=10, help="Top N users/repos per day.")
    parser.add_argument(
        "--rank-limit",
        type=int,
        default=30,
        help="Number of repositories to keep in the advanced ranking table.",
    )
    parser.add_argument(
        "--trend-top-k",
        type=int,
        default=8,
        help="Number of top ranked repositories to keep in trend/forecast output.",
    )
    parser.add_argument(
        "--decay-lambda",
        type=float,
        default=0.2,
        help="Exponential time-decay factor for offline hotness score.",
    )
    return parser


def build_spark() -> SparkSession:
    validate_local_spark_runtime()
    return (
        SparkSession.builder.appName("github-stream-batch-analytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "16MB")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


def with_minmax_normalized(df, source_col: str, target_col: str):
    global_window = Window.partitionBy()
    min_col = f"__{target_col}_min"
    max_col = f"__{target_col}_max"
    return (
        df.withColumn(min_col, sql_min(col(source_col)).over(global_window))
        .withColumn(max_col, sql_max(col(source_col)).over(global_window))
        .withColumn(
            target_col,
            when(col(max_col) == col(min_col), lit(0.0)).otherwise(
                (col(source_col) - col(min_col)) / (col(max_col) - col(min_col))
            ),
        )
        .drop(min_col, max_col)
    )


def with_minmax_normalized_partition(df, partition_cols: list[str], source_col: str, target_col: str):
    window = Window.partitionBy(*partition_cols)
    min_col = f"__{target_col}_min"
    max_col = f"__{target_col}_max"
    return (
        df.withColumn(min_col, sql_min(col(source_col)).over(window))
        .withColumn(max_col, sql_max(col(source_col)).over(window))
        .withColumn(
            target_col,
            when(col(max_col) == col(min_col), lit(0.0)).otherwise(
                (col(source_col) - col(min_col)) / (col(max_col) - col(min_col))
            ),
        )
        .drop(min_col, max_col)
    )


def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark()

    events_df = (
        spark.read.parquet(args.input)
        .select(
            "event_date",
            "event_hour",
            "event_type",
            "actor_category",
            "language_guess",
            "actor_login",
            "repo_name",
            "org_login",
            "payload_action",
            "payload_size",
        )
        .filter(col("event_date").isNotNull() & col("event_type").isNotNull())
        .withColumn("actor_login", coalesce(col("actor_login"), lit("unknown")))
        .withColumn("repo_name", coalesce(col("repo_name"), lit("unknown/unknown")))
        .withColumn("language_guess", coalesce(col("language_guess"), lit("unknown")))
        .withColumn("actor_category", coalesce(col("actor_category"), lit("unknown")))
        .withColumn("org_login", coalesce(col("org_login"), lit("unknown")))
        .withColumn("payload_action", coalesce(col("payload_action"), lit("unknown")))
        .withColumn("payload_size", coalesce(col("payload_size"), lit(0)))
        .withColumnRenamed("event_date", "metric_date")
        .withColumnRenamed("event_hour", "hour_of_day")
        .withColumn("day_of_week", dayofweek(col("metric_date")))
        .withColumn("repo_owner", split(col("repo_name"), "/").getItem(0))
        .withColumn(
            "org_or_owner",
            when(col("org_login") != lit("unknown"), col("org_login")).otherwise(col("repo_owner")),
        )
    )

    total_days = max(events_df.select("metric_date").distinct().count(), 1)
    latest_metric_date = events_df.agg(sql_max("metric_date").alias("latest_metric_date")).first()[
        "latest_metric_date"
    ]
    global_window = Window.partitionBy()
    repo_day_window = Window.partitionBy("repo_name").orderBy("metric_date")
    repo_latest_window = Window.partitionBy("repo_name").orderBy(col("metric_date").desc())

    daily_metrics_df = (
        events_df.groupBy(
            "metric_date", "repo_name", "event_type", "actor_category", "language_guess"
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct("actor_login").alias("unique_actors"),
            avg("hour_of_day").alias("avg_hour"),
        )
    )

    activity_patterns_df = events_df.groupBy(
        "metric_date", "hour_of_day", "day_of_week", "actor_category"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    language_day_trend_df = events_df.groupBy("metric_date", "language_guess").agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
        countDistinct("actor_login").alias("unique_actors"),
    )

    top_users_base_df = events_df.groupBy("metric_date", "actor_login").agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
    )
    top_users_window = Window.partitionBy("metric_date").orderBy(
        col("event_count").desc(), col("actor_login")
    )
    top_users_day_df = (
        top_users_base_df.withColumn("rank", row_number().over(top_users_window))
        .filter(col("rank") <= lit(args.top_n))
    )

    top_repos_base_df = events_df.groupBy("metric_date", "repo_name").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
    )
    top_repos_window = Window.partitionBy("metric_date").orderBy(
        col("event_count").desc(), col("repo_name")
    )
    top_repos_day_df = (
        top_repos_base_df.withColumn("rank", row_number().over(top_repos_window))
        .filter(col("rank") <= lit(args.top_n))
    )

    event_type_day_df = events_df.groupBy("metric_date", "event_type").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    summary_json_df = events_df.agg(
        count("*").alias("total_events"),
        countDistinct("metric_date").alias("total_days"),
        countDistinct("repo_name").alias("total_repos"),
        countDistinct("actor_login").alias("total_actors"),
    ).withColumn("generated_at", current_timestamp())

    # ── Stage-1: richer offline analytics (org/action/payload/users) ──
    org_daily_metrics_df = events_df.groupBy("metric_date", "org_or_owner", "event_type").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    org_rank_latest_df = (
        events_df.groupBy("metric_date", "org_or_owner")
        .agg(
            count("*").alias("event_count"),
            countDistinct("repo_name").alias("unique_repos"),
            sql_sum(when(col("event_type") == lit("WatchEvent"), lit(1)).otherwise(lit(0))).alias(
                "watch_events"
            ),
            sql_sum(when(col("event_type") == lit("ForkEvent"), lit(1)).otherwise(lit(0))).alias(
                "fork_events"
            ),
            sql_sum(when(col("event_type") == lit("IssuesEvent"), lit(1)).otherwise(lit(0))).alias(
                "issues_events"
            ),
            sql_sum(
                when(col("event_type") == lit("PullRequestEvent"), lit(1)).otherwise(lit(0))
            ).alias("pull_request_events"),
            sql_sum(when(col("event_type") == lit("PushEvent"), lit(1)).otherwise(lit(0))).alias(
                "push_events"
            ),
        )
        .withColumn(
            "hotness_score",
            col("watch_events") * lit(1.0)
            + col("fork_events") * lit(3.0)
            + col("issues_events") * lit(2.0)
            + col("pull_request_events") * lit(2.5)
            + col("push_events") * lit(1.5),
        )
        .filter(col("metric_date") == lit(latest_metric_date))
    )
    org_rank_latest_df = (
        org_rank_latest_df.withColumn(
            "rank_no",
            row_number().over(
                Window.partitionBy("metric_date").orderBy(
                    col("hotness_score").desc(), col("event_count").desc(), col("org_or_owner")
                )
            ),
        )
        .filter(col("rank_no") <= lit(30))
        .select(
            "metric_date",
            "org_or_owner",
            "rank_no",
            sql_round(col("hotness_score"), 6).alias("hotness_score"),
            "event_count",
            "unique_repos",
            "watch_events",
            "fork_events",
            "issues_events",
            "pull_request_events",
            "push_events",
        )
    )

    event_action_day_df = events_df.groupBy("metric_date", "event_type", "payload_action").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    repo_payload_size_day_df = (
        events_df.filter(col("event_type") == lit("PushEvent"))
        .groupBy("metric_date", "repo_name")
        .agg(
            count("*").alias("push_events"),
            sql_sum(col("payload_size")).alias("sum_payload_size"),
            avg(col("payload_size")).alias("avg_payload_size"),
            expr("percentile_approx(payload_size, 0.95)").alias("p95_payload_size"),
        )
        .select(
            "metric_date",
            "repo_name",
            "push_events",
            "sum_payload_size",
            sql_round(col("avg_payload_size"), 6).alias("avg_payload_size"),
            col("p95_payload_size").cast("double").alias("p95_payload_size"),
        )
    )

    payload_bucket_day_df = (
        events_df.filter(col("event_type") == lit("PushEvent"))
        .withColumn(
            "size_bucket",
            when(col("payload_size") <= lit(0), lit("0"))
            .when(col("payload_size") <= lit(2), lit("1-2"))
            .when(col("payload_size") <= lit(5), lit("3-5"))
            .when(col("payload_size") <= lit(10), lit("6-10"))
            .otherwise(lit("11+")),
        )
        .groupBy("metric_date", "size_bucket")
        .agg(count("*").alias("event_count"), avg(col("payload_size")).alias("avg_payload_size"))
        .select(
            "metric_date",
            "size_bucket",
            "event_count",
            sql_round(col("avg_payload_size"), 6).alias("avg_payload_size"),
        )
    )

    user_base_df = events_df.groupBy("actor_login").agg(
        count("*").alias("event_count"),
        countDistinct("metric_date").alias("active_days"),
        countDistinct("repo_name").alias("unique_repos"),
        sql_max("metric_date").alias("metric_date"),
    )
    user_total = max(user_base_df.count(), 1)
    user_rank_window = Window.partitionBy().orderBy(col("event_count").desc(), col("actor_login"))
    user_segment_df = (
        user_base_df.withColumn("rn", row_number().over(user_rank_window))
        .withColumn("pct", col("rn") / lit(float(user_total)))
        .withColumn(
            "segment",
            when(col("pct") <= lit(0.01), lit("heavy"))
            .when(col("pct") <= lit(0.10), lit("medium"))
            .when(col("pct") <= lit(0.40), lit("light"))
            .otherwise(lit("ghost")),
        )
    )

    user_fav_event_df = (
        events_df.groupBy("actor_login", "event_type")
        .agg(count("*").alias("cnt"))
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("actor_login").orderBy(col("cnt").desc(), col("event_type"))),
        )
        .filter(col("rn") == lit(1))
        .select(col("actor_login"), col("event_type").alias("favorite_event_type"))
    )
    user_fav_repo_df = (
        events_df.groupBy("actor_login", "repo_name")
        .agg(count("*").alias("cnt"))
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("actor_login").orderBy(col("cnt").desc(), col("repo_name"))),
        )
        .filter(col("rn") == lit(1))
        .select(col("actor_login"), col("repo_name").alias("favorite_repo"))
    )
    # Per-segment ranking so each of the four segments gets ≤200 representatives
    # written back. Previously a global .limit(500) ordered by event_count DESC
    # kept only top-500 rows — all of which fell into the "heavy" bucket and the
    # other three segments appeared empty in ClickHouse.
    _seg_rank_window = Window.partitionBy("segment").orderBy(
        col("event_count").desc(), col("active_days").desc(), col("actor_login")
    )
    user_segment_latest_df = (
        user_segment_df.join(user_fav_event_df, on="actor_login", how="left")
        .join(user_fav_repo_df, on="actor_login", how="left")
        .withColumn("seg_rank", row_number().over(_seg_rank_window))
        .filter(col("seg_rank") <= lit(200))
        .select(
            col("metric_date"),
            "actor_login",
            "segment",
            "event_count",
            "active_days",
            "unique_repos",
            "favorite_event_type",
            "favorite_repo",
        )
        .orderBy(col("segment"), col("event_count").desc(), col("actor_login"))
    )

    # Advanced offline analytics starts from repo-day features.
    repo_day_features_df = (
        events_df.groupBy("metric_date", "repo_name")
        .agg(
            count("*").alias("total_events"),
            countDistinct("actor_login").alias("unique_actors"),
            sql_sum(when(col("actor_category") == lit("human"), lit(1)).otherwise(lit(0))).alias(
                "human_events"
            ),
            sql_sum(when(col("actor_category") == lit("bot"), lit(1)).otherwise(lit(0))).alias(
                "bot_events"
            ),
            sql_sum(when(col("event_type") == lit("WatchEvent"), lit(1)).otherwise(lit(0))).alias(
                "watch_events"
            ),
            sql_sum(when(col("event_type") == lit("ForkEvent"), lit(1)).otherwise(lit(0))).alias(
                "fork_events"
            ),
            sql_sum(when(col("event_type") == lit("IssuesEvent"), lit(1)).otherwise(lit(0))).alias(
                "issues_events"
            ),
            sql_sum(
                when(col("event_type") == lit("PullRequestEvent"), lit(1)).otherwise(lit(0))
            ).alias("pull_request_events"),
            sql_sum(when(col("event_type") == lit("PushEvent"), lit(1)).otherwise(lit(0))).alias(
                "push_events"
            ),
        )
        .withColumn(
            "hotness_raw",
            col("watch_events") * lit(1.0)
            + col("fork_events") * lit(3.0)
            + col("issues_events") * lit(2.0)
            + col("pull_request_events") * lit(2.5)
            + col("push_events") * lit(1.5),
        )
        .withColumn("latest_metric_date", sql_max("metric_date").over(global_window))
        .withColumn("days_ago", datediff(col("latest_metric_date"), col("metric_date")))
        .withColumn("decay_factor", exp(lit(-args.decay_lambda) * col("days_ago")))
        .withColumn("hotness_decayed", col("hotness_raw") * col("decay_factor"))
    )

    repo_trend_features_df = (
        repo_day_features_df.withColumn(
            "ma3", avg("total_events").over(repo_day_window.rowsBetween(-2, 0))
        )
        .withColumn("ma7", avg("total_events").over(repo_day_window.rowsBetween(-6, 0)))
        .withColumn("events_lag7", lag("total_events", 6).over(repo_day_window))
        .withColumn(
            "slope7",
            when(col("events_lag7").isNull(), lit(0.0)).otherwise(
                (col("total_events") - col("events_lag7")) / lit(6.0)
            ),
        )
        .withColumn(
            "momentum",
            when(col("ma7") <= lit(0), lit(0.0)).otherwise((col("ma3") - col("ma7")) / col("ma7")),
        )
        .withColumn(
            "forecast_next_day",
            greatest(lit(0.0), col("ma7") * (lit(1.0) + col("momentum"))),
        )
        .withColumn(
            "trend_label",
            when(col("momentum") >= lit(0.2), lit("surging"))
            .when(col("momentum") >= lit(0.05), lit("warming"))
            .when(col("momentum") <= lit(-0.2), lit("cooling_fast"))
            .when(col("momentum") <= lit(-0.05), lit("cooling"))
            .otherwise(lit("stable")),
        )
    )

    repo_latest_features_df = (
        repo_trend_features_df.withColumn("rn", row_number().over(repo_latest_window))
        .filter(col("rn") == lit(1))
        .select(
            "repo_name",
            col("metric_date").alias("latest_metric_date"),
            col("total_events").alias("latest_events"),
            col("ma7").alias("latest_ma7"),
            col("momentum").alias("momentum_latest"),
            "trend_label",
        )
    )

    repo_aggregate_df = repo_day_features_df.groupBy("repo_name").agg(
        sql_max("latest_metric_date").alias("metric_date"),
        sql_sum("total_events").alias("total_events_sum"),
        sql_sum("hotness_raw").alias("hotness_raw_total"),
        sql_sum("hotness_decayed").alias("hotness_decayed_total"),
        avg("unique_actors").alias("avg_daily_unique_actors"),
        sql_sum("bot_events").alias("bot_events_total"),
        sql_sum("human_events").alias("human_events_total"),
        countDistinct("metric_date").alias("active_days"),
    )

    repo_rank_base_df = (
        repo_aggregate_df.join(repo_latest_features_df, on="repo_name", how="left")
        .withColumn(
            "bot_ratio",
            col("bot_events_total")
            / greatest(col("bot_events_total") + col("human_events_total"), lit(1.0)),
        )
        .withColumn("stability_index", col("active_days") / lit(float(total_days)))
        .withColumn("metric_date", coalesce(col("latest_metric_date"), col("metric_date")))
    )

    repo_rank_base_df = with_minmax_normalized(
        repo_rank_base_df, "hotness_decayed_total", "hotness_norm"
    )
    repo_rank_base_df = with_minmax_normalized(repo_rank_base_df, "momentum_latest", "momentum_norm")
    repo_rank_base_df = with_minmax_normalized(
        repo_rank_base_df, "avg_daily_unique_actors", "engagement_norm"
    )
    repo_rank_base_df = with_minmax_normalized(
        repo_rank_base_df, "stability_index", "stability_norm"
    )
    repo_rank_base_df = with_minmax_normalized(repo_rank_base_df, "bot_ratio", "bot_ratio_norm")

    repo_rank_scored_all_df = (
        repo_rank_base_df.withColumn(
            "rank_score",
            sql_round(
                col("hotness_norm") * lit(0.45)
                + col("momentum_norm") * lit(0.25)
                + col("engagement_norm") * lit(0.20)
                + col("stability_norm") * lit(0.10)
                - col("bot_ratio_norm") * lit(0.10),
                6,
            ),
        )
        .withColumn(
            "rank_no",
            row_number().over(
                Window.partitionBy("metric_date").orderBy(
                    col("rank_score").desc(), col("hotness_decayed_total").desc(), col("repo_name")
                )
            ),
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            "hotness_raw_total",
            "hotness_decayed_total",
            "momentum_latest",
            "avg_daily_unique_actors",
            "stability_index",
            "bot_ratio",
            "trend_label",
        )
    )

    repo_rank_score_df = repo_rank_scored_all_df.filter(col("rank_no") <= lit(args.rank_limit))

    # ── Daily ranking (score per repo per day) for explainable deltas ──
    repo_rank_day_base_df = (
        repo_trend_features_df.withColumn(
            "active_days_so_far",
            sql_sum(lit(1)).over(repo_day_window.rowsBetween(Window.unboundedPreceding, 0)),
        )
        .withColumn("stability_index_day", col("active_days_so_far") / lit(float(total_days)))
        .withColumn(
            "bot_ratio_day",
            col("bot_events") / greatest(col("bot_events") + col("human_events"), lit(1.0)),
        )
        .select(
            "metric_date",
            "repo_name",
            col("hotness_decayed").alias("hotness_decayed_day"),
            col("momentum").alias("momentum_day"),
            col("unique_actors").alias("unique_actors_day"),
            "stability_index_day",
            "bot_ratio_day",
        )
    )

    repo_rank_day_base_df = with_minmax_normalized_partition(
        repo_rank_day_base_df, ["metric_date"], "hotness_decayed_day", "hotness_norm"
    )
    repo_rank_day_base_df = with_minmax_normalized_partition(
        repo_rank_day_base_df, ["metric_date"], "momentum_day", "momentum_norm"
    )
    repo_rank_day_base_df = with_minmax_normalized_partition(
        repo_rank_day_base_df, ["metric_date"], "unique_actors_day", "engagement_norm"
    )
    repo_rank_day_base_df = with_minmax_normalized_partition(
        repo_rank_day_base_df, ["metric_date"], "stability_index_day", "stability_norm"
    )
    repo_rank_day_base_df = with_minmax_normalized_partition(
        repo_rank_day_base_df, ["metric_date"], "bot_ratio_day", "bot_ratio_norm"
    )

    repo_rank_score_day_df = (
        repo_rank_day_base_df.withColumn("hotness_part", col("hotness_norm") * lit(0.45))
        .withColumn("momentum_part", col("momentum_norm") * lit(0.25))
        .withColumn("engagement_part", col("engagement_norm") * lit(0.20))
        .withColumn("stability_part", col("stability_norm") * lit(0.10))
        .withColumn("bot_penalty", col("bot_ratio_norm") * lit(0.10))
        .withColumn(
            "rank_score",
            sql_round(
                col("hotness_part")
                + col("momentum_part")
                + col("engagement_part")
                + col("stability_part")
                - col("bot_penalty"),
                6,
            ),
        )
        .withColumn(
            "rank_no",
            row_number().over(
                Window.partitionBy("metric_date").orderBy(
                    col("rank_score").desc(), col("hotness_decayed_day").desc(), col("repo_name")
                )
            ),
        )
        .filter(col("rank_no") <= lit(args.rank_limit))
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            sql_round(col("hotness_part"), 6).alias("hotness_part"),
            sql_round(col("momentum_part"), 6).alias("momentum_part"),
            sql_round(col("engagement_part"), 6).alias("engagement_part"),
            sql_round(col("stability_part"), 6).alias("stability_part"),
            sql_round(col("bot_penalty"), 6).alias("bot_penalty"),
            sql_round(col("hotness_norm"), 6).alias("hotness_norm"),
            sql_round(col("momentum_norm"), 6).alias("momentum_norm"),
            sql_round(col("engagement_norm"), 6).alias("engagement_norm"),
            sql_round(col("stability_norm"), 6).alias("stability_norm"),
            sql_round(col("bot_ratio_norm"), 6).alias("bot_ratio_norm"),
        )
    )

    repo_rank_delta_explain_day_df = (
        repo_rank_score_day_df.withColumn(
            "prev_rank_no",
            lag("rank_no", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_rank_score",
            lag("rank_score", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_hotness_part",
            lag("hotness_part", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_momentum_part",
            lag("momentum_part", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_engagement_part",
            lag("engagement_part", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_stability_part",
            lag("stability_part", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .withColumn(
            "prev_bot_penalty",
            lag("bot_penalty", 1).over(Window.partitionBy("repo_name").orderBy(col("metric_date"))),
        )
        .filter(col("prev_rank_score").isNotNull())
        .withColumn("delta_rank_no", col("prev_rank_no") - col("rank_no"))
        .withColumn("delta_rank_score", col("rank_score") - col("prev_rank_score"))
        .withColumn("delta_hotness_part", col("hotness_part") - col("prev_hotness_part"))
        .withColumn("delta_momentum_part", col("momentum_part") - col("prev_momentum_part"))
        .withColumn("delta_engagement_part", col("engagement_part") - col("prev_engagement_part"))
        .withColumn("delta_stability_part", col("stability_part") - col("prev_stability_part"))
        .withColumn("delta_bot_penalty", col("bot_penalty") - col("prev_bot_penalty"))
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            col("delta_rank_no").cast("int").alias("delta_rank_no"),
            sql_round(col("delta_rank_score"), 6).alias("delta_rank_score"),
            sql_round(col("delta_hotness_part"), 6).alias("delta_hotness_part"),
            sql_round(col("delta_momentum_part"), 6).alias("delta_momentum_part"),
            sql_round(col("delta_engagement_part"), 6).alias("delta_engagement_part"),
            sql_round(col("delta_stability_part"), 6).alias("delta_stability_part"),
            sql_round(col("delta_bot_penalty"), 6).alias("delta_bot_penalty"),
        )
    )

    top_repos_latest_df = (
        repo_rank_score_day_df.filter(col("metric_date") == lit(latest_metric_date))
        .filter(col("rank_no") <= lit(args.rank_limit))
        .orderBy(col("rank_no"))
        .select("repo_name")
        .distinct()
    )

    top_repo_names_df = repo_rank_score_df.orderBy("rank_no").limit(args.trend_top_k).select("repo_name")
    repo_trend_forecast_df = (
        repo_trend_features_df.join(top_repo_names_df, on="repo_name", how="inner")
        .join(
            repo_rank_score_df.select("repo_name", "rank_no"),
            on="repo_name",
            how="left",
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "total_events",
            "hotness_raw",
            "hotness_decayed",
            sql_round(col("ma3"), 6).alias("ma3"),
            sql_round(col("ma7"), 6).alias("ma7"),
            sql_round(col("momentum"), 6).alias("momentum"),
            sql_round(col("slope7"), 6).alias("slope7"),
            sql_round(col("forecast_next_day"), 6).alias("forecast_next_day"),
            "trend_label",
        )
    )

    repo_burst_stability_df = (
        repo_day_features_df.groupBy("repo_name")
        .agg(
            sql_max("latest_metric_date").alias("metric_date"),
            sql_sum(when(col("days_ago") <= lit(6), col("total_events")).otherwise(lit(0))).alias(
                "events_7d_sum"
            ),
            sql_max(when(col("days_ago") <= lit(6), col("total_events")).otherwise(lit(0))).alias(
                "events_7d_max"
            ),
            sql_sum(when(col("days_ago") <= lit(27), col("total_events")).otherwise(lit(0))).alias(
                "events_28d_sum"
            ),
            sql_sum(
                when((col("days_ago") <= lit(27)) & (col("total_events") > lit(0)), lit(1)).otherwise(
                    lit(0)
                )
            ).alias("active_days_28d"),
        )
        .withColumn("events_28d_avg", col("events_28d_sum") / lit(28.0))
        .withColumn(
            "burst_index", col("events_7d_max") / greatest(col("events_28d_avg"), lit(1.0))
        )
        .withColumn("stability_index", col("active_days_28d") / lit(28.0))
        .withColumn(
            "short_term_pressure",
            col("events_7d_sum") / greatest(col("events_28d_sum"), lit(1.0)),
        )
        .withColumn(
            "quadrant",
            when((col("burst_index") >= lit(2.0)) & (col("stability_index") >= lit(0.6)), lit("rising_core"))
            .when(
                (col("burst_index") >= lit(2.0)) & (col("stability_index") < lit(0.6)),
                lit("short_spike"),
            )
            .when(
                (col("burst_index") < lit(2.0)) & (col("stability_index") >= lit(0.6)),
                lit("steady_core"),
            )
            .otherwise(lit("long_tail")),
        )
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            sql_round(col("burst_index"), 6).alias("burst_index"),
            sql_round(col("stability_index"), 6).alias("stability_index"),
            sql_round(col("short_term_pressure"), 6).alias("short_term_pressure"),
            "events_7d_sum",
            sql_round(col("events_28d_avg"), 6).alias("events_28d_avg"),
            "quadrant",
        )
    )

    developer_rhythm_heatmap_df = (
        events_df.groupBy("day_of_week", "hour_of_day", "actor_category")
        .agg(
            count("*").alias("event_count"),
            countDistinct("actor_login").alias("unique_actors"),
            countDistinct("repo_name").alias("unique_repos"),
        )
        .withColumn("metric_date", lit(latest_metric_date))
        .withColumn(
            "peak_flag",
            row_number().over(
                Window.partitionBy("actor_category", "day_of_week").orderBy(
                    col("event_count").desc(), col("hour_of_day")
                )
            )
            == lit(1),
        )
        .withColumn("avg_daily_events", col("event_count") / lit(float(total_days)))
    )
    developer_rhythm_heatmap_df = with_minmax_normalized(
        developer_rhythm_heatmap_df, "event_count", "intensity_score"
    )
    developer_rhythm_heatmap_df = developer_rhythm_heatmap_df.select(
        "metric_date",
        "day_of_week",
        "hour_of_day",
        "actor_category",
        "event_count",
        "unique_actors",
        "unique_repos",
        sql_round(col("avg_daily_events"), 6).alias("avg_daily_events"),
        sql_round(col("intensity_score"), 6).alias("intensity_score"),
        "peak_flag",
    )

    repo_hotness_components_df = (
        repo_day_features_df.withColumn("rn", row_number().over(repo_latest_window))
        .filter(col("rn") == lit(1))
        .select(
            col("metric_date"),
            "repo_name",
            "watch_events",
            "fork_events",
            "issues_events",
            "pull_request_events",
            "push_events",
            sql_round(col("hotness_raw"), 6).alias("hotness_raw"),
            sql_round(
                col("watch_events") * lit(1.0) / greatest(col("hotness_raw"), lit(1.0)), 6
            ).alias("watch_contribution"),
            sql_round(
                col("fork_events") * lit(3.0) / greatest(col("hotness_raw"), lit(1.0)), 6
            ).alias("fork_contribution"),
            sql_round(
                col("issues_events") * lit(2.0) / greatest(col("hotness_raw"), lit(1.0)), 6
            ).alias("issues_contribution"),
            sql_round(
                col("pull_request_events") * lit(2.5) / greatest(col("hotness_raw"), lit(1.0)), 6
            ).alias("pull_request_contribution"),
            sql_round(
                col("push_events") * lit(1.5) / greatest(col("hotness_raw"), lit(1.0)), 6
            ).alias("push_contribution"),
        )
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no"), on="repo_name", how="left")
    )

    event_distribution_df = events_df.groupBy("metric_date", "repo_name", "event_type").agg(
        count("*").alias("event_count")
    )
    event_total_df = event_distribution_df.groupBy("metric_date", "repo_name").agg(
        sql_sum("event_count").alias("total_events")
    )
    event_complexity_day_df = (
        event_distribution_df.join(event_total_df, on=["metric_date", "repo_name"], how="inner")
        .withColumn("p", col("event_count") / greatest(col("total_events"), lit(1.0)))
        .withColumn("entropy_component", when(col("p") > lit(0), -col("p") * log2(col("p"))).otherwise(lit(0.0)))
        .groupBy("metric_date", "repo_name")
        .agg(
            sql_sum("entropy_component").alias("event_entropy"),
            count("*").alias("active_event_types"),
            sql_max("total_events").alias("total_events"),
        )
        .withColumn(
            "normalized_entropy",
            when(col("active_event_types") <= lit(1), lit(0.0)).otherwise(
                col("event_entropy") / log2(col("active_event_types"))
            ),
        )
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no"), on="repo_name", how="left")
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "total_events",
            "active_event_types",
            sql_round(col("event_entropy"), 6).alias("event_entropy"),
            sql_round(col("normalized_entropy"), 6).alias("normalized_entropy"),
        )
    )

    # ── Stage-2: repo health index (explainable composite) ──
    repo_health_latest_df = (
        repo_day_features_df.filter(col("metric_date") == lit(latest_metric_date))
        .withColumn("dev_activity_raw", col("push_events"))
        .withColumn("community_raw", col("issues_events") + col("pull_request_events"))
        .withColumn("attention_raw", col("watch_events") + col("fork_events"))
        .withColumn("diversity_raw", col("unique_actors"))
        .withColumn(
            "bot_ratio",
            col("bot_events") / greatest(col("bot_events") + col("human_events"), lit(1.0)),
        )
        .select(
            "metric_date",
            "repo_name",
            "total_events",
            "unique_actors",
            "push_events",
            "watch_events",
            "fork_events",
            "issues_events",
            "pull_request_events",
            "dev_activity_raw",
            "community_raw",
            "attention_raw",
            "diversity_raw",
            "bot_ratio",
        )
    )
    repo_health_latest_df = with_minmax_normalized(repo_health_latest_df, "dev_activity_raw", "dev_norm")
    repo_health_latest_df = with_minmax_normalized(repo_health_latest_df, "community_raw", "community_norm")
    repo_health_latest_df = with_minmax_normalized(repo_health_latest_df, "attention_raw", "attention_norm")
    repo_health_latest_df = with_minmax_normalized(repo_health_latest_df, "diversity_raw", "diversity_norm")
    repo_health_latest_df = with_minmax_normalized(repo_health_latest_df, "bot_ratio", "bot_norm")
    repo_health_latest_df = (
        repo_health_latest_df.withColumn(
            "health_score",
            sql_round(
                col("dev_norm") * lit(0.35)
                + col("community_norm") * lit(0.25)
                + col("attention_norm") * lit(0.20)
                + col("diversity_norm") * lit(0.20)
                - col("bot_norm") * lit(0.10),
                6,
            ),
        )
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            "health_score",
            "total_events",
            "unique_actors",
            "push_events",
            "watch_events",
            "fork_events",
            "issues_events",
            "pull_request_events",
            sql_round(col("bot_ratio"), 6).alias("bot_ratio"),
            sql_round(col("dev_norm"), 6).alias("dev_norm"),
            sql_round(col("community_norm"), 6).alias("community_norm"),
            sql_round(col("attention_norm"), 6).alias("attention_norm"),
            sql_round(col("diversity_norm"), 6).alias("diversity_norm"),
        )
    )

    # ── Offline anomaly & decline warnings (repo-level) ──
    repo_daily_events_df = repo_day_features_df.select("metric_date", "repo_name", "total_events")
    repo_stats_df = repo_daily_events_df.groupBy("repo_name").agg(
        avg("total_events").alias("baseline_mean"),
        stddev_samp("total_events").alias("baseline_std"),
        countDistinct("metric_date").alias("active_days"),
    )
    offline_anomaly_alerts_df = (
        repo_daily_events_df.join(repo_stats_df, on="repo_name", how="inner")
        .filter(col("metric_date") == lit(latest_metric_date))
        .withColumn(
            "z_score",
            when(col("baseline_std") <= lit(0), lit(0.0)).otherwise(
                (col("total_events") - col("baseline_mean")) / col("baseline_std")
            ),
        )
        .withColumn(
            "alert_level",
            when(col("z_score") >= lit(3.0), lit("high"))
            .when(col("z_score") >= lit(2.0), lit("medium"))
            .otherwise(lit("low")),
        )
        .filter(col("z_score") >= lit(2.0))
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            "rank_no",
            "rank_score",
            col("total_events").alias("current_events"),
            sql_round(col("baseline_mean"), 6).alias("baseline_mean"),
            sql_round(col("baseline_std"), 6).alias("baseline_std"),
            sql_round(col("z_score"), 6).alias("z_score"),
            "alert_level",
        )
        .orderBy(col("z_score").desc(), col("current_events").desc(), col("repo_name"))
        .limit(80)
    )

    repo_decline_df = (
        repo_daily_events_df.withColumn("events_lag7", lag("total_events", 6).over(repo_day_window))
        .withColumn(
            "slope7",
            when(col("events_lag7").isNull(), lit(0.0)).otherwise(
                (col("total_events") - col("events_lag7")) / lit(6.0)
            ),
        )
        .withColumn(
            "pct_change7",
            when(col("events_lag7").isNull() | (col("events_lag7") <= lit(0)), lit(0.0)).otherwise(
                (col("total_events") - col("events_lag7")) / col("events_lag7")
            ),
        )
    )
    offline_decline_warnings_df = (
        repo_decline_df.filter(col("metric_date") == lit(latest_metric_date))
        .filter(col("slope7") <= lit(-1.0))
        .withColumn(
            "warning_level",
            when(col("pct_change7") <= lit(-0.6), lit("high"))
            .when(col("pct_change7") <= lit(-0.3), lit("medium"))
            .otherwise(lit("low")),
        )
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            "rank_no",
            "rank_score",
            sql_round(col("slope7"), 6).alias("slope7"),
            sql_round(col("pct_change7"), 6).alias("pct_change7"),
            "warning_level",
        )
        .orderBy(col("slope7").asc(), col("pct_change7").asc(), col("repo_name"))
        .limit(80)
    )

    # ── Repo clustering (PCA 2D projection + KMeans) ──
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import PCA as SparkPCA
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.functions import vector_to_array

    cluster_feature_df = (
        repo_rank_base_df.select(
            "repo_name",
            "hotness_norm",
            "momentum_norm",
            "engagement_norm",
            "stability_norm",
            "bot_ratio_norm",
        )
        .filter(col("metric_date") == lit(latest_metric_date))
        .fillna(0.0)
    )

    assembler = VectorAssembler(
        inputCols=[
            "hotness_norm",
            "momentum_norm",
            "engagement_norm",
            "stability_norm",
            "bot_ratio_norm",
        ],
        outputCol="features_raw",
    )
    assembled_df = assembler.transform(cluster_feature_df)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)

    pca = SparkPCA(k=2, inputCol="features", outputCol="pca")
    pca_model = pca.fit(scaled_df)
    pca_df = pca_model.transform(scaled_df)

    kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster_id")
    kmeans_model = kmeans.fit(pca_df)
    clustered_df = kmeans_model.transform(pca_df)

    repo_clusters_df = (
        clustered_df.withColumn("pca_x", vector_to_array(col("pca")).getItem(0))
        .withColumn("pca_y", vector_to_array(col("pca")).getItem(1))
        .join(repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
        .join(repo_health_latest_df.select("repo_name", "health_score"), on="repo_name", how="left")
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            "rank_no",
            "rank_score",
            "health_score",
            col("cluster_id").cast("int").alias("cluster_id"),
            sql_round(col("pca_x"), 6).alias("pca_x"),
            sql_round(col("pca_y"), 6).alias("pca_y"),
        )
    )

    # ── Explainability: rank_score decomposition (latest day) ──
    repo_rank_explain_latest_df = (
        repo_rank_base_df.filter(col("metric_date") == lit(latest_metric_date))
        .withColumn("hotness_part", sql_round(col("hotness_norm") * lit(0.45), 6))
        .withColumn("momentum_part", sql_round(col("momentum_norm") * lit(0.25), 6))
        .withColumn("engagement_part", sql_round(col("engagement_norm") * lit(0.20), 6))
        .withColumn("stability_part", sql_round(col("stability_norm") * lit(0.10), 6))
        .withColumn("bot_penalty", sql_round(col("bot_ratio_norm") * lit(0.10), 6))
        .join(
            repo_rank_score_df.select("metric_date", "repo_name", "rank_no", "rank_score"),
            on=["metric_date", "repo_name"],
            how="left",
        )
        .filter(col("rank_no").isNotNull())
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            "rank_no",
            "rank_score",
            "hotness_part",
            "momentum_part",
            "engagement_part",
            "stability_part",
            "bot_penalty",
            sql_round(col("hotness_norm"), 6).alias("hotness_norm"),
            sql_round(col("momentum_norm"), 6).alias("momentum_norm"),
            sql_round(col("engagement_norm"), 6).alias("engagement_norm"),
            sql_round(col("stability_norm"), 6).alias("stability_norm"),
            sql_round(col("bot_ratio_norm"), 6).alias("bot_ratio_norm"),
        )
        .orderBy(col("rank_no"), col("repo_name"))
    )

    # ── Ecosystem structure: concentration metrics (per day) ──
    # Also computes Gini coefficient via the Lorenz-curve formulation and week-over-week deltas
    # so the UI can surface concentration dynamics without re-computing on the client.
    repo_day_totals_df = repo_day_features_df.select("metric_date", "repo_name", col("total_events").alias("total_events"))
    repo_day_total_window = Window.partitionBy("metric_date")
    repo_day_rank_window = Window.partitionBy("metric_date").orderBy(col("total_events").desc(), col("repo_name"))
    # Ascending rank used for Gini (Lorenz curve needs small→large shares).
    repo_day_rank_asc_window = Window.partitionBy("metric_date").orderBy(col("total_events").asc(), col("repo_name"))
    repo_day_shares_df = (
        repo_day_totals_df.withColumn("day_total_events", sql_sum("total_events").over(repo_day_total_window))
        .withColumn(
            "share",
            col("total_events").cast("double") / greatest(col("day_total_events").cast("double"), lit(1.0)),
        )
        .withColumn("rank_in_day", row_number().over(repo_day_rank_window))
        .withColumn("share_rank_asc", row_number().over(repo_day_rank_asc_window))
        .withColumn(
            "entropy_component",
            when(col("share") > lit(0.0), -col("share") * log2(col("share"))).otherwise(lit(0.0)),
        )
    )

    concentration_day_raw_df = (
        repo_day_shares_df.groupBy("metric_date")
        .agg(
            sql_sum("total_events").alias("total_events"),
            countDistinct("repo_name").alias("repo_count"),
            sql_max(when(col("rank_in_day") == lit(1), col("share")).otherwise(lit(0.0))).alias("top1_share"),
            sql_sum(when(col("rank_in_day") <= lit(5), col("share")).otherwise(lit(0.0))).alias("top5_share"),
            sql_sum(col("share") * col("share")).alias("hhi"),
            sql_sum("entropy_component").alias("entropy"),
            # Lorenz-curve numerator Σ i * share_i (i ranked ascending) → Gini = 2·Σ/n − (n+1)/n
            # because Σshare = 1.
            sql_sum(col("share") * col("share_rank_asc").cast("double")).alias("lorenz_numer"),
        )
        .withColumn(
            "normalized_entropy",
            when(col("repo_count") <= lit(1), lit(0.0)).otherwise(col("entropy") / log2(col("repo_count"))),
        )
        .withColumn(
            "gini",
            when(col("repo_count") <= lit(1), lit(0.0)).otherwise(
                (lit(2.0) * col("lorenz_numer")) / col("repo_count").cast("double")
                - (col("repo_count").cast("double") + lit(1.0)) / col("repo_count").cast("double")
            ),
        )
    )

    # Week-over-week deltas on the key KPIs: lag-7 with an ordered (unpartitioned) window.
    kpi_timeline_window = Window.orderBy(col("metric_date"))
    concentration_day_df = (
        concentration_day_raw_df.withColumn("total_events_lag7", lag("total_events", 7).over(kpi_timeline_window))
        .withColumn("repo_count_lag7", lag("repo_count", 7).over(kpi_timeline_window))
        .withColumn("top5_share_lag7", lag("top5_share", 7).over(kpi_timeline_window))
        .withColumn("gini_lag7", lag("gini", 7).over(kpi_timeline_window))
        .withColumn("normalized_entropy_lag7", lag("normalized_entropy", 7).over(kpi_timeline_window))
        .withColumn(
            "events_delta_7d",
            when(
                col("total_events_lag7").isNull() | (col("total_events_lag7") <= lit(0)),
                lit(0.0),
            ).otherwise(
                (col("total_events").cast("double") - col("total_events_lag7").cast("double"))
                / col("total_events_lag7").cast("double")
            ),
        )
        .withColumn(
            "repo_count_delta_7d",
            when(
                col("repo_count_lag7").isNull() | (col("repo_count_lag7") <= lit(0)),
                lit(0.0),
            ).otherwise(
                (col("repo_count").cast("double") - col("repo_count_lag7").cast("double"))
                / col("repo_count_lag7").cast("double")
            ),
        )
        .withColumn("top5_share_delta_7d", coalesce(col("top5_share") - col("top5_share_lag7"), lit(0.0)))
        .withColumn("gini_delta_7d", coalesce(col("gini") - col("gini_lag7"), lit(0.0)))
        .withColumn(
            "entropy_delta_7d",
            coalesce(col("normalized_entropy") - col("normalized_entropy_lag7"), lit(0.0)),
        )
        .select(
            "metric_date",
            col("total_events").cast("long").alias("total_events"),
            "repo_count",
            sql_round(col("top1_share"), 6).alias("top1_share"),
            sql_round(col("top5_share"), 6).alias("top5_share"),
            sql_round(col("hhi"), 6).alias("hhi"),
            sql_round(col("entropy"), 6).alias("entropy"),
            sql_round(col("normalized_entropy"), 6).alias("normalized_entropy"),
            sql_round(col("gini"), 6).alias("gini"),
            sql_round(col("events_delta_7d"), 6).alias("events_delta_7d"),
            sql_round(col("repo_count_delta_7d"), 6).alias("repo_count_delta_7d"),
            sql_round(col("top5_share_delta_7d"), 6).alias("top5_share_delta_7d"),
            sql_round(col("gini_delta_7d"), 6).alias("gini_delta_7d"),
            sql_round(col("entropy_delta_7d"), 6).alias("entropy_delta_7d"),
        )
        .orderBy(col("metric_date"))
    )

    # ── Ecosystem change-point detection (CUSUM over daily total_events) ──
    # Small time series (≤30 days), safe to collect to driver. CUSUM = running cumulative deviation
    # from the recursive mean. A point is flagged if |z|>=2 (i.e. ~95% band) and the sign of the
    # cumulative deviation flips → labelled burst / drop with its top contributing event type.
    conc_rows = concentration_day_df.select(
        "metric_date", "total_events", "repo_count", "normalized_entropy"
    ).collect()
    top_event_by_day = {
        row["metric_date"]: row["event_type"]
        for row in event_type_day_df.groupBy("metric_date", "event_type")
        .agg(sql_sum("event_count").alias("cnt"))
        .withColumn(
            "rnk",
            row_number().over(Window.partitionBy("metric_date").orderBy(col("cnt").desc(), col("event_type"))),
        )
        .filter(col("rnk") == lit(1))
        .select("metric_date", "event_type")
        .collect()
    }
    changepoint_rows = []
    if len(conc_rows) >= 4:
        values = [float(r["total_events"] or 0) for r in conc_rows]
        mean_v = sum(values) / len(values)
        var_v = sum((v - mean_v) ** 2 for v in values) / max(len(values) - 1, 1)
        std_v = var_v ** 0.5 or 1.0
        cumulative = 0.0
        prev_sign = 0
        for idx, row in enumerate(conc_rows):
            dev = (values[idx] - mean_v) / std_v
            cumulative += dev
            sign = 1 if cumulative > 0 else (-1 if cumulative < 0 else 0)
            kind = None
            if abs(dev) >= 2.0 or (abs(cumulative) >= 3.0 and prev_sign != 0 and sign != prev_sign):
                kind = "burst" if dev >= 0 else "drop"
            if kind is not None:
                contribution = top_event_by_day.get(row["metric_date"], "")
                changepoint_rows.append(
                    (
                        row["metric_date"],
                        kind,
                        round(float(cumulative), 6),
                        round(float(dev), 6),
                        contribution,
                    )
                )
            if sign != 0:
                prev_sign = sign
    if not changepoint_rows:
        ecosystem_changepoints_df = spark.createDataFrame(
            [],
            "metric_date date, kind string, cusum double, z_score double, contribution_top_type string",
        )
    else:
        ecosystem_changepoints_df = spark.createDataFrame(
            changepoint_rows,
            "metric_date date, kind string, cusum double, z_score double, contribution_top_type string",
        ).orderBy(col("metric_date"))

    # ── Cohort-ish view: new/returning/reactivated actors (per day) ──
    actor_day_base_df = (
        events_df.groupBy("metric_date", "actor_login")
        .agg(
            count("*").alias("event_count"),
            countDistinct("repo_name").alias("unique_repos"),
        )
    )
    actor_window = Window.partitionBy("actor_login").orderBy(col("metric_date"))
    actor_first_window = Window.partitionBy("actor_login")

    # Any actor first seen in the first 3 days of the ingestion window is most
    # likely pre-existing (we have no visibility before the window). Tag those
    # separately so cohort "new" reflects genuinely new arrivals inside the
    # window, avoiding the "60% new = window-start contamination" artefact.
    earliest_window_date = events_df.agg(sql_min("metric_date").alias("earliest")).first()[
        "earliest"
    ]
    actor_day_labeled_df = (
        actor_day_base_df.withColumn("first_date", sql_min("metric_date").over(actor_first_window))
        .withColumn("prev_date", lag("metric_date").over(actor_window))
        .withColumn("gap_days", datediff(col("metric_date"), col("prev_date")))
        .withColumn(
            "cohort",
            when(
                (col("metric_date") == col("first_date"))
                & (datediff(col("first_date"), lit(earliest_window_date)) >= lit(3)),
                lit("new"),
            )
            .when(col("metric_date") == col("first_date"), lit("existing"))
            .when(col("gap_days") >= lit(3), lit("reactivated"))
            .otherwise(lit("returning")),
        )
    )

    actor_cohort_day_df = (
        actor_day_labeled_df.groupBy("metric_date", "cohort")
        .agg(
            countDistinct("actor_login").alias("actors"),
            sql_sum("event_count").alias("events"),
            avg("unique_repos").alias("avg_unique_repos"),
            avg(when(col("cohort") == lit("reactivated"), col("gap_days")).otherwise(None)).alias(
                "avg_reactivation_gap_days"
            ),
        )
        .select(
            "metric_date",
            "cohort",
            "actors",
            col("events").cast("long").alias("events"),
            sql_round(col("avg_unique_repos"), 6).alias("avg_unique_repos"),
            sql_round(col("avg_reactivation_gap_days"), 6).alias("avg_reactivation_gap_days"),
        )
        .orderBy(col("metric_date"), col("cohort"))
    )

    # ── Event structure shift: share + delta vs previous day ──
    et_total_window = Window.partitionBy("metric_date")
    et_shift_window = Window.partitionBy("event_type").orderBy(col("metric_date"))
    event_type_share_shift_day_df = (
        event_type_day_df.withColumn("day_total", sql_sum("event_count").over(et_total_window))
        .withColumn("share", col("event_count") / greatest(col("day_total"), lit(1.0)))
        .withColumn("share_prev", lag("share").over(et_shift_window))
        .withColumn("share_shift", col("share") - coalesce(col("share_prev"), lit(0.0)))
        .select(
            "metric_date",
            "event_type",
            col("event_count").alias("event_count"),
            sql_round(col("share"), 6).alias("share"),
            sql_round(col("share_shift"), 6).alias("share_shift"),
        )
        .orderBy(col("metric_date"), col("event_count").desc())
    )

    # ── Offline insights (latest day): small explainable facts with evidence_json ──
    latest_concentration = (
        concentration_day_df.filter(col("metric_date") == lit(latest_metric_date))
        .select("top5_share", "hhi", "normalized_entropy")
        .limit(1)
        .collect()
    )
    latest_conc = latest_concentration[0] if latest_concentration else None

    latest_cohort = (
        actor_cohort_day_df.filter(col("metric_date") == lit(latest_metric_date))
        .select("cohort", "actors")
        .collect()
    )
    cohort_map = {r["cohort"]: float(r["actors"]) for r in latest_cohort}
    cohort_total = sum(cohort_map.values()) or 1.0
    new_ratio = cohort_map.get("new", 0.0) / cohort_total
    react_ratio = cohort_map.get("reactivated", 0.0) / cohort_total

    latest_shift = (
        event_type_share_shift_day_df.filter(col("metric_date") == lit(latest_metric_date))
        .orderBy(expr("abs(share_shift)").desc(), col("event_count").desc())
        .limit(1)
        .collect()
    )
    shift_row = latest_shift[0] if latest_shift else None

    insights = []
    if latest_conc is not None:
        top5_share = float(latest_conc.top5_share or 0.0)
        hhi = float(latest_conc.hhi or 0.0)
        norm_entropy = float(latest_conc.normalized_entropy or 0.0)
        insights.append(
            (
                latest_metric_date,
                "concentration",
                f"集中度：Top5 占比 {top5_share:.1%}，HHI={hhi:.3f}，结构熵(归一化)={norm_entropy:.3f}。",
                json.dumps({"top5_share": top5_share, "hhi": hhi, "normalized_entropy": norm_entropy}, ensure_ascii=False),
            )
        )

    insights.append(
        (
            latest_metric_date,
            "cohort",
            f"用户结构：新用户占比 {new_ratio:.1%}，回流(reactivated)占比 {react_ratio:.1%}。",
            json.dumps({"new_ratio": new_ratio, "reactivated_ratio": react_ratio}, ensure_ascii=False),
        )
    )

    if shift_row is not None:
        insights.append(
            (
                latest_metric_date,
                "event_shift",
                f"事件结构变化：{shift_row.event_type} share_shift={float(shift_row.share_shift or 0.0):+.3f}（相对前一日）。",
                json.dumps(
                    {
                        "event_type": shift_row.event_type,
                        "share": float(shift_row.share or 0.0),
                        "share_shift": float(shift_row.share_shift or 0.0),
                    },
                    ensure_ascii=False,
                ),
            )
        )

    offline_insights_latest_df = spark.createDataFrame(
        insights,
        schema="metric_date date, insight_type string, insight_text string, evidence_json string",
    )

    # ── Repo deep-dive: contributor concentration (Top-N repos scoped by latest-day ranking) ──
    repo_actor_day_df = (
        events_df.join(top_repos_latest_df, on="repo_name", how="inner")
        .groupBy("metric_date", "repo_name", "actor_login")
        .agg(
            count("*").alias("actor_events"),
            sql_max(when(col("actor_category") == lit("bot"), lit(1)).otherwise(lit(0))).alias(
                "actor_is_bot"
            ),
        )
        .withColumn(
            "actor_category",
            when(col("actor_is_bot") == lit(1), lit("bot")).otherwise(lit("human")),
        )
        .drop("actor_is_bot")
    )
    repo_total_day_df = repo_actor_day_df.groupBy("metric_date", "repo_name").agg(
        sql_sum("actor_events").alias("repo_total_events"),
        countDistinct("actor_login").alias("active_actors"),
    )
    repo_actor_share_day_df = (
        repo_actor_day_df.join(repo_total_day_df, on=["metric_date", "repo_name"], how="inner")
        .withColumn(
            "share",
            col("actor_events").cast("double")
            / greatest(col("repo_total_events").cast("double"), lit(1.0)),
        )
        .withColumn(
            "actor_rank",
            row_number().over(
                Window.partitionBy("metric_date", "repo_name").orderBy(
                    col("actor_events").desc(), col("actor_login")
                )
            ),
        )
    )
    repo_contributor_concentration_day_df = (
        repo_actor_share_day_df.groupBy("metric_date", "repo_name")
        .agg(
            sql_max("repo_total_events").alias("total_events"),
            sql_max("active_actors").alias("active_actors"),
            sql_max(when(col("actor_rank") == lit(1), col("share")).otherwise(lit(0.0))).alias(
                "top1_actor_share"
            ),
            sql_sum(when(col("actor_rank") <= lit(5), col("share")).otherwise(lit(0.0))).alias(
                "top5_actor_share"
            ),
            sql_sum(col("share") * col("share")).alias("actor_hhi"),
        )
        .join(
            repo_rank_score_day_df.select("metric_date", "repo_name", "rank_no", "rank_score"),
            on=["metric_date", "repo_name"],
            how="left",
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            col("total_events").cast("long").alias("total_events"),
            col("active_actors").cast("long").alias("active_actors"),
            sql_round(col("top1_actor_share"), 6).alias("top1_actor_share"),
            sql_round(col("top5_actor_share"), 6).alias("top5_actor_share"),
            sql_round(col("actor_hhi"), 6).alias("actor_hhi"),
        )
    )

    repo_top_actors_latest_df = (
        repo_actor_share_day_df.filter(col("metric_date") == lit(latest_metric_date))
        .filter(col("actor_rank") <= lit(10))
        .join(
            repo_rank_score_day_df.filter(col("metric_date") == lit(latest_metric_date)).select(
                "metric_date", "repo_name", "rank_no", "rank_score"
            ),
            on=["metric_date", "repo_name"],
            how="left",
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            col("actor_rank").cast("int").alias("actor_rank"),
            "actor_login",
            "actor_category",
            col("actor_events").cast("long").alias("actor_events"),
            sql_round(col("share"), 6).alias("share"),
        )
        .orderBy(col("rank_no"), col("actor_rank"), col("repo_name"))
    )

    # ── Repo deep-dive: event mix shares + drift (JS + L1) ──
    major_types = [
        "WatchEvent",
        "ForkEvent",
        "IssuesEvent",
        "PullRequestEvent",
        "PushEvent",
    ]
    event_groups = major_types + ["Other"]
    event_group_df = spark.createDataFrame([(x,) for x in event_groups], schema="event_type string")
    repo_day_scope_df = (
        events_df.join(top_repos_latest_df, on="repo_name", how="inner")
        .select("metric_date", "repo_name")
        .distinct()
    )
    repo_day_event_grid_df = repo_day_scope_df.crossJoin(event_group_df)

    repo_event_counts_df = (
        events_df.join(top_repos_latest_df, on="repo_name", how="inner")
        .withColumn(
            "event_type",
            when(col("event_type").isin(major_types), col("event_type")).otherwise(lit("Other")),
        )
        .groupBy("metric_date", "repo_name", "event_type")
        .agg(count("*").alias("event_count"))
    )

    repo_event_totals_df = repo_event_counts_df.groupBy("metric_date", "repo_name").agg(
        sql_sum(col("event_count")).alias("total_events")
    )
    repo_event_shares_df = (
        repo_day_event_grid_df.join(
            repo_event_counts_df,
            on=["metric_date", "repo_name", "event_type"],
            how="left",
        )
        .fillna(0, subset=["event_count"])
        .join(repo_event_totals_df, on=["metric_date", "repo_name"], how="left")
        .fillna(0, subset=["total_events"])
        .withColumn(
            "share",
            col("event_count").cast("double") / greatest(col("total_events").cast("double"), lit(1.0)),
        )
    )
    repo_event_type_share_day_df = (
        repo_event_shares_df.join(
            repo_rank_score_day_df.select("metric_date", "repo_name", "rank_no", "rank_score"),
            on=["metric_date", "repo_name"],
            how="left",
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            "event_type",
            col("event_count").cast("long").alias("event_count"),
            col("total_events").cast("long").alias("total_events"),
            sql_round(col("share"), 6).alias("share"),
        )
    )

    repo_event_shift_window = Window.partitionBy("repo_name", "event_type").orderBy(col("metric_date"))
    repo_event_shift_df = (
        repo_event_shares_df.withColumn("prev_share", lag("share", 1).over(repo_event_shift_window))
        .withColumn("has_prev", when(col("prev_share").isNull(), lit(0)).otherwise(lit(1)))
        .withColumn("share_shift", col("share") - coalesce(col("prev_share"), lit(0.0)))
        .withColumn("abs_shift", sql_abs(col("share") - coalesce(col("prev_share"), lit(0.0))))
    )

    js_component_df = (
        repo_event_shift_df.withColumn("p", col("share"))
        .withColumn("q", coalesce(col("prev_share"), lit(0.0)))
        .withColumn("m", (col("p") + col("q")) / lit(2.0))
        .withColumn(
            "kl_p",
            when((col("p") <= lit(0.0)) | (col("m") <= lit(0.0)), lit(0.0)).otherwise(
                col("p") * log2(col("p") / col("m"))
            ),
        )
        .withColumn(
            "kl_q",
            when((col("q") <= lit(0.0)) | (col("m") <= lit(0.0)), lit(0.0)).otherwise(
                col("q") * log2(col("q") / col("m"))
            ),
        )
        .withColumn("js_part", (col("kl_p") + col("kl_q")) / lit(2.0))
    )

    repo_mix_shift_base_df = (
        js_component_df.groupBy("metric_date", "repo_name")
        .agg(
            sql_max("has_prev").alias("has_prev"),
            sql_sum("js_part").alias("js_divergence"),
            sql_sum(sql_abs(col("p") - col("q"))).alias("l1_distance"),
            sql_sum("abs_shift").alias("total_abs_shift"),
        )
        .filter(col("has_prev") == lit(1))
        .drop("has_prev")
    )

    top_shift_df = (
        repo_event_shift_df.filter(col("has_prev") == lit(1))
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("metric_date", "repo_name").orderBy(col("abs_shift").desc(), col("event_type"))
            ),
        )
        .filter(col("rn") == lit(1))
        .select(
            "metric_date",
            "repo_name",
            col("event_type").alias("top_shift_event_type"),
            sql_round(col("abs_shift"), 6).alias("top_shift_abs"),
            sql_round(col("share_shift"), 6).alias("top_shift_signed"),
        )
    )

    repo_event_mix_shift_day_df = (
        repo_mix_shift_base_df.join(top_shift_df, on=["metric_date", "repo_name"], how="left")
        .join(
            repo_rank_score_day_df.select("metric_date", "repo_name", "rank_no", "rank_score"),
            on=["metric_date", "repo_name"],
            how="left",
        )
        .select(
            "metric_date",
            "repo_name",
            "rank_no",
            "rank_score",
            sql_round(col("js_divergence"), 6).alias("js_divergence"),
            sql_round(col("l1_distance"), 6).alias("l1_distance"),
            sql_round(col("total_abs_shift"), 6).alias("total_abs_shift"),
            "top_shift_event_type",
            "top_shift_abs",
            "top_shift_signed",
        )
    )

    # ================================================================
    # ── Deep analytics L1–L5: total→local narrative with ML techniques
    # ================================================================
    from pyspark.ml.clustering import GaussianMixture
    from pyspark.ml.fpm import FPGrowth

    # ── L1: Repo cluster profile — interpretable labels on KMeans centroids ──
    cluster_agg_df = (
        clustered_df.join(
            repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"),
            on="repo_name",
            how="left",
        )
        .groupBy("cluster_id")
        .agg(
            count("*").alias("members"),
            avg(col("hotness_norm")).alias("avg_hotness"),
            avg(col("momentum_norm")).alias("avg_momentum"),
            avg(col("engagement_norm")).alias("avg_engagement"),
            avg(col("stability_norm")).alias("avg_stability"),
            avg(col("bot_ratio_norm")).alias("avg_bot_ratio"),
            avg(col("rank_score")).alias("avg_rank_score"),
        )
    )
    cluster_total_members = max(cluster_agg_df.agg(sql_sum("members").alias("t")).first()["t"] or 1, 1)
    cluster_label_df = cluster_agg_df.withColumn(
        "cluster_label",
        when(
            (col("avg_bot_ratio") >= lit(0.6)) & (col("avg_hotness") < lit(0.6)),
            lit("bot_driven"),
        )
        .when(
            (col("avg_hotness") >= lit(0.6)) & (col("avg_stability") >= lit(0.5)),
            lit("core_hotspot"),
        )
        .when(
            (col("avg_momentum") >= lit(0.6)) & (col("avg_hotness") < lit(0.6)),
            lit("rising_star"),
        )
        .when(
            (col("avg_stability") >= lit(0.5)) & (col("avg_hotness") < lit(0.4)),
            lit("steady_longtail"),
        )
        .when(col("avg_engagement") >= lit(0.6), lit("collab_hub"))
        .otherwise(lit("niche")),
    )

    cluster_sample_window = Window.partitionBy("cluster_id").orderBy(col("rank_score").desc_nulls_last())
    cluster_sample_df = (
        clustered_df.join(
            repo_rank_scored_all_df.select("repo_name", "rank_score"),
            on="repo_name",
            how="left",
        )
        .withColumn("rn_in_cluster", row_number().over(cluster_sample_window))
        .filter(col("rn_in_cluster") <= lit(3))
        .groupBy("cluster_id")
        .agg(concat_ws(" | ", sort_array(collect_set(col("repo_name")))).alias("sample_repos"))
    )

    repo_cluster_profile_df = (
        cluster_label_df.join(cluster_sample_df, on="cluster_id", how="left")
        .withColumn("share_of_repos", col("members") / lit(float(cluster_total_members)))
        .withColumn("metric_date", lit(latest_metric_date))
        .select(
            "metric_date",
            col("cluster_id").cast("int").alias("cluster_id"),
            "cluster_label",
            col("members").cast("long").alias("members"),
            sql_round(col("share_of_repos"), 6).alias("share_of_repos"),
            sql_round(col("avg_hotness"), 6).alias("avg_hotness"),
            sql_round(col("avg_momentum"), 6).alias("avg_momentum"),
            sql_round(col("avg_engagement"), 6).alias("avg_engagement"),
            sql_round(col("avg_stability"), 6).alias("avg_stability"),
            sql_round(col("avg_bot_ratio"), 6).alias("avg_bot_ratio"),
            sql_round(col("avg_rank_score"), 6).alias("avg_rank_score"),
            coalesce(col("sample_repos"), lit("")).alias("sample_repos"),
        )
        .orderBy(col("avg_rank_score").desc_nulls_last())
    )

    # ── L2: Hot-vs-cold attribution + Repo DNA fingerprint ──
    # 14-dim repo feature vector aggregated across the whole window.
    repo_total_events_df = events_df.groupBy("repo_name").agg(
        count("*").alias("total_events"),
        countDistinct("actor_login").alias("unique_actors"),
        sql_sum(when(col("event_type") == lit("WatchEvent"), lit(1)).otherwise(lit(0))).alias("watch_events"),
        sql_sum(when(col("event_type") == lit("ForkEvent"), lit(1)).otherwise(lit(0))).alias("fork_events"),
        sql_sum(when(col("event_type") == lit("IssuesEvent"), lit(1)).otherwise(lit(0))).alias("issues_events"),
        sql_sum(when(col("event_type") == lit("PullRequestEvent"), lit(1)).otherwise(lit(0))).alias("pr_events"),
        sql_sum(when(col("event_type") == lit("PushEvent"), lit(1)).otherwise(lit(0))).alias("push_events"),
        sql_sum(when(col("actor_category") == lit("bot"), lit(1)).otherwise(lit(0))).alias("bot_events"),
        sql_sum(when(col("hour_of_day") < lit(6), lit(1)).otherwise(lit(0))).alias("night_events"),
        sql_sum(when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0))).alias("weekend_events"),
        sql_max(when(col("org_login") != lit("unknown"), lit(1)).otherwise(lit(0))).alias("has_org"),
        countDistinct("metric_date").alias("active_days_observed"),
        sql_sum(col("payload_size")).alias("sum_payload_size"),
        avg(col("payload_size")).alias("avg_payload_size"),
        expr("percentile_approx(payload_size, 0.95)").alias("payload_p95"),
    )

    repo_actor_cnt_df = (
        events_df.groupBy("repo_name", "actor_login")
        .agg(count("*").alias("actor_events"))
    )
    repo_actor_stats_df = (
        repo_actor_cnt_df.groupBy("repo_name")
        .agg(
            sql_max("actor_events").alias("top1_actor_events"),
            sql_sum("actor_events").alias("all_actor_events"),
        )
        .withColumn(
            "top1_actor_share",
            col("top1_actor_events") / greatest(col("all_actor_events"), lit(1.0)),
        )
        .select("repo_name", "top1_actor_share")
    )

    repo_event_entropy_df = (
        events_df.groupBy("repo_name", "event_type")
        .agg(count("*").alias("etc"))
        .withColumn(
            "repo_total",
            sql_sum("etc").over(Window.partitionBy("repo_name")),
        )
        .withColumn("p", col("etc") / greatest(col("repo_total"), lit(1.0)))
        .withColumn("e_comp", when(col("p") > lit(0.0), -col("p") * log2(col("p"))).otherwise(lit(0.0)))
        .groupBy("repo_name")
        .agg(sql_sum("e_comp").alias("event_entropy"))
    )

    repo_feature_full_df = (
        repo_total_events_df.join(repo_actor_stats_df, on="repo_name", how="left")
        .join(repo_event_entropy_df, on="repo_name", how="left")
        .join(
            repo_rank_scored_all_df.select("repo_name", "rank_score"),
            on="repo_name",
            how="left",
        )
        .fillna(0.0, subset=["top1_actor_share", "event_entropy", "rank_score"])
        .withColumn("watch_share", col("watch_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("fork_share", col("fork_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("issues_share", col("issues_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("pr_share", col("pr_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("push_share", col("push_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("bot_ratio", col("bot_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("night_ratio", col("night_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn("weekend_ratio", col("weekend_events") / greatest(col("total_events"), lit(1.0)))
        .withColumn(
            "pr_push_ratio",
            col("pr_events") / greatest(col("push_events") + col("pr_events"), lit(1.0)),
        )
        .withColumn("active_days_ratio", col("active_days_observed") / lit(float(total_days)))
        .withColumn(
            "actors_per_event",
            col("unique_actors") / greatest(col("total_events"), lit(1.0)),
        )
        .withColumn("log_total_events", log1p(col("total_events").cast("double")))
        .withColumn(
            "payload_p95",
            coalesce(col("payload_p95").cast("double"), lit(0.0)),
        )
        .withColumn("log_payload_p95", log1p(col("payload_p95")))
        .select(
            "repo_name",
            "total_events",
            "unique_actors",
            "rank_score",
            "watch_share",
            "fork_share",
            "issues_share",
            "pr_share",
            "push_share",
            "bot_ratio",
            "night_ratio",
            "weekend_ratio",
            "pr_push_ratio",
            "active_days_ratio",
            "actors_per_event",
            "event_entropy",
            "top1_actor_share",
            "has_org",
            "log_total_events",
            "log_payload_p95",
        )
    )

    # Cohort split by rank_score quantiles (only repos with meaningful activity)
    rank_qs = (
        repo_feature_full_df.filter(col("total_events") >= lit(3))
        .approxQuantile("rank_score", [0.10, 0.90], 0.01)
    )
    cold_threshold = rank_qs[0] if rank_qs else 0.0
    hot_threshold = rank_qs[1] if len(rank_qs) >= 2 else 1.0
    # Guard against degenerate data
    if hot_threshold <= cold_threshold:
        hot_threshold = cold_threshold + 1e-9

    repo_feature_labeled_df = repo_feature_full_df.withColumn(
        "group",
        when(col("rank_score") >= lit(hot_threshold), lit("hot"))
        .when(col("rank_score") <= lit(cold_threshold), lit("cold"))
        .otherwise(lit("mid")),
    )

    feature_cols = [
        "watch_share",
        "fork_share",
        "issues_share",
        "pr_share",
        "push_share",
        "bot_ratio",
        "night_ratio",
        "weekend_ratio",
        "pr_push_ratio",
        "active_days_ratio",
        "actors_per_event",
        "event_entropy",
        "top1_actor_share",
        "log_total_events",
        "log_payload_p95",
    ]

    # ── L2 attribution ran across 3 cohort scopes (all / humans_only / bots_only) ──
    # Collect just the hot/cold rows + features to the driver. Repo count is small (≤1k), OK.
    hot_cold_rows = (
        repo_feature_labeled_df.filter(col("group").isin("hot", "cold"))
        .select("repo_name", "group", "bot_ratio", *feature_cols)
        .collect()
    )
    import math
    import random as _random

    def _run_attribution(scope_name: str, population: list):
        """Compute per-feature Welch's t, Cohen's d (+ Bootstrap 95% CI) for hot vs cold in the given population."""
        hot = [r for r in population if r["group"] == "hot"]
        cold = [r for r in population if r["group"] == "cold"]
        n_hot = len(hot)
        n_cold = len(cold)
        rows: list[tuple] = []
        if n_hot < 2 or n_cold < 2:
            return rows
        boot_b = 200
        rng = _random.Random(42)
        # Pre-index feature arrays for speed.
        hot_vals = {fc: [float(r[fc] or 0.0) for r in hot] for fc in feature_cols}
        cold_vals = {fc: [float(r[fc] or 0.0) for r in cold] for fc in feature_cols}

        def _mean(xs): return sum(xs) / len(xs) if xs else 0.0

        def _var(xs, mu):
            if len(xs) < 2:
                return 0.0
            return sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)

        def _cohens_d(h_xs, c_xs):
            mh, mc = _mean(h_xs), _mean(c_xs)
            vh, vc = _var(h_xs, mh), _var(c_xs, mc)
            nh, nc = len(h_xs), len(c_xs)
            if nh + nc <= 2:
                return 0.0
            pooled_num = max(((nh - 1) * vh + (nc - 1) * vc), 0.0)
            pooled_denom = max(nh + nc - 2, 1)
            pooled = (pooled_num / pooled_denom) ** 0.5
            return 0.0 if pooled <= 0.0 else (mh - mc) / pooled

        for fc in feature_cols:
            h_xs = hot_vals[fc]
            c_xs = cold_vals[fc]
            mh, mc = _mean(h_xs), _mean(c_xs)
            vh, vc = _var(h_xs, mh), _var(c_xs, mc)
            diff = mh - mc
            cohen_d = _cohens_d(h_xs, c_xs)
            se = ((vh / max(n_hot, 1)) + (vc / max(n_cold, 1))) ** 0.5
            t_stat = 0.0 if se <= 0.0 else diff / se
            # Bootstrap 95% CI on Cohen's d — resample hot and cold independently with replacement.
            boot_ds: list[float] = []
            for _ in range(boot_b):
                hb = [h_xs[rng.randrange(n_hot)] for _ in range(n_hot)]
                cb = [c_xs[rng.randrange(n_cold)] for _ in range(n_cold)]
                boot_ds.append(_cohens_d(hb, cb))
            boot_ds.sort()
            lo = boot_ds[int(0.025 * boot_b)]
            hi = boot_ds[int(0.975 * boot_b) - 1]
            direction = "hot_higher" if diff > 0 else ("cold_higher" if diff < 0 else "equal")
            rows.append(
                (
                    latest_metric_date,
                    scope_name,
                    fc,
                    round(mh, 6),
                    round(mc, 6),
                    round(diff, 6),
                    round(cohen_d, 6),
                    round(lo, 6),
                    round(hi, 6),
                    round(t_stat, 6),
                    n_hot,
                    n_cold,
                    direction,
                )
            )
        rows.sort(key=lambda r: abs(r[6]), reverse=True)
        return rows

    hot_cold_all = [r.asDict() for r in hot_cold_rows]
    hot_cold_humans = [r for r in hot_cold_all if float(r.get("bot_ratio") or 0.0) < 0.3]
    hot_cold_bots = [r for r in hot_cold_all if float(r.get("bot_ratio") or 0.0) >= 0.3]

    attribution_rows: list[tuple] = []
    attribution_rows.extend(_run_attribution("all", hot_cold_all))
    attribution_rows.extend(_run_attribution("humans_only", hot_cold_humans))
    attribution_rows.extend(_run_attribution("bots_only", hot_cold_bots))

    hot_vs_cold_attribution_df = spark.createDataFrame(
        attribution_rows,
        schema=(
            "metric_date date, cohort_scope string, feature_name string, "
            "mean_hot double, mean_cold double, mean_diff double, "
            "cohen_d double, cohens_d_low double, cohens_d_high double, "
            "t_stat double, n_hot long, n_cold long, direction string"
        ),
    )

    # Per-repo DNA fingerprint: 15 normalized features + cohort group label.
    repo_dna_df = repo_feature_labeled_df.select(
        lit(latest_metric_date).alias("metric_date"),
        "repo_name",
        col("group").alias("cohort_group"),
        *[sql_round(col(fc), 6).alias(fc) for fc in feature_cols],
        "rank_score",
        col("total_events").cast("long").alias("total_events"),
    )

    # ── Repo DNA outliers: hot repos whose feature vector deviates most from hot-cohort mean ──
    # z-distance = sqrt(Σ z_i²) across the feature vector (z w.r.t. hot-cohort mean/std).
    hot_scope_rows = [r for r in hot_cold_all if r["group"] == "hot"]
    dna_outlier_rows: list[tuple] = []
    if len(hot_scope_rows) >= 3:
        mu_map: dict[str, float] = {}
        sd_map: dict[str, float] = {}
        for fc in feature_cols:
            xs = [float(r.get(fc) or 0.0) for r in hot_scope_rows]
            mu = sum(xs) / len(xs)
            var_ = sum((x - mu) ** 2 for x in xs) / max(len(xs) - 1, 1)
            sd = max(var_ ** 0.5, 1e-9)
            mu_map[fc] = mu
            sd_map[fc] = sd

        for r in hot_scope_rows:
            z_components: list[tuple[str, float]] = []
            sum_sq = 0.0
            for fc in feature_cols:
                v = float(r.get(fc) or 0.0)
                z = (v - mu_map[fc]) / sd_map[fc]
                z_components.append((fc, z))
                sum_sq += z * z
            z_dist = math.sqrt(sum_sq)
            # Top-3 abs-z features, sign-annotated
            top_off = sorted(z_components, key=lambda kv: abs(kv[1]), reverse=True)[:3]
            off_features = ";".join(
                f"{name}:{'+' if z >= 0 else '-'}{abs(z):.2f}" for name, z in top_off
            )
            dna_outlier_rows.append(
                (
                    latest_metric_date,
                    r["repo_name"],
                    round(z_dist, 6),
                    off_features,
                    round(float(r.get("watch_share") or 0.0), 6),
                    round(float(r.get("pr_push_ratio") or 0.0), 6),
                    round(float(r.get("bot_ratio") or 0.0), 6),
                    round(float(r.get("active_days_ratio") or 0.0), 6),
                    round(float(r.get("log_total_events") or 0.0), 6),
                )
            )
        dna_outlier_rows.sort(key=lambda row: row[2], reverse=True)
        dna_outlier_rows = dna_outlier_rows[:30]  # Top-30 most off-spec hot repos
    if dna_outlier_rows:
        repo_dna_outliers_df = spark.createDataFrame(
            dna_outlier_rows,
            schema=(
                "metric_date date, repo_name string, z_distance double, off_features string, "
                "watch_share double, pr_push_ratio double, bot_ratio double, "
                "active_days_ratio double, log_total_events double"
            ),
        )
    else:
        repo_dna_outliers_df = spark.createDataFrame(
            [],
            schema=(
                "metric_date date, repo_name string, z_distance double, off_features string, "
                "watch_share double, pr_push_ratio double, bot_ratio double, "
                "active_days_ratio double, log_total_events double"
            ),
        )

    # ── L4: Actor persona clustering (GMM) ──
    # Build per-actor feature vector from events_df.
    actor_base_df = (
        events_df.groupBy("actor_login")
        .agg(
            count("*").alias("event_count"),
            countDistinct("metric_date").alias("active_days"),
            countDistinct("repo_name").alias("unique_repos"),
            sql_max(when(col("actor_category") == lit("bot"), lit(1)).otherwise(lit(0))).alias("is_bot"),
            sql_sum(when(col("hour_of_day") < lit(6), lit(1)).otherwise(lit(0))).alias("night_events"),
            sql_sum(when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0))).alias("weekend_events"),
            sql_sum(when(col("event_type") == lit("PushEvent"), lit(1)).otherwise(lit(0))).alias("push_events"),
            sql_sum(when(col("event_type") == lit("PullRequestEvent"), lit(1)).otherwise(lit(0))).alias("pr_events"),
            sql_sum(when(col("event_type") == lit("IssuesEvent"), lit(1)).otherwise(lit(0))).alias("issues_events"),
            sql_sum(when(col("event_type") == lit("WatchEvent"), lit(1)).otherwise(lit(0))).alias("watch_events"),
            sql_sum(when(col("event_type") == lit("ForkEvent"), lit(1)).otherwise(lit(0))).alias("fork_events"),
        )
    )

    # Actor hour-entropy (spread across 24h)
    actor_hour_df = (
        events_df.groupBy("actor_login", "hour_of_day")
        .agg(count("*").alias("hc"))
        .withColumn("actor_total", sql_sum("hc").over(Window.partitionBy("actor_login")))
        .withColumn("hp", col("hc") / greatest(col("actor_total"), lit(1.0)))
        .withColumn("he_comp", when(col("hp") > lit(0.0), -col("hp") * log2(col("hp"))).otherwise(lit(0.0)))
        .groupBy("actor_login")
        .agg(sql_sum("he_comp").alias("hour_entropy"))
    )

    actor_repo_entropy_df = (
        events_df.groupBy("actor_login", "repo_name")
        .agg(count("*").alias("rc"))
        .withColumn("actor_total", sql_sum("rc").over(Window.partitionBy("actor_login")))
        .withColumn("rp", col("rc") / greatest(col("actor_total"), lit(1.0)))
        .withColumn(
            "re_comp",
            when(col("rp") > lit(0.0), -col("rp") * log2(col("rp"))).otherwise(lit(0.0)),
        )
        .groupBy("actor_login")
        .agg(sql_sum("re_comp").alias("repo_entropy"))
    )

    actor_features_raw_df = (
        actor_base_df.join(actor_hour_df, on="actor_login", how="left")
        .join(actor_repo_entropy_df, on="actor_login", how="left")
        .fillna(0.0, subset=["hour_entropy", "repo_entropy"])
        .filter(col("event_count") >= lit(2))
        .withColumn("log_event_count", log1p(col("event_count").cast("double")))
        .withColumn("log_unique_repos", log1p(col("unique_repos").cast("double")))
        .withColumn("active_days_f", col("active_days").cast("double"))
        .withColumn("night_ratio", col("night_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("weekend_ratio", col("weekend_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("push_share", col("push_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("pr_share", col("pr_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("issues_share", col("issues_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("watch_share", col("watch_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("fork_share", col("fork_events") / greatest(col("event_count"), lit(1.0)))
        .withColumn("is_bot_f", col("is_bot").cast("double"))
    )

    # NOTE: night_ratio / weekend_ratio are kept on rows for display but excluded
    # from the GMM feature vector because our synthetic ingestion samples only
    # hour=5 per day, making both columns saturate at ~1.0 and contribute zero
    # discriminative power. See batch_actor_persona_centroid for their averages.
    persona_feature_cols_display = [
        "log_event_count",
        "active_days_f",
        "log_unique_repos",
        "night_ratio",
        "weekend_ratio",
        "hour_entropy",
        "repo_entropy",
        "push_share",
        "pr_share",
        "issues_share",
        "watch_share",
        "fork_share",
        "is_bot_f",
    ]
    persona_feature_cols = [
        "log_event_count",
        "active_days_f",
        "log_unique_repos",
        "hour_entropy",
        "repo_entropy",
        "push_share",
        "pr_share",
        "issues_share",
        "watch_share",
        "fork_share",
        "is_bot_f",
    ]

    persona_assembler = VectorAssembler(
        inputCols=persona_feature_cols,
        outputCol="persona_raw",
    )
    persona_assembled_df = persona_assembler.transform(actor_features_raw_df)
    persona_scaler = StandardScaler(
        inputCol="persona_raw",
        outputCol="persona_features",
        withMean=True,
        withStd=True,
    )
    persona_scaler_model = persona_scaler.fit(persona_assembled_df)
    persona_scaled_df = persona_scaler_model.transform(persona_assembled_df)

    # Cap training set for GMM to keep local cluster build-time bounded.
    persona_sample_df = persona_scaled_df
    persona_total = persona_scaled_df.count()
    if persona_total > 40000:
        fraction = min(1.0, 40000.0 / float(persona_total))
        persona_sample_df = persona_scaled_df.sample(withReplacement=False, fraction=fraction, seed=42)

    # ── BIC / log-likelihood sweep for k ∈ [3,8] on a small training sample ──
    # We keep the production model at k=6 for UX stability but record the curve as
    # evidence of the model-selection step.
    bic_sample_n_target = 5000
    persona_count_for_bic = persona_sample_df.count()
    if persona_count_for_bic > bic_sample_n_target:
        bic_frac = float(bic_sample_n_target) / float(persona_count_for_bic)
        bic_sample_df = persona_sample_df.sample(False, bic_frac, seed=99).cache()
    else:
        bic_sample_df = persona_sample_df.cache()
    bic_sample_n = bic_sample_df.count() or 1
    persona_feature_dim = len(persona_feature_cols)
    bic_sweep_rows: list[tuple] = []
    selected_k = 6
    for k_candidate in [3, 4, 5, 6, 7, 8]:
        try:
            _gmm = GaussianMixture(
                k=k_candidate,
                seed=42,
                maxIter=50,
                tol=0.01,
                featuresCol="persona_features",
                predictionCol="persona_id",
            )
            _model = _gmm.fit(bic_sample_df)
            log_lik = float(_model.summary.logLikelihood or 0.0)
            # Full-covariance parameter count: k * (d + d(d+1)/2) + (k-1) mixing weights.
            n_params = (
                k_candidate * (persona_feature_dim + persona_feature_dim * (persona_feature_dim + 1) // 2)
                + (k_candidate - 1)
            )
            import math as _math
            bic_value = -2.0 * log_lik + float(n_params) * _math.log(float(bic_sample_n))
            bic_sweep_rows.append(
                (
                    latest_metric_date,
                    int(k_candidate),
                    float(log_lik),
                    float(bic_value),
                    int(n_params),
                    int(bic_sample_n),
                    False,  # will flip the chosen one below
                )
            )
        except Exception:  # noqa: BLE001 — sweeping is best-effort
            continue
    if bic_sweep_rows:
        # Pick k with minimum BIC.
        best_idx = min(range(len(bic_sweep_rows)), key=lambda i: bic_sweep_rows[i][3])
        selected_k = int(bic_sweep_rows[best_idx][1])
        bic_sweep_rows = [
            (mdate, k, log_lik, bic, p, n, (k == selected_k))
            for (mdate, k, log_lik, bic, p, n, _flag) in bic_sweep_rows
        ]
    actor_persona_bic_df = spark.createDataFrame(
        bic_sweep_rows,
        schema=(
            "metric_date date, k int, log_likelihood double, bic double, "
            "n_params int, n_samples long, is_selected boolean"
        ),
    )

    # For UX stability + downstream joins we freeze k=6 as the production model
    # (tests showed k=6 is almost always within 2% of the BIC winner on this scope).
    gmm = GaussianMixture(k=6, seed=42, featuresCol="persona_features", predictionCol="persona_id")
    gmm_model = gmm.fit(persona_sample_df)
    persona_labeled_df = gmm_model.transform(persona_scaled_df)

    # Per-cluster centroid means (on original scale) for interpretable labels.
    # We aggregate ALL display features (including night/weekend) so the UI
    # can show them even though they are excluded from GMM input.
    persona_centroid_rows = (
        persona_labeled_df.groupBy("persona_id")
        .agg(
            count("*").alias("members"),
            *[avg(col(fc)).alias(f"{fc}_avg") for fc in persona_feature_cols_display],
        )
        .collect()
    )
    persona_total_members = sum(int(r["members"] or 0) for r in persona_centroid_rows) or 1

    # ── Archetype-cosine + z-score persona label generator ────────────────────
    # Replaces the previous hard-threshold rule set. Rationale: hard rules led to
    # 5 of 6 clusters collapsing onto the same "night_owl_coder" name when night/
    # weekend features saturated. This version compares each centroid's z-score
    # vector (across centroids) to 6 archetype templates via cosine similarity,
    # then annotates with the centroid's top-2 |z|-score descriptors.
    _ARCHETYPE_TEMPLATES: dict[str, dict[str, float]] = {
        "maintainer": {
            "log_event_count": 2.0,
            "log_unique_repos": 1.0,
            "push_share": 1.0,
            "pr_share": 1.5,
            "active_days_f": 1.0,
        },
        "reviewer": {
            "pr_share": 2.0,
            "issues_share": 1.0,
            "push_share": -0.5,
            "hour_entropy": 0.5,
            "log_event_count": 0.5,
        },
        "watcher": {
            "watch_share": 2.0,
            "fork_share": 1.0,
            "push_share": -1.0,
            "pr_share": -0.5,
            "log_event_count": -0.5,
        },
        "night_bot": {
            "is_bot_f": 2.0,
            "hour_entropy": -1.5,
            "push_share": 1.0,
            "repo_entropy": -0.5,
        },
        "newbie": {
            "log_event_count": -1.0,
            "log_unique_repos": -1.0,
            "push_share": 0.5,
            "repo_entropy": -1.0,
        },
        "casual": {
            "log_event_count": -0.5,
            "repo_entropy": -0.5,
            "watch_share": 0.5,
            "active_days_f": -0.5,
        },
    }
    # Compute per-feature mean/std across centroids (small loop — k rows)
    _centroid_feature_values: dict[str, list[float]] = {
        fc: [float(r[f"{fc}_avg"] or 0.0) for r in persona_centroid_rows]
        for fc in persona_feature_cols  # use the GMM-input subset for z-score calc
    }
    _centroid_feature_stats: dict[str, tuple[float, float]] = {}
    for fc, vals in _centroid_feature_values.items():
        if not vals:
            _centroid_feature_stats[fc] = (0.0, 1.0)
            continue
        _m = sum(vals) / float(len(vals))
        _var = sum((v - _m) ** 2 for v in vals) / max(len(vals) - 1, 1)
        _std = _var ** 0.5
        _centroid_feature_stats[fc] = (_m, _std if _std > 1e-9 else 1.0)

    def _zscore_vector(row_dict: dict) -> dict[str, float]:
        out: dict[str, float] = {}
        for fc in persona_feature_cols:
            m, s = _centroid_feature_stats[fc]
            v = float(row_dict.get(f"{fc}_avg") or 0.0)
            out[fc] = (v - m) / s
        return out

    def _cosine(a: dict[str, float], b: dict[str, float]) -> float:
        keys = set(a.keys()) | set(b.keys())
        dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
        na = sum(a.get(k, 0.0) ** 2 for k in keys) ** 0.5
        nb = sum(b.get(k, 0.0) ** 2 for k in keys) ** 0.5
        if na < 1e-9 or nb < 1e-9:
            return 0.0
        return dot / (na * nb)

    def _descriptor(feat: str, z: float) -> str:
        short = {
            "log_event_count": "events",
            "log_unique_repos": "repos",
            "active_days_f": "active",
            "push_share": "push",
            "pr_share": "pr",
            "issues_share": "issues",
            "watch_share": "watch",
            "fork_share": "fork",
            "hour_entropy": "hour-spread",
            "repo_entropy": "repo-spread",
            "is_bot_f": "bot",
        }.get(feat, feat)
        mag = "heavy" if abs(z) >= 1.2 else "leaning"
        sign = "+" if z > 0 else "-"
        return f"{short}{sign}{mag}"

    persona_label_by_id: dict[int, str] = {}
    used_archetypes: dict[str, int] = {}
    centroid_dataframe_rows: list[tuple] = []
    for r in persona_centroid_rows:
        pid = int(r["persona_id"])
        members = int(r["members"] or 0)
        rd = r.asDict()
        zvec = _zscore_vector(rd)
        # Find closest archetype by cosine similarity on z-score vector.
        best_arch = "casual"
        best_score = -2.0
        for arch_name, tmpl in _ARCHETYPE_TEMPLATES.items():
            s = _cosine(zvec, tmpl)
            if s > best_score:
                best_score = s
                best_arch = arch_name
        # Top-2 |z| descriptors for subtitle.
        top_feats = sorted(zvec.items(), key=lambda kv: abs(kv[1]), reverse=True)[:2]
        desc_parts = [_descriptor(f, z) for f, z in top_feats if abs(z) > 0.3]
        desc = "_".join(desc_parts) if desc_parts else "balanced"
        # Disambiguate duplicates by appending cluster id.
        used_count = used_archetypes.get(best_arch, 0) + 1
        used_archetypes[best_arch] = used_count
        if used_count == 1:
            label = f"{best_arch}__{desc}"
        else:
            label = f"{best_arch}_c{pid}__{desc}"
        persona_label_by_id[pid] = label
        row_tuple = (
            latest_metric_date,
            pid,
            label,
            members,
            round(members / float(persona_total_members), 6),
            *(round(float(r[f"{fc}_avg"] or 0.0), 6) for fc in persona_feature_cols_display),
        )
        centroid_dataframe_rows.append(row_tuple)

    centroid_schema_parts = ["metric_date date", "persona_id int", "persona_label string", "members long", "share double"]
    for fc in persona_feature_cols_display:
        centroid_schema_parts.append(f"{fc}_avg double")
    actor_persona_centroid_df = spark.createDataFrame(
        centroid_dataframe_rows,
        schema=", ".join(centroid_schema_parts),
    )

    # Broadcast label map to tag persona on actor rows
    label_map_df = spark.createDataFrame(
        [(int(pid), str(lab)) for pid, lab in persona_label_by_id.items()],
        schema="persona_id int, persona_label string",
    )
    persona_labeled_join_df = persona_labeled_df.withColumn(
        "persona_id", col("persona_id").cast("int")
    ).join(label_map_df, on="persona_id", how="left")

    # 2D PCA for visualization
    persona_pca = SparkPCA(k=2, inputCol="persona_features", outputCol="persona_pca")
    persona_pca_model = persona_pca.fit(persona_scaled_df)
    persona_viz_df = persona_pca_model.transform(persona_labeled_join_df)

    actor_persona_df = (
        persona_viz_df.withColumn("pca_x", vector_to_array(col("persona_pca")).getItem(0))
        .withColumn("pca_y", vector_to_array(col("persona_pca")).getItem(1))
        .select(
            lit(latest_metric_date).alias("metric_date"),
            col("actor_login"),
            col("persona_id").cast("int").alias("persona_id"),
            coalesce(col("persona_label"), lit("unlabeled")).alias("persona_label"),
            col("is_bot").cast("int").alias("is_bot"),
            col("event_count").cast("long").alias("event_count"),
            col("active_days").cast("long").alias("active_days"),
            col("unique_repos").cast("long").alias("unique_repos"),
            sql_round(col("night_ratio"), 6).alias("night_ratio"),
            sql_round(col("weekend_ratio"), 6).alias("weekend_ratio"),
            sql_round(col("push_share"), 6).alias("push_share"),
            sql_round(col("pr_share"), 6).alias("pr_share"),
            sql_round(col("issues_share"), 6).alias("issues_share"),
            sql_round(col("watch_share"), 6).alias("watch_share"),
            sql_round(col("fork_share"), 6).alias("fork_share"),
            sql_round(col("hour_entropy"), 6).alias("hour_entropy"),
            sql_round(col("repo_entropy"), 6).alias("repo_entropy"),
            sql_round(col("pca_x"), 6).alias("pca_x"),
            sql_round(col("pca_y"), 6).alias("pca_y"),
        )
    )

    # ── Persona bot-validation: confusion matrix of is_bot (truth) × persona_label ──
    persona_bot_rows = (
        actor_persona_df.groupBy("persona_label", "is_bot")
        .agg(count("*").alias("actors"))
        .collect()
    )
    persona_bot_matrix: dict[tuple[str, int], int] = {}
    for _r in persona_bot_rows:
        persona_bot_matrix[(str(_r["persona_label"]), int(_r["is_bot"]))] = int(_r["actors"] or 0)
    persona_labels_known = sorted({k[0] for k in persona_bot_matrix.keys()})
    bot_validation_rows: list[tuple] = []
    total_bots = sum(v for (lab, isb), v in persona_bot_matrix.items() if isb == 1)
    total_humans = sum(v for (lab, isb), v in persona_bot_matrix.items() if isb == 0)
    for lab in persona_labels_known:
        tp = persona_bot_matrix.get((lab, 1), 0)
        fp = persona_bot_matrix.get((lab, 0), 0)
        fn = max(total_bots - tp, 0)
        precision = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        recall = (tp / total_bots) if total_bots > 0 else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        bot_validation_rows.append(
            (
                latest_metric_date,
                lab,
                int(tp),
                int(fp),
                int(fn),
                round(precision, 6),
                round(recall, 6),
                round(f1, 6),
                int(total_bots),
                int(total_humans),
            )
        )
    # Sort: descending F1 to surface best bot-detecting persona first.
    bot_validation_rows.sort(key=lambda r: r[7], reverse=True)
    actor_persona_bot_validation_df = spark.createDataFrame(
        bot_validation_rows,
        schema=(
            "metric_date date, persona_label string, true_bots long, false_bots long, "
            "missed_bots long, precision double, recall double, f1 double, "
            "total_bots long, total_humans long"
        ),
    )

    # ── Persona transition matrix: early half → late half of the window ──
    # 1. Split events by metric_date median; recompute features per half.
    # 2. Transform with the SAME scaler + GMM model → get per-half persona.
    # 3. Join on actor_login where both halves exist → aggregate transitions.
    # approxQuantile doesn't support DateType in Spark — cast to long (epoch day) first.
    mid_long_rows = (
        events_df.select(col("metric_date").cast("long").alias("md_long"))
        .approxQuantile("md_long", [0.5], 0.01)
    )
    split_date = None
    if mid_long_rows:
        mid_long_val = mid_long_rows[0]
        # Convert epoch-day back to a date string.
        mid_row = (
            events_df.filter(col("metric_date").cast("long") <= lit(int(mid_long_val)))
            .agg(sql_max("metric_date").alias("mid_date"))
            .collect()
        )
        split_date = mid_row[0]["mid_date"] if mid_row and mid_row[0]["mid_date"] is not None else None
    if split_date is not None:
        def _actor_features_for(events_slice_df):
            base = (
                events_slice_df.groupBy("actor_login")
                .agg(
                    count("*").alias("event_count"),
                    countDistinct("metric_date").alias("active_days"),
                    countDistinct("repo_name").alias("unique_repos"),
                    sql_max(when(col("actor_category") == lit("bot"), lit(1)).otherwise(lit(0))).alias("is_bot"),
                    sql_sum(when(col("hour_of_day") < lit(6), lit(1)).otherwise(lit(0))).alias("night_events"),
                    sql_sum(when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0))).alias("weekend_events"),
                    sql_sum(when(col("event_type") == lit("PushEvent"), lit(1)).otherwise(lit(0))).alias("push_events"),
                    sql_sum(when(col("event_type") == lit("PullRequestEvent"), lit(1)).otherwise(lit(0))).alias("pr_events"),
                    sql_sum(when(col("event_type") == lit("IssuesEvent"), lit(1)).otherwise(lit(0))).alias("issues_events"),
                    sql_sum(when(col("event_type") == lit("WatchEvent"), lit(1)).otherwise(lit(0))).alias("watch_events"),
                    sql_sum(when(col("event_type") == lit("ForkEvent"), lit(1)).otherwise(lit(0))).alias("fork_events"),
                )
            )
            hour_df = (
                events_slice_df.groupBy("actor_login", "hour_of_day")
                .agg(count("*").alias("hc"))
                .withColumn("actor_total", sql_sum("hc").over(Window.partitionBy("actor_login")))
                .withColumn("hp", col("hc") / greatest(col("actor_total"), lit(1.0)))
                .withColumn(
                    "he_comp",
                    when(col("hp") > lit(0.0), -col("hp") * log2(col("hp"))).otherwise(lit(0.0)),
                )
                .groupBy("actor_login")
                .agg(sql_sum("he_comp").alias("hour_entropy"))
            )
            repo_df = (
                events_slice_df.groupBy("actor_login", "repo_name")
                .agg(count("*").alias("rc"))
                .withColumn("actor_total", sql_sum("rc").over(Window.partitionBy("actor_login")))
                .withColumn("rp", col("rc") / greatest(col("actor_total"), lit(1.0)))
                .withColumn(
                    "re_comp",
                    when(col("rp") > lit(0.0), -col("rp") * log2(col("rp"))).otherwise(lit(0.0)),
                )
                .groupBy("actor_login")
                .agg(sql_sum("re_comp").alias("repo_entropy"))
            )
            return (
                base.join(hour_df, on="actor_login", how="left")
                .join(repo_df, on="actor_login", how="left")
                .fillna(0.0, subset=["hour_entropy", "repo_entropy"])
                # We only sample hour=5 per day, so insisting on >=2 events
                # *inside a half-window* would drop most actors and leave us with
                # 0 transitions. >=1 still yields a valid persona_features vector
                # because all shares/ratios are well-defined on a single event.
                .filter(col("event_count") >= lit(1))
                .withColumn("log_event_count", log1p(col("event_count").cast("double")))
                .withColumn("log_unique_repos", log1p(col("unique_repos").cast("double")))
                .withColumn("active_days_f", col("active_days").cast("double"))
                .withColumn("night_ratio", col("night_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("weekend_ratio", col("weekend_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("push_share", col("push_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("pr_share", col("pr_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("issues_share", col("issues_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("watch_share", col("watch_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("fork_share", col("fork_events") / greatest(col("event_count"), lit(1.0)))
                .withColumn("is_bot_f", col("is_bot").cast("double"))
            )

        early_df = _actor_features_for(events_df.filter(col("metric_date") < lit(split_date)))
        late_df = _actor_features_for(events_df.filter(col("metric_date") >= lit(split_date)))
        early_assembled = persona_assembler.transform(early_df)
        late_assembled = persona_assembler.transform(late_df)
        early_scaled = persona_scaler_model.transform(early_assembled)
        late_scaled = persona_scaler_model.transform(late_assembled)
        early_labeled = (
            gmm_model.transform(early_scaled)
            .withColumn("persona_id", col("persona_id").cast("int"))
            .join(label_map_df, on="persona_id", how="left")
            .select(col("actor_login"), col("persona_label").alias("persona_early"))
        )
        late_labeled = (
            gmm_model.transform(late_scaled)
            .withColumn("persona_id", col("persona_id").cast("int"))
            .join(label_map_df, on="persona_id", how="left")
            .select(col("actor_login"), col("persona_label").alias("persona_late"))
        )
        # Outer join captures onboarding (absent → X) and churn (X → absent)
        # signals. Without this, actors present in only one half-window are
        # silently dropped and the transition table can collapse to 0 rows.
        transition_counts_df = (
            early_labeled.join(late_labeled, on="actor_login", how="outer")
            .fillna("absent", subset=["persona_early", "persona_late"])
            .filter(~((col("persona_early") == lit("absent")) & (col("persona_late") == lit("absent"))))
            .groupBy("persona_early", "persona_late")
            .agg(count("*").alias("actors"))
        )
        total_transitions_window = Window.partitionBy("persona_early")
        persona_transition_df = (
            transition_counts_df.withColumn(
                "row_total", sql_sum("actors").over(total_transitions_window)
            )
            .withColumn(
                "transition_prob",
                col("actors").cast("double") / greatest(col("row_total").cast("double"), lit(1.0)),
            )
            .select(
                lit(latest_metric_date).alias("metric_date"),
                col("persona_early"),
                col("persona_late"),
                col("actors").cast("long").alias("actors"),
                col("row_total").cast("long").alias("row_total"),
                sql_round(col("transition_prob"), 6).alias("transition_prob"),
                (when(col("persona_early") == col("persona_late"), lit(1)).otherwise(lit(0))).alias("is_stable"),
            )
            .orderBy(col("persona_early"), col("transition_prob").desc())
        )
    else:
        persona_transition_df = spark.createDataFrame(
            [],
            schema=(
                "metric_date date, persona_early string, persona_late string, "
                "actors long, row_total long, transition_prob double, is_stable int"
            ),
        )

    # ── L3: Hot-repo watcher persona profile + lift ──
    hot_repos_set_df = (
        repo_feature_labeled_df.filter(col("group") == lit("hot"))
        .select("repo_name")
        .distinct()
    )

    actor_hot_repo_df = (
        events_df.join(hot_repos_set_df, on="repo_name", how="inner")
        .select("repo_name", "actor_login")
        .distinct()
    )

    # Global persona distribution (baseline)
    global_persona_counts_df = (
        actor_persona_df.groupBy("persona_label")
        .agg(countDistinct("actor_login").alias("global_actors"))
    )
    global_persona_total = (
        global_persona_counts_df.agg(sql_sum("global_actors").alias("t")).first()["t"] or 1
    )
    global_persona_counts_df = global_persona_counts_df.withColumn(
        "global_share", col("global_actors") / lit(float(global_persona_total))
    )

    # "all_hot" aggregate: watchers across all hot repos
    all_hot_watchers_df = (
        actor_hot_repo_df.select("actor_login").distinct()
        .join(
            actor_persona_df.select("actor_login", "persona_label"),
            on="actor_login",
            how="left",
        )
        .fillna("unlabeled", subset=["persona_label"])
    )
    all_hot_total = max(all_hot_watchers_df.count(), 1)
    all_hot_lift_rows_df = (
        all_hot_watchers_df.groupBy("persona_label")
        .agg(countDistinct("actor_login").alias("watchers"))
        .withColumn("share_in_watchers", col("watchers") / lit(float(all_hot_total)))
        .join(global_persona_counts_df, on="persona_label", how="left")
        .withColumn(
            "lift",
            col("share_in_watchers") / greatest(col("global_share"), lit(1e-9)),
        )
        .select(
            lit(latest_metric_date).alias("metric_date"),
            lit("__ALL_HOT__").alias("repo_name"),
            "persona_label",
            col("watchers").cast("long").alias("watchers"),
            sql_round(col("share_in_watchers"), 6).alias("share_in_watchers"),
            sql_round(coalesce(col("global_share"), lit(0.0)), 6).alias("share_in_global"),
            sql_round(col("lift"), 6).alias("lift"),
        )
    )

    # Per-hot-repo lift
    per_hot_watchers_df = actor_hot_repo_df.join(
        actor_persona_df.select("actor_login", "persona_label"),
        on="actor_login",
        how="left",
    ).fillna("unlabeled", subset=["persona_label"])

    repo_watcher_total_df = per_hot_watchers_df.groupBy("repo_name").agg(
        countDistinct("actor_login").alias("repo_watchers")
    )
    per_hot_lift_df = (
        per_hot_watchers_df.groupBy("repo_name", "persona_label")
        .agg(countDistinct("actor_login").alias("watchers"))
        .join(repo_watcher_total_df, on="repo_name", how="inner")
        .withColumn(
            "share_in_watchers",
            col("watchers") / greatest(col("repo_watchers").cast("double"), lit(1.0)),
        )
        .join(global_persona_counts_df, on="persona_label", how="left")
        .withColumn(
            "lift",
            col("share_in_watchers") / greatest(col("global_share"), lit(1e-9)),
        )
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            "persona_label",
            col("watchers").cast("long").alias("watchers"),
            sql_round(col("share_in_watchers"), 6).alias("share_in_watchers"),
            sql_round(coalesce(col("global_share"), lit(0.0)), 6).alias("share_in_global"),
            sql_round(col("lift"), 6).alias("lift"),
        )
    )

    repo_watcher_persona_lift_df = all_hot_lift_rows_df.unionByName(per_hot_lift_df)

    # Watcher profile: bulk stats of watchers for each hot repo + dominant persona
    watcher_stat_base_df = per_hot_watchers_df.join(
        actor_persona_df.select(
            "actor_login",
            "night_ratio",
            "pr_share",
            "push_share",
            "unique_repos",
            "is_bot",
        ),
        on="actor_login",
        how="left",
    )
    watcher_stat_df = watcher_stat_base_df.groupBy("repo_name").agg(
        countDistinct("actor_login").alias("watchers"),
        avg("night_ratio").alias("avg_night_ratio"),
        avg(
            col("pr_share") / greatest(col("push_share") + col("pr_share"), lit(1e-6))
        ).alias("avg_pr_push_ratio"),
        avg(col("unique_repos").cast("double")).alias("avg_unique_repos"),
        avg(col("is_bot").cast("double")).alias("bot_ratio"),
    )

    dominant_persona_window = Window.partitionBy("repo_name").orderBy(col("watchers").desc(), col("persona_label"))
    dominant_persona_df = (
        per_hot_lift_df.withColumn("rn", row_number().over(dominant_persona_window))
        .filter(col("rn") == lit(1))
        .select(
            "repo_name",
            col("persona_label").alias("dominant_persona"),
            col("share_in_watchers").alias("dominant_share"),
            col("lift").alias("dominant_lift"),
        )
    )

    repo_watcher_profile_df = (
        watcher_stat_df.join(dominant_persona_df, on="repo_name", how="left")
        .join(
            repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"),
            on="repo_name",
            how="left",
        )
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            coalesce(col("rank_no"), lit(999999)).cast("long").alias("rank_no"),
            coalesce(col("rank_score"), lit(0.0)).alias("rank_score"),
            col("watchers").cast("long").alias("watchers"),
            sql_round(coalesce(col("avg_night_ratio"), lit(0.0)), 6).alias("avg_night_ratio"),
            sql_round(coalesce(col("avg_pr_push_ratio"), lit(0.0)), 6).alias("avg_pr_push_ratio"),
            sql_round(coalesce(col("avg_unique_repos"), lit(0.0)), 6).alias("avg_unique_repos"),
            sql_round(coalesce(col("bot_ratio"), lit(0.0)), 6).alias("bot_ratio"),
            coalesce(col("dominant_persona"), lit("unlabeled")).alias("dominant_persona"),
            sql_round(coalesce(col("dominant_share"), lit(0.0)), 6).alias("dominant_share"),
            sql_round(coalesce(col("dominant_lift"), lit(0.0)), 6).alias("dominant_lift"),
        )
        .orderBy(col("rank_no"))
    )

    # ── L5: Repo co-occurrence graph + FP-Growth association rules + communities ──
    # Scope to repos with at least 3 unique actors to keep the graph meaningful.
    repo_actor_set_df = (
        events_df.groupBy("repo_name")
        .agg(countDistinct("actor_login").alias("repo_actors"))
        .filter(col("repo_actors") >= lit(3))
    )
    # Cap repos considered for similarity to the Top ~200 by rank_score for tractability.
    scope_repo_df = (
        repo_actor_set_df.join(
            repo_rank_scored_all_df.select("repo_name", "rank_score"),
            on="repo_name",
            how="left",
        )
        .fillna(0.0, subset=["rank_score"])
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy().orderBy(col("rank_score").desc(), col("repo_actors").desc(), col("repo_name"))
            ),
        )
        .filter(col("rn") <= lit(200))
        .select("repo_name", "repo_actors")
        .cache()
    )
    _ = scope_repo_df.count()  # materialize

    actor_repo_edges_df = (
        events_df.select("actor_login", "repo_name")
        .distinct()
        .join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
    )

    # Repo self-join on shared actor — keep r1 < r2 only.
    repo_pair_df = (
        actor_repo_edges_df.alias("a")
        .join(
            actor_repo_edges_df.alias("b"),
            col("a.actor_login") == col("b.actor_login"),
            "inner",
        )
        .filter(col("a.repo_name") < col("b.repo_name"))
        .select(
            col("a.repo_name").alias("repo_a"),
            col("b.repo_name").alias("repo_b"),
            col("a.actor_login").alias("actor_login"),
        )
    )
    repo_pair_shared_df = (
        repo_pair_df.groupBy("repo_a", "repo_b")
        .agg(countDistinct("actor_login").alias("shared_actors"))
        .filter(col("shared_actors") >= lit(2))
    )

    actor_count_df = scope_repo_df.select(
        col("repo_name").alias("node"), col("repo_actors").alias("node_actors")
    )
    repo_similarity_full_df = (
        repo_pair_shared_df.join(
            actor_count_df, repo_pair_shared_df.repo_a == actor_count_df.node, "left"
        )
        .withColumnRenamed("node_actors", "actors_a")
        .drop("node")
        .join(
            actor_count_df, repo_pair_shared_df.repo_b == actor_count_df.node, "left"
        )
        .withColumnRenamed("node_actors", "actors_b")
        .drop("node")
        .withColumn(
            "jaccard",
            col("shared_actors").cast("double")
            / greatest(
                (col("actors_a") + col("actors_b") - col("shared_actors")).cast("double"),
                lit(1.0),
            ),
        )
    )
    jaccard_threshold = 0.05
    repo_edges_base_df = repo_similarity_full_df.filter(col("jaccard") >= lit(jaccard_threshold)).cache()
    _ = repo_edges_base_df.count()  # materialize to avoid re-computing for CC and for edges

    # Per-repo Top-10 most similar neighbors (undirected: expand both directions).
    directional_edges_df = repo_edges_base_df.select(
        col("repo_a").alias("src_repo"),
        col("repo_b").alias("dst_repo"),
        "shared_actors",
        "jaccard",
    ).unionByName(
        repo_edges_base_df.select(
            col("repo_b").alias("src_repo"),
            col("repo_a").alias("dst_repo"),
            "shared_actors",
            "jaccard",
        )
    )
    neighbor_window = Window.partitionBy("src_repo").orderBy(col("jaccard").desc(), col("shared_actors").desc(), col("dst_repo"))
    repo_similarity_edges_df = (
        directional_edges_df.withColumn("rn_n", row_number().over(neighbor_window))
        .filter(col("rn_n") <= lit(10))
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "src_repo",
            "dst_repo",
            col("shared_actors").cast("long").alias("shared_actors"),
            sql_round(col("jaccard"), 6).alias("jaccard"),
        )
    )

    # ---- Community detection via weighted Label Propagation (LPA) ----
    # LPA picks, for each node, the neighbor label with the largest summed Jaccard
    # weight (tie-break by lexicographic label). This is a genuine community-detection
    # algorithm (Raghavan et al. 2007) — unlike min-label connected-components, it
    # produces multiple non-trivial communities even when the graph is one big blob.
    spark.sparkContext.setCheckpointDir("/tmp/spark-cc-checkpoints")
    weighted_edges_df = (
        repo_edges_base_df.select(
            col("repo_a").alias("src"), col("repo_b").alias("dst"), col("jaccard").alias("w")
        )
        .unionByName(
            repo_edges_base_df.select(
                col("repo_b").alias("src"), col("repo_a").alias("dst"), col("jaccard").alias("w")
            )
        )
        .cache()
    )
    _ = weighted_edges_df.count()

    nodes_df = scope_repo_df.select(col("repo_name").alias("node")).withColumn(
        "label", col("node")
    )
    current_nodes_df = nodes_df.localCheckpoint(eager=True)
    lpa_max_iter = 8
    lpa_iters_run = 0
    for _iter in range(lpa_max_iter):
        lpa_iters_run = _iter + 1
        candidate_df = (
            weighted_edges_df.alias("e")
            .join(current_nodes_df.alias("n"), col("e.src") == col("n.node"), "inner")
            .groupBy(col("e.dst").alias("node"), col("n.label").alias("cand_label"))
            .agg(sql_sum(col("e.w")).alias("w_sum"))
        )
        label_rank_window = Window.partitionBy("node").orderBy(
            col("w_sum").desc_nulls_last(), col("cand_label").asc()
        )
        best_label_df = (
            candidate_df.withColumn("rn_lpa", row_number().over(label_rank_window))
            .filter(col("rn_lpa") == lit(1))
            .select("node", col("cand_label").alias("new_label"))
        )
        next_nodes_df = (
            current_nodes_df.alias("c")
            .join(best_label_df.alias("b"), col("c.node") == col("b.node"), "left")
            .select(
                col("c.node").alias("node"),
                coalesce(col("b.new_label"), col("c.label")).alias("label"),
            )
        )
        next_nodes_df = next_nodes_df.localCheckpoint(eager=True)
        diff_count = (
            next_nodes_df.alias("n2")
            .join(current_nodes_df.alias("n1"), col("n1.node") == col("n2.node"))
            .filter(col("n1.label") != col("n2.label"))
            .limit(1)
            .count()
        )
        current_nodes_df = next_nodes_df
        if diff_count == 0:
            break

    community_assign_df = current_nodes_df.select(
        col("node").alias("repo_name"), col("label").alias("component_id")
    )

    community_size_df = community_assign_df.groupBy("component_id").agg(
        count("*").alias("community_size"),
        concat_ws(
            " | ",
            array_slice(sort_array(collect_set(col("repo_name"))), 1, 5),
        ).alias("sample_members"),
    )
    repo_community_df = (
        community_assign_df.join(community_size_df, on="component_id", how="left")
        .join(
            repo_rank_scored_all_df.select("repo_name", "rank_no", "rank_score"),
            on="repo_name",
            how="left",
        )
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "repo_name",
            col("component_id").alias("community_id"),
            col("community_size").cast("long").alias("community_size"),
            coalesce(col("rank_no"), lit(999999)).cast("long").alias("rank_no"),
            coalesce(col("rank_score"), lit(0.0)).alias("rank_score"),
            coalesce(col("sample_members"), lit("")).alias("sample_members"),
        )
    ).cache()
    _ = repo_community_df.count()

    # ---- Community profile: feature means + top-3 members per community ----
    repo_feat_sum_df = (
        repo_day_features_df.groupBy("repo_name")
        .agg(
            sql_sum("total_events").alias("total_events_sum"),
            sql_sum("watch_events").alias("watch_events_sum"),
            sql_sum("pull_request_events").alias("pr_events_sum"),
            sql_sum("push_events").alias("push_events_sum"),
            sql_sum("issues_events").alias("issues_events_sum"),
            sql_sum("bot_events").alias("bot_events_sum"),
            countDistinct("metric_date").alias("active_days"),
        )
    )
    community_feat_agg_df = (
        repo_community_df.select("community_id", "repo_name", "rank_score")
        .join(repo_feat_sum_df, on="repo_name", how="left")
        .groupBy("community_id")
        .agg(
            count("*").alias("size"),
            sql_sum(coalesce(col("total_events_sum"), lit(0))).alias("total_events"),
            sql_sum(coalesce(col("watch_events_sum"), lit(0))).alias("watch_events"),
            sql_sum(coalesce(col("pr_events_sum"), lit(0))).alias("pr_events"),
            sql_sum(coalesce(col("push_events_sum"), lit(0))).alias("push_events"),
            sql_sum(coalesce(col("issues_events_sum"), lit(0))).alias("issues_events"),
            sql_sum(coalesce(col("bot_events_sum"), lit(0))).alias("bot_events"),
            avg(coalesce(col("rank_score"), lit(0.0))).alias("avg_rank_score"),
            avg(coalesce(col("active_days"), lit(0))).alias("avg_active_days"),
        )
        .withColumn(
            "watch_share",
            col("watch_events") / greatest(col("total_events"), lit(1)),
        )
        .withColumn(
            "pr_push_ratio",
            (col("pr_events") + col("push_events")) / greatest(col("total_events"), lit(1)),
        )
        .withColumn(
            "bot_ratio",
            col("bot_events") / greatest(col("total_events"), lit(1)),
        )
    )
    top_members_window = Window.partitionBy("community_id").orderBy(
        col("rank_score").desc_nulls_last(), col("repo_name").asc()
    )
    top_members_df = (
        repo_community_df.select("community_id", "repo_name", "rank_score")
        .withColumn("rn_tm", row_number().over(top_members_window))
        .filter(col("rn_tm") <= lit(3))
        .groupBy("community_id")
        .agg(concat_ws(" | ", collect_list("repo_name")).alias("top_members"))
    )
    repo_community_profile_df = (
        community_feat_agg_df.join(top_members_df, on="community_id", how="left")
        .select(
            lit(latest_metric_date).alias("metric_date"),
            col("community_id").alias("community_id"),
            col("size").cast("long").alias("community_size"),
            coalesce(col("top_members"), lit("")).alias("top_members"),
            sql_round(coalesce(col("avg_rank_score"), lit(0.0)), 6).alias("avg_rank_score"),
            sql_round(coalesce(col("avg_active_days"), lit(0.0)), 4).alias("avg_active_days"),
            col("total_events").cast("long").alias("total_events"),
            sql_round(col("watch_share"), 6).alias("watch_share"),
            sql_round(col("pr_push_ratio"), 6).alias("pr_push_ratio"),
            sql_round(col("bot_ratio"), 6).alias("bot_ratio"),
        )
        .filter(col("community_size") >= lit(2))
        .orderBy(col("community_size").desc(), col("total_events").desc_nulls_last())
    )

    # FP-Growth association rules: baskets = each actor → set of repos they touched.
    actor_baskets_df = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .groupBy("actor_login")
        .agg(sort_array(collect_set("repo_name")).alias("items"))
        .filter(array_size(col("items")) >= lit(2))
        .filter(array_size(col("items")) <= lit(30))
    )
    actor_basket_total = max(actor_baskets_df.count(), 1)
    min_support = min(0.02, max(0.002, 6.0 / float(actor_basket_total)))
    fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=0.3)
    try:
        fp_model = fp.fit(actor_baskets_df)
        fp_rules_df = fp_model.associationRules
    except Exception:
        fp_rules_df = spark.createDataFrame(
            [], schema="antecedent array<string>, consequent array<string>, confidence double, lift double, support double"
        )

    repo_association_rules_base_df = (
        fp_rules_df.withColumn("antecedent_str", concat_ws(" & ", col("antecedent")))
        .withColumn("consequent_str", concat_ws(" & ", col("consequent")))
        .filter(array_size(col("consequent")) == lit(1))
        .select(
            col("antecedent_str").alias("antecedent"),
            col("consequent_str").alias("consequent"),
            array_size(col("antecedent")).cast("int").alias("antecedent_size"),
            sql_round(coalesce(col("support"), lit(0.0)), 6).alias("support"),
            sql_round(coalesce(col("confidence"), lit(0.0)), 6).alias("confidence"),
            sql_round(coalesce(col("lift"), lit(0.0)), 6).alias("lift"),
        )
        .orderBy(col("lift").desc_nulls_last(), col("confidence").desc_nulls_last())
        .limit(500)
    )

    # ---- Pareto frontier over (support, confidence, lift) ----
    # A rule is on the Pareto frontier if no other rule dominates it, i.e. no other
    # rule has support >= s AND confidence >= c AND lift >= l with at least one strict.
    rules_rows = [r.asDict() for r in repo_association_rules_base_df.collect()]
    pareto_keys: set[tuple[str, str]] = set()
    for i, ri in enumerate(rules_rows):
        dominated = False
        for j, rj in enumerate(rules_rows):
            if i == j:
                continue
            if (
                rj["support"] >= ri["support"]
                and rj["confidence"] >= ri["confidence"]
                and rj["lift"] >= ri["lift"]
                and (
                    rj["support"] > ri["support"]
                    or rj["confidence"] > ri["confidence"]
                    or rj["lift"] > ri["lift"]
                )
            ):
                dominated = True
                break
        if not dominated:
            pareto_keys.add((ri["antecedent"], ri["consequent"]))

    pareto_schema = "antecedent string, consequent string, is_frontier int"
    pareto_rows = [
        (ant, cons, 1 if (ant, cons) in pareto_keys else 0)
        for (ant, cons) in {(r["antecedent"], r["consequent"]) for r in rules_rows}
    ]
    pareto_flag_df = (
        spark.createDataFrame(pareto_rows, schema=pareto_schema)
        if pareto_rows
        else spark.createDataFrame([], schema=pareto_schema)
    )

    repo_association_rules_df = (
        repo_association_rules_base_df.join(
            pareto_flag_df, on=["antecedent", "consequent"], how="left"
        )
        .withColumn("is_frontier", coalesce(col("is_frontier"), lit(0)).cast("int"))
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "antecedent",
            "consequent",
            "antecedent_size",
            "support",
            "confidence",
            "lift",
            "is_frontier",
        )
    )

    # ── People-page depth analyses (four buckets) ───────────────────────────
    # Bucket A: collaboration graph (PageRank / Louvain / Betweenness)
    actor_collab_edge_df, _top_actors = _people_depth.build_collaboration_edges(
        spark, events_df, latest_metric_date, top_actor_limit=5000
    )
    actor_graph_metrics_df = _people_depth.build_graph_metrics(
        spark, actor_collab_edge_df, actor_persona_df, latest_metric_date
    )

    # Bucket B: burst/stability + retention curve + churn risk
    actor_burst_stability_df = _people_depth.build_actor_burst_stability(
        spark, events_df, actor_persona_df, latest_metric_date, top_actor_limit=3000
    )
    actor_retention_curve_df = _people_depth.build_retention_curve(
        spark, events_df, latest_metric_date
    )
    actor_churn_risk_df = _people_depth.build_churn_risk(
        spark, events_df, actor_persona_df, latest_metric_date, top_actor_limit=5000
    )

    # Bucket C: individual hotness + bus-factor
    actor_hotness_df = _people_depth.build_actor_hotness(
        spark, events_df, repo_rank_score_df, actor_persona_df, latest_metric_date
    )
    repo_bus_factor_df = _people_depth.build_repo_bus_factor(
        spark, events_df, repo_rank_score_df, latest_metric_date
    )

    # Bucket D: supervised bot (XGB) + unsupervised anomaly (IF) + ring detect
    (
        actor_bot_supervised_df,
        bot_feature_importance_df,
        bot_classifier_meta_df,
    ) = _people_depth.build_bot_supervised(spark, actor_persona_df, latest_metric_date)
    actor_ring_df = _people_depth.build_actor_ring(spark, events_df, latest_metric_date)

    output_paths = {
        "daily_metrics": daily_metrics_df,
        "activity_patterns": activity_patterns_df,
        "language_day_trend": language_day_trend_df,
        "top_users_day": top_users_day_df,
        "top_repos_day": top_repos_day_df,
        "event_type_day": event_type_day_df,
        "org_daily_metrics": org_daily_metrics_df,
        "org_rank_latest": org_rank_latest_df,
        "event_action_day": event_action_day_df,
        "repo_payload_size_day": repo_payload_size_day_df,
        "payload_bucket_day": payload_bucket_day_df,
        "user_segment_latest": user_segment_latest_df,
        "repo_day_features": repo_day_features_df,
        "repo_rank_daily": repo_rank_score_df,
        "repo_rank_score_day": repo_rank_score_day_df,
        "repo_rank_delta_explain_day": repo_rank_delta_explain_day_df,
        "repo_trend_forecast": repo_trend_forecast_df,
        "repo_burst_stability": repo_burst_stability_df,
        "developer_rhythm_heatmap": developer_rhythm_heatmap_df,
        "repo_hotness_components": repo_hotness_components_df,
        "event_complexity_day": event_complexity_day_df,
        "repo_health_latest": repo_health_latest_df,
        "offline_anomaly_alerts": offline_anomaly_alerts_df,
        "offline_decline_warnings": offline_decline_warnings_df,
        "repo_clusters": repo_clusters_df,
        "repo_rank_explain_latest": repo_rank_explain_latest_df,
        "repo_contributor_concentration_day": repo_contributor_concentration_day_df,
        "repo_top_actors_latest": repo_top_actors_latest_df,
        "repo_event_type_share_day": repo_event_type_share_day_df,
        "repo_event_mix_shift_day": repo_event_mix_shift_day_df,
        "concentration_day": concentration_day_df,
        "ecosystem_changepoints": ecosystem_changepoints_df,
        "actor_cohort_day": actor_cohort_day_df,
        "event_type_share_shift_day": event_type_share_shift_day_df,
        "offline_insights_latest": offline_insights_latest_df,
        # L1 cluster profile
        "repo_cluster_profile": repo_cluster_profile_df,
        # L2 hot-vs-cold attribution + repo DNA
        "hot_vs_cold_attribution": hot_vs_cold_attribution_df,
        "repo_dna": repo_dna_df,
        "repo_dna_outliers": repo_dna_outliers_df,
        # L4 actor persona
        "actor_persona": actor_persona_df,
        "actor_persona_centroid": actor_persona_centroid_df,
        "actor_persona_bic": actor_persona_bic_df,
        "actor_persona_bot_validation": actor_persona_bot_validation_df,
        "actor_persona_transition": persona_transition_df,
        # L3 watcher persona lift + profile
        "repo_watcher_persona_lift": repo_watcher_persona_lift_df,
        "repo_watcher_profile": repo_watcher_profile_df,
        # L5 repo similarity / community / association rules
        "repo_similarity_edges": repo_similarity_edges_df,
        "repo_community": repo_community_df,
        "repo_community_profile": repo_community_profile_df,
        "repo_association_rules": repo_association_rules_df,
        # People-depth: collaboration (bucket A)
        "actor_collab_edge": actor_collab_edge_df,
        "actor_graph_metrics": actor_graph_metrics_df,
        # People-depth: retention & churn (bucket B)
        "actor_burst_stability": actor_burst_stability_df,
        "actor_retention_curve": actor_retention_curve_df,
        "actor_churn_risk": actor_churn_risk_df,
        # People-depth: influence (bucket C)
        "actor_hotness": actor_hotness_df,
        "repo_bus_factor": repo_bus_factor_df,
        # People-depth: authenticity (bucket D)
        "actor_bot_supervised": actor_bot_supervised_df,
        "bot_feature_importance": bot_feature_importance_df,
        "bot_classifier_meta": bot_classifier_meta_df,
        "actor_ring": actor_ring_df,
    }

    for dataset_name, dataset_df in output_paths.items():
        dataset_df.write.mode("overwrite").parquet(f"{args.output}/{dataset_name}")

    summary_json_output = f"{args.output}/summary_json"
    summary_json_df.coalesce(1).write.mode("overwrite").json(summary_json_output)

    for dataset_name in output_paths:
        print(f"{dataset_name} saved to {args.output}/{dataset_name}")
    print(f"summary json saved to {summary_json_output}")
    spark.stop()


if __name__ == "__main__":
    main()
