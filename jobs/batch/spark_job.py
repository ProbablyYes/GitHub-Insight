from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    dayofweek,
    exp,
    greatest,
    lag,
    lit,
    log2,
    max as sql_max,
    min as sql_min,
    row_number,
    round as sql_round,
    sum as sql_sum,
    when,
)
from pyspark.sql.window import Window

try:
    from jobs.common.spark_runtime import validate_local_spark_runtime
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from jobs.common.spark_runtime import validate_local_spark_runtime


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
        )
        .filter(col("event_date").isNotNull() & col("event_type").isNotNull())
        .withColumn("actor_login", coalesce(col("actor_login"), lit("unknown")))
        .withColumn("repo_name", coalesce(col("repo_name"), lit("unknown/unknown")))
        .withColumn("language_guess", coalesce(col("language_guess"), lit("unknown")))
        .withColumn("actor_category", coalesce(col("actor_category"), lit("unknown")))
        .withColumnRenamed("event_date", "metric_date")
        .withColumnRenamed("event_hour", "hour_of_day")
        .withColumn("day_of_week", dayofweek(col("metric_date")))
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

    repo_rank_score_df = (
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
        .filter(col("rank_no") <= lit(args.rank_limit))
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
        .join(repo_rank_score_df.select("repo_name", "rank_no", "rank_score"), on="repo_name", how="left")
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
        .join(repo_rank_score_df.select("repo_name", "rank_no"), on="repo_name", how="left")
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
        .join(repo_rank_score_df.select("repo_name", "rank_no"), on="repo_name", how="left")
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

    output_paths = {
        "daily_metrics": daily_metrics_df,
        "activity_patterns": activity_patterns_df,
        "language_day_trend": language_day_trend_df,
        "top_users_day": top_users_day_df,
        "top_repos_day": top_repos_day_df,
        "event_type_day": event_type_day_df,
        "repo_day_features": repo_day_features_df,
        "repo_rank_daily": repo_rank_score_df,
        "repo_trend_forecast": repo_trend_forecast_df,
        "repo_burst_stability": repo_burst_stability_df,
        "developer_rhythm_heatmap": developer_rhythm_heatmap_df,
        "repo_hotness_components": repo_hotness_components_df,
        "event_complexity_day": event_complexity_day_df,
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
