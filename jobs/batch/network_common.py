"""Shared helpers for network_depth + network_ic.

Kept small on purpose: only reusable Spark primitives live here.
All heavy driver-side logic (ALS, archetype, k-core …) stays in the caller.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    dayofweek,
    greatest,
    lit,
    row_number,
    split,
)
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# IO & event dataframe
# ─────────────────────────────────────────────────────────────────────────────

def read_curated_events(spark: SparkSession, input_dir: str) -> DataFrame:
    """Read curated parquet the same way spark_job.py does.

    Normalises null logins / repos / languages, extracts owner, renames
    ``event_date → metric_date`` for downstream parity.
    """
    df = (
        spark.read.parquet(input_dir)
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
        # drop synthetic / unknown logins: they pollute similarity + communities
        .filter(col("actor_login") != lit("unknown"))
        .filter(col("repo_name") != lit("unknown/unknown"))
    )
    return df


def latest_metric_date(events_df: DataFrame):
    row = events_df.agg({"metric_date": "max"}).collect()[0]
    return row[0]


# ─────────────────────────────────────────────────────────────────────────────
# Repo scoping
# ─────────────────────────────────────────────────────────────────────────────

def scope_top_repos(
    events_df: DataFrame,
    top_n: int = 200,
    min_actors: int = 3,
) -> DataFrame:
    """Return top-N repos by (unique_actors × total_events) proxy rank_score.

    Columns: repo_name, repo_actors, total_events, rank_score.
    """
    from pyspark.sql.functions import sqrt

    per_repo = (
        events_df.groupBy("repo_name")
        .agg(
            countDistinct("actor_login").alias("repo_actors"),
            count("*").alias("total_events"),
        )
        .filter(col("repo_actors") >= lit(min_actors))
        .withColumn("rank_score", col("repo_actors") * sqrt(col("total_events").cast("double")))
    )
    window = Window.partitionBy().orderBy(
        col("rank_score").desc(), col("total_events").desc(), col("repo_name")
    )
    return (
        per_repo.withColumn("rn", row_number().over(window))
        .filter(col("rn") <= lit(top_n))
        .drop("rn")
        .cache()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Jaccard edges + LPA (re-used across layers & meta-paths)
# ─────────────────────────────────────────────────────────────────────────────

def jaccard_edges(
    actor_repo_df: DataFrame,
    node_count_df: DataFrame,
    threshold: float = 0.05,
    min_shared: int = 2,
) -> DataFrame:
    """Compute Jaccard on actor-repo bipartite. Symmetric output (repo_a<repo_b).

    ``node_count_df`` must have (repo_name, repo_actors).
    """
    pairs = (
        actor_repo_df.alias("a")
        .join(
            actor_repo_df.alias("b"),
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
    shared = (
        pairs.groupBy("repo_a", "repo_b")
        .agg(countDistinct("actor_login").alias("shared_actors"))
        .filter(col("shared_actors") >= lit(min_shared))
    )
    sizes = node_count_df.select(
        col("repo_name").alias("node"),
        col("repo_actors").alias("node_actors"),
    )
    return (
        shared.join(sizes, shared.repo_a == sizes.node, "left")
        .withColumnRenamed("node_actors", "actors_a")
        .drop("node")
        .join(sizes, shared.repo_b == sizes.node, "left")
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
        .filter(col("jaccard") >= lit(threshold))
        .select("repo_a", "repo_b", "shared_actors", "jaccard")
    )


# ─────────────────────────────────────────────────────────────────────────────
# LPA (driver-side; works on pandas edgelist to avoid another Spark iteration)
# ─────────────────────────────────────────────────────────────────────────────

def weighted_lpa(
    edges_pdf: pd.DataFrame,
    nodes: Iterable[str],
    weight_col: str = "jaccard",
    max_iter: int = 10,
    seed: int = 42,
) -> dict[str, str]:
    """Raghavan-style weighted LPA. ``edges_pdf`` is *undirected* (each pair once)."""
    import random

    random.seed(seed)
    nodes_list = list(nodes)
    label: dict[str, str] = {n: n for n in nodes_list}
    if edges_pdf.empty:
        return label

    adj: dict[str, list[tuple[str, float]]] = {n: [] for n in nodes_list}
    for s, d, w in zip(
        edges_pdf["src"].tolist(), edges_pdf["dst"].tolist(), edges_pdf[weight_col].tolist()
    ):
        if s in adj and d in adj:
            adj[s].append((d, float(w)))
            adj[d].append((s, float(w)))

    for _ in range(max_iter):
        changed = 0
        order = nodes_list[:]
        random.shuffle(order)
        for n in order:
            nbrs = adj[n]
            if not nbrs:
                continue
            scores: dict[str, float] = {}
            for nb, w in nbrs:
                lab = label[nb]
                scores[lab] = scores.get(lab, 0.0) + w
            # argmax weight, tie-break on lexicographic label
            best_label = None
            best_w = float("-inf")
            for lab, w in scores.items():
                if w > best_w or (w == best_w and (best_label is None or lab < best_label)):
                    best_w = w
                    best_label = lab
            if best_label and best_label != label[n]:
                label[n] = best_label
                changed += 1
        if changed == 0:
            break
    return label


# ─────────────────────────────────────────────────────────────────────────────
# Metric-date derivation: always use the global max of curated events
# ─────────────────────────────────────────────────────────────────────────────

def ensure_output_dir(base: str) -> Path:
    p = Path(base)
    p.mkdir(parents=True, exist_ok=True)
    return p
