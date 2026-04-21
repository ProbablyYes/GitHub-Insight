"""People-page depth analyses.

Four focus areas delivered by this module, each producing one or more batch
tables. Heavy graph / ML logic runs driver-side (pandas + NetworkX + sklearn +
XGBoost) because our graphs are small (≤5 k nodes) and the models are trained
on ≤ a few hundred thousand rows — a single node handles that easily while
keeping the Spark pipeline simple.

Bucket A (collaboration network)
    - build_collaboration_edges        → batch_actor_collab_edge
    - build_graph_metrics              → batch_actor_graph_metrics

Bucket B (retention & churn)
    - build_actor_burst_stability      → batch_actor_burst_stability
    - build_retention_curve            → batch_actor_retention_curve
    - build_churn_risk                 → batch_actor_churn_risk

Bucket C (influence)
    - build_actor_hotness              → batch_actor_hotness
    - build_repo_bus_factor            → batch_repo_bus_factor

Bucket D (authenticity)
    - build_bot_supervised             → batch_actor_bot_supervised
                                       + batch_bot_feature_importance
                                       + batch_bot_classifier_meta
    - build_actor_ring                 → batch_actor_ring

All functions are pure — they take Spark DataFrames / driver-side pandas data
as input and return Spark DataFrames ready for `.write.parquet(...)`.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    greatest,
    lit,
    row_number,
    when,
    sum as sql_sum,
)
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# Bucket A — collaboration network
# ─────────────────────────────────────────────────────────────────────────────

def build_collaboration_edges(
    spark: SparkSession,
    events_df: DataFrame,
    latest_metric_date,
    top_actor_limit: int = 5000,
) -> tuple[DataFrame, list[str]]:
    """Actor-actor collaboration edges among the top-N most active actors.

    Two actors are connected when they act on the same repo on the same day.
    Edge weight = number of (repo, day) co-occurrences, log-scaled.

    Returns the edges DataFrame plus the list of retained actor logins so the
    graph-metrics pass can reuse the sub-set without recomputing.
    """
    # 1) Keep only the top-N active actors to bound graph size.
    top_actors_pdf = (
        events_df.groupBy("actor_login")
        .agg(count("*").alias("event_count"))
        .orderBy(col("event_count").desc())
        .limit(top_actor_limit)
        .toPandas()
    )
    top_actors = top_actors_pdf["actor_login"].tolist()
    top_actors_df = spark.createDataFrame(
        [(a,) for a in top_actors], schema="actor_login string"
    )

    # 2) Reduce event stream to (actor, repo, day) triples among the top-N.
    triples = (
        events_df.join(top_actors_df, on="actor_login", how="inner")
        .select("actor_login", "repo_name", "metric_date")
        .distinct()
    )
    triples_a = triples.alias("a")
    triples_b = triples.alias("b")

    pairs = (
        triples_a.join(
            triples_b,
            on=[
                col("a.repo_name") == col("b.repo_name"),
                col("a.metric_date") == col("b.metric_date"),
                col("a.actor_login") < col("b.actor_login"),
            ],
        )
        .select(
            col("a.actor_login").alias("actor_a"),
            col("b.actor_login").alias("actor_b"),
            col("a.repo_name").alias("repo_name"),
            col("a.metric_date").alias("metric_date"),
        )
    )

    edges = (
        pairs.groupBy("actor_a", "actor_b")
        .agg(
            countDistinct("repo_name").alias("shared_repos"),
            countDistinct("metric_date").alias("co_days"),
            count("*").alias("co_events"),
        )
        .withColumn("weight", col("co_events").cast("double"))
        .select(
            lit(latest_metric_date).alias("metric_date"),
            col("actor_a"),
            col("actor_b"),
            col("shared_repos").cast("long"),
            col("co_days").cast("long"),
            col("weight"),
        )
    )
    return edges, top_actors


def build_graph_metrics(
    spark: SparkSession,
    edges_df: DataFrame,
    actor_persona_df: DataFrame,
    latest_metric_date,
) -> DataFrame:
    """PageRank + Louvain community + approximate Betweenness on the collab graph.

    Runs driver-side with NetworkX because the graph is capped at ~5 k nodes.
    """
    import networkx as nx  # local import keeps top of file light for unit tests

    edges_pdf = edges_df.select("actor_a", "actor_b", "weight").toPandas()
    persona_pdf = (
        actor_persona_df.select("actor_login", "persona_label").toPandas()
        if actor_persona_df is not None
        else pd.DataFrame(columns=["actor_login", "persona_label"])
    )

    schema = (
        "metric_date date, actor_login string, pagerank double, betweenness double, "
        "community_id int, degree long, persona_label string"
    )
    if edges_pdf.empty:
        return spark.createDataFrame([], schema=schema)

    graph = nx.Graph()
    for row in edges_pdf.itertuples(index=False):
        graph.add_edge(row.actor_a, row.actor_b, weight=float(row.weight))

    pr = nx.pagerank(graph, weight="weight", alpha=0.85, max_iter=100)
    # Approximate betweenness via k-sample; k ≤ 500 is accurate enough on
    # graphs of a few thousand nodes and ~100× faster than full computation.
    k_sample = min(500, max(2, graph.number_of_nodes() // 2))
    bt = nx.betweenness_centrality(graph, k=k_sample, weight="weight", seed=42)

    # Louvain community (python-louvain is a lightweight weighted-modularity
    # implementation). Fall back to a single-community labelling if missing.
    try:
        import community as community_louvain  # python-louvain
        partition = community_louvain.best_partition(graph, weight="weight", random_state=42)
    except Exception:  # pragma: no cover — fallback path
        partition = {n: 0 for n in graph.nodes()}

    degree = dict(graph.degree(weight="weight"))

    persona_map = dict(
        zip(persona_pdf["actor_login"].tolist(), persona_pdf["persona_label"].tolist())
    )

    rows: list[tuple] = []
    for actor, pagerank_value in pr.items():
        rows.append(
            (
                latest_metric_date,
                str(actor),
                round(float(pagerank_value), 8),
                round(float(bt.get(actor, 0.0)), 8),
                int(partition.get(actor, 0)),
                int(round(float(degree.get(actor, 0.0)))),
                str(persona_map.get(actor, "unlabeled")),
            )
        )
    return spark.createDataFrame(rows, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# Bucket B — retention & churn
# ─────────────────────────────────────────────────────────────────────────────

def build_actor_burst_stability(
    spark: SparkSession,
    events_df: DataFrame,
    actor_persona_df: DataFrame,
    latest_metric_date,
    top_actor_limit: int = 3000,
) -> DataFrame:
    """Per-actor burst (short-term concentration) and stability (long-run variance).

    Mirrors the repo burst/stability method but on actor daily event counts.
    Burst index = max(daily)/mean(daily); stability = 1 / (1 + coefficient of
    variation). Four quadrants map to rising-core / steady-core / short-spike /
    long-tail, same interpretation as the repo-level version.
    """
    daily = (
        events_df.groupBy("actor_login", "metric_date").agg(count("*").alias("daily_events"))
    )
    stats_pdf = daily.toPandas()

    if stats_pdf.empty:
        schema = (
            "metric_date date, actor_login string, burst_index double, "
            "stability_index double, quadrant string, rank_score double, "
            "total_events long, persona_label string"
        )
        return spark.createDataFrame([], schema=schema)

    grp = stats_pdf.groupby("actor_login")["daily_events"]
    agg = grp.agg(["count", "sum", "mean", "std", "max"]).fillna(0.0)
    agg = agg.rename(
        columns={"count": "days", "sum": "total_events", "mean": "avg", "std": "std", "max": "peak"}
    )
    # Keep top-N by total events so the scatter is readable; drop singletons.
    agg = agg.sort_values("total_events", ascending=False).head(top_actor_limit)
    agg = agg[agg["avg"] > 0.0].copy()

    agg["burst_index"] = agg["peak"] / agg["avg"]
    agg["cv"] = agg["std"] / agg["avg"]
    agg["stability_index"] = 1.0 / (1.0 + agg["cv"])
    # Normalise to 0..1 for quadrant cuts.
    def _normalize(s: pd.Series) -> pd.Series:
        lo, hi = s.min(), s.max()
        if hi - lo < 1e-9:
            return pd.Series([0.5] * len(s), index=s.index)
        return (s - lo) / (hi - lo)

    agg["burst_n"] = _normalize(agg["burst_index"])
    agg["stability_n"] = _normalize(agg["stability_index"])
    agg["rank_score"] = (agg["burst_n"] + agg["stability_n"]) / 2.0

    def _quadrant(row) -> str:
        if row["burst_n"] >= 0.5 and row["stability_n"] >= 0.5:
            return "rising_core"
        if row["burst_n"] >= 0.5:
            return "short_spike"
        if row["stability_n"] >= 0.5:
            return "steady_core"
        return "long_tail"

    agg["quadrant"] = agg.apply(_quadrant, axis=1)
    agg = agg.reset_index()  # actor_login becomes column

    persona_pdf = (
        actor_persona_df.select("actor_login", "persona_label").toPandas()
        if actor_persona_df is not None
        else pd.DataFrame(columns=["actor_login", "persona_label"])
    )
    agg = agg.merge(persona_pdf, on="actor_login", how="left")
    agg["persona_label"] = agg["persona_label"].fillna("unlabeled")

    rows = [
        (
            latest_metric_date,
            str(r["actor_login"]),
            round(float(r["burst_index"]), 6),
            round(float(r["stability_index"]), 6),
            str(r["quadrant"]),
            round(float(r["rank_score"]), 6),
            int(r["total_events"]),
            str(r["persona_label"]),
        )
        for _, r in agg.iterrows()
    ]
    schema = (
        "metric_date date, actor_login string, burst_index double, stability_index double, "
        "quadrant string, rank_score double, total_events long, persona_label string"
    )
    return spark.createDataFrame(rows, schema=schema)


def build_retention_curve(
    spark: SparkSession,
    events_df: DataFrame,
    latest_metric_date,
) -> DataFrame:
    """Weekly cohort retention curve — fraction of each week's newcomers still
    active at D={1,3,7,14,21,28} days after their first day.

    Week bucket = first_seen truncated to Monday. "Active at day D" = appears on
    any of the D following days (inclusive of day 0).
    """
    pdf = events_df.select("actor_login", "metric_date").distinct().toPandas()
    if pdf.empty:
        schema = (
            "metric_date date, cohort_week date, days_since_first int, "
            "retained_count long, cohort_size long, retention_rate double"
        )
        return spark.createDataFrame([], schema=schema)
    pdf["metric_date"] = pd.to_datetime(pdf["metric_date"])
    first = pdf.groupby("actor_login")["metric_date"].min().rename("first_date")
    pdf = pdf.merge(first, on="actor_login")
    pdf["days_since_first"] = (pdf["metric_date"] - pdf["first_date"]).dt.days
    pdf["cohort_week"] = pdf["first_date"].dt.to_period("W-MON").dt.start_time.dt.date

    results: list[tuple] = []
    for week, g in pdf.groupby("cohort_week"):
        cohort_size = int(g["actor_login"].nunique())
        if cohort_size == 0:
            continue
        for d in [0, 1, 3, 7, 14, 21, 28]:
            retained = int(g[g["days_since_first"] == d]["actor_login"].nunique())
            results.append(
                (
                    latest_metric_date,
                    week,
                    int(d),
                    retained,
                    cohort_size,
                    round(retained / cohort_size, 6),
                )
            )
    schema = (
        "metric_date date, cohort_week date, days_since_first int, retained_count long, "
        "cohort_size long, retention_rate double"
    )
    return spark.createDataFrame(results, schema=schema)


def build_churn_risk(
    spark: SparkSession,
    events_df: DataFrame,
    actor_persona_df: DataFrame,
    latest_metric_date,
    top_actor_limit: int = 5000,
) -> DataFrame:
    """Predict probability an actor will NOT appear in the last 7 days of the
    window, using features from the first 23 days. Logistic regression trained
    driver-side on the same corpus — a weak but interpretable baseline.
    """
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    pdf = (
        events_df.select("actor_login", "metric_date")
        .distinct()
        .toPandas()
    )
    if pdf.empty:
        schema = (
            "metric_date date, actor_login string, days_since_last int, "
            "decay_slope double, churn_prob double, risk_tier string, "
            "persona_label string"
        )
        return spark.createDataFrame([], schema=schema)

    pdf["metric_date"] = pd.to_datetime(pdf["metric_date"])
    window_end = pdf["metric_date"].max()
    train_cutoff = window_end - pd.Timedelta(days=7)

    # Per-actor: active days histogram + features
    actors = pdf["actor_login"].unique()
    # Restrict to top-N by event count to keep the predict set bounded.
    counts = pdf.groupby("actor_login").size().rename("events").reset_index()
    counts = counts.sort_values("events", ascending=False).head(top_actor_limit)
    keep = set(counts["actor_login"])
    pdf = pdf[pdf["actor_login"].isin(keep)]

    first = pdf.groupby("actor_login")["metric_date"].min()
    last = pdf.groupby("actor_login")["metric_date"].max()
    ev_count = pdf.groupby("actor_login").size()
    unique_days = pdf.groupby("actor_login")["metric_date"].nunique()

    # Decay slope: OLS slope of daily event count over last 14 days (pre-cutoff).
    recent = pdf[(pdf["metric_date"] >= train_cutoff - pd.Timedelta(days=14)) & (pdf["metric_date"] < train_cutoff)]
    slopes: dict[str, float] = {}
    for actor, g in recent.groupby("actor_login"):
        daily = g.groupby("metric_date").size()
        if len(daily) < 2:
            slopes[actor] = 0.0
            continue
        x = np.array([(d - train_cutoff).days for d in daily.index.to_pydatetime()], dtype=float)
        y = daily.values.astype(float)
        # simple slope
        if np.std(x) < 1e-9:
            slopes[actor] = 0.0
            continue
        slopes[actor] = float(np.polyfit(x, y, 1)[0])

    actor_rows: list[dict] = []
    # Labels: churn=1 if actor has zero events in [train_cutoff, window_end].
    late_actors = set(pdf[pdf["metric_date"] >= train_cutoff]["actor_login"].unique())
    for a in counts["actor_login"].tolist():
        last_date = last.get(a)
        days_since_last = int((window_end - last_date).days) if last_date is not None else 30
        feat = {
            "actor_login": a,
            "days_since_last": float(days_since_last),
            "decay_slope": float(slopes.get(a, 0.0)),
            "log_events": float(np.log1p(ev_count.get(a, 0))),
            "log_days": float(np.log1p(unique_days.get(a, 0))),
            "churn": int(a not in late_actors),
        }
        actor_rows.append(feat)
    df = pd.DataFrame(actor_rows)
    if len(df) < 50 or df["churn"].nunique() < 2:
        # Not enough variance — write fallback with zero probabilities.
        persona_pdf = (
            actor_persona_df.select("actor_login", "persona_label").toPandas()
            if actor_persona_df is not None
            else pd.DataFrame(columns=["actor_login", "persona_label"])
        )
        df = df.merge(persona_pdf, on="actor_login", how="left")
        df["persona_label"] = df["persona_label"].fillna("unlabeled")
        rows = [
            (
                latest_metric_date,
                str(r["actor_login"]),
                int(r["days_since_last"]),
                round(float(r["decay_slope"]), 6),
                0.0,
                "unknown",
                str(r["persona_label"]),
            )
            for _, r in df.iterrows()
        ]
        schema = (
            "metric_date date, actor_login string, days_since_last int, "
            "decay_slope double, churn_prob double, risk_tier string, "
            "persona_label string"
        )
        return spark.createDataFrame(rows, schema=schema)

    X = df[["days_since_last", "decay_slope", "log_events", "log_days"]].values
    y = df["churn"].values
    scaler = StandardScaler().fit(X)
    clf = LogisticRegression(max_iter=500)
    clf.fit(scaler.transform(X), y)
    probs = clf.predict_proba(scaler.transform(X))[:, 1]
    df["churn_prob"] = probs
    df["risk_tier"] = pd.cut(
        df["churn_prob"],
        bins=[-0.01, 0.3, 0.6, 1.01],
        labels=["healthy", "watch", "risk"],
    ).astype(str)
    persona_pdf = (
        actor_persona_df.select("actor_login", "persona_label").toPandas()
        if actor_persona_df is not None
        else pd.DataFrame(columns=["actor_login", "persona_label"])
    )
    df = df.merge(persona_pdf, on="actor_login", how="left")
    df["persona_label"] = df["persona_label"].fillna("unlabeled")

    rows = [
        (
            latest_metric_date,
            str(r["actor_login"]),
            int(r["days_since_last"]),
            round(float(r["decay_slope"]), 6),
            round(float(r["churn_prob"]), 6),
            str(r["risk_tier"]),
            str(r["persona_label"]),
        )
        for _, r in df.iterrows()
    ]
    schema = (
        "metric_date date, actor_login string, days_since_last int, "
        "decay_slope double, churn_prob double, risk_tier string, persona_label string"
    )
    return spark.createDataFrame(rows, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# Bucket C — individual influence
# ─────────────────────────────────────────────────────────────────────────────

def build_actor_hotness(
    spark: SparkSession,
    events_df: DataFrame,
    repo_rank_score_df: DataFrame | None,
    actor_persona_df: DataFrame,
    latest_metric_date,
) -> DataFrame:
    """Per-actor composite hotness score.

    hotness = log1p(events) * log1p(unique_repos) * avg(repo_rank_score)

    `repo_rank_score_df` is expected to have (metric_date, repo_name, rank_score)
    with metric_date = latest day.
    """
    from pyspark.sql.functions import log1p as sql_log1p, avg as sql_avg

    actor_repo_agg = (
        events_df.groupBy("actor_login", "repo_name")
        .agg(count("*").alias("ev"))
    )
    if repo_rank_score_df is not None:
        latest_ranks = (
            repo_rank_score_df.filter(col("metric_date") == lit(latest_metric_date))
            .select("repo_name", "rank_score")
        )
        actor_repo_agg = actor_repo_agg.join(latest_ranks, on="repo_name", how="left")
    else:
        actor_repo_agg = actor_repo_agg.withColumn("rank_score", lit(0.0))

    per_actor = (
        actor_repo_agg.groupBy("actor_login")
        .agg(
            sql_sum("ev").alias("event_count"),
            countDistinct("repo_name").alias("unique_repos"),
            sql_avg("rank_score").alias("avg_repo_rank_score"),
        )
    )
    per_actor = per_actor.withColumn(
        "hotness_score",
        sql_log1p(col("event_count").cast("double"))
        * sql_log1p(col("unique_repos").cast("double"))
        * (col("avg_repo_rank_score") + lit(1e-3)),
    )
    if actor_persona_df is not None:
        per_actor = per_actor.join(
            actor_persona_df.select("actor_login", "persona_label"),
            on="actor_login",
            how="left",
        )
    else:
        per_actor = per_actor.withColumn("persona_label", lit("unlabeled"))
    w = Window.partitionBy().orderBy(col("hotness_score").desc(), col("actor_login"))
    return (
        per_actor.withColumn("rank_no", row_number().over(w))
        .select(
            lit(latest_metric_date).alias("metric_date"),
            "actor_login",
            col("event_count").cast("long").alias("event_count"),
            col("unique_repos").cast("long").alias("unique_repos"),
            col("avg_repo_rank_score").cast("double").alias("avg_repo_rank_score"),
            col("hotness_score").cast("double").alias("hotness_score"),
            col("rank_no").cast("long").alias("rank_no"),
            col("persona_label"),
        )
        .filter(col("rank_no") <= lit(500))
    )


def build_repo_bus_factor(
    spark: SparkSession,
    events_df: DataFrame,
    repo_rank_score_df: DataFrame | None,
    latest_metric_date,
) -> DataFrame:
    """For each repo, the share of contribution coming from its top contributor.

    bus_factor tier:
        top_share ≥ 0.8 → "bus-1"  (one person sustains the repo)
        top_share ≥ 0.5 → "thin"   (concentrated risk)
        else           → "healthy"
    """
    per = (
        events_df.groupBy("repo_name", "actor_login")
        .agg(count("*").alias("ev"))
    )
    totals = per.groupBy("repo_name").agg(
        sql_sum("ev").alias("total_events"),
        countDistinct("actor_login").alias("contributor_count"),
    )
    ranked = per.withColumn(
        "rn",
        row_number().over(Window.partitionBy("repo_name").orderBy(col("ev").desc(), col("actor_login"))),
    ).filter(col("rn") == lit(1)).select(
        "repo_name",
        col("actor_login").alias("top_actor"),
        col("ev").alias("top_events"),
    )
    joined = ranked.join(totals, on="repo_name", how="inner").withColumn(
        "top_actor_share",
        col("top_events").cast("double") / greatest(col("total_events").cast("double"), lit(1.0)),
    )
    joined = joined.withColumn(
        "bus_factor",
        when(col("top_actor_share") >= lit(0.8), lit("bus-1"))
        .when(col("top_actor_share") >= lit(0.5), lit("thin"))
        .otherwise(lit("healthy")),
    )
    if repo_rank_score_df is not None:
        joined = joined.join(
            repo_rank_score_df.filter(col("metric_date") == lit(latest_metric_date))
            .select("repo_name", "rank_score"),
            on="repo_name",
            how="left",
        )
    else:
        joined = joined.withColumn("rank_score", lit(0.0))
    return joined.select(
        lit(latest_metric_date).alias("metric_date"),
        "repo_name",
        "top_actor",
        col("top_actor_share").cast("double"),
        col("contributor_count").cast("long"),
        col("bus_factor"),
        col("rank_score").alias("repo_rank_score"),
    ).filter(col("contributor_count") >= lit(3))


# ─────────────────────────────────────────────────────────────────────────────
# Bucket D — authenticity / anomaly
# ─────────────────────────────────────────────────────────────────────────────

def build_bot_supervised(
    spark: SparkSession,
    actor_persona_df: DataFrame,
    latest_metric_date,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Train an XGBoost bot classifier + IsolationForest anomaly scorer.

    Returns (scores_df, importance_df, meta_df) for three separate parquet
    outputs. Uses the 11 persona features already living on `actor_persona_df`
    plus the one-hot encoded persona_id. CV AUC is recorded in meta.
    """
    import numpy as np
    from sklearn.ensemble import IsolationForest
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import roc_auc_score, average_precision_score

    schema_scores = (
        "metric_date date, actor_login string, xgb_prob_bot double, iforest_score double, "
        "combined_score double, is_bot_truth int, rank_no long, persona_label string"
    )
    schema_imp = "metric_date date, feature string, importance double, rank_no int"
    schema_meta = "metric_date date, metric string, value double"

    pdf = actor_persona_df.toPandas()
    feature_cols = [
        "log_event_count",
        "active_days",
        "unique_repos",
        "hour_entropy",
        "repo_entropy",
        "push_share",
        "pr_share",
        "issues_share",
        "watch_share",
        "fork_share",
    ]
    available = [c for c in feature_cols if c in pdf.columns]
    if pdf.empty or not available or pdf["is_bot"].nunique() < 2:
        return (
            spark.createDataFrame([], schema=schema_scores),
            spark.createDataFrame([], schema=schema_imp),
            spark.createDataFrame([], schema=schema_meta),
        )

    # event_count / active_days / unique_repos arrive as longs — log-normalise
    # for tree splits that prefer smooth monotonic features.
    if "log_event_count" not in pdf.columns and "event_count" in pdf.columns:
        pdf["log_event_count"] = np.log1p(pdf["event_count"].astype(float))
    if "active_days" in pdf.columns:
        pdf["active_days"] = pdf["active_days"].astype(float)
    if "unique_repos" in pdf.columns:
        pdf["unique_repos"] = pdf["unique_repos"].astype(float)
    # Persona one-hots boost separation. Use a dedicated prefix so we don't
    # accidentally pick up the string column `persona_label` when filtering.
    persona_ids = pdf.get("persona_id")
    if persona_ids is not None:
        oh = pd.get_dummies(persona_ids.astype(int), prefix="personaid")
        pdf = pd.concat([pdf, oh], axis=1)
        feat = [c for c in pdf.columns if c.startswith("personaid_")] + available
    else:
        feat = available

    X = pdf[feat].astype(float).values
    y = pdf["is_bot"].astype(int).values

    import xgboost as xgb
    # CV AUC
    aucs: list[float] = []
    pr_aucs: list[float] = []
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr_idx, va_idx in skf.split(X, y):
        model_cv = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.08,
            subsample=0.9,
            colsample_bytree=0.9,
            eval_metric="auc",
            n_jobs=1,
            random_state=42,
        )
        model_cv.fit(X[tr_idx], y[tr_idx])
        proba = model_cv.predict_proba(X[va_idx])[:, 1]
        try:
            aucs.append(roc_auc_score(y[va_idx], proba))
            pr_aucs.append(average_precision_score(y[va_idx], proba))
        except ValueError:
            pass

    # Final full-fit for predictions + importances.
    final_model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.08,
        subsample=0.9,
        colsample_bytree=0.9,
        eval_metric="auc",
        n_jobs=1,
        random_state=42,
    )
    final_model.fit(X, y)
    xgb_prob = final_model.predict_proba(X)[:, 1]

    iso = IsolationForest(n_estimators=200, contamination="auto", random_state=42)
    iso.fit(X)
    # Invert so high = more anomalous and normalise 0..1.
    iso_score = -iso.score_samples(X)
    if iso_score.max() - iso_score.min() > 1e-9:
        iso_norm = (iso_score - iso_score.min()) / (iso_score.max() - iso_score.min())
    else:
        iso_norm = iso_score * 0.0

    combined = 0.7 * xgb_prob + 0.3 * iso_norm
    order = np.argsort(-combined)

    actor_col = pdf["actor_login"].astype(str).tolist()
    persona_col = pdf.get(
        "persona_label",
        pd.Series(["unlabeled"] * len(pdf), index=pdf.index),
    ).astype(str).tolist()

    rows = []
    for rank, idx in enumerate(order, start=1):
        rows.append(
            (
                latest_metric_date,
                actor_col[idx],
                round(float(xgb_prob[idx]), 6),
                round(float(iso_norm[idx]), 6),
                round(float(combined[idx]), 6),
                int(y[idx]),
                int(rank),
                persona_col[idx],
            )
        )
    scores_df = spark.createDataFrame(rows, schema=schema_scores)

    importances = list(zip(feat, final_model.feature_importances_.tolist()))
    importances.sort(key=lambda kv: kv[1], reverse=True)
    imp_rows = [
        (latest_metric_date, str(f), round(float(v), 6), int(i + 1))
        for i, (f, v) in enumerate(importances[:25])
    ]
    imp_df = spark.createDataFrame(imp_rows, schema=schema_imp)

    mean_auc = float(sum(aucs) / len(aucs)) if aucs else 0.0
    mean_pr = float(sum(pr_aucs) / len(pr_aucs)) if pr_aucs else 0.0
    meta_rows = [
        (latest_metric_date, "cv_auc_mean", round(mean_auc, 6)),
        (latest_metric_date, "cv_pr_auc_mean", round(mean_pr, 6)),
        (latest_metric_date, "cv_folds", 5.0),
        (latest_metric_date, "n_samples", float(len(y))),
        (latest_metric_date, "n_positive", float(int(y.sum()))),
    ]
    meta_df = spark.createDataFrame(meta_rows, schema=schema_meta)
    return scores_df, imp_df, meta_df


def build_actor_ring(
    spark: SparkSession,
    events_df: DataFrame,
    latest_metric_date,
    min_ring_size: int = 3,
    min_repos_shared: int = 2,
) -> DataFrame:
    """Ring detection on WatchEvent: find groups of ≥3 actors who star the
    same repositories within the same minute across ≥2 different repos.

    We only have hourly granularity in the curated data, so we treat each
    (metric_date, hour_of_day, repo_name) triple as a "burst". For each burst
    we capture the set of actors. Two bursts "match" when their actor sets
    have ≥3 common members — those common members are a ring candidate.
    """
    from itertools import combinations

    watch = (
        events_df.filter(col("event_type") == lit("WatchEvent"))
        .select("actor_login", "repo_name", "metric_date", "hour_of_day")
        .distinct()
        .toPandas()
    )
    schema = (
        "metric_date date, ring_id int, actor_count int, repos_shared int, "
        "sample_actors array<string>, avg_co_bursts double"
    )
    if watch.empty:
        return spark.createDataFrame([], schema=schema)

    buckets: dict[tuple, frozenset[str]] = {}
    for (d, h, r), g in watch.groupby(["metric_date", "hour_of_day", "repo_name"]):
        members = frozenset(g["actor_login"].tolist())
        if len(members) >= min_ring_size:
            buckets[(d, h, r)] = members

    # For pairs of buckets across DIFFERENT repos, find their common members.
    rings: dict[frozenset[str], dict] = {}
    keys = list(buckets.items())
    for i in range(len(keys)):
        (k_i, members_i) = keys[i]
        for j in range(i + 1, min(i + 1 + 200, len(keys))):  # window limit to avoid N^2 blowup
            (k_j, members_j) = keys[j]
            if k_i[2] == k_j[2]:  # same repo — skip
                continue
            inter = members_i & members_j
            if len(inter) >= min_ring_size:
                entry = rings.setdefault(
                    inter,
                    {"repos": set(), "co_bursts": 0},
                )
                entry["repos"].add(k_i[2])
                entry["repos"].add(k_j[2])
                entry["co_bursts"] += 1

    rows: list[tuple] = []
    ring_id = 0
    for members, stats_dict in rings.items():
        if len(stats_dict["repos"]) < min_repos_shared:
            continue
        ring_id += 1
        sample = sorted(list(members))[:10]
        rows.append(
            (
                latest_metric_date,
                int(ring_id),
                int(len(members)),
                int(len(stats_dict["repos"])),
                sample,
                round(float(stats_dict["co_bursts"]), 3),
            )
        )
    # Sort biggest-first and truncate to 50 rings.
    rows.sort(key=lambda r: (-r[2], -r[3]))
    return spark.createDataFrame(rows[:50], schema=schema)
