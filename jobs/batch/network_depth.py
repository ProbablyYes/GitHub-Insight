"""Network-page depth analyses (standalone Spark job).

Directions implemented:
    Phase 1  (one Spark run)
        D2  ALS implicit-feedback embedding + k-NN          → batch_repo_embedding
                                                            → batch_repo_als_neighbor
        D1  Multi-layer networks (watch / pr / push / fork) → batch_repo_layer_edge
                                                            → batch_repo_layer_community
        D4  k-core decomposition (actors + repos)           → batch_actor_coreness
                                                            → batch_repo_coreness
        D5  Meta-path similarity (A / L / O paths)          → batch_repo_metapath_sim
        D7  Repo archetype (rule + GMM validation)          → batch_repo_archetype
                                                            → batch_repo_archetype_centroid

    Phase 3  (separate run)
        D3  Temporal community evolution (weekly + Sankey)  → batch_repo_community_weekly
                                                            → batch_community_lineage

Direction 6 (Influence-Maximization) lives in a dedicated script
(``jobs/batch/network_ic.py``) because the Monte-Carlo loop can be long.

Run:
    spark-submit --master local[*] jobs/batch/network_depth.py \
        --input data/curated --output data/network_depth --phase 1
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    row_number,
    when,
)
from pyspark.sql.window import Window

try:
    from jobs.common.spark_runtime import validate_local_spark_runtime
except ModuleNotFoundError:  # support running from container where cwd is /app
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from jobs.common.spark_runtime import validate_local_spark_runtime

from jobs.batch import network_common as nc


LAYERS: dict[str, list[str]] = {
    "watch":  ["WatchEvent"],
    "fork":   ["ForkEvent"],
    "pr":     ["PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"],
    "push":   ["PushEvent"],
    "issue":  ["IssuesEvent", "IssueCommentEvent"],
}
ARCHETYPE_RULES = [
    "star_hungry_showcase",
    "bot_heavy_infra",
    "pr_heavy_collab",
    "burst_factory",
    "community_driven",
    "steady_core",
    "misc",
]


# ─────────────────────────────────────────────────────────────────────────────
# CLI + Spark
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Network-page depth analyses.")
    p.add_argument("--input", required=True, help="Curated parquet root.")
    p.add_argument("--output", required=True, help="Output parquet root.")
    p.add_argument("--phase", type=int, choices=(1, 3), default=1, help="Which phase to run.")
    p.add_argument("--top-n", type=int, default=200, help="Top-N repos kept for graph work.")
    p.add_argument("--als-rank", type=int, default=12, help="ALS latent-factor rank.")
    p.add_argument("--als-iter", type=int, default=10, help="ALS training iterations.")
    p.add_argument("--gmm-k", type=int, default=6, help="Number of GMM components for archetype.")
    p.add_argument("--weeks", type=int, default=4, help="Number of weeks for temporal evolution.")
    return p


def build_spark() -> SparkSession:
    validate_local_spark_runtime()
    return (
        SparkSession.builder.appName("github-network-depth")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "16MB")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# D2 · ALS embedding + k-NN
# ─────────────────────────────────────────────────────────────────────────────

def run_als_embedding(
    spark: SparkSession,
    events_df: DataFrame,
    scope_repo_df: DataFrame,
    metric_date,
    rank: int,
    max_iter: int,
) -> tuple[DataFrame, DataFrame]:
    """Implicit-feedback ALS → per-repo latent vector + top-5 cosine neighbors."""
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.recommendation import ALS

    # Build (actor, repo, events_count) triplet restricted to Top-N repos for tractability,
    # but keep ALL actors touching those repos so latent factors have enough signal.
    interactions = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .groupBy("actor_login", "repo_name")
        .agg(count("*").alias("n_events"))
    )
    interactions = interactions.withColumn("rating", col("n_events").cast("double"))

    actor_idx = StringIndexer(inputCol="actor_login", outputCol="actor_idx").fit(interactions)
    repo_idx = StringIndexer(inputCol="repo_name", outputCol="repo_idx").fit(interactions)
    indexed = repo_idx.transform(actor_idx.transform(interactions)).cache()
    _ = indexed.count()

    als = ALS(
        userCol="actor_idx",
        itemCol="repo_idx",
        ratingCol="rating",
        rank=rank,
        maxIter=max_iter,
        regParam=0.1,
        implicitPrefs=True,
        alpha=1.0,
        coldStartStrategy="drop",
        seed=42,
        nonnegative=False,
    )
    model = als.fit(indexed)

    # item factors: DataFrame(id → features Array[float])
    item_factors = model.itemFactors  # cols: id, features
    repo_labels = [(int(i), l) for i, l in zip(range(len(repo_idx.labels)), repo_idx.labels)]
    repo_labels_pdf = pd.DataFrame(repo_labels, columns=["repo_idx", "repo_name"])

    factors_pdf = item_factors.toPandas()
    factors_pdf = factors_pdf.rename(columns={"id": "repo_idx"})
    factors_pdf = factors_pdf.merge(repo_labels_pdf, on="repo_idx", how="inner")
    factors_pdf = factors_pdf.merge(
        scope_repo_df.select("repo_name", "rank_score", "total_events").toPandas(),
        on="repo_name",
        how="inner",
    )

    vec_matrix = np.asarray(factors_pdf["features"].tolist(), dtype=np.float32)
    # normalize for cosine
    norms = np.linalg.norm(vec_matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    unit = vec_matrix / norms

    embed_rows = []
    for repo_name, total_events, rank_score, feat in zip(
        factors_pdf["repo_name"],
        factors_pdf["total_events"].astype(int),
        factors_pdf["rank_score"].astype(float),
        factors_pdf["features"],
    ):
        embed_rows.append(
            (
                metric_date,
                repo_name,
                rank,
                [float(x) for x in feat],
                float(rank_score),
                int(total_events),
            )
        )
    embedding_schema = (
        "metric_date date, repo_name string, rank_dim int, factor_values array<float>, "
        "rank_score double, total_events bigint"
    )
    embedding_df = spark.createDataFrame(embed_rows, embedding_schema)

    # kNN cosine among the Top-N repos only → symmetric, exclude self, keep top-5
    sim = unit @ unit.T
    np.fill_diagonal(sim, -np.inf)

    # Load existing Jaccard edges (repo_similarity_full_df) to flag "jaccard-miss":
    # pairs discovered by ALS but without any shared-actor overlap. We approximate
    # by computing shared-actor count through the interactions table.
    actor_repo_pdf = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .select("actor_login", "repo_name")
        .distinct()
        .toPandas()
    )
    repo_to_actors: dict[str, set] = {
        r: set(g["actor_login"]) for r, g in actor_repo_pdf.groupby("repo_name")
    }

    names = factors_pdf["repo_name"].tolist()
    top_k = 5
    neighbor_rows = []
    for i, src in enumerate(names):
        order = np.argsort(-sim[i])[:top_k]
        src_actors = repo_to_actors.get(src, set())
        for rank_no, j in enumerate(order, start=1):
            dst = names[j]
            cos = float(sim[i, j])
            if not np.isfinite(cos):
                continue
            shared = len(src_actors & repo_to_actors.get(dst, set()))
            neighbor_rows.append(
                (
                    metric_date,
                    src,
                    dst,
                    round(cos, 6),
                    rank_no,
                    1 if shared < 2 else 0,
                )
            )
    neighbor_schema = (
        "metric_date date, src_repo string, dst_repo string, cosine double, "
        "rank_no int, is_jaccard_miss int"
    )
    neighbor_df = spark.createDataFrame(neighbor_rows, neighbor_schema)
    return embedding_df, neighbor_df


# ─────────────────────────────────────────────────────────────────────────────
# D1 · Multi-layer networks
# ─────────────────────────────────────────────────────────────────────────────

def run_multilayer(
    spark: SparkSession,
    events_df: DataFrame,
    scope_repo_df: DataFrame,
    metric_date,
) -> tuple[DataFrame, DataFrame]:
    """Build a Jaccard graph + LPA communities for each event-type layer."""
    edge_rows: list[tuple] = []
    comm_rows: list[tuple] = []

    scope_repo_list = scope_repo_df.select("repo_name", "rank_score").toPandas()
    rank_map = dict(zip(scope_repo_list["repo_name"], scope_repo_list["rank_score"]))

    for layer_name, event_types in LAYERS.items():
        layer_events = events_df.filter(col("event_type").isin(event_types))
        # per-repo actor sets inside this layer
        repo_actors = (
            layer_events.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
            .groupBy("repo_name")
            .agg(countDistinct("actor_login").alias("repo_actors"))
            .filter(col("repo_actors") >= lit(2))
        )
        actor_repo = (
            layer_events.select("actor_login", "repo_name")
            .distinct()
            .join(repo_actors.select("repo_name"), on="repo_name", how="inner")
        )
        edges = nc.jaccard_edges(actor_repo, repo_actors, threshold=0.05, min_shared=2)
        edges_pdf = edges.toPandas()
        if edges_pdf.empty:
            # still emit an empty community assignment so UI can render "no signal"
            continue

        for ra, rb, shared, jac in zip(
            edges_pdf["repo_a"], edges_pdf["repo_b"], edges_pdf["shared_actors"], edges_pdf["jaccard"]
        ):
            edge_rows.append(
                (metric_date, layer_name, str(ra), str(rb), float(jac), int(shared))
            )

        # LPA on (repo_a,repo_b,jaccard) — undirected, each pair once.
        nodes = repo_actors.select("repo_name").toPandas()["repo_name"].tolist()
        undirected_pdf = edges_pdf.rename(columns={"repo_a": "src", "repo_b": "dst"})
        label_map = nc.weighted_lpa(undirected_pdf, nodes, weight_col="jaccard")
        # compute community size + emit assignments
        comm_count: dict[str, int] = {}
        for c in label_map.values():
            comm_count[c] = comm_count.get(c, 0) + 1
        for repo, c in label_map.items():
            comm_rows.append(
                (
                    metric_date,
                    layer_name,
                    str(repo),
                    str(c),
                    int(comm_count.get(c, 1)),
                    float(rank_map.get(repo, 0.0) or 0.0),
                )
            )

    edge_schema = (
        "metric_date date, layer string, src_repo string, dst_repo string, "
        "jaccard double, shared_actors bigint"
    )
    comm_schema = (
        "metric_date date, layer string, repo_name string, community_id string, "
        "community_size int, rank_score double"
    )
    edges_df = spark.createDataFrame(edge_rows, edge_schema)
    comms_df = spark.createDataFrame(comm_rows, comm_schema)
    return edges_df, comms_df


# ─────────────────────────────────────────────────────────────────────────────
# D4 · k-core decomposition
# ─────────────────────────────────────────────────────────────────────────────

def run_kcore(
    spark: SparkSession,
    events_df: DataFrame,
    scope_repo_df: DataFrame,
    metric_date,
) -> tuple[DataFrame, DataFrame]:
    """Driver-side networkx k-core on (a) actor-actor (co-repo) graph limited to
    the top 3 000 actors, and (b) the repo-repo Jaccard graph we just built."""
    import networkx as nx

    # ── actor-actor co-repo graph
    top_actors_pdf = (
        events_df.groupBy("actor_login")
        .agg(count("*").alias("n_events"))
        .orderBy(col("n_events").desc())
        .limit(3000)
        .toPandas()
    )
    top_actor_set = set(top_actors_pdf["actor_login"].tolist())
    actor_repo_pdf = (
        events_df.filter(col("actor_login").isin(list(top_actor_set)))
        .select("actor_login", "repo_name", "actor_category")
        .distinct()
        .toPandas()
    )
    actor_to_repos: dict[str, set] = {}
    actor_to_cat: dict[str, str] = {}
    for a, r, c in zip(
        actor_repo_pdf["actor_login"], actor_repo_pdf["repo_name"], actor_repo_pdf["actor_category"]
    ):
        actor_to_repos.setdefault(a, set()).add(r)
        actor_to_cat[a] = c

    G_actor = nx.Graph()
    for a in actor_to_repos:
        G_actor.add_node(a)
    # cross-join only repos — build inverse index first
    repo_to_actors: dict[str, list[str]] = {}
    for a, repos in actor_to_repos.items():
        for r in repos:
            repo_to_actors.setdefault(r, []).append(a)

    for r, actors in repo_to_actors.items():
        if 2 <= len(actors) <= 300:  # skip mega-hubs to avoid n^2 blowup
            for i in range(len(actors)):
                for j in range(i + 1, len(actors)):
                    a1, a2 = actors[i], actors[j]
                    if G_actor.has_edge(a1, a2):
                        G_actor[a1][a2]["weight"] += 1
                    else:
                        G_actor.add_edge(a1, a2, weight=1)

    G_actor.remove_edges_from(nx.selfloop_edges(G_actor))
    core_actor = nx.core_number(G_actor) if G_actor.number_of_edges() else {n: 0 for n in G_actor}
    actor_rows = []
    for a in G_actor.nodes:
        deg = G_actor.degree(a)
        cat = (actor_to_cat.get(a) or "user").lower()
        is_bot = 1 if "bot" in cat else 0
        actor_rows.append(
            (
                metric_date,
                a,
                int(core_actor.get(a, 0)),
                int(deg),
                "bot" if is_bot else "human",
                is_bot,
            )
        )
    actor_schema = (
        "metric_date date, actor_login string, coreness int, degree int, "
        "persona_label string, is_bot int"
    )
    actor_df = spark.createDataFrame(actor_rows, actor_schema)

    # ── repo-repo Jaccard graph (reuse similarity)
    repo_actors_df = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .groupBy("repo_name")
        .agg(countDistinct("actor_login").alias("repo_actors"))
    )
    actor_repo_scope = (
        events_df.select("actor_login", "repo_name")
        .distinct()
        .join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
    )
    repo_edges_pdf = nc.jaccard_edges(actor_repo_scope, repo_actors_df, threshold=0.05, min_shared=2).toPandas()

    G_repo = nx.Graph()
    G_repo.add_nodes_from(scope_repo_df.select("repo_name").toPandas()["repo_name"].tolist())
    for ra, rb, jac in zip(
        repo_edges_pdf["repo_a"], repo_edges_pdf["repo_b"], repo_edges_pdf["jaccard"]
    ):
        G_repo.add_edge(ra, rb, weight=float(jac))
    G_repo.remove_edges_from(nx.selfloop_edges(G_repo))
    core_repo = nx.core_number(G_repo) if G_repo.number_of_edges() else {n: 0 for n in G_repo}

    repo_rank = dict(
        zip(
            scope_repo_df.select("repo_name").toPandas()["repo_name"],
            scope_repo_df.select("rank_score").toPandas()["rank_score"],
        )
    )
    coreness_values = np.array(list(core_repo.values())) if core_repo else np.array([0])
    p75 = float(np.percentile(coreness_values, 75)) if len(coreness_values) else 0.0
    p25 = float(np.percentile(coreness_values, 25)) if len(coreness_values) else 0.0

    def repo_cohort(k: int) -> str:
        if k >= p75 and k > 0:
            return "core"
        if k >= p25:
            return "middle"
        return "periphery"

    repo_rows = []
    for r in G_repo.nodes:
        k = int(core_repo.get(r, 0))
        repo_rows.append(
            (
                metric_date,
                r,
                k,
                int(G_repo.degree(r)),
                float(repo_rank.get(r, 0.0) or 0.0),
                repo_cohort(k),
            )
        )
    repo_schema = (
        "metric_date date, repo_name string, coreness int, degree int, "
        "rank_score double, cohort_group string"
    )
    repo_df = spark.createDataFrame(repo_rows, repo_schema)
    return actor_df, repo_df


# ─────────────────────────────────────────────────────────────────────────────
# D5 · Meta-path similarity
# ─────────────────────────────────────────────────────────────────────────────

def run_metapath(
    spark: SparkSession,
    events_df: DataFrame,
    scope_repo_df: DataFrame,
    metric_date,
) -> DataFrame:
    """Compute three meta-path similarity flavours among Top-N repos:

    * R-A-R  (shared actors, our "classic" view)
    * R-L-R  (same language_guess)
    * R-O-R  (same repo_owner / org)

    Similarity is Jaccard-style over the bridging-node set. Output is top-5
    neighbours per (path_type, src_repo).
    """
    scope_names = set(scope_repo_df.select("repo_name").toPandas()["repo_name"])

    # bring to driver
    pdf = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .select("actor_login", "repo_name", "language_guess", "repo_owner")
        .distinct()
        .toPandas()
    )

    def sim_from_sets(sets: dict[str, set]) -> list[tuple]:
        names = list(sets.keys())
        rows: list[tuple[str, str, int, float]] = []
        for i, a in enumerate(names):
            sa = sets[a]
            if not sa:
                continue
            for b in names[i + 1:]:
                sb = sets[b]
                if not sb:
                    continue
                shared = len(sa & sb)
                if shared == 0:
                    continue
                union = len(sa | sb)
                if union == 0:
                    continue
                jac = shared / union
                if jac >= 0.05:
                    rows.append((a, b, shared, jac))
        return rows

    # R-A-R
    r_a: dict[str, set] = {r: set(g["actor_login"]) for r, g in pdf.groupby("repo_name")}
    # R-L-R — language is just a scalar per repo; set-similarity on actor overlap *inside* the same language? No — meta-path asks "repos bridged by language", which is trivially equivalence-class. We instead define R-L-R as Jaccard on the shared **actors who also touched same-language repos**, which collapses to R-A-R filtered. For the demo, we use a simpler variant: same-language repos get similarity 1.0, different-language 0.0. We emit that as top-neighbors grouped by language.
    r_l: dict[str, str] = {
        r: g["language_guess"].mode().iloc[0] if len(g) else "unknown" for r, g in pdf.groupby("repo_name")
    }
    r_o: dict[str, str] = {
        r: g["repo_owner"].mode().iloc[0] if len(g) else "unknown" for r, g in pdf.groupby("repo_name")
    }

    rows_out: list[tuple] = []
    # path = actor
    for a, b, shared, jac in sim_from_sets(r_a):
        rows_out.append(("RAR", a, b, shared, jac))
    # path = language — pair up repos sharing non-"unknown" language, sim = 1.0
    lang_to_repos: dict[str, list[str]] = {}
    for r, lang in r_l.items():
        if lang and lang != "unknown":
            lang_to_repos.setdefault(lang, []).append(r)
    for lang, repos in lang_to_repos.items():
        if len(repos) < 2:
            continue
        # cap to avoid n^2 blowup
        repos = sorted(repos)[:50]
        for i, a in enumerate(repos):
            for b in repos[i + 1:]:
                rows_out.append(("RLR", a, b, len(repos), 1.0))
    # path = owner — same idea
    owner_to_repos: dict[str, list[str]] = {}
    for r, own in r_o.items():
        if own and own != "unknown":
            owner_to_repos.setdefault(own, []).append(r)
    for own, repos in owner_to_repos.items():
        if len(repos) < 2:
            continue
        for i, a in enumerate(repos):
            for b in repos[i + 1:]:
                rows_out.append(("ROR", a, b, len(repos), 1.0))

    # symmetric expand + per (path_type, src) top-5
    expanded = []
    for path_type, a, b, shared, jac in rows_out:
        expanded.append((path_type, a, b, shared, jac))
        expanded.append((path_type, b, a, shared, jac))

    out_rows: list[tuple] = []
    bucket: dict[tuple[str, str], list[tuple[str, int, float]]] = {}
    for p, a, b, shared, jac in expanded:
        bucket.setdefault((p, a), []).append((b, shared, jac))
    for (p, src), lst in bucket.items():
        lst.sort(key=lambda x: (-x[2], -x[1], x[0]))
        for rank_no, (dst, shared, jac) in enumerate(lst[:5], start=1):
            out_rows.append(
                (metric_date, p, src, dst, float(jac), int(shared), rank_no)
            )

    schema = (
        "metric_date date, path_type string, src_repo string, dst_repo string, "
        "sim double, shared_actors bigint, rank_no int"
    )
    return spark.createDataFrame(out_rows, schema)


# ─────────────────────────────────────────────────────────────────────────────
# D7 · Repo archetype (rule + GMM)
# ─────────────────────────────────────────────────────────────────────────────

def run_archetype(
    spark: SparkSession,
    events_df: DataFrame,
    scope_repo_df: DataFrame,
    metric_date,
    gmm_k: int,
) -> tuple[DataFrame, DataFrame]:
    """Feature-engineer each Top-N repo, assign rule label + GMM cluster id,
    project to 2-D via PCA for plotting."""
    from sklearn.decomposition import PCA
    from sklearn.mixture import GaussianMixture
    from sklearn.preprocessing import StandardScaler

    scope_names = scope_repo_df.select("repo_name").toPandas()["repo_name"].tolist()

    # feature frame
    from pyspark.sql.functions import (
        countDistinct as cd,
        count as c_,
        sum as sum_,
        when as wh,
    )

    feats_df = (
        events_df.join(scope_repo_df.select("repo_name"), on="repo_name", how="inner")
        .groupBy("repo_name")
        .agg(
            c_("*").alias("total_events"),
            cd("actor_login").alias("contributor_count"),
            cd("metric_date").alias("active_days"),
            sum_(wh(col("event_type") == "WatchEvent", 1).otherwise(0)).alias("watch_events"),
            sum_(wh(col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_events"),
            sum_(wh(col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_events"),
            sum_(wh(col("event_type").isin("PullRequestEvent", "PullRequestReviewEvent",
                                            "PullRequestReviewCommentEvent"), 1).otherwise(0))
            .alias("pr_events"),
            sum_(wh(col("event_type").isin("IssuesEvent", "IssueCommentEvent"), 1).otherwise(0))
            .alias("issue_events"),
            sum_(wh(col("actor_category").contains("bot"), 1).otherwise(0)).alias("bot_events"),
        )
    )
    pdf = feats_df.toPandas()
    pdf = pdf.merge(
        scope_repo_df.select("repo_name", "rank_score").toPandas(),
        on="repo_name",
        how="inner",
    )
    # derived ratios
    pdf["watch_share"] = pdf["watch_events"] / pdf["total_events"].clip(lower=1)
    pdf["pr_push_ratio"] = pdf["pr_events"] / pdf["push_events"].clip(lower=1)
    pdf["bot_ratio"] = pdf["bot_events"] / pdf["total_events"].clip(lower=1)
    pdf["events_per_active_day"] = pdf["total_events"] / pdf["active_days"].clip(lower=1)
    pdf = pdf.fillna(0.0)

    # ── rule labels
    def rule(row):
        if row["bot_ratio"] > 0.5:
            return "bot_heavy_infra"
        if row["watch_share"] > 0.6 and row["pr_push_ratio"] < 0.1:
            return "star_hungry_showcase"
        if row["pr_push_ratio"] > 2.0:
            return "pr_heavy_collab"
        if row["events_per_active_day"] > 400 and row["active_days"] <= 5:
            return "burst_factory"
        if row["watch_share"] > 0.3 and row["contributor_count"] > 20:
            return "community_driven"
        if row["active_days"] >= 4 and 0.2 <= row["pr_push_ratio"] <= 3.0 and row["contributor_count"] >= 5:
            return "steady_core"
        return "misc"

    pdf["archetype_rule"] = pdf.apply(rule, axis=1)

    feature_cols = [
        "watch_share",
        "pr_push_ratio",
        "bot_ratio",
        "active_days",
        "contributor_count",
        "events_per_active_day",
    ]
    X = pdf[feature_cols].to_numpy(dtype=float)
    # clip extreme values so one outlier doesn't dominate the scaler
    X = np.clip(X, -1e6, 1e6)
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)

    gmm = GaussianMixture(
        n_components=min(gmm_k, max(2, len(Xs) // 10)),
        covariance_type="full",
        random_state=42,
        reg_covar=1e-4,
        max_iter=200,
    )
    gmm.fit(Xs)
    gmm_ids = gmm.predict(Xs)
    gmm_prob = gmm.predict_proba(Xs).max(axis=1)

    pca = PCA(n_components=2, random_state=42).fit(Xs)
    pca_xy = pca.transform(Xs)

    pdf["archetype_gmm_id"] = gmm_ids.astype(int)
    pdf["archetype_confidence"] = gmm_prob.astype(float)
    pdf["pca_x"] = pca_xy[:, 0]
    pdf["pca_y"] = pca_xy[:, 1]

    def note_for(row) -> str:
        return (
            f"watch {row['watch_share']:.0%} · pr/push {row['pr_push_ratio']:.2f} · "
            f"bot {row['bot_ratio']:.0%} · {int(row['active_days'])}d × {int(row['contributor_count'])}ppl"
        )

    pdf["sample_note"] = pdf.apply(note_for, axis=1)

    arch_rows = [
        (
            metric_date,
            str(r["repo_name"]),
            str(r["archetype_rule"]),
            int(r["archetype_gmm_id"]),
            float(r["archetype_confidence"]),
            float(r["pca_x"]),
            float(r["pca_y"]),
            float(r["rank_score"]),
            int(r["total_events"]),
            float(r["watch_share"]),
            float(r["pr_push_ratio"]),
            float(r["bot_ratio"]),
            int(r["active_days"]),
            int(r["contributor_count"]),
            str(r["sample_note"]),
        )
        for _, r in pdf.iterrows()
    ]
    arch_schema = (
        "metric_date date, repo_name string, archetype_rule string, archetype_gmm_id int, "
        "archetype_confidence double, pca_x double, pca_y double, rank_score double, "
        "total_events bigint, watch_share double, pr_push_ratio double, bot_ratio double, "
        "active_days int, contributor_count int, sample_note string"
    )
    archetype_df = spark.createDataFrame(arch_rows, arch_schema)

    # centroid — one row per (rule, gmm_id)
    cent_rows: list[tuple] = []
    for (rule_name, gid), grp in pdf.groupby(["archetype_rule", "archetype_gmm_id"]):
        overlap_total = len(pdf[pdf["archetype_gmm_id"] == gid])
        members = len(grp)
        samples = " | ".join(
            grp.sort_values("rank_score", ascending=False).head(3)["repo_name"].tolist()
        )
        cent_rows.append(
            (
                metric_date,
                str(rule_name),
                int(gid),
                int(members),
                float(grp["watch_share"].mean()),
                float(grp["pr_push_ratio"].mean()),
                float(grp["bot_ratio"].mean()),
                float(grp["active_days"].mean()),
                float(grp["rank_score"].mean()),
                float(members / overlap_total) if overlap_total else 0.0,
                samples,
            )
        )
    cent_schema = (
        "metric_date date, archetype_rule string, archetype_gmm_id int, members int, "
        "avg_watch_share double, avg_pr_push_ratio double, avg_bot_ratio double, "
        "avg_active_days double, avg_rank_score double, rule_gmm_overlap double, "
        "sample_repos string"
    )
    centroid_df = spark.createDataFrame(cent_rows, cent_schema)
    return archetype_df, centroid_df


# ─────────────────────────────────────────────────────────────────────────────
# D3 · Temporal community evolution (Phase 3)
# ─────────────────────────────────────────────────────────────────────────────

def run_temporal_evolution(
    spark: SparkSession,
    events_df: DataFrame,
    metric_date,
    num_weeks: int,
) -> tuple[DataFrame, DataFrame]:
    """Slide a weekly window, rebuild Top-N-repo Jaccard graph + LPA per week,
    then match communities across consecutive weeks via Jaccard on member sets."""
    from pyspark.sql.functions import date_sub

    # collect distinct dates, pick latest K*7 days, bucket into K weeks
    dates_pdf = (
        events_df.select("metric_date").distinct().orderBy(col("metric_date")).toPandas()
    )
    if dates_pdf.empty:
        empty_weekly = spark.createDataFrame(
            [],
            "metric_date date, week_idx int, week_start date, week_end date, "
            "repo_name string, community_id string, community_size int",
        )
        empty_lineage = spark.createDataFrame(
            [],
            "metric_date date, lineage_id int, week_idx int, community_id string, "
            "prev_community_id string, event_type string, members_count int, "
            "overlap_jaccard double, sample_members string",
        )
        return empty_weekly, empty_lineage

    dates = pd.to_datetime(dates_pdf["metric_date"])
    latest = dates.max()
    buckets: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for i in range(num_weeks):
        end = latest - timedelta(days=7 * i)
        start = end - timedelta(days=6)
        buckets.append((start, end))
    buckets = list(reversed(buckets))  # oldest first, week_idx 0..K-1

    weekly_rows: list[tuple] = []
    week_communities: list[dict[str, set[str]]] = []  # week_idx → {community_id → set(members)}

    for w_idx, (ws, we) in enumerate(buckets):
        week_df = events_df.filter(
            (col("metric_date") >= lit(ws.date())) & (col("metric_date") <= lit(we.date()))
        )
        if week_df.limit(1).count() == 0:
            week_communities.append({})
            continue
        scope_week = nc.scope_top_repos(week_df, top_n=120)
        repo_actors = (
            week_df.join(scope_week.select("repo_name"), on="repo_name", how="inner")
            .groupBy("repo_name")
            .agg(countDistinct("actor_login").alias("repo_actors"))
            .filter(col("repo_actors") >= lit(2))
        )
        actor_repo = (
            week_df.select("actor_login", "repo_name")
            .distinct()
            .join(repo_actors.select("repo_name"), on="repo_name", how="inner")
        )
        edges_pdf = nc.jaccard_edges(actor_repo, repo_actors, threshold=0.05, min_shared=2).toPandas()
        nodes = repo_actors.select("repo_name").toPandas()["repo_name"].tolist()
        if edges_pdf.empty or not nodes:
            week_communities.append({})
            continue
        labels = nc.weighted_lpa(
            edges_pdf.rename(columns={"repo_a": "src", "repo_b": "dst"}), nodes, "jaccard"
        )
        community_to_members: dict[str, set[str]] = {}
        for node, c in labels.items():
            community_to_members.setdefault(c, set()).add(node)
        week_communities.append(community_to_members)

        for node, c in labels.items():
            weekly_rows.append(
                (
                    metric_date,
                    w_idx,
                    ws.date(),
                    we.date(),
                    node,
                    c,
                    len(community_to_members[c]),
                )
            )

    # ── lineage: match each community at week w to best predecessor at w-1 by Jaccard
    lineage_rows: list[tuple] = []
    lineage_id = 0
    for w_idx in range(1, len(week_communities)):
        prev = week_communities[w_idx - 1]
        cur = week_communities[w_idx]
        for c_id, members in cur.items():
            best_prev, best_j = "", 0.0
            for p_id, p_members in prev.items():
                inter = len(members & p_members)
                if inter == 0:
                    continue
                union = len(members | p_members)
                if union == 0:
                    continue
                j = inter / union
                if j > best_j:
                    best_j = j
                    best_prev = p_id
            if not best_prev:
                event_type = "birth"
            elif best_j >= 0.6:
                event_type = "continue"
            elif best_j >= 0.2:
                event_type = "merge_or_split"
            else:
                event_type = "reform"
            lineage_id += 1
            sample = " | ".join(sorted(members)[:3])
            lineage_rows.append(
                (
                    metric_date,
                    lineage_id,
                    w_idx,
                    c_id,
                    best_prev,
                    event_type,
                    len(members),
                    float(best_j),
                    sample,
                )
            )
        # also flag deaths — communities in prev but not continuing forward
        for p_id, p_members in prev.items():
            # matched forward?
            any_match = False
            for c_id, members in cur.items():
                inter = len(members & p_members)
                if inter and (inter / max(len(members | p_members), 1)) >= 0.2:
                    any_match = True
                    break
            if not any_match:
                lineage_id += 1
                sample = " | ".join(sorted(p_members)[:3])
                lineage_rows.append(
                    (
                        metric_date,
                        lineage_id,
                        w_idx,
                        "",
                        p_id,
                        "death",
                        len(p_members),
                        0.0,
                        sample,
                    )
                )

    weekly_schema = (
        "metric_date date, week_idx int, week_start date, week_end date, "
        "repo_name string, community_id string, community_size int"
    )
    lineage_schema = (
        "metric_date date, lineage_id int, week_idx int, community_id string, "
        "prev_community_id string, event_type string, members_count int, "
        "overlap_jaccard double, sample_members string"
    )
    return (
        spark.createDataFrame(weekly_rows, weekly_schema),
        spark.createDataFrame(lineage_rows, lineage_schema),
    )


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

def _write(df: DataFrame, output_root: str, name: str) -> int:
    rows = df.count()
    (df.write.mode("overwrite").parquet(str(Path(output_root) / name)))
    print(f"  • {name}: {rows} rows")
    return rows


def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark()
    try:
        events_df = nc.read_curated_events(spark, args.input).cache()
        total_events = events_df.count()
        metric_date = nc.latest_metric_date(events_df)
        print(
            f"network_depth.py  events={total_events:,}  metric_date={metric_date}  phase={args.phase}"
        )

        scope_repo_df = nc.scope_top_repos(events_df, top_n=args.top_n)
        scoped = scope_repo_df.count()
        print(f"  scope_top_repos: {scoped} repos")

        nc.ensure_output_dir(args.output)
        out = args.output

        summary: dict[str, int] = {}

        if args.phase == 1:
            print("[phase 1 · D2] ALS embedding + k-NN")
            embed_df, neigh_df = run_als_embedding(
                spark, events_df, scope_repo_df, metric_date, args.als_rank, args.als_iter
            )
            summary["repo_embedding"] = _write(embed_df, out, "repo_embedding")
            summary["repo_als_neighbor"] = _write(neigh_df, out, "repo_als_neighbor")

            print("[phase 1 · D1] Multi-layer networks")
            layer_edge_df, layer_comm_df = run_multilayer(spark, events_df, scope_repo_df, metric_date)
            summary["repo_layer_edge"] = _write(layer_edge_df, out, "repo_layer_edge")
            summary["repo_layer_community"] = _write(layer_comm_df, out, "repo_layer_community")

            print("[phase 1 · D4] k-core decomposition")
            actor_core_df, repo_core_df = run_kcore(spark, events_df, scope_repo_df, metric_date)
            summary["actor_coreness"] = _write(actor_core_df, out, "actor_coreness")
            summary["repo_coreness"] = _write(repo_core_df, out, "repo_coreness")

            print("[phase 1 · D5] Meta-path similarity")
            metapath_df = run_metapath(spark, events_df, scope_repo_df, metric_date)
            summary["repo_metapath_sim"] = _write(metapath_df, out, "repo_metapath_sim")

            print("[phase 1 · D7] Repo archetype")
            arch_df, arch_cent_df = run_archetype(
                spark, events_df, scope_repo_df, metric_date, args.gmm_k
            )
            summary["repo_archetype"] = _write(arch_df, out, "repo_archetype")
            summary["repo_archetype_centroid"] = _write(arch_cent_df, out, "repo_archetype_centroid")

        elif args.phase == 3:
            print("[phase 3 · D3] Temporal community evolution")
            weekly_df, lineage_df = run_temporal_evolution(
                spark, events_df, metric_date, args.weeks
            )
            summary["repo_community_weekly"] = _write(weekly_df, out, "repo_community_weekly")
            summary["community_lineage"] = _write(lineage_df, out, "community_lineage")

        print("\nSUMMARY:")
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
