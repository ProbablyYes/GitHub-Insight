"""Influence-Maximization on the GitHub actor–actor collaboration graph.

Direction 6 (Phase 4) of the Network-page depth analysis.

Pipeline:
    1. Build actor–actor weighted graph (weight = #shared repos among top-N actors).
    2. Convert weights to IC activation probabilities (WIC-style, see `edge_prob`).
    3. Pre-sample R *live-edge* subgraphs (standard IC trick).
    4. Pre-compute baseline centralities (degree / PageRank / coreness).
    5. Run CELF-greedy up to k_max seeds.
    6. Monte-Carlo evaluate several strategies (greedy / top_degree / top_pagerank /
       top_coreness / random) at multiple k values and save expected reach.

Outputs (parquet, consumed by load_batch_metrics_to_clickhouse.py --only network):
    seed_greedy/        → batch_seed_greedy
    actor_ic_reach/     → batch_actor_ic_reach

Run:
    spark-submit --master local[*] jobs/batch/network_ic.py \
        --input data/curated --output data/network_depth \
        --top-actors 1500 --mc-runs 200 --k-max 20
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from heapq import heappop, heappush
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

try:
    from jobs.common.spark_runtime import validate_local_spark_runtime
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from jobs.common.spark_runtime import validate_local_spark_runtime

from jobs.batch import network_common as nc


# ─────────────────────────────────────────────────────────────────────────────
# CLI + Spark
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="IC + greedy seed on actor–actor graph.")
    p.add_argument("--input", required=True, help="Curated parquet root.")
    p.add_argument("--output", required=True, help="Output parquet root.")
    p.add_argument("--top-actors", type=int, default=1500,
                   help="Top-N actors by event count kept in the graph.")
    p.add_argument("--max-repo-actors", type=int, default=300,
                   help="Skip repos with >this many actors (avoid n^2 hub blow-up).")
    p.add_argument("--min-shared", type=int, default=1,
                   help="Drop actor-pairs with fewer shared repos.")
    p.add_argument("--p0", type=float, default=0.02,
                   help="Per-repo activation probability (edge prob = 1-(1-p0)^w).")
    p.add_argument("--p-max", type=float, default=0.20,
                   help="Upper cap on activation probability.")
    p.add_argument("--mc-runs", type=int, default=200,
                   help="Monte-Carlo live-edge samples (more = tighter estimate).")
    p.add_argument("--k-max", type=int, default=20,
                   help="Max seed-set size for greedy + eval.")
    p.add_argument("--seed", type=int, default=42)
    return p


def build_spark() -> SparkSession:
    validate_local_spark_runtime()
    return (
        SparkSession.builder.appName("github-network-ic")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "16MB")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Graph construction (driver-side pandas/numpy)
# ─────────────────────────────────────────────────────────────────────────────

def build_actor_graph(
    events_df: DataFrame,
    top_actors: int,
    max_repo_actors: int,
    min_shared: int,
):
    """Return (nodes, is_bot, persona, adj, weights, degree).

    adj[i]   → int32 numpy array of neighbor indices (sorted).
    weights[i] → int32 numpy array, weight(i,j) = #shared repos.
    """
    actor_events_pdf = (
        events_df.groupBy("actor_login")
        .agg(count("*").alias("n_events"))
        .orderBy(col("n_events").desc())
        .limit(top_actors)
        .toPandas()
    )
    kept_actors = actor_events_pdf["actor_login"].tolist()
    actor_to_idx = {a: i for i, a in enumerate(kept_actors)}

    # category lookup (bot detection) — one per actor, take first seen
    cat_pdf = (
        events_df.filter(col("actor_login").isin(kept_actors))
        .select("actor_login", "actor_category")
        .dropDuplicates(["actor_login"])
        .toPandas()
    )
    cat_map = dict(zip(cat_pdf["actor_login"], cat_pdf["actor_category"]))

    # actor-repo bipartite (only within kept actors)
    actor_repo_pdf = (
        events_df.filter(col("actor_login").isin(kept_actors))
        .select("actor_login", "repo_name")
        .distinct()
        .toPandas()
    )

    repo_to_actors: dict[str, list[int]] = {}
    for a, r in zip(actor_repo_pdf["actor_login"], actor_repo_pdf["repo_name"]):
        idx = actor_to_idx[a]
        repo_to_actors.setdefault(r, []).append(idx)

    # accumulate pair weights via sparse COO dict
    from collections import defaultdict
    pair_w: dict[tuple[int, int], int] = defaultdict(int)
    for r, actors in repo_to_actors.items():
        if len(actors) < 2 or len(actors) > max_repo_actors:
            continue
        actors.sort()
        for i in range(len(actors)):
            ai = actors[i]
            for j in range(i + 1, len(actors)):
                aj = actors[j]
                pair_w[(ai, aj)] += 1

    # build adjacency lists
    n = len(kept_actors)
    adj_lists: list[list[int]] = [[] for _ in range(n)]
    w_lists: list[list[int]] = [[] for _ in range(n)]
    for (i, j), w in pair_w.items():
        if w < min_shared:
            continue
        adj_lists[i].append(j)
        w_lists[i].append(w)
        adj_lists[j].append(i)
        w_lists[j].append(w)

    adj = [np.asarray(a, dtype=np.int32) for a in adj_lists]
    weights = [np.asarray(w, dtype=np.int32) for w in w_lists]
    degree = np.asarray([a.size for a in adj], dtype=np.int32)

    # sort neighbors by index (helps cache locality in BFS)
    for i in range(n):
        if adj[i].size > 1:
            order = np.argsort(adj[i])
            adj[i] = adj[i][order]
            weights[i] = weights[i][order]

    is_bot = np.asarray(
        [1 if "bot" in (cat_map.get(a) or "user").lower() else 0 for a in kept_actors],
        dtype=np.int8,
    )
    persona = np.asarray(
        ["bot" if b else "human" for b in is_bot], dtype=object
    )
    return kept_actors, is_bot, persona, adj, weights, degree


def edge_prob(weight_array: np.ndarray, p0: float, p_max: float) -> np.ndarray:
    """Weighted IC activation probability: p = min(p_max, 1 − (1−p0)^w)."""
    p = 1.0 - np.power(1.0 - p0, weight_array.astype(np.float64))
    return np.minimum(p, p_max).astype(np.float32)


# ─────────────────────────────────────────────────────────────────────────────
# Live-edge sampling + reach
# ─────────────────────────────────────────────────────────────────────────────

def sample_live_edge_graphs(
    adj: list[np.ndarray],
    probs: list[np.ndarray],
    runs: int,
    rng: np.random.Generator,
) -> list[list[np.ndarray]]:
    """Pre-sample `runs` live-edge subgraphs.

    Return: list of length `runs`, each a list[np.ndarray]; element i is the array
    of neighbors of node i that SURVIVED in that sample.
    The IC activation spread starting at seed S in sample r is exactly the BFS
    reach of S in sample r's undirected live-edge graph.
    """
    n = len(adj)
    samples: list[list[np.ndarray]] = []
    for _ in range(runs):
        per_node: list[np.ndarray] = [np.empty(0, dtype=np.int32)] * n
        for i in range(n):
            if adj[i].size == 0:
                continue
            mask = rng.random(adj[i].size) < probs[i]
            if mask.any():
                per_node[i] = adj[i][mask]
        samples.append(per_node)
    return samples


def bfs_reach(seeds: list[int] | set[int], sample: list[np.ndarray]) -> int:
    """Count nodes reachable from `seeds` in a single live-edge sample."""
    if not seeds:
        return 0
    seen = bytearray(len(sample))
    stack = list(seeds)
    for s in stack:
        seen[s] = 1
    # iterative DFS (no recursion risk)
    i = 0
    while i < len(stack):
        u = stack[i]
        i += 1
        nbrs = sample[u]
        if nbrs.size == 0:
            continue
        for v in nbrs:
            if not seen[v]:
                seen[v] = 1
                stack.append(int(v))
    return len(stack)


def expected_reach(
    seeds: list[int] | set[int],
    samples: list[list[np.ndarray]],
) -> tuple[float, float]:
    vals = np.asarray([bfs_reach(seeds, s) for s in samples], dtype=np.float64)
    return float(vals.mean()), float(vals.std())


def _marginal_gain(
    node: int,
    current_seed_set: set[int],
    samples: list[list[np.ndarray]],
    current_reach_per_sample: np.ndarray,
) -> float:
    """Marginal increase in reach if `node` is added to `current_seed_set`."""
    if node in current_seed_set:
        return 0.0
    acc = 0.0
    seeds = current_seed_set | {node}
    for r, sample in enumerate(samples):
        acc += bfs_reach(seeds, sample) - current_reach_per_sample[r]
    return acc / len(samples)


# ─────────────────────────────────────────────────────────────────────────────
# CELF greedy
# ─────────────────────────────────────────────────────────────────────────────

def celf_greedy(
    samples: list[list[np.ndarray]],
    n: int,
    k_max: int,
    candidate_pool: list[int] | None = None,
) -> list[tuple[int, float, float]]:
    """Standard CELF (Leskovec 2007).

    Returns list of (node, marginal_gain, cumulative_reach) in pick order.

    The candidate_pool argument lets us skip actors with zero reach potential
    (isolated nodes) to cut runtime.
    """
    pool = candidate_pool if candidate_pool is not None else list(range(n))

    # reach per sample at the current seed set = 0 initially
    cur_reach_per_sample = np.zeros(len(samples), dtype=np.float64)
    cur_seed: set[int] = set()
    cumulative = 0.0
    picked: list[tuple[int, float, float]] = []

    # Initial priority queue: single-seed reach for every candidate
    # heap stores (-gain, node, last_update_idx)
    heap: list[tuple[float, int, int]] = []
    for v in pool:
        g = 0.0
        for sample in samples:
            g += bfs_reach([v], sample)
        g /= len(samples)
        heappush(heap, (-g, v, 0))

    while heap and len(picked) < k_max:
        neg_g, v, last_idx = heappop(heap)
        if last_idx == len(picked):
            # gain was computed against the current seed set → accept
            gain = -neg_g
            cur_seed.add(v)
            cumulative += gain
            # refresh per-sample reach with new seed
            for r, sample in enumerate(samples):
                cur_reach_per_sample[r] = bfs_reach(cur_seed, sample)
            picked.append((v, gain, cumulative))
        else:
            # stale → re-evaluate marginal gain
            gain = _marginal_gain(v, cur_seed, samples, cur_reach_per_sample)
            heappush(heap, (-gain, v, len(picked)))
    return picked


# ─────────────────────────────────────────────────────────────────────────────
# Centralities
# ─────────────────────────────────────────────────────────────────────────────

def compute_pagerank_and_coreness(
    n: int, adj: list[np.ndarray], weights: list[np.ndarray],
) -> tuple[np.ndarray, np.ndarray]:
    """PageRank + k-core via NetworkX (already available in spark-batch image)."""
    import networkx as nx

    G = nx.Graph()
    G.add_nodes_from(range(n))
    seen: set[tuple[int, int]] = set()
    for u in range(n):
        for v, w in zip(adj[u].tolist(), weights[u].tolist()):
            a, b = (u, int(v)) if u < int(v) else (int(v), u)
            if (a, b) in seen:
                continue
            seen.add((a, b))
            G.add_edge(a, b, weight=int(w))

    if G.number_of_edges() == 0:
        return np.zeros(n, dtype=np.float64), np.zeros(n, dtype=np.int32)

    pr_dict = nx.pagerank(G, alpha=0.85, weight="weight", max_iter=100, tol=1e-6)
    core_dict = nx.core_number(G)

    pr = np.zeros(n, dtype=np.float64)
    core = np.zeros(n, dtype=np.int32)
    for i in range(n):
        pr[i] = float(pr_dict.get(i, 0.0))
        core[i] = int(core_dict.get(i, 0))
    return pr, core


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark()
    try:
        events_df = nc.read_curated_events(spark, args.input).cache()
        total = events_df.count()
        metric_date = nc.latest_metric_date(events_df)
        print(f"network_ic.py  events={total:,}  metric_date={metric_date}")

        print(f"[1/5] building actor graph (top {args.top_actors})")
        nodes, is_bot, persona, adj, weights_w, degree = build_actor_graph(
            events_df, args.top_actors, args.max_repo_actors, args.min_shared
        )
        n = len(nodes)
        n_edges = int(sum(len(a) for a in adj) // 2)
        print(f"  nodes={n}  edges={n_edges}  (directed-repr: "
              f"{sum(a.size for a in adj):,} entries)")
        if n == 0 or n_edges == 0:
            raise SystemExit("empty actor graph — nothing to do")

        # probabilities per edge
        probs = [edge_prob(w, args.p0, args.p_max) for w in weights_w]

        print(f"[2/5] sampling {args.mc_runs} live-edge subgraphs")
        rng = np.random.default_rng(args.seed)
        samples = sample_live_edge_graphs(adj, probs, args.mc_runs, rng)
        # quick sanity: avg size of live subgraph
        live_edges = np.asarray([sum(a.size for a in s) // 2 for s in samples])
        print(f"  live-edge mean={live_edges.mean():.0f}  "
              f"min={live_edges.min()}  max={live_edges.max()}")

        print("[3/5] centralities: degree / pagerank / coreness")
        pr, core = compute_pagerank_and_coreness(n, adj, weights_w)

        # candidate pool: drop isolated nodes
        pool = [i for i in range(n) if adj[i].size > 0]
        print(f"  candidate pool after dropping isolates: {len(pool)}")

        print(f"[4/5] CELF greedy up to k={args.k_max}")
        greedy_pick = celf_greedy(samples, n, args.k_max, pool)
        # quick debug print
        for rk, (v, gain, cum) in enumerate(greedy_pick, start=1):
            print(f"  seed#{rk}: {nodes[v]}  Δ={gain:.2f}  σ={cum:.2f}")

        print("[5/5] evaluating baseline strategies")

        def topk_by(score: np.ndarray, k: int) -> list[int]:
            # exclude isolated nodes for fair comparison
            idx_pool = np.asarray(pool, dtype=np.int64)
            order = idx_pool[np.argsort(-score[idx_pool])]
            return order[:k].tolist()

        random_rng = random.Random(args.seed)
        random_perm = pool[:]
        random_rng.shuffle(random_perm)

        greedy_order = [v for v, _, _ in greedy_pick]

        def seeds_for(strategy: str, k: int) -> list[int]:
            if strategy == "greedy":
                return greedy_order[:k]
            if strategy == "top_degree":
                return topk_by(degree.astype(np.float64), k)
            if strategy == "top_pagerank":
                return topk_by(pr, k)
            if strategy == "top_coreness":
                return topk_by(core.astype(np.float64), k)
            if strategy == "random":
                return random_perm[:k]
            raise ValueError(strategy)

        strategies = ["greedy", "top_degree", "top_pagerank", "top_coreness", "random"]
        k_grid = sorted({1, 2, 3, 5, 7, 10, 15, args.k_max})
        k_grid = [k for k in k_grid if 1 <= k <= args.k_max]

        reach_rows: list[tuple] = []
        for strat in strategies:
            for k in k_grid:
                seeds = seeds_for(strat, k)
                mu, sd = expected_reach(set(seeds), samples)
                seeds_str = ",".join(nodes[s] for s in seeds)
                if len(seeds_str) > 1000:
                    seeds_str = seeds_str[:997] + "..."
                reach_rows.append(
                    (metric_date, int(k), strat, seeds_str, float(mu), float(sd), int(args.mc_runs))
                )
                print(f"  {strat:>13s} k={k:>2}: reach={mu:.2f} (±{sd:.2f})")

        # ── seed_greedy rows (per-rank marginal-gain details)
        seed_rows: list[tuple] = []
        for rk, (v, gain, cum) in enumerate(greedy_pick, start=1):
            seed_rows.append(
                (
                    metric_date,
                    int(rk),
                    nodes[v],
                    "bot" if is_bot[v] else "human",
                    int(is_bot[v]),
                    float(gain),
                    float(cum),
                    int(degree[v]),
                    float(pr[v]),
                )
            )

        seed_schema = (
            "metric_date date, seed_rank int, actor_login string, persona_label string, "
            "is_bot int, marginal_gain double, cumulative_reach double, degree int, pagerank double"
        )
        reach_schema = (
            "metric_date date, k int, strategy string, seeds string, "
            "expected_reach double, reach_stddev double, sim_runs int"
        )
        seed_df = spark.createDataFrame(seed_rows, seed_schema)
        reach_df = spark.createDataFrame(reach_rows, reach_schema)

        nc.ensure_output_dir(args.output)
        out = args.output
        seed_df.write.mode("overwrite").parquet(str(Path(out) / "seed_greedy"))
        reach_df.write.mode("overwrite").parquet(str(Path(out) / "actor_ic_reach"))
        summary = {
            "seed_greedy": len(seed_rows),
            "actor_ic_reach": len(reach_rows),
            "nodes": n,
            "edges": n_edges,
            "mc_runs": args.mc_runs,
            "k_max": args.k_max,
            "greedy_best_reach": float(greedy_pick[-1][2]) if greedy_pick else 0.0,
        }
        print("\nSUMMARY:")
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
