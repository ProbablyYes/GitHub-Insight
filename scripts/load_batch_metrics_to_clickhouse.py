from __future__ import annotations

import argparse
import os
import sys


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from pathlib import Path

import pandas as pd

from jobs.common.clickhouse_client import insert_records


# ─────────────────────────────────────────────────────────────────────────────
# Table → parquet-folder mapping, grouped so we can load subsets
# ─────────────────────────────────────────────────────────────────────────────

CORE_TABLES: list[tuple[str, str]] = [
    ("batch_daily_metrics", "daily_metrics"),
    ("batch_activity_patterns", "activity_patterns"),
    ("batch_language_day_trend", "language_day_trend"),
    ("batch_top_users_day", "top_users_day"),
    ("batch_top_repos_day", "top_repos_day"),
    ("batch_event_type_day", "event_type_day"),
    ("batch_org_daily_metrics", "org_daily_metrics"),
    ("batch_org_rank_latest", "org_rank_latest"),
    ("batch_event_action_day", "event_action_day"),
    ("batch_repo_payload_size_day", "repo_payload_size_day"),
    ("batch_payload_bucket_day", "payload_bucket_day"),
    ("batch_user_segment_latest", "user_segment_latest"),
    ("batch_repo_rank_daily", "repo_rank_daily"),
    ("batch_repo_trend_forecast", "repo_trend_forecast"),
    ("batch_repo_burst_stability", "repo_burst_stability"),
    ("batch_developer_rhythm_heatmap", "developer_rhythm_heatmap"),
    ("batch_repo_hotness_components", "repo_hotness_components"),
    ("batch_event_complexity_day", "event_complexity_day"),
    ("batch_repo_health_latest", "repo_health_latest"),
    ("batch_offline_anomaly_alerts", "offline_anomaly_alerts"),
    ("batch_offline_decline_warnings", "offline_decline_warnings"),
    ("batch_repo_clusters", "repo_clusters"),
    ("batch_repo_rank_explain_latest", "repo_rank_explain_latest"),
    ("batch_repo_rank_score_day", "repo_rank_score_day"),
    ("batch_repo_rank_delta_explain_day", "repo_rank_delta_explain_day"),
    ("batch_repo_contributor_concentration_day", "repo_contributor_concentration_day"),
    ("batch_repo_top_actors_latest", "repo_top_actors_latest"),
    ("batch_repo_event_type_share_day", "repo_event_type_share_day"),
    ("batch_repo_event_mix_shift_day", "repo_event_mix_shift_day"),
    ("batch_concentration_day", "concentration_day"),
    ("batch_ecosystem_changepoints", "ecosystem_changepoints"),
    ("batch_actor_cohort_day", "actor_cohort_day"),
    ("batch_event_type_share_shift_day", "event_type_share_shift_day"),
    ("batch_offline_insights_latest", "offline_insights_latest"),
    # L4–L5 deep analytics (non-network)
    ("batch_repo_cluster_profile", "repo_cluster_profile"),
    ("batch_hot_vs_cold_attribution", "hot_vs_cold_attribution"),
    ("batch_repo_dna", "repo_dna"),
    ("batch_repo_dna_outliers", "repo_dna_outliers"),
    ("batch_actor_persona", "actor_persona"),
    ("batch_actor_persona_centroid", "actor_persona_centroid"),
    ("batch_actor_persona_bic", "actor_persona_bic"),
    ("batch_actor_persona_bot_validation", "actor_persona_bot_validation"),
    ("batch_actor_persona_transition", "actor_persona_transition"),
    ("batch_repo_watcher_persona_lift", "repo_watcher_persona_lift"),
    ("batch_repo_watcher_profile", "repo_watcher_profile"),
]

# Network-page tables produced by spark_job.py (legacy Jaccard + community + rules)
NETWORK_LEGACY_TABLES: list[tuple[str, str]] = [
    ("batch_repo_similarity_edges", "repo_similarity_edges"),
    ("batch_repo_community", "repo_community"),
    ("batch_repo_community_profile", "repo_community_profile"),
    ("batch_repo_association_rules", "repo_association_rules"),
]

# People-page depth tables (produced by people_depth.py inside spark_job.py)
PEOPLE_TABLES: list[tuple[str, str]] = [
    ("batch_actor_collab_edge", "actor_collab_edge"),
    ("batch_actor_graph_metrics", "actor_graph_metrics"),
    ("batch_actor_burst_stability", "actor_burst_stability"),
    ("batch_actor_retention_curve", "actor_retention_curve"),
    ("batch_actor_churn_risk", "actor_churn_risk"),
    ("batch_actor_hotness", "actor_hotness"),
    ("batch_repo_bus_factor", "repo_bus_factor"),
    ("batch_actor_bot_supervised", "actor_bot_supervised"),
    ("batch_bot_feature_importance", "bot_feature_importance"),
    ("batch_bot_classifier_meta", "bot_classifier_meta"),
    ("batch_actor_ring", "actor_ring"),
]

# Network-page depth tables (produced by network_depth.py / network_ic.py)
NETWORK_DEPTH_TABLES: list[tuple[str, str]] = [
    # Phase 1
    ("batch_repo_embedding", "repo_embedding"),
    ("batch_repo_als_neighbor", "repo_als_neighbor"),
    ("batch_repo_layer_edge", "repo_layer_edge"),
    ("batch_repo_layer_community", "repo_layer_community"),
    ("batch_actor_coreness", "actor_coreness"),
    ("batch_repo_coreness", "repo_coreness"),
    ("batch_repo_metapath_sim", "repo_metapath_sim"),
    ("batch_repo_archetype", "repo_archetype"),
    ("batch_repo_archetype_centroid", "repo_archetype_centroid"),
    # Phase 3
    ("batch_repo_community_weekly", "repo_community_weekly"),
    ("batch_community_lineage", "community_lineage"),
    # Phase 4
    ("batch_actor_ic_reach", "actor_ic_reach"),
    ("batch_seed_greedy", "seed_greedy"),
]


GROUPS: dict[str, list[tuple[str, str]]] = {
    "core": CORE_TABLES,
    "people": PEOPLE_TABLES,
    "network_legacy": NETWORK_LEGACY_TABLES,
    "network": NETWORK_DEPTH_TABLES,
    "all": CORE_TABLES + NETWORK_LEGACY_TABLES + PEOPLE_TABLES + NETWORK_DEPTH_TABLES,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load parquet outputs into ClickHouse.")
    parser.add_argument("--input", required=True, help="Directory containing parquet outputs.")
    parser.add_argument(
        "--only",
        choices=sorted(GROUPS.keys()),
        default="all",
        help="Subset of tables to load. Default = all. Use 'network' to load only "
        "Network-page depth tables (fastest iteration).",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Silently skip tables whose parquet directory does not exist. "
        "Recommended when loading a subset and upstream job only emitted that subset.",
    )
    return parser


def load_parquet_records(path: Path):
    if not path.exists():
        return None
    frame = pd.read_parquet(path)
    if frame.empty:
        return []
    return frame.to_dict(orient="records")


def main() -> None:
    args = build_parser().parse_args()
    input_dir = Path(args.input)
    tables = GROUPS[args.only]

    loaded: list[tuple[str, int]] = []
    skipped: list[str] = []
    for table_name, folder in tables:
        records = load_parquet_records(input_dir / folder)
        if records is None:
            if args.skip_missing or args.only != "all":
                skipped.append(folder)
                continue
            records = []
        insert_records(table_name, records, truncate=True)
        loaded.append((table_name, len(records)))

    print(f"=== Loaded {len(loaded)} tables (group={args.only}) ===")
    for name, n in loaded:
        # ASCII-only bullet so Windows gbk consoles don't crash on U+2022
        print(f"  - {name}: {n} rows")
    if skipped:
        print(f"  (skipped {len(skipped)} missing dirs: {', '.join(skipped[:10])}"
              f"{'...' if len(skipped) > 10 else ''})")


if __name__ == "__main__":
    main()
