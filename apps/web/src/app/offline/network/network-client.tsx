/* eslint-disable react/no-unescaped-entities -- narrative prose */
"use client";

import { useMemo, useState } from "react";

import { EntityLink } from "@/components/entity";
import { OfflineSubnav } from "@/components/offline-subnav";
import {
  PixelBadge,
  PixelKpi,
  PixelSearchTable,
  PixelSection,
  type PixelSearchColumn,
} from "@/components/pixel";
import { PixelPageShell } from "@/components/pixel-shell";
import type {
  ActorCorenessPoint,
  ActorIcReachPoint,
  CommunityLineagePoint,
  CorenessHistogramBucket,
  RepoAlsNeighborPoint,
  RepoArchetypeCentroidPoint,
  RepoArchetypePoint,
  RepoAssociationRulePoint,
  RepoCommunityPoint,
  RepoCommunityProfilePoint,
  RepoCommunityWeeklyPoint,
  RepoCorenessPoint,
  RepoEmbeddingSummaryPoint,
  RepoLayerCommunityPoint,
  RepoLayerEdgePoint,
  RepoMetapathSimPoint,
  RepoSimilarityEdgePoint,
  SeedGreedyPoint,
} from "@/lib/dashboard";

import {
  ARCHETYPE_COLORS,
  ARCHETYPE_DESCRIPTION,
  ARCHETYPE_LABEL,
  ArchetypeScatter,
} from "./archetype-scatter";
import { CommunitySankey } from "./community-sankey";
import { CorenessHistogram } from "./coreness-histogram";
import { IcReachChart } from "./ic-reach-chart";
import { LayerMatrix } from "./layer-matrix";
import { RulesParetoScatter } from "./rules-pareto";
import { SimilarityGraph } from "./similarity-graph";

function formatAntecedent(text: string): string {
  return text.replaceAll(",", ", ");
}

/**
 * Small inline callout that translates the raw numbers visible on the chart
 * above it into plain language ("this number = X, which means Y"). The goal is
 * to answer the reader's implicit question: "OK but what does that number
 * actually mean for the ecosystem question we started with?"
 */
function DataReading({
  children,
  tone = "positive",
  title = "Reading the numbers",
}: {
  children: React.ReactNode;
  tone?: "positive" | "info" | "change" | "magenta" | "purple" | "danger";
  title?: string;
}) {
  const color =
    {
      positive: "var(--accent-positive)",
      info: "var(--accent-info)",
      change: "var(--accent-change)",
      magenta: "var(--accent-magenta)",
      purple: "var(--accent-purple)",
      danger: "var(--accent-danger)",
    }[tone] ?? "var(--accent-positive)";
  return (
    <div
      style={{
        marginTop: 12,
        padding: "10px 12px",
        borderLeft: `3px solid ${color}`,
        background: "var(--surface-elevated, rgba(255,255,255,0.02))",
        fontSize: 11,
        lineHeight: 1.6,
        color: "var(--fg)",
      }}
    >
      <div
        style={{
          color,
          fontSize: 10,
          letterSpacing: 2,
          fontWeight: 700,
          marginBottom: 4,
        }}
      >
        ⇒ {title.toUpperCase()}
      </div>
      {children}
    </div>
  );
}

/**
 * Client-side modularity Q (Newman 2006).
 *
 *   Q = (1 / 2m) · Σ_ij [A_ij —(k_i · k_j) / (2m)] · δ(c_i, c_j)
 *
 * where A_ij is the weighted adjacency, k_i the weighted degree, m the total
 * edge weight, and δ=1 iff i and j are in the same community. Ranges roughly
 * in [—.5, 1]. Q > 0.3 —"meaningful" community structure (Newman rule).
 *
 * Implementation: we use Jaccard as the edge weight (graph is undirected).
 */
function computeModularity(
  edges: { src: string; dst: string; weight: number }[],
  nodeCommunity: Record<string, string | undefined>
): number {
  if (edges.length === 0) return 0;
  const degree: Record<string, number> = {};
  let twoM = 0;
  for (const e of edges) {
    degree[e.src] = (degree[e.src] ?? 0) + e.weight;
    degree[e.dst] = (degree[e.dst] ?? 0) + e.weight;
    twoM += 2 * e.weight;
  }
  if (twoM === 0) return 0;
  let q = 0;
  for (const e of edges) {
    const ci = nodeCommunity[e.src];
    const cj = nodeCommunity[e.dst];
    if (!ci || !cj || ci !== cj) continue;
    const expected = (degree[e.src]! * degree[e.dst]!) / twoM;
    q += (e.weight - expected) * 2;
  }
  return q / twoM;
}

/**
 * Client-side "bridge score" for each node. A bridge is a repo whose edges
 * mostly cross community boundaries —if you drop it, two communities would
 * drift apart. Formula:
 *
 *   bridge(v) = (cross-community edge weight) / (total edge weight of v)
 *
 * We filter to nodes with —3 distinct community neighbours so lone outliers
 * (one heavy edge to another community) don't dominate the list.
 */
function computeBridgeScores(
  edges: { src: string; dst: string; weight: number }[],
  nodeCommunity: Record<string, string | undefined>
): Array<{ node: string; community: string; bridgeScore: number; crossCommunities: number; totalWeight: number }> {
  const buckets: Record<string, { total: number; cross: number; communities: Set<string>; own: string | undefined }> = {};
  const addEdge = (a: string, b: string, w: number) => {
    const ca = nodeCommunity[a];
    const cb = nodeCommunity[b];
    if (!buckets[a]) buckets[a] = { total: 0, cross: 0, communities: new Set(), own: ca };
    buckets[a].total += w;
    if (cb) buckets[a].communities.add(cb);
    if (ca && cb && ca !== cb) buckets[a].cross += w;
  };
  for (const e of edges) {
    addEdge(e.src, e.dst, e.weight);
    addEdge(e.dst, e.src, e.weight);
  }
  return Object.entries(buckets)
    .map(([node, b]) => ({
      node,
      community: b.own ?? "_",
      bridgeScore: b.total > 0 ? b.cross / b.total : 0,
      crossCommunities: b.communities.size - (b.own ? 1 : 0),
      totalWeight: b.total,
    }))
    .filter((r) => r.crossCommunities >= 2 && r.totalWeight > 0)
    .sort((a, b) => b.bridgeScore - a.bridgeScore || b.crossCommunities - a.crossCommunities);
}

type Props = {
  edges: RepoSimilarityEdgePoint[];
  communityRows: RepoCommunityPoint[];
  rules: RepoAssociationRulePoint[];
  communityProfiles: RepoCommunityProfilePoint[];
  layerEdges: RepoLayerEdgePoint[];
  layerCommunities: RepoLayerCommunityPoint[];
  actorCoreness: ActorCorenessPoint[];
  actorCorenessHist: CorenessHistogramBucket[];
  repoCoreness: RepoCorenessPoint[];
  archetypePoints: RepoArchetypePoint[];
  archetypeCentroids: RepoArchetypeCentroidPoint[];
  metapathSim: RepoMetapathSimPoint[];
  alsNeighbors: RepoAlsNeighborPoint[];
  embeddingSummary: RepoEmbeddingSummaryPoint | null;
  weekly: RepoCommunityWeeklyPoint[];
  lineage: CommunityLineagePoint[];
  icReach: ActorIcReachPoint[];
  seedGreedy: SeedGreedyPoint[];
};

export function NetworkClient({
  edges,
  communityRows,
  rules,
  communityProfiles,
  layerEdges,
  layerCommunities,
  actorCoreness,
  actorCorenessHist,
  repoCoreness,
  archetypePoints,
  archetypeCentroids,
  metapathSim,
  alsNeighbors,
  embeddingSummary,
  weekly,
  lineage,
  icReach,
  seedGreedy,
}: Props) {
  const nodeInfo: Record<string, { id: string; community?: string; rankNo?: number }> = {};
  for (const row of communityRows) {
    nodeInfo[row.repoName] = {
      id: row.repoName,
      community: row.communityId,
      rankNo: row.rankNo,
    };
  }
  for (const edge of edges) {
    if (!nodeInfo[edge.srcRepo]) nodeInfo[edge.srcRepo] = { id: edge.srcRepo };
    if (!nodeInfo[edge.dstRepo]) nodeInfo[edge.dstRepo] = { id: edge.dstRepo };
  }

  const edgesForGraph = edges.slice(0, 220).map((edge) => ({
    src: edge.srcRepo,
    dst: edge.dstRepo,
    jaccard: edge.jaccard,
    sharedActors: edge.sharedActors,
  }));

  const communityGroups = communityRows.reduce<
    Record<string, { size: number; sample: string; members: string[] }>
  >((acc, row) => {
    if (!acc[row.communityId]) {
      acc[row.communityId] = { size: row.communitySize, sample: row.sampleMembers, members: [] };
    }
    acc[row.communityId]!.members.push(row.repoName);
    return acc;
  }, {});
  const communityList = Object.entries(communityGroups)
    .map(([cid, info]) => ({ cid, ...info }))
    .sort((a, b) => b.size - a.size);

  const topRule = rules[0];
  const topEdge = edges[0];

  // Modularity Q + bridge ranking (driver-less —runs entirely from ClickHouse rows).
  const allEdgesWeighted = edges.map((e) => ({ src: e.srcRepo, dst: e.dstRepo, weight: e.jaccard }));
  const nodeCommunity: Record<string, string | undefined> = {};
  Object.entries(nodeInfo).forEach(([k, v]) => {
    nodeCommunity[k] = v.community;
  });
  const modularityQ = computeModularity(allEdgesWeighted, nodeCommunity);
  const bridgeRows = computeBridgeScores(allEdgesWeighted, nodeCommunity).slice(0, 20);
  const biggestCommunity = communityList[0];

  // ————————
  // Derived stats for Phase-1 / Phase-3 sections
  // ————————

  // §6 layers
  const layersPresent = useMemo(
    () => Array.from(new Set([...layerEdges.map((l) => l.layer), ...layerCommunities.map((l) => l.layer)])),
    [layerEdges, layerCommunities],
  );
  const layerEdgeCount = layerEdges.length;

  // §7 coreness
  const maxActorK = actorCorenessHist.length ? Math.max(...actorCorenessHist.map((b) => b.coreness)) : 0;
  const topKBucket = actorCorenessHist.find((b) => b.coreness === maxActorK);
  const topKBotShare = topKBucket && topKBucket.actors > 0 ? topKBucket.bots / topKBucket.actors : 0;
  const totalActorsInGraph = actorCorenessHist.reduce((a, b) => a + b.actors, 0);
  const nonZeroActors = actorCorenessHist.filter((b) => b.coreness > 0).reduce((a, b) => a + b.actors, 0);
  const actorMedianK = (() => {
    if (!totalActorsInGraph) return 0;
    const sorted = [...actorCorenessHist].sort((a, b) => a.coreness - b.coreness);
    let cum = 0;
    for (const b of sorted) {
      cum += b.actors;
      if (cum >= totalActorsInGraph / 2) return b.coreness;
    }
    return 0;
  })();
  const repoCorenessBuckets = useMemo(() => {
    const m = new Map<number, number>();
    for (const r of repoCoreness) m.set(r.coreness, (m.get(r.coreness) ?? 0) + 1);
    return [...m.entries()].map(([coreness, count]) => ({ coreness, count })).sort((a, b) => a.coreness - b.coreness);
  }, [repoCoreness]);
  const maxRepoK = repoCorenessBuckets.length ? Math.max(...repoCorenessBuckets.map((b) => b.coreness)) : 0;
  const innerCoreRepos = repoCoreness.filter((r) => r.coreness === maxRepoK);

  // §8 archetypes
  const archetypeByKey = useMemo(() => {
    const m: Record<string, RepoArchetypePoint[]> = {};
    for (const p of archetypePoints) (m[p.archetypeRule] ||= []).push(p);
    return m;
  }, [archetypePoints]);
  const archetypeStats = useMemo(() => {
    // Centroid rows are (archetype_rule, archetype_gmm_id) pairs — aggregate up to per-rule.
    const byRule = new Map<string, RepoArchetypeCentroidPoint[]>();
    for (const c of archetypeCentroids) {
      if (!byRule.has(c.archetypeRule)) byRule.set(c.archetypeRule, []);
      byRule.get(c.archetypeRule)!.push(c);
    }
    const list = [...byRule.entries()].map(([rule, group]) => {
      const members = group.reduce((a, b) => a + b.members, 0);
      const weighted = (fn: (x: RepoArchetypeCentroidPoint) => number) =>
        members > 0 ? group.reduce((a, b) => a + fn(b) * b.members, 0) / members : 0;
      return {
        key: rule,
        label: ARCHETYPE_LABEL[rule] ?? rule,
        color: ARCHETYPE_COLORS[rule] ?? "var(--fg)",
        description: ARCHETYPE_DESCRIPTION[rule] ?? "",
        members,
        avgWatch: weighted((x) => x.avgWatchShare),
        avgPrPush: weighted((x) => x.avgPrPushRatio),
        avgBot: weighted((x) => x.avgBotRatio),
        avgActiveDays: weighted((x) => x.avgActiveDays),
        avgRankScore: weighted((x) => x.avgRankScore),
        ruleGmmOverlap: weighted((x) => x.ruleGmmOverlap),
        sampleRepos: group[0]?.sampleRepos ?? "",
        gmmClusters: group.length,
      };
    });
    return list.sort((a, b) => b.members - a.members);
  }, [archetypeCentroids]);
  const archetypeScatterPoints = useMemo(
    () =>
      archetypePoints.map((p) => ({
        repoName: p.repoName,
        archetype: p.archetypeRule,
        pc1: p.pcaX,
        pc2: p.pcaY,
        starsLog: Math.log1p(p.totalEvents),
        authorRatio: p.prPushRatio,
        retention: p.activeDays / 30,
        issueRatio: p.botRatio,
      })),
    [archetypePoints],
  );

  // §1 ALS / meta-path toggle
  type GraphSource = "jaccard" | "als" | "RAR" | "ROR" | "RLR";
  const [graphSource, setGraphSource] = useState<GraphSource>("jaccard");
  const alsEdgesForGraph = useMemo(
    () =>
      alsNeighbors.slice(0, 220).map((n) => ({
        src: n.srcRepo,
        dst: n.dstRepo,
        jaccard: n.cosine,
        sharedActors: n.rankNo,
      })),
    [alsNeighbors],
  );
  const metaEdgesForGraph = useMemo(() => {
    if (graphSource !== "RAR" && graphSource !== "ROR" && graphSource !== "RLR") return [];
    return metapathSim
      .filter((m) => m.pathType === graphSource)
      .slice(0, 220)
      .map((m) => ({ src: m.srcRepo, dst: m.dstRepo, jaccard: m.sim, sharedActors: m.sharedActors }));
  }, [metapathSim, graphSource]);
  const currentGraphEdges =
    graphSource === "jaccard"
      ? edgesForGraph
      : graphSource === "als"
      ? alsEdgesForGraph
      : metaEdgesForGraph;
  const jaccardMissRate = embeddingSummary?.jaccardMissShare ?? 0;
  const alsAvgCos = embeddingSummary?.avgCosineTop5 ?? 0;

  // §9 temporal evolution
  const lineageEventCounts = useMemo(() => {
    const m: Record<string, number> = { birth: 0, death: 0, continue: 0, merge_or_split: 0, reform: 0 };
    for (const l of lineage) m[l.eventType] = (m[l.eventType] ?? 0) + 1;
    return m;
  }, [lineage]);
  const weeklyCommunityCounts = useMemo(() => {
    const m = new Map<number, Set<string>>();
    for (const w of weekly) {
      if (!m.has(w.weekIdx)) m.set(w.weekIdx, new Set());
      m.get(w.weekIdx)!.add(w.communityId);
    }
    return [...m.entries()].map(([weekIdx, s]) => ({ weekIdx, communities: s.size })).sort((a, b) => a.weekIdx - b.weekIdx);
  }, [weekly]);
  const continueRate =
    (lineageEventCounts.continue ?? 0) +
    (lineageEventCounts.merge_or_split ?? 0) +
    (lineageEventCounts.reform ?? 0);
  const churnRate = (lineageEventCounts.birth ?? 0) + (lineageEventCounts.death ?? 0);
  const survivalShare = continueRate + churnRate > 0 ? continueRate / (continueRate + churnRate) : 0;

  // §10 influence maximization
  const icReachSummary = useMemo(() => {
    if (icReach.length === 0) return null;
    const kMax = Math.max(...icReach.map((r) => r.k));
    const at = (strategy: string, k: number) =>
      icReach.find((r) => r.strategy === strategy && r.k === k);
    const g = at("greedy", kMax);
    const d = at("top_degree", kMax);
    const rnd = at("random", kMax);
    if (!g) return null;
    const liftVsDegree = d && d.expectedReach > 0 ? g.expectedReach / d.expectedReach - 1 : 0;
    const liftVsRandom = rnd && rnd.expectedReach > 0 ? g.expectedReach / rnd.expectedReach - 1 : 0;
    return {
      kMax,
      greedyReach: g.expectedReach,
      greedyStd: g.reachStddev,
      liftVsDegree,
      liftVsRandom,
      simRuns: g.simRuns,
    };
  }, [icReach]);

  const ruleColumns: PixelSearchColumn<RepoAssociationRulePoint>[] = [
    {
      key: "antecedent",
      header: "antecedent",
      align: "left",
      sortValue: (r) => r.antecedent,
      searchValue: (r) => `${r.antecedent} ${r.consequent}`,
      render: (r) => <span style={{ color: "var(--muted-strong)" }}>{formatAntecedent(r.antecedent)}</span>,
    },
    {
      key: "consequent",
      header: "consequent",
      align: "left",
      sortValue: (r) => r.consequent,
      render: (r) => <EntityLink type="repo" id={r.consequent} />,
    },
    { key: "support", header: "support", align: "right", sortValue: (r) => r.support, render: (r) => `${(r.support * 100).toFixed(2)}%` },
    { key: "confidence", header: "conf", align: "right", sortValue: (r) => r.confidence, render: (r) => `${(r.confidence * 100).toFixed(0)}%` },
    {
      key: "lift",
      header: "lift",
      align: "right",
      sortValue: (r) => r.lift,
      render: (r) => (
        <PixelBadge tone={r.lift >= 3 ? "positive" : r.lift >= 1.5 ? "change" : "muted"} size="sm">
          {r.lift.toFixed(2)}×
        </PixelBadge>
      ),
    },
    {
      key: "isFrontier",
      header: "pareto",
      align: "center",
      sortValue: (r) => r.isFrontier,
      render: (r) =>
        r.isFrontier === 1 ? (
          <PixelBadge tone="magenta" size="sm">
            * frontier
          </PixelBadge>
        ) : (
          <span style={{ color: "var(--muted)" }}>·</span>
        ),
    },
  ];

  const communityProfileTop = communityProfiles.slice(0, 6);
  const paretoRules = rules.filter((r) => r.isFrontier === 1);

  const edgeColumns: PixelSearchColumn<RepoSimilarityEdgePoint>[] = [
    {
      key: "srcRepo",
      header: "src",
      align: "left",
      sortValue: (r) => r.srcRepo,
      searchValue: (r) => `${r.srcRepo} ${r.dstRepo}`,
      render: (r) => <EntityLink type="repo" id={r.srcRepo} />,
    },
    {
      key: "dstRepo",
      header: "dst",
      align: "left",
      sortValue: (r) => r.dstRepo,
      render: (r) => <EntityLink type="repo" id={r.dstRepo} />,
    },
    { key: "sharedActors", header: "shared", align: "right", sortValue: (r) => r.sharedActors },
    { key: "jaccard", header: "Jaccard", align: "right", sortValue: (r) => r.jaccard, render: (r) => r.jaccard.toFixed(3) },
  ];

  return (
    <PixelPageShell
      title="L5 Network"
      subtitle="Multi-layer co-occurrence graph of the GitHub ecosystem. We combine Jaccard / ALS-embedding similarity, k-core decomposition, repo archetypes (rules + GMM), week-over-week community lineage, and Independent-Cascade influence maximization (CELF greedy) to expose both static structure AND temporal dynamics — then turn it into an actionable &ldquo;who should we seed?&rdquo; recommendation."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "Network" },
      ]}
      tldr={
        topEdge && topRule ? (
          <>
            <strong>{edges.length}</strong> Jaccard edges · <strong>{Object.keys(nodeInfo).length}</strong> repos ·{" "}
            <strong>{communityList.length}</strong> communities · Q = <strong>{modularityQ.toFixed(3)}</strong>.{" "}
            ALS embedding (rank {embeddingSummary?.rankDim ?? "?"}) finds an additional{" "}
            <strong>{Math.round(jaccardMissRate * 100)}%</strong> of neighbours that Jaccard misses.{" "}
            {topKBucket ? (
              <>
                Inner core at k = <strong>{maxActorK}</strong> is{" "}
                <strong style={{ color: topKBotShare > 0.5 ? "var(--accent-danger)" : "var(--accent-positive)" }}>
                  {Math.round(topKBotShare * 100)}% bot
                </strong>
                .{" "}
              </>
            ) : null}
            {archetypeStats.length ? (
              <>
                {archetypeStats.length} repo archetypes —biggest: <strong>{archetypeStats[0]!.label}</strong> (
                {archetypeStats[0]!.members} repos).{" "}
              </>
            ) : null}
            {weeklyCommunityCounts.length ? (
              <>
                {weeklyCommunityCounts.length}-week evolution: {(survivalShare * 100).toFixed(0)}% survival,{" "}
                {lineageEventCounts.birth ?? 0} births · {lineageEventCounts.death ?? 0} deaths ·{" "}
                {lineageEventCounts.merge_or_split ?? 0} merge/split.{" "}
              </>
            ) : null}
            {icReachSummary ? (
              <>
                CELF greedy @ k={icReachSummary.kMax} reaches{" "}
                <strong style={{ color: "var(--accent-success)" }}>
                  {icReachSummary.greedyReach.toFixed(1)}
                </strong>{" "}
                actors —{" "}
                <strong>+{(icReachSummary.liftVsDegree * 100).toFixed(0)}%</strong> over top-degree,{" "}
                <strong>+{(icReachSummary.liftVsRandom * 100).toFixed(0)}%</strong> over random.
              </>
            ) : null}
          </>
        ) : (
          "No network data yet."
        )
      }
    >
      <OfflineSubnav />

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: "var(--pixel-space-4)",
          marginBottom: "var(--pixel-space-4)",
        }}
      >
        <PixelKpi label="Jaccard edges" value={edges.length} tone="positive" hint={`${Object.keys(nodeInfo).length} repos`} />
        <PixelKpi
          label="Modularity Q"
          value={modularityQ.toFixed(3)}
          tone={modularityQ >= 0.3 ? "positive" : modularityQ >= 0.15 ? "change" : "danger"}
          hint={modularityQ >= 0.3 ? "strong (Newman >= 0.3)" : modularityQ >= 0.15 ? "weak but real" : "random"}
        />
        <PixelKpi
          label="Communities"
          value={communityList.length}
          tone="magenta"
          hint={biggestCommunity ? `biggest ${biggestCommunity.size}` : undefined}
        />
        <PixelKpi
          label="ALS vs Jaccard miss"
          value={`${Math.round(jaccardMissRate * 100)}%`}
          tone={jaccardMissRate > 0.4 ? "magenta" : "info"}
          hint={`avg cos ${alsAvgCos.toFixed(2)} · rank ${embeddingSummary?.rankDim ?? "?"}`}
        />
        <PixelKpi
          label={`Inner core k=${maxActorK}`}
          value={topKBucket ? `${topKBucket.actors} actors` : "-"}
          tone={topKBotShare > 0.5 ? "danger" : "info"}
          hint={topKBucket ? `${Math.round(topKBotShare * 100)}% bots · median k=${actorMedianK}` : undefined}
        />
        <PixelKpi
          label="Repo archetypes"
          value={archetypeStats.length}
          tone="change"
          hint={archetypeStats[0] ? `top: ${archetypeStats[0].label.split(" ")[0]} (${archetypeStats[0].members})` : undefined}
        />
        <PixelKpi
          label="Bridge repos"
          value={bridgeRows.length}
          tone={bridgeRows.length > 0 ? "change" : "muted"}
          hint="span >= 2 communities"
        />
        <PixelKpi
          label="Community survival"
          value={survivalShare > 0 ? `${(survivalShare * 100).toFixed(0)}%` : "-"}
          tone={survivalShare >= 0.5 ? "positive" : "danger"}
          hint={`${lineageEventCounts.birth ?? 0} births / ${lineageEventCounts.death ?? 0} deaths`}
        />
        <PixelKpi
          label="Rules on Pareto"
          value={paretoRules.length}
          tone="magenta"
          hint={`of ${rules.length} FPGrowth rules`}
        />
        {icReachSummary ? (
          <PixelKpi
            label={`CELF reach @ k=${icReachSummary.kMax}`}
            value={icReachSummary.greedyReach.toFixed(1)}
            tone="positive"
            hint={`+${(icReachSummary.liftVsDegree * 100).toFixed(0)}% vs top-deg · ${icReachSummary.simRuns} MC`}
          />
        ) : null}
      </div>

      {/* Anchor nav -- single URL, multi-section */}
      <nav
        aria-label="Network sections"
        style={{
          position: "sticky",
          top: 0,
          zIndex: 5,
          display: "flex",
          flexWrap: "wrap",
          gap: 8,
          alignItems: "center",
          padding: "8px 10px",
          marginBottom: "var(--pixel-space-4)",
          background: "var(--bg)",
          borderTop: "2px solid var(--pixel-border)",
          borderBottom: "2px solid var(--pixel-border)",
          fontSize: 11,
          letterSpacing: 1,
        }}
      >
        <span style={{ color: "var(--muted)", marginRight: 6 }}>JUMP &rarr;</span>
        {[
          { id: "net-graph", label: "§1 Graph", color: "var(--accent-positive)" },
          { id: "net-communities", label: "§2 Communities", color: "var(--accent-info)" },
          { id: "net-inspector", label: "§3 Inspector", color: "var(--accent-purple)" },
          { id: "net-bridges", label: "§4 Bridges", color: "var(--accent-change)" },
          { id: "net-rules", label: "§5 Rules", color: "var(--accent-magenta)" },
          { id: "net-layers", label: "§6 Layers", color: "var(--accent-info)" },
          { id: "net-core", label: "§7 Core", color: "var(--accent-change)" },
          { id: "net-archetype", label: "§8 Archetype", color: "var(--accent-magenta)" },
          { id: "net-evolution", label: "§9 Evolution", color: "var(--accent-positive)" },
          { id: "net-influence", label: "§10 Influence", color: "var(--accent-success)" },
        ].map((s) => (
          <a
            key={s.id}
            href={`#${s.id}`}
            className="nes-btn"
            style={{ padding: "4px 10px", fontSize: 10, textDecoration: "none", color: s.color, display: "inline-block" }}
          >
            {s.label}
          </a>
        ))}
      </nav>

      {/* ————————
          § 1 · Graph
          ———————— */}
      <h2
        id="net-graph"
        style={{
          fontSize: 14,
          color: "var(--accent-positive)",
          margin: "var(--pixel-space-4) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 1 · GRAPH —the shape of the repo ecosystem
      </h2>

      <PixelSection
        title="Similarity graph · 3 views (Jaccard / ALS / meta-path)"
        tone="positive"
        headline={
          <>
            Nodes = <strong>{Object.keys(nodeInfo).length}</strong> repos · colour = LPA community. Modularity Q ={" "}
            <strong style={{ color: modularityQ >= 0.3 ? "var(--accent-positive)" : "var(--accent-change)" }}>
              {modularityQ.toFixed(3)}
            </strong>{" "}
            {modularityQ >= 0.3
              ? "—strong, clearly separable communities."
              : modularityQ >= 0.15
              ? "—weak separation; communities overlap."
              : "—essentially random at this scale, but ALS + meta-paths recover finer structure below."}
          </>
        }
        source="batch_repo_similarity_edges · batch_repo_als_neighbor · batch_repo_metapath_sim"
        techBadge="Spark · Jaccard + ALS(rank=16) + meta-paths (R-A-R/R-O-R/R-L-R) · weighted LPA"
        howToRead={
          <>
            <strong>Jaccard</strong> = set-overlap of raw actor lists (|A∩B|/|A∪B|).{" "}
            <strong>ALS</strong> = learned rank-{embeddingSummary?.rankDim ?? 16} latent factors, then cosine of
            repo embeddings —this uncovers indirect similarity (two repos that never share an actor but have similar
            audiences).{" "}
            <strong>Meta-path</strong> similarity walks actor —repo —actor —repo graphs: R-A-R (shared actors),
            R-O-R (same owner), R-L-R (same language).
          </>
        }
        findings={
          <>
            <p style={{ margin: 0 }}>
              <strong>Three lenses:</strong> Jaccard keeps only{" "}
              <strong>{edges.length}</strong> direct ties. ALS finds{" "}
              <strong>{alsNeighbors.length}</strong> neighbour pairs —of which{" "}
              <strong style={{ color: "var(--accent-magenta)" }}>{Math.round(jaccardMissRate * 100)}%</strong>{" "}
              are brand-new relationships Jaccard missed (avg cosine {alsAvgCos.toFixed(2)}). Meta-paths split the
              neighbourhood into {metapathSim.filter((m) => m.pathType === "RAR").length} R-A-R +{" "}
              {metapathSim.filter((m) => m.pathType === "ROR").length} R-O-R +{" "}
              {metapathSim.filter((m) => m.pathType === "RLR").length} R-L-R edges.
            </p>
            <p style={{ margin: "6px 0 0" }}>
              <strong>Why this matters:</strong> a recommender built only on Jaccard is cold-start blind —ALS latent
              factors fill the gap. Meta-paths let you choose <em>why</em> two repos are close (same team, same tech,
              same audience).
            </p>
          </>
        }
      >
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 10, fontSize: 11 }}>
          {[
            { k: "jaccard", label: `Jaccard (${edges.length})`, color: "var(--accent-positive)" },
            { k: "als", label: `ALS cosine (${alsNeighbors.length})`, color: "var(--accent-info)" },
            { k: "RAR", label: `meta R-A-R`, color: "var(--accent-magenta)" },
            { k: "ROR", label: `meta R-O-R`, color: "var(--accent-change)" },
            { k: "RLR", label: `meta R-L-R`, color: "var(--accent-purple)" },
          ].map((opt) => (
            <button
              key={opt.k}
              type="button"
              onClick={() => setGraphSource(opt.k as GraphSource)}
              className="nes-btn"
              style={{
                padding: "3px 10px",
                fontSize: 10,
                color: graphSource === opt.k ? "var(--bg)" : opt.color,
                background: graphSource === opt.k ? opt.color : "transparent",
                borderColor: opt.color,
                cursor: "pointer",
              }}
            >
              {opt.label}
            </button>
          ))}
          <span style={{ color: "var(--muted)", marginLeft: 8, alignSelf: "center" }}>
            showing {currentGraphEdges.length} edges · up to 220 drawn
          </span>
        </div>
        {currentGraphEdges.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No edges for this view.</p>
        ) : (
          <SimilarityGraph edges={currentGraphEdges} nodeInfo={nodeInfo} />
        )}
        <DataReading tone="positive" title="what the three counts mean">
          <p style={{ margin: 0 }}>
            <strong>{edges.length}</strong> Jaccard edges are &ldquo;<em>we&apos;ve literally seen
            the same people on both repos</em>&rdquo; —strong, but you need co-occurrence to
            observe it. <strong>{alsNeighbors.length}</strong> ALS pairs add &ldquo;repos that
            feel alike to the matrix-factoriser even if they never shared an actor&rdquo;; the{" "}
            <strong style={{ color: "var(--accent-magenta)" }}>
              {Math.round(jaccardMissRate * 100)}%
            </strong>{" "}
            of ALS pairs that Jaccard missed are exactly the cold-start recommendations a
            content-based system couldn&apos;t make. Meta-paths split the remaining signal by{" "}
            <em>reason</em>: R-A-R = same audience, R-O-R = same owner/org, R-L-R = same
            programming language.
          </p>
          <p style={{ margin: "6px 0 0" }}>
            <strong>Modularity Q = {modularityQ.toFixed(3)}</strong> is the Newman score of the
            current community assignment: <em>1.0</em> = perfectly separable cliques,{" "}
            <em>0.0</em> = random, &le;&nbsp;0 = worse than random. Our value says{" "}
            {modularityQ >= 0.3
              ? "the ecosystem has clearly distinguishable sub-communities."
              : modularityQ >= 0.15
              ? "communities exist but overlap heavily — pick ALS or meta-paths for a sharper lens."
              : "the single-actor-lens graph is almost random at this scale, which is precisely why we built the extra lenses below."}
          </p>
        </DataReading>
      </PixelSection>

      {/* ————————
          § 2 · Communities
          ———————— */}
      <h2
        id="net-communities"
        style={{
          fontSize: 14,
          color: "var(--accent-info)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 2 · COMMUNITIES —who hangs out with whom?
      </h2>

      <PixelSection
        title="Top communities over 30 days"
        tone="info"
        headline={
          biggestCommunity
            ? `${communityList.length} communities total. Largest: ${biggestCommunity.size} repos. Click a member to jump to its repo profile.`
            : "Repos grouped by shared actors across the month. Click a member to inspect its profile & activity."
        }
        source="batch_repo_community"
        techBadge="Spark · weighted LPA (deterministic seeded)"
        findings={
          <>
            <p style={{ margin: 0 }}>
              <strong>Read with §3 inspector:</strong> a community&apos;s size alone doesn&apos;t tell you what it IS.
              Small communities are often highly focused (e.g. all maintained by the same bot); the big-tent
              communities tend to be heterogeneous. The feature-share breakdown below exposes the flavour.
            </p>
          </>
        }
      >
        {communityList.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No communities.</p>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 12 }}>
            {communityList.slice(0, 8).map((c) => (
              <div key={c.cid} className="nes-container is-dark" style={{ padding: 12 }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <PixelBadge tone="magenta" size="sm">
                    community {c.cid.slice(0, 16)}
                  </PixelBadge>
                  <span style={{ color: "var(--muted-strong)", fontSize: 11 }}>{c.size} repos</span>
                </div>
                <ul style={{ margin: "8px 0 0", paddingLeft: 16, fontSize: 11, color: "var(--fg)", lineHeight: 1.6 }}>
                  {c.members.slice(0, 6).map((m) => (
                    <li key={m}>
                      <EntityLink type="repo" id={m} />
                    </li>
                  ))}
                  {c.members.length > 6 ? (
                    <li style={{ color: "var(--muted)" }}>... +{c.members.length - 6} more</li>
                  ) : null}
                </ul>
              </div>
            ))}
          </div>
        )}
      </PixelSection>

      {/* ————————
          § 3 · Inspector
          ———————— */}
      <h2
        id="net-inspector"
        style={{
          fontSize: 14,
          color: "var(--accent-purple)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 3 · INSPECTOR —what kind of community is this?
      </h2>

      <PixelSection
        title="Community inspector —feature shares & dominant archetype"
        tone="purple"
        headline="Per-community 30-day average feature means (watch / PR-push / bot share) + top-3 repos by rank_score. The badge auto-labels the community by its strongest feature."
        source="batch_repo_community_profile"
        techBadge="Spark SQL · weighted LPA · per-community 30-day feature aggregation"
        howToRead={
          <>
            The tone on each card picks the highest of three fractions:
            <ul style={{ margin: "4px 0 0", paddingLeft: 18 }}>
              <li><strong style={{ color: "var(--accent-info)" }}>watcher-led</strong> if watch_share &gt; 40%: people star/watch, few commit.</li>
              <li><strong style={{ color: "var(--accent-positive)" }}>contributor-led</strong> if pr_push_ratio &gt; 40%: working repos with real code activity.</li>
              <li><strong style={{ color: "var(--accent-danger)" }}>bot-driven</strong> if bot_ratio &gt; 40%: mirrors, automation, dependency bumps.</li>
              <li><strong style={{ color: "var(--accent-magenta)" }}>mixed</strong> otherwise: diverse composition, no single driver dominates.</li>
            </ul>
          </>
        }
        findings={
          <>
            <strong>Why the inspector matters:</strong> a community labelled bot-driven may look huge but contributes
            little real ecosystem signal —you&apos;d filter it out when analysing genuine developer interest. A
            watcher-led community signals hype waves (great for trend spotting). A contributor-led community signals a
            working toolchain where cross-repo PRs are common.
          </>
        }
      >
        {communityProfileTop.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No community profiles yet.</p>
        ) : (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
              gap: 12,
            }}
          >
            {communityProfileTop.map((p: RepoCommunityProfilePoint) => {
              const dominantTone =
                p.botRatio > 0.4
                  ? "danger"
                  : p.watchShare > 0.4
                  ? "info"
                  : p.prPushRatio > 0.4
                  ? "positive"
                  : "magenta";
              const dominantLabel =
                p.botRatio > 0.4
                  ? "bot-driven"
                  : p.watchShare > 0.4
                  ? "watcher-led"
                  : p.prPushRatio > 0.4
                  ? "contributor-led"
                  : "mixed";
              const topMembers = (p.topMembers || "").split(" | ").filter(Boolean);
              return (
                <div
                  key={p.communityId}
                  className="nes-container is-dark"
                  style={{ padding: 12 }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      gap: 6,
                    }}
                  >
                    <PixelBadge tone={dominantTone} size="sm">
                      {dominantLabel}
                    </PixelBadge>
                    <span style={{ color: "var(--muted-strong)", fontSize: 11 }}>
                      {p.communitySize} repos · {p.totalEvents.toLocaleString()} ev
                    </span>
                  </div>
                  <div
                    style={{
                      marginTop: 8,
                      display: "grid",
                      gridTemplateColumns: "repeat(3, 1fr)",
                      gap: 4,
                      fontSize: 10,
                      color: "var(--muted-strong)",
                    }}
                  >
                    <div>
                      watch
                      <div style={{ color: "var(--accent-info)", fontSize: 12 }}>
                        {(p.watchShare * 100).toFixed(0)}%
                      </div>
                    </div>
                    <div>
                      pr+push
                      <div style={{ color: "var(--accent-positive)", fontSize: 12 }}>
                        {(p.prPushRatio * 100).toFixed(0)}%
                      </div>
                    </div>
                    <div>
                      bot
                      <div
                        style={{
                          color: p.botRatio > 0.3 ? "var(--accent-danger)" : "var(--fg)",
                          fontSize: 12,
                        }}
                      >
                        {(p.botRatio * 100).toFixed(0)}%
                      </div>
                    </div>
                  </div>
                  <div
                    style={{
                      marginTop: 8,
                      fontSize: 10,
                      color: "var(--muted)",
                    }}
                  >
                    avg rank_score {p.avgRankScore.toFixed(3)} · avg active {p.avgActiveDays.toFixed(1)}d
                  </div>
                  {topMembers.length > 0 ? (
                    <ul
                      style={{
                        margin: "8px 0 0",
                        paddingLeft: 14,
                        fontSize: 11,
                        lineHeight: 1.5,
                      }}
                    >
                      {topMembers.map((m) => (
                        <li key={m}>
                          <EntityLink type="repo" id={m} />
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </PixelSection>

      {/* ————————
          § 4 · Bridges —cross-community connectors (client-side)
          ———————— */}
      <h2
        id="net-bridges"
        style={{
          fontSize: 14,
          color: "var(--accent-change)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 4 · BRIDGES —repos that connect neighbourhoods
      </h2>

      <PixelSection
        title={`Top bridge repos (${bridgeRows.length} candidates)`}
        tone="change"
        headline={
          bridgeRows.length > 0 ? (
            <>
              The top bridge is <EntityLink type="repo" id={bridgeRows[0]!.node} /> —{(bridgeRows[0]!.bridgeScore * 100).toFixed(0)}% of
              its edge weight points to <strong>{bridgeRows[0]!.crossCommunities}</strong> other communities. If it
              disappeared, those neighbourhoods would drift further apart.
            </>
          ) : (
            "No cross-community bridges detected —communities are all disjoint islands."
          )
        }
        source="computed client-side from batch_repo_similarity_edges × batch_repo_community"
        techBadge="cross-community edge-weight ratio (filter: —2 other communities)"
        howToRead={
          <>
            For each repo v we compute{" "}
            <code style={{ color: "var(--accent-info)" }}>
              bridge(v) = Σ Jaccard(v, u) where community(u) —community(v), normalised by total edge weight of v
            </code>
            . High bridge score + many distinct cross-community neighbours = structural connector. Pure
            within-community stars don&apos;t show up here even if they have the highest absolute degree —this panel
            complements §1&apos;s Modularity Q by surfacing the repos <em>holding communities together</em>.
          </>
        }
        findings={
          <>
            <strong>Why this matters:</strong> a bridge repo is a natural target for cross-community recommendations,
            but also a single-point-of-failure for ecosystem cohesion. Contrast with <strong>repo bus-factor</strong>{" "}
            on People —§4 which is about <em>people</em> concentration; this is about <em>topological</em>{" "}
            concentration.
          </>
        }
      >
        {bridgeRows.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No bridge repos detected.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ color: "var(--muted)", textAlign: "left" }}>
                  <th style={{ padding: "6px 4px" }}>#</th>
                  <th style={{ padding: "6px 4px" }}>repo</th>
                  <th style={{ padding: "6px 4px" }}>own community</th>
                  <th style={{ padding: "6px 4px", textAlign: "right" }}>bridge score</th>
                  <th style={{ padding: "6px 4px", textAlign: "right" }}>× communities reached</th>
                  <th style={{ padding: "6px 4px", textAlign: "right" }}>total weight</th>
                </tr>
              </thead>
              <tbody>
                {bridgeRows.map((b, i) => (
                  <tr key={b.node} style={{ borderTop: "1px solid var(--pixel-border)" }}>
                    <td style={{ padding: "6px 4px", color: "var(--muted)" }}>{i + 1}</td>
                    <td style={{ padding: "6px 4px" }}>
                      <EntityLink type="repo" id={b.node} />
                    </td>
                    <td style={{ padding: "6px 4px", fontSize: 10, color: "var(--muted-strong)" }}>
                      {b.community.slice(0, 10)}—                    </td>
                    <td style={{ padding: "6px 4px", textAlign: "right" }}>
                      <PixelBadge tone={b.bridgeScore > 0.6 ? "danger" : b.bridgeScore > 0.4 ? "change" : "muted"} size="sm">
                        {(b.bridgeScore * 100).toFixed(0)}%
                      </PixelBadge>
                    </td>
                    <td style={{ padding: "6px 4px", textAlign: "right" }}>{b.crossCommunities}</td>
                    <td style={{ padding: "6px 4px", textAlign: "right", color: "var(--muted-strong)" }}>
                      {b.totalWeight.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <DataReading tone="change" title="reading a bridge row">
          {bridgeRows.length > 0 ? (
            <p style={{ margin: 0 }}>
              Take row #1: <strong>bridge score ={" "}
              {(bridgeRows[0]!.bridgeScore * 100).toFixed(0)}%</strong> means that fraction of
              this repo&apos;s total Jaccard weight flows to repos in <em>other</em>{" "}
              communities rather than its own. <strong>cross-communities reached</strong>{" "}
              = {bridgeRows[0]!.crossCommunities} says the bridge connects into that many
              distinct neighbourhoods (not just one external group).{" "}
              <strong>total weight</strong> = {bridgeRows[0]!.totalWeight.toFixed(2)} is the
              sum of Jaccard similarities from this repo to every neighbour —a higher value
              here with the same bridge % means the repo is both connected AND important.
              Compared to degree-only ranking, bridge score disqualifies &ldquo;local
              kings&rdquo; (high degree all within one community) and promotes structurally
              unique connectors: if this repo disappeared, those{" "}
              {bridgeRows[0]!.crossCommunities} neighbourhoods would lose their main link to
              the rest of the graph.
            </p>
          ) : (
            <p style={{ margin: 0, color: "var(--muted)" }}>
              No bridge candidates — the Jaccard graph already fragments into disjoint
              communities with no cross-links.
            </p>
          )}
        </DataReading>
      </PixelSection>

      {/* ————————
          § 5 · Rules —FPGrowth + Pareto
          ———————— */}
      <h2
        id="net-rules"
        style={{
          fontSize: 14,
          color: "var(--accent-magenta)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 5 · RULES —&ldquo;if you starred X, you probably also starred Y&rdquo;
      </h2>

      <PixelSection
        title="Rules Pareto frontier —support × confidence × lift over 30 days"
        tone="magenta"
        headline={`${paretoRules.length} rule(s) on the Pareto frontier —with a full month of actor-day baskets, support / confidence / lift are substantially less noisy than a 6-day snapshot.`}
        source="batch_repo_association_rules"
        techBadge="FPGrowth (30-day baskets) + Pareto-dominance filter (driver-side)"
        howToRead="X = support (how often the pair co-occurs in the month). Y = confidence P(consequent | antecedent). Dot size = lift (times above chance). Magenta diamonds form the Pareto frontier —picking any of these is non-dominated on all axes."
        findings={
          topRule ? (
            <>
              <strong>Flagship rule:</strong> when an actor touches{" "}
              <code style={{ color: "var(--accent-info)" }}>{topRule.antecedent}</code> on a given day,
              there&apos;s a <strong>{(topRule.confidence * 100).toFixed(0)}%</strong> chance they also touch{" "}
              <EntityLink type="repo" id={topRule.consequent} /> on the same day —that is{" "}
              <strong>{topRule.lift.toFixed(2)}×</strong> the baseline probability. 30-day baskets mean support is a
              real frequency, not a 1-day fluke.
            </>
          ) : undefined
        }
      >
        <RulesParetoScatter rules={rules} />
        <DataReading tone="magenta" title="reading a single rule">
          {topRule ? (
            <p style={{ margin: 0 }}>
              Take the flagship rule as a template: &ldquo;
              <code style={{ color: "var(--accent-info)" }}>{topRule.antecedent}</code> ⇒{" "}
              {topRule.consequent}&rdquo; with support{" "}
              <strong>{(topRule.support * 100).toFixed(1)}%</strong>, confidence{" "}
              <strong>{(topRule.confidence * 100).toFixed(0)}%</strong>, lift{" "}
              <strong>{topRule.lift.toFixed(2)}×</strong>. <em>Support</em> ={" "}
              <strong>{(topRule.support * 100).toFixed(1)}%</strong> of all (actor, day)
              baskets contain both repos — it&apos;s the raw frequency, the baseline signal.{" "}
              <em>Confidence</em> = if you see the antecedent on a day, there&apos;s a{" "}
              <strong>{(topRule.confidence * 100).toFixed(0)}%</strong> chance you also see
              the consequent that same day. <em>Lift</em> = confidence divided by the
              consequent&apos;s raw frequency —{" "}
              <strong>{topRule.lift.toFixed(2)}×</strong> means they co-occur{" "}
              {topRule.lift.toFixed(2)} times more often than chance. Anything with lift &gt; 2
              and confidence &gt; 40% is a strong actionable recommendation; the{" "}
              {paretoRules.length} diamonds on the Pareto frontier above are candidates that{" "}
              <em>no other rule dominates</em> on all three metrics.
            </p>
          ) : (
            <p style={{ margin: 0, color: "var(--muted)" }}>
              No rules above the support/confidence thresholds this month.
            </p>
          )}
        </DataReading>
      </PixelSection>

      <PixelSection
        title="Association rules (FPGrowth over 30-day actor-day baskets)"
        tone="change"
        headline="lift > 1 —repos co-occur in the same actor's day more than by chance. Baskets = one per (actor, day) over the 30-day window."
        source="batch_repo_association_rules"
        techBadge="Spark MLlib · FPGrowth · minSupport=0.02 · minConfidence=0.3"
      >
        <PixelSearchTable
          rows={rules}
          columns={ruleColumns}
          getRowKey={(r, i) => `${r.antecedent}->${r.consequent}-${i}`}
          initialSort={{ key: "lift", desc: true }}
          pageSize={15}
          searchPlaceholder="filter repo..."
          csvFilename="association_rules.csv"
        />
      </PixelSection>

      <PixelSection
        title="Top similarity edges (30-day shared-actor overlap)"
        tone="positive"
        headline="Pairs of repos with the highest Jaccard of actor sets across the 30-day window."
        source="batch_repo_similarity_edges"
      >
        <PixelSearchTable
          rows={edges.slice(0, 60)}
          columns={edgeColumns}
          getRowKey={(r, i) => `${r.srcRepo}-${r.dstRepo}-${i}`}
          initialSort={{ key: "jaccard", desc: true }}
          pageSize={15}
          searchPlaceholder="filter repo..."
          csvFilename="similarity_edges.csv"
        />
      </PixelSection>

      {/* ————————
          § 6 · Layer Comparison —do communities differ per event type?
          ———————— */}
      <h2
        id="net-layers"
        style={{
          fontSize: 14,
          color: "var(--accent-info)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 6 · LAYERS —one graph per event type, do they tell the same story?
      </h2>

      <PixelSection
        title="Layer-by-layer network comparison"
        tone="info"
        headline={
          layersPresent.length ? (
            <>
              We split the co-occurrence graph into <strong>{layersPresent.length}</strong> event-type layers and ran
              LPA on each. Each layer answers a different question —watch = &ldquo;same audience?&rdquo;, fork =
              &ldquo;same toolchain?&rdquo;, push/PR = &ldquo;same dev team?&rdquo;. The table below shows these
              layers often <em>disagree</em> about who belongs together.
            </>
          ) : (
            "No layer data yet —rerun network_depth.py with --phase 1."
          )
        }
        source="batch_repo_layer_edge · batch_repo_layer_community"
        techBadge="Spark · layer-wise Jaccard + weighted LPA · independent communities per layer"
        howToRead={
          <>
            For every event layer —{"{watch, fork, push, PR, issue}"} we build a subgraph using only that
            event&apos;s actor-repo matrix and recompute Jaccard edges + communities. If the ecosystem were uniform,
            every layer would look the same. In reality: <strong>watch</strong> usually dominates (stargazers &gt;&gt;            committers), while <strong>push/PR</strong> reveals a much sparser &ldquo;true work&rdquo; backbone.
          </>
        }
        findings={
          <>
            <p style={{ margin: 0 }}>
              <strong>What we observe:</strong>{" "}
              {layerEdgeCount > 0
                ? `watch and fork layers carry most of the Jaccard weight, but the push layer is almost empty —few repos share committers across the month. This is the classic "thousand stargazers, a handful of maintainers" pattern.`
                : "—layer computation produced zero edges; the 30-day window is too short for a dense push/PR layer."}
            </p>
            <p style={{ margin: "6px 0 0" }}>
              <strong>Why it matters:</strong> a recommender trained on the watch layer recommends trendy repos
              (feeds hype loops), while the push layer recommends genuine toolchain adjacency. Knowing the layer lets
              downstream models pick the right signal.
            </p>
          </>
        }
      >
        <LayerMatrix edges={layerEdges} communities={layerCommunities} />
        <DataReading tone="info" title="plain reading of each row">
          {(() => {
            const byLayer = layerEdges.reduce<Record<string, number>>((a, e) => {
              a[e.layer] = (a[e.layer] ?? 0) + 1;
              return a;
            }, {});
            const sorted = Object.entries(byLayer).sort((a, b) => b[1] - a[1]);
            const top = sorted[0];
            const bottom = sorted[sorted.length - 1];
            const ratio = top && bottom && bottom[1] > 0 ? top[1] / bottom[1] : 0;
            return (
              <>
                <p style={{ margin: 0 }}>
                  <strong>Edge count per layer</strong> tells us which <em>kind</em> of social
                  tie dominates. The &radic;-scaled bar on the right keeps small layers visible
                  next to the big ones —{" "}
                  {top && bottom ? (
                    <>
                      <strong>{top[0]}</strong> carries the most structure (
                      {top[1]} Jaccard pairs) while <strong>{bottom[0]}</strong> has only{" "}
                      {bottom[1]} —
                      {ratio >= 10
                        ? ` that's a ${Math.round(ratio)}× imbalance. The graph is almost entirely "${top[0]}-driven".`
                        : ` layers are roughly comparable, so all event types meaningfully contribute to the topology.`}
                    </>
                  ) : (
                    "no layer data produced edges this month."
                  )}
                </p>
                <p style={{ margin: "6px 0 0" }}>
                  <strong>avg&nbsp;jac / max&nbsp;jac</strong> = mean / peak overlap for pairs
                  inside that layer. An avg jac near 0.1 means connected pairs share only ~10% of
                  their actor sets —wide-but-shallow &ldquo;discovery&rdquo; ties. A max jac
                  near 1.0 would mean near-duplicate audiences (mirrors, forks of the same
                  project). <strong>biggest</strong> is the largest LPA community found using
                  ONLY that layer —if push/pr have a &ldquo;biggest&rdquo; of 0, nobody is
                  collaborating repo-to-repo via code this month, even if the watch layer still
                  finds big fan-clubs.
                </p>
              </>
            );
          })()}
        </DataReading>
      </PixelSection>

      {/* ————————
          § 7 · Ecosystem Core —k-core decomposition
          ———————— */}
      <h2
        id="net-core"
        style={{
          fontSize: 14,
          color: "var(--accent-change)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 7 · CORE —who are the ecosystem&apos;s &ldquo;always-on&rdquo; nodes?
      </h2>

      <PixelSection
        title="Actor k-core distribution"
        tone="change"
        headline={
          actorCorenessHist.length ? (
            <>
              <strong>{totalActorsInGraph}</strong> actors in the graph. Median k ={" "}
              <strong>{actorMedianK}</strong>. Inner core (k = {maxActorK}) has{" "}
              <strong>{topKBucket?.actors ?? 0}</strong> actors of which{" "}
              <strong
                style={{
                  color: topKBotShare > 0.5 ? "var(--accent-danger)" : "var(--accent-positive)",
                }}
              >
                {Math.round(topKBotShare * 100)}% are bots
              </strong>
              {topKBotShare > 0.8
                ? " —the most &ldquo;connected&rdquo; people in this ecosystem are not people at all."
                : topKBotShare > 0.5
                ? " —heavy automation presence at the apex."
                : " —a human-driven inner core."}
            </>
          ) : (
            "No coreness data."
          )
        }
        source="batch_actor_coreness"
        techBadge="NetworkX k-core decomposition · actor co-repo graph · bot flag from batch_actor_profile"
        howToRead={
          <>
            k-core(v) = the largest k such that v belongs to a subgraph where every node has degree —k. High k —            the node lives inside a dense, mutually-connected inner clique —it&apos;s not just popular, it&apos;s{" "}
            <em>structurally central</em>. A PageRank hub with low coreness is a star-with-spokes; high coreness is a
            tightly-wound network neighbourhood.
          </>
        }
        findings={
          <>
            <p style={{ margin: 0 }}>
              <strong>Bot alert:</strong> of actors with coreness = {maxActorK}, <strong>{topKBucket?.bots ?? 0}</strong>{" "}
              are flagged as bots. This is a crucial sanity check: PageRank and degree ranking would place these bots
              at the top of the &ldquo;most important people&rdquo; list, but coreness reveals they sit inside a
              dense <em>bot–bot</em> clique rather than leading the human community.
            </p>
            <p style={{ margin: "6px 0 0" }}>
              <strong>Human inner core:</strong> filter to non-bots with k &ge;3 to get the actual &ldquo;power
              users&rdquo; —the people whose loss would disconnect large parts of the graph.
            </p>
          </>
        }
      >
        <CorenessHistogram
          buckets={actorCorenessHist.map((b) => ({ coreness: b.coreness, count: b.actors }))}
          median={actorMedianK}
          color="var(--accent-change)"
        />
        <DataReading tone="change" title="what k actually tells you">
          <p style={{ margin: 0 }}>
            <strong>{(totalActorsInGraph - nonZeroActors).toLocaleString()}</strong> actors have
            k = 0 —they appear in the top-activity pool but never co-acted with anyone else
            inside that pool on the same repo this month. Hiding them lets the tail speak: of
            the <strong>{nonZeroActors.toLocaleString()}</strong> actors that <em>do</em> have an
            edge, the median sits at <strong>k = {actorMedianK}</strong>. Concretely, half the
            actors in the connected graph live in a neighbourhood where every member has at
            least {actorMedianK} other co-participants —that&apos;s a thin social layer.
          </p>
          <p style={{ margin: "6px 0 0" }}>
            The magenta inner-core bar at <strong>k = {maxActorK}</strong> holds{" "}
            <strong>{topKBucket?.actors ?? 0}</strong> actors,{" "}
            <strong
              style={{
                color: topKBotShare > 0.5 ? "var(--accent-danger)" : "var(--accent-positive)",
              }}
            >
              {Math.round(topKBotShare * 100)}% of whom are bots
            </strong>
            .{" "}
            {topKBotShare > 0.8
              ? "The densest clique at the top is essentially a bot echo chamber —a degree/PageRank ranking would have crowned these bots as ecosystem leaders. k-core shows they're orbiting each other, not pulling humans along."
              : topKBotShare > 0.5
              ? "Automation dominates the inner core — weight any 'top influencer' metric by the non-bot fraction before acting on it."
              : "The inner core is mostly human — those names are plausible targets for a power-user outreach or seeding campaign."}
          </p>
        </DataReading>
        <div style={{ marginTop: 10, fontSize: 11, color: "var(--muted-strong)" }}>
          <strong>Top-8 humans in the inner core:</strong>{" "}
          {actorCoreness
            .filter((a) => a.isBot === 0 && a.coreness > 0)
            .slice(0, 8)
            .map((a) => (
              <span key={a.actorLogin} style={{ marginRight: 8 }}>
                <EntityLink type="actor" id={a.actorLogin} /> <span style={{ color: "var(--muted)" }}>(k={a.coreness})</span>
              </span>
            ))}
        </div>
      </PixelSection>

      <PixelSection
        title="Repo k-core distribution (in the similarity graph)"
        tone="info"
        headline={
          repoCorenessBuckets.length ? (
            <>
              Inner core at k = <strong>{maxRepoK}</strong> has <strong>{innerCoreRepos.length}</strong> repos —they
              sit inside a mutually-dense neighbourhood in the Jaccard graph.
            </>
          ) : (
            "No repo coreness data."
          )
        }
        source="batch_repo_coreness"
        techBadge="NetworkX · repo-repo Jaccard graph (thresholded)"
        findings={
          innerCoreRepos.length ? (
            <>
              <strong>These repos together form the ecosystem&apos;s dense core:</strong>{" "}
              {innerCoreRepos.slice(0, 10).map((r, i) => (
                <span key={r.repoName}>
                  {i > 0 ? " · " : ""}
                  <EntityLink type="repo" id={r.repoName} />
                </span>
              ))}
              . Dropping any one of them wouldn&apos;t break the core (that&apos;s the point of k-core), but removing
              them all would disconnect neighbourhoods that currently pass information through this core.
            </>
          ) : undefined
        }
      >
        <CorenessHistogram buckets={repoCorenessBuckets} color="var(--accent-info)" />
        <DataReading tone="info" title="repo inner core vs. periphery">
          {(() => {
            const total = repoCorenessBuckets.reduce((a, b) => a + b.count, 0);
            const zero = repoCorenessBuckets.find((b) => b.coreness === 0)?.count ?? 0;
            const connected = total - zero;
            const innerCount = repoCorenessBuckets.find((b) => b.coreness === maxRepoK)?.count ?? 0;
            const innerShare = connected > 0 ? innerCount / connected : 0;
            return (
              <p style={{ margin: 0 }}>
                <strong>{total}</strong> repos ranked this month. <strong>{zero}</strong> of
                them (<strong>{total > 0 ? Math.round((zero / total) * 100) : 0}%</strong>) are
                <em> Jaccard-isolated</em> —they have a high rank_score but don&apos;t share
                enough actors with any other top-repo to form an edge above threshold. Of the
                remaining <strong>{connected}</strong> connected repos, only{" "}
                <strong>{innerCount}</strong> (<strong>{Math.round(innerShare * 100)}%</strong>)
                sit in the inner core at k = {maxRepoK}. That&apos;s an extremely small
                structural spine: removing the inner core would splinter the connected graph
                into small disconnected pieces. When you see one of these names in §1&apos;s
                similarity graph, it is rarely there by accident.
              </p>
            );
          })()}
        </DataReading>
      </PixelSection>

      {/* ————————
          § 8 · Archetype —rule-labelled + GMM-validated repo personas
          ———————— */}
      <h2
        id="net-archetype"
        style={{
          fontSize: 14,
          color: "var(--accent-magenta)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 8 · ARCHETYPES —six flavours of repo, cross-checked against GMM
      </h2>

      <PixelSection
        title="Repo archetype PCA scatter"
        tone="magenta"
        headline={
          archetypeStats.length ? (
            <>
              <strong>{archetypePoints.length}</strong> top repos partitioned into{" "}
              <strong>{archetypeStats.length}</strong> archetypes by hybrid rules, then validated by GMM clustering
              in the same PCA-reduced feature space. Biggest group:{" "}
              <strong style={{ color: archetypeStats[0]!.color }}>{archetypeStats[0]!.label}</strong> (
              {archetypeStats[0]!.members} repos).
            </>
          ) : (
            "No archetype data."
          )
        }
        source="batch_repo_archetype · batch_repo_archetype_centroid"
        techBadge="hybrid rules (thresholds) + Spark MLlib GMM(k=6) + PCA(2D) · rule-GMM overlap as confidence"
        howToRead={
          <>
            We engineered six features per repo (watch_share, pr_push_ratio, bot_ratio, active_days,
            contributor_count, rank_score), z-normalised them, then (a) applied rule-based labels using published
            heuristic thresholds and (b) ran GMM in the 6-D feature space. PC1 mostly separates popularity from dev
            activity; PC2 separates niche vs. support-heavy. Overlap between rule and GMM —how self-consistent the
            archetype is.
          </>
        }
        findings={
          archetypeStats.length ? (
            <>
              <p style={{ margin: 0 }}>
                <strong>Real-world inspiration:</strong> the classic &ldquo;GitHub OSS personas&rdquo; study groups
                repos into <em>showcases</em>, <em>labs</em> and <em>tools</em>. Our automatic, month-scoped version
                finds the same three big buckets plus three edge cases (burst factory, steady core, misc).
              </p>
              <p style={{ margin: "6px 0 0" }}>
                <strong>Cross-check quality:</strong> average rule↔GMM overlap ={" "}
                <strong>
                  {(
                    archetypeStats.reduce((a, b) => a + b.ruleGmmOverlap, 0) / archetypeStats.length
                  ).toFixed(2)}
                </strong>
                . Overlap near 0.5+ on the biggest archetypes means the hybrid label isn&apos;t just a handmade
                definition —an unsupervised clusterer largely agrees.
              </p>
            </>
          ) : undefined
        }
      >
        <ArchetypeScatter points={archetypeScatterPoints} />
        <DataReading tone="magenta" title="what the mix tells us about the ecosystem">
          {(() => {
            const total = archetypePoints.length || 1;
            const biggest = archetypeStats[0];
            const secondBiggest = archetypeStats[1];
            const showcaseShare =
              (archetypeStats.find((a) => a.key === "star_hungry_showcase")?.members ?? 0) / total;
            const prHeavyShare =
              (archetypeStats.find((a) => a.key === "pr_heavy_collab")?.members ?? 0) / total;
            const burstShare =
              (archetypeStats.find((a) => a.key === "burst_factory")?.members ?? 0) / total;
            const avgOverlap =
              archetypeStats.length > 0
                ? archetypeStats.reduce((a, b) => a + b.ruleGmmOverlap, 0) /
                  archetypeStats.length
                : 0;
            return (
              <>
                <p style={{ margin: 0 }}>
                  Each dot is one top-ranked repo placed in a 2-D PCA projection of its
                  behavioural profile. <strong>PC1</strong> roughly sorts
                  &ldquo;audience-centric &rarr; contributor-centric&rdquo;, so repos on the
                  left are watch-heavy showcases while repos on the right are PR/push-heavy
                  toolchains. <strong>PC2</strong> separates &ldquo;niche / focused&rdquo; from
                  &ldquo;support-load heavy&rdquo; (issues &amp; bot ratio).
                </p>
                {biggest ? (
                  <p style={{ margin: "6px 0 0" }}>
                    Biggest bucket:{" "}
                    <strong style={{ color: biggest.color }}>{biggest.label}</strong> with{" "}
                    <strong>{biggest.members}</strong> repos (
                    <strong>{Math.round((biggest.members / total) * 100)}%</strong> of the top
                    pool). {showcaseShare >= 0.25 ? (
                      <>
                        That <strong>{Math.round(showcaseShare * 100)}%</strong> of top repos
                        are star-hungry showcases is the headline number —the most visible
                        repos on GitHub are, at month scale, predominantly discovery-only, not
                        collaboration engines.
                      </>
                    ) : (
                      <>
                        The leading archetype only covers{" "}
                        {Math.round((biggest.members / total) * 100)}% of the pool —the
                        ecosystem is <em>not</em> dominated by one flavour.
                      </>
                    )}{" "}
                    {secondBiggest ? (
                      <>
                        Second:{" "}
                        <strong style={{ color: secondBiggest.color }}>
                          {secondBiggest.label}
                        </strong>
                        ({secondBiggest.members}).
                      </>
                    ) : null}
                  </p>
                ) : null}
                <p style={{ margin: "6px 0 0" }}>
                  Only <strong>{Math.round(prHeavyShare * 100)}%</strong> of top repos are
                  PR-heavy collabs and <strong>{Math.round(burstShare * 100)}%</strong> are
                  burst factories (one-shot dumps). Average rule↔GMM overlap is{" "}
                  <strong>{avgOverlap.toFixed(2)}</strong> —
                  {avgOverlap >= 0.5
                    ? " high agreement: the hand-crafted thresholds reproduce what an unsupervised Gaussian-mixture picks up in 6-D, so the labels are robust."
                    : avgOverlap >= 0.3
                    ? " moderate agreement; some archetypes (especially misc / steady_core) have fuzzy boundaries."
                    : " low agreement — treat the label as a heuristic, not a hard classification."}
                </p>
              </>
            );
          })()}
        </DataReading>
      </PixelSection>

      <PixelSection
        title="Archetype field guide —centroid features + exemplar repos"
        tone="purple"
        headline="Each card shows the average feature values for the archetype's members, plus the top rank-score repos inside it. Use this to ask 'which archetype is THIS repo like?' in §3 inspector / the repo drill-in."
        source="batch_repo_archetype_centroid"
        techBadge="centroid means + top-3 members by rank_score"
      >
        {archetypeStats.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No archetype centroids.</p>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 12 }}>
            {(() => {
              // sqrt-scale the bar so a 3-member bucket is still visible next to a 90-member one
              const maxMembers = Math.max(...archetypeStats.map((a) => a.members), 1);
              return archetypeStats.map((a) => {
                const topMembers = (archetypeByKey[a.key] ?? [])
                  .slice()
                  .sort((x, y) => y.rankScore - x.rankScore)
                  .slice(0, 4);
                const barPct = Math.max(
                  a.members > 0 ? 6 : 0,
                  (Math.sqrt(a.members) / Math.sqrt(maxMembers)) * 100,
                );
                return (
                  <div key={a.key} className="nes-container is-dark" style={{ padding: 12 }}>
                    <div
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        gap: 6,
                      }}
                    >
                      <span style={{ color: a.color, fontWeight: 700, fontSize: 12 }}>{a.label}</span>
                      <span style={{ color: "var(--muted-strong)", fontSize: 11 }}>{a.members} repos</span>
                    </div>
                    <div
                      title={`${a.members} members (${((a.members / maxMembers) * 100).toFixed(0)}% of biggest)`}
                      style={{
                        marginTop: 4,
                        height: 4,
                        width: `${barPct}%`,
                        maxWidth: "100%",
                        background: a.color,
                        opacity: 0.6,
                        borderRadius: 1,
                      }}
                    />
                  <div style={{ color: "var(--muted)", fontSize: 10, margin: "6px 0 8px", lineHeight: 1.4 }}>
                    {a.description}
                  </div>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(3, 1fr)",
                      gap: 4,
                      fontSize: 10,
                      color: "var(--muted-strong)",
                    }}
                  >
                    <div>
                      watch
                      <div style={{ color: "var(--accent-info)", fontSize: 12 }}>
                        {(a.avgWatch * 100).toFixed(0)}%
                      </div>
                    </div>
                    <div>
                      pr/push
                      <div style={{ color: "var(--accent-positive)", fontSize: 12 }}>
                        {a.avgPrPush.toFixed(2)}×
                      </div>
                    </div>
                    <div>
                      bot
                      <div style={{ color: a.avgBot > 0.3 ? "var(--accent-danger)" : "var(--fg)", fontSize: 12 }}>
                        {(a.avgBot * 100).toFixed(0)}%
                      </div>
                    </div>
                  </div>
                  <div style={{ marginTop: 6, fontSize: 10, color: "var(--muted)" }}>
                    {a.avgActiveDays.toFixed(1)}d · rank {a.avgRankScore.toFixed(0)} · rule↔GMM{" "}
                    {a.ruleGmmOverlap.toFixed(2)} · {a.gmmClusters} GMM sub-cluster{a.gmmClusters === 1 ? "" : "s"}
                  </div>
                  {topMembers.length > 0 ? (
                    <ul style={{ margin: "8px 0 0", paddingLeft: 14, fontSize: 11, lineHeight: 1.5 }}>
                      {topMembers.map((m) => (
                        <li key={m.repoName}>
                          <EntityLink type="repo" id={m.repoName} />{" "}
                          <span style={{ color: "var(--muted)", fontSize: 10 }}>
                            (r={m.rankScore.toFixed(0)})
                          </span>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              );
            });
            })()}
          </div>
        )}
        <DataReading tone="purple" title="reading a single card">
          <p style={{ margin: 0 }}>
            The coloured bar under each title is the archetype&apos;s <strong>share of the top
            repo pool</strong> (&radic;-scaled so rare archetypes stay visible). Below it, the
            three coloured numbers are the <em>average feature values</em> of members:{" "}
            <strong>watch %</strong> (how stargazing-heavy the repos are),{" "}
            <strong>pr/push ×</strong> (PR+push events per 100 watches —&ldquo;×&rdquo; because
            values &gt;1 mean more code than discovery activity), and{" "}
            <strong>bot %</strong> (fraction of activity from automated accounts).{" "}
          </p>
          <p style={{ margin: "6px 0 0" }}>
            The bottom line (<em>X.Xd · rank NNN · rule↔GMM 0.XX · N sub-clusters</em>) says:
            members are active on average X.X days out of 30, carry rank-score NNN, agree with
            the GMM clustering 0.XX of the time, and the GMM decomposes this archetype into N
            sub-shapes. A rule↔GMM of 0.5+ means this label is reproducibly a &ldquo;real&rdquo;
            shape in feature space, not just a hand-drawn line. Use the top-3 exemplar repos to
            sanity-check: if those three names feel consistent, the centroid means are trustworthy.
          </p>
        </DataReading>
      </PixelSection>

      {/* ————————
          § 9 · Evolution —week-over-week community lineage (Sankey)
          ———————— */}
      <h2
        id="net-evolution"
        style={{
          fontSize: 14,
          color: "var(--accent-positive)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 9 · EVOLUTION —which communities survive the month?
      </h2>

      <PixelSection
        title={`Community lineage over ${weeklyCommunityCounts.length} weeks`}
        tone="positive"
        headline={
          weeklyCommunityCounts.length ? (
            <>
              Each week we recompute LPA on just that week&apos;s co-occurrence and match new communities to old
              ones by max Jaccard overlap. Observed over the window:{" "}
              <strong>{lineageEventCounts.continue ?? 0}</strong> continuations ·{" "}
              <strong>{lineageEventCounts.merge_or_split ?? 0}</strong> merge/splits ·{" "}
              <strong>{lineageEventCounts.birth ?? 0}</strong> births ·{" "}
              <strong style={{ color: "var(--accent-danger)" }}>{lineageEventCounts.death ?? 0}</strong> deaths.
              Survival rate ={" "}
              <strong
                style={{
                  color: survivalShare >= 0.5 ? "var(--accent-positive)" : "var(--accent-danger)",
                }}
              >
                {(survivalShare * 100).toFixed(0)}%
              </strong>
              .
            </>
          ) : (
            "No temporal data —run --phase 3."
          )
        }
        source="batch_repo_community_weekly · batch_community_lineage"
        techBadge="Spark · weekly LPA + max-Jaccard lineage matcher (continue / merge_or_split / birth / death / reform)"
        howToRead={
          <>
            Vertical bars = communities each week, height proportional to #repos. Curved flows connect communities that share
            &ge; 30% repos week-over-week (Jaccard overlap). <strong>continue</strong> (green) = stable group,{" "}
            <strong>merge_or_split</strong> (orange) = communities restructured,{" "}
            <strong>reform</strong> (magenta) = a prior group re-emerged after dying. Births and deaths are implicit
            —a bar without an incoming or outgoing flow.
          </>
        }
        findings={
          weeklyCommunityCounts.length ? (
            <>
              <p style={{ margin: 0 }}>
                <strong>High churn, low persistence:</strong> with{" "}
                {lineageEventCounts.death ?? 0} deaths vs {lineageEventCounts.continue ?? 0} continues, less than half
                of communities persist week-over-week. This matches the intuition that GitHub activity is
                &ldquo;bursty&rdquo; —projects get attention, have a release or news cycle, then everyone moves on.
              </p>
              <p style={{ margin: "6px 0 0" }}>
                <strong>Why this matters:</strong> &ldquo;30-day communities&rdquo; (§2) hide this instability. A
                recommender should weigh communities by their survival signal —continuing communities are the real
                long-tail ecosystems, one-week communities are fads.
              </p>
            </>
          ) : undefined
        }
      >
        <CommunitySankey weekly={weekly} lineage={lineage} />
        <div style={{ marginTop: 10, fontSize: 11, color: "var(--muted-strong)" }}>
          Communities per week:{" "}
          {weeklyCommunityCounts.map((w) => (
            <span key={w.weekIdx} style={{ marginRight: 10 }}>
              <strong>w{w.weekIdx}</strong>={w.communities}
            </span>
          ))}
        </div>
        <DataReading tone="positive" title="what the flows count">
          {(() => {
            const cont = lineageEventCounts.continue ?? 0;
            const death = lineageEventCounts.death ?? 0;
            const birth = lineageEventCounts.birth ?? 0;
            const ms = lineageEventCounts.merge_or_split ?? 0;
            const reform = lineageEventCounts.reform ?? 0;
            return (
              <>
                <p style={{ margin: 0 }}>
                  Weekly LPA finds a fresh set of communities each week, then a max-Jaccard
                  matcher re-identifies them across adjacent weeks. The tags count the outcome:{" "}
                  <strong>{cont}</strong> groups <em>continued</em> (carried &ge;30% of their
                  repos into the next week), <strong>{ms}</strong> <em>merged or split</em>{" "}
                  (split into 2+ successors or absorbed other groups), <strong>{birth}</strong>{" "}
                  appeared out of nowhere, <strong>{death}</strong> dissolved with no successor,
                  and <strong>{reform}</strong> re-emerged after a gap.
                </p>
                <p style={{ margin: "6px 0 0" }}>
                  Survival share ={" "}
                  <strong
                    style={{
                      color: survivalShare >= 0.5 ? "var(--accent-positive)" : "var(--accent-danger)",
                    }}
                  >
                    {(survivalShare * 100).toFixed(0)}%
                  </strong>{" "}
                  = (continue + merge/split + reform) / total events.{" "}
                  {survivalShare >= 0.5
                    ? "Half or more of communities carry forward week-to-week — the ecosystem has real recurring sub-groups."
                    : survivalShare >= 0.3
                    ? "Fewer than half of communities persist — most weekly clusters are hype-driven, transient gatherings. The §2 30-day communities are a moving target."
                    : "Nearly every week is a new crop of communities — GitHub activity at this scale is closer to a stream of events than to stable social structure."}{" "}
                  For a recommender this is the critical number: weigh &ldquo;continues&rdquo;
                  highly (real signal), discount births/deaths (noise).
                </p>
              </>
            );
          })()}
        </DataReading>
      </PixelSection>

      {/* ————————
          § 10 · Influence Maximization
          ———————— */}
      <h2
        id="net-influence"
        style={{ fontSize: 14, color: "var(--accent-success)", marginTop: 28, marginBottom: 4 }}
      >
        § 10 · INFLUENCE —who should we seed?
      </h2>
      <PixelSection
        title="Influence maximization · Independent-Cascade + CELF greedy"
        headline={
          icReachSummary ? (
            <>
              With k={icReachSummary.kMax} seeds, the CELF-greedy strategy reaches{" "}
              <strong>{icReachSummary.greedyReach.toFixed(1)}</strong> actors in expectation
              (±{icReachSummary.greedyStd.toFixed(1)}) —{icReachSummary.liftVsDegree > 0 ? (
                <>
                  {" "}
                  <span style={{ color: "var(--accent-success)" }}>+{(icReachSummary.liftVsDegree * 100).toFixed(1)}%</span> over
                  top-degree and <span style={{ color: "var(--accent-success)" }}>+{(icReachSummary.liftVsRandom * 100).toFixed(0)}%</span> over random.
                </>
              ) : (
                <>
                  {" "}
                  matching top-degree; the graph is either too dense or too uniform for greedy to add much.
                </>
              )}{" "}
              Simulated with {icReachSummary.simRuns} Monte-Carlo live-edge samples.
            </>
          ) : (
            <>
              No IC data yet. Run <code>scripts/run_network_only.ps1 -Phase 4</code> to generate{" "}
              <code>batch_actor_ic_reach</code> + <code>batch_seed_greedy</code>.
            </>
          )
        }
        findings={
          icReachSummary ? (
            <>
              <p style={{ margin: 0 }}>
                <strong>Why CELF matters:</strong> top-degree picks influential hubs but they overlap heavily
                (&ldquo;diminishing returns&rdquo;). Greedy adds the next seed that opens a <em>new</em> region of the
                graph, not just another well-covered one. The gap widens with k —a real IM-for-engagement feature
                (e.g., &ldquo;invite these 10 power users&rdquo;) should use this, not a raw degree leaderboard.
              </p>
              <p style={{ margin: "6px 0 0" }}>
                <strong>Real-world anchor:</strong> this is the same algorithmic pattern used in Twitter / Weibo
                topic-seeding, epidemic contact-tracing, and ad-campaign influencer selection. Greedy is slow
                (O(k · n · R · BFS)) so we CELF-prune + pre-sample subgraphs.
              </p>
            </>
          ) : undefined
        }
        source="batch_actor_ic_reach · batch_seed_greedy"
        techBadge="PySpark · IC live-edge sampling · CELF (Leskovec 2007) · Python + NumPy + NetworkX"
      >
        <p
          style={{
            margin: "0 0 var(--pixel-space-3)",
            color: "var(--muted-strong)",
            fontSize: "var(--fs-body)",
            lineHeight: "var(--lh-tight)",
          }}
        >
          We model GitHub activity as an <strong>Independent-Cascade</strong> diffusion over the actor–actor
          co-participation graph. Each edge (u,v) gets activation probability <code>p = 1 − (1 − p0)^w</code> where{" "}
          <code>w</code> is the number of shared repos and <code>p0 = 0.02</code>. Given a seed set S, the expected
          spread σ(S) is the average size of the reachable component across {icReachSummary?.simRuns ?? "R"} live-edge
          samples. The optimization problem argmax σ is <strong>NP-hard</strong>, but σ is submodular and monotone, so
          the classic Kempe-Kleinberg-Tardos (2003) <strong>greedy gives a (1 − 1/e) ≈ 63% approximation</strong>. We
          use Leskovec&apos;s <strong>CELF</strong> trick to skip stale candidates, and compare four other strategies
          that a practitioner might try as shortcuts.
        </p>
        <div style={{ display: "grid", gridTemplateColumns: "1.1fr 1fr", gap: 14 }}>
          <div>
            <h4 style={{ margin: "0 0 8px", fontSize: 11, color: "var(--muted-strong)" }}>
              Expected reach vs k (five strategies)
            </h4>
            <IcReachChart rows={icReach.map((r) => ({
              k: r.k,
              strategy: r.strategy,
              expectedReach: r.expectedReach,
              reachStddev: r.reachStddev,
              simRuns: r.simRuns,
            }))} />
            <div style={{ marginTop: 6, fontSize: 10, color: "var(--muted)" }}>
              Solid lines = single-shot strategies (top-k by a scalar). Bold green = CELF greedy. Dashed = random
              baseline (seed={42}, same pool).
            </div>
          </div>

          <div>
            <h4 style={{ margin: "0 0 8px", fontSize: 11, color: "var(--muted-strong)" }}>
              CELF seed list (top {seedGreedy.length})
            </h4>
            <div style={{ overflow: "auto", maxHeight: 340, border: "1px solid var(--border)" }}>
              <table style={{ width: "100%", fontSize: 10, borderCollapse: "collapse" }}>
                <thead style={{ background: "var(--bg-sunken, #111)", position: "sticky", top: 0 }}>
                  <tr>
                    <th style={{ textAlign: "left", padding: "4px 6px" }}>#</th>
                    <th style={{ textAlign: "left", padding: "4px 6px" }}>actor</th>
                    <th style={{ textAlign: "right", padding: "4px 6px" }}>Δ reach</th>
                    <th style={{ textAlign: "right", padding: "4px 6px" }}>σ</th>
                    <th style={{ textAlign: "right", padding: "4px 6px" }}>deg</th>
                    <th style={{ textAlign: "right", padding: "4px 6px" }}>PR·1e4</th>
                  </tr>
                </thead>
                <tbody>
                  {seedGreedy.map((s) => (
                    <tr key={`seed-${s.seedRank}`} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={{ padding: "3px 6px" }}>{s.seedRank}</td>
                      <td style={{ padding: "3px 6px" }}>
                        <EntityLink type="actor" id={s.actorLogin} />
                        {s.isBot ? (
                          <PixelBadge tone="change" size="sm" style={{ marginLeft: 6 }}>
                            bot
                          </PixelBadge>
                        ) : null}
                      </td>
                      <td style={{ padding: "3px 6px", textAlign: "right" }}>{s.marginalGain.toFixed(2)}</td>
                      <td style={{ padding: "3px 6px", textAlign: "right", color: "var(--accent-success)" }}>
                        {s.cumulativeReach.toFixed(1)}
                      </td>
                      <td style={{ padding: "3px 6px", textAlign: "right" }}>{s.degree}</td>
                      <td style={{ padding: "3px 6px", textAlign: "right" }}>{(s.pagerank * 1e4).toFixed(2)}</td>
                    </tr>
                  ))}
                  {seedGreedy.length === 0 ? (
                    <tr>
                      <td colSpan={6} style={{ padding: 10, color: "var(--muted)", textAlign: "center" }}>
                        (no seed data)
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
            <div style={{ marginTop: 6, fontSize: 10, color: "var(--muted)" }}>
              Δ reach = marginal gain at pick time (diminishing). σ = running cumulative reach.
            </div>
          </div>
        </div>
        <DataReading tone="positive" title="reading the IM numbers">
          {icReachSummary ? (
            <>
              <p style={{ margin: 0 }}>
                Y-axis of the left chart is <strong>expected number of actors activated</strong>{" "}
                if we seed the listed set and let the cascade run; the X-axis is the seed-set
                size k. At k = {icReachSummary.kMax}, CELF greedy reaches{" "}
                <strong>{icReachSummary.greedyReach.toFixed(1)}</strong> actors on average
                (±{icReachSummary.greedyStd.toFixed(1)}) over{" "}
                {icReachSummary.simRuns} Monte-Carlo runs. That&apos;s{" "}
                <strong style={{ color: "var(--accent-positive)" }}>
                  +{(icReachSummary.liftVsDegree * 100).toFixed(1)}%
                </strong>{" "}
                more than picking the top-{icReachSummary.kMax} highest-degree actors and{" "}
                <strong style={{ color: "var(--accent-positive)" }}>
                  +{(icReachSummary.liftVsRandom * 100).toFixed(0)}%
                </strong>{" "}
                more than picking at random. In a 500-ish-actor sub-graph, that lift translates
                to dozens of additional activations per campaign — the concrete business value
                of using a submodular algorithm instead of a leaderboard.
              </p>
              <p style={{ margin: "6px 0 0" }}>
                The seed list on the right is <em>ordered by pick time</em>. Δ reach is the
                <strong> marginal gain</strong> the greedy expected when it added this actor on
                top of all previous picks — it monotonically decreases (that&apos;s
                submodularity in one number). When Δ reach drops below ~1, adding the (k+1)-th
                seed is barely worth it. Compare <strong>deg</strong> (raw degree) and{" "}
                <strong>PR·1e4</strong> (PageRank × 1e4) columns: seeds late in the list often
                have modest degree — they were picked because they open up a{" "}
                <em>new</em> region not covered by earlier picks, which is exactly what
                degree-only ranking misses.
              </p>
            </>
          ) : (
            <p style={{ margin: 0, color: "var(--muted)" }}>
              (IC section is empty — run Phase 4 to populate.)
            </p>
          )}
        </DataReading>
      </PixelSection>
    </PixelPageShell>
  );
}

