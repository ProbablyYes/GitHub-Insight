"use client";

import { LineChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid, Legend } from "recharts";

import { BurstScatterChart } from "@/components/advanced-trend-chart";
import { DashboardCharts } from "@/components/dashboard-charts";
import { EntityLink } from "@/components/entity";
import { OfflineSubnav } from "@/components/offline-subnav";
import {
  PixelBadge,
  PixelKpi,
  PixelSection,
  PixelSearchTable,
  type PixelSearchColumn,
} from "@/components/pixel";
import { PixelPageShell } from "@/components/pixel-shell";
import type {
  ActorBotSupervisedPoint,
  ActorBurstStabilityPoint,
  ActorChurnRiskPoint,
  ActorCohortDayPoint,
  ActorCollabEdgePoint,
  ActorGraphMetricPoint,
  ActorHotnessPoint,
  ActorPersonaBicPoint,
  ActorPersonaBotValidationPoint,
  ActorPersonaCentroidPoint,
  ActorPersonaPoint,
  ActorPersonaTransitionPoint,
  ActorRetentionCurvePoint,
  ActorRingPoint,
  BotClassifierMetaPoint,
  BotFeatureImportancePoint,
  OrgRankPoint,
  RepoBusFactorPoint,
  UserSegmentPoint,
} from "@/lib/dashboard";

import { CohortStackedBar } from "../ecosystem/cohort-stacked-bar";

import { BicElbowChart } from "./bic-elbow";
import { PersonaScatter } from "./persona-scatter";

function formatPercent(value: number, digits = 1): string {
  if (!Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

function shortLabel(value: string, maxLength = 18): string {
  if (!value) return "";
  return value.length <= maxLength ? value : `${value.slice(0, maxLength - 3)}...`;
}

type Props = {
  personaSample: ActorPersonaPoint[];
  centroids: ActorPersonaCentroidPoint[];
  orgRank: OrgRankPoint[];
  userSegments: UserSegmentPoint[];
  cohort: ActorCohortDayPoint[];
  bicSweep: ActorPersonaBicPoint[];
  botValidation: ActorPersonaBotValidationPoint[];
  transitions: ActorPersonaTransitionPoint[];
  graphMetrics: ActorGraphMetricPoint[];
  collabEdges: ActorCollabEdgePoint[];
  burstStability: ActorBurstStabilityPoint[];
  retentionCurves: ActorRetentionCurvePoint[];
  churnRisk: ActorChurnRiskPoint[];
  actorHotness: ActorHotnessPoint[];
  busFactor: RepoBusFactorPoint[];
  botSupervised: ActorBotSupervisedPoint[];
  botImportance: BotFeatureImportancePoint[];
  botMeta: BotClassifierMetaPoint[];
  actorRings: ActorRingPoint[];
};

export function PeopleClient({
  personaSample,
  centroids,
  orgRank,
  userSegments,
  cohort,
  bicSweep,
  botValidation,
  transitions,
  graphMetrics,
  collabEdges,
  burstStability,
  retentionCurves,
  churnRisk,
  actorHotness,
  busFactor,
  botSupervised,
  botImportance,
  botMeta,
  actorRings,
}: Props) {
  // ── Personas summary ────────────────────────────────────────────────────
  // Sort by share so "top persona" is deterministic regardless of load order.
  const sortedCentroids = [...centroids].sort((a, b) => b.share - a.share);
  const topCentroid = sortedCentroids[0];
  const totalPersonaActors = sortedCentroids.reduce((sum, row) => sum + row.members, 0);
  // "Bot-leaning" = any persona where the centroid's mean is_bot flag is meaningfully
  // above the ~0.5% population base rate. 0.05 is our soft cutoff because GMM centroids
  // smooth out single-bot outliers; anything above that reflects a bot-heavy bucket.
  const botPersonaShare = sortedCentroids
    .filter((row) => row.isBotAvg >= 0.05 || row.personaLabel.includes("bot"))
    .reduce((sum, row) => sum + row.share, 0);

  // ── Cohort summary (incl. existing pre-window flag) ─────────────────────
  const cohortMonthly = cohort.reduce<Record<string, number>>((acc, row) => {
    acc[row.cohort] = (acc[row.cohort] || 0) + row.actors;
    return acc;
  }, {});
  const cohortMonthlyTotal = Object.values(cohortMonthly).reduce((s, v) => s + v, 0);
  const newShare30d = cohortMonthlyTotal > 0 ? (cohortMonthly["new"] ?? 0) / cohortMonthlyTotal : 0;
  const reactivatedShare30d =
    cohortMonthlyTotal > 0 ? (cohortMonthly["reactivated"] ?? 0) / cohortMonthlyTotal : 0;
  const existingShare30d =
    cohortMonthlyTotal > 0 ? (cohortMonthly["existing"] ?? 0) / cohortMonthlyTotal : 0;

  const centroidBar = centroids.map((row) => ({
    label: `${row.personaId}:${shortLabel(row.personaLabel, 14)}`,
    value: Number((row.share * 100).toFixed(2)),
  }));

  const bicSelected = bicSweep.find((r) => r.isSelected);
  const bestBotPersona = botValidation[0];
  // Persona transition matrix is disabled: on our current sample (event_hour=5 slice)
  // almost no actors appear in BOTH halves of the window, so the outer-joined matrix
  // is empty in practice — we log the count silently instead of showing an empty grid.
  void transitions;

  // ── Collaboration / graph metrics ───────────────────────────────────────
  const topPageRank = [...graphMetrics].sort((a, b) => b.pagerank - a.pagerank).slice(0, 10);
  const topBridges = [...graphMetrics]
    .filter((r) => r.betweenness > 0)
    .sort((a, b) => b.betweenness - a.betweenness)
    .slice(0, 10);
  const communityCount = new Set(graphMetrics.map((r) => r.communityId)).size;
  const graphDensity = graphMetrics.length > 0 ? collabEdges.length / graphMetrics.length : 0;

  // ── Retention / churn ───────────────────────────────────────────────────
  const retentionByWeek = retentionCurves.reduce<Record<string, ActorRetentionCurvePoint[]>>(
    (acc, row) => {
      const key = row.cohortWeek;
      (acc[key] ??= []).push(row);
      return acc;
    },
    {},
  );
  const cohortWeeks = Object.keys(retentionByWeek).sort();
  // Line data: one row per "day_since_first" with one column per cohort week.
  const retentionLineData = (() => {
    const days = [0, 1, 3, 7, 14, 21, 28];
    return days.map((d) => {
      const row: Record<string, number | string> = { day: d };
      for (const week of cohortWeeks) {
        const match = retentionByWeek[week]?.find((r) => r.daysSinceFirst === d);
        if (match) row[week] = Number((match.retentionRate * 100).toFixed(1));
      }
      return row;
    });
  })();
  const avgDay7Retention = (() => {
    const vals = cohortWeeks
      .map((w) => retentionByWeek[w]?.find((r) => r.daysSinceFirst === 7)?.retentionRate ?? null)
      .filter((v): v is number => v !== null);
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
  })();

  const churnCounts = churnRisk.reduce<Record<string, number>>((acc, row) => {
    acc[row.riskTier] = (acc[row.riskTier] || 0) + 1;
    return acc;
  }, {});
  const atRisk = churnRisk.filter((r) => r.riskTier === "risk").slice(0, 20);
  const healthyCount = churnCounts["healthy"] ?? 0;
  const riskCount = churnCounts["risk"] ?? 0;

  // ── Influence ───────────────────────────────────────────────────────────
  const topHotness = actorHotness.slice(0, 12);
  const bus1Repos = busFactor.filter((r) => r.busFactor === "bus-1");
  const thinRepos = busFactor.filter((r) => r.busFactor === "thin");

  // ── Authenticity ────────────────────────────────────────────────────────
  const xgbAucRow = botMeta.find((m) => m.metric === "cv_auc_mean");
  const xgbPrRow = botMeta.find((m) => m.metric === "cv_pr_auc_mean");
  const nSamples = botMeta.find((m) => m.metric === "n_samples");
  const nPositive = botMeta.find((m) => m.metric === "n_positive");
  const topImportance = botImportance.slice(0, 8);
  const topSuspects = botSupervised.slice(0, 15);
  const ringCount = actorRings.length;

  // ── Table columns ───────────────────────────────────────────────────────
  const botValidationColumns: PixelSearchColumn<ActorPersonaBotValidationPoint>[] = [
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaLabel,
      searchValue: (r) => r.personaLabel,
      render: (r) => (
        <PixelBadge tone={r.f1 >= 0.5 ? "positive" : r.f1 >= 0.2 ? "change" : "muted"} size="sm">
          {r.personaLabel}
        </PixelBadge>
      ),
    },
    { key: "trueBots", header: "TP (bot)", align: "right", sortValue: (r) => r.trueBots },
    { key: "falseBots", header: "FP (human)", align: "right", sortValue: (r) => r.falseBots },
    {
      key: "precision",
      header: "precision",
      align: "right",
      sortValue: (r) => r.precision,
      render: (r) => formatPercent(r.precision, 1),
    },
    {
      key: "recall",
      header: "recall",
      align: "right",
      sortValue: (r) => r.recall,
      render: (r) => formatPercent(r.recall, 1),
    },
    {
      key: "f1",
      header: "F1",
      align: "right",
      sortValue: (r) => r.f1,
      render: (r) => (
        <span style={{ color: r.f1 >= 0.5 ? "var(--accent-positive)" : "var(--fg)" }}>
          {r.f1.toFixed(3)}
        </span>
      ),
    },
  ];

  const centroidColumns: PixelSearchColumn<ActorPersonaCentroidPoint>[] = [
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaId,
      searchValue: (r) => r.personaLabel,
      render: (r) => (
        <PixelBadge tone={r.isBotAvg > 0.5 ? "danger" : "magenta"} size="sm">
          {r.personaId}:{r.personaLabel}
        </PixelBadge>
      ),
    },
    { key: "share", header: "share", align: "right", sortValue: (r) => r.share, render: (r) => formatPercent(r.share, 1) },
    { key: "pushShareAvg", header: "push", align: "right", sortValue: (r) => r.pushShareAvg, render: (r) => formatPercent(r.pushShareAvg, 0) },
    { key: "prShareAvg", header: "pr", align: "right", sortValue: (r) => r.prShareAvg, render: (r) => formatPercent(r.prShareAvg, 0) },
    { key: "issuesShareAvg", header: "issues", align: "right", sortValue: (r) => r.issuesShareAvg, render: (r) => formatPercent(r.issuesShareAvg, 0) },
    { key: "watchShareAvg", header: "watch", align: "right", sortValue: (r) => r.watchShareAvg, render: (r) => formatPercent(r.watchShareAvg, 0) },
    { key: "forkShareAvg", header: "fork", align: "right", sortValue: (r) => r.forkShareAvg, render: (r) => formatPercent(r.forkShareAvg, 0) },
    { key: "isBotAvg", header: "bot", align: "right", sortValue: (r) => r.isBotAvg, render: (r) => formatPercent(r.isBotAvg, 0) },
  ];

  const pageRankColumns: PixelSearchColumn<ActorGraphMetricPoint>[] = [
    {
      key: "actorLogin",
      header: "actor",
      align: "left",
      sortValue: (r) => r.actorLogin,
      searchValue: (r) => r.actorLogin,
      render: (r) => <EntityLink type="actor" id={r.actorLogin} />,
    },
    {
      key: "pagerank",
      header: "PageRank",
      align: "right",
      sortValue: (r) => r.pagerank,
      render: (r) => (
        <span style={{ color: "var(--accent-info)" }}>{r.pagerank.toExponential(2)}</span>
      ),
    },
    {
      key: "betweenness",
      header: "betweenness",
      align: "right",
      sortValue: (r) => r.betweenness,
      render: (r) => r.betweenness.toFixed(4),
    },
    {
      key: "degree",
      header: "weighted-degree",
      align: "right",
      sortValue: (r) => r.degree,
    },
    {
      key: "communityId",
      header: "community",
      align: "right",
      sortValue: (r) => r.communityId,
      render: (r) => <PixelBadge size="sm" tone="purple">C{r.communityId}</PixelBadge>,
    },
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaLabel,
      render: (r) => <span style={{ color: "var(--muted)", fontSize: 11 }}>{r.personaLabel}</span>,
    },
  ];

  const bridgeColumns: PixelSearchColumn<ActorGraphMetricPoint>[] = [
    {
      key: "actorLogin",
      header: "bridge",
      align: "left",
      sortValue: (r) => r.actorLogin,
      render: (r) => <EntityLink type="actor" id={r.actorLogin} />,
    },
    {
      key: "betweenness",
      header: "betweenness",
      align: "right",
      sortValue: (r) => r.betweenness,
      render: (r) => (
        <span style={{ color: "var(--accent-magenta)" }}>{r.betweenness.toFixed(4)}</span>
      ),
    },
    { key: "degree", header: "degree", align: "right", sortValue: (r) => r.degree },
    {
      key: "communityId",
      header: "community",
      align: "right",
      sortValue: (r) => r.communityId,
      render: (r) => <PixelBadge size="sm" tone="purple">C{r.communityId}</PixelBadge>,
    },
  ];

  const churnColumns: PixelSearchColumn<ActorChurnRiskPoint>[] = [
    {
      key: "actorLogin",
      header: "actor",
      align: "left",
      sortValue: (r) => r.actorLogin,
      render: (r) => <EntityLink type="actor" id={r.actorLogin} />,
    },
    {
      key: "churnProb",
      header: "churn P",
      align: "right",
      sortValue: (r) => r.churnProb,
      render: (r) => (
        <span style={{ color: r.churnProb >= 0.6 ? "var(--accent-danger)" : "var(--fg)" }}>
          {formatPercent(r.churnProb, 0)}
        </span>
      ),
    },
    {
      key: "daysSinceLast",
      header: "idle days",
      align: "right",
      sortValue: (r) => r.daysSinceLast,
    },
    {
      key: "decaySlope",
      header: "slope",
      align: "right",
      sortValue: (r) => r.decaySlope,
      render: (r) => (
        <span style={{ color: r.decaySlope < 0 ? "var(--accent-danger)" : "var(--accent-positive)" }}>
          {r.decaySlope.toFixed(2)}
        </span>
      ),
    },
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaLabel,
      render: (r) => <span style={{ color: "var(--muted)", fontSize: 11 }}>{r.personaLabel}</span>,
    },
  ];

  const hotnessColumns: PixelSearchColumn<ActorHotnessPoint>[] = [
    {
      key: "rankNo",
      header: "#",
      align: "right",
      sortValue: (r) => r.rankNo,
    },
    {
      key: "actorLogin",
      header: "actor",
      align: "left",
      sortValue: (r) => r.actorLogin,
      render: (r) => <EntityLink type="actor" id={r.actorLogin} />,
    },
    {
      key: "hotnessScore",
      header: "hotness",
      align: "right",
      sortValue: (r) => r.hotnessScore,
      render: (r) => (
        <span style={{ color: "var(--accent-info)" }}>{r.hotnessScore.toFixed(3)}</span>
      ),
    },
    { key: "eventCount", header: "events", align: "right", sortValue: (r) => r.eventCount },
    { key: "uniqueRepos", header: "repos", align: "right", sortValue: (r) => r.uniqueRepos },
    {
      key: "avgRepoRankScore",
      header: "avg repo rank",
      align: "right",
      sortValue: (r) => r.avgRepoRankScore,
      render: (r) => r.avgRepoRankScore.toFixed(3),
    },
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaLabel,
      render: (r) => <span style={{ color: "var(--muted)", fontSize: 11 }}>{r.personaLabel}</span>,
    },
  ];

  const busFactorColumns: PixelSearchColumn<RepoBusFactorPoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    {
      key: "busFactor",
      header: "risk",
      align: "left",
      sortValue: (r) => r.busFactor,
      render: (r) => (
        <PixelBadge tone={r.busFactor === "bus-1" ? "danger" : "change"} size="sm">
          {r.busFactor}
        </PixelBadge>
      ),
    },
    {
      key: "topActor",
      header: "top contributor",
      align: "left",
      sortValue: (r) => r.topActor,
      render: (r) => <EntityLink type="actor" id={r.topActor} />,
    },
    {
      key: "topActorShare",
      header: "share",
      align: "right",
      sortValue: (r) => r.topActorShare,
      render: (r) => (
        <span style={{ color: r.topActorShare >= 0.8 ? "var(--accent-danger)" : "var(--fg)" }}>
          {formatPercent(r.topActorShare, 0)}
        </span>
      ),
    },
    {
      key: "contributorCount",
      header: "#contribs",
      align: "right",
      sortValue: (r) => r.contributorCount,
    },
    {
      key: "repoRankScore",
      header: "repo rank",
      align: "right",
      sortValue: (r) => r.repoRankScore,
      render: (r) => r.repoRankScore.toFixed(3),
    },
  ];

  const suspectsColumns: PixelSearchColumn<ActorBotSupervisedPoint>[] = [
    { key: "rankNo", header: "#", align: "right", sortValue: (r) => r.rankNo },
    {
      key: "actorLogin",
      header: "actor",
      align: "left",
      sortValue: (r) => r.actorLogin,
      render: (r) => <EntityLink type="actor" id={r.actorLogin} />,
    },
    {
      key: "xgbProbBot",
      header: "P(bot) XGB",
      align: "right",
      sortValue: (r) => r.xgbProbBot,
      render: (r) => (
        <span style={{ color: r.xgbProbBot >= 0.7 ? "var(--accent-danger)" : "var(--fg)" }}>
          {formatPercent(r.xgbProbBot, 0)}
        </span>
      ),
    },
    {
      key: "iforestScore",
      header: "IF anomaly",
      align: "right",
      sortValue: (r) => r.iforestScore,
      render: (r) => formatPercent(r.iforestScore, 0),
    },
    {
      key: "combinedScore",
      header: "combined",
      align: "right",
      sortValue: (r) => r.combinedScore,
      render: (r) => formatPercent(r.combinedScore, 0),
    },
    {
      key: "isBotTruth",
      header: "truth",
      align: "right",
      sortValue: (r) => r.isBotTruth,
      render: (r) => (
        <PixelBadge tone={r.isBotTruth === 1 ? "danger" : "muted"} size="sm">
          {r.isBotTruth === 1 ? "bot" : "human"}
        </PixelBadge>
      ),
    },
    {
      key: "personaLabel",
      header: "persona",
      align: "left",
      sortValue: (r) => r.personaLabel,
      render: (r) => <span style={{ color: "var(--muted)", fontSize: 11 }}>{r.personaLabel}</span>,
    },
  ];

  // A palette for retention curve lines.
  const retentionColors = [
    "#33ccff",
    "#ff66cc",
    "#33ff57",
    "#ffcc00",
    "#ff6b6b",
    "#9b6bff",
    "#6bffe4",
  ];

  return (
    <PixelPageShell
      title="L4 People"
      subtitle="Actor personas + collaboration + retention + influence + authenticity over a 30-day window."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "People" },
      ]}
      tldr={
        topCentroid ? (
          <>
            {totalPersonaActors.toLocaleString()} actors across {centroids.length} personas ·{" "}
            largest <span style={{ color: "var(--accent-magenta)" }}>{topCentroid.personaLabel}</span>{" "}
            ({formatPercent(topCentroid.share)}) · bot-like personas {formatPercent(botPersonaShare)}.
            {" "}Collab graph has {graphMetrics.length.toLocaleString()} nodes in{" "}
            {communityCount} communities, {collabEdges.length.toLocaleString()} edges (density{" "}
            {graphDensity.toFixed(2)}).{" "}
            XGBoost bot classifier CV-AUC ={" "}
            <span style={{ color: "var(--accent-info)" }}>
              {xgbAucRow ? xgbAucRow.value.toFixed(3) : "—"}
            </span>
            , {ringCount} activity ring{ringCount === 1 ? "" : "s"} flagged.
          </>
        ) : (
          "Persona data not loaded yet — run the offline pipeline."
        )
      }
    >
      <OfflineSubnav />

      {/* ────────────────── KPI strip ────────────────── */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: "var(--pixel-space-4)",
          marginBottom: "var(--pixel-space-5)",
        }}
      >
        <PixelKpi label="Active actors" value={totalPersonaActors.toLocaleString()} tone="info" />
        <PixelKpi
          label="Persona k (BIC vs prod)"
          value={bicSelected ? `${bicSelected.k} / 6` : `— / 6`}
          tone="magenta"
          hint={bicSelected ? `BIC=${bicSelected.bic.toFixed(0)} · prod frozen at 6` : "GMM k"}
        />
        <PixelKpi
          label="Top collaboration hub"
          value={topPageRank[0]?.actorLogin ?? "—"}
          tone="info"
          hint={
            topPageRank[0]
              ? `PageRank ${topPageRank[0].pagerank.toExponential(2)} · comm. C${topPageRank[0].communityId}`
              : "batch_actor_graph_metrics"
          }
        />
        <PixelKpi
          label="D7 retention (avg)"
          value={formatPercent(avgDay7Retention, 0)}
          tone={avgDay7Retention >= 0.4 ? "positive" : avgDay7Retention >= 0.2 ? "change" : "danger"}
          hint={`across ${cohortWeeks.length} weekly cohort${cohortWeeks.length === 1 ? "" : "s"}`}
        />
        <PixelKpi
          label="At-risk actors"
          value={riskCount.toLocaleString()}
          tone={riskCount > 0 ? "danger" : "muted"}
          hint={`healthy ${healthyCount} · predicted by LogReg on decay-slope + idle`}
        />
        <PixelKpi
          label="Bus-factor=1 repos"
          value={bus1Repos.length.toLocaleString()}
          tone={bus1Repos.length > 0 ? "danger" : "muted"}
          hint={`thin ${thinRepos.length} · top-1 contributor ≥ 80%`}
        />
        <PixelKpi
          label="XGB bot CV-AUC"
          value={xgbAucRow ? xgbAucRow.value.toFixed(3) : "—"}
          tone={xgbAucRow && xgbAucRow.value >= 0.8 ? "positive" : "change"}
          hint={
            xgbPrRow && nSamples && nPositive
              ? `PR-AUC ${xgbPrRow.value.toFixed(3)} · n=${Math.round(nSamples.value)} · pos=${Math.round(nPositive.value)}`
              : "5-fold stratified CV"
          }
        />
        <PixelKpi
          label="Activity rings flagged"
          value={ringCount.toLocaleString()}
          tone={ringCount > 0 ? "danger" : "positive"}
          hint="same-hour coordinated WatchEvents across ≥2 repos"
        />
      </div>

      {/* ════════════════════════════════════════════════════════════════════
          Inner anchor nav — 保持单 URL 多 section
          ════════════════════════════════════════════════════════════════════ */}
      <nav
        aria-label="People sections"
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
        <span style={{ color: "var(--muted)", marginRight: 6 }}>JUMP →</span>
        {[
          { id: "sec-personas", label: "§1 Personas", color: "var(--accent-magenta)" },
          { id: "sec-collab", label: "§2 Collab", color: "var(--accent-info)" },
          { id: "sec-retention", label: "§3 Retention", color: "var(--accent-change)" },
          { id: "sec-influence", label: "§4 Influence", color: "var(--accent-purple)" },
          { id: "sec-authenticity", label: "§5 Authenticity", color: "var(--accent-danger)" },
          { id: "sec-context", label: "§6 Context", color: "var(--accent-positive)" },
        ].map((s) => (
          <a
            key={s.id}
            href={`#${s.id}`}
            className="nes-btn"
            style={{
              padding: "4px 10px",
              fontSize: 10,
              textDecoration: "none",
              color: s.color,
              display: "inline-block",
            }}
          >
            {s.label}
          </a>
        ))}
      </nav>

      {/* ════════════════════════════════════════════════════════════════════
          Section 1 · PERSONAS (who are our actors?)
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-personas"
        style={{
          fontSize: 14,
          color: "var(--accent-magenta)",
          margin: "var(--pixel-space-4) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 1 · PERSONAS — who are our actors?
      </h2>

      <PixelSection
        title="L4 Persona map (PCA-2 scatter)"
        tone="positive"
        headline="Each dot is one actor. Colour = GMM cluster. Bot-tagged actors rendered as ×."
        source="batch_actor_persona"
        techBadge="Spark MLlib · StandardScaler · GMM(k=6) · PCA(2)"
        howToRead="Axes are PC1 / PC2 — top-2 variance directions after z-scoring 11 behaviour features (event volume, active days, repo entropy, hour entropy, push/pr/issues/watch/fork share, is_bot). Dots close together share behaviour; distant clusters = different personas."
        findings={
          topCentroid ? (
            <>
              <p>
                The largest persona this month is{" "}
                <strong style={{ color: "var(--accent-magenta)" }}>{topCentroid.personaLabel}</strong>{" "}
                ({formatPercent(topCentroid.share, 1)} of {totalPersonaActors.toLocaleString()}{" "}
                active actors). The label format is{" "}
                <code>archetype__top-2-z-score-features</code> — e.g. a{" "}
                <em>reviewer__pr+heavy_push−leaning</em> cluster is matched closest to the
                &quot;reviewer&quot; template by cosine similarity on the centroid&apos;s z-score
                vector, and its two most distinctive features are heavy PR share (+) and low push
                share (−).
              </p>
              <p style={{ marginTop: 8 }}>
                Together the {centroids.length} clusters cover{" "}
                <strong>{formatPercent(centroids.reduce((s, r) => s + r.share, 0), 0)}</strong> of
                the population. Bot-like personas make up{" "}
                <span style={{ color: "var(--accent-danger)" }}>{formatPercent(botPersonaShare)}</span>{" "}
                — use them as ground-truth seeds for the supervised model in §5.
              </p>
            </>
          ) : null
        }
      >
        {personaSample.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No persona data yet.</p>
        ) : (
          <PersonaScatter
            data={personaSample.map((row) => ({
              actorLogin: row.actorLogin,
              personaId: row.personaId,
              personaLabel: row.personaLabel,
              pcaX: row.pcaX,
              pcaY: row.pcaY,
              eventCount: row.eventCount,
              isBot: row.isBot,
            }))}
          />
        )}
      </PixelSection>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection title="Persona share" tone="info" headline="% of actors per persona.">
          <DashboardCharts variant="bar" data={centroidBar} color="#33ccff" />
        </PixelSection>

        <PixelSection
          title="Centroid explanation"
          tone="change"
          headline="Average feature value per persona — what makes each bucket tick."
          source="batch_actor_persona_centroid"
          findings={
            <p>
              Pay attention to <code>push</code>, <code>pr</code>, <code>watch</code> and{" "}
              <code>bot</code> columns — these are the features that still contain signal after we
              dropped <code>night_ratio</code>/<code>weekend_ratio</code> (they saturate at ≈1.0 on
              our sampled corpus and carry no discriminative power). The archetype name comes from
              matching z-scored centroids to 6 templates (maintainer / reviewer / watcher /
              night_bot / newbie / casual) via cosine similarity.
            </p>
          }
        >
          <PixelSearchTable
            rows={centroids}
            columns={centroidColumns}
            getRowKey={(r) => r.personaId}
            initialSort={{ key: "share", desc: true }}
            pageSize={8}
            searchable={false}
            fontSize={11}
          />
        </PixelSection>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection
          title="Model selection — BIC sweep (k = 3..8)"
          tone="purple"
          headline={
            bicSelected
              ? `BIC minimum at k=${bicSelected.k}; production frozen at k=6 for stable UX across reruns.`
              : "BIC curve over k."
          }
          source="batch_actor_persona_bic"
          techBadge="Spark MLlib · GMM (full covariance) · BIC = -2·logL + p·log(n)"
          howToRead="Lower BIC is better. The elbow / minimum tells us which k optimally balances fit quality vs model complexity penalty."
          findings={
            bicSelected ? (
              <p>
                At k={bicSelected.k} BIC bottoms out ({bicSelected.bic.toFixed(0)}). We keep k=6 in
                production so persona IDs stay stable across runs — a {bicSelected.k === 6 ? "no-op" : "small reputation penalty"} for the downstream UX.
              </p>
            ) : null
          }
        >
          <BicElbowChart rows={bicSweep} />
        </PixelSection>

        <PixelSection
          title="Bot validation — is persona `night_bot` actually bots?"
          tone="danger"
          headline={`Precision/recall/F1 against actor_category=='bot' truth. Window totals: ${botValidation[0]?.totalBots ?? 0} bots / ${botValidation[0]?.totalHumans ?? 0} humans.`}
          source="batch_actor_persona_bot_validation"
          techBadge="Spark SQL · confusion matrix"
          findings={
            bestBotPersona ? (
              <p>
                Best bot-catching persona: <strong>{bestBotPersona.personaLabel}</strong> (F1 ={" "}
                {bestBotPersona.f1.toFixed(3)}, precision {formatPercent(bestBotPersona.precision, 0)},
                recall {formatPercent(bestBotPersona.recall, 0)}). Unsupervised alone is only part of
                the answer — §5 XGBoost model lifts AUC to{" "}
                {xgbAucRow ? xgbAucRow.value.toFixed(3) : "—"}.
              </p>
            ) : null
          }
        >
          <PixelSearchTable
            rows={botValidation}
            columns={botValidationColumns}
            getRowKey={(r) => r.personaLabel}
            initialSort={{ key: "f1", desc: true }}
            pageSize={10}
            searchable={false}
            fontSize={11}
            csvFilename="persona_bot_validation.csv"
          />
        </PixelSection>
      </div>

      <PixelSection
        title="Actor cohorts — new / existing / returning / reactivated"
        tone="info"
        headline="Daily stacked breakdown. `existing` = first seen in the first 3 days of the window (likely pre-existing, not truly new)."
        source="batch_actor_cohort_day"
        techBadge="Spark SQL · windowed first-seen / gap-day detection"
        findings={
          <p>
            Over the whole window: {formatPercent(existingShare30d, 0)} existing ·{" "}
            {formatPercent(newShare30d, 0)} new ·{" "}
            {formatPercent(reactivatedShare30d, 0)} reactivated. We added the{" "}
            <code>existing</code> bucket precisely because actors first seen on Day 1–2 of the
            window almost certainly existed before — counting them as &quot;new&quot; inflates the
            growth signal. The remaining <code>new</code> figure is a much more honest proxy for
            new-user arrivals.
          </p>
        }
      >
        <CohortStackedBar rows={cohort} days={30} />
      </PixelSection>

      {/* ════════════════════════════════════════════════════════════════════
          Section 2 · COLLABORATION
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-collab"
        style={{
          fontSize: 14,
          color: "var(--accent-info)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 2 · COLLABORATION — who connects the network?
      </h2>

      <PixelSection
        title="Graph metrics summary"
        tone="info"
        headline={`Collab graph: ${graphMetrics.length.toLocaleString()} nodes, ${collabEdges.length.toLocaleString()} edges, ${communityCount} Louvain communities.`}
        source="batch_actor_graph_metrics + batch_actor_collab_edge"
        techBadge="NetworkX PageRank · Louvain modularity · approx betweenness (k=500)"
        howToRead="We build an undirected actor-actor graph where two actors are connected whenever they act on the same repo the same day. Edge weight = number of such co-occurrences. We run PageRank (influence), Louvain (cluster assignment) and approximate Betweenness (bridging power) on top-5k most-active actors."
        findings={
          topPageRank.length > 0 ? (
            <p>
              The #1 by PageRank is{" "}
              <EntityLink type="actor" id={topPageRank[0].actorLogin} /> (community C
              {topPageRank[0].communityId}). The #1 bridge by betweenness —
              connecting otherwise-distant communities — is{" "}
              {topBridges[0] ? <EntityLink type="actor" id={topBridges[0].actorLogin} /> : "—"}.
              Bridges are disproportionately maintainers of shared tooling (CI libs, linters) and are
              the people to copy when a knowledge-transfer stalls between teams.
            </p>
          ) : null
        }
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
            gap: "var(--pixel-space-4)",
          }}
        >
          <div>
            <div style={{ color: "var(--muted)", fontSize: 11, marginBottom: 6 }}>
              Top PageRank (influence)
            </div>
            <PixelSearchTable
              rows={topPageRank}
              columns={pageRankColumns}
              getRowKey={(r) => r.actorLogin}
              initialSort={{ key: "pagerank", desc: true }}
              pageSize={10}
              searchable={false}
              fontSize={11}
            />
          </div>
          <div>
            <div style={{ color: "var(--muted)", fontSize: 11, marginBottom: 6 }}>
              Top Bridges (betweenness)
            </div>
            <PixelSearchTable
              rows={topBridges}
              columns={bridgeColumns}
              getRowKey={(r) => r.actorLogin}
              initialSort={{ key: "betweenness", desc: true }}
              pageSize={10}
              searchable={false}
              fontSize={11}
            />
          </div>
        </div>
      </PixelSection>

      {/* ════════════════════════════════════════════════════════════════════
          Section 3 · RETENTION
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-retention"
        style={{
          fontSize: 14,
          color: "var(--accent-positive)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 3 · RETENTION &amp; CHURN — who keeps coming back?
      </h2>

      <PixelSection
        title="Weekly cohort retention curves"
        tone="positive"
        headline={`Each line = one week's newcomers. D7 avg retention: ${formatPercent(avgDay7Retention, 0)}.`}
        source="batch_actor_retention_curve"
        techBadge="Spark → pandas · cohort × days-since-first pivot"
        howToRead="X = days since the actor's first appearance. Y = % of that week's cohort still active on day X. D0 is always 100% by definition. Steeper decay = worse retention."
        findings={
          cohortWeeks.length > 0 ? (
            <p>
              The best-retaining cohort is week <strong>{cohortWeeks[0]}</strong>, the worst is{" "}
              <strong>{cohortWeeks[cohortWeeks.length - 1]}</strong>. If a recent-cohort curve
              drops faster than the baseline, something regressed in onboarding or marketing — the
              churn-risk model in the next chart tells you <em>who</em> specifically.
            </p>
          ) : null
        }
      >
        {cohortWeeks.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>
            No retention data yet — rerun Spark.
          </p>
        ) : (
          <div style={{ width: "100%", height: 300 }}>
            <ResponsiveContainer>
              <LineChart data={retentionLineData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--muted)" opacity={0.2} />
                <XAxis
                  dataKey="day"
                  label={{ value: "days since first seen", position: "insideBottom", offset: -2, fontSize: 10 }}
                  stroke="var(--muted)"
                />
                <YAxis
                  label={{ value: "% retained", angle: -90, position: "insideLeft", fontSize: 10 }}
                  stroke="var(--muted)"
                />
                <Tooltip />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                {cohortWeeks.map((week, i) => (
                  <Line
                    key={`retention-${week}`}
                    type="monotone"
                    dataKey={week}
                    stroke={retentionColors[i % retentionColors.length]}
                    strokeWidth={2}
                    dot={{ r: 2 }}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </PixelSection>

      <PixelSection
        title="Activity burst vs. stability (per actor)"
        tone="info"
        headline={`${burstStability.length.toLocaleString()} actors mapped into 4 behavioural quadrants. Short-spikers churn fastest, steady-core retains best.`}
        source="batch_actor_burst_stability"
        techBadge="Spark SQL · burst = (peak_day − mean) / mean ; stability = activity_days / span_days"
        howToRead="X-axis = stability (fraction of days active). Y-axis = burst (peak-day dominance over the mean). Bubbles split into four quadrants: short_spike (one-off surge), rising_core (spike on a stable base), steady_core (low-burst, high-stability — the real regulars), long_tail (neither)."
        findings={
          burstStability.length > 0 ? (
            <p>
              The <strong>steady_core</strong> quadrant tends to overlap with the lowest-churn slice
              in the next leaderboard, while <strong>short_spike</strong> actors are the first to
              be flagged as at-risk — they showed up once for a trending repo and then disappeared.
              Cross-reference with the churn probability column below to confirm this link.
            </p>
          ) : null
        }
      >
        {burstStability.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No burst/stability data.</p>
        ) : (
          <BurstScatterChart
            data={burstStability.map((r) => ({
              repoName: r.actorLogin,
              burstIndex: r.burstIndex,
              stabilityIndex: r.stabilityIndex,
              rankScore: r.rankScore,
              quadrant: r.quadrant,
            }))}
          />
        )}
      </PixelSection>

      <PixelSection
        title="Churn-risk leaderboard (top 20 at-risk actors)"
        tone="danger"
        headline={`${riskCount.toLocaleString()} actors flagged 'risk' · ${healthyCount.toLocaleString()} 'healthy'. Trained on 23-day features predicting last-7-day absence.`}
        source="batch_actor_churn_risk"
        techBadge="sklearn LogisticRegression · features = idle-days + 14d OLS slope + log(events) + log(days)"
        howToRead="Churn probability comes from a Logistic Regression fit to whether each actor disappeared in the last 7 days. A negative slope + many idle days both push the probability up."
        findings={
          atRisk.length > 0 ? (
            <p>
              The top at-risk actor right now is <EntityLink type="actor" id={atRisk[0].actorLogin} />{" "}
              ({formatPercent(atRisk[0].churnProb, 0)} churn probability,{" "}
              {atRisk[0].daysSinceLast} days idle, activity slope{" "}
              <span style={{ color: atRisk[0].decaySlope < 0 ? "var(--accent-danger)" : "inherit" }}>
                {atRisk[0].decaySlope.toFixed(2)}
              </span>
              ). For maintainers this is a loud signal to reach out for code-handoff. A non-linear
              model (e.g. XGBoost with persona one-hots) would push AUC higher — the logistic baseline
              is chosen for interpretability.
            </p>
          ) : null
        }
      >
        <PixelSearchTable
          rows={atRisk}
          columns={churnColumns}
          getRowKey={(r) => r.actorLogin}
          initialSort={{ key: "churnProb", desc: true }}
          pageSize={10}
          searchable={true}
          fontSize={11}
          csvFilename="churn_risk.csv"
        />
      </PixelSection>

      {/* ════════════════════════════════════════════════════════════════════
          Section 4 · INFLUENCE
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-influence"
        style={{
          fontSize: 14,
          color: "var(--accent-change)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 4 · INFLUENCE &amp; RISK — who matters, what breaks if they leave?
      </h2>

      <PixelSection
        title="Individual hotness leaderboard"
        tone="info"
        headline="hotness = log1p(events) × log1p(unique_repos) × avg(repo_rank_score). Rewards breadth × depth × the quality of the repos they touch."
        source="batch_actor_hotness"
        techBadge="Spark SQL join · multiplicative composite score"
        howToRead="Three factors are combined multiplicatively so you can't simply spam events to climb — you also need to be working on high-quality repos and on several of them."
        findings={
          topHotness[0] ? (
            <p>
              The most influential individual this month is{" "}
              <EntityLink type="actor" id={topHotness[0].actorLogin} /> (score{" "}
              {topHotness[0].hotnessScore.toFixed(3)}, {topHotness[0].eventCount.toLocaleString()}{" "}
              events across {topHotness[0].uniqueRepos} repos). Multiplying by{" "}
              <code>avg_repo_rank_score</code> is what separates genuine high-impact actors from
              CI-bots or drive-by committers that would otherwise dominate a raw event-count
              leaderboard.
            </p>
          ) : null
        }
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
            gap: "var(--pixel-space-4)",
          }}
        >
          <div>
            <DashboardCharts
              variant="bar"
              data={topHotness.map((r) => ({
                label: `#${r.rankNo} ${shortLabel(r.actorLogin, 14)}`,
                value: Number(r.hotnessScore.toFixed(3)),
              }))}
              color="#ff66cc"
            />
          </div>
          <PixelSearchTable
            rows={actorHotness}
            columns={hotnessColumns}
            getRowKey={(r) => r.actorLogin}
            initialSort={{ key: "rankNo", desc: false }}
            pageSize={10}
            searchable={true}
            fontSize={11}
            csvFilename="actor_hotness.csv"
          />
        </div>
      </PixelSection>

      <PixelSection
        title="Bus-factor watch-list (top contributor ≥ 50%)"
        tone="danger"
        headline={`${bus1Repos.length} bus-factor-1 repos (top person ≥ 80%) · ${thinRepos.length} thin (top person ≥ 50%).`}
        source="batch_repo_bus_factor"
        techBadge="Spark SQL · row_number + groupBy top-actor share"
        howToRead="If one person does 80% of the work, the repo has 'bus factor = 1' — a well-known software-sustainability red flag. Thin (50–80%) deserves a second-pair mentor."
        findings={
          bus1Repos[0] ? (
            <p>
              Example: <EntityLink type="repo" id={bus1Repos[0].repoName} /> has{" "}
              <EntityLink type="actor" id={bus1Repos[0].topActor} /> doing{" "}
              {formatPercent(bus1Repos[0].topActorShare, 0)} of activity ({bus1Repos[0].contributorCount}{" "}
              contributors total). If this repo also has a high rank score
              ({bus1Repos[0].repoRankScore.toFixed(3)}), treat it as a critical single-point-of-failure
              worth proactively mentoring a co-maintainer into.
            </p>
          ) : null
        }
      >
        <PixelSearchTable
          rows={busFactor}
          columns={busFactorColumns}
          getRowKey={(r) => r.repoName}
          initialSort={{ key: "topActorShare", desc: true }}
          pageSize={12}
          searchable={true}
          fontSize={11}
          csvFilename="bus_factor.csv"
        />
      </PixelSection>

      {/* ════════════════════════════════════════════════════════════════════
          Section 5 · AUTHENTICITY
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-authenticity"
        style={{
          fontSize: 14,
          color: "var(--accent-danger)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 5 · AUTHENTICITY — which activity is real?
      </h2>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection
          title="Supervised bot classifier · XGBoost (5-fold CV)"
          tone="danger"
          headline={
            xgbAucRow
              ? `CV-AUC = ${xgbAucRow.value.toFixed(3)} · PR-AUC = ${xgbPrRow ? xgbPrRow.value.toFixed(3) : "—"} · n = ${nSamples ? Math.round(nSamples.value) : "—"} (${nPositive ? Math.round(nPositive.value) : "—"} bots).`
              : "Training pending — rerun Spark."
          }
          source="batch_bot_classifier_meta + batch_bot_feature_importance"
          techBadge="xgboost XGBClassifier · 200 trees · max_depth 4 · lr 0.08"
          howToRead="AUC = how well the model ranks bots above humans (0.5=chance, 1=perfect). PR-AUC is the more honest metric when bots are rare in the dataset."
          findings={
            <p>
              5-fold stratified CV gives a model-selection estimate that is honest about the small
              positive class. Feature importance reveals <strong>which</strong> behaviours carry the
              bot signal — typically high <code>push_share</code> plus low <code>hour_entropy</code>
              for CI bots, unusual persona one-hots for platform bots.
            </p>
          }
        >
          {topImportance.length === 0 ? (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No importance data.</p>
          ) : (
            <DashboardCharts
              variant="bar"
              data={topImportance.map((r) => ({
                label: shortLabel(r.feature, 18),
                value: Number(r.importance.toFixed(4)),
              }))}
              color="#ff6b6b"
            />
          )}
        </PixelSection>

        <PixelSection
          title="Top suspected bots (XGB + IsolationForest combined)"
          tone="danger"
          headline="Combined score = 0.7 · P(bot XGB) + 0.3 · normalised IsolationForest anomaly."
          source="batch_actor_bot_supervised"
          techBadge="xgboost + sklearn IsolationForest"
          howToRead="XGB leverages the labelled ground truth; IsolationForest is unsupervised — it flags statistical outliers regardless of label. Agreement between the two is strong evidence."
          findings={
            topSuspects[0] ? (
              <p>
                Top suspect: <EntityLink type="actor" id={topSuspects[0].actorLogin} /> (P(bot) ={" "}
                {formatPercent(topSuspects[0].xgbProbBot, 0)}, IF anomaly{" "}
                {formatPercent(topSuspects[0].iforestScore, 0)}, ground truth ={" "}
                {topSuspects[0].isBotTruth === 1 ? "bot" : "human"}). Cases where truth=human but
                both scores are high are the <strong>interesting</strong> ones — they are
                unflagged-but-suspicious accounts.
              </p>
            ) : null
          }
        >
          <PixelSearchTable
            rows={topSuspects}
            columns={suspectsColumns}
            getRowKey={(r) => r.actorLogin}
            initialSort={{ key: "rankNo", desc: false }}
            pageSize={10}
            searchable={true}
            fontSize={11}
            csvFilename="bot_suspects.csv"
          />
        </PixelSection>
      </div>

      <PixelSection
        title="Coordinated-activity rings (WatchEvent bursts)"
        tone="danger"
        headline={`${ringCount} ring${ringCount === 1 ? "" : "s"} detected — groups of ≥3 accounts starring the same repos inside the same hour across ≥2 repos.`}
        source="batch_actor_ring"
        techBadge="Spark → pandas set-intersection over hour-bucketed WatchEvents"
        howToRead="A ring is NOT by itself proof of fraud — it could be a coordinated marketing push. But combined with high IsolationForest score + newly-created accounts it is a classic star-farming pattern."
        findings={
          actorRings[0] ? (
            <p>
              Largest ring: {actorRings[0].actorCount} accounts co-starring{" "}
              {actorRings[0].reposShared} repos. Sample members:{" "}
              {actorRings[0].sampleActors.slice(0, 5).map((a, i) => (
                <span key={`sa-${actorRings[0].ringId}-${i}`}>
                  {i > 0 ? ", " : ""}
                  <EntityLink type="actor" id={a} />
                </span>
              ))}
              .
            </p>
          ) : (
            <p>No rings detected in this window — a healthy sign of organic star activity.</p>
          )
        }
      >
        {actorRings.length === 0 ? (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No ring data.</p>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ color: "var(--muted)" }}>
                  <th style={{ textAlign: "left", padding: "6px 4px" }}>ring</th>
                  <th style={{ textAlign: "right", padding: "6px 4px" }}>accounts</th>
                  <th style={{ textAlign: "right", padding: "6px 4px" }}>repos</th>
                  <th style={{ textAlign: "right", padding: "6px 4px" }}>co-bursts</th>
                  <th style={{ textAlign: "left", padding: "6px 4px" }}>members (sample)</th>
                </tr>
              </thead>
              <tbody>
                {actorRings.map((r) => (
                  <tr key={`ring-${r.ringId}`} style={{ borderTop: "1px dashed var(--muted)" }}>
                    <td style={{ padding: "6px 4px" }}>
                      <PixelBadge tone="danger" size="sm">
                        R{r.ringId}
                      </PixelBadge>
                    </td>
                    <td style={{ textAlign: "right", padding: "6px 4px" }}>{r.actorCount}</td>
                    <td style={{ textAlign: "right", padding: "6px 4px" }}>{r.reposShared}</td>
                    <td style={{ textAlign: "right", padding: "6px 4px" }}>
                      {r.avgCoBursts.toFixed(1)}
                    </td>
                    <td style={{ padding: "6px 4px" }}>
                      {r.sampleActors.slice(0, 6).map((a, i) => (
                        <span key={`ring-${r.ringId}-actor-${i}`}>
                          {i > 0 ? ", " : ""}
                          <EntityLink type="actor" id={a} />
                        </span>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </PixelSection>

      {/* ════════════════════════════════════════════════════════════════════
          Section 6 · CONTEXT (orgs + reference user segments)
          ════════════════════════════════════════════════════════════════════ */}
      <h2
        id="sec-context"
        style={{
          fontSize: 14,
          color: "var(--muted-strong)",
          margin: "var(--pixel-space-6) 0 var(--pixel-space-3)",
          letterSpacing: 2,
          scrollMarginTop: 60,
        }}
      >
        § 6 · CONTEXT — orgs &amp; rule-based reference segments
      </h2>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection
          title="Top organizations / owners"
          tone="positive"
          headline="Hotness summed across their repos."
          source="batch_org_rank_latest"
        >
          {orgRank.length === 0 ? (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No org data.</p>
          ) : (
            <DashboardCharts
              variant="bar"
              data={orgRank.map((row) => ({
                label: `#${row.rankNo} ${shortLabel(row.orgOrOwner, 14)}`,
                value: Number(row.hotnessScore.toFixed(3)),
              }))}
              color="#33ff57"
            />
          )}
        </PixelSection>

        <PixelSection
          title="Rule-based user segments (reference)"
          tone="change"
          headline="Compare rule-based tiers vs GMM clusters above — overlap = validation of the clustering."
          source="batch_user_segment_latest"
        >
          {userSegments.length === 0 ? (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No segment data.</p>
          ) : (
            <DashboardCharts
              variant="bar"
              data={userSegments.map((row) => ({
                label: shortLabel(row.segment, 16),
                value: row.users,
              }))}
              color="#ffcc00"
            />
          )}
        </PixelSection>
      </div>
    </PixelPageShell>
  );
}
