/* eslint-disable react/no-unescaped-entities -- long narrative paragraphs are
   intentional prose and come from us, not user input; escaping every apostrophe
   would hurt readability. */
"use client";

import { useState } from "react";

import { BurstScatterChart } from "@/components/advanced-trend-chart";
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
import type { AuthenticityRow } from "@/lib/authenticity";
import { SIGNAL_LABEL } from "@/lib/authenticity";
import type {
  AdvancedRepoRankPoint,
  BurstStabilityPoint,
  HotVsColdAttributionPoint,
  OfflineAnomalyAlertPoint,
  OfflineDeclineWarningPoint,
  RepoContributorConcentrationPoint,
  RepoDnaCohortStats,
  RepoDnaOutlierPoint,
  RepoDnaPoint,
  RepoRankDeltaExplainPoint,
  RepoRankHistoryPoint,
  RepoWatcherProfilePoint,
} from "@/lib/dashboard";
import { AttributionForestPlot } from "./attribution-forest";
import { AuthenticityTable } from "./authenticity-table";
import { RankPathChart } from "./rank-path-chart";

function signed(value: number, digits = 3): string {
  const f = value.toFixed(digits);
  return value >= 0 ? `+${f}` : f;
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function dominantDriverLabel(row: RepoRankDeltaExplainPoint): string {
  const drivers = [
    { key: "hotness", value: row.deltaHotnessPart },
    { key: "momentum", value: row.deltaMomentumPart },
    { key: "engagement", value: row.deltaEngagementPart },
    { key: "stability", value: row.deltaStabilityPart },
    { key: "bot penalty", value: -row.deltaBotPenalty },
  ];
  drivers.sort((a, b) => Math.abs(b.value) - Math.abs(a.value));
  return drivers[0]?.key ?? "unknown";
}

type Props = {
  rankings: AdvancedRepoRankPoint[];
  burstSnapshot: BurstStabilityPoint[];
  contributorConcentration: RepoContributorConcentrationPoint[];
  rankDeltas: RepoRankDeltaExplainPoint[];
  attribution: HotVsColdAttributionPoint[];
  dynamicAttribution: HotVsColdAttributionPoint[];
  dnaTopHot: RepoDnaPoint[];
  dnaCohortStats: RepoDnaCohortStats[];
  dnaOutliers: RepoDnaOutlierPoint[];
  watcherProfile: RepoWatcherProfilePoint[];
  anomalyAlerts: OfflineAnomalyAlertPoint[];
  declineWarnings: OfflineDeclineWarningPoint[];
  rankPaths: RepoRankHistoryPoint[];
  authenticity: AuthenticityRow[];
};

export function ReposClient({
  rankings,
  burstSnapshot,
  contributorConcentration,
  rankDeltas,
  attribution,
  dynamicAttribution,
  dnaTopHot,
  dnaCohortStats,
  dnaOutliers,
  watcherProfile,
  anomalyAlerts,
  declineWarnings,
  rankPaths,
  authenticity,
}: Props) {
  const [attrMode, setAttrMode] = useState<"static" | "dynamic">("static");

  const topBusFactor = contributorConcentration[0];
  const topMover = rankDeltas[0];
  const attrAll = attribution.filter((r) => r.cohortScope === "all");
  const topHotReason = attrAll.filter((row) => row.direction === "hot_higher")[0];
  const topColdReason = attrAll.filter((row) => row.direction === "cold_higher")[0];
  const humansAttr = attribution.filter((r) => r.cohortScope === "humans_only");
  const botsAttr = attribution.filter((r) => r.cohortScope === "bots_only");
  const scopesAvailable = [
    { key: "all", count: attrAll.length },
    { key: "humans_only", count: humansAttr.length },
    { key: "bots_only", count: botsAttr.length },
  ].filter((s) => s.count > 0);

  // Dynamic attribution: risers (mean_hot) vs fallers (mean_cold), cohortScope always "all".
  const dynRiserSide = [...dynamicAttribution]
    .filter((r) => r.direction === "hot_higher")
    .sort((a, b) => Math.abs(b.cohenD) - Math.abs(a.cohenD))[0];
  const dynFallerSide = [...dynamicAttribution]
    .filter((r) => r.direction === "cold_higher")
    .sort((a, b) => Math.abs(b.cohenD) - Math.abs(a.cohenD))[0];
  const hasDynamic = dynamicAttribution.length > 0;
  const activeAttribution = attrMode === "dynamic" ? dynamicAttribution : attribution;
  const topAuthenticity = authenticity[0];

  const dnaColumns: PixelSearchColumn<RepoDnaPoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      searchValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    { key: "watchShare", header: "watch", align: "right", sortValue: (r) => r.watchShare, render: (r) => `${(r.watchShare * 100).toFixed(0)}%` },
    { key: "forkShare", header: "fork", align: "right", sortValue: (r) => r.forkShare, render: (r) => `${(r.forkShare * 100).toFixed(0)}%` },
    { key: "issuesShare", header: "issues", align: "right", sortValue: (r) => r.issuesShare, render: (r) => `${(r.issuesShare * 100).toFixed(0)}%` },
    { key: "prShare", header: "pr", align: "right", sortValue: (r) => r.prShare, render: (r) => `${(r.prShare * 100).toFixed(0)}%` },
    { key: "pushShare", header: "push", align: "right", sortValue: (r) => r.pushShare, render: (r) => `${(r.pushShare * 100).toFixed(0)}%` },
    { key: "botRatio", header: "bot", align: "right", sortValue: (r) => r.botRatio, render: (r) => `${(r.botRatio * 100).toFixed(0)}%` },
    { key: "nightRatio", header: "night", align: "right", sortValue: (r) => r.nightRatio, render: (r) => `${(r.nightRatio * 100).toFixed(0)}%` },
    { key: "top1ActorShare", header: "top1", align: "right", sortValue: (r) => r.top1ActorShare, render: (r) => `${(r.top1ActorShare * 100).toFixed(0)}%` },
    { key: "eventEntropy", header: "H", align: "right", sortValue: (r) => r.eventEntropy, render: (r) => r.eventEntropy.toFixed(2) },
  ];

  const watcherColumns: PixelSearchColumn<RepoWatcherProfilePoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      searchValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    { key: "watchers", header: "watchers", align: "right", sortValue: (r) => r.watchers },
    { key: "dominantPersona", header: "persona", align: "left", sortValue: (r) => r.dominantPersona, render: (r) => <PixelBadge tone="magenta" size="sm">{r.dominantPersona}</PixelBadge> },
    {
      key: "dominantLift",
      header: "lift",
      align: "right",
      sortValue: (r) => r.dominantLift,
      render: (r) => (
        <span style={{ color: r.dominantLift >= 1.5 ? "var(--accent-positive)" : "var(--fg)" }}>
          {r.dominantLift.toFixed(2)}×
        </span>
      ),
    },
    { key: "avgNightRatio", header: "night", align: "right", sortValue: (r) => r.avgNightRatio, render: (r) => `${(r.avgNightRatio * 100).toFixed(0)}%` },
    { key: "avgPrPushRatio", header: "pr/push", align: "right", sortValue: (r) => r.avgPrPushRatio, render: (r) => `${(r.avgPrPushRatio * 100).toFixed(0)}%` },
    { key: "avgUniqueRepos", header: "avg repos", align: "right", sortValue: (r) => r.avgUniqueRepos, render: (r) => r.avgUniqueRepos.toFixed(1) },
    { key: "botRatio", header: "bot", align: "right", sortValue: (r) => r.botRatio, render: (r) => `${(r.botRatio * 100).toFixed(0)}%` },
  ];

  const rankingColumns: PixelSearchColumn<AdvancedRepoRankPoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      searchValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    { key: "rankNo", header: "rank", align: "right", sortValue: (r) => r.rankNo, render: (r) => `#${r.rankNo}` },
    { key: "rankScore", header: "score", align: "right", sortValue: (r) => r.rankScore, render: (r) => r.rankScore.toFixed(4) },
    { key: "trendLabel", header: "trend", align: "left", sortValue: (r) => r.trendLabel },
  ];

  const outlierColumns: PixelSearchColumn<RepoDnaOutlierPoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      searchValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    {
      key: "zDistance",
      header: "z-dist",
      align: "right",
      sortValue: (r) => r.zDistance,
      render: (r) => (
        <PixelBadge tone={r.zDistance >= 4 ? "danger" : r.zDistance >= 3 ? "change" : "info"} size="sm">
          {r.zDistance.toFixed(2)}
        </PixelBadge>
      ),
    },
    {
      key: "offFeatures",
      header: "off-axis features (sign · |z|)",
      align: "left",
      searchValue: (r) => r.offFeatures,
      render: (r) => (
        <span style={{ fontSize: 10, color: "var(--muted-strong)" }}>{r.offFeatures}</span>
      ),
    },
    {
      key: "botRatio",
      header: "bot",
      align: "right",
      sortValue: (r) => r.botRatio,
      render: (r) => formatPercent(r.botRatio),
    },
    {
      key: "prPushRatio",
      header: "pr/push",
      align: "right",
      sortValue: (r) => r.prPushRatio,
      render: (r) => r.prPushRatio.toFixed(2),
    },
    {
      key: "watchShare",
      header: "watch",
      align: "right",
      sortValue: (r) => r.watchShare,
      render: (r) => formatPercent(r.watchShare),
    },
  ];

  const deltaColumns: PixelSearchColumn<RepoRankDeltaExplainPoint>[] = [
    {
      key: "repoName",
      header: "repo",
      align: "left",
      sortValue: (r) => r.repoName,
      searchValue: (r) => r.repoName,
      render: (r) => <EntityLink type="repo" id={r.repoName} />,
    },
    {
      key: "deltaRankNo",
      header: "Δrank",
      align: "right",
      sortValue: (r) => -r.deltaRankNo,
      render: (r) => <span style={{ color: r.deltaRankNo > 0 ? "var(--accent-positive)" : r.deltaRankNo < 0 ? "var(--accent-danger)" : "var(--fg)" }}>{r.deltaRankNo >= 0 ? "+" : ""}{r.deltaRankNo}</span>,
    },
    { key: "deltaRankScore", header: "Δscore", align: "right", sortValue: (r) => r.deltaRankScore, render: (r) => signed(r.deltaRankScore, 4) },
    { key: "driver", header: "driver", align: "left", sortValue: (r) => dominantDriverLabel(r), render: (r) => <PixelBadge tone="info" size="sm">{dominantDriverLabel(r)}</PixelBadge> },
  ];

  const totalAlerts = anomalyAlerts.length + declineWarnings.length;

  // 30-day climber analysis across rankPaths. Guard against empty data and
  // collapsed windows where earliestDate === latestDate (so every delta is 0
  // and neither "climber" nor "faller" means anything).
  const rankPathDates = Array.from(new Set(rankPaths.map((r) => r.metricDate))).sort();
  const earliestDate = rankPathDates[0];
  const latestDate = rankPathDates[rankPathDates.length - 1];
  const earliestByRepo = new Map<string, number>();
  const latestByRepo = new Map<string, number>();
  if (earliestDate && latestDate && earliestDate !== latestDate) {
    for (const r of rankPaths) {
      if (!r.repoName) continue;
      if (r.metricDate === earliestDate) earliestByRepo.set(r.repoName, r.rankNo);
      if (r.metricDate === latestDate) latestByRepo.set(r.repoName, r.rankNo);
    }
  }
  const climbers: { repoName: string; early: number; latest: number; delta: number }[] = [];
  latestByRepo.forEach((latest, repoName) => {
    const early = earliestByRepo.get(repoName);
    if (early != null) {
      climbers.push({ repoName, early, latest, delta: early - latest });
    }
  });
  climbers.sort((a, b) => b.delta - a.delta);
  const topClimber =
    climbers.length > 0 && climbers[0] && climbers[0].delta > 0 ? climbers[0] : undefined;
  const topFaller =
    climbers.length > 0 &&
    climbers[climbers.length - 1] &&
    climbers[climbers.length - 1]!.delta < 0
      ? climbers[climbers.length - 1]
      : undefined;
  const monthSpanDays = rankPathDates.length;

  // Most-persistent repo across the 30-day window (= days in ranking × mean rank_score).
  const repoPresence = new Map<string, { days: number; sumScore: number; meanScore: number }>();
  for (const r of rankPaths) {
    if (!r.repoName) continue;
    const prev = repoPresence.get(r.repoName) ?? { days: 0, sumScore: 0, meanScore: 0 };
    prev.days += 1;
    prev.sumScore += r.rankScore;
    prev.meanScore = prev.sumScore / prev.days;
    repoPresence.set(r.repoName, prev);
  }
  const rankPathRepos = [...repoPresence.entries()]
    .map(([repoName, v]) => ({ repoName, days: v.days, meanScore: v.meanScore }))
    .sort(
      (a, b) => b.days * b.meanScore - a.days * a.meanScore || b.days - a.days,
    );
  const mostPersistent = rankPathRepos[0];

  // Burst scatter representatives per quadrant (highest rank_score within each).
  const firstByQuadrant: Record<string, BurstStabilityPoint | undefined> = {};
  for (const p of [...burstSnapshot].sort((a, b) => b.rankScore - a.rankScore)) {
    if (!firstByQuadrant[p.quadrant]) firstByQuadrant[p.quadrant] = p;
  }
  const burstShortSpike = firstByQuadrant.short_spike;
  const burstRisingCore = firstByQuadrant.rising_core;
  const burstSteadyCore = firstByQuadrant.steady_core;

  // Top watcher-profile suspects (high bot share) and highest lift.
  const watcherByBot = [...watcherProfile].sort((a, b) => b.botRatio - a.botRatio);
  const watcherByLift = [...watcherProfile]
    .filter((w) => w.watchers >= 30 && w.dominantLift < 1e6)
    .sort((a, b) => b.dominantLift - a.dominantLift);
  const topBotWatcher = watcherByBot[0];
  const topLiftWatcher = watcherByLift[0];

  // DNA outlier highest z.
  const topOutlier = [...dnaOutliers].sort((a, b) => b.zDistance - a.zDistance)[0];

  // Rank delta biggest gainer/loser (latest day vs previous day).
  const deltaSorted = [...rankDeltas].sort((a, b) => b.deltaRankScore - a.deltaRankScore);
  const biggestGainer = deltaSorted[0];
  const biggestLoser = deltaSorted[deltaSorted.length - 1];

  return (
    <PixelPageShell
      title="L1-L3 Repos"
      subtitle="What makes a repo hot, who watches it, and where the risk is."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "Repos" },
      ]}
      tldr={
        <>
          Static view — top driver of "hot" over 30 days is{" "}
          {topHotReason ? (
            <span style={{ color: "var(--accent-positive)" }}>
              {topHotReason.featureName} (d={topHotReason.cohenD.toFixed(2)})
            </span>
          ) : (
            "no attribution yet"
          )}
          . Dynamic view — what separates risers from fallers is{" "}
          {dynRiserSide ? (
            <span style={{ color: "var(--accent-change)" }}>
              {dynRiserSide.featureName} (d={dynRiserSide.cohenD.toFixed(2)})
            </span>
          ) : (
            "not enough slope data"
          )}
          .{" "}
          {topClimber ? (
            <>
              Biggest climber over {monthSpanDays} days:{" "}
              <EntityLink type="repo" id={topClimber.repoName} /> (
              <span style={{ color: "var(--accent-positive)" }}>
                #{topClimber.early} → #{topClimber.latest}, +{topClimber.delta}
              </span>
              ).{" "}
            </>
          ) : null}
          {topAuthenticity ? (
            <>
              Top suspected fake-hot:{" "}
              <EntityLink type="repo" id={topAuthenticity.repoName} /> (composite
              {" "}
              <span style={{ color: "var(--accent-danger)" }}>
                {topAuthenticity.composite.toFixed(2)}
              </span>{" "}
              · via {SIGNAL_LABEL[topAuthenticity.dominantSignal]}).{" "}
            </>
          ) : null}
          Latest-day largest score mover:{" "}
          {topMover ? (
            <>
              <EntityLink type="repo" id={topMover.repoName} /> (Δ{signed(topMover.deltaRankScore, 3)} via{" "}
              {dominantDriverLabel(topMover)})
            </>
          ) : (
            "n/a"
          )}
          .
        </>
      }
    >
      <OfflineSubnav />

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "var(--pixel-space-4)",
          marginBottom: "var(--pixel-space-5)",
        }}
      >
        <PixelKpi label="Ranked repos" value={rankings.length} tone="positive" hint="rows in batch_advanced_repo_rankings" />
        <PixelKpi
          label="Top fake-hot suspect"
          value={topAuthenticity ? topAuthenticity.composite.toFixed(2) : "—"}
          deltaLabel={
            topAuthenticity
              ? `${topAuthenticity.repoName} · ${SIGNAL_LABEL[topAuthenticity.dominantSignal]}`
              : `${totalAlerts} raw alerts`
          }
          tone={topAuthenticity && topAuthenticity.composite >= 0.5 ? "danger" : "change"}
          hint="Composite of 4 suspicion signals, 0..1 higher = more suspicious"
        />
        <PixelKpi
          label="Top-1 bus-factor"
          value={topBusFactor ? formatPercent(topBusFactor.top1ActorShare) : "—"}
          tone="change"
          hint={topBusFactor?.repoName ?? undefined}
        />
        <PixelKpi
          label={attrMode === "dynamic" ? "Rising-side feature" : "Hot-leading feature"}
          value={
            attrMode === "dynamic"
              ? dynRiserSide?.featureName ?? "—"
              : topHotReason?.featureName ?? "—"
          }
          deltaLabel={
            attrMode === "dynamic"
              ? dynRiserSide
                ? `d=${dynRiserSide.cohenD.toFixed(2)} (risers vs fallers)`
                : undefined
              : topHotReason
                ? `d=${topHotReason.cohenD.toFixed(2)} (hot vs cold)`
                : undefined
          }
          tone="info"
        />
      </div>

      {(authenticity.length > 0 || anomalyAlerts.length > 0 || declineWarnings.length > 0) && (
        <PixelSection
          title="Alerts — fused authenticity score + raw signals"
          tone="danger"
          headline={
            topAuthenticity
              ? `Top suspected fake-hot: ${topAuthenticity.repoName} (composite ${topAuthenticity.composite.toFixed(2)}, dominant = ${SIGNAL_LABEL[topAuthenticity.dominantSignal]}). Each composite is the mean of 4 min-max normalised signals; dominant is the single signal with the largest normalised value.`
              : "Anomaly + decline signals merged from the former /offline/risk."
          }
          source="batch_offline_anomaly_alerts ∪ batch_repo_dna_outliers ∪ batch_repo_watcher_profile ∪ batch_repo_community_profile"
          techBadge="4 signals · per-signal min-max normalisation · equal-weight composite"
          howToRead="composite ≥ 0.6 is highly suspicious across multiple axes; 0.35–0.6 is a warning; single tall bar + low composite means one specific suspicion — check the dominant column."
          findings={
            authenticity.length > 0 ? (
              <>
                <p style={{ margin: "0 0 8px" }}>
                  The table ranks repos by a <strong>composite suspicion score</strong>{" "}
                  (0–1, higher = more likely to be "fake-hot"). Behind the score sit four
                  independent signals: volume-anomaly Z, DNA outlier Z, watcher bot
                  ratio, and community bot ratio — all min-max normalised to comparable 0–1
                  scales, then averaged.
                </p>
                <p style={{ margin: "0 0 8px" }}>
                  <strong>Top-3 suspects:</strong>{" "}
                  {authenticity.slice(0, 3).map((r, i) => (
                    <span key={r.repoName}>
                      {i > 0 ? " · " : ""}
                      <EntityLink type="repo" id={r.repoName} /> (
                      <span style={{ color: "var(--accent-danger)" }}>
                        {r.composite.toFixed(2)}
                      </span>{" "}
                      via {SIGNAL_LABEL[r.dominantSignal]})
                    </span>
                  ))}
                  .
                </p>
                <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                  Interpretation: if the dominant signal is <em>watcher_bot</em> / <em>community_bot</em>,
                  the repo looks inorganically "popular" (many bots in its star-gazer pool). If it is
                  <em>{" "}alert_z</em>, there is a one-day volume spike that is far above baseline. If it
                  is <em>dna_outlier</em>, the repo behaves unlike its hot-cohort peers on multiple
                  DNA features at once. A composite above 0.6 means multiple axes agree — investigate first.
                </p>
              </>
            ) : (
              <p style={{ margin: 0 }}>
                No authenticity rows yet — the fused table needs at least one repo that
                appears in any of the four suspicion tables.
              </p>
            )
          }
        >
          {authenticity.length > 0 ? (
            <>
              <AuthenticityTable rows={authenticity.slice(0, 20)} />
              <div
                style={{
                  marginTop: 14,
                  borderTop: "1px dashed var(--divider)",
                  paddingTop: 12,
                }}
              >
                <p style={{ color: "var(--muted)", fontSize: 11, margin: "0 0 6px" }}>
                  Raw single-signal lists (the inputs the composite is built from):
                </p>
                <ul
                  style={{
                    margin: 0,
                    paddingLeft: 18,
                    fontSize: 12,
                    color: "var(--fg)",
                    lineHeight: 1.5,
                  }}
                >
                  {anomalyAlerts.slice(0, 5).map((row) => (
                    <li key={`a-${row.repoName}`}>
                      <EntityLink type="repo" id={row.repoName} /> — volume anomaly{" "}
                      <PixelBadge tone="danger" size="sm">
                        Z={row.zScore.toFixed(2)} · {row.alertLevel}
                      </PixelBadge>
                    </li>
                  ))}
                  {declineWarnings.slice(0, 5).map((row) => (
                    <li key={`d-${row.repoName}`}>
                      <EntityLink type="repo" id={row.repoName} /> — decline{" "}
                      <PixelBadge tone="change" size="sm">
                        slope7={row.slope7.toFixed(2)} · {(row.pctChange7 * 100).toFixed(1)}%
                      </PixelBadge>
                    </li>
                  ))}
                </ul>
              </div>
            </>
          ) : (
            <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: "var(--fg)", lineHeight: 1.5 }}>
              {anomalyAlerts.slice(0, 5).map((row) => (
                <li key={`a-${row.repoName}`}>
                  <EntityLink type="repo" id={row.repoName} /> — volume anomaly{" "}
                  <PixelBadge tone="danger" size="sm">
                    Z={row.zScore.toFixed(2)} · {row.alertLevel}
                  </PixelBadge>
                </li>
              ))}
              {declineWarnings.slice(0, 5).map((row) => (
                <li key={`d-${row.repoName}`}>
                  <EntityLink type="repo" id={row.repoName} /> — decline{" "}
                  <PixelBadge tone="change" size="sm">
                    slope7={row.slope7.toFixed(2)} · {(row.pctChange7 * 100).toFixed(1)}%
                  </PixelBadge>
                </li>
              ))}
            </ul>
          )}
        </PixelSection>
      )}

      <PixelSection
        title="L1 30-day rank trajectory — who climbed, who slid?"
        tone="info"
        headline={
          topClimber && topFaller && topClimber.delta !== topFaller.delta ? (
            <>
              Over {monthSpanDays} days, <EntityLink type="repo" id={topClimber.repoName} /> rose{" "}
              <span style={{ color: "var(--accent-positive)" }}>#{topClimber.early} → #{topClimber.latest}</span>
              {"; "}
              <EntityLink type="repo" id={topFaller.repoName} /> dropped{" "}
              <span style={{ color: "var(--accent-danger)" }}>#{topFaller.early} → #{topFaller.latest}</span>. Toggle
              to rank_score to see the score gap.
            </>
          ) : (
            "Daily rank_no / rank_score for the top-8 repos over the 30-day window."
          )
        }
        source="batch_repo_rank_score_day"
        techBadge="Spark SQL · daily rank_no / rank_score · 30-day window"
        howToRead="rank_no view: lower = better, so a line going DOWN means that repo is climbing. rank_score view: larger = hotter, so a line going UP means that repo is getting hotter. Crossings reveal overtakes, flat plateaus show incumbents holding their position."
        findings={
          rankPathRepos.length > 0 ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                These 8 lines are <strong>the 8 most persistent repos</strong> over the
                30-day window — ranked by{" "}
                <code>days_in_top × mean rank_score</code>, not by the single
                latest-day rank (which would pick one-day wonders that collapse to
                single points).
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Lines in the chart</strong> (click to drill-down):{" "}
                {rankPathRepos.slice(0, 8).map((r, i) => (
                  <span key={r.repoName}>
                    {i > 0 ? " · " : ""}
                    <EntityLink type="repo" id={r.repoName} />{" "}
                    <span style={{ color: "var(--muted)" }}>
                      ({r.days}/{monthSpanDays}d, μ={r.meanScore.toFixed(2)})
                    </span>
                  </span>
                ))}
              </p>
              <p style={{ margin: "0 0 8px" }}>
                {mostPersistent ? (
                  <>
                    <strong>Most persistent:</strong>{" "}
                    <EntityLink type="repo" id={mostPersistent.repoName} /> —{" "}
                    <span style={{ color: "var(--accent-positive)" }}>
                      {mostPersistent.days} / {monthSpanDays} days
                    </span>{" "}
                    on the ranking with an average score of{" "}
                    {mostPersistent.meanScore.toFixed(3)}. Repos that stay this long
                    on the list tend to be the real infrastructure — not one-day
                    hype.{" "}
                  </>
                ) : null}
                {topClimber ? (
                  <>
                    <strong>Biggest climber:</strong>{" "}
                    <EntityLink type="repo" id={topClimber.repoName} /> rose{" "}
                    <span style={{ color: "var(--accent-positive)" }}>
                      #{topClimber.early} → #{topClimber.latest} (+{topClimber.delta})
                    </span>
                    .{" "}
                  </>
                ) : null}
                {topFaller ? (
                  <>
                    <strong>Biggest faller:</strong>{" "}
                    <EntityLink type="repo" id={topFaller.repoName} /> dropped{" "}
                    <span style={{ color: "var(--accent-danger)" }}>
                      #{topFaller.early} → #{topFaller.latest} ({topFaller.delta})
                    </span>
                    .
                  </>
                ) : null}
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: rank at one moment is noisy — a repo only matters if it
                stays on the leaderboard for many days. Use the rank_score toggle
                above when you want absolute intensity instead of relative position.
              </p>
            </>
          ) : (
            <p style={{ margin: 0 }}>
              No persistent repos found in the 30-day window. Run the Spark job to
              regenerate <code>batch_repo_rank_score_day</code>.
            </p>
          )
        }
      >
        <RankPathChart rows={rankPaths} />
      </PixelSection>

      <PixelSection
        title={
          attrMode === "dynamic"
            ? "L2 Attribution · Dynamic — why are risers rising? (30-day slope cohorts)"
            : "L2 Attribution · Static — what makes a repo hot? (30-day forest plot)"
        }
        tone="positive"
        headline={
          attrMode === "dynamic"
            ? hasDynamic && dynRiserSide
              ? `Risers (top-30 positive 30-day rank_score slopes) vs fallers (top-30 negative) — the biggest separating feature is ${dynRiserSide.featureName} (Cohen's d=${dynRiserSide.cohenD.toFixed(2)}, 95% CI [${dynRiserSide.cohensDLow.toFixed(2)}, ${dynRiserSide.cohensDHigh.toFixed(2)}]).`
              : "Not enough slope data yet — need at least 5 days of rank_score history per repo."
            : `Welch's t + Cohen's d on hot vs cold cohorts over the 30-day window (Bootstrap 95% CI, B=200). Toggle cohort scope to strip out bots. ${scopesAvailable.length} scope${scopesAvailable.length === 1 ? "" : "s"} available.`
        }
        source={
          attrMode === "dynamic"
            ? "batch_repo_rank_score_day JOIN batch_repo_dna (in-memory OLS slope + Welch + Cohen's d)"
            : "batch_hot_vs_cold_attribution"
        }
        techBadge={
          attrMode === "dynamic"
            ? "OLS slope · Welch's t · Cohen's d · Bootstrap CI · Risers vs Fallers"
            : "Spark SQL · Welch's t · Cohen's d · Bootstrap CI · 30-day cohorts"
        }
        howToRead={
          attrMode === "dynamic"
            ? "Cohorts are defined by 30-day rank_score trajectory: risers = top-30 positive OLS slopes, fallers = top-30 negative. Same forest plot — X-axis Cohen's d, bar 95% CI. A feature high on the right means risers are more driven by it than fallers; on the left means fallers have more of it."
            : "X-axis = Cohen's d (standardised mean difference). Bar = 95% CI. Point = estimate. Rule of thumb: |d|≈0.2 small · 0.5 medium · 0.8 large. A CI that covers 0 is NOT a robust signal (greyed)."
        }
        findings={
          attrMode === "static" ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                <strong>What this forest plot shows</strong>: for every DNA feature we
                compared the <span style={{ color: "var(--accent-positive)" }}>hot cohort</span>{" "}
                (top-10% by rank_score) against the{" "}
                <span style={{ color: "var(--accent-danger)" }}>cold cohort</span> (bottom-50%)
                on the 30-day window. Each bar = one feature; its position on X is
                the <em>Cohen's d</em> (how many standard deviations the means differ),
                and the bar width is the Bootstrap 95% CI.
              </p>
              {topHotReason && topColdReason ? (
                <p style={{ margin: "0 0 8px" }}>
                  <strong>Result (this dataset):</strong> the single biggest
                  hot-leading feature is <code>{topHotReason.featureName}</code>{" "}
                  (d={topHotReason.cohenD.toFixed(2)}, CI [{topHotReason.cohensDLow.toFixed(2)},{" "}
                  {topHotReason.cohensDHigh.toFixed(2)}]) — hot repos sit about{" "}
                  {Math.abs(topHotReason.cohenD).toFixed(1)} σ above cold repos on it.
                  The biggest cold-leading feature is{" "}
                  <code>{topColdReason.featureName}</code> (d={topColdReason.cohenD.toFixed(2)}) —
                  cold repos are disproportionately heavy on it.
                </p>
              ) : null}
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: if you want a "what makes a hot repo hot" checklist,
                these bars are the ranking. A very negative bar (e.g. bot_ratio
                strongly on the cold side) is just as informative — it tells you
                what hot repos <em>don't</em> have.
              </p>
            </>
          ) : (
            <>
              <p style={{ margin: "0 0 8px" }}>
                <strong>What this forest plot shows</strong>: we re-cohort by
                30-day <em>rank_score trajectory</em>. For every repo we fit an OLS
                slope over its daily score, then pick the 30 most positive slopes
                (<span style={{ color: "var(--accent-positive)" }}>risers</span>) and
                the 30 most negative (<span style={{ color: "var(--accent-danger)" }}>fallers</span>).
                Same statistics as the static plot — Welch's t, Cohen's d, Bootstrap CI.
              </p>
              {dynRiserSide && dynFallerSide ? (
                <p style={{ margin: "0 0 8px" }}>
                  <strong>Result:</strong> the feature that most separates risers
                  from fallers is <code>{dynRiserSide.featureName}</code>{" "}
                  (d={dynRiserSide.cohenD.toFixed(2)}, CI [{dynRiserSide.cohensDLow.toFixed(2)},{" "}
                  {dynRiserSide.cohensDHigh.toFixed(2)}], n={dynRiserSide.nHot} vs {dynRiserSide.nCold}).
                  The strongest fallers-only feature is{" "}
                  <code>{dynFallerSide.featureName}</code> (d={dynFallerSide.cohenD.toFixed(2)}).
                </p>
              ) : (
                <p style={{ margin: "0 0 8px" }}>
                  Not enough slope data — need at least 5 days of daily rank_score per
                  repo. Run more of the 30-day window first.
                </p>
              )}
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: the <em>static</em> view answers "what does a hot repo
                look like right now?", the <em>dynamic</em> view answers "what is
                pushing a repo to keep rising?". If the top feature differs between
                the two, that's the clue — some features correlate with hotness
                but don't cause momentum. Toggle back-and-forth to compare.
              </p>
            </>
          )
        }
      >
        <div
          style={{
            display: "flex",
            gap: 6,
            marginBottom: 10,
            alignItems: "center",
            flexWrap: "wrap",
          }}
        >
          <span style={{ color: "var(--muted)", fontSize: 11 }}>Attribution mode:</span>
          <button
            type="button"
            onClick={() => setAttrMode("static")}
            className="nes-btn"
            style={{
              padding: "2px 10px",
              fontSize: 11,
              background:
                attrMode === "static" ? "var(--accent-positive)" : undefined,
              color: attrMode === "static" ? "#0a0a0a" : undefined,
            }}
          >
            Static · hot vs cold
          </button>
          <button
            type="button"
            onClick={() => setAttrMode("dynamic")}
            className="nes-btn"
            disabled={!hasDynamic}
            style={{
              padding: "2px 10px",
              fontSize: 11,
              background:
                attrMode === "dynamic" ? "var(--accent-change)" : undefined,
              color: attrMode === "dynamic" ? "#0a0a0a" : undefined,
              opacity: hasDynamic ? 1 : 0.4,
            }}
            title={
              hasDynamic
                ? "Re-cohort by 30-day rank_score slope direction."
                : "Not enough daily rank history for slope cohorts."
            }
          >
            Dynamic · risers vs fallers
          </button>
          {attrMode === "dynamic" && dynRiserSide ? (
            <span style={{ color: "var(--muted-strong)", fontSize: 11, marginLeft: 10 }}>
              n = {dynRiserSide.nHot} risers · {dynRiserSide.nCold} fallers
            </span>
          ) : null}
        </div>

        {activeAttribution.length > 0 ? (
          <AttributionForestPlot rows={activeAttribution} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>
            {attrMode === "dynamic"
              ? "No dynamic attribution yet."
              : "No attribution rows yet."}
          </p>
        )}

        {attrMode === "static" && topHotReason && topColdReason ? (
          <p style={{ color: "var(--muted-strong)", fontSize: 11, marginTop: 12 }}>
            <span style={{ color: "var(--accent-positive)" }}>Hot</span> lean →{" "}
            <code>{topHotReason.featureName}</code>{" "}
            (d={topHotReason.cohenD.toFixed(2)}, 95% CI [{topHotReason.cohensDLow.toFixed(2)}, {topHotReason.cohensDHigh.toFixed(2)}])
            · <span style={{ color: "var(--accent-danger)" }}>Cold</span> lean →{" "}
            <code>{topColdReason.featureName}</code>{" "}
            (d={topColdReason.cohenD.toFixed(2)}).
          </p>
        ) : null}

        {attrMode === "dynamic" && dynRiserSide && dynFallerSide ? (
          <p style={{ color: "var(--muted-strong)", fontSize: 11, marginTop: 12 }}>
            <span style={{ color: "var(--accent-positive)" }}>Risers</span> lean →{" "}
            <code>{dynRiserSide.featureName}</code>{" "}
            (d={dynRiserSide.cohenD.toFixed(2)}, 95% CI [{dynRiserSide.cohensDLow.toFixed(2)}, {dynRiserSide.cohensDHigh.toFixed(2)}])
            · <span style={{ color: "var(--accent-danger)" }}>Fallers</span> lean →{" "}
            <code>{dynFallerSide.featureName}</code>{" "}
            (d={dynFallerSide.cohenD.toFixed(2)}). This decouples{" "}
            <em>what makes a repo hot right now</em> from{" "}
            <em>what drives it to keep rising</em> — compare the two modes to
            see whether snapshot hotness features also predict momentum.
          </p>
        ) : null}
      </PixelSection>

      <PixelSection
        title="L2 Repo DNA — 14-feature fingerprint (cohort averages + leaderboard tail)"
        tone="info"
        headline="Cohort means first (so the shape of a typical hot / mid / cold repo is obvious), then the 10 rank-score leaders so the pathology of the leaderboard is visible."
        source="batch_repo_dna"
        techBadge="Spark SQL · feature engineering · cohort aggregation"
        findings={(() => {
          const hotMean = dnaCohortStats.find((s) => s.cohortGroup === "hot");
          const midMean = dnaCohortStats.find((s) => s.cohortGroup === "mid");
          const coldMean = dnaCohortStats.find((s) => s.cohortGroup === "cold");
          const allZeroDiversity =
            dnaTopHot.length > 0 &&
            dnaTopHot.filter((r) => r.eventEntropy < 0.01).length / dnaTopHot.length >= 0.8;
          return (
            <>
              <p style={{ margin: "0 0 8px" }}>
                Every cell is a <em>fraction of that repo&apos;s total events</em>, so each
                row sums to roughly 1 across the share columns. Two tables below, same
                schema, answering two different questions:
              </p>
              <ol style={{ margin: "0 0 8px 18px", padding: 0 }}>
                <li style={{ margin: "0 0 4px" }}>
                  <strong>Top table — cohort averages.</strong> What does a <em>typical</em>{" "}
                  hot / mid / cold repo look like across {(dnaCohortStats.reduce((s, c) => s + c.repos, 0)).toLocaleString()}{" "}
                  repos? This is the DNA baseline you should compare anything against.
                </li>
                <li style={{ margin: 0 }}>
                  <strong>Bottom table — 10 leaderboard winners.</strong> The 10 hot-cohort
                  repos with the highest <code>rank_score</code>. In a healthy ranking these
                  should look roughly like the hot-cohort mean; in this data most of them{" "}
                  <strong>don&apos;t</strong>.
                </li>
              </ol>
              {hotMean && coldMean ? (
                <p style={{ margin: "0 0 8px", color: "var(--muted-strong)" }}>
                  <strong>The story in one line:</strong> the average <em>hot</em> repo mixes
                  watchers ({formatPercent(hotMean.watchShare)}) + commits ({formatPercent(hotMean.pushShare)})
                  with event entropy <strong>H≈{hotMean.eventEntropy.toFixed(2)}</strong> and
                  bus-factor top-1 share <strong>{formatPercent(hotMean.top1ActorShare)}</strong>.
                  The average <em>cold</em> repo is a bot-authored monoculture — bot ratio{" "}
                  <strong>{formatPercent(coldMean.botRatio)}</strong>, top-1 share{" "}
                  <strong>{formatPercent(coldMean.top1ActorShare)}</strong>.
                  {allZeroDiversity ? (
                    <>
                      {" "}But the 10 rank-score leaders (below) have <strong>H ≈ 0</strong>{" "}
                      and <strong>top-1 ≈ 100%</strong> — the exact fingerprint of the cold
                      cohort. <strong>Sorting by rank_score alone surfaces push-spam from
                      single-actor bot farms</strong>, not real hot repos; this is the
                      motivation for the Authenticity / outlier sections above.
                    </>
                  ) : null}
                </p>
              ) : null}
              {midMean ? (
                <p style={{ margin: 0, color: "var(--muted)" }}>
                  Mid-cohort mean for reference: push {formatPercent(midMean.pushShare)} ·
                  watch {formatPercent(midMean.watchShare)} · bot{" "}
                  {formatPercent(midMean.botRatio)} · H={midMean.eventEntropy.toFixed(2)}.
                  &nbsp;Most of GitHub lives here — small personal projects with one
                  contributor pushing code.
                </p>
              ) : null}
            </>
          );
        })()}
      >
        {/* ——— cohort-average mini table ——— */}
        {dnaCohortStats.length > 0 ? (
          <div
            style={{
              border: "1px solid var(--divider)",
              background: "var(--bg)",
              marginBottom: 12,
              overflowX: "auto",
            }}
          >
            <table suppressHydrationWarning style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
              <thead>
                <tr
                  style={{
                    color: "var(--muted)",
                    borderBottom: "1px solid var(--divider)",
                    textAlign: "right",
                  }}
                >
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>cohort mean</th>
                  <th style={{ padding: "6px 8px" }}>repos</th>
                  <th style={{ padding: "6px 8px" }}>avg events/repo</th>
                  <th style={{ padding: "6px 8px" }}>watch</th>
                  <th style={{ padding: "6px 8px" }}>fork</th>
                  <th style={{ padding: "6px 8px" }}>issues</th>
                  <th style={{ padding: "6px 8px" }}>pr</th>
                  <th style={{ padding: "6px 8px" }}>push</th>
                  <th style={{ padding: "6px 8px" }}>bot</th>
                  <th style={{ padding: "6px 8px" }}>night</th>
                  <th style={{ padding: "6px 8px" }}>top1</th>
                  <th style={{ padding: "6px 8px" }}>H</th>
                </tr>
              </thead>
              <tbody>
                {dnaCohortStats.map((c) => {
                  const color =
                    c.cohortGroup === "hot"
                      ? "var(--accent-positive)"
                      : c.cohortGroup === "mid"
                        ? "var(--accent-info)"
                        : "var(--accent-danger)";
                  return (
                    <tr key={c.cohortGroup} style={{ borderBottom: "1px dashed var(--divider)" }}>
                      <td style={{ padding: "6px 8px", color, fontWeight: 700 }}>
                        {c.cohortGroup}
                      </td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>
                        {c.repos.toLocaleString()}
                      </td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>
                        {c.avgEvents.toFixed(1)}
                      </td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.watchShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.forkShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.issuesShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.prShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.pushShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.botRatio)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.nightRatio)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{formatPercent(c.top1ActorShare)}</td>
                      <td style={{ padding: "6px 8px", textAlign: "right" }}>{c.eventEntropy.toFixed(2)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}

        {/* ——— leaderboard-tail table (the original 10 rows) ——— */}
        <div
          style={{
            fontSize: 10,
            letterSpacing: 2,
            color: "var(--muted)",
            marginBottom: 6,
            fontWeight: 700,
          }}
        >
          TOP-10 HOT-COHORT REPOS BY RANK_SCORE &nbsp;·&nbsp; compare each row against the
          &ldquo;hot&rdquo; mean above
        </div>
        <PixelSearchTable
          csvFilename="repo_dna.csv"
          rows={dnaTopHot}
          columns={dnaColumns}
          getRowKey={(r) => r.repoName}
          initialSort={{ key: "watchShare", desc: true }}
          pageSize={10}
          searchPlaceholder="filter repo..."
          fontSize={11}
        />

        {/* ——— DataReading: what does this actually tell us ——— */}
        {(() => {
          const hotMean = dnaCohortStats.find((s) => s.cohortGroup === "hot");
          if (!hotMean) return null;
          const degenerate = dnaTopHot.filter(
            (r) => r.eventEntropy < 0.05 && r.top1ActorShare > 0.95,
          ).length;
          return (
            <div
              style={{
                marginTop: 12,
                padding: "10px 12px",
                borderLeft: "3px solid var(--accent-info)",
                background: "var(--surface-elevated, rgba(255,255,255,0.02))",
                fontSize: 11,
                lineHeight: 1.6,
                color: "var(--fg)",
              }}
            >
              <div
                style={{
                  color: "var(--accent-info)",
                  fontSize: 10,
                  letterSpacing: 2,
                  fontWeight: 700,
                  marginBottom: 4,
                }}
              >
                {"==> READING THE TWO TABLES"}
              </div>
              <div>
                <strong>Why values look &ldquo;all 0% or all 100%&rdquo; in the bottom table:</strong>{" "}
                {degenerate} of {dnaTopHot.length} leaderboard rows have
                <code> entropy&nbsp;H ≈ 0</code> and <code>top1_actor_share ≈ 100%</code>.
                Translated: one actor pushed thousands of commits in a row and nothing else
                happened — no stars, no forks, no issues, no PRs. A DNA fingerprint with 100%
                push, 100% top-1 and 0 entropy is the <em>canonical signature of a push-spam
                farm</em>, not a popular project.
              </div>
              <div style={{ marginTop: 4 }}>
                <strong>What the cohort means reveal:</strong> the <em>real</em> hot cohort
                (22k+ repos, top row) averages watch {formatPercent(hotMean.watchShare)},
                entropy <strong>H={hotMean.eventEntropy.toFixed(2)}</strong>, top-1 share{" "}
                {formatPercent(hotMean.top1ActorShare)} — genuinely diverse, multi-actor
                activity. The leaderboard tail doesn&apos;t look like that <em>at all</em>.
                This mismatch is exactly what the <strong>DNA outlier z-distance</strong>{" "}
                section below ranks: repos that are &ldquo;hot&rdquo; by score but off-axis
                from the fingerprint their peers share.
              </div>
              <div style={{ marginTop: 4, color: "var(--muted)" }}>
                <strong>Real-world takeaway:</strong> raw event-count rankings on GitHub are
                trivially gameable — a scripted actor can push 30k commits in a week and
                dominate every leaderboard that treats &ldquo;volume&rdquo; as signal. Any
                production analytic has to either (a) cap per-repo top-1 share, (b) floor on
                entropy H, or (c) blend watch/fork/issues/PR into the score so a single push
                loop can&apos;t win. This table is the visible evidence for doing so.
              </div>
              <div style={{ marginTop: 6, color: "var(--muted)", fontSize: 10 }}>
                Column cheat-sheet: <code>watch</code>/<code>fork</code>/<code>issues</code>/<code>pr</code>/<code>push</code>{" "}
                are event-type shares. <code>bot</code> = events attributed to automation
                accounts. <code>night</code> = 22:00-06:00 share. <code>top1</code> = share
                of the single most active actor (bus-factor proxy). <code>H</code> = Shannon
                entropy across event types (max ≈ 2.08 for 8 evenly-used types; 0 = single
                type).
              </div>
            </div>
          );
        })()}
      </PixelSection>

      <PixelSection
        title="L2 Counter-examples — hot repos that don't fit the mould"
        tone="danger"
        headline={`Hot cohort members ranked by Mahalanobis-style z-distance to the hot-cohort mean. These ${dnaOutliers.length} repos are "hot for a different reason".`}
        source="batch_repo_dna_outliers"
        techBadge="Spark SQL · z-standardisation · feature off-axis ranking"
        howToRead='off-axis column shows the top-3 most extreme features with sign and |z|. e.g. "bot_ratio:+3.2" = bot_ratio 3.2 σ above hot cohort mean.'
        findings={
          topOutlier ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Sorted by z-distance</strong> — a single number that measures
                how far a repo's 14-feature DNA fingerprint is from the{" "}
                <em>average hot repo</em> (in σ-units summed across features). A high
                z-distance means the repo is on the leaderboard but got there through
                a weird feature mix, so it deserves scrutiny.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Top outlier:</strong>{" "}
                <EntityLink type="repo" id={topOutlier.repoName} /> with
                z-distance ={" "}
                <span style={{ color: "var(--accent-danger)" }}>
                  {topOutlier.zDistance.toFixed(2)}
                </span>{" "}
                and off-axis signature{" "}
                <code style={{ fontSize: 11 }}>{topOutlier.offFeatures}</code>. Read
                the signature like this: <em>bot_ratio:+10.56</em> means this repo's
                bot ratio is 10.56 σ above the hot-cohort average — a clear red flag.
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: these repos are "hot" by the ranker but <em>not</em> by
                typical-hot-repo fingerprint. Most either have artificially inflated
                stars (bot_ratio positive outliers), unusual discussion/PR shares, or
                extreme activity bursts. Pair this table with the Alerts / Authenticity
                section above — a repo appearing in both is very likely non-organic.
              </p>
            </>
          ) : null
        }
      >
        {dnaOutliers.length > 0 ? (
          <PixelSearchTable
            csvFilename="dna_outliers.csv"
            rows={dnaOutliers}
            columns={outlierColumns}
            getRowKey={(r) => r.repoName}
            initialSort={{ key: "zDistance", desc: true }}
            pageSize={12}
            searchPlaceholder="filter repo..."
          />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No outliers available.</p>
        )}
      </PixelSection>

      <PixelSection
        title="L3 Watcher profile — who gathers around hot repos? (30-day)"
        tone="change"
        headline="Dominant GMM persona + lift(persona | watcher) / share(persona | global), computed over 30-day watcher populations."
        source="batch_repo_watcher_profile · batch_repo_watcher_persona_lift"
        techBadge="Spark SQL · GMM · lift analysis"
        findings={
          watcherProfile.length > 0 ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                Sorted by <strong>dominant lift</strong> —{" "}
                lift = <code>P(persona | watchers of this repo) / P(persona | any GitHub user)</code>.
                A lift of 2× means this repo's watchers are twice as likely to fit that
                persona than the global population would suggest.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Columns:</strong>{" "}
                <code>watchers</code> — distinct watchers in 30 days ·{" "}
                <code>persona</code> — most common GMM cluster among them ·{" "}
                <code>lift</code> — how strong that concentration is (huge lift typically means a tiny audience all in one cluster) ·{" "}
                <code>night / pr/push / avg repos</code> — the behavioural averages of
                this repo's watchers ·{" "}
                <code>bot</code> — bot share of the watcher pool.
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                {topBotWatcher ? (
                  <>
                    <strong>Highest-bot watcher pool</strong> (≥30 watchers):{" "}
                    <EntityLink type="repo" id={topBotWatcher.repoName} /> at{" "}
                    {formatPercent(topBotWatcher.botRatio)} bots — means 1 in{" "}
                    {Math.max(1, Math.round(1 / Math.max(topBotWatcher.botRatio, 0.0001)))}{" "}
                    watchers is automation.{" "}
                  </>
                ) : null}
                {topLiftWatcher ? (
                  <>
                    <strong>Highest real-persona lift</strong> (excludes explosive
                    tiny-pool noise):{" "}
                    <EntityLink type="repo" id={topLiftWatcher.repoName} /> —{" "}
                    {topLiftWatcher.dominantLift.toFixed(2)}× over baseline on persona{" "}
                    <code>{topLiftWatcher.dominantPersona}</code>. That means its
                    audience is disproportionately this kind of developer.
                  </>
                ) : null}
              </p>
            </>
          ) : null
        }
      >
        <PixelSearchTable
          csvFilename="watcher_profile.csv"
          rows={watcherProfile}
          columns={watcherColumns}
          getRowKey={(r) => r.repoName}
          initialSort={{ key: "dominantLift", desc: true }}
          pageSize={15}
          searchPlaceholder="filter repo..."
        />
      </PixelSection>

      <PixelSection
        title="Burst vs stability — four personalities of a hot repo"
        tone="change"
        headline={(() => {
          const byQ: Record<string, number> = {};
          for (const p of burstSnapshot) byQ[p.quadrant] = (byQ[p.quadrant] ?? 0) + 1;
          const total = burstSnapshot.length;
          const ss = byQ.short_spike ?? 0;
          const rc = byQ.rising_core ?? 0;
          const sc = byQ.steady_core ?? 0;
          return total > 0
            ? `Top-${total} repos by rank_score, colored by burst/stability quadrant: ${ss} short-spike (fast + fragile), ${rc} rising-core (fast + locked-in), ${sc} steady-core (slow + locked-in). Dashed lines mark the cutoffs burst=2 and stability=0.6.`
            : "Quadrant view of where each repo sits — bursty + unstable vs steady growers.";
        })()}
        source="batch_repo_burst_stability"
        techBadge="Spark · 7-day rolling burst_index (sum of daily rank jumps) + stability_index (fraction of days in top-1000) · quadrant rule"
        howToRead="X-axis = stability (how many days the repo stays in top-1000 over the 7-day window, 0-1). Y-axis = burst (sum of one-day rank-ladder jumps). Top-left = short_spike (one-off viral hit, rarely sustains). Top-right = rising_core (fast climber that is also sticky — the real rising stars). Bottom-right = steady_core (old incumbent, slow but sticky). Bottom-left = long_tail (quiet repos, not interesting here). Color = quadrant, circle size = rank_score."
        findings={
          burstSnapshot.length > 0 ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                Four quadrants give a repo a "personality":{" "}
                <span style={{ color: "#ff5252" }}>short_spike</span> (red) = one-day
                viral hit, rarely sustains ·{" "}
                <span style={{ color: "#ffb020" }}>rising_core</span> (orange) = fast
                climber that is already sticky — the real rising stars ·{" "}
                <span style={{ color: "#33ccff" }}>steady_core</span> (blue) = slow
                but dependable incumbents ·{" "}
                <span style={{ color: "#888" }}>long_tail</span> (grey) = low activity,
                ignore.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Representatives in this snapshot</strong>:{" "}
                {burstShortSpike ? (
                  <>
                    short_spike →{" "}
                    <EntityLink type="repo" id={burstShortSpike.repoName} /> (burst{" "}
                    {burstShortSpike.burstIndex.toFixed(1)}, stability{" "}
                    {burstShortSpike.stabilityIndex.toFixed(2)}){" "}
                  </>
                ) : null}
                {burstRisingCore ? (
                  <>
                    · rising_core →{" "}
                    <EntityLink type="repo" id={burstRisingCore.repoName} /> (burst{" "}
                    {burstRisingCore.burstIndex.toFixed(1)}, stability{" "}
                    {burstRisingCore.stabilityIndex.toFixed(2)}){" "}
                  </>
                ) : null}
                {burstSteadyCore ? (
                  <>
                    · steady_core →{" "}
                    <EntityLink type="repo" id={burstSteadyCore.repoName} /> (burst{" "}
                    {burstSteadyCore.burstIndex.toFixed(1)}, stability{" "}
                    {burstSteadyCore.stabilityIndex.toFixed(2)})
                  </>
                ) : null}
                .
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: do not treat one-day viral hits (red) and sticky climbers
                (orange) as the same thing — they need different follow-ups.
                <em> rising_core</em> is the most valuable quadrant for discovery;
                <em> short_spike</em> is where to look for hype / fake-viral; and{" "}
                <em>steady_core</em> is the "old reliable" shortlist.
              </p>
            </>
          ) : null
        }
      >
        <BurstScatterChart
          data={burstSnapshot.map((item) => ({
            repoName: item.repoName,
            burstIndex: item.burstIndex,
            stabilityIndex: item.stabilityIndex,
            rankScore: item.rankScore,
            quadrant: item.quadrant,
          }))}
        />
      </PixelSection>

      <PixelSection
        title="Latest-day score movers (day-over-day)"
        tone="info"
        headline="Which repos moved most by rank_score between the two most recent days, and the dominant driver. For longer trajectories see the 30-day rank chart above."
        source="batch_repo_rank_delta_explain_latest"
        findings={
          rankDeltas.length > 0 ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                Sorted by <strong>Δ rank_score</strong> (today − yesterday) to
                answer: "who moved, and why?". Behind each repo the day-over-day
                change is decomposed into 5 additive parts: hotness, momentum,
                engagement, stability, bot penalty. The one with the largest
                absolute contribution is shown as the <code>driver</code> badge.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Biggest gainer today:</strong>{" "}
                {biggestGainer ? (
                  <>
                    <EntityLink type="repo" id={biggestGainer.repoName} /> (Δ
                    <span style={{ color: "var(--accent-positive)" }}>
                      {signed(biggestGainer.deltaRankScore, 3)}
                    </span>
                    , driver = {dominantDriverLabel(biggestGainer)})
                  </>
                ) : (
                  "none"
                )}
                . <strong>Biggest loser:</strong>{" "}
                {biggestLoser && biggestLoser !== biggestGainer ? (
                  <>
                    <EntityLink type="repo" id={biggestLoser.repoName} /> (Δ
                    <span style={{ color: "var(--accent-danger)" }}>
                      {signed(biggestLoser.deltaRankScore, 3)}
                    </span>
                    , driver = {dominantDriverLabel(biggestLoser)})
                  </>
                ) : (
                  "none"
                )}
                .
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                Take-away: if the driver is <em>momentum</em>, the change is
                because growth accelerated (not because total events increased);{" "}
                <em>hotness</em> means raw event volume went up;{" "}
                <em>engagement</em> means the mix tilted toward PRs/issues;{" "}
                <em>stability</em> means the 7-day sticky-factor changed;{" "}
                <em>bot penalty</em> means the bot share is actively dragging the
                score down. This is a per-repo version of the Attribution forest
                plot — but at daily granularity.
              </p>
            </>
          ) : null
        }
      >
        <PixelSearchTable
          rows={rankDeltas}
          columns={deltaColumns}
          getRowKey={(r) => r.repoName}
          initialSort={{ key: "deltaRankScore", desc: true }}
          pageSize={10}
          searchPlaceholder="filter repo..."
        />
      </PixelSection>

      <PixelSection
        title="Drill-down — full rankings"
        tone="positive"
        headline="Click a name to open the right-side analysis drawer."
        source="batch_advanced_repo_rankings"
        findings={
          rankings.length > 0 ? (
            <>
              <p style={{ margin: "0 0 8px" }}>
                Master leaderboard. Sorted by <strong>rank_score</strong> (composite
                0–1 hotness score) by default — click <code>rank</code> to switch to
                ordinal position, <code>trend</code> to group by the arrow tag, or
                the repo name to sort alphabetically.
              </p>
              <p style={{ margin: 0, color: "var(--muted-strong)" }}>
                The <code>trend</code> column reflects the snapshot-to-snapshot
                movement used by the ranker (new / accelerating / slowing / stable).
                Use the search box for quick lookups and the CSV button to export.
                Every repo row links to a detailed drill-down page with DNA bars,
                watchers, and anomaly signals.
              </p>
            </>
          ) : null
        }
      >
        <PixelSearchTable
          csvFilename="repo_rankings.csv"
          rows={rankings}
          columns={rankingColumns}
          getRowKey={(r) => r.repoName}
          initialSort={{ key: "rankScore", desc: true }}
          pageSize={12}
          searchPlaceholder="filter repo..."
        />
      </PixelSection>
    </PixelPageShell>
  );
}
