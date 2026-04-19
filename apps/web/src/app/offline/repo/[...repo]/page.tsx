/* eslint-disable react/no-unescaped-entities -- narrative prose */
import { AdvancedTrendChart } from "@/components/advanced-trend-chart";
import { DashboardCharts } from "@/components/dashboard-charts";
import { EntityLink } from "@/components/entity";
import { OfflineSubnav } from "@/components/offline-subnav";
import { PixelBadge } from "@/components/pixel";
import { PixelPageShell } from "@/components/pixel-shell";
import {
  getRepoAssociationRulesByRepo,
  getRepoClusterByRepo,
  getRepoClusterProfileLatest,
  getRepoCommunityByRepo,
  getRepoContributorConcentrationByRepo,
  getRepoDnaByRepo,
  getRepoEventMixShiftByRepo,
  getRepoEventTypeShareByRepo,
  getRepoHotnessComponentsByRepo,
  getRepoRankHistoryByRepo,
  getRepoSimilarityEdgesByRepo,
  getRepoTopActorsLatestByRepo,
  getRepoTrendForecastByRepo,
  getRepoWatcherPersonaLiftByRepo,
  getRepoWatcherProfileByRepo,
} from "@/lib/dashboard";

function shortLabel(value: string, maxLength = 18): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 3)}...`;
}

export default async function OfflineRepoDetailPage({
  params,
}: {
  params: Promise<{ repo: string[] }>;
}) {
  const { repo } = await params;
  const repoName = repo.join("/");

  const [
    rankHistory,
    trendForecast,
    hotnessComponents,
    topActors,
    contributorConcentration,
    eventTypeShares,
    mixShift,
    clusterAssignment,
    clusterProfiles,
    dna,
    watcherProfile,
    watcherLifts,
    community,
    similarEdges,
    assocRules,
  ] = await Promise.all([
    getRepoRankHistoryByRepo(repoName, 30),
    getRepoTrendForecastByRepo(repoName, 30),
    getRepoHotnessComponentsByRepo(repoName, 14),
    getRepoTopActorsLatestByRepo(repoName, 10),
    getRepoContributorConcentrationByRepo(repoName, 14),
    getRepoEventTypeShareByRepo(repoName, 14),
    getRepoEventMixShiftByRepo(repoName, 14),
    getRepoClusterByRepo(repoName).catch(() => null),
    getRepoClusterProfileLatest().catch(() => []),
    getRepoDnaByRepo(repoName).catch(() => null),
    getRepoWatcherProfileByRepo(repoName).catch(() => null),
    getRepoWatcherPersonaLiftByRepo(repoName).catch(() => []),
    getRepoCommunityByRepo(repoName).catch(() => null),
    getRepoSimilarityEdgesByRepo(repoName, 10).catch(() => []),
    getRepoAssociationRulesByRepo(repoName, 10).catch(() => []),
  ]);

  const clusterLabel = clusterAssignment
    ? clusterProfiles.find((p) => p.clusterId === clusterAssignment.clusterId)?.clusterLabel ?? null
    : null;

  const scoreSeries = rankHistory.map((row) => ({
    label: row.metricDate.slice(5),
    value: Number(row.rankScore.toFixed(4)),
  }));

  const trendData = trendForecast.map((item) => ({
    label: item.metricDate.slice(5),
    actual: item.totalEvents,
    ma7: item.ma7,
    forecast: item.forecastNextDay,
  }));

  const componentShare = hotnessComponents[0]
    ? [
        { label: "watch", value: hotnessComponents[0].watchContribution },
        { label: "fork", value: hotnessComponents[0].forkContribution },
        { label: "issues", value: hotnessComponents[0].issuesContribution },
        { label: "pr", value: hotnessComponents[0].pullRequestContribution },
        { label: "push", value: hotnessComponents[0].pushContribution },
      ].map((x) => ({ label: x.label, value: Math.round(x.value * 1000) / 10 }))
    : [];

  const latestConcentration = contributorConcentration[contributorConcentration.length - 1];
  const worstDrift = [...mixShift].sort((a, b) => b.jsDivergence - a.jsDivergence)[0];

  const topActorShareData = topActors.map((row) => ({
    label: `${shortLabel(row.actorLogin, 14)}${row.actorCategory === "bot" ? " (bot)" : ""}`,
    value: Number((row.share * 100).toFixed(2)),
  }));

  const jsDriftSeries = mixShift.map((row) => ({
    label: row.metricDate.slice(5),
    value: Number(row.jsDivergence.toFixed(6)),
  }));

  const shareByType = new Map<string, { label: string; value: number }[]>();
  for (const row of eventTypeShares) {
    const key = row.eventType;
    const series = shareByType.get(key) ?? [];
    series.push({ label: row.metricDate.slice(5), value: Number((row.share * 100).toFixed(2)) });
    shareByType.set(key, series);
  }
  const eventTypesSorted = [...shareByType.entries()]
    .map(([eventType, series]) => ({
      eventType,
      latest: series[series.length - 1]?.value ?? 0,
    }))
    .sort((a, b) => b.latest - a.latest)
    .slice(0, 4)
    .map((x) => x.eventType);

  return (
    <PixelPageShell
      title={`Repo · ${shortLabel(repoName, 40)}`}
      subtitle="Drill-down: trend, score, actors, event-mix drift + L1-L5 context."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "Repos", href: "/offline/repos" },
        { label: shortLabel(repoName, 30) },
      ]}
      tldr={
        <>
          Rank score{" "}
          <span style={{ color: "var(--accent-positive)" }}>
            {rankHistory[rankHistory.length - 1]?.rankScore?.toFixed(3) ?? "n/a"}
          </span>
          {dna ? (
            <>
              {" · "}
              <PixelBadge tone={dna.cohortGroup === "hot" ? "positive" : dna.cohortGroup === "cold" ? "danger" : "change"} size="sm">
                {dna.cohortGroup}
              </PixelBadge>
            </>
          ) : null}
          {clusterLabel ? (
            <>
              {" · "}
              <PixelBadge tone="info" size="sm">
                cluster {clusterLabel}
              </PixelBadge>
            </>
          ) : null}
          {" · "}
          <a
            href={`https://github.com/${repoName}`}
            target="_blank"
            rel="noreferrer noopener"
            style={{ color: "var(--accent-info)" }}
          >
            Open on GitHub »
          </a>
        </>
      }
    >
      <OfflineSubnav />

      <section className="nes-container with-title is-dark" style={{ marginBottom: 16, minWidth: 0 }}>
        <p className="title" style={{ color: "var(--green)" }}>
          Where this repo sits (L1–L5 context)
        </p>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
            gap: 12,
            fontSize: 12,
          }}
        >
          <div>
            <p style={{ color: "var(--muted)", margin: 0 }}>L1 Cluster</p>
            {clusterAssignment ? (
              <p style={{ color: "var(--fg)", margin: "4px 0 0" }}>
                #{clusterAssignment.clusterId}
                {clusterLabel ? ` · ${clusterLabel}` : ""} &nbsp;
                <span style={{ color: "var(--muted)" }}>
                  (PCA {clusterAssignment.pcaX.toFixed(2)}, {clusterAssignment.pcaY.toFixed(2)})
                </span>
              </p>
            ) : (
              <p style={{ color: "var(--muted)", margin: "4px 0 0" }}>n/a</p>
            )}
          </div>
          <div>
            <p style={{ color: "var(--muted)", margin: 0 }}>L2 Cohort group (hot/warm/cold)</p>
            {dna ? (
              <p
                style={{
                  color:
                    dna.cohortGroup === "hot"
                      ? "var(--green)"
                      : dna.cohortGroup === "cold"
                      ? "var(--red, #ff6666)"
                      : "var(--fg)",
                  margin: "4px 0 0",
                }}
              >
                {dna.cohortGroup} · rank_score {dna.rankScore.toFixed(3)} · total events{" "}
                {dna.totalEvents.toLocaleString()}
              </p>
            ) : (
              <p style={{ color: "var(--muted)", margin: "4px 0 0" }}>n/a</p>
            )}
          </div>
          <div>
            <p style={{ color: "var(--muted)", margin: 0 }}>L3 Watcher profile</p>
            {watcherProfile ? (
              <p style={{ color: "var(--fg)", margin: "4px 0 0" }}>
                {watcherProfile.watchers} actors · dominant{" "}
                <strong>{watcherProfile.dominantPersona}</strong> (lift {watcherProfile.dominantLift.toFixed(2)}×) · night{" "}
                {(watcherProfile.avgNightRatio * 100).toFixed(0)}% · pr/push{" "}
                {(watcherProfile.avgPrPushRatio * 100).toFixed(0)}%
              </p>
            ) : (
              <p style={{ color: "var(--muted)", margin: "4px 0 0" }}>n/a (not a hot repo?)</p>
            )}
          </div>
          <div>
            <p style={{ color: "var(--muted)", margin: 0 }}>L5 Community</p>
            {community ? (
              <p style={{ color: "var(--fg)", margin: "4px 0 0" }}>
                id {shortLabel(community.communityId, 14)} · size {community.communitySize}
              </p>
            ) : (
              <p style={{ color: "var(--muted)", margin: "4px 0 0" }}>n/a</p>
            )}
          </div>
        </div>

        {dna ? (
          <div style={{ marginTop: 12, overflowX: "auto" }}>
            <p style={{ color: "var(--muted)", fontSize: 11, margin: "0 0 4px" }}>L2 Repo DNA</p>
            <table suppressHydrationWarning style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ color: "var(--muted)" }}>
                  <th style={{ textAlign: "right", padding: "4px" }}>watch</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>fork</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>issues</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>pr</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>push</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>bot</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>night</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>weekend</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>top1</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>entropy</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>active days</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.watchShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.forkShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.issuesShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.prShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.pushShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.botRatio * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.nightRatio * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.weekendRatio * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.top1ActorShare * 100).toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {dna.eventEntropy.toFixed(2)}
                  </td>
                  <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                    {(dna.activeDaysRatio * 100).toFixed(0)}%
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        ) : null}

        {watcherLifts.length > 0 ? (
          <div style={{ marginTop: 12, overflowX: "auto" }}>
            <p style={{ color: "var(--muted)", fontSize: 11, margin: "0 0 4px" }}>
              L3 Watcher persona lift (vs global)
            </p>
            <table suppressHydrationWarning style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr style={{ color: "var(--muted)" }}>
                  <th style={{ textAlign: "left", padding: "4px" }}>persona</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>share in watchers</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>share global</th>
                  <th style={{ textAlign: "right", padding: "4px" }}>lift</th>
                </tr>
              </thead>
              <tbody>
                {watcherLifts.map((row) => (
                  <tr key={row.personaLabel}>
                    <td style={{ padding: "4px", color: "var(--fg)" }}>{row.personaLabel}</td>
                    <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                      {(row.shareInWatchers * 100).toFixed(1)}%
                    </td>
                    <td style={{ padding: "4px", textAlign: "right", color: "var(--fg)" }}>
                      {(row.shareInGlobal * 100).toFixed(1)}%
                    </td>
                    <td
                      style={{
                        padding: "4px",
                        textAlign: "right",
                        color: row.lift >= 1.5 ? "var(--green)" : row.lift < 0.7 ? "var(--red, #ff6666)" : "var(--fg)",
                      }}
                    >
                      {row.lift.toFixed(2)}×
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}

        {similarEdges.length > 0 ? (
          <div style={{ marginTop: 12 }}>
            <p style={{ color: "var(--muted)", fontSize: 11, margin: "0 0 4px" }}>L5 Nearest repos (Jaccard)</p>
            <ul style={{ margin: 0, paddingLeft: 16, fontSize: 11 }}>
              {similarEdges.map((edge) => (
                <li key={edge.dstRepo}>
                  <EntityLink type="repo" id={edge.dstRepo} label={shortLabel(edge.dstRepo, 42)} />{" "}
                  <span style={{ color: "var(--muted)" }}>
                    J={edge.jaccard.toFixed(3)} · {edge.sharedActors} shared actors
                  </span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}

        {assocRules.length > 0 ? (
          <div style={{ marginTop: 12 }}>
            <p style={{ color: "var(--muted)", fontSize: 11, margin: "0 0 4px" }}>L5 Association rules involving this repo</p>
            <ul style={{ margin: 0, paddingLeft: 16, fontSize: 11 }}>
              {assocRules.map((rule, idx) => (
                <li key={`${rule.antecedent}->${rule.consequent}-${idx}`}>
                  <span style={{ color: "var(--fg)" }}>{rule.antecedent}</span>
                  {" ⇒ "}
                  <EntityLink type="repo" id={rule.consequent} label={shortLabel(rule.consequent, 36)} />
                  <span style={{ color: "var(--muted)" }}>
                    &nbsp;· lift {rule.lift.toFixed(2)}× · conf {(rule.confidence * 100).toFixed(0)}%
                  </span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </section>

      <section className="nes-container is-dark" style={{ marginBottom: 16 }}>
        <p style={{ color: "var(--muted)", marginBottom: 8 }}>Key findings</p>
        <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: "var(--fg)" }}>
          <li>
            Latest rank score: {rankHistory[rankHistory.length - 1]?.rankScore?.toFixed(4) ?? "n/a"} (daily series).
          </li>
          <li>
            {latestConcentration
              ? `Bus-factor signal: Top-1 actor share ${(latestConcentration.top1ActorShare * 100).toFixed(1)}% (Top-5 ${(latestConcentration.top5ActorShare * 100).toFixed(1)}%).`
              : "Bus-factor signal: n/a."}
          </li>
          <li>
            {worstDrift
              ? `Largest event-mix drift day: ${worstDrift.metricDate} (JS=${worstDrift.jsDivergence.toFixed(4)}, L1=${worstDrift.l1Distance.toFixed(4)}), dominated by ${worstDrift.topShiftEventType}.`
              : "Event-mix drift: n/a."}
          </li>
        </ul>
      </section>

      <section className="nes-container with-title is-dark" style={{ marginBottom: 24, minWidth: 0 }}>
        <p className="title" style={{ color: "var(--cyan)" }}>
          Activity trend (MA7 + forecast)
        </p>
        {trendData.length > 0 ? (
          <AdvancedTrendChart data={trendData} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No trend data.</p>
        )}
        <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
          <summary>Evidence (ClickHouse)</summary>
          <div style={{ marginTop: 6 }}>
            `batch_repo_trend_forecast` WHERE repo_name = '{repoName}'
          </div>
        </details>
      </section>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--green)" }}>
            Rank score trajectory (daily)
          </p>
          {scoreSeries.length > 0 ? (
            <DashboardCharts variant="line" data={scoreSeries} color="#33ff57" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No score data.</p>
          )}
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>Evidence (ClickHouse)</summary>
            <div style={{ marginTop: 6 }}>
              `batch_repo_rank_score_day` WHERE repo_name = '{repoName}'
            </div>
          </details>
        </section>

        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--yellow)" }}>
            Top actors (latest day share %)
          </p>
          {topActorShareData.length > 0 ? (
            <DashboardCharts variant="bar" data={topActorShareData} color="#ffcc00" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No actor-share data.</p>
          )}
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>Evidence (ClickHouse)</summary>
            <div style={{ marginTop: 6 }}>
              `batch_repo_top_actors_latest` WHERE repo_name = '{repoName}'
            </div>
          </details>
        </section>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--cyan)" }}>
            Event-mix drift (JS divergence)
          </p>
          {jsDriftSeries.length > 0 ? (
            <DashboardCharts variant="line" data={jsDriftSeries} color="#00e5ff" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No drift data.</p>
          )}
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>How it is computed</summary>
            <div style={{ marginTop: 6 }}>
              Source: `batch_repo_event_mix_shift_day` (JS divergence + L1 between consecutive-day event-type share vectors).
            </div>
          </details>
        </section>

        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--green)" }}>
            Event-type share (Top types, %)
          </p>
          {eventTypesSorted.length > 0 ? (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
              {eventTypesSorted.map((eventType) => (
                <div key={eventType} style={{ minWidth: 0 }}>
                  <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 6 }}>
                    {eventType}
                  </div>
                  <DashboardCharts variant="line" data={shareByType.get(eventType) ?? []} color="#33ff57" />
                </div>
              ))}
            </div>
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No event-share data.</p>
          )}
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>Evidence (ClickHouse)</summary>
            <div style={{ marginTop: 6 }}>
              `batch_repo_event_type_share_day` WHERE repo_name = '{repoName}'
            </div>
          </details>
        </section>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--yellow)" }}>
            Contributor concentration (Top-1 share %)
          </p>
          <DashboardCharts
            variant="line"
            data={contributorConcentration.map((row) => ({
              label: row.metricDate.slice(5),
              value: Number((row.top1ActorShare * 100).toFixed(2)),
            }))}
            color="#ffcc00"
          />
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>Evidence (ClickHouse)</summary>
            <div style={{ marginTop: 6 }}>
              `batch_repo_contributor_concentration_day` WHERE repo_name = '{repoName}'
            </div>
          </details>
        </section>

        <section className="nes-container with-title is-dark" style={{ minWidth: 0 }}>
          <p className="title" style={{ color: "var(--yellow)" }}>
            Hotness composition (latest day, %)
          </p>
          {componentShare.length > 0 ? (
            <DashboardCharts variant="bar" data={componentShare} color="#ffcc00" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No composition data.</p>
          )}
          <details style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
            <summary>Evidence (ClickHouse)</summary>
            <div style={{ marginTop: 6 }}>
              `batch_repo_hotness_components` WHERE repo_name = '{repoName}'
            </div>
          </details>
        </section>
      </div>
    </PixelPageShell>
  );
}
