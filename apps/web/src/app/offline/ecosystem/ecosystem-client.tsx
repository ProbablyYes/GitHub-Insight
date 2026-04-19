"use client";

import Link from "next/link";
import { useState } from "react";

import { AdvancedTrendChart } from "@/components/advanced-trend-chart";
import { DashboardCharts } from "@/components/dashboard-charts";
import { EntityLink } from "@/components/entity";
import { OfflineSubnav } from "@/components/offline-subnav";
import {
  InsightsTicker,
  PixelBadge,
  PixelKpi,
  PixelMiniBar,
  PixelSection,
  PixelSearchTable,
  type PixelSearchColumn,
} from "@/components/pixel";
import { PixelPageShell } from "@/components/pixel-shell";
import type {
  getActorCohortDay,
  getConcentrationDay,
  getDailyTrend,
  getEcosystemChangepoints,
  getEventTypeBreakdown,
  getEventTypeShareShiftDay,
  getLanguageDayTrend,
  getOfflineInsightsLatest,
  getRepoClusterProfileLatest,
  getRepoTrendForecast,
  getTopReposLatest,
  getTopReposMonth,
  getTopReposWeek,
  getTopUsersLatestWithBot,
  getTopUsersMonth,
  getTopUsersWeek,
  RepoClusterProfilePoint,
  TopUserWithBotPoint,
} from "@/lib/dashboard";

type Awaited2<T> = T extends Promise<infer U> ? U : T;
type Ret<T extends (...args: never[]) => unknown> = Awaited2<ReturnType<T>>;
import { CohortStackedBar } from "./cohort-stacked-bar";
import { EcosystemTrendWithFlags } from "./ecosystem-trend";
import { EventShiftExplorer } from "./event-shift-explorer";
import { WeeklySeasonalityChart } from "./weekly-seasonality";

const EVENT_TYPE_DESC: Record<string, string> = {
  PushEvent: "code writes (production activity)",
  WatchEvent: "star / follow",
  PullRequestEvent: "PR open / merge (collaboration)",
  PullRequestReviewEvent: "code review",
  PullRequestReviewCommentEvent: "review discussion",
  IssuesEvent: "bug / feature request",
  IssueCommentEvent: "issue discussion",
  ForkEvent: "derivative work",
  CreateEvent: "new repo / branch / tag",
  DeleteEvent: "branch / tag delete",
  ReleaseEvent: "version release",
  CommitCommentEvent: "commit-level discussion",
  PublicEvent: "repo made public",
  MemberEvent: "collaborator change",
  GollumEvent: "wiki edit",
};

const LANGUAGE_TYPICAL: Record<string, string> = {
  JavaScript: "React · Vue · Next.js",
  TypeScript: "VSCode · Next.js · Nest",
  Python: "PyTorch · Django · FastAPI",
  Go: "Kubernetes · Docker · etcd",
  Java: "Spring · Elastic · Hadoop",
  Rust: "Cargo · Tokio · SurrealDB",
  "C++": "LLVM · TensorFlow · Electron",
  C: "Linux · Redis · PostgreSQL",
  "C#": ".NET · Unity · Roslyn",
  PHP: "Laravel · WordPress · Symfony",
  Ruby: "Rails · Discourse · Jekyll",
  Shell: "oh-my-zsh · dotfiles",
  HTML: "static sites · docs",
  CSS: "design systems",
  Kotlin: "Android apps",
  Swift: "iOS apps",
  Scala: "Spark · Akka",
  R: "data science notebooks",
  Dart: "Flutter",
  Lua: "Neovim · Factorio mods",
};

function shortLabel(value: string, maxLength = 22): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 3)}...`;
}

function pctDelta(current: number | undefined, previous: number | undefined): number | null {
  if (current == null || previous == null || !Number.isFinite(previous) || previous === 0) return null;
  return ((current - previous) / previous) * 100;
}

type EcosystemProps = {
  dailyTrend: Ret<typeof getDailyTrend>;
  eventBreakdown: Ret<typeof getEventTypeBreakdown>;
  languageTrend: Ret<typeof getLanguageDayTrend>;
  trendForecast: Ret<typeof getRepoTrendForecast>;
  topRepos: Ret<typeof getTopReposLatest>;
  topReposWeek: Ret<typeof getTopReposWeek>;
  topReposMonth: Ret<typeof getTopReposMonth>;
  topUsers: Ret<typeof getTopUsersLatestWithBot>;
  topUsersWeek: Ret<typeof getTopUsersWeek>;
  topUsersMonth: Ret<typeof getTopUsersMonth>;
  concentrationDay: Ret<typeof getConcentrationDay>;
  actorCohortDay: Ret<typeof getActorCohortDay>;
  eventTypeShareShiftDay: Ret<typeof getEventTypeShareShiftDay>;
  offlineInsights: Ret<typeof getOfflineInsightsLatest>;
  clusterProfile: Ret<typeof getRepoClusterProfileLatest>;
  changepoints: Ret<typeof getEcosystemChangepoints>;
};

type TopWindow = "today" | "week" | "month";

export function EcosystemClient({
  dailyTrend,
  eventBreakdown,
  languageTrend,
  trendForecast,
  topRepos,
  topReposWeek,
  topReposMonth,
  topUsers,
  topUsersWeek,
  topUsersMonth,
  concentrationDay,
  actorCohortDay,
  eventTypeShareShiftDay,
  offlineInsights,
  clusterProfile,
  changepoints,
}: EcosystemProps) {
  const [topWindow, setTopWindow] = useState<TopWindow>("month");

  const concByDate = new Map(concentrationDay.map((c) => [c.metricDate, c] as const));
  const dailyTrendRich = dailyTrend.map((item, idx, arr) => {
    const c = concByDate.get(item.metricDate);
    const window = arr.slice(Math.max(0, idx - 6), idx + 1);
    const ma7 = window.length === 0 ? null : window.reduce((s, d) => s + d.totalEvents, 0) / window.length;
    return {
      label: item.metricDate.slice(5),
      metricDate: item.metricDate,
      value: item.totalEvents,
      ma7: Number(ma7?.toFixed(1) ?? 0),
      gini: c ? Number(c.gini.toFixed(3)) : null,
    };
  });
  // Month totals + half-month comparison (earlier 15 vs latest 15) — gives a
  // meaningful WoW-style delta on a 30d window instead of fragile 7d-vs-7d.
  const recentDays = dailyTrend.slice(-15);
  const previousDays = dailyTrend.slice(-30, -15);
  const last30 = dailyTrend.slice(-30);
  const monthTotal = last30.reduce((s, d) => s + d.totalEvents, 0);
  const recentHalfTotal = recentDays.reduce((s, d) => s + d.totalEvents, 0);
  const prevHalfTotal = previousDays.reduce((s, d) => s + d.totalEvents, 0);
  const monthRange = {
    start: last30[0]?.metricDate ?? null,
    end: last30[last30.length - 1]?.metricDate ?? null,
    days: last30.length,
  };

  const eventTypeTotals = eventBreakdown.reduce<Record<string, number>>((acc, item) => {
    acc[item.eventType] = (acc[item.eventType] || 0) + item.totalEvents;
    return acc;
  }, {});
  const eventTypeData = Object.entries(eventTypeTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([eventType, totalEvents]) => ({ label: eventType, value: totalEvents }));

  const languageTotalsAll = languageTrend.reduce<Record<string, number>>((acc, item) => {
    const lang = item.languageGuess || "unknown";
    acc[lang] = (acc[lang] || 0) + item.totalEvents;
    return acc;
  }, {});
  const totalLanguageEvents = Object.values(languageTotalsAll).reduce((s, v) => s + v, 0);
  const unknownEvents = languageTotalsAll["unknown"] ?? 0;
  const unknownShare =
    totalLanguageEvents > 0 ? (unknownEvents / totalLanguageEvents) * 100 : 0;
  const languageData = Object.entries(languageTotalsAll)
    .filter(([lang]) => lang && lang !== "unknown")
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([lang, total]) => ({ label: lang, value: total }));

  const trendData = trendForecast.map((item) => ({
    label: item.metricDate.slice(5),
    actual: item.totalEvents,
    ma7: item.ma7,
    forecast: item.forecastNextDay,
  }));
  const forecastRepoName = trendForecast[0]?.repoName ?? null;
  const forecastLastRow = trendForecast[trendForecast.length - 1];
  const forecastFirstRow = trendForecast[0];
  const forecastDays = trendForecast.length;
  // derived "what does the chart actually say right now"
  const forecastStats = (() => {
    if (!forecastLastRow || !forecastFirstRow) return null;
    const firstMa = Number(forecastFirstRow.ma7) || 0;
    const lastMa = Number(forecastLastRow.ma7) || 0;
    const next = Number(forecastLastRow.forecastNextDay) || 0;
    const maDelta = firstMa > 0 ? (lastMa - firstMa) / firstMa : null; // overall ma7 trajectory
    const nextVsLastMa = lastMa > 0 ? (next - lastMa) / lastMa : null; // tomorrow vs today's trend
    const peakEvents = trendForecast.reduce(
      (m, r) => Math.max(m, Number(r.totalEvents) || 0),
      0,
    );
    const lastEvents = Number(forecastLastRow.totalEvents) || 0;
    let momentumVerdict: "accelerating" | "flat" | "decaying";
    if (nextVsLastMa == null) momentumVerdict = "flat";
    else if (nextVsLastMa > 0.1) momentumVerdict = "accelerating";
    else if (nextVsLastMa < -0.1) momentumVerdict = "decaying";
    else momentumVerdict = "flat";
    return {
      firstDate: forecastFirstRow.metricDate,
      lastDate: forecastLastRow.metricDate,
      firstMa,
      lastMa,
      next,
      maDelta,
      nextVsLastMa,
      peakEvents,
      lastEvents,
      momentumVerdict,
    };
  })();

  // Sparklines now span the full 30-day window, not just 7.
  const sparkEvents = last30.map((d) => d.totalEvents);
  const last30Conc = concentrationDay.slice(-30);
  const sparkTop5 = last30Conc.map((d) => d.top5Share);
  const sparkActiveRepos = last30Conc.map((d) => d.repoCount ?? 0);
  const sparkGini = last30Conc.map((d) => d.gini ?? 0);
  const sparkEntropy = last30Conc.map((d) => d.normalizedEntropy ?? 0);

  const avg = (xs: number[]) =>
    xs.length === 0 ? null : xs.reduce((s, v) => s + v, 0) / xs.length;
  const meanGini = avg(last30Conc.map((d) => d.gini));
  const meanTop5 = avg(last30Conc.map((d) => d.top5Share));
  const meanEntropy = avg(last30Conc.map((d) => d.normalizedEntropy));
  const meanActiveRepos = avg(last30Conc.map((d) => d.repoCount ?? 0));

  // Half-month deltas: compare mean of last 15 days vs mean of days 16-30.
  const firstHalfConc = last30Conc.slice(0, 15);
  const secondHalfConc = last30Conc.slice(15);
  const meanGiniFirst = avg(firstHalfConc.map((d) => d.gini ?? 0));
  const meanGiniSecond = avg(secondHalfConc.map((d) => d.gini ?? 0));
  const meanTop5First = avg(firstHalfConc.map((d) => d.top5Share ?? 0));
  const meanTop5Second = avg(secondHalfConc.map((d) => d.top5Share ?? 0));
  const meanEntFirst = avg(firstHalfConc.map((d) => d.normalizedEntropy ?? 0));
  const meanEntSecond = avg(secondHalfConc.map((d) => d.normalizedEntropy ?? 0));
  const meanReposFirst = avg(firstHalfConc.map((d) => d.repoCount ?? 0));
  const meanReposSecond = avg(secondHalfConc.map((d) => d.repoCount ?? 0));

  const deltaEvents = pctDelta(recentHalfTotal, prevHalfTotal);
  const deltaTop5_pp =
    meanTop5First != null && meanTop5Second != null
      ? (meanTop5Second - meanTop5First) * 100
      : null;
  const deltaGini_abs =
    meanGiniFirst != null && meanGiniSecond != null ? meanGiniSecond - meanGiniFirst : null;
  const deltaEntropy_abs =
    meanEntFirst != null && meanEntSecond != null ? meanEntSecond - meanEntFirst : null;
  const deltaRepos_pct =
    meanReposFirst != null && meanReposFirst !== 0 && meanReposSecond != null
      ? ((meanReposSecond - meanReposFirst) / meanReposFirst) * 100
      : null;

  const currentTopRepos =
    topWindow === "today" ? topRepos : topWindow === "week" ? topReposWeek : topReposMonth;
  const currentTopUsers =
    topWindow === "today" ? topUsers : topWindow === "week" ? topUsersWeek : topUsersMonth;

  const clusterFeatMax = clusterProfile.reduce(
    (m, r) => ({
      hot: Math.max(m.hot, r.avgHotness),
      mom: Math.max(m.mom, r.avgMomentum),
      eng: Math.max(m.eng, r.avgEngagement),
    }),
    { hot: 0.01, mom: 0.01, eng: 0.01 },
  );

  const clusterColumns: PixelSearchColumn<RepoClusterProfilePoint>[] = [
    {
      key: "clusterLabel",
      header: "role",
      align: "left",
      sortValue: (r) => r.clusterLabel,
      render: (r) => (
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <PixelBadge tone="info">#{r.clusterId}</PixelBadge>
          <span style={{ fontSize: 11 }}>{r.clusterLabel}</span>
        </div>
      ),
    },
    { key: "members", header: "#", align: "right", sortValue: (r) => r.members },
    {
      key: "shareOfRepos",
      header: "share",
      align: "right",
      sortValue: (r) => r.shareOfRepos,
      render: (r) => <>{(r.shareOfRepos * 100).toFixed(1)}%</>,
    },
    {
      key: "profile",
      header: "profile (hot / mom / eng / bot)",
      align: "left",
      render: (r) => (
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <PixelMiniBar
            value={r.avgHotness}
            max={clusterFeatMax.hot}
            tone="positive"
            width={120}
            height={6}
            label="hot"
            valueText={r.avgHotness.toFixed(2)}
          />
          <PixelMiniBar
            value={r.avgMomentum}
            max={clusterFeatMax.mom}
            tone="info"
            width={120}
            height={6}
            label="mom"
            valueText={r.avgMomentum.toFixed(2)}
          />
          <PixelMiniBar
            value={r.avgEngagement}
            max={clusterFeatMax.eng}
            tone="purple"
            width={120}
            height={6}
            label="eng"
            valueText={r.avgEngagement.toFixed(2)}
          />
          <PixelMiniBar
            value={r.avgBotRatio}
            max={1}
            tone="danger"
            width={120}
            height={6}
            label="bot"
            valueText={r.avgBotRatio.toFixed(2)}
          />
        </div>
      ),
    },
    {
      key: "sampleRepos",
      header: "sample",
      align: "left",
      searchValue: (r) => r.sampleRepos,
      render: (r) => (
        <span style={{ color: "var(--muted-strong)", fontSize: 11 }}>
          {r.sampleRepos
            ?.split(",")
            .slice(0, 3)
            .map((name, i, arr) => (
              <span key={`${name}-${i}`}>
                <EntityLink type="repo" id={name.trim()} />
                {i < arr.length - 1 ? ", " : ""}
              </span>
            ))}
        </span>
      ),
    },
    {
      key: "inspect",
      header: "",
      align: "right",
      render: (r) => (
        <Link
          href={`/offline/ml?cluster=${r.clusterId}`}
          className="nes-btn"
          style={{ padding: "1px 6px", fontSize: 10, textDecoration: "none" }}
        >
          inspect »
        </Link>
      ),
    },
  ];

  return (
    <PixelPageShell
      title="L0 Ecosystem"
      subtitle="Volume, structure, concentration, cohorts, event mix and repo role clusters."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "Ecosystem" },
      ]}
      tldr={
        <>
          {monthTotal.toLocaleString()} events across {monthRange.days} days
          {monthRange.start && monthRange.end ? (
            <>
              {" "}
              ({monthRange.start} → {monthRange.end})
            </>
          ) : null}
          {deltaEvents != null ? (
            <>
              . Second half{" "}
              <span style={{ color: deltaEvents >= 0 ? "var(--accent-positive)" : "var(--accent-danger)" }}>
                {deltaEvents >= 0 ? "+" : ""}
                {deltaEvents.toFixed(1)}%
              </span>{" "}
              vs first half
            </>
          ) : null}
          . 30-day avg top-5 concentration ={" "}
          {meanTop5 != null ? (meanTop5 * 100).toFixed(1) : "—"}% — {" "}
          {meanEntropy != null && meanEntropy < 0.6 ? "highly concentrated" : "moderately spread"}.
          {changepoints.length > 0 ? (
            <>
              {" "}
              {changepoints.length} CUSUM changepoint{changepoints.length === 1 ? "" : "s"} detected.
            </>
          ) : null}
        </>
      }
    >
      <OfflineSubnav />

      <InsightsTicker insights={offlineInsights} />

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "var(--pixel-space-4)",
          marginBottom: "var(--pixel-space-5)",
        }}
      >
        <PixelKpi
          label="Events / 30d"
          value={monthTotal.toLocaleString()}
          delta={deltaEvents ?? undefined}
          deltaLabel="H2 vs H1 %"
          spark={sparkEvents}
          tone="positive"
          hint="Sum of daily events over the full 30-day window. Delta = last-15d total vs first-15d total."
        />
        <PixelKpi
          label="Active repos (30d avg)"
          value={meanActiveRepos != null ? Math.round(meanActiveRepos).toLocaleString() : "—"}
          delta={deltaRepos_pct ?? undefined}
          deltaLabel="H2 vs H1 %"
          spark={sparkActiveRepos}
          tone="info"
          hint="Average distinct active repos per day, averaged over 30 days"
        />
        <PixelKpi
          label="Gini (30d avg)"
          value={meanGini != null ? meanGini.toFixed(3) : "—"}
          delta={deltaGini_abs != null ? deltaGini_abs * 100 : undefined}
          deltaLabel="H2 vs H1 abs×100"
          spark={sparkGini}
          tone="purple"
          hint="0 = perfectly equal activity across repos; 1 = one repo dominates. Averaged over 30 days."
        />
        <PixelKpi
          label="Top-5 share (30d avg)"
          value={meanTop5 != null ? `${(meanTop5 * 100).toFixed(1)}%` : "—"}
          delta={deltaTop5_pp ?? undefined}
          deltaLabel="H2 vs H1 pp"
          spark={sparkTop5}
          tone="change"
          hint="Share of total events held by the top-5 repos each day, averaged over 30 days"
        />
        <PixelKpi
          label="Entropy (30d avg)"
          value={meanEntropy != null ? meanEntropy.toFixed(3) : "—"}
          delta={deltaEntropy_abs != null ? deltaEntropy_abs * 100 : undefined}
          deltaLabel="H2 vs H1 abs×100"
          spark={sparkEntropy}
          hint="Normalised Shannon entropy: 1 = evenly spread, 0 = single repo dominates. Averaged over 30 days."
          tone="info"
        />
      </div>

      <PixelSection
        title="Daily volume + Gini + changepoints · 30-day window"
        tone="info"
        headline={`${dailyTrend.length}-day series. ${changepoints.length} changepoint${changepoints.length === 1 ? "" : "s"} detected by CUSUM. Weekend bands shaded to expose weekly seasonality.`}
        source="batch_daily_metrics · batch_concentration_day · batch_ecosystem_changepoints"
        techBadge="CUSUM · Gini(Lorenz) · Spark window/lag · MA7"
        howToRead="Left axis = events (blue) & MA7 (yellow). Right axis = Gini (purple). Flags ▲/▼ mark CUSUM burst/drop days. Light grey bands = Sat+Sun. With 30 days you can see ~4 weekly cycles — the dip inside each band shows the weekend effect."
      >
        <EcosystemTrendWithFlags data={dailyTrendRich} flags={changepoints} />
      </PixelSection>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection
          title="Event structure · 30-day mix"
          tone="positive"
          headline="What kind of work dominates the month? — top 8 event types aggregated over 30 days."
          source="batch_event_type_day"
          howToRead={
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 12px" }}>
              {eventTypeData.map((e) => (
                <div key={e.label} style={{ color: "var(--muted)", fontSize: 11 }}>
                  <code style={{ color: "var(--accent-info)" }}>{e.label}</code>:{" "}
                  {EVENT_TYPE_DESC[e.label] ?? "other GitHub event"}
                </div>
              ))}
            </div>
          }
        >
          <DashboardCharts variant="pie" data={eventTypeData} />
        </PixelSection>

        <PixelSection
          title="Top languages · 30-day totals"
          tone="info"
          headline={
            unknownShare > 0
              ? `Top ${languageData.length} detected languages over 30 days — ${unknownShare.toFixed(
                  1
                )}% of events are in repos without detected language metadata and are excluded from this chart.`
              : `Top ${languageData.length} language communities by events over 30 days.`
          }
          source="batch_language_day_trend"
          howToRead={
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 12px" }}>
              {languageData.map((l) => (
                <div key={l.label} style={{ color: "var(--muted)", fontSize: 11 }}>
                  <code style={{ color: "var(--accent-info)" }}>{l.label}</code>:{" "}
                  {LANGUAGE_TYPICAL[l.label] ?? "various projects"}
                </div>
              ))}
            </div>
          }
        >
          <DashboardCharts variant="bar" data={languageData} color="#33ccff" />
        </PixelSection>

        <PixelSection
          title="Actor cohorts · 30-day stack"
          tone="positive"
          headline="Daily mix of new / returning / reactivated actors across 30 days — is growth driven by newcomers or returning users?"
          source="batch_actor_cohort_day"
          howToRead="new = first-seen in window. returning = last seen within 2 days. reactivated = gap ≥ 3 days. One stacked bar per day; the colour mix shifting across the month reveals whether the platform is onboarding or retaining."
        >
          <CohortStackedBar rows={actorCohortDay} days={30} />
        </PixelSection>
      </div>

      <PixelSection
        title="Event-mix shift (pp) · pick any of 30 days"
        tone="change"
        headline="Pick any day from the 4+ weeks to see how that day's event-type mix shifted vs the day before. Green bars = that event type gained share; red bars = it lost share. The size of each bar is in percentage points (pp)."
        source="batch_event_type_share_shift_day"
        techBadge="Δshare = share(t) − share(t−1), summed to 0 across event types on any given day"
        howToRead="Days are grouped by week; ★ = day with the largest absolute shift across the full 30-day window (default selection). A 1 pp shift for WatchEvent means watches went from e.g. 12% to 13% of all events that day."
      >
        <EventShiftExplorer rows={eventTypeShareShiftDay} />
      </PixelSection>

      <PixelSection
        title="Weekly seasonality — Mon…Sun averages over 30 days"
        tone="info"
        headline="Aggregate of each weekday across the month. Tall bars = busy weekdays, dips on Sat/Sun = the weekend effect that CUSUM changepoints partially reflect."
        source="batch_daily_metrics · batch_concentration_day"
        techBadge="Group-by weekday + mean/min/max"
        howToRead="Each bar is the mean event count on that weekday across the 30-day window; the whisker shows min/max. If Sat/Sun are much lower than Mon–Fri, bursts/drops you see on the trend chart often just follow this weekly rhythm."
      >
        <WeeklySeasonalityChart rows={dailyTrend} />
      </PixelSection>

      <PixelSection
        title="Trend + forecast · daily events + next-day extrapolation (single repo)"
        tone="change"
        headline={
          forecastRepoName
            ? `Repo-level 1-day extrapolation for trending repo ${forecastRepoName} — ${forecastDays} day${forecastDays === 1 ? "" : "s"} of actual events (blue) · MA7 (yellow) · forecast for the day after (dashed).`
            : "Repo-level 1-day extrapolation for a trending repo: actual events · MA7 · next-day forecast."
        }
        source="batch_repo_trend_forecast"
        techBadge="forecast = max(0, MA7 × (1 + momentum)) · momentum = (MA3 − MA7) / MA7"
        actions={
          <>
            <PixelBadge tone="info">
              {forecastStats
                ? `${forecastDays}d window · ${forecastStats.firstDate.slice(5)}→${forecastStats.lastDate.slice(5)}`
                : "trending repo · 1-day extrapolation"}
            </PixelBadge>
            {forecastLastRow?.forecastNextDay != null ? (
              <PixelBadge tone="change">
                next ≈ {Math.round(forecastLastRow.forecastNextDay).toLocaleString()}
              </PixelBadge>
            ) : null}
            {forecastStats ? (
              <PixelBadge
                tone={
                  forecastStats.momentumVerdict === "accelerating"
                    ? "positive"
                    : forecastStats.momentumVerdict === "decaying"
                      ? "danger"
                      : "muted"
                }
              >
                {forecastStats.momentumVerdict}
              </PixelBadge>
            ) : null}
          </>
        }
        howToRead="This is NOT an ecosystem-wide forecast. It extrapolates ONE trending repo's next-day activity from its own MA7 momentum — a sanity check on short-term inertia, not a prediction for the platform."
      >
        {trendData.length > 0 ? (
          <>
            <AdvancedTrendChart data={trendData} />
            {forecastStats ? (
              <div
                style={{
                  marginTop: 12,
                  padding: "10px 12px",
                  borderLeft: "3px solid var(--accent-change)",
                  background: "var(--surface-elevated, rgba(255,255,255,0.02))",
                  fontSize: 11,
                  lineHeight: 1.6,
                  color: "var(--fg)",
                }}
              >
                <div
                  style={{
                    color: "var(--accent-change)",
                    fontSize: 10,
                    letterSpacing: 2,
                    fontWeight: 700,
                    marginBottom: 4,
                  }}
                >
                  {`==> READING ${forecastRepoName ?? "this repo"}`}
                </div>
                <div>
                  Over {forecastDays} days ({forecastStats.firstDate} →{" "}
                  {forecastStats.lastDate}), this repo&apos;s MA7 went from{" "}
                  <strong>{forecastStats.firstMa.toFixed(2)}</strong> events/day to{" "}
                  <strong>{forecastStats.lastMa.toFixed(2)}</strong> ({" "}
                  {forecastStats.maDelta == null
                    ? "—"
                    : `${forecastStats.maDelta >= 0 ? "+" : ""}${(forecastStats.maDelta * 100).toFixed(0)}%`}{" "}
                  over the window). The peak single day hit{" "}
                  <strong>{forecastStats.peakEvents}</strong> events; the last observed day had{" "}
                  <strong>{forecastStats.lastEvents}</strong>.
                </div>
                <div style={{ marginTop: 4 }}>
                  Tomorrow&apos;s extrapolation is{" "}
                  <strong style={{
                    color:
                      forecastStats.momentumVerdict === "accelerating"
                        ? "var(--accent-positive)"
                        : forecastStats.momentumVerdict === "decaying"
                          ? "var(--accent-danger)"
                          : "var(--muted-strong)",
                  }}>
                    {forecastStats.next.toFixed(1)} events
                  </strong>
                  {forecastStats.nextVsLastMa != null ? (
                    <>
                      {" "}({forecastStats.nextVsLastMa >= 0 ? "+" : ""}
                      {(forecastStats.nextVsLastMa * 100).toFixed(0)}% vs current MA7) — which means the
                      3-day running average (MA3) is{" "}
                      {forecastStats.nextVsLastMa > 0.1
                        ? "above"
                        : forecastStats.nextVsLastMa < -0.1
                          ? "below"
                          : "roughly equal to"}{" "}
                      the 7-day one, so the short-term wave is{" "}
                      <strong>{forecastStats.momentumVerdict}</strong>.
                    </>
                  ) : null}
                </div>
                <div style={{ marginTop: 6, color: "var(--muted)" }}>
                  What the chart really tells us: <code>total_events</code> (blue) is the raw daily volume —
                  spiky, noisy, reacts to every Hacker-News post or holiday. <code>MA7</code> (yellow) is the
                  7-day rolling mean that smooths out weekends and single-day bursts; the slope of this line
                  IS the repo&apos;s trend. <code>forecast_next_day</code> (dashed) is not a &ldquo;model
                  prediction&rdquo; in the ML sense — it&apos;s <code>MA7 × (1 + momentum)</code> where
                  momentum is <code>(MA3 − MA7) / MA7</code>. Plain English: if the last three days ran
                  hotter than the last seven, we nudge the forecast up; if cooler, we nudge it down. It&apos;s
                  a moving-average slope extrapolation, not LSTM — honest about being a baseline.
                </div>
                <div style={{ marginTop: 6, color: "var(--muted)" }}>
                  Why this specific repo: the Spark job only emits forecast rows for the top-K repos by{" "}
                  rank score, so the forecast table holds {`\u2248`}8 trending projects rather than all 40k+
                  repos. We picked the one with the most days of coverage inside that top-K, so you can see
                  the full rise-and-fall shape. The flat or dropping tail on many trending repos is itself
                  the lesson — viral bursts usually decay inside 2 weeks, and the momentum penalty in the
                  formula captures that decay faster than a plain MA would.
                </div>
              </div>
            ) : null}
          </>
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>No trend data.</p>
        )}
      </PixelSection>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
          gap: "var(--pixel-space-4)",
        }}
      >
        <PixelSection
          title="Top repos"
          tone="positive"
          headline={
            topWindow === "today"
              ? "Latest day's top repos by event count — click a name to inspect."
              : topWindow === "week"
                ? "7-day cumulative top repos — click a name to inspect."
                : "30-day cumulative top repos — the most active projects of the month."
          }
          source="batch_top_repos_day"
          actions={
            <div style={{ display: "flex", gap: 4 }}>
              {(["today", "week", "month"] as const).map((w) => (
                <button
                  key={w}
                  onClick={() => setTopWindow(w)}
                  className="nes-btn"
                  style={{
                    padding: "1px 8px",
                    fontSize: 10,
                    background:
                      topWindow === w ? "var(--accent-change)" : undefined,
                    color: topWindow === w ? "#111" : undefined,
                  }}
                >
                  {w === "today" ? "Today" : w === "week" ? "7d" : "30d"}
                </button>
              ))}
            </div>
          }
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 12 }}>
            {currentTopRepos.map((r) => (
              <div
                key={r.repoName}
                style={{ display: "flex", justifyContent: "space-between", gap: 8 }}
              >
                <span>
                  <span style={{ color: "var(--muted)" }}>#{r.rank}</span>{" "}
                  <EntityLink type="repo" id={r.repoName} label={shortLabel(r.repoName)} />
                </span>
                <span style={{ color: "var(--muted-strong)" }}>{r.eventCount.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </PixelSection>

        <PixelSection
          title="Top actors"
          tone="info"
          headline={
            topWindow === "today"
              ? "Latest day's top actors. [bot] tag = heuristic bot login."
              : topWindow === "week"
                ? "7-day cumulative top actors. [bot] tag = heuristic bot login."
                : "30-day cumulative top actors — who drove the month. [bot] tag = heuristic bot login."
          }
          source="batch_top_users_day"
          actions={
            <div style={{ display: "flex", gap: 4 }}>
              {(["today", "week", "month"] as const).map((w) => (
                <button
                  key={w}
                  onClick={() => setTopWindow(w)}
                  className="nes-btn"
                  style={{
                    padding: "1px 8px",
                    fontSize: 10,
                    background:
                      topWindow === w ? "var(--accent-change)" : undefined,
                    color: topWindow === w ? "#111" : undefined,
                  }}
                >
                  {w === "today" ? "Today" : w === "week" ? "7d" : "30d"}
                </button>
              ))}
            </div>
          }
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 12 }}>
            {currentTopUsers.map((u: TopUserWithBotPoint) => (
              <div
                key={u.actorLogin}
                style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}
              >
                <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ color: "var(--muted)" }}>#{u.rank}</span>{" "}
                  <EntityLink type="actor" id={u.actorLogin} label={shortLabel(u.actorLogin)} />
                  {u.isBot ? <PixelBadge tone="muted">bot</PixelBadge> : null}
                </span>
                <span style={{ color: "var(--muted-strong)" }}>{u.eventCount.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </PixelSection>
      </div>

      {clusterProfile.length > 0 ? (
        <PixelSection
          title="Repo role clusters (L1 preview)"
          tone="positive"
          headline="KMeans(k=4) on normalised hotness/momentum/engagement/stability/bot."
          source="batch_repo_cluster_profile"
          techBadge="Spark MLlib · KMeans · PCA(2)"
          actions={
            <Link
              href="/offline/ml"
              className="nes-btn"
              style={{
                padding: "2px 8px",
                fontSize: 11,
                textDecoration: "none",
              }}
            >
              ML Lab »
            </Link>
          }
        >
          <PixelSearchTable
            rows={clusterProfile}
            columns={clusterColumns}
            getRowKey={(r) => r.clusterId}
            initialSort={{ key: "members", desc: true }}
            pageSize={8}
            searchPlaceholder="filter by label or repo..."
          />
        </PixelSection>
      ) : null}
    </PixelPageShell>
  );
}
