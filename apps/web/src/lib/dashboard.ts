import { clickhouse } from "@/lib/clickhouse";
import { bootstrapCIofCohenD, cohenD, olsSlope, welchT } from "@/lib/stats";

export type SummaryMetrics = {
  realtimeEventRows: number;
  realtimeRepos: number;
  anomalyAlerts: number;
  batchMetricRows: number;
};

export type EventTrendPoint = {
  windowStart: string;
  totalEvents: number;
};

export type HotRepo = {
  repoName: string;
  hotnessScore: number;
  pushEvents: number;
  watchEvents: number;
  forkEvents: number;
};

export type AlertRow = {
  windowStart: string;
  repoName: string;
  currentEvents: number;
  baselineEvents: number;
  anomalyRatio: number;
  alertLevel: string;
};

export type DailyTrendPoint = {
  metricDate: string;
  totalEvents: number;
};

export type ActorMixPoint = {
  actorCategory: string;
  totalEvents: number;
};

export type ActivityPoint = {
  hourOfDay: number;
  actorCategory: string;
  totalEvents: number;
};

export type LanguageDayTrendPoint = {
  metricDate: string;
  languageGuess: string;
  totalEvents: number;
};

export type TopUserPoint = {
  metricDate: string;
  actorLogin: string;
  eventCount: number;
  rank: number;
};

export type TopRepoPoint = {
  metricDate: string;
  repoName: string;
  eventCount: number;
  rank: number;
};

export type EventTypeDayPoint = {
  metricDate: string;
  eventType: string;
  totalEvents: number;
};

export type AdvancedRepoRankPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  hotnessDecayedTotal: number;
  momentumLatest: number;
  stabilityIndex: number;
  botRatio: number;
  trendLabel: string;
};

export type RepoTrendForecastPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  totalEvents: number;
  ma7: number;
  forecastNextDay: number;
};

export type BurstStabilityPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  burstIndex: number;
  stabilityIndex: number;
  shortTermPressure: number;
  quadrant: string;
};

export type RhythmHeatmapCell = {
  metricDate: string;
  dayOfWeek: number;
  hourOfDay: number;
  actorCategory: string;
  eventCount: number;
  intensityScore: number;
  peakFlag: boolean;
};

export type HotnessComponentPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  hotnessRaw: number;
  watchContribution: number;
  forkContribution: number;
  issuesContribution: number;
  pullRequestContribution: number;
  pushContribution: number;
};

export type OrgRankPoint = {
  metricDate: string;
  orgOrOwner: string;
  rankNo: number;
  hotnessScore: number;
  eventCount: number;
  uniqueRepos: number;
};

export type EventActionPoint = {
  metricDate: string;
  eventType: string;
  payloadAction: string;
  eventCount: number;
};

export type PayloadBucketPoint = {
  metricDate: string;
  sizeBucket: string;
  eventCount: number;
  avgPayloadSize: number;
};

export type UserSegmentPoint = {
  metricDate: string;
  segment: string;
  users: number;
  events: number;
};

export type RepoHealthPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  healthScore: number;
  botRatio: number;
  devNorm: number;
  communityNorm: number;
  attentionNorm: number;
  diversityNorm: number;
};

export type OfflineAnomalyAlertPoint = {
  metricDate: string;
  repoName: string;
  currentEvents: number;
  baselineMean: number;
  zScore: number;
  alertLevel: string;
};

export type OfflineDeclineWarningPoint = {
  metricDate: string;
  repoName: string;
  slope7: number;
  pctChange7: number;
  warningLevel: string;
};

export type RepoClusterPoint = {
  metricDate: string;
  repoName: string;
  clusterId: number;
  pcaX: number;
  pcaY: number;
  rankNo: number;
  healthScore: number;
};

export type ConcentrationDayPoint = {
  metricDate: string;
  totalEvents: number;
  repoCount: number;
  top1Share: number;
  top5Share: number;
  hhi: number;
  entropy: number;
  normalizedEntropy: number;
  gini: number;
  eventsDelta7d: number;
  repoCountDelta7d: number;
  top5ShareDelta7d: number;
  giniDelta7d: number;
  entropyDelta7d: number;
};

export type EcosystemChangepoint = {
  metricDate: string;
  kind: "burst" | "drop";
  cusum: number;
  zScore: number;
  contributionTopType: string;
};

export type ActorCohortDayPoint = {
  metricDate: string;
  cohort: string;
  actors: number;
  events: number;
  avgUniqueRepos: number;
  avgReactivationGapDays: number;
};

export type EventTypeShareShiftDayPoint = {
  metricDate: string;
  eventType: string;
  eventCount: number;
  share: number;
  shareShift: number;
};

export type OfflineInsightPoint = {
  metricDate: string;
  insightType: string;
  insightText: string;
  evidenceJson: string;
};

export type RepoRankExplainPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  hotnessPart: number;
  momentumPart: number;
  engagementPart: number;
  stabilityPart: number;
  botPenalty: number;
};

export type RepoRankDeltaExplainPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  deltaRankNo: number;
  deltaRankScore: number;
  deltaHotnessPart: number;
  deltaMomentumPart: number;
  deltaEngagementPart: number;
  deltaStabilityPart: number;
  deltaBotPenalty: number;
};

export type RepoContributorConcentrationPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  totalEvents: number;
  activeActors: number;
  top1ActorShare: number;
  top5ActorShare: number;
  actorHhi: number;
};

export type RepoTopActorPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  actorRank: number;
  actorLogin: string;
  actorCategory: string;
  actorEvents: number;
  share: number;
};

export type RepoEventTypeSharePoint = {
  metricDate: string;
  repoName: string;
  eventType: string;
  eventCount: number;
  totalEvents: number;
  share: number;
};

export type RepoEventMixShiftPoint = {
  metricDate: string;
  repoName: string;
  jsDivergence: number;
  l1Distance: number;
  totalAbsShift: number;
  topShiftEventType: string;
  topShiftAbs: number;
  topShiftSigned: number;
};

type CountRow = { value: string | number };

async function queryJson<T>(query: string): Promise<T[]> {
  try {
    const result = await clickhouse.query({
      query,
      format: "JSONEachRow",
    });

    return (await result.json()) as T[];
  } catch {
    return [];
  }
}

async function queryCount(query: string): Promise<number> {
  const rows = await queryJson<CountRow>(query);
  const value = rows[0]?.value ?? 0;
  return Number(value);
}

function sqlStringLiteral(value: string): string {
  // ClickHouse string literal escaping: single quote is doubled.
  return `'${value.replaceAll("'", "''")}'`;
}

export async function getSummaryMetrics(): Promise<SummaryMetrics> {
  const [realtimeEventRows, realtimeRepos, anomalyAlerts, batchMetricRows] =
    await Promise.all([
      queryCount("SELECT count() AS value FROM realtime_event_metrics"),
      queryCount("SELECT countDistinct(repo_name) AS value FROM realtime_repo_scores"),
      queryCount("SELECT count() AS value FROM realtime_anomaly_alerts"),
      queryCount("SELECT count() AS value FROM batch_daily_metrics"),
    ]);

  return {
    realtimeEventRows,
    realtimeRepos,
    anomalyAlerts,
    batchMetricRows,
  };
}

export async function getEventTrend(): Promise<EventTrendPoint[]> {
  const rows = await queryJson<{
    window_start: string;
    total_events: number;
  }>(`
    SELECT
      toString(window_start) AS window_start,
      sum(event_count) AS total_events
    FROM realtime_event_metrics
    GROUP BY window_start
    ORDER BY window_start
    LIMIT 240
  `);

  return rows.map((row) => ({
    windowStart: row.window_start,
    totalEvents: Number(row.total_events),
  }));
}

export async function getHotRepos(): Promise<HotRepo[]> {
  const rows = await queryJson<{
    repo_name: string;
    hotness_score: number;
    push_events: number;
    watch_events: number;
    fork_events: number;
  }>(`
    SELECT
      repo_name,
      max(hotness_score) AS hotness_score,
      sum(push_events) AS push_events,
      sum(watch_events) AS watch_events,
      sum(fork_events) AS fork_events
    FROM realtime_repo_scores
    GROUP BY repo_name
    ORDER BY hotness_score DESC
    LIMIT 10
  `);

  return rows.map((row) => ({
    repoName: row.repo_name,
    hotnessScore: Number(row.hotness_score),
    pushEvents: Number(row.push_events),
    watchEvents: Number(row.watch_events),
    forkEvents: Number(row.fork_events),
  }));
}

export async function getAlerts(): Promise<AlertRow[]> {
  const rows = await queryJson<{
    window_start: string;
    repo_name: string;
    current_events: number;
    baseline_events: number;
    anomaly_ratio: number;
    alert_level: string;
  }>(`
    SELECT
      toString(window_start) AS window_start,
      repo_name,
      current_events,
      baseline_events,
      round(anomaly_ratio, 2) AS anomaly_ratio,
      alert_level
    FROM realtime_anomaly_alerts
    ORDER BY window_start DESC, anomaly_ratio DESC
    LIMIT 12
  `);

  return rows.map((row) => ({
    windowStart: row.window_start,
    repoName: row.repo_name,
    currentEvents: Number(row.current_events),
    baselineEvents: Number(row.baseline_events),
    anomalyRatio: Number(row.anomaly_ratio),
    alertLevel: row.alert_level,
  }));
}

export async function getDailyTrend(): Promise<DailyTrendPoint[]> {
  const rows = await queryJson<{
    metric_date: string;
    total_events: number;
  }>(`
    SELECT
      toString(metric_date) AS metric_date,
      sum(event_count) AS total_events
    FROM batch_daily_metrics
    GROUP BY metric_date
    ORDER BY metric_date
    LIMIT 90
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date,
    totalEvents: Number(row.total_events),
  }));
}

export async function getActorMix(): Promise<ActorMixPoint[]> {
  const rows = await queryJson<{
    actor_category: string;
    total_events: number;
  }>(`
    SELECT
      actor_category,
      sum(event_count) AS total_events
    FROM batch_daily_metrics
    GROUP BY actor_category
    ORDER BY total_events DESC
  `);

  return rows.map((row) => ({
    actorCategory: row.actor_category,
    totalEvents: Number(row.total_events),
  }));
}

export async function getActivityPattern(): Promise<ActivityPoint[]> {
  const rows = await queryJson<{
    hour_of_day: number;
    actor_category: string;
    total_events: number;
  }>(`
    SELECT
      hour_of_day,
      actor_category,
      sum(event_count) AS total_events
    FROM batch_activity_patterns
    GROUP BY hour_of_day, actor_category
    ORDER BY hour_of_day, actor_category
  `);

  return rows.map((row) => ({
    hourOfDay: Number(row.hour_of_day),
    actorCategory: row.actor_category,
    totalEvents: Number(row.total_events),
  }));
}

export async function getLanguageDayTrend(limit = 36): Promise<LanguageDayTrendPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    language_guess: string;
    total_events: number;
  }>(`
    SELECT
      toString(metric_date) AS metric_date_str,
      language_guess,
      sum(event_count) AS total_events
    FROM batch_language_day_trend
    GROUP BY metric_date, language_guess
    ORDER BY metric_date DESC, total_events DESC
    LIMIT ${Number(limit)}
  `);

  return rows
    .reverse()
    .map((row) => ({
      metricDate: row.metric_date_str,
      languageGuess: row.language_guess,
      totalEvents: Number(row.total_events),
    }));
}

export async function getTopUsersLatest(topN = 10): Promise<TopUserPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    event_count: number;
    rank: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_users_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      event_count,
      rank
    FROM batch_top_users_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank ASC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    eventCount: Number(row.event_count),
    rank: Number(row.rank),
  }));
}

export async function getTopReposWeek(topN = 10): Promise<TopRepoPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    event_count: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_repos_day
    )
    SELECT
      toString(max(metric_date)) AS metric_date_str,
      repo_name,
      sum(event_count) AS event_count
    FROM batch_top_repos_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), 6)
      AND metric_date <= (SELECT d FROM latest_date)
    GROUP BY repo_name
    ORDER BY event_count DESC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row, i) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    eventCount: Number(row.event_count),
    rank: i + 1,
  }));
}

export async function getTopReposMonth(topN = 10, days = 30): Promise<TopRepoPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    event_count: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_repos_day
    )
    SELECT
      toString(max(metric_date)) AS metric_date_str,
      repo_name,
      sum(event_count) AS event_count
    FROM batch_top_repos_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    GROUP BY repo_name
    ORDER BY event_count DESC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row, i) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    eventCount: Number(row.event_count),
    rank: i + 1,
  }));
}

export type TopUserWithBotPoint = TopUserPoint & { isBot: boolean };

export async function getTopUsersWeek(topN = 10): Promise<TopUserWithBotPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    event_count: number;
    is_bot: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_users_day
    )
    SELECT
      toString(max(tu.metric_date)) AS metric_date_str,
      tu.actor_login,
      sum(tu.event_count) AS event_count,
      max(if(lower(tu.actor_login) LIKE '%[bot]%' OR lower(tu.actor_login) LIKE '%-bot' OR lower(tu.actor_login) LIKE 'bot-%', 1, 0)) AS is_bot
    FROM batch_top_users_day tu
    WHERE tu.metric_date >= subtractDays((SELECT d FROM latest_date), 6)
      AND tu.metric_date <= (SELECT d FROM latest_date)
    GROUP BY tu.actor_login
    ORDER BY event_count DESC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row, i) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    eventCount: Number(row.event_count),
    rank: i + 1,
    isBot: Number(row.is_bot) === 1,
  }));
}

export async function getTopUsersMonth(topN = 10, days = 30): Promise<TopUserWithBotPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    event_count: number;
    is_bot: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_users_day
    )
    SELECT
      toString(max(tu.metric_date)) AS metric_date_str,
      tu.actor_login,
      sum(tu.event_count) AS event_count,
      max(if(lower(tu.actor_login) LIKE '%[bot]%' OR lower(tu.actor_login) LIKE '%-bot' OR lower(tu.actor_login) LIKE 'bot-%', 1, 0)) AS is_bot
    FROM batch_top_users_day tu
    WHERE tu.metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND tu.metric_date <= (SELECT d FROM latest_date)
    GROUP BY tu.actor_login
    ORDER BY event_count DESC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row, i) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    eventCount: Number(row.event_count),
    rank: i + 1,
    isBot: Number(row.is_bot) === 1,
  }));
}

export async function getTopUsersLatestWithBot(topN = 10): Promise<TopUserWithBotPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    event_count: number;
    rank: number;
    is_bot: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_users_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      event_count,
      rank,
      if(lower(actor_login) LIKE '%[bot]%' OR lower(actor_login) LIKE '%-bot' OR lower(actor_login) LIKE 'bot-%', 1, 0) AS is_bot
    FROM batch_top_users_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank ASC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    eventCount: Number(row.event_count),
    rank: Number(row.rank),
    isBot: Number(row.is_bot) === 1,
  }));
}

export async function getTopReposLatest(topN = 10): Promise<TopRepoPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    event_count: number;
    rank: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_top_repos_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      event_count,
      rank
    FROM batch_top_repos_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank ASC
    LIMIT ${Number(topN)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    eventCount: Number(row.event_count),
    rank: Number(row.rank),
  }));
}

export async function getEventTypeBreakdown(days = 30): Promise<EventTypeDayPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    event_type: string;
    total_events: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_event_type_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      event_type,
      sum(event_count) AS total_events
    FROM batch_event_type_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    GROUP BY metric_date, event_type
    ORDER BY metric_date, total_events DESC
    LIMIT 64
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    eventType: row.event_type,
    totalEvents: Number(row.total_events),
  }));
}

export async function getAdvancedRepoRankings(
  limit = 12
): Promise<AdvancedRepoRankPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    hotness_decayed_total: number;
    momentum_latest: number;
    stability_index: number;
    bot_ratio: number;
    trend_label: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_rank_daily
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      hotness_decayed_total,
      momentum_latest,
      stability_index,
      bot_ratio,
      trend_label
    FROM batch_repo_rank_daily
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    hotnessDecayedTotal: Number(row.hotness_decayed_total),
    momentumLatest: Number(row.momentum_latest),
    stabilityIndex: Number(row.stability_index),
    botRatio: Number(row.bot_ratio),
    trendLabel: row.trend_label,
  }));
}

export async function getRepoTrendForecast(
  days = 30
): Promise<RepoTrendForecastPoint[]> {
  // ─── Pick the repo that (a) has a forecast row AT ALL and (b) has the most
  // days of coverage in the forecast table. The Spark job only emits forecast
  // rows for the top-K trending repos, so the global #1 in `batch_repo_rank_daily`
  // is not guaranteed to be present here — previously that mismatch produced
  // an empty result and a "No trend data." placeholder on the dashboard.
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    total_events: number;
    ma7: number;
    forecast_next_day: number;
  }>(`
    WITH focus_repo AS (
      SELECT repo_name
      FROM batch_repo_trend_forecast
      GROUP BY repo_name
      ORDER BY count() DESC, max(metric_date) DESC
      LIMIT 1
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      total_events,
      ma7,
      forecast_next_day
    FROM batch_repo_trend_forecast
    WHERE repo_name = (SELECT repo_name FROM focus_repo)
    ORDER BY metric_date DESC
    LIMIT ${Math.max(Number(days), 7)}
  `);

  return rows
    .reverse()
    .map((row) => ({
      metricDate: row.metric_date_str,
      repoName: row.repo_name,
      rankNo: Number(row.rank_no),
      totalEvents: Number(row.total_events),
      ma7: Number(row.ma7),
      forecastNextDay: Number(row.forecast_next_day),
    }));
}

export type RepoRankHistoryPoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
};

// Fetches 30-day rank_no / rank_score time-series for the top-N repos.
// IMPORTANT: selection is based on *persistence* over the window (days in top-list
// × average rank_score), not just the latest day. This prevents the "one-day
// wonders" from collapsing the chart into 8 single-point lines.
export async function getTopRepoRankPaths(
  topN = 8,
  days = 30,
): Promise<RepoRankHistoryPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
  }>(`
    WITH date_window AS (
      SELECT DISTINCT metric_date
      FROM batch_repo_rank_score_day
      ORDER BY metric_date DESC
      LIMIT ${Math.max(Number(days), 7)}
    ),
    repo_presence AS (
      SELECT
        r.repo_name AS repo_name,
        count() AS days_in_top,
        avg(r.rank_score) AS mean_score,
        count() * avg(r.rank_score) AS persistence
      FROM batch_repo_rank_score_day r
      INNER JOIN date_window d ON r.metric_date = d.metric_date
      GROUP BY r.repo_name
      HAVING days_in_top >= 2
    ),
    top_repos AS (
      SELECT repo_name
      FROM repo_presence
      ORDER BY persistence DESC, mean_score DESC
      LIMIT ${Math.max(Number(topN), 1)}
    )
    SELECT
      toString(r.metric_date) AS metric_date_str,
      r.repo_name AS repo_name,
      r.rank_no AS rank_no,
      r.rank_score AS rank_score
    FROM batch_repo_rank_score_day r
    INNER JOIN top_repos t ON r.repo_name = t.repo_name
    INNER JOIN date_window d ON r.metric_date = d.metric_date
    ORDER BY r.metric_date ASC, r.rank_no ASC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
  }));
}

export async function getRepoRankHistoryByRepo(
  repoName: string,
  days = 30
): Promise<RepoRankHistoryPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
  }>(`
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score
    FROM batch_repo_rank_score_day
    WHERE repo_name = ${repoLit}
    ORDER BY metric_date DESC
    LIMIT ${Math.max(Number(days), 7)}
  `);

  return rows
    .reverse()
    .map((row) => ({
      metricDate: row.metric_date_str,
      repoName: row.repo_name,
      rankNo: Number(row.rank_no),
      rankScore: Number(row.rank_score),
    }));
}

export async function getRepoTrendForecastByRepo(
  repoName: string,
  days = 30
): Promise<RepoTrendForecastPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    total_events: number;
    ma7: number;
    forecast_next_day: number;
  }>(`
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      total_events,
      ma7,
      forecast_next_day
    FROM batch_repo_trend_forecast
    WHERE repo_name = ${repoLit}
    ORDER BY metric_date DESC
    LIMIT ${Math.max(Number(days), 7)}
  `);

  return rows
    .reverse()
    .map((row) => ({
      metricDate: row.metric_date_str,
      repoName: row.repo_name,
      rankNo: Number(row.rank_no),
      totalEvents: Number(row.total_events),
      ma7: Number(row.ma7),
      forecastNextDay: Number(row.forecast_next_day),
    }));
}

export async function getRepoHotnessComponentsByRepo(
  repoName: string,
  limit = 7
): Promise<HotnessComponentPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    hotness_raw: number;
    watch_contribution: number;
    fork_contribution: number;
    issues_contribution: number;
    pull_request_contribution: number;
    push_contribution: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_hotness_components
      WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      hotness_raw,
      watch_contribution,
      fork_contribution,
      issues_contribution,
      pull_request_contribution,
      push_contribution
    FROM batch_repo_hotness_components
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC, hotness_raw DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    hotnessRaw: Number(row.hotness_raw),
    watchContribution: Number(row.watch_contribution),
    forkContribution: Number(row.fork_contribution),
    issuesContribution: Number(row.issues_contribution),
    pullRequestContribution: Number(row.pull_request_contribution),
    pushContribution: Number(row.push_contribution),
  }));
}

export async function getRepoRankExplainLatest(
  limit = 20
): Promise<RepoRankExplainPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    hotness_part: number;
    momentum_part: number;
    engagement_part: number;
    stability_part: number;
    bot_penalty: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_rank_explain_latest
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      hotness_part,
      momentum_part,
      engagement_part,
      stability_part,
      bot_penalty
    FROM batch_repo_rank_explain_latest
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    hotnessPart: Number(row.hotness_part),
    momentumPart: Number(row.momentum_part),
    engagementPart: Number(row.engagement_part),
    stabilityPart: Number(row.stability_part),
    botPenalty: Number(row.bot_penalty),
  }));
}

export async function getRepoRankDeltaExplainLatest(
  limit = 20
): Promise<RepoRankDeltaExplainPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    delta_rank_no: number;
    delta_rank_score: number;
    delta_hotness_part: number;
    delta_momentum_part: number;
    delta_engagement_part: number;
    delta_stability_part: number;
    delta_bot_penalty: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_rank_delta_explain_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      delta_rank_no,
      delta_rank_score,
      delta_hotness_part,
      delta_momentum_part,
      delta_engagement_part,
      delta_stability_part,
      delta_bot_penalty
    FROM batch_repo_rank_delta_explain_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY abs(delta_rank_score) DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    deltaRankNo: Number(row.delta_rank_no),
    deltaRankScore: Number(row.delta_rank_score),
    deltaHotnessPart: Number(row.delta_hotness_part),
    deltaMomentumPart: Number(row.delta_momentum_part),
    deltaEngagementPart: Number(row.delta_engagement_part),
    deltaStabilityPart: Number(row.delta_stability_part),
    deltaBotPenalty: Number(row.delta_bot_penalty),
  }));
}

export async function getRepoContributorConcentrationLatest(
  limit = 12
): Promise<RepoContributorConcentrationPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    total_events: number;
    active_actors: number;
    top1_actor_share: number;
    top5_actor_share: number;
    actor_hhi: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_contributor_concentration_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      coalesce(rank_score, 0) AS rank_score,
      total_events,
      active_actors,
      top1_actor_share,
      top5_actor_share,
      actor_hhi
    FROM batch_repo_contributor_concentration_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY top1_actor_share DESC, active_actors ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    totalEvents: Number(row.total_events),
    activeActors: Number(row.active_actors),
    top1ActorShare: Number(row.top1_actor_share),
    top5ActorShare: Number(row.top5_actor_share),
    actorHhi: Number(row.actor_hhi),
  }));
}

export async function getRepoContributorConcentrationByRepo(
  repoName: string,
  days = 14
): Promise<RepoContributorConcentrationPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    total_events: number;
    active_actors: number;
    top1_actor_share: number;
    top5_actor_share: number;
    actor_hhi: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_contributor_concentration_day
      WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      coalesce(rank_score, 0) AS rank_score,
      total_events,
      active_actors,
      top1_actor_share,
      top5_actor_share,
      actor_hhi
    FROM batch_repo_contributor_concentration_day
    WHERE repo_name = ${repoLit}
      AND metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    totalEvents: Number(row.total_events),
    activeActors: Number(row.active_actors),
    top1ActorShare: Number(row.top1_actor_share),
    top5ActorShare: Number(row.top5_actor_share),
    actorHhi: Number(row.actor_hhi),
  }));
}

export async function getRepoTopActorsLatestByRepo(
  repoName: string,
  limit = 10
): Promise<RepoTopActorPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    actor_rank: number;
    actor_login: string;
    actor_category: string;
    actor_events: number;
    share: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_top_actors_latest
      WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      actor_rank,
      actor_login,
      actor_category,
      actor_events,
      share
    FROM batch_repo_top_actors_latest
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    ORDER BY actor_rank ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    actorRank: Number(row.actor_rank),
    actorLogin: row.actor_login,
    actorCategory: row.actor_category,
    actorEvents: Number(row.actor_events),
    share: Number(row.share),
  }));
}

export async function getRepoEventTypeShareByRepo(
  repoName: string,
  days = 14
): Promise<RepoEventTypeSharePoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    event_type: string;
    event_count: number;
    total_events: number;
    share: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_event_type_share_day
      WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      event_type,
      event_count,
      total_events,
      share
    FROM batch_repo_event_type_share_day
    WHERE repo_name = ${repoLit}
      AND metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date, share DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    eventType: row.event_type,
    eventCount: Number(row.event_count),
    totalEvents: Number(row.total_events),
    share: Number(row.share),
  }));
}

export async function getRepoEventMixShiftByRepo(
  repoName: string,
  days = 14
): Promise<RepoEventMixShiftPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    js_divergence: number;
    l1_distance: number;
    total_abs_shift: number;
    top_shift_event_type: string;
    top_shift_abs: number;
    top_shift_signed: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_event_mix_shift_day
      WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      js_divergence,
      l1_distance,
      total_abs_shift,
      coalesce(top_shift_event_type, '') AS top_shift_event_type,
      coalesce(top_shift_abs, 0) AS top_shift_abs,
      coalesce(top_shift_signed, 0) AS top_shift_signed
    FROM batch_repo_event_mix_shift_day
    WHERE repo_name = ${repoLit}
      AND metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    jsDivergence: Number(row.js_divergence),
    l1Distance: Number(row.l1_distance),
    totalAbsShift: Number(row.total_abs_shift),
    topShiftEventType: row.top_shift_event_type,
    topShiftAbs: Number(row.top_shift_abs),
    topShiftSigned: Number(row.top_shift_signed),
  }));
}

export async function getConcentrationDay(
  days = 30
): Promise<ConcentrationDayPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    total_events: number;
    repo_count: number;
    top1_share: number;
    top5_share: number;
    hhi: number;
    entropy: number;
    normalized_entropy: number;
    gini: number;
    events_delta_7d: number;
    repo_count_delta_7d: number;
    top5_share_delta_7d: number;
    gini_delta_7d: number;
    entropy_delta_7d: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_concentration_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      total_events,
      repo_count,
      top1_share,
      top5_share,
      hhi,
      entropy,
      normalized_entropy,
      gini,
      events_delta_7d,
      repo_count_delta_7d,
      top5_share_delta_7d,
      gini_delta_7d,
      entropy_delta_7d
    FROM batch_concentration_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    totalEvents: Number(row.total_events),
    repoCount: Number(row.repo_count),
    top1Share: Number(row.top1_share),
    top5Share: Number(row.top5_share),
    hhi: Number(row.hhi),
    entropy: Number(row.entropy),
    normalizedEntropy: Number(row.normalized_entropy),
    gini: Number(row.gini),
    eventsDelta7d: Number(row.events_delta_7d),
    repoCountDelta7d: Number(row.repo_count_delta_7d),
    top5ShareDelta7d: Number(row.top5_share_delta_7d),
    giniDelta7d: Number(row.gini_delta_7d),
    entropyDelta7d: Number(row.entropy_delta_7d),
  }));
}

export async function getEcosystemChangepoints(
  days = 30
): Promise<EcosystemChangepoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    kind: string;
    cusum: number;
    z_score: number;
    contribution_top_type: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_concentration_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      kind,
      cusum,
      z_score,
      contribution_top_type
    FROM batch_ecosystem_changepoints
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    kind: row.kind === "drop" ? "drop" : "burst",
    cusum: Number(row.cusum),
    zScore: Number(row.z_score),
    contributionTopType: row.contribution_top_type,
  }));
}

export async function getActorCohortDay(
  days = 30
): Promise<ActorCohortDayPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    cohort: string;
    actors: number;
    events: number;
    avg_unique_repos: number;
    avg_reactivation_gap_days: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_actor_cohort_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      cohort,
      actors,
      events,
      avg_unique_repos,
      avg_reactivation_gap_days
    FROM batch_actor_cohort_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date, cohort
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    cohort: row.cohort,
    actors: Number(row.actors),
    events: Number(row.events),
    avgUniqueRepos: Number(row.avg_unique_repos),
    avgReactivationGapDays: Number(row.avg_reactivation_gap_days),
  }));
}

export async function getEventTypeShareShiftDay(
  days = 30
): Promise<EventTypeShareShiftDayPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    event_type: string;
    event_count: number;
    share: number;
    share_shift: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_event_type_share_shift_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      event_type,
      event_count,
      share,
      share_shift
    FROM batch_event_type_share_shift_day
    WHERE metric_date >= subtractDays((SELECT d FROM latest_date), ${Math.max(Number(days) - 1, 0)})
      AND metric_date <= (SELECT d FROM latest_date)
    ORDER BY metric_date, event_count DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    eventType: row.event_type,
    eventCount: Number(row.event_count),
    share: Number(row.share),
    shareShift: Number(row.share_shift),
  }));
}

export async function getOfflineInsightsLatest(
  limit = 8
): Promise<OfflineInsightPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    insight_type: string;
    insight_text: string;
    evidence_json: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_offline_insights_latest
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      insight_type,
      insight_text,
      evidence_json
    FROM batch_offline_insights_latest
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY insight_type
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    insightType: row.insight_type,
    insightText: row.insight_text,
    evidenceJson: row.evidence_json,
  }));
}

export async function getBurstStabilitySnapshot(
  limit = 60
): Promise<BurstStabilityPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    burst_index: number;
    stability_index: number;
    short_term_pressure: number;
    quadrant: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_burst_stability
    ),
    base AS (
      SELECT
        toString(metric_date) AS metric_date_str,
        repo_name,
        coalesce(rank_no, 999999) AS rank_no,
        coalesce(rank_score, 0.0) AS rank_score,
        burst_index,
        stability_index,
        short_term_pressure,
        quadrant
      FROM batch_repo_burst_stability
      WHERE metric_date = (SELECT d FROM latest_date)
        AND rank_score > 0
    ),
    -- Take top-N from each of the four quadrants, ordered by rank_score.
    -- This guarantees the scatter plot covers the full burst/stability plane
    -- instead of collapsing onto a single (max-burst, min-stability) pixel.
    ranked AS (
      SELECT
        *,
        row_number() OVER (PARTITION BY quadrant ORDER BY rank_score DESC) AS rn
      FROM base
    )
    SELECT
      metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      burst_index,
      stability_index,
      short_term_pressure,
      quadrant
    FROM ranked
    WHERE rn <= ceil(${Math.max(Number(limit), 1)} / 4.0)
    ORDER BY rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    burstIndex: Number(row.burst_index),
    stabilityIndex: Number(row.stability_index),
    shortTermPressure: Number(row.short_term_pressure),
    quadrant: row.quadrant,
  }));
}

export async function getDeveloperRhythmHeatmap(
  actorCategory: "human" | "bot" = "human"
): Promise<RhythmHeatmapCell[]> {
  const category = actorCategory === "bot" ? "bot" : "human";
  const rows = await queryJson<{
    metric_date_str: string;
    day_of_week: number;
    hour_of_day: number;
    actor_category: string;
    event_count: number;
    intensity_score: number;
    peak_flag: boolean;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_developer_rhythm_heatmap
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      day_of_week,
      hour_of_day,
      actor_category,
      event_count,
      intensity_score,
      peak_flag
    FROM batch_developer_rhythm_heatmap
    WHERE metric_date = (SELECT d FROM latest_date)
      AND actor_category = '${category}'
    ORDER BY day_of_week, hour_of_day
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    dayOfWeek: Number(row.day_of_week),
    hourOfDay: Number(row.hour_of_day),
    actorCategory: row.actor_category,
    eventCount: Number(row.event_count),
    intensityScore: Number(row.intensity_score),
    peakFlag: Boolean(row.peak_flag),
  }));
}

export async function getHotnessComponentsLatest(
  limit = 10
): Promise<HotnessComponentPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    hotness_raw: number;
    watch_contribution: number;
    fork_contribution: number;
    issues_contribution: number;
    pull_request_contribution: number;
    push_contribution: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_hotness_components
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      hotness_raw,
      watch_contribution,
      fork_contribution,
      issues_contribution,
      pull_request_contribution,
      push_contribution
    FROM batch_repo_hotness_components
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC, hotness_raw DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    hotnessRaw: Number(row.hotness_raw),
    watchContribution: Number(row.watch_contribution),
    forkContribution: Number(row.fork_contribution),
    issuesContribution: Number(row.issues_contribution),
    pullRequestContribution: Number(row.pull_request_contribution),
    pushContribution: Number(row.push_contribution),
  }));
}

export async function getOrgRankLatest(limit = 12): Promise<OrgRankPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    org_or_owner: string;
    rank_no: number;
    hotness_score: number;
    event_count: number;
    unique_repos: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_org_rank_latest
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      org_or_owner,
      rank_no,
      hotness_score,
      event_count,
      unique_repos
    FROM batch_org_rank_latest
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    orgOrOwner: row.org_or_owner,
    rankNo: Number(row.rank_no),
    hotnessScore: Number(row.hotness_score),
    eventCount: Number(row.event_count),
    uniqueRepos: Number(row.unique_repos),
  }));
}

export async function getEventActionsLatest(
  eventType: "PullRequestEvent" | "IssuesEvent" = "PullRequestEvent",
  limit = 12
): Promise<EventActionPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    event_type: string;
    payload_action: string;
    event_count: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_event_action_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      event_type,
      payload_action,
      event_count
    FROM batch_event_action_day
    WHERE metric_date = (SELECT d FROM latest_date)
      AND event_type = '${eventType}'
    ORDER BY event_count DESC, payload_action ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    eventType: row.event_type,
    payloadAction: row.payload_action,
    eventCount: Number(row.event_count),
  }));
}

export async function getPayloadBucketsLatest(): Promise<PayloadBucketPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    size_bucket: string;
    event_count: number;
    avg_payload_size: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_payload_bucket_day
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      size_bucket,
      event_count,
      avg_payload_size
    FROM batch_payload_bucket_day
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY
      multiIf(size_bucket='0', 0, size_bucket='1-2', 1, size_bucket='3-5', 2, size_bucket='6-10', 3, 4) ASC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    sizeBucket: row.size_bucket,
    eventCount: Number(row.event_count),
    avgPayloadSize: Number(row.avg_payload_size),
  }));
}

export async function getUserSegmentsLatest(): Promise<UserSegmentPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    segment: string;
    users: number;
    events: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_user_segment_latest
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      segment,
      countDistinct(actor_login) AS users,
      sum(event_count) AS events
    FROM batch_user_segment_latest
    WHERE metric_date = (SELECT d FROM latest_date)
    GROUP BY metric_date, segment
    ORDER BY users DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    segment: row.segment,
    users: Number(row.users),
    events: Number(row.events),
  }));
}

export async function getRepoHealthLatest(limit = 12): Promise<RepoHealthPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    health_score: number;
    bot_ratio: number;
    dev_norm: number;
    community_norm: number;
    attention_norm: number;
    diversity_norm: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_health_latest
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      coalesce(rank_no, 999999) AS rank_no,
      health_score,
      bot_ratio,
      dev_norm,
      community_norm,
      attention_norm,
      diversity_norm
    FROM batch_repo_health_latest
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY health_score DESC, rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    healthScore: Number(row.health_score),
    botRatio: Number(row.bot_ratio),
    devNorm: Number(row.dev_norm),
    communityNorm: Number(row.community_norm),
    attentionNorm: Number(row.attention_norm),
    diversityNorm: Number(row.diversity_norm),
  }));
}

export async function getOfflineAnomalyAlertsLatest(
  limit = 20
): Promise<OfflineAnomalyAlertPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    current_events: number;
    baseline_mean: number;
    z_score: number;
    alert_level: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_offline_anomaly_alerts
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      current_events,
      baseline_mean,
      z_score,
      alert_level
    FROM batch_offline_anomaly_alerts
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY z_score DESC, current_events DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    currentEvents: Number(row.current_events),
    baselineMean: Number(row.baseline_mean),
    zScore: Number(row.z_score),
    alertLevel: row.alert_level,
  }));
}

export async function getOfflineDeclineWarningsLatest(
  limit = 20
): Promise<OfflineDeclineWarningPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    slope7: number;
    pct_change7: number;
    warning_level: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_offline_decline_warnings
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      slope7,
      pct_change7,
      warning_level
    FROM batch_offline_decline_warnings
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY slope7 ASC, pct_change7 ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    slope7: Number(row.slope7),
    pctChange7: Number(row.pct_change7),
    warningLevel: row.warning_level,
  }));
}

export async function getRepoClustersLatest(limit = 120): Promise<RepoClusterPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    cluster_id: number;
    pca_x: number;
    pca_y: number;
    rank_no: number;
    health_score: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d
      FROM batch_repo_clusters
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      cluster_id,
      pca_x,
      pca_y,
      coalesce(rank_no, 999999) AS rank_no,
      coalesce(health_score, 0.0) AS health_score
    FROM batch_repo_clusters
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY health_score DESC, rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    clusterId: Number(row.cluster_id),
    pcaX: Number(row.pca_x),
    pcaY: Number(row.pca_y),
    rankNo: Number(row.rank_no),
    healthScore: Number(row.health_score),
  }));
}

// ────────────────────────────────────────────────────────────
// Deep analytics (L1–L5) — total → local narrative fetchers
// ────────────────────────────────────────────────────────────

export type RepoClusterProfilePoint = {
  metricDate: string;
  clusterId: number;
  clusterLabel: string;
  members: number;
  shareOfRepos: number;
  avgHotness: number;
  avgMomentum: number;
  avgEngagement: number;
  avgStability: number;
  avgBotRatio: number;
  avgRankScore: number;
  sampleRepos: string;
};

export async function getRepoClusterProfileLatest(): Promise<RepoClusterProfilePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    cluster_id: number;
    cluster_label: string;
    members: number;
    share_of_repos: number;
    avg_hotness: number;
    avg_momentum: number;
    avg_engagement: number;
    avg_stability: number;
    avg_bot_ratio: number;
    avg_rank_score: number;
    sample_repos: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_cluster_profile
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      cluster_id,
      cluster_label,
      members,
      share_of_repos,
      avg_hotness,
      avg_momentum,
      avg_engagement,
      avg_stability,
      avg_bot_ratio,
      avg_rank_score,
      sample_repos
    FROM batch_repo_cluster_profile
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY avg_rank_score DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    clusterId: Number(row.cluster_id),
    clusterLabel: row.cluster_label,
    members: Number(row.members),
    shareOfRepos: Number(row.share_of_repos),
    avgHotness: Number(row.avg_hotness),
    avgMomentum: Number(row.avg_momentum),
    avgEngagement: Number(row.avg_engagement),
    avgStability: Number(row.avg_stability),
    avgBotRatio: Number(row.avg_bot_ratio),
    avgRankScore: Number(row.avg_rank_score),
    sampleRepos: row.sample_repos,
  }));
}

export type HotVsColdAttributionPoint = {
  metricDate: string;
  cohortScope: "all" | "humans_only" | "bots_only";
  featureName: string;
  meanHot: number;
  meanCold: number;
  meanDiff: number;
  cohenD: number;
  cohensDLow: number;
  cohensDHigh: number;
  tStat: number;
  nHot: number;
  nCold: number;
  direction: string;
};

export async function getHotVsColdAttribution(
  limit = 40,
  cohortScope: "all" | "humans_only" | "bots_only" | "__all__" = "__all__",
): Promise<HotVsColdAttributionPoint[]> {
  const scopeFilter =
    cohortScope === "__all__"
      ? ""
      : `AND cohort_scope = '${String(cohortScope).replace(/[^a-z_]/g, "")}'`;
  const rows = await queryJson<{
    metric_date_str: string;
    cohort_scope: string;
    feature_name: string;
    mean_hot: number;
    mean_cold: number;
    mean_diff: number;
    cohen_d: number;
    cohens_d_low: number;
    cohens_d_high: number;
    t_stat: number;
    n_hot: number;
    n_cold: number;
    direction: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_hot_vs_cold_attribution
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      cohort_scope,
      feature_name,
      mean_hot,
      mean_cold,
      mean_diff,
      cohen_d,
      cohens_d_low,
      cohens_d_high,
      t_stat,
      n_hot,
      n_cold,
      direction
    FROM batch_hot_vs_cold_attribution
    WHERE metric_date = (SELECT d FROM latest_date)
      ${scopeFilter}
    ORDER BY cohort_scope, abs(cohen_d) DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => {
    const rawScope = (row.cohort_scope || "all").toLowerCase();
    const scope: HotVsColdAttributionPoint["cohortScope"] =
      rawScope === "humans_only" ? "humans_only" : rawScope === "bots_only" ? "bots_only" : "all";
    return {
      metricDate: row.metric_date_str,
      cohortScope: scope,
      featureName: row.feature_name,
      meanHot: Number(row.mean_hot),
      meanCold: Number(row.mean_cold),
      meanDiff: Number(row.mean_diff),
      cohenD: Number(row.cohen_d),
      cohensDLow: Number(row.cohens_d_low),
      cohensDHigh: Number(row.cohens_d_high),
      tStat: Number(row.t_stat),
      nHot: Number(row.n_hot),
      nCold: Number(row.n_cold),
      direction: row.direction,
    };
  });
}

export type RepoDnaOutlierPoint = {
  metricDate: string;
  repoName: string;
  zDistance: number;
  offFeatures: string;
  watchShare: number;
  prPushRatio: number;
  botRatio: number;
  activeDaysRatio: number;
  logTotalEvents: number;
};

export async function getRepoDnaOutliers(limit = 20): Promise<RepoDnaOutlierPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    z_distance: number;
    off_features: string;
    watch_share: number;
    pr_push_ratio: number;
    bot_ratio: number;
    active_days_ratio: number;
    log_total_events: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_dna_outliers
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      z_distance,
      off_features,
      watch_share,
      pr_push_ratio,
      bot_ratio,
      active_days_ratio,
      log_total_events
    FROM batch_repo_dna_outliers
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY z_distance DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    zDistance: Number(row.z_distance),
    offFeatures: row.off_features,
    watchShare: Number(row.watch_share),
    prPushRatio: Number(row.pr_push_ratio),
    botRatio: Number(row.bot_ratio),
    activeDaysRatio: Number(row.active_days_ratio),
    logTotalEvents: Number(row.log_total_events),
  }));
}

export type RepoDnaPoint = {
  metricDate: string;
  repoName: string;
  cohortGroup: string;
  watchShare: number;
  forkShare: number;
  issuesShare: number;
  prShare: number;
  pushShare: number;
  botRatio: number;
  nightRatio: number;
  weekendRatio: number;
  prPushRatio: number;
  activeDaysRatio: number;
  actorsPerEvent: number;
  eventEntropy: number;
  top1ActorShare: number;
  logTotalEvents: number;
  logPayloadP95: number;
  rankScore: number;
  totalEvents: number;
};

function mapRepoDnaRow(row: {
  metric_date_str: string;
  repo_name: string;
  cohort_group: string;
  watch_share: number;
  fork_share: number;
  issues_share: number;
  pr_share: number;
  push_share: number;
  bot_ratio: number;
  night_ratio: number;
  weekend_ratio: number;
  pr_push_ratio: number;
  active_days_ratio: number;
  actors_per_event: number;
  event_entropy: number;
  top1_actor_share: number;
  log_total_events: number;
  log_payload_p95: number;
  rank_score: number;
  total_events: number;
}): RepoDnaPoint {
  return {
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    cohortGroup: row.cohort_group,
    watchShare: Number(row.watch_share),
    forkShare: Number(row.fork_share),
    issuesShare: Number(row.issues_share),
    prShare: Number(row.pr_share),
    pushShare: Number(row.push_share),
    botRatio: Number(row.bot_ratio),
    nightRatio: Number(row.night_ratio),
    weekendRatio: Number(row.weekend_ratio),
    prPushRatio: Number(row.pr_push_ratio),
    activeDaysRatio: Number(row.active_days_ratio),
    actorsPerEvent: Number(row.actors_per_event),
    eventEntropy: Number(row.event_entropy),
    top1ActorShare: Number(row.top1_actor_share),
    logTotalEvents: Number(row.log_total_events),
    logPayloadP95: Number(row.log_payload_p95),
    rankScore: Number(row.rank_score),
    totalEvents: Number(row.total_events),
  };
}

export type RepoDnaCohortStats = {
  cohortGroup: "hot" | "mid" | "cold";
  repos: number;
  avgEvents: number;
  watchShare: number;
  forkShare: number;
  issuesShare: number;
  prShare: number;
  pushShare: number;
  botRatio: number;
  nightRatio: number;
  top1ActorShare: number;
  eventEntropy: number;
};

/**
 * Average DNA fingerprint per cohort (hot / mid / cold). Sampling the top
 * 10 hot-cohort repos by rank_score surfaces degenerate push-spam outliers
 * (watch_share≈0, push_share≈1, entropy=0) — comparing against the cohort
 * MEAN reveals that the real hot population is far more diverse, and the
 * leaderboard tail is the pathology. This is what makes the 10-row table
 * below actually interpretable.
 */
export async function getRepoDnaCohortStats(): Promise<RepoDnaCohortStats[]> {
  const rows = await queryJson<{
    cohort_group: string;
    repos: number;
    avg_events: number;
    watch_share: number;
    fork_share: number;
    issues_share: number;
    pr_share: number;
    push_share: number;
    bot_ratio: number;
    night_ratio: number;
    top1_actor_share: number;
    event_entropy: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_dna)
    SELECT
      cohort_group,
      count() AS repos,
      avg(total_events) AS avg_events,
      avg(watch_share) AS watch_share,
      avg(fork_share) AS fork_share,
      avg(issues_share) AS issues_share,
      avg(pr_share) AS pr_share,
      avg(push_share) AS push_share,
      avg(bot_ratio) AS bot_ratio,
      avg(night_ratio) AS night_ratio,
      avg(top1_actor_share) AS top1_actor_share,
      avg(event_entropy) AS event_entropy
    FROM batch_repo_dna
    WHERE metric_date = (SELECT d FROM latest)
    GROUP BY cohort_group
  `);
  const order: Record<string, number> = { hot: 0, mid: 1, cold: 2 };
  return rows
    .map((r) => ({
      cohortGroup: (r.cohort_group as RepoDnaCohortStats["cohortGroup"]) || "mid",
      repos: Number(r.repos),
      avgEvents: Number(r.avg_events),
      watchShare: Number(r.watch_share),
      forkShare: Number(r.fork_share),
      issuesShare: Number(r.issues_share),
      prShare: Number(r.pr_share),
      pushShare: Number(r.push_share),
      botRatio: Number(r.bot_ratio),
      nightRatio: Number(r.night_ratio),
      top1ActorShare: Number(r.top1_actor_share),
      eventEntropy: Number(r.event_entropy),
    }))
    .sort((a, b) => (order[a.cohortGroup] ?? 9) - (order[b.cohortGroup] ?? 9));
}

export async function getRepoDnaTopHot(limit = 30): Promise<RepoDnaPoint[]> {
  const rows = await queryJson<Parameters<typeof mapRepoDnaRow>[0]>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_dna
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      cohort_group,
      watch_share,
      fork_share,
      issues_share,
      pr_share,
      push_share,
      bot_ratio,
      night_ratio,
      weekend_ratio,
      pr_push_ratio,
      active_days_ratio,
      actors_per_event,
      event_entropy,
      top1_actor_share,
      log_total_events,
      log_payload_p95,
      rank_score,
      total_events
    FROM batch_repo_dna
    WHERE metric_date = (SELECT d FROM latest_date)
      AND cohort_group = 'hot'
    ORDER BY rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map(mapRepoDnaRow);
}

export async function getRepoDnaByRepo(repoName: string): Promise<RepoDnaPoint | null> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<Parameters<typeof mapRepoDnaRow>[0]>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_dna WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      cohort_group,
      watch_share,
      fork_share,
      issues_share,
      pr_share,
      push_share,
      bot_ratio,
      night_ratio,
      weekend_ratio,
      pr_push_ratio,
      active_days_ratio,
      actors_per_event,
      event_entropy,
      top1_actor_share,
      log_total_events,
      log_payload_p95,
      rank_score,
      total_events
    FROM batch_repo_dna
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    LIMIT 1
  `);
  if (rows.length === 0) return null;
  return mapRepoDnaRow(rows[0]);
}

export type ActorPersonaPoint = {
  metricDate: string;
  actorLogin: string;
  personaId: number;
  personaLabel: string;
  isBot: number;
  eventCount: number;
  activeDays: number;
  uniqueRepos: number;
  nightRatio: number;
  weekendRatio: number;
  pushShare: number;
  prShare: number;
  issuesShare: number;
  watchShare: number;
  forkShare: number;
  hourEntropy: number;
  repoEntropy: number;
  pcaX: number;
  pcaY: number;
};

export async function getActorPersonaSample(limit = 2000): Promise<ActorPersonaPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    persona_id: number;
    persona_label: string;
    is_bot: number;
    event_count: number;
    active_days: number;
    unique_repos: number;
    night_ratio: number;
    weekend_ratio: number;
    push_share: number;
    pr_share: number;
    issues_share: number;
    watch_share: number;
    fork_share: number;
    hour_entropy: number;
    repo_entropy: number;
    pca_x: number;
    pca_y: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_actor_persona
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      persona_id,
      persona_label,
      is_bot,
      event_count,
      active_days,
      unique_repos,
      night_ratio,
      weekend_ratio,
      push_share,
      pr_share,
      issues_share,
      watch_share,
      fork_share,
      hour_entropy,
      repo_entropy,
      pca_x,
      pca_y
    FROM batch_actor_persona
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY event_count DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    personaId: Number(row.persona_id),
    personaLabel: row.persona_label,
    isBot: Number(row.is_bot),
    eventCount: Number(row.event_count),
    activeDays: Number(row.active_days),
    uniqueRepos: Number(row.unique_repos),
    nightRatio: Number(row.night_ratio),
    weekendRatio: Number(row.weekend_ratio),
    pushShare: Number(row.push_share),
    prShare: Number(row.pr_share),
    issuesShare: Number(row.issues_share),
    watchShare: Number(row.watch_share),
    forkShare: Number(row.fork_share),
    hourEntropy: Number(row.hour_entropy),
    repoEntropy: Number(row.repo_entropy),
    pcaX: Number(row.pca_x),
    pcaY: Number(row.pca_y),
  }));
}

export type ActorPersonaCentroidPoint = {
  metricDate: string;
  personaId: number;
  personaLabel: string;
  members: number;
  share: number;
  logEventCountAvg: number;
  activeDaysAvg: number;
  logUniqueReposAvg: number;
  nightRatioAvg: number;
  weekendRatioAvg: number;
  hourEntropyAvg: number;
  repoEntropyAvg: number;
  pushShareAvg: number;
  prShareAvg: number;
  issuesShareAvg: number;
  watchShareAvg: number;
  forkShareAvg: number;
  isBotAvg: number;
};

export async function getActorPersonaCentroids(): Promise<ActorPersonaCentroidPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    persona_id: number;
    persona_label: string;
    members: number;
    share: number;
    log_event_count_avg: number;
    active_days_f_avg: number;
    log_unique_repos_avg: number;
    night_ratio_avg: number;
    weekend_ratio_avg: number;
    hour_entropy_avg: number;
    repo_entropy_avg: number;
    push_share_avg: number;
    pr_share_avg: number;
    issues_share_avg: number;
    watch_share_avg: number;
    fork_share_avg: number;
    is_bot_f_avg: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_actor_persona_centroid
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      persona_id,
      persona_label,
      members,
      share,
      log_event_count_avg,
      active_days_f_avg,
      log_unique_repos_avg,
      night_ratio_avg,
      weekend_ratio_avg,
      hour_entropy_avg,
      repo_entropy_avg,
      push_share_avg,
      pr_share_avg,
      issues_share_avg,
      watch_share_avg,
      fork_share_avg,
      is_bot_f_avg
    FROM batch_actor_persona_centroid
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY members DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    personaId: Number(row.persona_id),
    personaLabel: row.persona_label,
    members: Number(row.members),
    share: Number(row.share),
    logEventCountAvg: Number(row.log_event_count_avg),
    activeDaysAvg: Number(row.active_days_f_avg),
    logUniqueReposAvg: Number(row.log_unique_repos_avg),
    nightRatioAvg: Number(row.night_ratio_avg),
    weekendRatioAvg: Number(row.weekend_ratio_avg),
    hourEntropyAvg: Number(row.hour_entropy_avg),
    repoEntropyAvg: Number(row.repo_entropy_avg),
    pushShareAvg: Number(row.push_share_avg),
    prShareAvg: Number(row.pr_share_avg),
    issuesShareAvg: Number(row.issues_share_avg),
    watchShareAvg: Number(row.watch_share_avg),
    forkShareAvg: Number(row.fork_share_avg),
    isBotAvg: Number(row.is_bot_f_avg),
  }));
}

export type ActorPersonaBicPoint = {
  metricDate: string;
  k: number;
  logLikelihood: number;
  bic: number;
  nParams: number;
  nSamples: number;
  isSelected: boolean;
};

export async function getActorPersonaBic(): Promise<ActorPersonaBicPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    k: number;
    log_likelihood: number;
    bic: number;
    n_params: number;
    n_samples: number;
    is_selected: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_actor_persona_bic
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      k,
      log_likelihood,
      bic,
      n_params,
      n_samples,
      is_selected
    FROM batch_actor_persona_bic
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY k
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    k: Number(row.k),
    logLikelihood: Number(row.log_likelihood),
    bic: Number(row.bic),
    nParams: Number(row.n_params),
    nSamples: Number(row.n_samples),
    isSelected: Number(row.is_selected) === 1,
  }));
}

export type ActorPersonaBotValidationPoint = {
  metricDate: string;
  personaLabel: string;
  trueBots: number;
  falseBots: number;
  missedBots: number;
  precision: number;
  recall: number;
  f1: number;
  totalBots: number;
  totalHumans: number;
};

export async function getActorPersonaBotValidation(): Promise<ActorPersonaBotValidationPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    persona_label: string;
    true_bots: number;
    false_bots: number;
    missed_bots: number;
    precision: number;
    recall: number;
    f1: number;
    total_bots: number;
    total_humans: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_actor_persona_bot_validation
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      persona_label,
      true_bots,
      false_bots,
      missed_bots,
      precision,
      recall,
      f1,
      total_bots,
      total_humans
    FROM batch_actor_persona_bot_validation
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY f1 DESC
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    personaLabel: row.persona_label,
    trueBots: Number(row.true_bots),
    falseBots: Number(row.false_bots),
    missedBots: Number(row.missed_bots),
    precision: Number(row.precision),
    recall: Number(row.recall),
    f1: Number(row.f1),
    totalBots: Number(row.total_bots),
    totalHumans: Number(row.total_humans),
  }));
}

export type ActorPersonaTransitionPoint = {
  metricDate: string;
  personaEarly: string;
  personaLate: string;
  actors: number;
  rowTotal: number;
  transitionProb: number;
  isStable: boolean;
};

export async function getActorPersonaTransition(): Promise<ActorPersonaTransitionPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    persona_early: string;
    persona_late: string;
    actors: number;
    row_total: number;
    transition_prob: number;
    is_stable: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_actor_persona_transition
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      persona_early,
      persona_late,
      actors,
      row_total,
      transition_prob,
      is_stable
    FROM batch_actor_persona_transition
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY persona_early, transition_prob DESC
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    personaEarly: row.persona_early,
    personaLate: row.persona_late,
    actors: Number(row.actors),
    rowTotal: Number(row.row_total),
    transitionProb: Number(row.transition_prob),
    isStable: Number(row.is_stable) === 1,
  }));
}

export type RepoWatcherPersonaLiftPoint = {
  metricDate: string;
  repoName: string;
  personaLabel: string;
  watchers: number;
  shareInWatchers: number;
  shareInGlobal: number;
  lift: number;
};

export async function getAllHotWatcherPersonaLift(): Promise<RepoWatcherPersonaLiftPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    persona_label: string;
    watchers: number;
    share_in_watchers: number;
    share_in_global: number;
    lift: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_watcher_persona_lift
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      persona_label,
      watchers,
      share_in_watchers,
      share_in_global,
      lift
    FROM batch_repo_watcher_persona_lift
    WHERE metric_date = (SELECT d FROM latest_date)
      AND repo_name = '__ALL_HOT__'
    ORDER BY lift DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    personaLabel: row.persona_label,
    watchers: Number(row.watchers),
    shareInWatchers: Number(row.share_in_watchers),
    shareInGlobal: Number(row.share_in_global),
    lift: Number(row.lift),
  }));
}

export async function getRepoWatcherPersonaLiftByRepo(
  repoName: string
): Promise<RepoWatcherPersonaLiftPoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    persona_label: string;
    watchers: number;
    share_in_watchers: number;
    share_in_global: number;
    lift: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_watcher_persona_lift
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      persona_label,
      watchers,
      share_in_watchers,
      share_in_global,
      lift
    FROM batch_repo_watcher_persona_lift
    WHERE metric_date = (SELECT d FROM latest_date)
      AND repo_name = ${repoLit}
    ORDER BY lift DESC
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    personaLabel: row.persona_label,
    watchers: Number(row.watchers),
    shareInWatchers: Number(row.share_in_watchers),
    shareInGlobal: Number(row.share_in_global),
    lift: Number(row.lift),
  }));
}

export type RepoWatcherProfilePoint = {
  metricDate: string;
  repoName: string;
  rankNo: number;
  rankScore: number;
  watchers: number;
  avgNightRatio: number;
  avgPrPushRatio: number;
  avgUniqueRepos: number;
  botRatio: number;
  dominantPersona: string;
  dominantShare: number;
  dominantLift: number;
};

export async function getRepoWatcherProfileLatest(limit = 30): Promise<RepoWatcherProfilePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    watchers: number;
    avg_night_ratio: number;
    avg_pr_push_ratio: number;
    avg_unique_repos: number;
    bot_ratio: number;
    dominant_persona: string;
    dominant_share: number;
    dominant_lift: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_watcher_profile
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      watchers,
      avg_night_ratio,
      avg_pr_push_ratio,
      avg_unique_repos,
      bot_ratio,
      dominant_persona,
      dominant_share,
      dominant_lift
    FROM batch_repo_watcher_profile
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY rank_no ASC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    watchers: Number(row.watchers),
    avgNightRatio: Number(row.avg_night_ratio),
    avgPrPushRatio: Number(row.avg_pr_push_ratio),
    avgUniqueRepos: Number(row.avg_unique_repos),
    botRatio: Number(row.bot_ratio),
    dominantPersona: row.dominant_persona,
    dominantShare: Number(row.dominant_share),
    dominantLift: Number(row.dominant_lift),
  }));
}

export async function getRepoWatcherProfileByRepo(
  repoName: string
): Promise<RepoWatcherProfilePoint | null> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_no: number;
    rank_score: number;
    watchers: number;
    avg_night_ratio: number;
    avg_pr_push_ratio: number;
    avg_unique_repos: number;
    bot_ratio: number;
    dominant_persona: string;
    dominant_share: number;
    dominant_lift: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_watcher_profile WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_no,
      rank_score,
      watchers,
      avg_night_ratio,
      avg_pr_push_ratio,
      avg_unique_repos,
      bot_ratio,
      dominant_persona,
      dominant_share,
      dominant_lift
    FROM batch_repo_watcher_profile
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    LIMIT 1
  `);
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    watchers: Number(row.watchers),
    avgNightRatio: Number(row.avg_night_ratio),
    avgPrPushRatio: Number(row.avg_pr_push_ratio),
    avgUniqueRepos: Number(row.avg_unique_repos),
    botRatio: Number(row.bot_ratio),
    dominantPersona: row.dominant_persona,
    dominantShare: Number(row.dominant_share),
    dominantLift: Number(row.dominant_lift),
  };
}

export type RepoSimilarityEdgePoint = {
  metricDate: string;
  srcRepo: string;
  dstRepo: string;
  sharedActors: number;
  jaccard: number;
};

export async function getRepoSimilarityEdges(limit = 400): Promise<RepoSimilarityEdgePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    src_repo: string;
    dst_repo: string;
    shared_actors: number;
    jaccard: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_similarity_edges
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      src_repo,
      dst_repo,
      shared_actors,
      jaccard
    FROM batch_repo_similarity_edges
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY jaccard DESC, shared_actors DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    srcRepo: row.src_repo,
    dstRepo: row.dst_repo,
    sharedActors: Number(row.shared_actors),
    jaccard: Number(row.jaccard),
  }));
}

export async function getRepoSimilarityEdgesByRepo(
  repoName: string,
  limit = 10
): Promise<RepoSimilarityEdgePoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    src_repo: string;
    dst_repo: string;
    shared_actors: number;
    jaccard: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_similarity_edges
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      src_repo,
      dst_repo,
      shared_actors,
      jaccard
    FROM batch_repo_similarity_edges
    WHERE metric_date = (SELECT d FROM latest_date)
      AND src_repo = ${repoLit}
    ORDER BY jaccard DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    srcRepo: row.src_repo,
    dstRepo: row.dst_repo,
    sharedActors: Number(row.shared_actors),
    jaccard: Number(row.jaccard),
  }));
}

export type RepoCommunityPoint = {
  metricDate: string;
  repoName: string;
  communityId: string;
  communitySize: number;
  rankNo: number;
  rankScore: number;
  sampleMembers: string;
};

export async function getRepoCommunitiesTop(limit = 8): Promise<RepoCommunityPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    community_id: string;
    community_size: number;
    rank_no: number;
    rank_score: number;
    sample_members: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_community
    ),
    community_stats AS (
      SELECT
        community_id,
        max(community_size) AS cs,
        avg(rank_score) AS avg_rs,
        any(sample_members) AS sample_members_any
      FROM batch_repo_community
      WHERE metric_date = (SELECT d FROM latest_date)
        AND community_size >= 2
      GROUP BY community_id
    ),
    top_communities AS (
      SELECT community_id
      FROM community_stats
      ORDER BY cs DESC, avg_rs DESC
      LIMIT ${Math.max(Number(limit), 1)}
    )
    SELECT
      toString(bc.metric_date) AS metric_date_str,
      bc.repo_name,
      bc.community_id,
      bc.community_size,
      bc.rank_no,
      bc.rank_score,
      bc.sample_members
    FROM batch_repo_community bc
    WHERE bc.metric_date = (SELECT d FROM latest_date)
      AND bc.community_id IN (SELECT community_id FROM top_communities)
    ORDER BY bc.community_size DESC, bc.rank_score DESC, bc.repo_name
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    communityId: row.community_id,
    communitySize: Number(row.community_size),
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    sampleMembers: row.sample_members,
  }));
}

export async function getRepoCommunityByRepo(
  repoName: string
): Promise<RepoCommunityPoint | null> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    community_id: string;
    community_size: number;
    rank_no: number;
    rank_score: number;
    sample_members: string;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_community WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      community_id,
      community_size,
      rank_no,
      rank_score,
      sample_members
    FROM batch_repo_community
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    LIMIT 1
  `);
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    communityId: row.community_id,
    communitySize: Number(row.community_size),
    rankNo: Number(row.rank_no),
    rankScore: Number(row.rank_score),
    sampleMembers: row.sample_members,
  };
}

export type RepoAssociationRulePoint = {
  metricDate: string;
  antecedent: string;
  consequent: string;
  antecedentSize: number;
  support: number;
  confidence: number;
  lift: number;
  isFrontier: number;
};

export async function getRepoAssociationRules(limit = 40): Promise<RepoAssociationRulePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    antecedent: string;
    consequent: string;
    antecedent_size: number;
    support: number;
    confidence: number;
    lift: number;
    is_frontier: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_association_rules
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      antecedent,
      consequent,
      antecedent_size,
      support,
      confidence,
      lift,
      is_frontier
    FROM batch_repo_association_rules
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY lift DESC, confidence DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    antecedent: row.antecedent,
    consequent: row.consequent,
    antecedentSize: Number(row.antecedent_size),
    support: Number(row.support),
    confidence: Number(row.confidence),
    lift: Number(row.lift),
    isFrontier: Number(row.is_frontier ?? 0),
  }));
}

export type RepoCommunityProfilePoint = {
  metricDate: string;
  communityId: string;
  communitySize: number;
  topMembers: string;
  avgRankScore: number;
  avgActiveDays: number;
  totalEvents: number;
  watchShare: number;
  prPushRatio: number;
  botRatio: number;
};

export async function getRepoCommunityProfile(limit = 20): Promise<RepoCommunityProfilePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    community_id: string;
    community_size: number;
    top_members: string;
    avg_rank_score: number;
    avg_active_days: number;
    total_events: number;
    watch_share: number;
    pr_push_ratio: number;
    bot_ratio: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_community_profile
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      community_id,
      community_size,
      top_members,
      avg_rank_score,
      avg_active_days,
      total_events,
      watch_share,
      pr_push_ratio,
      bot_ratio
    FROM batch_repo_community_profile
    WHERE metric_date = (SELECT d FROM latest_date)
    ORDER BY community_size DESC, total_events DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    communityId: row.community_id,
    communitySize: Number(row.community_size),
    topMembers: row.top_members,
    avgRankScore: Number(row.avg_rank_score),
    avgActiveDays: Number(row.avg_active_days),
    totalEvents: Number(row.total_events),
    watchShare: Number(row.watch_share),
    prPushRatio: Number(row.pr_push_ratio),
    botRatio: Number(row.bot_ratio),
  }));
}

export async function getRepoAssociationRulesByRepo(
  repoName: string,
  limit = 20
): Promise<RepoAssociationRulePoint[]> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    antecedent: string;
    consequent: string;
    antecedent_size: number;
    support: number;
    confidence: number;
    lift: number;
    is_frontier: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_association_rules
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      antecedent,
      consequent,
      antecedent_size,
      support,
      confidence,
      lift,
      is_frontier
    FROM batch_repo_association_rules
    WHERE metric_date = (SELECT d FROM latest_date)
      AND (position(antecedent, ${repoLit}) > 0 OR consequent = ${repoLit})
    ORDER BY lift DESC, confidence DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    antecedent: row.antecedent,
    consequent: row.consequent,
    antecedentSize: Number(row.antecedent_size),
    support: Number(row.support),
    confidence: Number(row.confidence),
    lift: Number(row.lift),
    isFrontier: Number(row.is_frontier ?? 0),
  }));
}

export type RepoClusterAssignmentPoint = {
  metricDate: string;
  repoName: string;
  clusterId: number;
  pcaX: number;
  pcaY: number;
};

export async function getRepoClusterByRepo(
  repoName: string
): Promise<RepoClusterAssignmentPoint | null> {
  const repoLit = sqlStringLiteral(repoName);
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    cluster_id: number;
    pca_x: number;
    pca_y: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_clusters WHERE repo_name = ${repoLit}
    )
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      cluster_id,
      pca_x,
      pca_y
    FROM batch_repo_clusters
    WHERE repo_name = ${repoLit}
      AND metric_date = (SELECT d FROM latest_date)
    LIMIT 1
  `);
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    clusterId: Number(row.cluster_id),
    pcaX: Number(row.pca_x),
    pcaY: Number(row.pca_y),
  };
}

// -----------------------------------------------------------------------------
// Dynamic attribution — Risers vs Fallers over 30-day rank_score trajectory.
// Re-uses the HotVsColdAttributionPoint shape so the existing forest plot works
// without any structural changes. cohortScope is stamped as "all" so the scope
// toggle on the forest plot becomes a no-op in Dynamic mode (by design).
// -----------------------------------------------------------------------------

const DNA_FEATURES: readonly {
  key: string;
  sql: string;
}[] = [
  { key: "watch_share", sql: "watch_share" },
  { key: "fork_share", sql: "fork_share" },
  { key: "issues_share", sql: "issues_share" },
  { key: "pr_share", sql: "pr_share" },
  { key: "push_share", sql: "push_share" },
  { key: "bot_ratio", sql: "bot_ratio" },
  { key: "night_ratio", sql: "night_ratio" },
  { key: "weekend_ratio", sql: "weekend_ratio" },
  { key: "pr_push_ratio", sql: "pr_push_ratio" },
  { key: "active_days_ratio", sql: "active_days_ratio" },
  { key: "actors_per_event", sql: "actors_per_event" },
  { key: "event_entropy", sql: "event_entropy" },
  { key: "top1_actor_share", sql: "top1_actor_share" },
  { key: "log_total_events", sql: "log_total_events" },
  { key: "log_payload_p95", sql: "log_payload_p95" },
] as const;

export async function getDynamicAttribution30d(
  K = 30,
): Promise<HotVsColdAttributionPoint[]> {
  // 1. 30-day rank_score trajectory per repo.
  const slopeRows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    rank_score: number;
  }>(`
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      rank_score
    FROM batch_repo_rank_score_day
    ORDER BY repo_name, metric_date
  `);

  if (slopeRows.length === 0) return [];

  const byRepo = new Map<string, number[]>();
  for (const row of slopeRows) {
    const arr = byRepo.get(row.repo_name) ?? [];
    arr.push(Number(row.rank_score));
    byRepo.set(row.repo_name, arr);
  }
  const repoSlopes: { repoName: string; slope: number }[] = [];
  byRepo.forEach((scores, repoName) => {
    if (scores.length < 5) return; // Need enough days for a meaningful slope.
    repoSlopes.push({ repoName, slope: olsSlope(scores) });
  });
  if (repoSlopes.length < 20) return [];

  // 2. Pick top-K risers and top-K fallers by slope magnitude.
  repoSlopes.sort((a, b) => b.slope - a.slope);
  const risers = repoSlopes.slice(0, K).filter((r) => r.slope > 0);
  const fallers = repoSlopes
    .slice(-K)
    .filter((r) => r.slope < 0)
    .map((r) => r);
  if (risers.length < 5 || fallers.length < 5) return [];

  const riserNames = new Set(risers.map((r) => r.repoName));
  const fallerNames = new Set(fallers.map((r) => r.repoName));
  const allNames = [...riserNames, ...fallerNames];

  // 3. Fetch DNA features for those repos at the latest metric_date.
  const escaped = allNames
    .map((n) => "'" + n.replace(/'/g, "''") + "'")
    .join(",");
  const dnaRows = await queryJson<Record<string, number | string>>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_dna
    )
    SELECT
      repo_name,
      ${DNA_FEATURES.map((f) => f.sql).join(",\n      ")}
    FROM batch_repo_dna
    WHERE metric_date = (SELECT d FROM latest_date)
      AND repo_name IN (${escaped})
  `);

  // 4. Group values by (feature, cohort).
  const values: Record<string, { risers: number[]; fallers: number[] }> = {};
  for (const f of DNA_FEATURES) {
    values[f.key] = { risers: [], fallers: [] };
  }
  for (const row of dnaRows) {
    const name = String(row.repo_name);
    const bucket = riserNames.has(name)
      ? "risers"
      : fallerNames.has(name)
        ? "fallers"
        : null;
    if (!bucket) continue;
    for (const f of DNA_FEATURES) {
      const v = Number(row[f.key]);
      if (isFinite(v)) values[f.key]![bucket].push(v);
    }
  }

  const latestDate = slopeRows[slopeRows.length - 1]!.metric_date_str;
  const points: HotVsColdAttributionPoint[] = [];
  for (const f of DNA_FEATURES) {
    const { risers: a, fallers: b } = values[f.key]!;
    if (a.length < 3 || b.length < 3) continue;
    const meanA = a.reduce((s, x) => s + x, 0) / a.length;
    const meanB = b.reduce((s, x) => s + x, 0) / b.length;
    const d = cohenD(a, b);
    const [lo, hi] = bootstrapCIofCohenD(a, b, 200, 0.05);
    const { t } = welchT(a, b);
    points.push({
      metricDate: latestDate,
      cohortScope: "all",
      featureName: f.key,
      meanHot: meanA,
      meanCold: meanB,
      meanDiff: meanA - meanB,
      cohenD: d,
      cohensDLow: lo,
      cohensDHigh: hi,
      tStat: t,
      nHot: a.length,
      nCold: b.length,
      direction: d >= 0 ? "hot_higher" : "cold_higher",
    });
  }
  return points;
}

// -----------------------------------------------------------------------------
// Authenticity — fuses 4 independent "suspicion" signals into a single ranked
// list: (1) volume anomaly z-score, (2) DNA outlier z-distance, (3) watcher bot
// ratio, (4) community bot ratio. Left-joined on repo_name with COALESCE so a
// missing signal in any single table doesn't drop the repo.
// -----------------------------------------------------------------------------

export type AuthenticityInputRow = {
  repoName: string;
  alertZ: number;
  dnaOutlierZ: number;
  watcherBot: number;
  communityBot: number;
  watchers: number;
};

export async function getAuthenticityInputs(
  limit = 150,
): Promise<AuthenticityInputRow[]> {
  const rows = await queryJson<{
    repo_name: string;
    alert_z: number;
    dna_outlier_z: number;
    watcher_bot: number;
    community_bot: number;
    watchers: number;
  }>(`
    WITH alerts AS (
      SELECT repo_name, max(abs(z_score)) AS alert_z
      FROM batch_offline_anomaly_alerts
      GROUP BY repo_name
    ),
    outliers AS (
      SELECT repo_name, max(z_distance) AS dna_outlier_z
      FROM batch_repo_dna_outliers
      WHERE metric_date = (SELECT max(metric_date) FROM batch_repo_dna_outliers)
      GROUP BY repo_name
    ),
    watcher AS (
      SELECT
        repo_name,
        max(bot_ratio) AS watcher_bot,
        max(watchers)  AS watchers
      FROM batch_repo_watcher_profile
      WHERE metric_date = (SELECT max(metric_date) FROM batch_repo_watcher_profile)
      GROUP BY repo_name
    ),
    community AS (
      SELECT
        rc.repo_name      AS repo_name,
        max(cp.bot_ratio) AS community_bot
      FROM batch_repo_community rc
      LEFT JOIN batch_repo_community_profile cp
        ON rc.community_id = cp.community_id
       AND rc.metric_date  = cp.metric_date
      WHERE rc.metric_date = (SELECT max(metric_date) FROM batch_repo_community)
      GROUP BY rc.repo_name
    ),
    base AS (
      SELECT repo_name FROM alerts
      UNION DISTINCT SELECT repo_name FROM outliers
      UNION DISTINCT SELECT repo_name FROM watcher
    )
    SELECT
      b.repo_name                         AS repo_name,
      coalesce(a.alert_z, 0)              AS alert_z,
      coalesce(o.dna_outlier_z, 0)        AS dna_outlier_z,
      coalesce(w.watcher_bot, 0)          AS watcher_bot,
      coalesce(c.community_bot, 0)        AS community_bot,
      coalesce(w.watchers, 0)             AS watchers
    FROM base b
    LEFT JOIN alerts    a ON a.repo_name = b.repo_name
    LEFT JOIN outliers  o ON o.repo_name = b.repo_name
    LEFT JOIN watcher   w ON w.repo_name = b.repo_name
    LEFT JOIN community c ON c.repo_name = b.repo_name
    ORDER BY
      coalesce(a.alert_z, 0) + coalesce(o.dna_outlier_z, 0) +
      coalesce(w.watcher_bot, 0) + coalesce(c.community_bot, 0) DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);

  return rows.map((row) => ({
    repoName: row.repo_name,
    alertZ: Number(row.alert_z),
    dnaOutlierZ: Number(row.dna_outlier_z),
    watcherBot: Number(row.watcher_bot),
    communityBot: Number(row.community_bot),
    watchers: Number(row.watchers),
  }));
}

// ═════════════════════════════════════════════════════════════════════════════
// People-page depth analyses (bucket A–D)
// ═════════════════════════════════════════════════════════════════════════════

export type ActorGraphMetricPoint = {
  metricDate: string;
  actorLogin: string;
  pagerank: number;
  betweenness: number;
  communityId: number;
  degree: number;
  personaLabel: string;
};

export async function getActorGraphMetrics(limit = 400): Promise<ActorGraphMetricPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    pagerank: number;
    betweenness: number;
    community_id: number;
    degree: number;
    persona_label: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_graph_metrics)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      pagerank,
      betweenness,
      community_id,
      degree,
      persona_label
    FROM batch_actor_graph_metrics
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY pagerank DESC, actor_login
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    pagerank: Number(row.pagerank),
    betweenness: Number(row.betweenness),
    communityId: Number(row.community_id),
    degree: Number(row.degree),
    personaLabel: row.persona_label,
  }));
}

export type ActorCollabEdgePoint = {
  metricDate: string;
  actorA: string;
  actorB: string;
  sharedRepos: number;
  coDays: number;
  weight: number;
};

export async function getActorCollabEdges(
  seedActors: string[] = [],
  limit = 400,
): Promise<ActorCollabEdgePoint[]> {
  // Pull the top-weight edges; if seedActors given, restrict to rows touching them.
  const seedFilter =
    seedActors.length > 0
      ? `AND (actor_a IN (${seedActors.map((a) => `'${a.replace(/'/g, "''")}'`).join(",")})
            OR actor_b IN (${seedActors.map((a) => `'${a.replace(/'/g, "''")}'`).join(",")}))`
      : "";
  const rows = await queryJson<{
    metric_date_str: string;
    actor_a: string;
    actor_b: string;
    shared_repos: number;
    co_days: number;
    weight: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_collab_edge)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_a,
      actor_b,
      shared_repos,
      co_days,
      weight
    FROM batch_actor_collab_edge
    WHERE metric_date = (SELECT d FROM latest)
    ${seedFilter}
    ORDER BY weight DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorA: row.actor_a,
    actorB: row.actor_b,
    sharedRepos: Number(row.shared_repos),
    coDays: Number(row.co_days),
    weight: Number(row.weight),
  }));
}

export type ActorBurstStabilityPoint = {
  metricDate: string;
  actorLogin: string;
  burstIndex: number;
  stabilityIndex: number;
  quadrant: string;
  rankScore: number;
  totalEvents: number;
  personaLabel: string;
};

export async function getActorBurstStability(limit = 400): Promise<ActorBurstStabilityPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    burst_index: number;
    stability_index: number;
    quadrant: string;
    rank_score: number;
    total_events: number;
    persona_label: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_burst_stability)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      burst_index,
      stability_index,
      quadrant,
      rank_score,
      total_events,
      persona_label
    FROM batch_actor_burst_stability
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY total_events DESC, actor_login
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    burstIndex: Number(row.burst_index),
    stabilityIndex: Number(row.stability_index),
    quadrant: row.quadrant,
    rankScore: Number(row.rank_score),
    totalEvents: Number(row.total_events),
    personaLabel: row.persona_label,
  }));
}

export type ActorRetentionCurvePoint = {
  metricDate: string;
  cohortWeek: string;
  daysSinceFirst: number;
  retainedCount: number;
  cohortSize: number;
  retentionRate: number;
};

export async function getActorRetentionCurves(): Promise<ActorRetentionCurvePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    cohort_week_str: string;
    days_since_first: number;
    retained_count: number;
    cohort_size: number;
    retention_rate: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_retention_curve)
    SELECT
      toString(metric_date) AS metric_date_str,
      toString(cohort_week) AS cohort_week_str,
      days_since_first,
      retained_count,
      cohort_size,
      retention_rate
    FROM batch_actor_retention_curve
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY cohort_week, days_since_first
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    cohortWeek: row.cohort_week_str,
    daysSinceFirst: Number(row.days_since_first),
    retainedCount: Number(row.retained_count),
    cohortSize: Number(row.cohort_size),
    retentionRate: Number(row.retention_rate),
  }));
}

export type ActorChurnRiskPoint = {
  metricDate: string;
  actorLogin: string;
  daysSinceLast: number;
  decaySlope: number;
  churnProb: number;
  riskTier: string;
  personaLabel: string;
};

export async function getActorChurnRisk(limit = 400): Promise<ActorChurnRiskPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    days_since_last: number;
    decay_slope: number;
    churn_prob: number;
    risk_tier: string;
    persona_label: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_churn_risk)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      days_since_last,
      decay_slope,
      churn_prob,
      risk_tier,
      persona_label
    FROM batch_actor_churn_risk
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY churn_prob DESC, actor_login
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    daysSinceLast: Number(row.days_since_last),
    decaySlope: Number(row.decay_slope),
    churnProb: Number(row.churn_prob),
    riskTier: row.risk_tier,
    personaLabel: row.persona_label,
  }));
}

export type ActorHotnessPoint = {
  metricDate: string;
  actorLogin: string;
  eventCount: number;
  uniqueRepos: number;
  avgRepoRankScore: number;
  hotnessScore: number;
  rankNo: number;
  personaLabel: string;
};

export async function getActorHotness(limit = 50): Promise<ActorHotnessPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    event_count: number;
    unique_repos: number;
    avg_repo_rank_score: number;
    hotness_score: number;
    rank_no: number;
    persona_label: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_hotness)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      event_count,
      unique_repos,
      avg_repo_rank_score,
      hotness_score,
      rank_no,
      persona_label
    FROM batch_actor_hotness
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY rank_no
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    eventCount: Number(row.event_count),
    uniqueRepos: Number(row.unique_repos),
    avgRepoRankScore: Number(row.avg_repo_rank_score),
    hotnessScore: Number(row.hotness_score),
    rankNo: Number(row.rank_no),
    personaLabel: row.persona_label,
  }));
}

export type RepoBusFactorPoint = {
  metricDate: string;
  repoName: string;
  topActor: string;
  topActorShare: number;
  contributorCount: number;
  busFactor: string;
  repoRankScore: number;
};

export async function getRepoBusFactor(limit = 100): Promise<RepoBusFactorPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    top_actor: string;
    top_actor_share: number;
    contributor_count: number;
    bus_factor: string;
    repo_rank_score: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_bus_factor)
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name,
      top_actor,
      top_actor_share,
      contributor_count,
      bus_factor,
      repo_rank_score
    FROM batch_repo_bus_factor
    WHERE metric_date = (SELECT d FROM latest)
      AND bus_factor IN ('bus-1','thin')
    ORDER BY top_actor_share DESC, repo_rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    repoName: row.repo_name,
    topActor: row.top_actor,
    topActorShare: Number(row.top_actor_share),
    contributorCount: Number(row.contributor_count),
    busFactor: row.bus_factor,
    repoRankScore: Number(row.repo_rank_score),
  }));
}

export type ActorBotSupervisedPoint = {
  metricDate: string;
  actorLogin: string;
  xgbProbBot: number;
  iforestScore: number;
  combinedScore: number;
  isBotTruth: number;
  rankNo: number;
  personaLabel: string;
};

export async function getActorBotSupervised(limit = 80): Promise<ActorBotSupervisedPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    xgb_prob_bot: number;
    iforest_score: number;
    combined_score: number;
    is_bot_truth: number;
    rank_no: number;
    persona_label: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_bot_supervised)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login,
      xgb_prob_bot,
      iforest_score,
      combined_score,
      is_bot_truth,
      rank_no,
      persona_label
    FROM batch_actor_bot_supervised
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY rank_no
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    actorLogin: row.actor_login,
    xgbProbBot: Number(row.xgb_prob_bot),
    iforestScore: Number(row.iforest_score),
    combinedScore: Number(row.combined_score),
    isBotTruth: Number(row.is_bot_truth),
    rankNo: Number(row.rank_no),
    personaLabel: row.persona_label,
  }));
}

export type BotFeatureImportancePoint = {
  metricDate: string;
  feature: string;
  importance: number;
  rankNo: number;
};

export async function getBotFeatureImportance(): Promise<BotFeatureImportancePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    feature: string;
    importance: number;
    rank_no: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_bot_feature_importance)
    SELECT
      toString(metric_date) AS metric_date_str,
      feature,
      importance,
      rank_no
    FROM batch_bot_feature_importance
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY rank_no
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    feature: row.feature,
    importance: Number(row.importance),
    rankNo: Number(row.rank_no),
  }));
}

export type BotClassifierMetaPoint = {
  metricDate: string;
  metric: string;
  value: number;
};

export async function getBotClassifierMeta(): Promise<BotClassifierMetaPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    metric: string;
    value: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_bot_classifier_meta)
    SELECT
      toString(metric_date) AS metric_date_str,
      metric,
      value
    FROM batch_bot_classifier_meta
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY metric
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    metric: row.metric,
    value: Number(row.value),
  }));
}

export type ActorRingPoint = {
  metricDate: string;
  ringId: number;
  actorCount: number;
  reposShared: number;
  sampleActors: string[];
  avgCoBursts: number;
};

export async function getActorRings(limit = 30): Promise<ActorRingPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    ring_id: number;
    actor_count: number;
    repos_shared: number;
    sample_actors: string[];
    avg_co_bursts: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_ring)
    SELECT
      toString(metric_date) AS metric_date_str,
      ring_id,
      actor_count,
      repos_shared,
      sample_actors,
      avg_co_bursts
    FROM batch_actor_ring
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY actor_count DESC, repos_shared DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((row) => ({
    metricDate: row.metric_date_str,
    ringId: Number(row.ring_id),
    actorCount: Number(row.actor_count),
    reposShared: Number(row.repos_shared),
    sampleActors: Array.isArray(row.sample_actors) ? row.sample_actors : [],
    avgCoBursts: Number(row.avg_co_bursts),
  }));
}

// =========================================================================
// Network-page depth analyses (L5 deep-dive; produced by network_depth.py)
// =========================================================================

export type RepoAlsNeighborPoint = {
  metricDate: string;
  srcRepo: string;
  dstRepo: string;
  cosine: number;
  rankNo: number;
  isJaccardMiss: number;
};

export async function getRepoAlsNeighbors(limit = 600): Promise<RepoAlsNeighborPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    src_repo: string;
    dst_repo: string;
    cosine: number;
    rank_no: number;
    is_jaccard_miss: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_als_neighbor)
    SELECT
      toString(metric_date) AS metric_date_str,
      src_repo, dst_repo, cosine, rank_no, is_jaccard_miss
    FROM batch_repo_als_neighbor
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY src_repo, rank_no
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    srcRepo: r.src_repo,
    dstRepo: r.dst_repo,
    cosine: Number(r.cosine),
    rankNo: Number(r.rank_no),
    isJaccardMiss: Number(r.is_jaccard_miss ?? 0),
  }));
}

export type RepoEmbeddingSummaryPoint = {
  totalRepos: number;
  rankDim: number;
  avgCosineTop5: number;
  jaccardMissShare: number;
};

export async function getRepoEmbeddingSummary(): Promise<RepoEmbeddingSummaryPoint | null> {
  const countRows = await queryJson<{ n: number; rank_dim: number }>(`
    SELECT count() AS n, any(rank_dim) AS rank_dim
    FROM batch_repo_embedding
    WHERE metric_date = (SELECT max(metric_date) FROM batch_repo_embedding)
  `);
  const simRows = await queryJson<{ avg_cos: number; miss_share: number }>(`
    SELECT avg(cosine) AS avg_cos, avg(is_jaccard_miss) AS miss_share
    FROM batch_repo_als_neighbor
    WHERE metric_date = (SELECT max(metric_date) FROM batch_repo_als_neighbor)
      AND rank_no <= 5
  `);
  if (!countRows[0] || Number(countRows[0].n ?? 0) === 0) return null;
  return {
    totalRepos: Number(countRows[0].n),
    rankDim: Number(countRows[0].rank_dim ?? 0),
    avgCosineTop5: Number(simRows[0]?.avg_cos ?? 0),
    jaccardMissShare: Number(simRows[0]?.miss_share ?? 0),
  };
}

export type RepoLayerEdgePoint = {
  metricDate: string;
  layer: string;
  srcRepo: string;
  dstRepo: string;
  jaccard: number;
  sharedActors: number;
};

export async function getRepoLayerEdges(limit = 1500): Promise<RepoLayerEdgePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    layer: string;
    src_repo: string;
    dst_repo: string;
    jaccard: number;
    shared_actors: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_layer_edge)
    SELECT
      toString(metric_date) AS metric_date_str,
      layer, src_repo, dst_repo, jaccard, shared_actors
    FROM batch_repo_layer_edge
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY layer, jaccard DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    layer: r.layer,
    srcRepo: r.src_repo,
    dstRepo: r.dst_repo,
    jaccard: Number(r.jaccard),
    sharedActors: Number(r.shared_actors),
  }));
}

export type RepoLayerCommunityPoint = {
  metricDate: string;
  layer: string;
  repoName: string;
  communityId: string;
  communitySize: number;
  rankScore: number;
};

export async function getRepoLayerCommunities(limit = 3000): Promise<RepoLayerCommunityPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    layer: string;
    repo_name: string;
    community_id: string;
    community_size: number;
    rank_score: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_layer_community)
    SELECT
      toString(metric_date) AS metric_date_str,
      layer, repo_name, community_id, community_size, rank_score
    FROM batch_repo_layer_community
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY layer, community_size DESC, rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    layer: r.layer,
    repoName: r.repo_name,
    communityId: r.community_id,
    communitySize: Number(r.community_size),
    rankScore: Number(r.rank_score),
  }));
}

export type ActorCorenessPoint = {
  metricDate: string;
  actorLogin: string;
  coreness: number;
  degree: number;
  personaLabel: string;
  isBot: number;
};

export async function getActorCoreness(limit = 200): Promise<ActorCorenessPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    actor_login: string;
    coreness: number;
    degree: number;
    persona_label: string;
    is_bot: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_coreness)
    SELECT
      toString(metric_date) AS metric_date_str,
      actor_login, coreness, degree, persona_label, is_bot
    FROM batch_actor_coreness
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY coreness DESC, degree DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    actorLogin: r.actor_login,
    coreness: Number(r.coreness),
    degree: Number(r.degree),
    personaLabel: r.persona_label,
    isBot: Number(r.is_bot ?? 0),
  }));
}

export type CorenessHistogramBucket = {
  coreness: number;
  actors: number;
  bots: number;
};

export async function getActorCorenessHistogram(): Promise<CorenessHistogramBucket[]> {
  const rows = await queryJson<{ coreness: number; actors: number; bots: number }>(`
    SELECT
      coreness,
      count() AS actors,
      sum(is_bot) AS bots
    FROM batch_actor_coreness
    WHERE metric_date = (SELECT max(metric_date) FROM batch_actor_coreness)
    GROUP BY coreness
    ORDER BY coreness
  `);
  return rows.map((r) => ({
    coreness: Number(r.coreness),
    actors: Number(r.actors),
    bots: Number(r.bots),
  }));
}

export type RepoCorenessPoint = {
  metricDate: string;
  repoName: string;
  coreness: number;
  degree: number;
  rankScore: number;
  cohortGroup: string;
};

export async function getRepoCoreness(limit = 400): Promise<RepoCorenessPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    coreness: number;
    degree: number;
    rank_score: number;
    cohort_group: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_coreness)
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name, coreness, degree, rank_score, cohort_group
    FROM batch_repo_coreness
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY coreness DESC, degree DESC, rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    repoName: r.repo_name,
    coreness: Number(r.coreness),
    degree: Number(r.degree),
    rankScore: Number(r.rank_score),
    cohortGroup: r.cohort_group,
  }));
}

export type RepoMetapathSimPoint = {
  metricDate: string;
  pathType: string;
  srcRepo: string;
  dstRepo: string;
  sim: number;
  sharedActors: number;
  rankNo: number;
};

export async function getRepoMetapathSim(
  pathType: string | null = null,
  limit = 1200,
): Promise<RepoMetapathSimPoint[]> {
  const whereType = pathType ? `AND path_type = ${sqlStringLiteral(pathType)}` : "";
  const rows = await queryJson<{
    metric_date_str: string;
    path_type: string;
    src_repo: string;
    dst_repo: string;
    sim: number;
    shared_actors: number;
    rank_no: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_metapath_sim)
    SELECT
      toString(metric_date) AS metric_date_str,
      path_type, src_repo, dst_repo, sim, shared_actors, rank_no
    FROM batch_repo_metapath_sim
    WHERE metric_date = (SELECT d FROM latest) ${whereType}
    ORDER BY path_type, src_repo, rank_no
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    pathType: r.path_type,
    srcRepo: r.src_repo,
    dstRepo: r.dst_repo,
    sim: Number(r.sim),
    sharedActors: Number(r.shared_actors),
    rankNo: Number(r.rank_no),
  }));
}

export type RepoArchetypePoint = {
  metricDate: string;
  repoName: string;
  archetypeRule: string;
  archetypeGmmId: number;
  archetypeConfidence: number;
  pcaX: number;
  pcaY: number;
  rankScore: number;
  totalEvents: number;
  watchShare: number;
  prPushRatio: number;
  botRatio: number;
  activeDays: number;
  contributorCount: number;
  sampleNote: string;
};

export async function getRepoArchetype(limit = 400): Promise<RepoArchetypePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    repo_name: string;
    archetype_rule: string;
    archetype_gmm_id: number;
    archetype_confidence: number;
    pca_x: number;
    pca_y: number;
    rank_score: number;
    total_events: number;
    watch_share: number;
    pr_push_ratio: number;
    bot_ratio: number;
    active_days: number;
    contributor_count: number;
    sample_note: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_archetype)
    SELECT
      toString(metric_date) AS metric_date_str,
      repo_name, archetype_rule, archetype_gmm_id, archetype_confidence,
      pca_x, pca_y, rank_score, total_events, watch_share, pr_push_ratio,
      bot_ratio, active_days, contributor_count, sample_note
    FROM batch_repo_archetype
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY archetype_rule, rank_score DESC
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    repoName: r.repo_name,
    archetypeRule: r.archetype_rule,
    archetypeGmmId: Number(r.archetype_gmm_id),
    archetypeConfidence: Number(r.archetype_confidence),
    pcaX: Number(r.pca_x),
    pcaY: Number(r.pca_y),
    rankScore: Number(r.rank_score),
    totalEvents: Number(r.total_events),
    watchShare: Number(r.watch_share),
    prPushRatio: Number(r.pr_push_ratio),
    botRatio: Number(r.bot_ratio),
    activeDays: Number(r.active_days),
    contributorCount: Number(r.contributor_count),
    sampleNote: r.sample_note,
  }));
}

export type RepoArchetypeCentroidPoint = {
  metricDate: string;
  archetypeRule: string;
  archetypeGmmId: number;
  members: number;
  avgWatchShare: number;
  avgPrPushRatio: number;
  avgBotRatio: number;
  avgActiveDays: number;
  avgRankScore: number;
  ruleGmmOverlap: number;
  sampleRepos: string;
};

export async function getRepoArchetypeCentroids(): Promise<RepoArchetypeCentroidPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    archetype_rule: string;
    archetype_gmm_id: number;
    members: number;
    avg_watch_share: number;
    avg_pr_push_ratio: number;
    avg_bot_ratio: number;
    avg_active_days: number;
    avg_rank_score: number;
    rule_gmm_overlap: number;
    sample_repos: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_archetype_centroid)
    SELECT
      toString(metric_date) AS metric_date_str,
      archetype_rule, archetype_gmm_id, members,
      avg_watch_share, avg_pr_push_ratio, avg_bot_ratio,
      avg_active_days, avg_rank_score, rule_gmm_overlap, sample_repos
    FROM batch_repo_archetype_centroid
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY members DESC
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    archetypeRule: r.archetype_rule,
    archetypeGmmId: Number(r.archetype_gmm_id),
    members: Number(r.members),
    avgWatchShare: Number(r.avg_watch_share),
    avgPrPushRatio: Number(r.avg_pr_push_ratio),
    avgBotRatio: Number(r.avg_bot_ratio),
    avgActiveDays: Number(r.avg_active_days),
    avgRankScore: Number(r.avg_rank_score),
    ruleGmmOverlap: Number(r.rule_gmm_overlap),
    sampleRepos: r.sample_repos,
  }));
}

// -------- Phase 3: temporal community evolution ---------

export type RepoCommunityWeeklyPoint = {
  metricDate: string;
  weekIdx: number;
  weekStart: string;
  weekEnd: string;
  repoName: string;
  communityId: string;
  communitySize: number;
};

export async function getRepoCommunityWeekly(limit = 2000): Promise<RepoCommunityWeeklyPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    week_idx: number;
    week_start_str: string;
    week_end_str: string;
    repo_name: string;
    community_id: string;
    community_size: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_community_weekly)
    SELECT
      toString(metric_date) AS metric_date_str,
      week_idx,
      toString(week_start) AS week_start_str,
      toString(week_end) AS week_end_str,
      repo_name, community_id, community_size
    FROM batch_repo_community_weekly
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY week_idx, community_id
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    weekIdx: Number(r.week_idx),
    weekStart: r.week_start_str,
    weekEnd: r.week_end_str,
    repoName: r.repo_name,
    communityId: r.community_id,
    communitySize: Number(r.community_size),
  }));
}

export type CommunityLineagePoint = {
  metricDate: string;
  lineageId: number;
  weekIdx: number;
  communityId: string;
  prevCommunityId: string;
  eventType: string;
  membersCount: number;
  overlapJaccard: number;
  sampleMembers: string;
};

export async function getCommunityLineage(limit = 500): Promise<CommunityLineagePoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    lineage_id: number;
    week_idx: number;
    community_id: string;
    prev_community_id: string;
    event_type: string;
    members_count: number;
    overlap_jaccard: number;
    sample_members: string;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_community_lineage)
    SELECT
      toString(metric_date) AS metric_date_str,
      lineage_id, week_idx, community_id, prev_community_id, event_type,
      members_count, overlap_jaccard, sample_members
    FROM batch_community_lineage
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY week_idx, lineage_id
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    lineageId: Number(r.lineage_id),
    weekIdx: Number(r.week_idx),
    communityId: r.community_id,
    prevCommunityId: r.prev_community_id,
    eventType: r.event_type,
    membersCount: Number(r.members_count),
    overlapJaccard: Number(r.overlap_jaccard),
    sampleMembers: r.sample_members,
  }));
}

// -------- Phase 4: influence maximization ---------

export type ActorIcReachPoint = {
  metricDate: string;
  k: number;
  strategy: string;
  seeds: string;
  expectedReach: number;
  reachStddev: number;
  simRuns: number;
};

export async function getActorIcReach(): Promise<ActorIcReachPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    k: number;
    strategy: string;
    seeds: string;
    expected_reach: number;
    reach_stddev: number;
    sim_runs: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_ic_reach)
    SELECT
      toString(metric_date) AS metric_date_str,
      k, strategy, seeds, expected_reach, reach_stddev, sim_runs
    FROM batch_actor_ic_reach
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY strategy, k
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    k: Number(r.k),
    strategy: r.strategy,
    seeds: r.seeds,
    expectedReach: Number(r.expected_reach),
    reachStddev: Number(r.reach_stddev),
    simRuns: Number(r.sim_runs),
  }));
}

export type SeedGreedyPoint = {
  metricDate: string;
  seedRank: number;
  actorLogin: string;
  personaLabel: string;
  isBot: number;
  marginalGain: number;
  cumulativeReach: number;
  degree: number;
  pagerank: number;
};

export async function getSeedGreedy(limit = 20): Promise<SeedGreedyPoint[]> {
  const rows = await queryJson<{
    metric_date_str: string;
    seed_rank: number;
    actor_login: string;
    persona_label: string;
    is_bot: number;
    marginal_gain: number;
    cumulative_reach: number;
    degree: number;
    pagerank: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_seed_greedy)
    SELECT
      toString(metric_date) AS metric_date_str,
      seed_rank, actor_login, persona_label, is_bot,
      marginal_gain, cumulative_reach, degree, pagerank
    FROM batch_seed_greedy
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY seed_rank
    LIMIT ${Math.max(Number(limit), 1)}
  `);
  return rows.map((r) => ({
    metricDate: r.metric_date_str,
    seedRank: Number(r.seed_rank),
    actorLogin: r.actor_login,
    personaLabel: r.persona_label,
    isBot: Number(r.is_bot ?? 0),
    marginalGain: Number(r.marginal_gain),
    cumulativeReach: Number(r.cumulative_reach),
    degree: Number(r.degree),
    pagerank: Number(r.pagerank),
  }));
}

// ————————
// ML Lab methodology · BIC sweep + bot validation + repo DNA
// ————————

export type PersonaBicPoint = {
  k: number;
  logLikelihood: number;
  bic: number;
  nParams: number;
  nSamples: number;
  isSelected: number;
};

/** BIC / log-likelihood per candidate k used by the GMM(k=?) model selection. */
export async function getPersonaBic(): Promise<PersonaBicPoint[]> {
  const rows = await queryJson<{
    k: number;
    log_likelihood: number;
    bic: number;
    n_params: number;
    n_samples: number;
    is_selected: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_persona_bic)
    SELECT k, log_likelihood, bic, n_params, n_samples, is_selected
    FROM batch_actor_persona_bic
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY k
  `);
  return rows.map((r) => ({
    k: Number(r.k),
    logLikelihood: Number(r.log_likelihood),
    bic: Number(r.bic),
    nParams: Number(r.n_params),
    nSamples: Number(r.n_samples),
    isSelected: Number(r.is_selected),
  }));
}

export type PersonaBotValidationPoint = {
  personaLabel: string;
  trueBots: number;
  falseBots: number;
  missedBots: number;
  precision: number;
  recall: number;
  f1: number;
  totalBots: number;
  totalHumans: number;
};

/** Per-persona confusion-matrix metrics against the ground-truth is_bot flag. */
export async function getPersonaBotValidation(): Promise<PersonaBotValidationPoint[]> {
  const rows = await queryJson<{
    persona_label: string;
    true_bots: number;
    false_bots: number;
    missed_bots: number;
    precision: number;
    recall: number;
    f1: number;
    total_bots: number;
    total_humans: number;
  }>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_actor_persona_bot_validation)
    SELECT
      persona_label,
      true_bots, false_bots, missed_bots,
      precision, recall, f1,
      total_bots, total_humans
    FROM batch_actor_persona_bot_validation
    WHERE metric_date = (SELECT d FROM latest)
    ORDER BY f1 DESC, precision DESC
  `);
  return rows.map((r) => ({
    personaLabel: r.persona_label,
    trueBots: Number(r.true_bots),
    falseBots: Number(r.false_bots),
    missedBots: Number(r.missed_bots),
    precision: Number(r.precision),
    recall: Number(r.recall),
    f1: Number(r.f1),
    totalBots: Number(r.total_bots),
    totalHumans: Number(r.total_humans),
  }));
}

/**
 * Top-N repos by rank_score within each cohort (hot / cold). We only need a
 * handful for the DNA radar; loading the whole 680k-row table would be pointless.
 * Reuses the existing RepoDnaPoint / mapRepoDnaRow defined above.
 */
export async function getRepoDnaTop(
  topPerCohort = 6,
): Promise<RepoDnaPoint[]> {
  const n = Math.max(Number(topPerCohort), 1);
  const rows = await queryJson<Parameters<typeof mapRepoDnaRow>[0]>(`
    WITH latest AS (SELECT max(metric_date) AS d FROM batch_repo_dna),
    ranked AS (
      SELECT
        toString(metric_date) AS metric_date_str,
        repo_name, cohort_group,
        watch_share, fork_share, issues_share, pr_share, push_share,
        bot_ratio, night_ratio, weekend_ratio, pr_push_ratio,
        active_days_ratio, actors_per_event, event_entropy,
        top1_actor_share, log_total_events, log_payload_p95,
        rank_score, total_events,
        row_number() OVER (PARTITION BY cohort_group ORDER BY rank_score DESC) AS rn
      FROM batch_repo_dna
      WHERE metric_date = (SELECT d FROM latest)
        AND cohort_group IN ('hot','cold')
    )
    SELECT *
    FROM ranked
    WHERE rn <= ${n}
    ORDER BY cohort_group DESC, rank_score DESC
  `);
  return rows.map(mapRepoDnaRow);
}

