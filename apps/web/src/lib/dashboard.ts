import { clickhouse } from "@/lib/clickhouse";

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
