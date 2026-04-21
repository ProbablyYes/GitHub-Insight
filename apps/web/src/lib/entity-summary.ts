import {
  getRepoAssociationRulesByRepo,
  getRepoClusterByRepo,
  getRepoClusterProfileLatest,
  getRepoCommunityByRepo,
  getRepoDnaByRepo,
  getRepoSimilarityEdgesByRepo,
  getRepoTopActorsLatestByRepo,
  getRepoWatcherPersonaLiftByRepo,
  getRepoWatcherProfileByRepo,
  type RepoAssociationRulePoint,
  type RepoClusterAssignmentPoint,
  type RepoClusterProfilePoint,
  type RepoCommunityPoint,
  type RepoDnaPoint,
  type RepoSimilarityEdgePoint,
  type RepoTopActorPoint,
  type RepoWatcherPersonaLiftPoint,
  type RepoWatcherProfilePoint,
} from "./dashboard";
import { clickhouse } from "./clickhouse";

export type RepoSummary = {
  repoName: string;
  clusterAssignment: RepoClusterAssignmentPoint | null;
  clusterProfile: RepoClusterProfilePoint | null;
  dna: RepoDnaPoint | null;
  cohortAverages: Record<string, Record<string, number>>;
  watcherProfile: RepoWatcherProfilePoint | null;
  watcherLifts: RepoWatcherPersonaLiftPoint[];
  community: RepoCommunityPoint | null;
  similarEdges: RepoSimilarityEdgePoint[];
  assocRules: RepoAssociationRulePoint[];
  topActors: RepoTopActorPoint[];
};

async function queryJson<T>(query: string): Promise<T[]> {
  try {
    const result = await clickhouse.query({ query, format: "JSONEachRow" });
    return (await result.json()) as T[];
  } catch {
    return [];
  }
}

async function getCohortAverages(): Promise<Record<string, Record<string, number>>> {
  const rows = await queryJson<{
    cohort_group: string;
    avg_watch_share: number;
    avg_fork_share: number;
    avg_issues_share: number;
    avg_pr_share: number;
    avg_push_share: number;
    avg_bot_ratio: number;
    avg_night_ratio: number;
    avg_weekend_ratio: number;
    avg_pr_push_ratio: number;
    avg_active_days_ratio: number;
    avg_actors_per_event: number;
    avg_event_entropy: number;
    avg_top1_actor_share: number;
    avg_log_total_events: number;
    avg_log_payload_p95: number;
  }>(`
    WITH latest_date AS (
      SELECT max(metric_date) AS d FROM batch_repo_dna
    )
    SELECT
      cohort_group,
      avg(watch_share) AS avg_watch_share,
      avg(fork_share) AS avg_fork_share,
      avg(issues_share) AS avg_issues_share,
      avg(pr_share) AS avg_pr_share,
      avg(push_share) AS avg_push_share,
      avg(bot_ratio) AS avg_bot_ratio,
      avg(night_ratio) AS avg_night_ratio,
      avg(weekend_ratio) AS avg_weekend_ratio,
      avg(pr_push_ratio) AS avg_pr_push_ratio,
      avg(active_days_ratio) AS avg_active_days_ratio,
      avg(actors_per_event) AS avg_actors_per_event,
      avg(event_entropy) AS avg_event_entropy,
      avg(top1_actor_share) AS avg_top1_actor_share,
      avg(log_total_events) AS avg_log_total_events,
      avg(log_payload_p95) AS avg_log_payload_p95
    FROM batch_repo_dna
    WHERE metric_date = (SELECT d FROM latest_date)
    GROUP BY cohort_group
  `);

  const result: Record<string, Record<string, number>> = {};
  for (const row of rows) {
    result[row.cohort_group] = {
      watchShare: Number(row.avg_watch_share),
      forkShare: Number(row.avg_fork_share),
      issuesShare: Number(row.avg_issues_share),
      prShare: Number(row.avg_pr_share),
      pushShare: Number(row.avg_push_share),
      botRatio: Number(row.avg_bot_ratio),
      nightRatio: Number(row.avg_night_ratio),
      weekendRatio: Number(row.avg_weekend_ratio),
      prPushRatio: Number(row.avg_pr_push_ratio),
      activeDaysRatio: Number(row.avg_active_days_ratio),
      actorsPerEvent: Number(row.avg_actors_per_event),
      eventEntropy: Number(row.avg_event_entropy),
      top1ActorShare: Number(row.avg_top1_actor_share),
      logTotalEvents: Number(row.avg_log_total_events),
      logPayloadP95: Number(row.avg_log_payload_p95),
    };
  }
  return result;
}

export async function getRepoSummary(repoName: string): Promise<RepoSummary> {
  const [
    clusterAssignment,
    clusterProfilesAll,
    dna,
    cohortAverages,
    watcherProfile,
    watcherLifts,
    community,
    similarEdges,
    assocRules,
    topActors,
  ] = await Promise.all([
    getRepoClusterByRepo(repoName).catch(() => null),
    getRepoClusterProfileLatest().catch(() => []),
    getRepoDnaByRepo(repoName).catch(() => null),
    getCohortAverages().catch(() => ({})),
    getRepoWatcherProfileByRepo(repoName).catch(() => null),
    getRepoWatcherPersonaLiftByRepo(repoName).catch(() => []),
    getRepoCommunityByRepo(repoName).catch(() => null),
    getRepoSimilarityEdgesByRepo(repoName, 10).catch(() => []),
    getRepoAssociationRulesByRepo(repoName, 10).catch(() => []),
    getRepoTopActorsLatestByRepo(repoName, 10).catch(() => []),
  ]);

  const clusterProfile =
    clusterAssignment != null
      ? clusterProfilesAll.find((p) => p.clusterId === clusterAssignment.clusterId) ?? null
      : null;

  return {
    repoName,
    clusterAssignment,
    clusterProfile,
    dna,
    cohortAverages,
    watcherProfile,
    watcherLifts,
    community,
    similarEdges,
    assocRules,
    topActors,
  };
}

export type ActorSummary = {
  actorLogin: string;
  persona: {
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
  } | null;
  topRepos: Array<{ repoName: string; events: number; share: number }>;
  hourHeatmap: Array<{ dow: number; hour: number; events: number }>;
};

export async function getActorSummary(actorLogin: string): Promise<ActorSummary> {
  const actorLit = `'${actorLogin.replaceAll("'", "''")}'`;

  const [personaRows, topRepoRows, heatmapRows] = await Promise.all([
    queryJson<{
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
        SELECT max(metric_date) AS d FROM batch_actor_persona WHERE actor_login = ${actorLit}
      )
      SELECT
        persona_id, persona_label, is_bot, event_count, active_days, unique_repos,
        night_ratio, weekend_ratio, push_share, pr_share, issues_share, watch_share, fork_share,
        hour_entropy, repo_entropy, pca_x, pca_y
      FROM batch_actor_persona
      WHERE actor_login = ${actorLit}
        AND metric_date = (SELECT d FROM latest_date)
      LIMIT 1
    `),
    queryJson<{ repo_name: string; events: number; share: number }>(`
      WITH latest_date AS (
        SELECT max(metric_date) AS d FROM batch_repo_top_actors_latest
      ), actor_events AS (
        SELECT
          repo_name,
          actor_events AS events,
          actor_events / nullIf(sum(actor_events) OVER (), 0) AS share
        FROM batch_repo_top_actors_latest
        WHERE actor_login = ${actorLit}
          AND metric_date = (SELECT d FROM latest_date)
      )
      SELECT repo_name, events, coalesce(share, 0) AS share
      FROM actor_events
      ORDER BY events DESC
      LIMIT 8
    `),
    queryJson<{ dow: number; hour: number; events: number }>(`
      SELECT
        toDayOfWeek(event_date) AS dow,
        event_hour AS hour,
        sum(total_events) AS events
      FROM batch_daily_metrics
      WHERE 1=0
      GROUP BY dow, hour
    `).then(() => [] as { dow: number; hour: number; events: number }[]),
    // The heatmap would need per-actor per-hour data which we don't currently store;
    // we keep a stub to preserve the shape so the drawer can degrade gracefully.
  ]);

  const personaRow = personaRows[0] ?? null;

  return {
    actorLogin,
    persona: personaRow
      ? {
          personaId: Number(personaRow.persona_id),
          personaLabel: personaRow.persona_label,
          isBot: Number(personaRow.is_bot),
          eventCount: Number(personaRow.event_count),
          activeDays: Number(personaRow.active_days),
          uniqueRepos: Number(personaRow.unique_repos),
          nightRatio: Number(personaRow.night_ratio),
          weekendRatio: Number(personaRow.weekend_ratio),
          pushShare: Number(personaRow.push_share),
          prShare: Number(personaRow.pr_share),
          issuesShare: Number(personaRow.issues_share),
          watchShare: Number(personaRow.watch_share),
          forkShare: Number(personaRow.fork_share),
          hourEntropy: Number(personaRow.hour_entropy),
          repoEntropy: Number(personaRow.repo_entropy),
          pcaX: Number(personaRow.pca_x),
          pcaY: Number(personaRow.pca_y),
        }
      : null,
    topRepos: topRepoRows.map((r) => ({
      repoName: r.repo_name,
      events: Number(r.events),
      share: Number(r.share ?? 0),
    })),
    hourHeatmap: heatmapRows,
  };
}
