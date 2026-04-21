import {
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
} from "@/lib/dashboard";

import { EcosystemClient } from "./ecosystem-client";

export default async function OfflineEcosystemPage() {
  const [
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
  ] = await Promise.all([
    getDailyTrend(),
    getEventTypeBreakdown(30),
    getLanguageDayTrend(2000),
    getRepoTrendForecast(30),
    getTopReposLatest(10),
    getTopReposWeek(10).catch(() => []),
    getTopReposMonth(10, 30).catch(() => []),
    getTopUsersLatestWithBot(10).catch(() => []),
    getTopUsersWeek(10).catch(() => []),
    getTopUsersMonth(10, 30).catch(() => []),
  ]);

  const [
    concentrationDay,
    actorCohortDay,
    eventTypeShareShiftDay,
    offlineInsights,
    clusterProfile,
    changepoints,
  ] = await Promise.all([
    getConcentrationDay(30).catch(() => []),
    getActorCohortDay(30).catch(() => []),
    getEventTypeShareShiftDay(30).catch(() => []),
    getOfflineInsightsLatest(8).catch(() => []),
    getRepoClusterProfileLatest().catch(() => []),
    getEcosystemChangepoints(30).catch(() => []),
  ]);

  return (
    <EcosystemClient
      dailyTrend={dailyTrend}
      eventBreakdown={eventBreakdown}
      languageTrend={languageTrend}
      trendForecast={trendForecast}
      topRepos={topRepos}
      topReposWeek={topReposWeek}
      topReposMonth={topReposMonth}
      topUsers={topUsers}
      topUsersWeek={topUsersWeek}
      topUsersMonth={topUsersMonth}
      concentrationDay={concentrationDay}
      actorCohortDay={actorCohortDay}
      eventTypeShareShiftDay={eventTypeShareShiftDay}
      offlineInsights={offlineInsights}
      clusterProfile={clusterProfile}
      changepoints={changepoints}
    />
  );
}
