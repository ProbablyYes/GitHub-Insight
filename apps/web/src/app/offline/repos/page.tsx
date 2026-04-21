import {
  getAdvancedRepoRankings,
  getAuthenticityInputs,
  getBurstStabilitySnapshot,
  getDynamicAttribution30d,
  getHotVsColdAttribution,
  getOfflineAnomalyAlertsLatest,
  getOfflineDeclineWarningsLatest,
  getRepoContributorConcentrationLatest,
  getRepoDnaCohortStats,
  getRepoDnaOutliers,
  getRepoDnaTopHot,
  getRepoRankDeltaExplainLatest,
  getRepoWatcherProfileLatest,
  getTopRepoRankPaths,
} from "@/lib/dashboard";
import { computeAuthenticity } from "@/lib/authenticity";

import { ReposClient } from "./repos-client";

export default async function OfflineReposPage() {
  const [
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
    authenticityInputs,
  ] = await Promise.all([
    getAdvancedRepoRankings(16),
    getBurstStabilitySnapshot(80),
    getRepoContributorConcentrationLatest(12),
    getRepoRankDeltaExplainLatest(12),
    getHotVsColdAttribution(90, "__all__").catch(() => []),
    getDynamicAttribution30d(30).catch(() => []),
    getRepoDnaTopHot(10).catch(() => []),
    getRepoDnaCohortStats().catch(() => []),
    getRepoDnaOutliers(12).catch(() => []),
    getRepoWatcherProfileLatest(20).catch(() => []),
    getOfflineAnomalyAlertsLatest(10).catch(() => []),
    getOfflineDeclineWarningsLatest(10).catch(() => []),
    getTopRepoRankPaths(8, 30).catch(() => []),
    getAuthenticityInputs(150).catch(() => []),
  ]);

  const authenticity = computeAuthenticity(authenticityInputs);

  return (
    <ReposClient
      rankings={rankings}
      burstSnapshot={burstSnapshot}
      contributorConcentration={contributorConcentration}
      rankDeltas={rankDeltas}
      attribution={attribution}
      dynamicAttribution={dynamicAttribution}
      dnaTopHot={dnaTopHot}
      dnaCohortStats={dnaCohortStats}
      dnaOutliers={dnaOutliers}
      watcherProfile={watcherProfile}
      anomalyAlerts={anomalyAlerts}
      declineWarnings={declineWarnings}
      rankPaths={rankPaths}
      authenticity={authenticity}
    />
  );
}
