import {
  getActorBotSupervised,
  getActorBurstStability,
  getActorChurnRisk,
  getActorCohortDay,
  getActorCollabEdges,
  getActorGraphMetrics,
  getActorHotness,
  getActorPersonaBic,
  getActorPersonaBotValidation,
  getActorPersonaCentroids,
  getActorPersonaSample,
  getActorPersonaTransition,
  getActorRetentionCurves,
  getActorRings,
  getBotClassifierMeta,
  getBotFeatureImportance,
  getOrgRankLatest,
  getRepoBusFactor,
  getUserSegmentsLatest,
} from "@/lib/dashboard";

import { PeopleClient } from "./people-client";

export default async function OfflinePeoplePage() {
  const [
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
  ] = await Promise.all([
    getActorPersonaSample(2000).catch(() => []),
    getActorPersonaCentroids().catch(() => []),
    getOrgRankLatest(12).catch(() => []),
    getUserSegmentsLatest().catch(() => []),
    getActorCohortDay(30).catch(() => []),
    getActorPersonaBic().catch(() => []),
    getActorPersonaBotValidation().catch(() => []),
    getActorPersonaTransition().catch(() => []),
    getActorGraphMetrics(300).catch(() => []),
    getActorCollabEdges([], 300).catch(() => []),
    getActorBurstStability(300).catch(() => []),
    getActorRetentionCurves().catch(() => []),
    getActorChurnRisk(300).catch(() => []),
    getActorHotness(50).catch(() => []),
    getRepoBusFactor(80).catch(() => []),
    getActorBotSupervised(80).catch(() => []),
    getBotFeatureImportance().catch(() => []),
    getBotClassifierMeta().catch(() => []),
    getActorRings(30).catch(() => []),
  ]);

  return (
    <PeopleClient
      personaSample={personaSample}
      centroids={centroids}
      orgRank={orgRank}
      userSegments={userSegments}
      cohort={cohort}
      bicSweep={bicSweep}
      botValidation={botValidation}
      transitions={transitions}
      graphMetrics={graphMetrics}
      collabEdges={collabEdges}
      burstStability={burstStability}
      retentionCurves={retentionCurves}
      churnRisk={churnRisk}
      actorHotness={actorHotness}
      busFactor={busFactor}
      botSupervised={botSupervised}
      botImportance={botImportance}
      botMeta={botMeta}
      actorRings={actorRings}
    />
  );
}
