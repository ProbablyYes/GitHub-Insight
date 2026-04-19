import {
  getActorCoreness,
  getActorCorenessHistogram,
  getActorIcReach,
  getCommunityLineage,
  getRepoAlsNeighbors,
  getRepoArchetype,
  getRepoArchetypeCentroids,
  getRepoAssociationRules,
  getRepoCommunitiesTop,
  getRepoCommunityProfile,
  getRepoCommunityWeekly,
  getRepoCoreness,
  getRepoEmbeddingSummary,
  getRepoLayerCommunities,
  getRepoLayerEdges,
  getRepoMetapathSim,
  getRepoSimilarityEdges,
  getSeedGreedy,
} from "@/lib/dashboard";

import { NetworkClient } from "./network-client";

export default async function OfflineNetworkPage() {
  const [
    edges,
    communityRows,
    rules,
    communityProfiles,
    layerEdges,
    layerCommunities,
    actorCoreness,
    actorCorenessHist,
    repoCoreness,
    archetypePoints,
    archetypeCentroids,
    metapathSim,
    alsNeighbors,
    embeddingSummary,
    weekly,
    lineage,
    icReach,
    seedGreedy,
  ] = await Promise.all([
    getRepoSimilarityEdges(400).catch(() => []),
    getRepoCommunitiesTop(8).catch(() => []),
    getRepoAssociationRules(120).catch(() => []),
    getRepoCommunityProfile(12).catch(() => []),
    getRepoLayerEdges(1500).catch(() => []),
    getRepoLayerCommunities(3000).catch(() => []),
    getActorCoreness(200).catch(() => []),
    getActorCorenessHistogram().catch(() => []),
    getRepoCoreness(400).catch(() => []),
    getRepoArchetype(400).catch(() => []),
    getRepoArchetypeCentroids().catch(() => []),
    getRepoMetapathSim(null, 1200).catch(() => []),
    getRepoAlsNeighbors(600).catch(() => []),
    getRepoEmbeddingSummary().catch(() => null),
    getRepoCommunityWeekly(2000).catch(() => []),
    getCommunityLineage(500).catch(() => []),
    getActorIcReach().catch(() => []),
    getSeedGreedy(20).catch(() => []),
  ]);

  return (
    <NetworkClient
      edges={edges}
      communityRows={communityRows}
      rules={rules}
      communityProfiles={communityProfiles}
      layerEdges={layerEdges}
      layerCommunities={layerCommunities}
      actorCoreness={actorCoreness}
      actorCorenessHist={actorCorenessHist}
      repoCoreness={repoCoreness}
      archetypePoints={archetypePoints}
      archetypeCentroids={archetypeCentroids}
      metapathSim={metapathSim}
      alsNeighbors={alsNeighbors}
      embeddingSummary={embeddingSummary}
      weekly={weekly}
      lineage={lineage}
      icReach={icReach}
      seedGreedy={seedGreedy}
    />
  );
}
