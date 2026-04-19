import {
  getActorPersonaCentroids,
  getHotVsColdAttribution,
  getPersonaBic,
  getPersonaBotValidation,
  getRepoClusterProfileLatest,
  getRepoClustersLatest,
  getRepoDnaTop,
} from "@/lib/dashboard";

import { MlClient } from "./ml-client";

type PageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function OfflineMlPage({ searchParams }: PageProps) {
  const resolvedParams = searchParams ? await searchParams : {};
  const rawCluster = resolvedParams?.cluster;
  const clusterStr = Array.isArray(rawCluster) ? rawCluster[0] : rawCluster;
  const focusCluster =
    clusterStr != null && /^\d+$/.test(clusterStr) ? Number(clusterStr) : null;

  const [
    clusters,
    clusterProfiles,
    personaCentroids,
    attributionAll,
    attributionHumans,
    attributionBots,
    personaBic,
    botValidation,
    repoDna,
  ] = await Promise.all([
    getRepoClustersLatest(),
    getRepoClusterProfileLatest().catch(() => []),
    getActorPersonaCentroids().catch(() => []),
    getHotVsColdAttribution(20, "all").catch(() => []),
    getHotVsColdAttribution(20, "humans_only").catch(() => []),
    getHotVsColdAttribution(20, "bots_only").catch(() => []),
    getPersonaBic().catch(() => []),
    getPersonaBotValidation().catch(() => []),
    getRepoDnaTop(6).catch(() => []),
  ]);

  return (
    <MlClient
      clusters={clusters}
      clusterProfiles={clusterProfiles}
      personaCentroids={personaCentroids}
      attributionAll={attributionAll}
      attributionHumans={attributionHumans}
      attributionBots={attributionBots}
      personaBic={personaBic}
      botValidation={botValidation}
      repoDna={repoDna}
      focusCluster={focusCluster}
    />
  );
}
