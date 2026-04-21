"use client";

import { useEffect, useMemo, useRef } from "react";

import { DashboardCharts } from "@/components/dashboard-charts";
import { EntityLink } from "@/components/entity";
import { OfflineSubnav } from "@/components/offline-subnav";
import {
  PixelBadge,
  PixelKpi,
  PixelSearchTable,
  PixelSection,
  type PixelSearchColumn,
} from "@/components/pixel";
import { PixelPageShell } from "@/components/pixel-shell";
import type {
  getActorPersonaCentroids,
  getHotVsColdAttribution,
  getPersonaBic,
  getPersonaBotValidation,
  getRepoClusterProfileLatest,
  getRepoClustersLatest,
  getRepoDnaTop,
  RepoClusterProfilePoint,
  PersonaBotValidationPoint,
} from "@/lib/dashboard";

import { AttributionForest } from "./attribution-forest";
import { BicSweepChart } from "./bic-sweep-chart";
import { ClusterScatter } from "./cluster-scatter";
import { DnaRadar } from "./dna-radar";

type Awaited2<T> = T extends Promise<infer U> ? U : T;
type Ret<T extends (...args: never[]) => unknown> = Awaited2<ReturnType<T>>;

function shortLabel(value: string, maxLength = 18): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 3)}...`;
}

function formatPercent(value: number, digits = 1): string {
  return `${(value * 100).toFixed(digits)}%`;
}

/** Shared explanation block — same visual language as the Network page. */
function DataReading({
  children,
  tone = "positive",
  title = "Reading the numbers",
}: {
  children: React.ReactNode;
  tone?: "positive" | "info" | "change" | "magenta" | "purple" | "danger";
  title?: string;
}) {
  const color =
    {
      positive: "var(--accent-positive)",
      info: "var(--accent-info)",
      change: "var(--accent-change)",
      magenta: "var(--accent-magenta)",
      purple: "var(--accent-purple)",
      danger: "var(--accent-danger)",
    }[tone] ?? "var(--accent-positive)";
  return (
    <div
      style={{
        marginTop: 12,
        padding: "10px 12px",
        borderLeft: `3px solid ${color}`,
        background: "var(--surface-elevated, rgba(255,255,255,0.02))",
        fontSize: 11,
        lineHeight: 1.6,
        color: "var(--fg)",
      }}
    >
      <div
        style={{
          color,
          fontSize: 10,
          letterSpacing: 2,
          fontWeight: 700,
          marginBottom: 4,
        }}
      >
        {`==> ${title.toUpperCase()}`}
      </div>
      {children}
    </div>
  );
}

type MlProps = {
  clusters: Ret<typeof getRepoClustersLatest>;
  clusterProfiles: Ret<typeof getRepoClusterProfileLatest>;
  personaCentroids: Ret<typeof getActorPersonaCentroids>;
  attributionAll: Ret<typeof getHotVsColdAttribution>;
  attributionHumans: Ret<typeof getHotVsColdAttribution>;
  attributionBots: Ret<typeof getHotVsColdAttribution>;
  personaBic: Ret<typeof getPersonaBic>;
  botValidation: Ret<typeof getPersonaBotValidation>;
  repoDna: Ret<typeof getRepoDnaTop>;
  focusCluster?: number | null;
};

export function MlClient({
  clusters,
  clusterProfiles,
  personaCentroids,
  attributionAll,
  attributionHumans,
  attributionBots,
  personaBic,
  botValidation,
  repoDna,
  focusCluster = null,
}: MlProps) {
  const scatterRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (focusCluster != null && scatterRef.current) {
      scatterRef.current.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }, [focusCluster]);

  /* ──────────── derived summaries used in KPIs / headlines ──────────── */
  const bicSelected = useMemo(
    () =>
      personaBic.find((b) => b.isSelected === 1) ??
      personaBic.reduce<{ k: number; bic: number } | null>(
        (acc, b) => (acc == null || b.bic < acc.bic ? { k: b.k, bic: b.bic } : acc),
        null,
      ),
    [personaBic],
  );

  const botPersona = useMemo(() => {
    const byF1 = [...botValidation].sort((a, b) => b.f1 - a.f1);
    return byF1[0] ?? null;
  }, [botValidation]);

  const attributionRobustness = useMemo(() => {
    // Count features where removing bots (humans_only) flips the sign of Cohen's d
    // compared to the full-population (all) result.
    const allMap = new Map(attributionAll.map((r) => [r.featureName, r]));
    const humanMap = new Map(attributionHumans.map((r) => [r.featureName, r]));
    const overlap = [...allMap.keys()].filter((f) => humanMap.has(f));
    let flipped = 0;
    let largeInAll = 0;
    for (const f of overlap) {
      const a = allMap.get(f)!;
      const h = humanMap.get(f)!;
      if (Math.sign(a.cohenD) !== 0 && Math.sign(a.cohenD) !== Math.sign(h.cohenD)) {
        flipped += 1;
      }
      if (Math.abs(a.cohenD) >= 0.8) largeInAll += 1;
    }
    return { overlap: overlap.length, flipped, largeInAll };
  }, [attributionAll, attributionHumans]);

  const mergedAttribution = useMemo(
    () => [...attributionAll, ...attributionHumans, ...attributionBots],
    [attributionAll, attributionHumans, attributionBots],
  );

  /* ──────────── legacy visualisations we keep ──────────── */
  const clusterCounts = clusters.reduce<Record<string, number>>((acc, item) => {
    const key = String(item.clusterId);
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const clusterCountData = Object.entries(clusterCounts)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([clusterId, countValue]) => ({ label: `cluster ${clusterId}`, value: countValue }));

  const personaShareData = personaCentroids.map((row) => ({
    label: `${row.personaId}:${shortLabel(row.personaLabel, 12)}`,
    value: Number((row.share * 100).toFixed(2)),
  }));

  const clusterProfileCols: PixelSearchColumn<RepoClusterProfilePoint>[] = [
    { key: "clusterId", header: "id", align: "right", sortValue: (r) => r.clusterId },
    {
      key: "clusterLabel",
      header: "label",
      align: "left",
      sortValue: (r) => r.clusterLabel,
      render: (r) => (
        <PixelBadge tone="info" size="sm">
          {r.clusterLabel}
        </PixelBadge>
      ),
    },
    { key: "members", header: "#", align: "right", sortValue: (r) => r.members },
    {
      key: "shareOfRepos",
      header: "share",
      align: "right",
      sortValue: (r) => r.shareOfRepos,
      render: (r) => formatPercent(r.shareOfRepos),
    },
    { key: "avgHotness", header: "hot", align: "right", sortValue: (r) => r.avgHotness, render: (r) => r.avgHotness.toFixed(3) },
    { key: "avgMomentum", header: "mom", align: "right", sortValue: (r) => r.avgMomentum, render: (r) => r.avgMomentum.toFixed(3) },
    { key: "avgEngagement", header: "eng", align: "right", sortValue: (r) => r.avgEngagement, render: (r) => r.avgEngagement.toFixed(3) },
    { key: "avgStability", header: "stab", align: "right", sortValue: (r) => r.avgStability, render: (r) => r.avgStability.toFixed(3) },
    { key: "avgBotRatio", header: "bot", align: "right", sortValue: (r) => r.avgBotRatio, render: (r) => formatPercent(r.avgBotRatio, 0) },
    {
      key: "sampleRepos",
      header: "sample",
      align: "left",
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
  ];

  const botValCols: PixelSearchColumn<PersonaBotValidationPoint>[] = [
    { key: "personaLabel", header: "persona", align: "left", sortValue: (r) => r.personaLabel,
      render: (r) => (
        <PixelBadge
          tone={r.personaLabel.includes("bot") ? "magenta" : "info"}
          size="sm"
        >
          {r.personaLabel}
        </PixelBadge>
      ),
    },
    { key: "trueBots", header: "TP", align: "right", sortValue: (r) => r.trueBots },
    { key: "falseBots", header: "FP", align: "right", sortValue: (r) => r.falseBots },
    { key: "missedBots", header: "FN", align: "right", sortValue: (r) => r.missedBots },
    {
      key: "precision",
      header: "precision",
      align: "right",
      sortValue: (r) => r.precision,
      render: (r) => (
        <span style={{ color: r.precision >= 0.8 ? "var(--accent-positive)" : r.precision >= 0.5 ? "var(--accent-change)" : "var(--muted)" }}>
          {(r.precision * 100).toFixed(1)}%
        </span>
      ),
    },
    {
      key: "recall",
      header: "recall",
      align: "right",
      sortValue: (r) => r.recall,
      render: (r) => (
        <span style={{ color: r.recall >= 0.8 ? "var(--accent-positive)" : r.recall >= 0.5 ? "var(--accent-change)" : "var(--muted)" }}>
          {(r.recall * 100).toFixed(1)}%
        </span>
      ),
    },
    {
      key: "f1",
      header: "F1",
      align: "right",
      sortValue: (r) => r.f1,
      render: (r) => (
        <strong
          style={{
            color: r.f1 >= 0.7 ? "var(--accent-positive)" : r.f1 >= 0.4 ? "var(--accent-change)" : "var(--muted)",
          }}
        >
          {(r.f1 * 100).toFixed(1)}%
        </strong>
      ),
    },
  ];

  return (
    <PixelPageShell
      title="ML Lab · Methodology & Validation"
      subtitle="How we picked k, how we know the personas are real, and where the hot/cold signal actually comes from — the receipts behind every label on the other pages."
      breadcrumbs={[
        { label: "Offline", href: "/offline" },
        { label: "ML Lab" },
      ]}
      tldr={
        <>
          Four decisions back every label shown elsewhere: <strong>model choice</strong>, <strong>persona count</strong>,
          <strong> bot disambiguation</strong>, <strong>cohort effects</strong>. Each now has an auditable number —
          BIC picked <strong>k = {bicSelected?.k ?? "?"}</strong>,{" "}
          <strong>{botPersona ? `${(botPersona.precision * 100).toFixed(0)}%` : "?"}</strong> precision on the bot persona,{" "}
          <strong>{attributionRobustness.largeInAll}</strong> large-effect features survived,{" "}
          <strong>{attributionRobustness.flipped}</strong> flipped sign once bots were removed.
        </>
      }
    >
      <OfflineSubnav />

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "var(--pixel-space-4)",
          marginBottom: "var(--pixel-space-5)",
        }}
      >
        <PixelKpi
          label="Selected k (GMM)"
          value={bicSelected?.k ?? "—"}
          tone="magenta"
          hint={`BIC = ${bicSelected ? Math.round(bicSelected.bic).toLocaleString() : "—"}`}
        />
        <PixelKpi
          label="Bot persona F1"
          value={botPersona ? `${(botPersona.f1 * 100).toFixed(1)}%` : "—"}
          tone="positive"
          hint={botPersona ? `prec ${(botPersona.precision * 100).toFixed(0)}% · rec ${(botPersona.recall * 100).toFixed(0)}%` : undefined}
        />
        <PixelKpi
          label="Large effects (|d|>=0.8)"
          value={attributionRobustness.largeInAll}
          tone="info"
          hint={`out of ${attributionRobustness.overlap} features`}
        />
        <PixelKpi
          label="Sign flips after bot removal"
          value={attributionRobustness.flipped}
          tone={attributionRobustness.flipped === 0 ? "positive" : "change"}
          hint="humans-only vs all"
        />
      </div>

      {/* §1 Pipeline ──────────── */}
      <PixelSection
        title="§1 Pipeline — one parquet, five sibling jobs"
        tone="positive"
        headline="Every number below comes from the same curated 30-day parquet, so cross-page comparisons are coherent."
        source="jobs/batch/spark_job.py"
        techBadge="Apache Spark · PySpark MLlib"
      >
        <pre
          style={{
            color: "var(--fg)",
            background: "var(--bg)",
            padding: "var(--pixel-space-3)",
            border: "1px solid var(--divider)",
            fontSize: 11,
            margin: 0,
            whiteSpace: "pre-wrap",
            lineHeight: 1.5,
          }}
        >
{`30-day curated parquet ──► Spark DataFrames ──► feature engineering
   |
   |--> StandardScaler --> KMeans(k=4) + PCA(2)      -> batch_repo_clusters / cluster_profile
   |--> StandardScaler --> GMM(k=* from BIC, full)   -> batch_actor_persona
   |                    + PCA(2)                     -> batch_actor_persona_centroid
   |                    + BIC sweep k=3..8           -> batch_actor_persona_bic
   |                    + bot-label confusion        -> batch_actor_persona_bot_validation
   |--> Welch's t + Cohen's d + Bootstrap 95% CI     -> batch_hot_vs_cold_attribution (all/humans_only/bots_only)
   |--> per-repo 17-dim standardised feature vector  -> batch_repo_dna
   \\--> Jaccard + wLPA + FPGrowth (companion jobs)  -> Network / Ecosystem pages`}
        </pre>
        <DataReading tone="positive" title="Why this page exists">
          The other four offline pages present <em>conclusions</em> (who is hot, which repos cluster together,
          who automates everything). This page presents the <em>receipts</em>: at every place the pipeline made
          an arbitrary-looking choice — how many clusters? which persona is the &ldquo;bot&rdquo; one? is the
          hot/cold gap real or just bots? — we record the supporting statistic here so the decision can be
          audited rather than trusted on faith.
        </DataReading>
      </PixelSection>

      {/* §2 BIC ──────────── */}
      <PixelSection
        title="§2 How we picked k — BIC sweep for the GMM"
        tone="magenta"
        headline={
          bicSelected
            ? `Sweep over k=3..8 · selected k=${bicSelected.k} at the BIC minimum.`
            : "BIC sweep over k=3..8 for the persona GaussianMixture."
        }
        source="batch_actor_persona_bic"
        techBadge="GMM · BIC = k·ln(n) − 2·ln(L)"
      >
        <BicSweepChart
          data={personaBic.map((b) => ({
            k: b.k,
            bic: b.bic,
            logLikelihood: b.logLikelihood,
            nParams: b.nParams,
            isSelected: b.isSelected,
          }))}
        />
        <DataReading tone="magenta" title="Reading the curves">
          Log-likelihood (dashed blue) rises monotonically — any extra cluster lets the model memorise the data
          better. That&apos;s why we can&apos;t use likelihood alone to choose <em>k</em>. BIC (solid magenta)
          adds a penalty proportional to how many free parameters the model has, so it forms a U-shape: the
          bottom of the U is the <strong>simplest</strong> model that still explains the data well. We pick the{" "}
          <span style={{ color: "var(--accent-positive)", fontWeight: 700 }}>green dot</span> —{" "}
          <strong>k = {bicSelected?.k ?? "?"}</strong> — and every persona mentioned on the People/Network
          pages is a member of one of those {bicSelected?.k ?? "?"} Gaussian components.
        </DataReading>
      </PixelSection>

      {/* §3 Bot validation ──────────── */}
      <PixelSection
        title="§3 Do the personas actually catch bots? — confusion-matrix validation"
        tone="info"
        headline={
          botPersona
            ? `Each persona is scored against the is_bot ground-truth label. The ${botPersona.personaLabel} persona achieves F1 = ${(botPersona.f1 * 100).toFixed(1)}%.`
            : "Each persona is scored against the is_bot ground-truth label."
        }
        source="batch_actor_persona_bot_validation"
        techBadge="Precision · Recall · F1 · ground truth: actor.is_bot"
      >
        {botValidation.length ? (
          <PixelSearchTable
            rows={botValidation}
            columns={botValCols}
            getRowKey={(r) => r.personaLabel}
            initialSort={{ key: "f1", desc: true }}
            pageSize={10}
            searchable={false}
            fontSize={11}
          />
        ) : (
          <p style={{ color: "var(--muted)", fontSize: 11 }}>No validation data.</p>
        )}
        <DataReading tone="info" title="Why this matters">
          GMM is unsupervised — nothing told it which cluster should be &ldquo;the bot cluster.&rdquo; We
          label each persona purely from centroid thresholds (<code>is_bot&nbsp;&gt;&nbsp;0.5</code> ⇒{" "}
          <code>bot_fleet</code>). The table above treats those labels as if they were a bot classifier and
          compares them against the ground-truth <code>actor.is_bot</code> flag.{" "}
          <strong>Precision</strong> = &ldquo;of the accounts I called bots, what fraction actually are bots?&rdquo;,{" "}
          <strong>recall</strong> = &ldquo;of all real bots, how many did I catch?&rdquo;, <strong>F1</strong>{" "}
          balances the two. A persona labelled <code>bot_*</code> with precision above ~80% means the unsupervised
          step independently rediscovered the same accounts the <code>is_bot</code> column tags — a far
          stronger argument than &ldquo;look, the cluster looks bot-ish.&rdquo;
        </DataReading>
      </PixelSection>

      {/* §4 Attribution forest ──────────── */}
      <PixelSection
        title="§4 Attribution — which features separate hot vs cold, robustly?"
        tone="change"
        headline={
          attributionRobustness.flipped === 0
            ? `Top features by |Cohen's d|, shown in three cohort scopes. Across ${attributionRobustness.overlap} overlapping features, zero change direction when bots are removed — the hot/cold story is driven by humans.`
            : `Top features by |Cohen's d|. ${attributionRobustness.flipped} of ${attributionRobustness.overlap} features FLIP direction after removing bots — those should not be trusted as human signal.`
        }
        source="batch_hot_vs_cold_attribution"
        techBadge="Welch's t-test · Cohen's d · 1000× bootstrap 95% CI"
      >
        <AttributionForest rows={mergedAttribution} maxFeatures={10} />
        <DataReading tone="change" title="Reading the forest plot">
          Each row is a feature. Per feature we plot three bars: <span style={{ color: "var(--accent-positive)" }}>all
          accounts</span>, <span style={{ color: "var(--accent-info)" }}>humans only</span>,{" "}
          <span style={{ color: "var(--accent-change)" }}>bots only</span>. The dot is the point estimate of{" "}
          Cohen&apos;s d (standardised mean difference between hot and cold repos on that feature); the bar is
          the bootstrap 95% CI. By convention <em>|d|&nbsp;≥&nbsp;0.2 small · 0.5 medium · 0.8 large</em>. A
          faded bar crosses zero — not distinguishable from noise. The comparison is the entire point: if the{" "}
          <span style={{ color: "var(--accent-info)" }}>humans</span> bar is a close copy of the{" "}
          <span style={{ color: "var(--accent-positive)" }}>all</span> bar, the signal isn&apos;t an artefact
          of bot activity. If they disagree — especially if one is positive and the other negative — that
          feature is bot-driven and anything inferred from it on other pages is shaky.
        </DataReading>
      </PixelSection>

      {/* §5 DNA radar ──────────── */}
      <PixelSection
        title="§5 Repo DNA — the hot/cold difference from a second angle"
        tone="purple"
        headline="Mean behavioural fingerprint of the 6 most-hot vs 6 most-cold repos, on 8 raw [0,1] dimensions."
        source="batch_repo_dna"
        techBadge="17-dim behavioural fingerprint · z-scored + unscaled share features"
      >
        <DnaRadar rows={repoDna} />
        <DataReading tone="purple" title="Why two views of the same thing">
          §4 answered &ldquo;which single features matter&rdquo; in z-score units with formal CIs. §5 answers{" "}
          &ldquo;what <em>shape</em> does a hot repo have overall&rdquo; on the raw share axes. They should
          tell a consistent story: if the magenta polygon sticks out on <em>watch</em> and <em>fork</em>, §4
          should also be placing those features near the top with positive <em>d</em>. When the two views
          agree, the conclusion is structural; when they disagree, something funny is happening in one
          direction (e.g. a handful of outlier repos pulling the mean in §5 that the CI in §4 correctly flags
          as overlapping zero).
        </DataReading>
      </PixelSection>

      {/* §6 KMeans recipe ──────────── */}
      <PixelSection
        title="§6 The 4-cluster recipe used on the Repos page"
        tone="info"
        headline="KMeans(k=4) on 5 z-normalised features (hotness · momentum · engagement · stability · bot). This is literally how each repo gets its cluster id."
        source="batch_repo_clusters + batch_repo_cluster_profile"
        techBadge="Spark MLlib · StandardScaler · KMeans(k=4) · PCA(2) for display only"
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
            gap: "var(--pixel-space-4)",
          }}
        >
          <div>
            <div style={{ color: "var(--muted)", fontSize: 10, letterSpacing: 1, marginBottom: 4 }}>
              CLUSTER SIZES
            </div>
            <DashboardCharts variant="bar" data={clusterCountData} color="#33ccff" />
          </div>
          <div ref={scatterRef}>
            <div style={{ color: "var(--muted)", fontSize: 10, letterSpacing: 1, marginBottom: 4 }}>
              PCA(2) PROJECTION
              {focusCluster != null ? ` · focus cluster #${focusCluster}` : ""}
            </div>
            {clusters.length > 0 ? (
              <ClusterScatter
                data={clusters.map((c) => ({
                  repoName: c.repoName,
                  clusterId: c.clusterId,
                  pcaX: c.pcaX,
                  pcaY: c.pcaY,
                  healthScore: c.healthScore,
                  rankNo: c.rankNo,
                }))}
                focusCluster={focusCluster}
              />
            ) : (
              <p style={{ color: "var(--muted)", textAlign: "center" }}>No clustering data.</p>
            )}
          </div>
        </div>

        <div style={{ marginTop: 16 }}>
          <div style={{ color: "var(--muted)", fontSize: 10, letterSpacing: 1, marginBottom: 4 }}>
            CENTROID PROFILES
          </div>
          <PixelSearchTable
            rows={clusterProfiles}
            columns={clusterProfileCols}
            getRowKey={(r) => r.clusterId}
            initialSort={{ key: "members", desc: true }}
            pageSize={8}
            searchable={false}
            fontSize={11}
          />
        </div>

        <DataReading tone="info" title="What you're seeing">
          Bar chart (left) = how many repos landed in each of the 4 KMeans clusters — the skew is not a bug, it
          reflects that most repos share the &ldquo;average&rdquo; behaviour. PCA scatter (right) = the same
          repos projected onto the two axes that capture the most variance; points are coloured by cluster id.
          Well-separated colours = the 4 clusters live in distinct regions of feature space, not arbitrary
          slices of a continuous blob. The profile table below lists the mean of each feature per cluster —
          that&apos;s how the &ldquo;hot/momentum/engagement/stability/bot&rdquo; labels on the Repos page are
          derived (thresholding centroid means).
        </DataReading>
      </PixelSection>

      {/* §7 GMM persona share ──────────── */}
      <PixelSection
        title="§7 GMM persona mix — soft assignment at selected k"
        tone="magenta"
        headline={
          bicSelected
            ? `k = ${bicSelected.k} (chosen in §2) · share = fraction of actors whose highest-probability component is that persona.`
            : "Share = fraction of actors whose highest-probability component is that persona."
        }
        source="batch_actor_persona_centroid"
        techBadge="GaussianMixture · full covariance · soft assignment"
        howToRead="Labels come from thresholding centroids (e.g. night_ratio>0.5 ⇒ night_owl_coder, pr_share>0.4 ⇒ pr_reviewer, is_bot>0.5 ⇒ bot_fleet)."
      >
        <DashboardCharts variant="bar" data={personaShareData} color="#ff66cc" />
        <DataReading tone="magenta" title="Cross-check with §3">
          This bar chart is what the People page ships as its &ldquo;audience mix&rdquo;. The fact that it can
          be trusted relies on two things on this page: §2 shows <em>k</em> wasn&apos;t cherry-picked, §3 shows
          the <code>bot_*</code> persona on the right end of this chart really is dominated by bot accounts
          (high precision against ground truth). Anything else claimed about these personas elsewhere
          ultimately comes from the centroid numbers behind this bar — which is why §6 above reports the
          analogous centroid-profile table for the repo-side KMeans.
        </DataReading>
      </PixelSection>
    </PixelPageShell>
  );
}
