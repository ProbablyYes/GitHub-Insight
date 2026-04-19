"use client";

import { useState } from "react";

import { PixelBadge, PixelChip, PixelMiniBar } from "@/components/pixel";

import { EntityLink } from "./EntityLink";

type RepoSummary = {
  repoName: string;
  clusterAssignment: { clusterId: number; pcaX: number; pcaY: number } | null;
  clusterProfile: {
    clusterId: number;
    clusterLabel: string;
    members: number;
    avgHotness: number;
  } | null;
  dna: Record<string, unknown> | null;
  cohortAverages: Record<string, Record<string, number>>;
  watcherProfile: {
    rankNo: number;
    watchers: number;
    dominantPersona: string;
    dominantShare: number;
    dominantLift: number;
    botRatio: number;
  } | null;
  watcherLifts: Array<{ personaLabel: string; shareInWatchers: number; shareInGlobal: number; lift: number }>;
  community: { communityId: string; communitySize: number; sampleMembers: string } | null;
  similarEdges: Array<{ srcRepo: string; dstRepo: string; jaccard: number; sharedActors: number }>;
  assocRules: Array<{ antecedent: string; consequent: string; support: number; confidence: number; lift: number }>;
  topActors: Array<{ actorLogin: string; actorCategory: string; actorEvents: number; share: number }>;
};

const DNA_FEATURES: Array<{ key: string; label: string }> = [
  { key: "watchShare", label: "watch_share" },
  { key: "forkShare", label: "fork_share" },
  { key: "issuesShare", label: "issues_share" },
  { key: "prShare", label: "pr_share" },
  { key: "pushShare", label: "push_share" },
  { key: "botRatio", label: "bot_ratio" },
  { key: "nightRatio", label: "night_ratio" },
  { key: "weekendRatio", label: "weekend_ratio" },
  { key: "prPushRatio", label: "pr_push_ratio" },
  { key: "activeDaysRatio", label: "active_days_ratio" },
  { key: "actorsPerEvent", label: "actors_per_event" },
  { key: "eventEntropy", label: "event_entropy" },
  { key: "top1ActorShare", label: "top1_actor_share" },
];

const TABS = ["overview", "dna", "watchers", "network"] as const;
type Tab = (typeof TABS)[number];

export function RepoDrawerContent({ summary }: { summary: RepoSummary }) {
  const [tab, setTab] = useState<Tab>("overview");

  if (!summary || typeof summary !== "object") {
    return <div style={{ color: "var(--muted)", fontSize: 12 }}>No repo data.</div>;
  }

  // Harden: ensure array props exist even if the API returned a partial shape.
  const safeSummary: RepoSummary = {
    ...summary,
    topActors: Array.isArray(summary.topActors) ? summary.topActors : [],
    watcherLifts: Array.isArray(summary.watcherLifts) ? summary.watcherLifts : [],
    similarEdges: Array.isArray(summary.similarEdges) ? summary.similarEdges : [],
    assocRules: Array.isArray(summary.assocRules) ? summary.assocRules : [],
    cohortAverages: summary.cohortAverages ?? {},
  };

  const cohort = (safeSummary.dna as { cohortGroup?: string } | null)?.cohortGroup ?? null;
  const cohortAvg = cohort ? safeSummary.cohortAverages[cohort] : undefined;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-3)" }}>
      <div style={{ display: "flex", gap: 4, borderBottom: "1px solid var(--divider)", paddingBottom: 6 }}>
        {TABS.map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setTab(t)}
            className={`nes-btn ${tab === t ? "is-primary" : ""}`}
            style={{ padding: "2px 8px", fontSize: 11 }}
          >
            {t}
          </button>
        ))}
      </div>

      {tab === "overview" ? <OverviewTab summary={safeSummary} /> : null}
      {tab === "dna" ? <DnaTab summary={safeSummary} cohort={cohort} cohortAvg={cohortAvg} /> : null}
      {tab === "watchers" ? <WatchersTab summary={safeSummary} /> : null}
      {tab === "network" ? <NetworkTab summary={safeSummary} /> : null}
    </div>
  );
}

function OverviewTab({ summary }: { summary: RepoSummary }) {
  const dna = summary.dna as Record<string, number | string> | null;
  const cohort = (dna?.cohortGroup as string) ?? "unknown";
  const tone = cohort === "hot" ? "positive" : cohort === "cold" ? "danger" : "change";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-3)", fontSize: 12 }}>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <PixelBadge tone={tone}>cohort: {cohort}</PixelBadge>
        {summary.clusterProfile ? (
          <PixelBadge tone="info">
            cluster #{summary.clusterProfile.clusterId} · {summary.clusterProfile.clusterLabel}
          </PixelBadge>
        ) : null}
        {summary.watcherProfile ? (
          <PixelBadge tone="magenta">
            rank #{summary.watcherProfile.rankNo} · {summary.watcherProfile.watchers} watchers
          </PixelBadge>
        ) : null}
      </div>

      {dna ? (
        <div style={{ color: "var(--muted-strong)", lineHeight: 1.5 }}>
          Events: <span style={{ color: "var(--fg)" }}>{Number(dna.totalEvents).toLocaleString()}</span> · rank
          score: <span style={{ color: "var(--accent-positive)" }}>{Number(dna.rankScore).toFixed(3)}</span>
        </div>
      ) : (
        <div style={{ color: "var(--muted)" }}>No DNA record for this repo.</div>
      )}

      {summary.topActors.length > 0 ? (
        <div>
          <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Top actors</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            {summary.topActors.slice(0, 6).map((a) => (
              <div key={a.actorLogin} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                <EntityLink type="actor" id={a.actorLogin} />
                <span style={{ color: "var(--muted)", fontSize: 11 }}>
                  {a.actorEvents.toLocaleString()} evts ({(Number(a.share) * 100).toFixed(1)}%)
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <a
        href={`https://github.com/${summary.repoName}`}
        target="_blank"
        rel="noreferrer noopener"
        className="nes-btn is-primary"
        style={{ textAlign: "center", padding: "6px 10px", fontSize: 12, textDecoration: "none" }}
      >
        {">> Open on GitHub"}
      </a>
    </div>
  );
}

function DnaTab({
  summary,
  cohort,
  cohortAvg,
}: {
  summary: RepoSummary;
  cohort: string | null;
  cohortAvg: Record<string, number> | undefined;
}) {
  const dna = summary.dna as Record<string, number | string> | null;
  if (!dna) {
    return <div style={{ color: "var(--muted)" }}>No DNA profile available.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-2)" }}>
      <div style={{ color: "var(--muted-strong)", fontSize: 11 }}>
        White line = <span style={{ color: "var(--fg-strong)" }}>{cohort ?? "n/a"}</span> cohort mean; bar = this
        repo.
      </div>
      {DNA_FEATURES.map((f) => {
        const value = Number(dna[f.key] ?? 0);
        const ref = cohortAvg ? cohortAvg[f.key] : null;
        const max = Math.max(value, ref ?? 0, 0.05) * 1.1;
        const dev = ref != null && ref > 1e-6 ? (value - ref) / ref : 0;
        const tone = Math.abs(dev) < 0.1 ? "muted" : dev > 0 ? "positive" : "danger";
        return (
          <PixelMiniBar
            key={f.key}
            value={value}
            reference={ref ?? null}
            min={0}
            max={max}
            tone={tone}
            width={120}
            height={8}
            label={f.label}
            valueText={value.toFixed(3)}
          />
        );
      })}
    </div>
  );
}

function WatchersTab({ summary }: { summary: RepoSummary }) {
  if (!summary.watcherProfile && summary.watcherLifts.length === 0) {
    return <div style={{ color: "var(--muted)" }}>No watcher data.</div>;
  }
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-3)", fontSize: 12 }}>
      {summary.watcherProfile ? (
        <div>
          <div style={{ color: "var(--muted-strong)" }}>Headline</div>
          <div style={{ color: "var(--fg)", marginTop: 2 }}>
            Dominant persona <span style={{ color: "var(--accent-magenta)" }}>{summary.watcherProfile.dominantPersona}</span>
            {" · "}
            share {(summary.watcherProfile.dominantShare * 100).toFixed(1)}% · lift
            {" "}
            <span style={{ color: "var(--accent-positive)" }}>{summary.watcherProfile.dominantLift.toFixed(2)}×</span>
            {" · "}
            bots {(summary.watcherProfile.botRatio * 100).toFixed(1)}%
          </div>
        </div>
      ) : null}

      {summary.watcherLifts.length > 0 ? (
        <div>
          <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Persona lift vs global</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            {summary.watcherLifts.slice(0, 8).map((l) => (
              <PixelMiniBar
                key={l.personaLabel}
                value={Math.min(l.lift, 5)}
                reference={1}
                min={0}
                max={5}
                tone={l.lift > 1 ? "positive" : "muted"}
                width={130}
                height={8}
                label={l.personaLabel}
                valueText={`${l.lift.toFixed(2)}×`}
              />
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function NetworkTab({ summary }: { summary: RepoSummary }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-3)", fontSize: 12 }}>
      {summary.community ? (
        <div>
          <div style={{ color: "var(--muted-strong)" }}>Community</div>
          <div style={{ color: "var(--fg)", marginTop: 2 }}>
            id <span style={{ color: "var(--accent-info)" }}>{summary.community.communityId}</span> · size{" "}
            {summary.community.communitySize}
          </div>
          <div style={{ color: "var(--muted)", marginTop: 2, fontSize: 11, lineHeight: 1.3 }}>
            {summary.community.sampleMembers
              ?.split(",")
              .map((m, i, arr) => (
                <span key={`${m}-${i}`}>
                  <EntityLink type="repo" id={m.trim()} />
                  {i < arr.length - 1 ? ", " : ""}
                </span>
              ))}
          </div>
        </div>
      ) : null}

      {summary.similarEdges.length > 0 ? (
        <div>
          <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Nearest neighbours</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            {summary.similarEdges.slice(0, 8).map((e, i) => {
              const other = e.srcRepo === summary.repoName ? e.dstRepo : e.srcRepo;
              return (
                <div key={`${other}-${i}`} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <EntityLink type="repo" id={other} />
                  <span style={{ color: "var(--muted)", fontSize: 11 }}>
                    jaccard {(e.jaccard * 100).toFixed(1)}% · {e.sharedActors} shared
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      ) : null}

      {summary.assocRules.length > 0 ? (
        <div>
          <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Association rules</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3, fontSize: 11 }}>
            {summary.assocRules.slice(0, 5).map((r, i) => (
              <div key={`${r.antecedent}->${r.consequent}-${i}`}>
                <span style={{ color: "var(--muted-strong)" }}>{r.antecedent}</span>
                {" → "}
                <EntityLink type="repo" id={r.consequent} />
                {" "}
                <PixelChip tone="positive" style={{ marginLeft: 4 }}>
                  lift {r.lift.toFixed(2)}
                </PixelChip>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
