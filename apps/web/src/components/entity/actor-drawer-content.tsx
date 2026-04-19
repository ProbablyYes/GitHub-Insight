"use client";

import { PixelBadge, PixelMiniBar } from "@/components/pixel";

import { EntityLink } from "./EntityLink";

type ActorSummary = {
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
};

const SHARES: Array<{ key: keyof NonNullable<ActorSummary["persona"]>; label: string }> = [
  { key: "pushShare", label: "push_share" },
  { key: "prShare", label: "pr_share" },
  { key: "issuesShare", label: "issues_share" },
  { key: "watchShare", label: "watch_share" },
  { key: "forkShare", label: "fork_share" },
];

const TEMPORAL: Array<{ key: keyof NonNullable<ActorSummary["persona"]>; label: string }> = [
  { key: "nightRatio", label: "night_ratio" },
  { key: "weekendRatio", label: "weekend_ratio" },
];

export function ActorDrawerContent({ summary }: { summary: ActorSummary }) {
  if (!summary || typeof summary !== "object") {
    return <div style={{ color: "var(--muted)", fontSize: 12 }}>No actor data.</div>;
  }
  const p = summary.persona;
  const topRepos = Array.isArray(summary.topRepos) ? summary.topRepos : [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--pixel-space-3)", fontSize: 12 }}>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {p ? (
          <>
            <PixelBadge tone="magenta">persona: {p.personaLabel}</PixelBadge>
            <PixelBadge tone={p.isBot ? "danger" : "info"}>{p.isBot ? "bot" : "human"}</PixelBadge>
            <PixelBadge tone="muted">
              {Number(p.eventCount || 0).toLocaleString()} evts · {p.uniqueRepos ?? 0} repos · {p.activeDays ?? 0}d
            </PixelBadge>
          </>
        ) : (
          <PixelBadge tone="muted">no persona record</PixelBadge>
        )}
      </div>

      {p ? (
        <>
          <div>
            <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Activity mix</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {SHARES.map((f) => {
                const v = Number(p[f.key] || 0);
                return (
                  <PixelMiniBar
                    key={f.key}
                    value={v}
                    min={0}
                    max={1}
                    tone="info"
                    width={130}
                    height={8}
                    label={f.label}
                    valueText={`${(v * 100).toFixed(1)}%`}
                  />
                );
              })}
            </div>
          </div>

          <div>
            <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Temporal bias</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {TEMPORAL.map((f) => {
                const v = Number(p[f.key] || 0);
                return (
                  <PixelMiniBar
                    key={f.key}
                    value={v}
                    min={0}
                    max={1}
                    tone={v > 0.5 ? "change" : "info"}
                    width={130}
                    height={8}
                    label={f.label}
                    valueText={`${(v * 100).toFixed(1)}%`}
                  />
                );
              })}
              {/* hour/repo entropy are log-bit values — normalize to a soft cap of 4 bits */}
              <PixelMiniBar
                value={Math.min(Number(p.hourEntropy || 0), 4)}
                min={0}
                max={4}
                tone="purple"
                width={130}
                height={8}
                label="hour_entropy"
                valueText={(Number(p.hourEntropy || 0)).toFixed(2)}
              />
              <PixelMiniBar
                value={Math.min(Number(p.repoEntropy || 0), 14)}
                min={0}
                max={14}
                tone="purple"
                width={130}
                height={8}
                label="repo_entropy"
                valueText={(Number(p.repoEntropy || 0)).toFixed(2)}
              />
            </div>
          </div>
        </>
      ) : null}

      {topRepos.length > 0 ? (
        <div>
          <div style={{ color: "var(--muted-strong)", marginBottom: 4 }}>Top repos</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            {topRepos.map((r) => (
              <div key={r.repoName} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                <EntityLink type="repo" id={r.repoName} />
                <span style={{ color: "var(--muted)", fontSize: 11 }}>
                  {Number(r.events || 0).toLocaleString()} evts
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <div style={{ color: "var(--muted)", fontSize: 11 }}>
          No per-repo attribution data (actor doesn&apos;t appear in any repo&apos;s top-contributor list).
        </div>
      )}

      <a
        href={`https://github.com/${summary.actorLogin}`}
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
