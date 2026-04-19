"use client";

import { useMemo } from "react";

export type LayerEdge = {
  layer: string;
  srcRepo: string;
  dstRepo: string;
  jaccard: number;
  sharedActors: number;
};

export type LayerCommunity = {
  layer: string;
  repoName: string;
  communityId: string;
  communitySize: number;
  rankScore: number;
};

type LayerStat = {
  layer: string;
  edges: number;
  repos: number;
  communities: number;
  avgJaccard: number;
  maxJaccard: number;
  biggestCommunity: number;
};

const LAYER_ORDER = ["watch", "fork", "pr", "push", "issue"];
const LAYER_LABEL: Record<string, string> = {
  watch: "WatchEvent",
  fork: "ForkEvent",
  pr: "PullRequest*",
  push: "PushEvent",
  issue: "Issue*",
};
const LAYER_COLOR: Record<string, string> = {
  watch: "var(--accent-magenta)",
  fork: "var(--accent-info)",
  pr: "var(--accent-positive)",
  push: "var(--accent-change)",
  issue: "var(--accent-purple)",
};

export function LayerMatrix({
  edges,
  communities,
}: {
  edges: LayerEdge[];
  communities: LayerCommunity[];
}) {
  const stats: LayerStat[] = useMemo(() => {
    const byLayer: Record<string, LayerEdge[]> = {};
    for (const e of edges) (byLayer[e.layer] ||= []).push(e);
    const commByLayer: Record<string, LayerCommunity[]> = {};
    for (const c of communities) (commByLayer[c.layer] ||= []).push(c);

    const present = new Set<string>([...Object.keys(byLayer), ...Object.keys(commByLayer)]);
    const ordered = LAYER_ORDER.filter((l) => present.has(l)).concat(
      [...present].filter((l) => !LAYER_ORDER.includes(l)),
    );

    return ordered.map((layer) => {
      const es = byLayer[layer] ?? [];
      const cs = commByLayer[layer] ?? [];
      const commSizes = new Map<string, number>();
      for (const c of cs) commSizes.set(c.communityId, (commSizes.get(c.communityId) ?? 0) + 1);
      const nonTrivial = [...commSizes.values()].filter((s) => s >= 2);
      const repos = new Set([...es.flatMap((e) => [e.srcRepo, e.dstRepo]), ...cs.map((c) => c.repoName)]);
      return {
        layer,
        edges: es.length,
        repos: repos.size,
        communities: nonTrivial.length,
        avgJaccard: es.length ? es.reduce((a, b) => a + b.jaccard, 0) / es.length : 0,
        maxJaccard: es.length ? Math.max(...es.map((e) => e.jaccard)) : 0,
        biggestCommunity: nonTrivial.length ? Math.max(...nonTrivial) : 0,
      };
    });
  }, [edges, communities]);

  if (!stats.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No layer data.</div>;
  }

  const maxEdges = Math.max(...stats.map((s) => s.edges), 1);
  const maxRepos = Math.max(...stats.map((s) => s.repos), 1);
  // sqrt scaling keeps tiny layers (e.g. push=1) visible next to big layers (watch=53)
  const barWidth = (v: number) =>
    maxEdges <= 1 ? 100 : Math.max(v > 0 ? 6 : 0, (Math.sqrt(v) / Math.sqrt(maxEdges)) * 100);

  return (
    <div style={{ overflowX: "auto" }}>
      <table suppressHydrationWarning style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
        <thead>
          <tr style={{ color: "var(--muted)", textAlign: "left" }}>
            <th style={{ padding: "6px 4px" }}>layer</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>edges</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>repos</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>communities &ge;2</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>biggest</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>avg jac</th>
            <th style={{ padding: "6px 4px", textAlign: "right" }}>max jac</th>
            <th style={{ padding: "6px 4px", textAlign: "left" }}>edges (&radic; scaled)</th>
          </tr>
        </thead>
        <tbody>
          {stats.map((s) => (
            <tr key={s.layer} style={{ borderTop: "1px dashed var(--pixel-border)" }}>
              <td style={{ padding: "6px 4px", color: LAYER_COLOR[s.layer] ?? "var(--fg)" }}>
                {LAYER_LABEL[s.layer] ?? s.layer}
              </td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.edges}</td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.repos}</td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.communities}</td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.biggestCommunity || "-"}</td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.avgJaccard.toFixed(3)}</td>
              <td style={{ padding: "6px 4px", textAlign: "right" }}>{s.maxJaccard.toFixed(3)}</td>
              <td style={{ padding: "6px 4px" }}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                    minWidth: 170,
                  }}
                  title={`${s.edges} edges (${((s.edges / maxEdges) * 100).toFixed(0)}% of max)`}
                >
                  <div
                    style={{
                      width: `${barWidth(s.edges)}%`,
                      height: 8,
                      background: LAYER_COLOR[s.layer] ?? "var(--fg)",
                      maxWidth: 160,
                      minWidth: s.edges > 0 ? 4 : 0,
                      borderRadius: 1,
                    }}
                  />
                  <span style={{ color: "var(--muted)", fontSize: 10, whiteSpace: "nowrap" }}>
                    {s.repos}/{maxRepos} repos
                  </span>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ marginTop: 8, fontSize: 10, color: "var(--muted)", lineHeight: 1.4 }}>
        <strong style={{ color: "var(--muted-strong)" }}>Reading the table:</strong>{" "}
        <em>edges</em> = Jaccard pairs in that single-event subgraph, <em>avg&nbsp;jac</em> =
        mean overlap of actor sets for connected pairs, <em>biggest</em> = largest LPA
        community found inside that layer. A layer with many <em>edges</em> but tiny{" "}
        <em>avg&nbsp;jac</em> is &ldquo;broad + shallow&rdquo; (many weak ties), while
        few-edge-but-high-jac layers are &ldquo;tight teams.&rdquo; The bar is &radic;-scaled so
        a layer with 1 edge next to a layer with 50 is still visible.
      </div>
    </div>
  );
}

