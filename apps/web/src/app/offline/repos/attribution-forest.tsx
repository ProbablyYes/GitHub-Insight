"use client";

import { useMemo, useState } from "react";

import type { HotVsColdAttributionPoint } from "@/lib/dashboard";

type Props = {
  rows: HotVsColdAttributionPoint[];
};

const FEATURE_DOCS: Record<string, string> = {
  watch_share: "Share of WatchEvent — attention / stargazing intensity.",
  fork_share: "Share of ForkEvent — desire to work from the repo.",
  issues_share: "Share of IssuesEvent — community discussion load.",
  pr_share: "Share of PullRequestEvent — review / incoming-change load.",
  push_share: "Share of PushEvent — raw commit activity.",
  bot_ratio: "Share of events by actors classified as bots.",
  night_ratio: "Share of events at UTC 0–6 — night-shift intensity.",
  weekend_ratio: "Share of events on Sat/Sun.",
  pr_push_ratio: "pr / (pr + push) — review-to-commit balance.",
  active_days_ratio: "Active days / window days — persistence.",
  actors_per_event: "Unique actors per event.",
  event_entropy: "Shannon entropy of event-type shares (base 2).",
  top1_actor_share: "Share held by the single most active actor.",
  log_total_events: "log(1 + total_events).",
  log_payload_p95: "log(1 + payload_p95).",
};

const SCOPE_LABEL: Record<"all" | "humans_only" | "bots_only", string> = {
  all: "All repos",
  humans_only: "Humans only",
  bots_only: "Bot-heavy",
};

export function AttributionForestPlot({ rows }: Props) {
  const [scope, setScope] = useState<"all" | "humans_only" | "bots_only">("all");

  const scopedRows = useMemo(() => rows.filter((r) => r.cohortScope === scope), [rows, scope]);
  const scopesPresent = useMemo(() => {
    const set = new Set(rows.map((r) => r.cohortScope));
    return (["all", "humans_only", "bots_only"] as const).filter((s) => set.has(s));
  }, [rows]);

  // x-axis range — ensure ±0.2 minimum so small effects are visible.
  const maxAbs = Math.max(
    0.4,
    ...scopedRows.map((r) => Math.max(Math.abs(r.cohenD), Math.abs(r.cohensDLow), Math.abs(r.cohensDHigh))),
  );
  const axisMax = Math.ceil(maxAbs * 10) / 10;

  // Sort by |d| desc so the strongest signals are at the top.
  const sorted = [...scopedRows].sort((a, b) => Math.abs(b.cohenD) - Math.abs(a.cohenD));

  const rowHeight = 26;
  const plotHeight = Math.max(sorted.length * rowHeight, rowHeight * 3);
  const leftW = 150; // label column

  const xScale = (d: number) => {
    // -axisMax .. +axisMax → 0 .. 1
    return 0.5 + (d / (axisMax * 2));
  };

  return (
    <div>
      <div
        style={{
          display: "flex",
          gap: 6,
          marginBottom: 10,
          flexWrap: "wrap",
          alignItems: "center",
        }}
      >
        <span style={{ color: "var(--muted)", fontSize: 11 }}>Cohort scope:</span>
        {scopesPresent.map((s) => (
          <button
            key={s}
            type="button"
            onClick={() => setScope(s)}
            className="nes-btn"
            style={{
              padding: "2px 10px",
              fontSize: 11,
              background: scope === s ? "var(--accent-info)" : undefined,
              color: scope === s ? "#0a0a0a" : undefined,
            }}
          >
            {SCOPE_LABEL[s]}
          </button>
        ))}
        <span style={{ color: "var(--muted-strong)", fontSize: 11, marginLeft: 10 }}>
          n = {sorted[0]?.nHot ?? 0} hot · {sorted[0]?.nCold ?? 0} cold
        </span>
      </div>

      <div style={{ position: "relative", width: "100%", overflowX: "auto" }}>
        <div style={{ minWidth: 620, position: "relative" }}>
          {/* Axis */}
          <div
            style={{
              position: "relative",
              height: 18,
              marginLeft: leftW,
              marginRight: 40,
              borderBottom: "1px solid #333",
              color: "var(--muted)",
              fontSize: 10,
            }}
          >
            <span style={{ position: "absolute", left: "0%" }}>-{axisMax.toFixed(1)}</span>
            <span style={{ position: "absolute", left: "25%" }}>-{(axisMax / 2).toFixed(1)}</span>
            <span style={{ position: "absolute", left: "50%", transform: "translateX(-50%)" }}>
              0 · cold — hot →
            </span>
            <span style={{ position: "absolute", left: "75%" }}>+{(axisMax / 2).toFixed(1)}</span>
            <span style={{ position: "absolute", right: 0 }}>+{axisMax.toFixed(1)}</span>
          </div>

          <div style={{ position: "relative", height: plotHeight }}>
            {/* Vertical zero line */}
            <div
              style={{
                position: "absolute",
                top: 0,
                bottom: 0,
                left: `calc(${leftW}px + (100% - ${leftW + 40}px) * 0.5)`,
                borderLeft: "1px dashed #555",
              }}
            />
            {/* Large-effect bands */}
            {[-0.8, -0.5, 0.5, 0.8].map((d) => {
              if (Math.abs(d) > axisMax) return null;
              const pct = xScale(d) * 100;
              const tone =
                Math.abs(d) >= 0.8 ? "#33ff57" : "#ffcc00";
              return (
                <div
                  key={`band-${d}`}
                  style={{
                    position: "absolute",
                    top: 0,
                    bottom: 0,
                    left: `calc(${leftW}px + (100% - ${leftW + 40}px) * ${pct / 100})`,
                    borderLeft: `1px dotted ${tone}33`,
                  }}
                />
              );
            })}

            {sorted.map((row, idx) => {
              const y = idx * rowHeight + 4;
              const xEst = xScale(row.cohenD) * 100;
              const xLo = xScale(Math.max(row.cohensDLow, -axisMax)) * 100;
              const xHi = xScale(Math.min(row.cohensDHigh, axisMax)) * 100;
              const tone =
                Math.abs(row.cohenD) >= 0.8
                  ? "#33ff57"
                  : Math.abs(row.cohenD) >= 0.5
                  ? "#ffcc00"
                  : "#aaaaaa";
              const ciCrossesZero = row.cohensDLow <= 0 && row.cohensDHigh >= 0;
              return (
                <div
                  key={`${row.cohortScope}-${row.featureName}-${idx}`}
                  style={{
                    position: "absolute",
                    top: y,
                    left: 0,
                    right: 0,
                    height: rowHeight,
                    display: "flex",
                    alignItems: "center",
                  }}
                  title={FEATURE_DOCS[row.featureName] ?? row.featureName}
                >
                  <div
                    style={{
                      width: leftW,
                      paddingRight: 10,
                      color: "var(--fg)",
                      fontSize: 11,
                      textAlign: "right",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {row.featureName}
                  </div>
                  <div style={{ position: "relative", flex: 1, height: rowHeight, marginRight: 40 }}>
                    {/* CI bar */}
                    <div
                      style={{
                        position: "absolute",
                        top: rowHeight / 2 - 2,
                        left: `${xLo}%`,
                        width: `${Math.max(xHi - xLo, 0.5)}%`,
                        height: 4,
                        background: ciCrossesZero ? "#555" : tone,
                        opacity: 0.5,
                      }}
                    />
                    {/* Point estimate */}
                    <div
                      style={{
                        position: "absolute",
                        top: rowHeight / 2 - 5,
                        left: `calc(${xEst}% - 5px)`,
                        width: 10,
                        height: 10,
                        background: tone,
                        border: "1px solid #000",
                      }}
                    />
                    {/* Numeric label */}
                    <div
                      style={{
                        position: "absolute",
                        right: -38,
                        top: rowHeight / 2 - 7,
                        width: 36,
                        textAlign: "right",
                        color: tone,
                        fontSize: 10,
                      }}
                    >
                      {row.cohenD >= 0 ? "+" : ""}
                      {row.cohenD.toFixed(2)}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <div
        style={{
          marginTop: 10,
          color: "var(--muted-strong)",
          fontSize: 11,
          lineHeight: 1.5,
        }}
      >
        <span style={{ color: "#33ff57" }}>■</span> large effect (|d| ≥ 0.8){"  "}
        <span style={{ color: "#ffcc00", marginLeft: 14 }}>■</span> medium (|d| ≥ 0.5){"  "}
        <span style={{ color: "#aaa", marginLeft: 14 }}>■</span> small
        <span style={{ marginLeft: 14 }}>·</span> CI bar that covers 0 is greyed-out (non-significant).
      </div>
    </div>
  );
}
