"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type ShiftRow = {
  metricDate: string;
  eventType: string;
  share: number;
  shareShift: number;
};

type Props = {
  rows: ShiftRow[];
};

/**
 * Per-event-type real-world driver. Used to translate a bar ("+1.4 pp on
 * WatchEvent") into a human-readable explanation ("more people stargazed
 * this day — often a promo / Show HN / newsletter mention effect").
 */
const DRIVER: Record<
  string,
  { label: string; up: string; down: string }
> = {
  PushEvent: {
    label: "code pushes",
    up: "a heavier coding day — more commits landed than the day before",
    down: "fewer commits than yesterday — typical of weekends or holidays",
  },
  WatchEvent: {
    label: "stars / watches",
    up: "a promotional bump — something got amplified on HN / X / newsletters",
    down: "the promo wave cooled off — star-gazing dropped back to baseline",
  },
  ForkEvent: {
    label: "forks",
    up: "more copies being made — often a tutorial / course cohort spike",
    down: "fork activity eased",
  },
  PullRequestEvent: {
    label: "pull requests",
    up: "more PRs opened / closed — a heavy merge-review day",
    down: "PR throughput dipped",
  },
  PullRequestReviewEvent: {
    label: "PR reviews",
    up: "more code review happening",
    down: "review activity dropped",
  },
  PullRequestReviewCommentEvent: {
    label: "inline review comments",
    up: "deeper review discussion — people debating diffs line-by-line",
    down: "review chatter slowed down",
  },
  IssuesEvent: {
    label: "issue activity",
    up: "bug / feature-request discussions picked up",
    down: "issue activity quieted down",
  },
  IssueCommentEvent: {
    label: "issue comments",
    up: "long threads — people arguing on existing issues",
    down: "issue threads calmed down",
  },
  CreateEvent: {
    label: "creations (branches / tags / repos)",
    up: "more new branches / tags / repos — often release-cutting days",
    down: "fewer new branches or repos",
  },
  DeleteEvent: {
    label: "deletions",
    up: "more cleanup — stale branches being removed",
    down: "cleanup activity eased",
  },
  ReleaseEvent: {
    label: "releases",
    up: "release day — more tags published",
    down: "no major release activity",
  },
  MemberEvent: {
    label: "team changes",
    up: "more collaborator adds / removes",
    down: "team membership stable",
  },
  PublicEvent: {
    label: "repos going public",
    up: "more private→public flips",
    down: "fewer flips to public",
  },
  GollumEvent: {
    label: "wiki edits",
    up: "wiki / docs getting refreshed",
    down: "wiki activity dropped",
  },
  CommitCommentEvent: {
    label: "commit comments",
    up: "more comments on specific commits",
    down: "commit-thread activity dropped",
  },
};

function weekIndex(firstDateIso: string, thisDateIso: string): number {
  const a = new Date(`${firstDateIso}T00:00:00Z`).getTime();
  const b = new Date(`${thisDateIso}T00:00:00Z`).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  const diffDays = Math.floor((b - a) / 86400000);
  return Math.floor(diffDays / 7);
}

function weekdayLabel(iso: string): string {
  const d = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return "";
  const names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  return names[d.getUTCDay()] ?? "";
}

function prettyPP(v: number): string {
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  return `${sign}${Math.abs(v).toFixed(2)} pp`;
}

export function EventShiftExplorer({ rows }: Props) {
  const { dates, mostInformativeDate, maxAbsShiftByDate } = useMemo(() => {
    const byDate = new Map<string, number>();
    rows.forEach((r) => {
      byDate.set(
        r.metricDate,
        Math.max(byDate.get(r.metricDate) ?? 0, Math.abs(r.shareShift))
      );
    });
    const ds = [...byDate.keys()].sort();
    const best = [...byDate.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
    return { dates: ds, mostInformativeDate: best, maxAbsShiftByDate: byDate };
  }, [rows]);

  const [selected, setSelected] = useState<string | null>(mostInformativeDate);

  const weekGroups = useMemo(() => {
    if (dates.length === 0) return [] as Array<{ week: number; days: string[] }>;
    const first = dates[0];
    const byWeek = new Map<number, string[]>();
    for (const d of dates) {
      const wi = weekIndex(first, d);
      const list = byWeek.get(wi) ?? [];
      list.push(d);
      byWeek.set(wi, list);
    }
    return [...byWeek.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([week, days]) => ({ week, days }));
  }, [dates]);

  const chartData = useMemo(() => {
    if (!selected) return [];
    return rows
      .filter((r) => r.metricDate === selected)
      .sort((a, b) => Math.abs(b.shareShift) - Math.abs(a.shareShift))
      .slice(0, 10)
      .map((r) => ({
        label: r.eventType,
        value: Number((r.shareShift * 100).toFixed(3)),
        share: Number((r.share * 100).toFixed(2)),
      }));
  }, [rows, selected]);

  /** Plain-English interpretation of the currently selected day. */
  const interpretation = useMemo(() => {
    if (!selected || chartData.length === 0) return null;
    const winner = chartData[0]!; // largest |shift|, regardless of sign
    const topPositive = chartData.find((d) => d.value > 0);
    const topNegative = [...chartData].reverse().find((d) => d.value < 0);
    const dow = weekdayLabel(selected);
    const isWeekend = dow === "Sat" || dow === "Sun";
    const absMax = Math.abs(winner.value);

    let magnitude: string;
    if (absMax < 0.5) magnitude = "barely budged";
    else if (absMax < 1.5) magnitude = "a mild reshuffle";
    else if (absMax < 3) magnitude = "a clear shift";
    else if (absMax < 8) magnitude = "a big compositional swing";
    else magnitude = "an outlier-scale swing (prior day was probably a holiday / outage / low-volume day)";

    return {
      dow,
      isWeekend,
      magnitude,
      winner,
      topPositive,
      topNegative,
    };
  }, [selected, chartData]);

  if (dates.length === 0) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center", fontSize: 12 }}>
        No shift data available.
      </p>
    );
  }

  return (
    <div>
      {/* —————— date picker, one row per week —————— */}
      <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 12 }}>
        {weekGroups.map(({ week, days }) => (
          <div
            key={week}
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 6,
              alignItems: "center",
              fontSize: 11,
            }}
          >
            <span
              style={{
                color: "var(--muted)",
                minWidth: 48,
                fontVariantNumeric: "tabular-nums",
              }}
            >
              W{week + 1}
            </span>
            {days.map((d) => {
              const isSelected = d === selected;
              const isBest = d === mostInformativeDate;
              const absShift = maxAbsShiftByDate.get(d) ?? 0;
              return (
                <button
                  key={d}
                  onClick={() => setSelected(d)}
                  style={{
                    padding: "2px 8px",
                    background: isSelected ? "var(--accent-change)" : "transparent",
                    color: isSelected ? "#111" : "var(--fg)",
                    border: `1px solid ${isSelected ? "var(--accent-change)" : "var(--divider)"}`,
                    fontSize: 10,
                    cursor: "pointer",
                    fontFamily: "inherit",
                  }}
                  title={`${weekdayLabel(d)} · max |shift| = ${(absShift * 100).toFixed(2)} pp${
                    isBest ? " · largest in month" : ""
                  }`}
                >
                  {d.slice(5)}
                  {isBest ? "★" : ""}
                </button>
              );
            })}
          </div>
        ))}
      </div>

      {/* —————— selected-day header —————— */}
      {selected ? (
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            alignItems: "baseline",
            gap: 12,
            marginBottom: 8,
            fontSize: 12,
            color: "var(--muted-strong)",
          }}
        >
          <span style={{ color: "var(--accent-change)", fontWeight: 700 }}>
            {selected}
          </span>
          <span style={{ color: "var(--muted)" }}>({weekdayLabel(selected)})</span>
          <span style={{ color: "var(--muted)", marginLeft: "auto", fontSize: 11 }}>
            bars show top-10 event types by |Δshare| on this date
          </span>
        </div>
      ) : null}

      {/* —————— the wide bar chart —————— */}
      <div style={{ width: "100%", height: 340 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} margin={{ top: 20, right: 20, bottom: 50, left: 12 }}>
            <CartesianGrid stroke="var(--divider)" strokeDasharray="2 2" />
            <XAxis
              dataKey="label"
              stroke="var(--muted)"
              tick={{ fontSize: 10 }}
              angle={-20}
              height={60}
              interval={0}
            />
            <YAxis
              stroke="var(--muted)"
              tick={{ fontSize: 10 }}
              width={52}
              label={{
                value: "Δshare (pp)",
                angle: -90,
                position: "insideLeft",
                fill: "var(--muted)",
                fontSize: 10,
              }}
            />
            <ReferenceLine y={0} stroke="var(--muted)" strokeWidth={1} />
            <Tooltip
              contentStyle={{
                background: "var(--bg)",
                border: "1px solid var(--divider)",
                fontSize: 11,
                color: "var(--fg)",
              }}
              formatter={(_v, _n, item) => {
                const p = (item as { payload?: { value: number; share: number } }).payload;
                if (!p) return ["", ""];
                return [
                  `${prettyPP(p.value)} (now ${p.share.toFixed(1)}% of day)`,
                  "share shift",
                ];
              }}
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
            />
            <Bar dataKey="value" radius={0}>
              {chartData.map((entry, i) => (
                <Cell
                  key={i}
                  fill={entry.value >= 0 ? "var(--accent-positive)" : "var(--accent-danger)"}
                />
              ))}
              <LabelList
                dataKey="value"
                position="top"
                formatter={(v: unknown) => {
                  const num = Number(v);
                  if (!Number.isFinite(num) || Math.abs(num) < 0.3) return "";
                  return prettyPP(num);
                }}
                style={{ fontSize: 10, fill: "var(--muted-strong)" }}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* —————— live interpretation for the selected day —————— */}
      {interpretation ? (
        <div
          style={{
            marginTop: 10,
            padding: "10px 12px",
            borderLeft: "3px solid var(--accent-change)",
            background: "var(--surface-elevated, rgba(255,255,255,0.02))",
            fontSize: 11,
            lineHeight: 1.6,
            color: "var(--fg)",
          }}
        >
          <div
            style={{
              color: "var(--accent-change)",
              fontSize: 10,
              letterSpacing: 2,
              fontWeight: 700,
              marginBottom: 4,
            }}
          >
            {`==> READING ${selected?.slice(5) ?? ""} (${interpretation.dow.toUpperCase()})`}
          </div>
          <div>
            The biggest move was{" "}
            <strong style={{ color: interpretation.winner.value >= 0 ? "var(--accent-positive)" : "var(--accent-danger)" }}>
              {prettyPP(interpretation.winner.value)}
            </strong>{" "}
            on <code style={{ color: "var(--accent-info)" }}>{interpretation.winner.label}</code> — {interpretation.magnitude}.{" "}
            {(() => {
              const drv = DRIVER[interpretation.winner.label];
              if (!drv) return null;
              const txt = interpretation.winner.value >= 0 ? drv.up : drv.down;
              return <span>In plain terms: <em>{txt}</em>.</span>;
            })()}
          </div>
          <div style={{ marginTop: 4 }}>
            {interpretation.topPositive && interpretation.topPositive.label !== interpretation.winner.label ? (
              <span>
                Also rising:{" "}
                <code style={{ color: "var(--accent-positive)" }}>{interpretation.topPositive.label}</code>{" "}
                ({prettyPP(interpretation.topPositive.value)}).{" "}
              </span>
            ) : null}
            {interpretation.topNegative ? (
              <span>
                Giving up share:{" "}
                <code style={{ color: "var(--accent-danger)" }}>{interpretation.topNegative.label}</code>{" "}
                ({prettyPP(interpretation.topNegative.value)}).
              </span>
            ) : null}
          </div>
          {interpretation.isWeekend ? (
            <div style={{ marginTop: 4, color: "var(--muted)" }}>
              Weekend context: the weekday calendar naturally suppresses <code>PushEvent</code> / review events
              and inflates <code>WatchEvent</code> / <code>ForkEvent</code> as leisure reading. A shift that
              would be surprising on a Wednesday may just be the weekly rhythm here.
            </div>
          ) : null}
          <div style={{ marginTop: 6, color: "var(--muted)" }}>
            Scale check: every day&apos;s Δshare values sum to ~0 (one event type can only gain share if
            another loses it). A ±1–3 pp move is the typical weekday-to-weekday churn; ±5 pp or more
            usually means a release day, a viral promo, or an outage. Jan 1 → Jan 2 in this window sits at
            the extreme end of the scale precisely because Jan 1 was a near-empty holiday — it&apos;s a
            data-side artefact, not a real &ldquo;trend reversal.&rdquo;
          </div>
        </div>
      ) : null}

      <p style={{ fontSize: 10, color: "var(--muted)", marginTop: 6 }}>
        ★ = day with largest |shift| across the full 30-day window. W1…W5 = calendar weeks from the start of
        the window. Green = the event type <em>gained</em> share vs the prior day, red = <em>lost</em> share.
      </p>
    </div>
  );
}
