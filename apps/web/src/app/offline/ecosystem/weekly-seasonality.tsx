"use client";

import { useMemo } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ErrorBar,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type DailyRow = {
  metricDate: string;
  totalEvents: number;
};

type Props = {
  rows: DailyRow[];
  days?: number;
};

const DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const WEEKEND_FILL = "#ff8c42";
const WEEKDAY_FILL = "#33ccff";

function dayOfWeekMon0(isoDate: string): number {
  // Construct UTC date to avoid local TZ drifting across month borders.
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return -1;
  // getUTCDay(): Sun=0, Mon=1, ..., Sat=6 → shift to Mon=0..Sun=6
  const js = d.getUTCDay();
  return js === 0 ? 6 : js - 1;
}

export function WeeklySeasonalityChart({ rows, days = 30 }: Props) {
  const data = useMemo(() => {
    const window = rows.slice(-days);
    const buckets: { total: number[]; }[] = Array.from({ length: 7 }, () => ({ total: [] }));
    for (const row of window) {
      const idx = dayOfWeekMon0(row.metricDate);
      if (idx < 0) continue;
      buckets[idx].total.push(row.totalEvents);
    }
    return buckets.map((b, i) => {
      if (b.total.length === 0) {
        return {
          label: DOW_LABELS[i],
          mean: 0,
          min: 0,
          max: 0,
          n: 0,
          err: [0, 0] as [number, number],
          isWeekend: i >= 5,
        };
      }
      const mean = b.total.reduce((s, v) => s + v, 0) / b.total.length;
      const min = Math.min(...b.total);
      const max = Math.max(...b.total);
      return {
        label: DOW_LABELS[i],
        mean: Math.round(mean),
        min,
        max,
        n: b.total.length,
        err: [mean - min, max - mean] as [number, number],
        isWeekend: i >= 5,
      };
    });
  }, [rows, days]);

  const hasData = data.some((d) => d.n > 0);
  if (!hasData) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center", fontSize: 12 }}>
        Not enough daily rows for weekly seasonality.
      </p>
    );
  }

  return (
    <div style={{ width: "100%", height: 240 }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
          <CartesianGrid stroke="var(--divider)" strokeDasharray="2 2" />
          <XAxis dataKey="label" stroke="var(--muted)" tick={{ fontSize: 11 }} />
          <YAxis stroke="var(--muted)" tick={{ fontSize: 10 }} width={48} />
          <Tooltip
            contentStyle={{
              background: "var(--bg)",
              border: "1px solid var(--divider)",
              fontSize: 11,
              color: "var(--fg)",
            }}
            formatter={(value, name) => {
              const n = String(name);
              if (n === "mean") return [Number(value ?? 0).toLocaleString(), "mean"];
              return [String(value ?? ""), n];
            }}
            labelFormatter={(label, payload) => {
              const entry = payload?.[0]?.payload as
                | { label: string; min: number; max: number; n: number }
                | undefined;
              if (!entry) return label;
              return `${entry.label} — min ${entry.min.toLocaleString()} · max ${entry.max.toLocaleString()} · n=${entry.n}`;
            }}
            cursor={{ fill: "rgba(255,255,255,0.05)" }}
          />
          <Bar dataKey="mean" radius={0}>
            {data.map((entry, i) => (
              <Cell key={i} fill={entry.isWeekend ? WEEKEND_FILL : WEEKDAY_FILL} />
            ))}
            <ErrorBar dataKey="err" width={5} stroke="var(--muted)" strokeWidth={1} direction="y" />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <p style={{ fontSize: 10, color: "var(--muted)", marginTop: 4 }}>
        blue = weekday · orange = weekend. Whisker = min/max across the 30-day window (typically 4–5 samples per weekday).
      </p>
    </div>
  );
}
