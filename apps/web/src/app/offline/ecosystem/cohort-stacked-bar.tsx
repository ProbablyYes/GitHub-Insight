"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type CohortRow = {
  metricDate: string;
  cohort: string;
  actors: number;
};

type Props = {
  rows: CohortRow[];
  days?: number;
};

const COLORS = {
  new: "#7cc57c",
  returning: "#33ccff",
  reactivated: "#ffcc00",
} as const;

export function CohortStackedBar({ rows, days = 30 }: Props) {
  const dates = Array.from(new Set(rows.map((it) => it.metricDate))).sort().slice(-days);
  const byDate = new Map<string, CohortRow[]>();
  for (const r of rows) {
    const bucket = byDate.get(r.metricDate) ?? [];
    bucket.push(r);
    byDate.set(r.metricDate, bucket);
  }
  const data = dates.map((d) => {
    const bucket = byDate.get(d) ?? [];
    const pick = (key: string) =>
      bucket.find((r) => r.cohort === key)?.actors ?? 0;
    return {
      label: d.slice(5),
      new: pick("new"),
      returning: pick("returning"),
      reactivated: pick("reactivated"),
    };
  });

  if (data.length === 0) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center", fontSize: 12 }}>
        No cohort data for the selected window.
      </p>
    );
  }

  // On a 30-day view, 30 tick labels are illegible. Thin them out to every 3rd day.
  const manyDays = data.length > 14;
  const tickInterval = manyDays ? 2 : 0;

  return (
    <div style={{ width: "100%", height: 260 }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
          <CartesianGrid stroke="var(--divider)" strokeDasharray="2 2" />
          <XAxis
            dataKey="label"
            stroke="var(--muted)"
            tick={{ fontSize: 10 }}
            interval={tickInterval}
          />
          <YAxis stroke="var(--muted)" tick={{ fontSize: 10 }} width={44} />
          <Tooltip
            contentStyle={{
              background: "var(--bg)",
              border: "1px solid var(--divider)",
              fontSize: 11,
              color: "var(--fg)",
            }}
            cursor={{ fill: "rgba(255,255,255,0.05)" }}
          />
          <Legend
            verticalAlign="bottom"
            height={24}
            wrapperStyle={{ fontSize: 10, color: "var(--muted-strong)" }}
          />
          <Bar dataKey="new" name="new (first time)" stackId="a" fill={COLORS.new} />
          <Bar
            dataKey="returning"
            name="returning (gap < 3d)"
            stackId="a"
            fill={COLORS.returning}
          />
          <Bar
            dataKey="reactivated"
            name="reactivated (gap ≥ 3d)"
            stackId="a"
            fill={COLORS.reactivated}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
