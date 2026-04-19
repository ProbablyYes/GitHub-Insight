"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type DailyPoint = {
  label: string;
  metricDate: string;
  value: number;
  ma7?: number | null;
  gini?: number | null;
};

type Flag = {
  metricDate: string;
  kind: "burst" | "drop";
  zScore: number;
  cusum: number;
  contributionTopType: string;
};

function toneFor(kind: "burst" | "drop"): string {
  return kind === "burst" ? "#33ff57" : "#ff5566";
}

type WeekendBand = {
  x1: string;
  x2: string;
};

function buildWeekendBands(data: DailyPoint[]): WeekendBand[] {
  // Detect Sat+Sun pairs in the data and render a single band per weekend.
  // We anchor on label values because the XAxis is categorical.
  const isWeekendUtc = (iso: string): number => {
    const t = new Date(`${iso}T00:00:00Z`);
    if (Number.isNaN(t.getTime())) return -1;
    const js = t.getUTCDay();
    return js === 0 ? 7 : js === 6 ? 6 : 0;
  };
  const bands: WeekendBand[] = [];
  for (let i = 0; i < data.length; i++) {
    const w = isWeekendUtc(data[i].metricDate);
    if (w === 6) {
      const sat = data[i];
      const sun = data[i + 1] && isWeekendUtc(data[i + 1].metricDate) === 7 ? data[i + 1] : sat;
      bands.push({ x1: sat.label, x2: sun.label });
    } else if (w === 7 && i === 0) {
      bands.push({ x1: data[i].label, x2: data[i].label });
    }
  }
  return bands;
}

export function EcosystemTrendWithFlags({
  data,
  flags,
}: {
  data: DailyPoint[];
  flags: Flag[];
}) {
  const labelByDate = new Map(data.map((d) => [d.metricDate, d.label]));
  const valueByDate = new Map(data.map((d) => [d.metricDate, d.value]));
  const weekendBands = buildWeekendBands(data);

  return (
    <div style={{ width: "100%", height: 320, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 12, left: -6, bottom: 0 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.06)" />
          <XAxis
            dataKey="label"
            tick={{ fill: "#9aa", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
            minTickGap={24}
          />
          <YAxis
            yAxisId="left"
            tick={{ fill: "#9aa", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            domain={[0, 1]}
            tick={{ fill: "#9aa", fontSize: 11, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              borderRadius: 0,
              border: "2px solid #333",
              background: "#111",
              color: "#d4d4d4",
              fontFamily: '"Zpix", monospace',
              fontSize: 12,
            }}
            formatter={(value, name) => {
              const v = typeof value === "number" ? value : Number(value ?? 0);
              const n = typeof name === "string" ? name : String(name ?? "");
              if (n === "Gini") return [v.toFixed(3), n];
              return [v.toLocaleString(), n];
            }}
          />
          <Legend wrapperStyle={{ fontSize: 11, color: "#9aa" }} />
          {weekendBands.map((b, i) => (
            <ReferenceArea
              key={`wknd-${i}`}
              yAxisId="left"
              x1={b.x1}
              x2={b.x2}
              strokeOpacity={0}
              fill="rgba(255,255,255,0.04)"
              ifOverflow="extendDomain"
            />
          ))}
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="value"
            name="Events"
            stroke="#33ccff"
            strokeWidth={2}
            dot={false}
          />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="ma7"
            name="MA7"
            stroke="#ffcc00"
            strokeWidth={2}
            dot={false}
            strokeDasharray="5 4"
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="gini"
            name="Gini"
            stroke="#a07bff"
            strokeWidth={1.5}
            dot={false}
          />
          {flags.map((f) => {
            const label = labelByDate.get(f.metricDate);
            const y = valueByDate.get(f.metricDate);
            if (label == null || y == null) return null;
            return (
              <ReferenceDot
                key={`${f.metricDate}-${f.kind}`}
                yAxisId="left"
                x={label}
                y={y}
                r={6}
                fill={toneFor(f.kind)}
                stroke="#0a0a0a"
                strokeWidth={2}
                label={{
                  value: f.kind === "burst" ? "▲" : "▼",
                  position: "top",
                  fill: toneFor(f.kind),
                  fontSize: 12,
                  fontFamily: '"Zpix", monospace',
                }}
              />
            );
          })}
        </LineChart>
      </ResponsiveContainer>
      {flags.length > 0 ? (
        <div style={{ marginTop: 8, color: "var(--muted)", fontSize: 11, fontFamily: '"Zpix", monospace' }}>
          {flags.map((f) => (
            <span key={`${f.metricDate}-${f.kind}-lbl`} style={{ marginRight: 14 }}>
              <span style={{ color: toneFor(f.kind) }}>
                {f.kind === "burst" ? "▲ burst" : "▼ drop"}
              </span>{" "}
              {f.metricDate} · z={f.zScore.toFixed(2)} · top: {f.contributionTopType || "n/a"}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}
