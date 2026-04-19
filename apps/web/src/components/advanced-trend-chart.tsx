"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

type TrendPoint = {
  label: string;
  actual: number;
  ma7: number;
  forecast: number;
};

export function AdvancedTrendChart({ data }: { data: TrendPoint[] }) {
  return (
    <div style={{ width: "100%", height: 300, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 12, left: -8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.06)" />
          <XAxis
            dataKey="label"
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
            minTickGap={24}
          />
          <YAxis
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
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
              fontSize: 13,
            }}
          />
          <Legend />
          <Line type="monotone" dataKey="actual" name="Actual" stroke="#33ccff" strokeWidth={2} dot={false} />
          <Line type="monotone" dataKey="ma7" name="MA7" stroke="#ffcc00" strokeWidth={2} dot={false} />
          <Line
            type="monotone"
            dataKey="forecast"
            name="Forecast"
            stroke="#33ff57"
            strokeWidth={2}
            dot={false}
            strokeDasharray="6 4"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

type BurstPoint = {
  repoName: string;
  burstIndex: number;
  stabilityIndex: number;
  rankScore: number;
  quadrant: string;
};

const QUADRANT_COLORS: Record<string, string> = {
  short_spike: "#ff5252",
  rising_core: "#ffb020",
  steady_core: "#33ccff",
  long_tail: "#888888",
};

const QUADRANT_ORDER = ["short_spike", "rising_core", "steady_core", "long_tail"];

export function BurstScatterChart({ data }: { data: BurstPoint[] }) {
  const chartData = data.map((item) => ({
    x: item.stabilityIndex,
    y: item.burstIndex,
    z: Math.max(item.rankScore * 100, 20),
    repoName: item.repoName,
    quadrant: item.quadrant,
  }));

  const grouped = QUADRANT_ORDER.map((q) => ({
    quadrant: q,
    points: chartData.filter((d) => d.quadrant === q),
  })).filter((g) => g.points.length > 0);

  return (
    <div style={{ width: "100%", height: 340, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 10, right: 18, left: -8, bottom: 30 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.08)" />
          <XAxis
            type="number"
            dataKey="x"
            name="stability"
            domain={[0, 1]}
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
            label={{ value: "stability_index", position: "insideBottom", offset: -8, fill: "#888", fontSize: 11 }}
          />
          <YAxis
            type="number"
            dataKey="y"
            name="burst"
            domain={[0, "dataMax + 1"]}
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
            label={{ value: "burst_index", angle: -90, position: "insideLeft", offset: 18, fill: "#888", fontSize: 11 }}
          />
          <ZAxis type="number" dataKey="z" range={[40, 320]} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            formatter={(value, name) => {
              const numericValue = typeof value === "number" ? value : Number(value ?? 0);
              const label = typeof name === "string" ? name : String(name ?? "value");
              return [numericValue.toFixed(3), label];
            }}
            labelFormatter={(_, payload) => {
              const row = payload?.[0]?.payload as { repoName?: string; quadrant?: string } | undefined;
              return row ? `${row.repoName} (${row.quadrant})` : "repo";
            }}
            contentStyle={{
              borderRadius: 0,
              border: "2px solid #333",
              background: "#111",
              color: "#d4d4d4",
              fontFamily: '"Zpix", monospace',
              fontSize: 13,
            }}
          />
          <Legend
            verticalAlign="top"
            height={22}
            wrapperStyle={{ fontFamily: '"Zpix", monospace', fontSize: 11, color: "#aaa" }}
          />
          <ReferenceLine x={0.6} stroke="#777" strokeDasharray="5 5" />
          <ReferenceLine y={2} stroke="#777" strokeDasharray="5 5" />
          {grouped.map((g) => (
            <Scatter
              key={g.quadrant}
              name={g.quadrant}
              data={g.points}
              fill={QUADRANT_COLORS[g.quadrant] ?? "#33ccff"}
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

type HeatmapCell = {
  dayOfWeek: number;
  hourOfDay: number;
  intensityScore: number;
  eventCount: number;
  peakFlag: boolean;
};

const DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function colorForIntensity(v: number): string {
  const alpha = Math.min(Math.max(v, 0), 1) * 0.85 + 0.1;
  return `rgba(51, 204, 255, ${alpha.toFixed(3)})`;
}

export function RhythmHeatmap({
  title,
  data,
}: {
  title: string;
  data: HeatmapCell[];
}) {
  const cellMap = new Map<string, HeatmapCell>();
  data.forEach((item) => {
    const key = `${item.dayOfWeek}-${item.hourOfDay}`;
    cellMap.set(key, item);
  });

  return (
    <div>
      <p style={{ color: "var(--fg-strong)", marginBottom: 10 }}>{title}</p>
      <div style={{ overflowX: "auto" }}>
        <div style={{ minWidth: 820 }}>
          <div style={{ display: "grid", gridTemplateColumns: "60px repeat(24, 1fr)", gap: 4 }}>
            <div />
            {Array.from({ length: 24 }, (_, h) => (
              <div key={`h-${h}`} style={{ textAlign: "center", color: "var(--muted)", fontSize: 10 }}>
                {String(h).padStart(2, "0")}
              </div>
            ))}

            {DAY_LABELS.map((dayLabel, idx) => {
              const day = idx + 1;
              return (
                <div key={`row-${dayLabel}`} style={{ display: "contents" }}>
                  <div style={{ color: "var(--muted)", fontSize: 12, paddingTop: 6 }}>{dayLabel}</div>
                  {Array.from({ length: 24 }, (_, hour) => {
                    const cell = cellMap.get(`${day}-${hour}`);
                    return (
                      <div
                        key={`${day}-${hour}`}
                        title={`${dayLabel} ${hour}:00 events=${cell?.eventCount ?? 0}`}
                        style={{
                          height: 20,
                          border: "1px solid rgba(255,255,255,0.08)",
                          background: colorForIntensity(cell?.intensityScore ?? 0),
                          boxShadow: cell?.peakFlag ? "0 0 0 1px #ffcc00 inset" : "none",
                        }}
                      />
                    );
                  })}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}



