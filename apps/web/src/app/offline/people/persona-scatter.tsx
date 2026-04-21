"use client";

import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useEntityDrawer } from "@/components/entity";

type Point = {
  actorLogin: string;
  personaId: number;
  personaLabel: string;
  pcaX: number;
  pcaY: number;
  eventCount: number;
  isBot: number;
};

const COLORS = [
  "#33ff57",
  "#33ccff",
  "#ffcc00",
  "#ff66cc",
  "#ff6633",
  "#a07bff",
  "#8bd17c",
  "#c6d1ff",
];

export function PersonaScatter({ data }: { data: Point[] }) {
  const { open } = useEntityDrawer();

  const grouped = new Map<number, { label: string; points: Point[] }>();
  for (const p of data) {
    if (!grouped.has(p.personaId)) {
      grouped.set(p.personaId, { label: p.personaLabel, points: [] });
    }
    grouped.get(p.personaId)!.points.push(p);
  }

  const series = Array.from(grouped.entries()).sort((a, b) => a[0] - b[0]);

  return (
    <div style={{ width: "100%", height: 380, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
          <CartesianGrid stroke="#223" strokeDasharray="3 3" />
          <XAxis type="number" dataKey="pcaX" name="PC1" tick={{ fill: "#9aa" }} />
          <YAxis type="number" dataKey="pcaY" name="PC2" tick={{ fill: "#9aa" }} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            contentStyle={{
              borderRadius: 0,
              border: "2px solid #333",
              background: "#111",
              color: "#d4d4d4",
              fontFamily: '"Zpix", monospace',
              fontSize: 12,
            }}
            formatter={(_value, _name, item: unknown) => {
              const payload = (item as { payload?: Point } | undefined)?.payload;
              if (!payload) return ["", ""];
              return [
                `${payload.personaLabel} · ${payload.eventCount} ev${payload.isBot ? " · bot" : ""}`,
                shortName(payload.actorLogin),
              ];
            }}
          />
          {series.map(([personaId, { label, points }], idx) => (
            <Scatter
              key={personaId}
              name={`${personaId}:${label}`}
              data={points}
              fill={COLORS[idx % COLORS.length]}
              onClick={(payload: unknown) => {
                const actor = (payload as { actorLogin?: string } | undefined)?.actorLogin;
                if (actor) open({ type: "actor", id: actor });
              }}
              cursor="pointer"
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
      <p style={{ color: "var(--muted)", fontSize: 11, marginTop: 6, textAlign: "center" }}>
        Tip: click a dot to open the actor analysis drawer.
      </p>
    </div>
  );
}

function shortName(name: string) {
  return name.length <= 24 ? name : `${name.slice(0, 21)}...`;
}
