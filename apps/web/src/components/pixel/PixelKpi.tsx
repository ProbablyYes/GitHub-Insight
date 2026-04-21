import type { ReactNode } from "react";

import { PixelSparkline } from "./PixelSparkline";
import { type Tone, toneColor } from "./tokens";

type Props = {
  label: string;
  value: ReactNode;
  unit?: string;
  delta?: number | null;
  deltaLabel?: string;
  spark?: number[];
  tone?: Tone;
  hint?: string;
};

function formatDelta(value: number): string {
  const abs = Math.abs(value);
  const formatted = abs >= 10 ? abs.toFixed(0) : abs.toFixed(1);
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${formatted}%`;
}

export function PixelKpi({ label, value, unit, delta, deltaLabel, spark, tone = "positive", hint }: Props) {
  const deltaTone: Tone =
    delta == null ? "muted" : delta > 0 ? "positive" : delta < 0 ? "danger" : "muted";
  const arrow = delta == null ? "" : delta > 0 ? "↑" : delta < 0 ? "↓" : "→";

  return (
    <div
      className="nes-container is-dark"
      style={{
        minWidth: 0,
        padding: "var(--pixel-space-4) var(--pixel-space-4)",
        display: "flex",
        flexDirection: "column",
        gap: "var(--pixel-space-2)",
      }}
    >
      <div
        style={{
          color: "var(--muted-strong)",
          fontSize: "var(--fs-caption)",
          letterSpacing: 1,
          textTransform: "uppercase",
        }}
        title={hint}
      >
        {label}
      </div>
      <div style={{ display: "flex", alignItems: "baseline", gap: "var(--pixel-space-2)", flexWrap: "wrap" }}>
        <div
          style={{
            color: toneColor[tone],
            fontSize: "var(--fs-hero)",
            lineHeight: 1.1,
            wordBreak: "break-all",
          }}
        >
          {value}
        </div>
        {unit ? (
          <div style={{ color: "var(--muted-strong)", fontSize: "var(--fs-body)" }}>{unit}</div>
        ) : null}
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "var(--pixel-space-2)",
          minHeight: 28,
        }}
      >
        {delta != null && Number.isFinite(delta) ? (
          <div
            style={{
              color: toneColor[deltaTone],
              fontSize: "var(--fs-caption)",
              lineHeight: 1.2,
            }}
          >
            {arrow} {formatDelta(delta)}
            {deltaLabel ? (
              <span style={{ color: "var(--muted)", marginLeft: 4 }}>{deltaLabel}</span>
            ) : null}
          </div>
        ) : (
          <div style={{ color: "var(--muted)", fontSize: "var(--fs-caption)" }}>
            {deltaLabel ?? ""}
          </div>
        )}
        {spark && spark.length > 1 ? (
          <PixelSparkline values={spark} tone={tone} width={90} height={22} />
        ) : null}
      </div>
    </div>
  );
}
