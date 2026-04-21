import type { CSSProperties } from "react";

import { type Tone, toneColor } from "./tokens";

type Props = {
  value: number;
  min?: number;
  max?: number;
  tone?: Tone;
  width?: number;
  height?: number;
  reference?: number | null;
  label?: string;
  valueText?: string;
  style?: CSSProperties;
  title?: string;
};

/**
 * A horizontal pixel-block bar used to show feature values on a 0..1 (or custom) range.
 * Optionally draws a reference marker (e.g. cohort mean) as a vertical divider.
 */
export function PixelMiniBar({
  value,
  min = 0,
  max = 1,
  tone = "info",
  width = 140,
  height = 10,
  reference = null,
  label,
  valueText,
  style,
  title,
}: Props) {
  const range = Math.max(max - min, 1e-9);
  const ratio = Math.max(0, Math.min(1, (value - min) / range));
  const color = toneColor[tone];

  const refRatio =
    reference == null ? null : Math.max(0, Math.min(1, (reference - min) / range));

  const content = (
    <div
      title={title}
      style={{
        position: "relative",
        width,
        height,
        background: "var(--divider)",
        boxShadow: "inset 0 0 0 1px rgba(255,255,255,0.04)",
        ...style,
      }}
    >
      <div
        style={{
          position: "absolute",
          left: 0,
          top: 0,
          bottom: 0,
          width: `${(ratio * 100).toFixed(2)}%`,
          background: color,
          imageRendering: "pixelated",
        }}
      />
      {refRatio != null ? (
        <div
          style={{
            position: "absolute",
            left: `${(refRatio * 100).toFixed(2)}%`,
            top: -2,
            bottom: -2,
            width: 2,
            background: "var(--fg-strong)",
          }}
        />
      ) : null}
    </div>
  );

  if (!label && !valueText) return content;

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "var(--pixel-space-2)",
        fontSize: "var(--fs-caption)",
        color: "var(--fg)",
      }}
    >
      {label ? (
        <span style={{ color: "var(--muted-strong)", minWidth: 110, textAlign: "right" }}>
          {label}
        </span>
      ) : null}
      {content}
      {valueText ? (
        <span style={{ color: "var(--fg)", minWidth: 48, textAlign: "left" }}>{valueText}</span>
      ) : null}
    </div>
  );
}
