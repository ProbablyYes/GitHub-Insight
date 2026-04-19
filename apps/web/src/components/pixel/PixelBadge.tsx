import type { CSSProperties, ReactNode } from "react";

import { type Tone, toneBg, toneColor } from "./tokens";

type Props = {
  tone?: Tone;
  children: ReactNode;
  title?: string;
  style?: CSSProperties;
  size?: "sm" | "md";
};

export function PixelBadge({ tone = "info", children, title, style, size = "md" }: Props) {
  const fontSize = size === "sm" ? "var(--fs-micro)" : "var(--fs-caption)";
  const padV = size === "sm" ? 2 : 3;
  const padH = size === "sm" ? 5 : 8;
  return (
    <span
      title={title}
      style={{
        display: "inline-block",
        padding: `${padV}px ${padH}px`,
        background: toneBg[tone],
        color: toneColor[tone],
        fontSize,
        lineHeight: 1.2,
        border: `2px solid ${toneColor[tone]}`,
        letterSpacing: 0.5,
        whiteSpace: "nowrap",
        ...style,
      }}
    >
      {children}
    </span>
  );
}
