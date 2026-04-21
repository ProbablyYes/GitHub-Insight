import type { CSSProperties, ReactNode } from "react";

import { type Tone, toneColor } from "./tokens";

type Props = {
  tone?: Tone;
  children: ReactNode;
  style?: CSSProperties;
  title?: string;
};

export function PixelChip({ tone = "info", children, style, title }: Props) {
  return (
    <span
      title={title}
      style={{
        display: "inline-block",
        padding: "2px 6px",
        background: "transparent",
        color: toneColor[tone],
        fontSize: "var(--fs-caption)",
        lineHeight: 1.2,
        border: `1px solid ${toneColor[tone]}`,
        letterSpacing: 0.3,
        ...style,
      }}
    >
      {children}
    </span>
  );
}
