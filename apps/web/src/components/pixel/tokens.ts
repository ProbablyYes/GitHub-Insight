export type Tone =
  | "positive"
  | "change"
  | "info"
  | "danger"
  | "magenta"
  | "purple"
  | "muted";

export const toneColor: Record<Tone, string> = {
  positive: "var(--accent-positive)",
  change: "var(--accent-change)",
  info: "var(--accent-info)",
  danger: "var(--accent-danger)",
  magenta: "var(--accent-magenta)",
  purple: "var(--accent-purple)",
  muted: "var(--muted-strong)",
};

export const toneBg: Record<Tone, string> = {
  positive: "var(--accent-positive-dim)",
  change: "var(--accent-change-dim)",
  info: "var(--accent-info-dim)",
  danger: "var(--accent-danger-dim)",
  magenta: "var(--accent-magenta-dim)",
  purple: "var(--accent-purple-dim)",
  muted: "var(--divider)",
};

export const space = {
  s1: "var(--pixel-space-1)",
  s2: "var(--pixel-space-2)",
  s3: "var(--pixel-space-3)",
  s4: "var(--pixel-space-4)",
  s5: "var(--pixel-space-5)",
  s6: "var(--pixel-space-6)",
  s7: "var(--pixel-space-7)",
} as const;

export const fs = {
  hero: "var(--fs-hero)",
  title: "var(--fs-title)",
  section: "var(--fs-section)",
  body: "var(--fs-body)",
  caption: "var(--fs-caption)",
  micro: "var(--fs-micro)",
} as const;
