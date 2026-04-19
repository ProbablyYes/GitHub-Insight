import { type Tone, toneColor } from "./tokens";

type Props = {
  values: number[];
  tone?: Tone;
  width?: number;
  height?: number;
};

export function PixelSparkline({ values, tone = "info", width = 90, height = 24 }: Props) {
  if (!values || values.length === 0) {
    return <svg width={width} height={height} aria-hidden />;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 1e-9);
  const step = width / Math.max(values.length - 1, 1);
  const color = toneColor[tone];

  const points = values
    .map((v, i) => {
      const x = i * step;
      const y = height - ((v - min) / range) * (height - 2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden>
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="square"
        strokeLinejoin="miter"
      />
      {/* pixel cursor on last point */}
      <rect
        x={width - 3}
        y={height - ((values[values.length - 1]! - min) / range) * (height - 2) - 2}
        width={3}
        height={3}
        fill={color}
      />
    </svg>
  );
}
