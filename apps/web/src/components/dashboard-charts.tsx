"use client";

import dynamic from "next/dynamic";

type ChartPoint = {
  label: string;
  value: number;
};

type DashboardChartsProps = {
  variant: "line" | "bar" | "pie";
  data: ChartPoint[];
  color?: string;
  onSliceClick?: (label: string) => void;
};

const MetricLineChart = dynamic(
  () => import("@/components/metric-line-chart").then((module) => module.MetricLineChart),
  {
    ssr: false,
    loading: () => <div className="h-72 w-full min-w-0 rounded-2xl bg-slate-950/40" />,
  }
);

const MetricBarChart = dynamic(
  () => import("@/components/metric-bar-chart").then((module) => module.MetricBarChart),
  {
    ssr: false,
    loading: () => <div className="h-72 w-full min-w-0 rounded-2xl bg-slate-950/40" />,
  }
);

const MetricPieChart = dynamic(
  () => import("@/components/metric-pie-chart").then((module) => module.MetricPieChart),
  {
    ssr: false,
    loading: () => <div className="h-72 w-full min-w-0 rounded-2xl bg-slate-950/40" />,
  }
);

export function DashboardCharts({
  variant,
  data,
  color,
  onSliceClick,
}: DashboardChartsProps) {
  if (variant === "bar") {
    return <MetricBarChart data={data} color={color} />;
  }

  if (variant === "pie") {
    return <MetricPieChart data={data} onSliceClick={onSliceClick} />;
  }

  return <MetricLineChart data={data} color={color} />;
}
