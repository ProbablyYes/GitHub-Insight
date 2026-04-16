import { DashboardCharts } from "@/components/dashboard-charts";
import { PixelPageShell } from "@/components/pixel-shell";
import {
  getDailyTrend,
  getEventTypeBreakdown,
  getLanguageDayTrend,
  getTopReposLatest,
  getTopUsersLatest,
} from "@/lib/dashboard";

function shortLabel(value: string, maxLength = 16): string {
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength - 3)}...`;
}

export default async function OfflinePage() {
  const [dailyTrend, languageTrend, topUsers, topRepos, eventBreakdown] = await Promise.all([
    getDailyTrend(),
    getLanguageDayTrend(),
    getTopUsersLatest(),
    getTopReposLatest(),
    getEventTypeBreakdown(),
  ]);

  const dailyTrendData = dailyTrend.map((item) => ({
    label: item.metricDate.slice(5),
    value: item.totalEvents,
  }));

  const languageTrendData = languageTrend.map((item) => ({
    label: `${item.metricDate.slice(5)} ${shortLabel(item.languageGuess, 8)}`,
    value: item.totalEvents,
  }));

  const topUsersData = topUsers.map((item) => ({
    label: `#${item.rank} ${shortLabel(item.actorLogin)}`,
    value: item.eventCount,
  }));

  const topReposData = topRepos.map((item) => ({
    label: `#${item.rank} ${shortLabel(item.repoName)}`,
    value: item.eventCount,
  }));

  const eventTypeTotals = eventBreakdown.reduce<Record<string, number>>((acc, item) => {
    acc[item.eventType] = (acc[item.eventType] || 0) + item.totalEvents;
    return acc;
  }, {});

  const eventTypeData = Object.entries(eventTypeTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([eventType, totalEvents]) => ({
      label: eventType,
      value: totalEvents,
    }));

  return (
    <PixelPageShell
      title="离线分析实验室"
      subtitle="Spark 全量批处理 → Parquet/JSON → ClickHouse · 趋势、TopN 与多维拆分"
    >
      {/* ── Daily Trend ── */}
      <section className="nes-container with-title is-dark animate-float delay-1" style={{ marginBottom: 24 }}>
        <p className="title" style={{ color: "var(--cyan)" }}>◆ 长期趋势</p>
        {dailyTrendData.length > 0 ? (
          <DashboardCharts variant="line" data={dailyTrendData} color="#33ccff" />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center", padding: 40 }}>
            <span className="animate-blink">▶</span> 等待离线数据...
          </p>
        )}
      </section>

      {/* ── Language+Day + Event Split ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-2">
          <p className="title" style={{ color: "var(--cyan)" }}>技术趋势（language + day）</p>
          {languageTrendData.length > 0 ? (
            <DashboardCharts variant="bar" data={languageTrendData} color="#33ccff" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-3">
          <p className="title" style={{ color: "var(--yellow)" }}>事件类型拆分（近7天）</p>
          {eventTypeData.length > 0 ? (
            <DashboardCharts variant="pie" data={eventTypeData} />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>
      </div>

      {/* ── Top Users + Top Repos ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-4">
          <p className="title" style={{ color: "var(--green)" }}>Top 用户（最新日）</p>
          {topUsersData.length > 0 ? (
            <DashboardCharts variant="bar" data={topUsersData} color="#33ff57" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-5">
          <p className="title" style={{ color: "var(--green)" }}>Top 仓库（最新日）</p>
          {topReposData.length > 0 ? (
            <DashboardCharts variant="bar" data={topReposData} color="#33ff57" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>
      </div>
    </PixelPageShell>
  );
}
