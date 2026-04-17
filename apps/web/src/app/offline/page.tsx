import {
  AdvancedTrendChart,
  BurstScatterChart,
  RhythmHeatmap,
} from "@/components/advanced-trend-chart";
import { DashboardCharts } from "@/components/dashboard-charts";
import { PixelPageShell } from "@/components/pixel-shell";
import {
  getAdvancedRepoRankings,
  getBurstStabilitySnapshot,
  getDailyTrend,
  getDeveloperRhythmHeatmap,
  getEventTypeBreakdown,
  getHotnessComponentsLatest,
  getLanguageDayTrend,
  getRepoTrendForecast,
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
  const [
    dailyTrend,
    languageTrend,
    topUsers,
    topRepos,
    eventBreakdown,
    advancedRankings,
    trendForecast,
    burstSnapshot,
    rhythmHuman,
    rhythmBot,
    hotnessComponents,
  ] = await Promise.all([
    getDailyTrend(),
    getLanguageDayTrend(),
    getTopUsersLatest(),
    getTopReposLatest(),
    getEventTypeBreakdown(),
    getAdvancedRepoRankings(),
    getRepoTrendForecast(),
    getBurstStabilitySnapshot(),
    getDeveloperRhythmHeatmap("human"),
    getDeveloperRhythmHeatmap("bot"),
    getHotnessComponentsLatest(),
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

  const rankScoreData = advancedRankings.map((item) => ({
    label: `#${item.rankNo} ${shortLabel(item.repoName, 12)}`,
    value: Number(item.rankScore.toFixed(4)),
  }));

  const trendData = trendForecast.map((item) => ({
    label: item.metricDate.slice(5),
    actual: item.totalEvents,
    ma7: item.ma7,
    forecast: item.forecastNextDay,
  }));

  const hotnessExplainData = hotnessComponents.map((item) => ({
    label: `#${item.rankNo} ${shortLabel(item.repoName, 10)}`,
    value: Number(item.hotnessRaw.toFixed(2)),
  }));

  return (
    <PixelPageShell
      title="离线分析实验室"
      subtitle="Spark 全量批处理 → 高级评分与预测 → ClickHouse · 趋势、排名、爆发稳定与节律热图"
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

      {/* ── Composite Ranking + Explainability ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-4">
          <p className="title" style={{ color: "var(--green)" }}>综合排名（非简单count）</p>
          {rankScoreData.length > 0 ? (
            <DashboardCharts variant="bar" data={rankScoreData} color="#33ff57" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无高级排名数据</p>
          )}
          <p style={{ color: "var(--muted)", marginTop: 8, fontSize: 12 }}>
            score = 0.45*hotness + 0.25*momentum + 0.20*engagement + 0.10*stability - 0.10*bot
          </p>
        </section>

        <section className="nes-container with-title is-dark animate-float delay-5">
          <p className="title" style={{ color: "var(--yellow)" }}>热度解释（权重贡献）</p>
          {hotnessExplainData.length > 0 ? (
            <DashboardCharts variant="bar" data={hotnessExplainData} color="#ffcc00" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无热度分解数据</p>
          )}
          {hotnessComponents.length > 0 ? (
            <div style={{ marginTop: 10, color: "var(--muted)", fontSize: 12 }}>
              Top repo 权重拆分：Fork({(hotnessComponents[0].forkContribution * 100).toFixed(1)}%) / PR({(
                hotnessComponents[0].pullRequestContribution * 100
              ).toFixed(1)}%) / Push({(hotnessComponents[0].pushContribution * 100).toFixed(1)}%)
            </div>
          ) : null}
        </section>
      </div>

      {/* ── Trend + Forecast ── */}
      <section className="nes-container with-title is-dark animate-float delay-6" style={{ marginBottom: 24 }}>
        <p className="title" style={{ color: "var(--cyan)" }}>趋势与预测（MA7 + 回归近似）</p>
        {trendData.length > 0 ? (
          <AdvancedTrendChart data={trendData} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无趋势预测数据</p>
        )}
      </section>

      {/* ── Burst vs Stability ── */}
      <section className="nes-container with-title is-dark animate-float delay-7" style={{ marginBottom: 24 }}>
        <p className="title" style={{ color: "var(--yellow)" }}>短期爆发 vs 长期稳定</p>
        {burstSnapshot.length > 0 ? (
          <BurstScatterChart
            data={burstSnapshot.map((item) => ({
              repoName: item.repoName,
              burstIndex: item.burstIndex,
              stabilityIndex: item.stabilityIndex,
              rankScore: item.rankScore,
              quadrant: item.quadrant,
            }))}
          />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无爆发稳定数据</p>
        )}
        <p style={{ color: "var(--muted)", marginTop: 8, fontSize: 12 }}>
          Quadrant: rising_core / short_spike / steady_core / long_tail
        </p>
      </section>

      {/* ── Developer Rhythm Heatmaps ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-8">
          <p className="title" style={{ color: "var(--cyan)" }}>Human 节律热图</p>
          {rhythmHuman.length > 0 ? (
            <RhythmHeatmap title="Human activity by weekday/hour" data={rhythmHuman} />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无 Human 节律数据</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-9">
          <p className="title" style={{ color: "var(--green)" }}>Bot 节律热图</p>
          {rhythmBot.length > 0 ? (
            <RhythmHeatmap title="Bot activity by weekday/hour" data={rhythmBot} />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无 Bot 节律数据</p>
          )}
        </section>
      </div>

      {/* ── Top Users + Top Repos ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-10">
          <p className="title" style={{ color: "var(--green)" }}>Top 用户（最新日）</p>
          {topUsersData.length > 0 ? (
            <DashboardCharts variant="bar" data={topUsersData} color="#33ff57" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-11">
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
