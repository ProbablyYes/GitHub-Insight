import { DashboardCharts } from "@/components/dashboard-charts";
import { PixelPageShell } from "@/components/pixel-shell";
import { getActivityPattern, getActorMix, getDailyTrend } from "@/lib/dashboard";

export default async function OfflinePage() {
  const [dailyTrend, actorMix, activityPattern] = await Promise.all([
    getDailyTrend(),
    getActorMix(),
    getActivityPattern(),
  ]);

  const dailyTrendData = dailyTrend.map((item) => ({
    label: item.metricDate.slice(5),
    value: item.totalEvents,
  }));

  const actorMixData = actorMix.map((item) => ({
    label: item.actorCategory,
    value: item.totalEvents,
  }));

  const activityHuman = activityPattern
    .filter((item) => item.actorCategory === "human")
    .map((item) => ({
      label: `${String(item.hourOfDay).padStart(2, "0")}:00`,
      value: item.totalEvents,
    }));

  return (
    <PixelPageShell
      title="离线分析实验室"
      subtitle="Spark 全量批处理 → Parquet → ClickHouse · 日级趋势与长期画像"
    >
      {/* ── Daily Trend ── */}
      <section className="nes-container with-title is-dark animate-float delay-1" style={{ marginBottom: 24 }}>
        <p className="title" style={{ color: "var(--cyan)" }}>◆ 长期趋势</p>
        {dailyTrendData.length > 0 ? (
          <DashboardCharts variant="bar" data={dailyTrendData} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center", padding: 40 }}>
            <span className="animate-blink">▶</span> 等待离线数据...
          </p>
        )}
      </section>

      {/* ── Actor Mix + Activity Rhythm ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-2">
          <p className="title" style={{ color: "var(--cyan)" }}>账号结构</p>
          {actorMixData.length > 0 ? (
            <DashboardCharts variant="pie" data={actorMixData} />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-3">
          <p className="title" style={{ color: "var(--yellow)" }}>活跃节律</p>
          {activityHuman.length > 0 ? (
            <DashboardCharts variant="line" data={activityHuman} color="#ffcc00" />
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>暂无数据</p>
          )}
        </section>
      </div>

      {/* ── Locked slots (flat) ── */}
      <section style={{ marginBottom: 24 }} className="animate-float delay-4">
        <p style={{ color: "var(--muted)", fontSize: 12, letterSpacing: 4, marginBottom: 12 }}>待解锁模块</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 10 }}>
          {["语言生态长期活跃度", "长期稳定 vs 短期爆发", "开发者节律画像", "仓库生命周期分析"].map((slot) => (
            <div key={slot} style={{ background: "var(--panel)", border: "2px solid var(--divider)", padding: "10px 14px", opacity: 0.5 }}>
              <span style={{ color: "var(--muted)" }}>🔒 {slot}</span>
            </div>
          ))}
        </div>
      </section>
    </PixelPageShell>
  );
}
