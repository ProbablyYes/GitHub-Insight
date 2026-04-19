import { getSummaryMetrics } from "@/lib/dashboard";
import { PixelPageShell } from "@/components/pixel-shell";
import Link from "next/link";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function fmt(v: number) {
  return new Intl.NumberFormat("zh-CN").format(v);
}

export default async function Home() {
  const s = await getSummaryMetrics();

  return (
    <PixelPageShell
      title="GitHub 开源生态监控台"
      subtitle="流批一体分析平台 ── 基于 GH Archive 的开发者行为观察站"
    >
      {/* ── Status ── */}
      <section className="nes-container with-title is-dark animate-float delay-1" style={{ marginBottom: 24 }}>
        <p className="title">系统状态</p>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 24 }}>
          <div>
            <p style={{ color: "var(--green)", fontSize: 14, marginBottom: 6 }}>♥ 实时链路</p>
            <progress className="nes-progress is-success" value={s.realtimeEventRows > 0 ? 80 : 10} max={100} />
            <p style={{ color: "var(--fg)", marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.realtimeEventRows)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>条记录 · {fmt(s.realtimeRepos)} 个仓库</span>
            </p>
          </div>

          <div>
            <p style={{ color: "var(--yellow)", fontSize: 14, marginBottom: 6 }}>⚠ 异常预警</p>
            <progress className="nes-progress is-warning" value={Math.min(s.anomalyAlerts * 5, 100)} max={100} />
            <p style={{ marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.anomalyAlerts)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>条告警</span>
            </p>
          </div>

          <div>
            <p style={{ color: "var(--cyan)", fontSize: 14, marginBottom: 6 }}>◆ 离线链路</p>
            <progress className="nes-progress is-primary" value={s.batchMetricRows > 0 ? 70 : 5} max={100} />
            <p style={{ marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.batchMetricRows)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>条记录</span>
            </p>
          </div>
        </div>
      </section>

      {/* ── Navigation ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-2">
          <p className="title" style={{ color: "var(--green)" }}>♦ 实时作战室</p>
          <p style={{ fontSize: 15, color: "var(--fg-strong)", marginBottom: 12 }}>
            Kafka → Consumer → ClickHouse
          </p>
          <p style={{ color: "var(--muted)", marginBottom: 16 }}>
            分钟级窗口聚合 · 热点仓库追踪 · 异常突增预警
          </p>
          <Link href="/realtime">
            <button type="button" className="nes-btn is-success" style={{ width: "100%" }}>
              进入实时作战室 &gt;&gt;
            </button>
          </Link>
        </section>

        <section className="nes-container with-title is-dark animate-float delay-3">
          <p className="title" style={{ color: "var(--cyan)" }}>♠ 离线实验室</p>
          <p style={{ fontSize: 15, color: "var(--fg-strong)", marginBottom: 12 }}>
            Spark → Parquet → ClickHouse
          </p>
          <p style={{ color: "var(--muted)", marginBottom: 16 }}>
            日级趋势分析 · Bot/Human 画像 · 活跃节律图谱
          </p>
          <Link href="/offline">
            <button type="button" className="nes-btn is-primary" style={{ width: "100%" }}>
              进入离线实验室 &gt;&gt;
            </button>
          </Link>
        </section>
      </div>

      {/* ── Architecture ── */}
      <section className="nes-container with-title is-dark animate-float delay-4" style={{ marginBottom: 24 }}>
        <p className="title">系统架构</p>
        <pre style={{ color: "var(--green)", lineHeight: 1.5, overflow: "auto", margin: 0, fontSize: 13 }}>
{`  ┌──────────┐     ┌───────┐     ┌──────────┐     ┌────────────┐
  │ GH       │────▶│ Kafka │────▶│ Realtime │────▶│            │
  │ Archive  │     └───────┘     │ Consumer │     │ ClickHouse │
  │          │                    └──────────┘     │            │
  │          │     ┌───────┐     ┌──────────┐     │            │
  │          │────▶│ Spark │────▶│ Parquet  │────▶│            │
  └──────────┘     └───────┘     └──────────┘     └─────┬──────┘
                                                        │
                                                        ▼
                                                  ┌──────────┐
                                                  │ Next.js  │
                                                  └──────────┘`}
        </pre>
      </section>

      {/* ── Quest Board ── */}
      <section className="nes-container with-title is-dark animate-float delay-5" style={{ marginBottom: 24 }}>
        <p className="title">任务面板</p>
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          {[
            { done: true, text: "实时雷达已上线", sub: "事件流 · 热点 · 预警", color: "var(--green)" },
            { done: true, text: "离线实验室已上线", sub: "趋势 · 画像 · 节律", color: "var(--cyan)" },
            { done: false, text: "扩展探索区", sub: "LOCKED", color: "var(--muted)" },
            { done: false, text: "答辩故事线", sub: "LOCKED", color: "var(--muted)" },
          ].map((q) => (
            <div key={q.text} style={{ display: "flex", alignItems: "center", gap: 16 }}>
              <label style={{ display: "flex", alignItems: "center" }}>
                <input type="checkbox" className="nes-checkbox is-dark" checked={q.done} readOnly />
                <span style={{ color: q.color, fontSize: 15 }}>{q.text}</span>
              </label>
              <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: 13 }}>{q.sub}</span>
            </div>
          ))}
        </div>
      </section>

      {/* ── Tech Stack (flat grid, no nested boxes) ── */}
      <section style={{ marginBottom: 24 }} className="animate-float delay-5">
        <p style={{ color: "var(--muted)", fontSize: 12, letterSpacing: 4, marginBottom: 12 }}>技术栈</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12 }}>
          {[
            { name: "Kafka", desc: "事件总线", c: "var(--green)" },
            { name: "ClickHouse", desc: "分析数据库", c: "var(--green)" },
            { name: "Spark", desc: "离线批处理", c: "var(--cyan)" },
            { name: "Next.js", desc: "展示层", c: "var(--cyan)" },
            { name: "MinIO", desc: "对象存储", c: "var(--yellow)" },
            { name: "Docker", desc: "容器编排", c: "var(--yellow)" },
          ].map((t) => (
            <div key={t.name} style={{ background: "var(--panel)", border: "2px solid var(--divider)", padding: "12px 16px" }}>
              <p style={{ color: t.c, fontSize: 15, marginBottom: 2 }}>{t.name}</p>
              <p style={{ color: "var(--muted)", fontSize: 13 }}>{t.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Footer */}
      <p style={{ textAlign: "center", color: "var(--muted)", fontSize: 13, padding: "16px 0" }}>
        ♥ GitHub Insight ── 大数据处理技术课程项目
        <span className="animate-blink" style={{ marginLeft: 8, color: "var(--green)" }}>_</span>
      </p>
    </PixelPageShell>
  );
}
