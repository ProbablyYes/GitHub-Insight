import { MetricBarChart } from "@/components/metric-bar-chart";
import { MetricLineChart } from "@/components/metric-line-chart";
import { MetricPieChart } from "@/components/metric-pie-chart";
import {
  getActivityPattern,
  getActorMix,
  getAlerts,
  getDailyTrend,
  getEventTrend,
  getHotRepos,
  getSummaryMetrics,
} from "@/lib/dashboard";

function formatNumber(value: number) {
  return new Intl.NumberFormat("zh-CN").format(value);
}

function formatScore(value: number) {
  return value.toFixed(2);
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex h-72 items-center justify-center rounded-2xl border border-dashed border-slate-300/50 bg-slate-50/60 text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-400">
      {message}
    </div>
  );
}

function MetricCard({
  title,
  value,
  hint,
}: {
  title: string;
  value: string;
  hint: string;
}) {
  return (
    <div className="rounded-3xl border border-[var(--color-panel-border)] bg-[var(--color-panel)] p-6 shadow-sm">
      <p className="text-sm text-slate-500 dark:text-slate-400">{title}</p>
      <p className="mt-3 text-3xl font-semibold tracking-tight">{value}</p>
      <p className="mt-2 text-sm text-slate-500 dark:text-slate-400">{hint}</p>
    </div>
  );
}

export default async function Home() {
  const [summary, eventTrend, hotRepos, alerts, dailyTrend, actorMix, activityPattern] =
    await Promise.all([
      getSummaryMetrics(),
      getEventTrend(),
      getHotRepos(),
      getAlerts(),
      getDailyTrend(),
      getActorMix(),
      getActivityPattern(),
    ]);

  const eventTrendData = eventTrend.map((item) => ({
    label: item.windowStart.slice(11, 16),
    value: item.totalEvents,
  }));

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
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <section className="mx-auto flex w-full max-w-7xl flex-col gap-8 px-6 py-10 lg:px-10">
        <div className="rounded-[32px] border border-slate-800 bg-gradient-to-br from-slate-900 via-slate-950 to-blue-950/50 p-8 shadow-2xl">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <p className="text-sm font-medium uppercase tracking-[0.28em] text-blue-300">
                GitHub Insight
              </p>
              <h1 className="mt-4 text-4xl font-semibold tracking-tight text-white lg:text-5xl">
                GitHub 开发者行为流批分析系统
              </h1>
              <p className="mt-4 max-w-2xl text-base leading-7 text-slate-300">
                基于 GH Archive、Kafka、轻量实时消费者、Spark、ClickHouse 与 Next.js
                的课程项目仪表盘。这里统一展示实时事件趋势、热门仓库、异常预警以及离线行为画像。
              </p>
            </div>
            <div className="grid gap-3 rounded-3xl border border-blue-400/20 bg-blue-400/10 p-5 text-sm text-slate-200">
              <p>实时链路：GH Archive 回放 → Kafka → 实时聚合 → ClickHouse</p>
              <p>离线链路：原始事件 → Spark 聚合 → ClickHouse</p>
              <p>展示层：Next.js 正式大屏，Streamlit 可作备用演示页</p>
            </div>
          </div>
        </div>

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            title="实时指标记录数"
            value={formatNumber(summary.realtimeEventRows)}
            hint="来自 realtime_event_metrics"
          />
          <MetricCard
            title="实时涉及仓库数"
            value={formatNumber(summary.realtimeRepos)}
            hint="来自 realtime_repo_scores"
          />
          <MetricCard
            title="异常预警数"
            value={formatNumber(summary.anomalyAlerts)}
            hint="来自 realtime_anomaly_alerts"
          />
          <MetricCard
            title="离线指标记录数"
            value={formatNumber(summary.batchMetricRows)}
            hint="来自 batch_daily_metrics"
          />
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.5fr_1fr]">
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">实时事件量趋势</h2>
              <p className="mt-1 text-sm text-slate-400">
                分钟级事件总量变化，用于展示 Kafka 回放后的实时处理结果。
              </p>
            </div>
            {eventTrendData.length > 0 ? (
              <MetricLineChart data={eventTrendData} />
            ) : (
              <EmptyState message="暂无实时数据，请先启动 Kafka 回放和实时消费者。" />
            )}
          </div>

          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">Bot 与人类账号占比</h2>
              <p className="mt-1 text-sm text-slate-400">离线聚合后的账号行为结构。</p>
            </div>
            {actorMixData.length > 0 ? (
              <MetricPieChart data={actorMixData} />
            ) : (
              <EmptyState message="暂无离线账号结构数据，请先运行 Spark 与导入脚本。" />
            )}
          </div>
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">热门仓库榜单</h2>
              <p className="mt-1 text-sm text-slate-400">
                基于 Watch、Fork、Issue、PR、Push 加权得到的热度分数。
              </p>
            </div>
            {hotRepos.length > 0 ? (
              <div className="overflow-hidden rounded-2xl border border-slate-800">
                <table className="min-w-full divide-y divide-slate-800 text-sm">
                  <thead className="bg-slate-950/60 text-slate-300">
                    <tr>
                      <th className="px-4 py-3 text-left font-medium">仓库</th>
                      <th className="px-4 py-3 text-right font-medium">热度分数</th>
                      <th className="px-4 py-3 text-right font-medium">Push</th>
                      <th className="px-4 py-3 text-right font-medium">Watch</th>
                      <th className="px-4 py-3 text-right font-medium">Fork</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {hotRepos.map((repo) => (
                      <tr key={repo.repoName} className="bg-slate-900/40">
                        <td className="px-4 py-3 text-slate-100">{repo.repoName}</td>
                        <td className="px-4 py-3 text-right text-blue-300">
                          {formatScore(repo.hotnessScore)}
                        </td>
                        <td className="px-4 py-3 text-right text-slate-300">
                          {formatNumber(repo.pushEvents)}
                        </td>
                        <td className="px-4 py-3 text-right text-slate-300">
                          {formatNumber(repo.watchEvents)}
                        </td>
                        <td className="px-4 py-3 text-right text-slate-300">
                          {formatNumber(repo.forkEvents)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyState message="暂无热门仓库数据。" />
            )}
          </div>

          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">异常活跃预警</h2>
              <p className="mt-1 text-sm text-slate-400">识别短时间内活跃度高于基线的仓库。</p>
            </div>
            {alerts.length > 0 ? (
              <div className="flex flex-col gap-3">
                {alerts.map((alert) => (
                  <div
                    key={`${alert.windowStart}-${alert.repoName}`}
                    className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <p className="font-medium text-white">{alert.repoName}</p>
                      <span className="rounded-full bg-amber-400/15 px-3 py-1 text-xs font-medium text-amber-300">
                        {alert.alertLevel}
                      </span>
                    </div>
                    <p className="mt-2 text-sm text-slate-400">
                      比值 {formatScore(alert.anomalyRatio)}，当前 {alert.currentEvents}，基线{" "}
                      {formatScore(alert.baselineEvents)}
                    </p>
                    <p className="mt-1 text-xs text-slate-500">{alert.windowStart}</p>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState message="暂无异常预警。" />
            )}
          </div>
        </section>

        <section className="grid gap-6 xl:grid-cols-2">
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">离线日级趋势</h2>
              <p className="mt-1 text-sm text-slate-400">
                Spark 聚合后的长期总事件量，用于和实时窗口结果形成对照。
              </p>
            </div>
            {dailyTrendData.length > 0 ? (
              <MetricBarChart data={dailyTrendData} />
            ) : (
              <EmptyState message="暂无离线趋势数据。" />
            )}
          </div>

          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
            <div className="mb-4">
              <h2 className="text-xl font-semibold text-white">开发者活跃节律</h2>
              <p className="mt-1 text-sm text-slate-400">
                先展示 human 账号按小时分布，后续可扩展为多序列对比。
              </p>
            </div>
            {activityHuman.length > 0 ? (
              <MetricLineChart data={activityHuman} color="#8b5cf6" />
            ) : (
              <EmptyState message="暂无活跃节律数据。" />
            )}
          </div>
        </section>

        <section className="rounded-3xl border border-slate-800 bg-slate-900/70 p-6 shadow-lg">
          <h2 className="text-xl font-semibold text-white">答辩可讲的系统亮点</h2>
          <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-2xl bg-slate-950/70 p-4">
              <p className="font-medium text-white">流批一体</p>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                实时链路负责热点感知，离线链路负责长期趋势和行为画像。
              </p>
            </div>
            <div className="rounded-2xl bg-slate-950/70 p-4">
              <p className="font-medium text-white">可重复演示</p>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                通过 GH Archive 历史回放模拟实时流，避免依赖现场网络与 GitHub API。
              </p>
            </div>
            <div className="rounded-2xl bg-slate-950/70 p-4">
              <p className="font-medium text-white">统一结果层</p>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                ClickHouse 汇总实时与离线指标，为前端、Superset 与备用页提供统一查询入口。
              </p>
            </div>
            <div className="rounded-2xl bg-slate-950/70 p-4">
              <p className="font-medium text-white">正式前端展示</p>
              <p className="mt-2 text-sm leading-6 text-slate-400">
                使用 Next.js 构建更适合课程答辩的大屏风格展示，而不是仅靠原型工具。
              </p>
            </div>
          </div>
        </section>
      </section>
    </main>
  );
}
