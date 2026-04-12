from __future__ import annotations

import pandas as pd
import streamlit as st

from jobs.common.clickhouse_client import get_clickhouse_client


st.set_page_config(page_title="GitHub Stream-Batch Analytics", layout="wide")


@st.cache_data(ttl=30)
def query_frame(sql: str) -> pd.DataFrame:
    client = get_clickhouse_client()
    return client.query_df(sql)


st.title("GitHub 开发者行为流批分析系统")
st.caption("基于 GH Archive、Kafka、Flink、Spark、ClickHouse 和 Streamlit 的课程项目演示页面")

st.markdown(
    """
    本系统将 GitHub 历史事件数据拆分为两条链路：

    - 实时链路：通过回放 GH Archive 数据模拟实时流，展示分钟级变化、热点仓库和异常预警。
    - 离线链路：通过 Spark 对历史事件进行全量聚合，展示长期趋势、活跃节律和 Bot 行为差异。
    """
)

col1, col2, col3 = st.columns(3)
with col1:
    event_total = query_frame("SELECT count() AS value FROM realtime_event_metrics")
    st.metric("实时指标记录数", int(event_total["value"].iloc[0]) if not event_total.empty else 0)
with col2:
    repo_total = query_frame("SELECT countDistinct(repo_name) AS value FROM realtime_repo_scores")
    st.metric("实时涉及仓库数", int(repo_total["value"].iloc[0]) if not repo_total.empty else 0)
with col3:
    anomaly_total = query_frame("SELECT count() AS value FROM realtime_anomaly_alerts")
    st.metric("异常预警数", int(anomaly_total["value"].iloc[0]) if not anomaly_total.empty else 0)

st.subheader("实时事件量趋势")
event_trend = query_frame(
    """
    SELECT
        window_start,
        sum(event_count) AS total_events
    FROM realtime_event_metrics
    GROUP BY window_start
    ORDER BY window_start
    LIMIT 500
    """
)
if event_trend.empty:
    st.info("暂无实时数据，请先运行 Kafka 回放和流处理脚本。")
else:
    st.line_chart(event_trend.set_index("window_start"))

st.subheader("热门仓库榜单")
hot_repos = query_frame(
    """
    SELECT
        repo_name,
        max(hotness_score) AS hotness_score,
        sum(push_events) AS push_events,
        sum(watch_events) AS watch_events,
        sum(fork_events) AS fork_events
    FROM realtime_repo_scores
    GROUP BY repo_name
    ORDER BY hotness_score DESC
    LIMIT 15
    """
)
if hot_repos.empty:
    st.info("暂无热门仓库数据。")
else:
    st.dataframe(hot_repos, use_container_width=True)

st.subheader("仓库异常活跃预警")
alerts = query_frame(
    """
    SELECT
        window_start,
        repo_name,
        current_events,
        baseline_events,
        round(anomaly_ratio, 2) AS anomaly_ratio,
        alert_level
    FROM realtime_anomaly_alerts
    ORDER BY window_start DESC, anomaly_ratio DESC
    LIMIT 20
    """
)
if alerts.empty:
    st.info("暂无异常预警。")
else:
    st.dataframe(alerts, use_container_width=True)

left_col, right_col = st.columns(2)
with left_col:
    st.subheader("离线日级趋势")
    daily_metrics = query_frame(
        """
        SELECT
            metric_date,
            sum(event_count) AS total_events
        FROM batch_daily_metrics
        GROUP BY metric_date
        ORDER BY metric_date
        LIMIT 90
        """
    )
    if daily_metrics.empty:
        st.info("暂无离线指标，请先运行 Spark 作业和 ClickHouse 导入脚本。")
    else:
        st.bar_chart(daily_metrics.set_index("metric_date"))

with right_col:
    st.subheader("Bot 与人类账号占比")
    actor_mix = query_frame(
        """
        SELECT
            actor_category,
            sum(event_count) AS total_events
        FROM batch_daily_metrics
        GROUP BY actor_category
        ORDER BY total_events DESC
        """
    )
    if actor_mix.empty:
        st.info("暂无 Bot 行为数据。")
    else:
        st.dataframe(actor_mix, use_container_width=True)

st.subheader("开发者活跃节律")
activity_pattern = query_frame(
    """
    SELECT
        hour_of_day,
        actor_category,
        sum(event_count) AS total_events
    FROM batch_activity_patterns
    GROUP BY hour_of_day, actor_category
    ORDER BY hour_of_day, actor_category
    """
)
if activity_pattern.empty:
    st.info("暂无节律分析数据。")
else:
    pivot_df = activity_pattern.pivot(index="hour_of_day", columns="actor_category", values="total_events")
    st.line_chart(pivot_df)

st.subheader("课程汇报可讲的技术亮点")
st.markdown(
    """
    - `Kafka` 负责承载 GitHub 事件流，支持历史事件重放和可重复演示。
    - `Flink` 链路负责分钟级聚合、热点仓库评分和异常预警。
    - `Spark` 链路负责日级和周级长期统计，与实时结果形成对照。
    - `ClickHouse` 提供统一的分析结果查询能力，支撑大屏和仪表盘。
    - 系统最终同时覆盖了 `实时分析`、`离线分析` 和 `可视化展示` 三个交付要求。
    """
)
