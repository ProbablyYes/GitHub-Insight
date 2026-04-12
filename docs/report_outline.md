# 项目报告提纲

## 1. Introduction

- 课程背景：大数据处理课程要求设计并实现一个完整的大数据应用系统。
- 研究动机：GitHub 公开事件能够反映开源生态活跃度、协作行为和热点变化。
- 项目意义：通过流批分析系统展示大数据采集、处理、存储和展示的完整能力。

## 2. Group Members And Task Assignment

- 成员 A：数据下载与回放
- 成员 B：Kafka 与流处理
- 成员 C：Spark 离线分析
- 成员 D：ClickHouse / Superset / Streamlit
- 成员 E：报告、PPT、答辩组织

## 3. System Design

- 总体架构图
- 数据流说明
- 模块划分
- 实时链路与离线链路的职责划分

## 4. Platform And Tools

- `GH Archive`
- `Kafka`
- `Flink`
- `Spark`
- `MinIO`
- `ClickHouse`
- `Superset`
- `Streamlit`

## 5. Implementation

- 数据下载与事件过滤
- Kafka 回放设计
- 实时窗口聚合
- 热点仓库评分方法
- 异常检测方法
- Spark 批处理逻辑
- ClickHouse 表设计
- 展示页面实现

## 6. Results And Demonstration

- 实时事件量趋势
- 热门仓库榜单
- 异常预警列表
- 日级趋势图
- Bot 与人类账号占比
- 开发者活跃节律图

## 7. Difficulties And Solutions

- 原始数据体量大，需控制在 GB 级
- GitHub 事件字段较复杂，需要统一 schema
- Flink 环境本地启动较重，因此提供轻量 Python 实时链路作为演示降级方案
- Superset 初始化较慢，因此提供 Streamlit 作为快速展示入口

## 8. Conclusion

- 系统完成了流处理、批处理和统一展示
- 证明了 GitHub 公开数据适合做大数据课程项目
- 后续可扩展真实实时 API、更多事件类型和更复杂模型
