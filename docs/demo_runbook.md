# 演示与交付 Runbook

## 1. 目标

保证课程答辩时系统可以稳定演示，并且出现问题时有降级方案。

## 2. 推荐演示顺序

1. 介绍系统架构图和技术栈。
2. 展示原始 GH Archive 数据文件。
3. 启动 Kafka、ClickHouse、MinIO、Superset 等基础服务。
4. 启动轻量实时消费者，等待 Kafka 数据入库。
5. 启动事件回放脚本，将历史事件写入 Kafka。
6. 运行 Spark 离线作业，导入 ClickHouse。
7. 打开 `Next.js` 仪表盘，展示实时趋势、热门仓库、异常预警和离线画像。
8. 展示总览页、实时页、离线页的分区结构和后续扩展位。
9. 总结流批结合的价值和课程收获。

## 3. 正常运行命令

### 启动基础服务

```bash
docker compose up -d zookeeper kafka clickhouse minio superset
```

如需把正式前端一起容器化：

```bash
docker compose up -d zookeeper kafka clickhouse minio superset web
```

### 下载数据

```bash
python jobs/ingest/download_gharchive.py --date 2024-01-01 --hours 0 1 2 --output-dir data/raw --upload-minio
```

也可以直接使用仓库里的固定样例：

```bash
powershell -ExecutionPolicy Bypass -File scripts/bootstrap_sample_data.ps1
```

### 回放事件

```bash
python jobs/replay/replay_gharchive_to_kafka.py --input data/raw --topic github_events --speedup 600
```

### 流式处理

```bash
python jobs/streaming/flink_job.py
```

说明：

- 当前课堂演示默认使用轻量 Python 实时消费者。
- `jobs/streaming/flink_sql_job.sql` 代表后续可接入的真实 `Flink SQL` 方案。

### 离线处理

```bash
python jobs/batch/spark_job.py --input data/raw --output data/sample
python scripts/load_batch_metrics_to_clickhouse.py --input data/sample
```

### 展示页面

正式仪表盘：

```bash
npm run dev --prefix apps/web
```

### 一键运行

默认启动正式前端：

```bash
powershell -ExecutionPolicy Bypass -File scripts/run_demo_pipeline.ps1
```

统一启动开发联调链路：

```bash
powershell -ExecutionPolicy Bypass -File scripts/start_all.ps1
```

## 4. 答辩时的讲解重点

- 为什么选 GitHub 公开事件数据。
- 为什么需要同时做实时分析和离线分析。
- Kafka、轻量实时消费者、Spark 在系统中的分工。
- 热门仓库、异常预警、Bot 行为分析分别说明了什么。
- 为什么展示层统一采用 `Next.js`，减少双入口维护成本。

## 5. 风险与降级方案

### 风险 1：Kafka 或流处理链路异常

- 降级方案：只展示已保存的离线结果和 ClickHouse 查询结果。

### 风险 2：Flink 环境较重，现场不好启动

- 降级方案：使用当前项目中的轻量 Python 流式消费者演示实时链路逻辑。
- 答辩时说明正式设计使用 Flink，本地演示为轻量实现。

### 风险 3：Next.js 页面未成功连接 ClickHouse

- 降级方案：直接展示 `ClickHouse` SQL 查询结果截图和录屏。

### 风险 4：Superset 初始化慢

- 降级方案：优先使用 `Next.js` 展示核心指标。

### 风险 5：现场网络不稳定

- 降级方案：提前准备下载好的样例数据和演示录屏。

## 6. 小组分工建议

- 成员 A：数据下载、过滤和回放
- 成员 B：Kafka 与实时处理
- 成员 C：Spark 离线分析
- 成员 D：ClickHouse、Next.js、Superset
- 成员 E：联调、验收、报告与答辩材料
