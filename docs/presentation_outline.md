# 答辩展示提纲

## 1. 开场

- 项目名称：GitHub 开发者行为流批分析 + 数据挖掘系统
- 课程目标：在真实公开数据上综合展示**大数据处理**、**机器学习**、**统计推断**、**图算法**四类技术
- 一句话定位：不是一个"列榜单"的 BI 报表，而是一个**能解释"为什么"的可交互分析平台**

## 2. 为什么选这个题目

- GitHub 数据公开、规模大、天然适合形成 GB 级数据集
- 既可以做**流式实时分析**（事件入库即反应），也可以做**离线深度挖掘**
- 结果可视化强，适合课堂演示与答辩

## 3. 系统架构 (总览一页)

- **采集层**：GH Archive JSON → curate_events（Spark，Parquet 分区为 `event_date=/event_hour=`）
- **总线层**：Kafka 事件主题 + Schema 约束
- **实时层**：Flink（正式）/ 轻量 Python 消费者（备用）→ 分钟级指标 → ClickHouse
- **离线层**：Spark 3 批处理 + Spark MLlib → ClickHouse 40+ 张分析表
- **存储层**：ClickHouse（OLAP）+ MinIO（对象存储）
- **展示层**：Next.js + Recharts + 像素风设计系统（NES.css + Zpix 字体）

## 4. 前端：问题驱动的 L0→L5 分层页

核心原则：**每个页面回答一个具体问题**，而不是把所有指标堆在一起。

- **L0 Ecosystem**：生态整体在发生什么？（KPI hero、变点旗标、Gini/熵 WoW 变化）
- **L2 Repos**：哪些仓库火，为什么火？（Forest Plot 归因、DNA 指纹、反例清单）
- **L3 Watcher Profile**：谁在围观热仓？（每仓的 persona 分布 + lift）
- **L4 People**：开发者人群长什么样？（PCA scatter、BIC 曲线、Bot 验证、transition 矩阵）
- **L5 Network**：仓库是如何结群的？（Jaccard 图、加权 LPA 社区、画像、规则 Pareto）
- **ML Lab**：技术 showcase（StandardScaler + KMeans + PCA + GMM + FPGrowth 的参数与解释）

每张图都带 `Tech badge`（用了什么算法）和 `How to read`（如何解读）。
每张表都可以 **CSV 导出** 当前视图（排序/过滤后的结果）。

## 5. 大数据 & 机器学习 技术落地（按层展开）

### 5.1 L0 Ecosystem — 生态级时序

- **Gini 系数 & 洛伦兹曲线**：衡量热度集中度，不再只看 Top-5 占比。
- **归一化熵**：`H / log2(N)`，与仓库数量解耦的多样性度量。
- **WoW 变化率**：`lag(7)` 窗口函数，把"今天 vs 上周今天"直接给出。
- **CUSUM 变点检测**：累积偏差 + z-score 标记生态层 burst/drop 变点，并归因到贡献最大的事件类型。

### 5.2 L1 Clustering — 仓库分群

- 15 维特征 → `StandardScaler` → `KMeans(k=6)` → `PCA(2)` 可视化
- `repo_cluster_profile`：每个 cluster 的均值特征解释，所以读者知道每类是"watcher 集中型"还是"PR 驱动型"

### 5.3 L2 Attribution — 热 vs 冷对比

- **Welch's t-test** 做均值差检验
- **Cohen's d** 量化效应量（不被样本数迷惑）
- **Bootstrap 200 次重采样**给出 Cohen's d 的 **95% 置信区间** → Forest Plot
- 三种 `cohort_scope`：All / Humans only / Bot-heavy → 剔除 bot 噪声的"干净" attribution
- **ChiSquareTest + Lift** 分析离散特征
- **DNA outliers**：Mahalanobis-like z-distance 找"虽然也是热仓但 DNA 不合群"的反例

### 5.4 L3 Watcher Profile — 热仓的围观者画像

- `repo_watcher_persona_lift`: `P(persona | watchers of repo) / P(persona | all actors)` → lift
- `repo_watcher_profile`: 每个热仓的 dominant watcher persona + lift + share

### 5.5 L4 People — 用户画像

- **GaussianMixture(k=6)** 软聚类，每个 actor 有概率属于多个 persona
- **BIC 扫描 (k=3..8)** 做模型选择（lower BIC = better），前端用肘形图展示证据
- **Bot validation**：per-persona precision / recall / F1 vs ground-truth `actor_category='bot'` → 证明 `bot_fleet` persona 真的捕捉到 bot
- **Persona transition matrix**：将事件时间窗一分为二，复用同一 scaler + GMM 计算早/晚两段 persona，统计 `P(late | early)`；对角线=稳定，非对角=漂移

### 5.6 L5 Network — 仓库协同网络

- **Jaccard 相似度**：`|A∩B|/|A∪B|`，基于共同 actor
- **加权 Label Propagation（LPA）** 做社区检测：每个 repo 迭代采纳"邻居加权 Jaccard 最大"的社区标签，比 min-label CC 产生更有意义的多社区分布
- **社区画像**：per-community watch_share / pr_push_ratio / bot_ratio + top-3 成员 → 把社区贴上"watcher-led / contributor-led / bot-driven"标签
- **FPGrowth 关联规则**：把每个 actor 当作购物篮，挖 repo 之间的共现模式
- **Pareto 前沿过滤**：在 (support, confidence, lift) 三维空间保留非支配规则，前端用散点图高亮

## 6. 工程与性能难点

- **OOM / Lineage 爆炸**：L5 迭代图算法 → 引入 `localCheckpoint(eager=True)` 截断血缘 + 早停 + 加权 LPA 降迭代次数
- **驱动内存调优**：`--driver-memory 5g --conf spark.sql.shuffle.partitions=24`
- **桌面环境**：Windows 上无 `winutils.exe` → 全部打进 Docker 容器（`spark-batch` 服务），一键 `docker compose run` 复现
- **ClickHouse Reserved Word 冲突**（`group`）→ 全链路重命名为 `cohort_group`
- **前端 hydration mismatch**：某些浏览器插件（tablecapture）在 `<table>` 注入属性 → 加 `suppressHydrationWarning`

## 7. 创新点 / 亮点

- 不是"列数据"而是"解释数据"：每个指标都带**统计证据**（CI / p-value / lift / BIC）
- 全栈可追溯：前端的每张图都能在 `Source` 里看到背后的 ClickHouse 表 + 用了哪个 Spark/ML 算法
- 完整一条链：从 Parquet 行级别 → Spark 特征 → ML 模型 → ClickHouse 结果表 → Next.js 可交互页
- 历史事件回放模拟实时分析，同一套前端能展示流处理 + 批处理结果

## 8. 总结

- 项目同时覆盖了课程强调的 **大数据存储 / 流处理 / 批处理 / ML / OLAP / 可视化** 六大板块
- 与普通 BI 项目的差异：**强调从"描述"升级到"解释"**，每个结论都有统计/算法证据
- 可扩展方向：MinHashLSH（百万级相似）、GraphFrames 的 Louvain、Flink CEP 规则告警

