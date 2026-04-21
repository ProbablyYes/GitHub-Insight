# 数据持久化说明

## 简答

**ClickHouse 中的表、Spark 产出的 parquet 文件，全部可以跨电脑重启保留**，
只要你按本项目提供的命令启停（见下）。

---

## 数据在哪里

| 数据 | 宿主位置 | 容器内位置 | 持久化机制 |
|------|----------|------------|------------|
| **ClickHouse 表数据** | Docker 命名卷 `gba_clickhouse_data` | `/var/lib/clickhouse` | Named volume |
| **ClickHouse 日志** | Docker 命名卷 `gba_clickhouse_logs` | `/var/log/clickhouse-server` | Named volume |
| **Spark 产出 parquet** | `./data/sample/` (repo 根下) | — | 直接落到宿主磁盘 |
| **curated parquet** | `./data/curated/` | — | 直接落到宿主磁盘 |
| **原始 GH Archive 下载** | `./data/raw/` | — | 直接落到宿主磁盘 |

---

## 一键启停（日常使用）

**启动** —— 早上/重启电脑后打开项目：

```powershell
scripts\resume.ps1
```

这条命令会：

1. 启动 ClickHouse（如果停着的话）—— 数据自动从命名卷挂载，不会丢。
2. 等 ClickHouse 健康（`/ping` 返回 200）并打印现有表数量。
3. 拉起 Next.js 开发服务器（默认 3000，端口占用则递增）。
4. **不会触发任何数据重建**。

**停止** —— 关机前或切任务时：

```powershell
scripts\stop_all.ps1
```

这条命令会：

1. `docker compose stop` —— 只停容器，不删容器、不删卷。
2. 停掉本项目开的 Next.js 进程。

---

## 什么操作会丢数据

| 操作 | 会丢数据吗？ |
|------|--------------|
| 关闭 Docker Desktop | ❌ 不会 |
| 重启电脑 | ❌ 不会 |
| `docker compose stop` / `scripts\stop_all.ps1` | ❌ 不会 |
| `docker compose restart clickhouse` | ❌ 不会 |
| `docker compose down` | ❌ 容器删了，**卷还在**，下次 up 自动挂回 |
| `docker compose down -v` | ✅ **会丢！** `-v` 表示同时删卷 |
| `docker volume rm gba_clickhouse_data` | ✅ **会丢！** |
| Docker Desktop → Settings → "Clean / Purge data" | ✅ **会丢！** |
| 在 Docker Desktop 的 Volumes 页面手动删除 `gba_clickhouse_*` | ✅ **会丢！** |

---

## 一次性迁移（仅需执行一次）

> **重要**：如果 ClickHouse 容器是在我们加上 named volume 之前就创建的，
> 数据现在住在"容器可写层"里，还没真正进命名卷。下一次容器被重建（比如
> `docker compose up` 检测到 compose 文件变更），数据就会丢。

执行这一条安全地把现有数据挪到命名卷：

```powershell
scripts\migrate_clickhouse_volume.ps1
```

脚本做的事：

1. `docker commit gba-clickhouse gba-clickhouse-pre-migration:snap`  
   —— 先把当前容器整个拍个镜像快照作为保险。
2. `docker compose rm -sf clickhouse` —— 停并删旧容器，**卷还在**。
3. `docker volume create gba_clickhouse_data` —— 确保卷存在。
4. 用一个一次性容器从快照镜像把 `/var/lib/clickhouse/` 整目录
   `cp -a` 到 `gba_clickhouse_data` 卷里（保留 owner/perms）。
5. `docker compose up -d clickhouse` —— 新容器起来、自动挂载有数据的卷。
6. 查一下 `github_analytics` 有多少张表，确认 OK。

脚本是**幂等的**：已经迁过就什么都不做。安全可重跑。

迁移完确认没问题后可以清掉快照镜像：

```powershell
docker image rm gba-clickhouse-pre-migration:snap
```

---

## 恢复保险（如果不小心 `down -v` 了怎么办）

`data/sample/` 下是所有批处理表的 parquet 文件。重新灌库：

```powershell
docker compose up -d clickhouse
python -m scripts.load_batch_metrics_to_clickhouse --input data/sample
```

只要 `data/sample/` 还在，就永远回得来。

---

## 想完整重跑一次批处理（数据更新）

```powershell
scripts\run_batch_pipeline.ps1
```

步骤：

1. curate 原始事件 → parquet。
2. Spark 跑全部离线计算（30+ 张表）。
3. 把 parquet 灌进 ClickHouse（`INSERT`，不会删历史表；若想重跑覆盖，
   `load_batch_metrics_to_clickhouse.py` 内部用 `TRUNCATE TABLE ... ; INSERT`
   的模式）。

---

## 首次启动（全新机器）

```powershell
# 1. 起全部核心服务（Kafka / ClickHouse / MinIO / ZK）
docker compose up -d zookeeper kafka clickhouse minio

# 2. 下载 / 生成样例数据、跑一轮批处理
scripts\run_batch_pipeline.ps1

# 3. 进入日常模式
scripts\resume.ps1
```

之后每天只需 `scripts\resume.ps1` / `scripts\stop_all.ps1`。
