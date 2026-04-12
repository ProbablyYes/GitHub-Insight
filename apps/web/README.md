# GitHub Insight Web Dashboard

这个目录承载课程项目的正式前端仪表盘，技术栈为 `Next.js + Tailwind CSS + Recharts`。

## 本地启动

```bash
npm install
npm run dev
```

默认访问地址：

- [http://localhost:3000](http://localhost:3000)

## 环境变量

复制 `.env.example` 为 `.env.local`，或者直接在 shell 中注入：

```bash
CLICKHOUSE_URL=http://127.0.0.1:8123
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=analytics
CLICKHOUSE_DATABASE=github_analytics
```

## 页面内容

当前首页包含：

- 实时事件量趋势
- 热门仓库榜单
- 异常活跃预警
- 离线日级趋势
- Bot 与人类账号占比
- 开发者活跃节律

## 容器运行

仓库根目录已提供 `docker-compose.yml` 中的 `web` 服务，也可以直接在项目根执行：

```bash
docker compose up -d web
```
