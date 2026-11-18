# Build Artifact Cache

CI 构建产物缓存服务：FastAPI + SQLite + 本地目录。解决"清理构建产物缓存时，
运行中的构建可能仍在下载旧产物"的问题——下载必须持有**限时占用令牌（租约）**，
回收只触碰**未被占用**的产物，且按**最久未用（LRU）**优先。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （Swagger UI，可直接操作全部接口）
```

本地开发：

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload        # http://127.0.0.1:8000/docs
.venv/bin/pytest                               # 运行测试
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `PUT` | `/artifacts/{number}?sha256=<hex>` | 上传/重传产物（≤ 1 MiB，校验 SHA-256，不匹配 400，超限 413） |
| `GET` | `/artifacts` / `/artifacts/{number}` | 列表 / 元数据（`downloadable` 仅在记录与文件一致且就绪时为真） |
| `POST` | `/artifacts/{number}/tokens` | 领取限时占用令牌（回收中的产物 409 拒发） |
| `GET` | `/artifacts/{number}/content?token=` | 凭令牌下载（无令牌 401，无效/过期/已释放 403，代际不符 409） |
| `POST` | `/tokens/{token}/renew` | 续期一个 TTL（已过期/已释放/产物变更均 409） |
| `POST` | `/tokens/{token}/release` | 完成后释放（幂等） |
| `POST` | `/reclaim` | 输入 `target_bytes`，LRU 优先回收未占用产物，返回实际 `freed_bytes` |
| `POST` | `/admin/sweep` | 手动触发恢复清扫（中断的回收、孤儿文件） |
| `GET` | `/stats` / `/health` | 统计 / 健康检查 |

配置（环境变量）：`TOKEN_TTL_SECONDS`（默认 300）、`MAX_UPLOAD_BYTES`（默认
1048576）、`CACHE_DB_PATH`、`CACHE_STORAGE_DIR`、`SWEEP_INTERVAL_SECONDS`（后台
清扫周期，0 关闭）、`ORPHAN_GRACE_SECONDS`（孤儿文件宽限期）。

## 关键设计

- **租约占用**：令牌绑定 `(number, generation)`，限时有效；回收候选必须
  `NOT EXISTS` 活跃令牌，因此**占用中的产物绝不会被删**，空间不足时只返回
  实际能释放的字节数，不强行误删。
- **代际（generation）**：每次上传 generation +1 且文件落**唯一新路径**
  （`<number>/<gen>-<uuid>.bin`）。旧令牌读不到同号新产物（409）；针对旧代际
  的待删除操作由条件 SQL（`WHERE generation=? AND state='reclaiming'`）保护，
  **不会误删重传的文件**。
- **两段式回收**：先条件 `UPDATE ... SET state='reclaiming'`（原子复查"无活跃
  令牌"，此后该产物拒发新令牌、拒绝续期），再删文件，最后条件删行。
- **失败与重启恢复**：删文件失败 → 行保持 `reclaiming`，由下次回收 / 后台清扫
  / **启动清扫**重试；孤儿文件（无记录引用且超过宽限期）由清扫删除；行在而文件
  缺失 → `downloadable=false`，下载 409，**绝不声称可下载**；下载前逐字节复核
  SHA-256，损坏即 409。
- **并发模型**：单进程单 SQLite 连接 + 全局锁串行化全部操作，领取/续期/回收的
  交错在进程内无竞态（WAL、显式 `BEGIN IMMEDIATE` 保证崩溃原子性）。多副本
  水平扩展不在本设计范围内。

## 已验证

- 21 项功能测试 + 1 项真实 uvicorn 下的 8 线程并发交错测试（上传/下载/续期/
  释放/回收混合压力，断言无 5xx、下载内容哈希一致、被持续占用的产物存活）。
- 失败情形：删文件失败继续清理其余并可恢复、进程重启后完成中断的回收、
  文件缺失不声称可下载、重传文件不被误删、孤儿文件宽限后清扫。
- 冒烟：uvicorn 直接启动、`/docs` 与 `/openapi.json` 可用、重启后数据存活。

限制：本沙箱无 Docker，`docker compose up` 未实际执行；Compose 使用的命令
（`uvicorn app.main:app`）与依赖已在本地逐一验证。
