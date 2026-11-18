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
| `GET` | `/availability/changes?cursor=&limit=` | **可用性变更游标流**：返回 `seq > cursor` 的事件，按提交顺序、无漏无重，可断点续领 |
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

## 可用性变更流（游标续领）

调度器离线后可能重传产物或回收空间，待执行构建会引用已失效代际。
`GET /availability/changes` 用只追加表 `availability_changes` 暴露每个
`(number, generation)` 的状态变迁，`seq` 为自增主键、即单调游标：

| 状态 | 含义 | 何时写入（与状态变更同一事务提交） |
|---|---|---|
| `available` | 该代际成功上传、可用 | 每次 `PUT /artifacts` |
| `invalidated` | 该代际已失效，构建不应再引用 | 同号重传取代旧代际；或回收被选中（行翻转为 `reclaiming`） |
| `released` | 文件已实际从磁盘移除、空间释放 | 仅在删盘成功（或确认本就不存在）并条件删行后 |

关键保证：

- **同次提交**：同号重传在一个事务内写入「旧代 `invalidated` + 新代
  `available`」，外部不可能只观察到其中一条。
- **失败不谎报**：删文件失败时行保持 `reclaiming`，**绝不写 `released`**；
  由下次回收 / 后台清扫 / **重启启动清扫**重试，成功后才补写。
- **不漏不重**：`released` 带 `NOT EXISTS` 幂等守卫，同一代际最多一条；
  重启清扫补全时不会漏记也不会重复。
- **迟到清扫不覆盖新代际**：旧代际的待删条件为
  `WHERE number=? AND generation=? AND state='reclaiming'`，重传使该条件命中
  0 行，新代际的行、文件与 `available` 事件均不受影响，也不会给旧代际误记释放。
- **代际不复用**：即使某产物号被整体回收（行删除），重传仍从事件表取高水位
  代际 +1，保证 `(number, generation)` 全局唯一，调度器能区分同名新旧代际。
- **续领语义**：返回 `{changes, next_cursor, has_more}`，客户端把
  `next_cursor` 作为下次 `cursor` 即可从下一提交精确续读；无新事件时
  `next_cursor` 等于入参游标（稳定不空转）。`limit ∈ [1,1000]`，非法值 422。

## 已验证

- 22 项原有功能测试 + 16 项可用性变更流测试 + 1 项变更流真实 uvicorn 并发测试
  （上传/重传/领令牌/回收/多页大小游标轮询混合压力，断言 `seq` 为无漏无重的
  1..N、每代际生命周期合法、无 5xx），合计 39 项全部通过；原 8 线程并发交错
  测试同样通过，下载与租约行为保持不变。
- 失败情形：删文件失败继续清理其余并可恢复（且失败期间不产生 `released`）、
  进程重启后完成中断的回收并补齐唯一一条 `released`、文件缺失不声称可下载、
  重传文件不被误删、迟到旧代际清扫不影响新代际事件、孤儿文件宽限后清扫。
- 真实进程冒烟（独立 uvicorn，非 TestClient）：完整走查上传/重传同事务事件、
  小页游标翻页、强制删盘失败→修复→清扫补全、整体回收后代际不复用、租约下载与
  旧令牌 409、`/docs` 与 `/openapi.json` 可用；并实测 kill 进程后以同一数据
  目录重启，启动清扫自动补全事件流、无漏重。
- 冒烟：uvicorn 直接启动、`/docs` 与 `/openapi.json` 可用、重启后数据存活。

限制：本沙箱无 Docker 守护进程，`docker compose up` 未实际执行；Compose 使用的
命令（`uvicorn app.main:app`）与依赖已在本地逐一验证，Dockerfile/compose 配置
未改动，沿用既有可启动结构。
