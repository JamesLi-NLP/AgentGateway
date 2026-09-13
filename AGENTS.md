# AGENTS.md — agent-gateway / TurnScheduler 设计文档库

> 本目录是**纯设计文档交付库**（无代码）。目标：基于 `docs/` 下基础文档，重写 Agent Gateway 的 Turn 调度模型（重写模型 + 重写调度）。所有文档与回复使用**中文**。

## 1. 交付物清单（权威文件）

| 文件 | 内容 | 规模 |
|---|---|---|
| `design/turn-scheduler-design.md` | 核心设计文档（数据模型 / 7 阶段调度流水线 / Lease 模型 / 幂等 / 分布式部署） | §4.2 五表 DDL，全篇 SQL |
| `design/turn-scheduler-hld.md` | 概要设计（模块 / 接口 / ER / 部署） | 含 7 图 |
| `design/turn-scheduler-lld.md` | 详细设计（服务 / DDL / 索引 / 时序 / 类图） | 含 9 图 |
| `design/architecture-review.md` | ✅ 架构审阅意见稿（A~D 级问题 16 项 + R1~R11 决策建议）——**已全部采纳**，作为 D17~D27 并入 design.md §19 并落地相关章节；本稿保留作追溯依据（确认后可归档） | 8 节 |
| `design/diagrams/plantuml/turn-scheduler-hld.puml` | HLD PlantUML 源 | 7 图 |
| `design/diagrams/plantuml/turn-scheduler-lld.puml` | LLD PlantUML 源 | 9 图 |
| `design/diagrams/drawio/turn-scheduler-hld.drawio` | HLD drawio 源 | 7 页 |
| `design/diagrams/drawio/turn-scheduler-lld.drawio` | LLD drawio 源 | 9 页 |
| `archive/turn-scheduler-diagrams.drawio` | ⚠️ 早期草稿（2 页），**非权威**，保留仅供追溯，确认后可删除 | — |

## 2. 数据模型现状（方案A：8 表 → 5 表，已完成全量合并）

Gateway 侧 5 张表：

1. **`turn_admission`** — 同步回执落账 + 调度队列合一（吸收原 `pending_turn_queue`）：Gateway 同步调会话管理 submit 拿回执，`(session_id, turn_id, attempt_id)` 幂等落账即入队，`priority`/排队语义由 admission 行自身承载；Phase 2 `SKIP LOCKED` 直接竞争 `status` 为 WAITING/queued 的行；无可用 Runtime 时可置 `suspended` 挂起（12.2.1）。
2. **`runtime_invocation`** — 执行工单 + Runtime 观测（吸收原 `runtime_observed_state`）：`runtime_status / event_sequence / progress_note` 等观测字段只做健康感知与卡死判定，不参与状态机；未 commit 无可用 Runtime 故障置 `paused` 挂起。
3. **`invocation_spec`** — 冻结的 ContextSnapshot 引用（`snapshot_id` + `content_hash`，正文权威在会话管理，决策 D3）。
4. **`lease_info`** — 统一租约表（合并原 `session_lease` + `controller_lease`），`lease_type='session'|'controller'` 区分；唯一键 `unique(lease_type, session_id)` + `unique(lease_type, invocation_id)`；Runtime 侧 fencing token 为运行时内存语义，**不落库**。
5. **`committed_content`** — 确认内容 **Gateway 本地镜像/缓存（非权威，可重建，决策 D2）**，`seq` 去重幂等；权威在会话管理消息表。

## 3. 已确认的关键设计决策（用户拍板，勿推翻）

- **Gateway 是用户唯一入口（永久）**：用户的一切交互（提交 Turn、SSE 订阅、retryTurn、查询）都终止于 Gateway；**会话管理对用户不可见**（它是 Gateway 内部/背后的职能，用户不直接接触）；TurnScheduler 与 Runtime 也从不直接面对用户，只通过**同步提交通道（submit/retry/cancel/commit/update，A1~A12 外部契约）** / Redis Stream / 内部接口协作。
- **对用户屏蔽 attempt**：仅 Gateway 侧处理，前端无感（retryTurn 时新 Attempt 新租约新 Watermark，SSE `stream_reset` 续读）。
- **同步提交通道（决策 D1/D15，废弃 outbox）**：Gateway 同步调会话管理 submit 拿回执，回执幂等落账 `turn_admission`（INBOX 即调度队列）；Attempt 业务记录只由会话管理创建（D7）；幂等键 = `(session_id, turn_id, attempt_id)`，用户防重放前移 Gateway 对外 API。
- **分布式约束**：Spring Boot 部署于 K8s 多 Pod；队列竞争用 PG `FOR UPDATE SKIP LOCKED`（不引入外部队列）；安全接管靠三类 Lease（Session 串行执行权 / Controller 控制权 `controller_epoch` 递增 / Runtime fencing token）。
- **基础可用组件**：Nacos（注册发现）、Redis（Stream 做 Delta 体验层，可丢）、PostgreSQL（权威存储）、K8s。
- **宁可慢不可双活**：提前接管必须以 fencing 授权为前提；Delta 可重建，权威在会话管理消息表（Gateway 侧 `committed_content` 仅镜像）。

## 4. 文档链风格差异（已知且有意保留，勿“统一”）

| 维度 | design.md | lld.md |
|---|---|---|
| 状态命名 | 小写 `queued/activating/...` | 大写 `WAITING/ACTIVATING/...` |
| id 风格 | `id bigint` + `invocation_id varchar(128)` | `bigserial` / `uuid` |
| 租约时长 | lease 120s / heartbeat 30s | session 30s / controller 60s |
| 幂等键 | `(session_id, turn_id, attempt_id)`（同步回执，D15；已删 client_app_id/request_id） | `(session_id, turn_id, attempt_id)`（同步回执，D15；`outbox_id` 已随 v2 同步删除） |

> lld.md / hld.md（含 puml/drawio 图源）**已随 v2 统一会话管理改造同步**：同步提交通道（D1/D15）全面落地，turn_ready_outbox 表/claim/ack/OutboxConsumer 均已移除（OutboxConsumer → SessionManagementClient），残留的 turn_ready_outbox 字样仅以"替代原/已废弃"说明性文字出现。

- 全库 grep 到 `吸收原 X` / `表合并说明（方案A）` / `替代原 turn_ready_outbox` 字样是**有意的改造说明**，不是残留错误。
- 跨文档 DDL 逐字统一是**可选项**，未被要求过；若做需整表逐列对齐，工作量独立。

## 5. 环境限制

- 无 drawio CLI、无 plantuml.jar、无渲染能力 → **只交付源文件**；drawio 的 value 用纯文本、换行写 `&#xa;`。
- 多轮委托子 agent（task）均失败（ProviderModelNotFoundError / 无输出），**方案A 合并全部在主会话内完成**；后续编辑优先主会话直接做。
- drawio 视觉规范参考：`C:\Users\jingbiao.li\.agents\skills\drawio-chart\references\style-spec.md` 与 `xml-and-layout.md`。

## 6. 验证 Playbook（改表结构/表名后必跑）

```powershell
# 1) 旧表名残留（大小写变体都要查；仅允许"吸收原 X"说明性文字）
#    grep pattern: pending_turn_queue|session_lease|controller_lease|runtime_observed_state|
#                  PendingTurnQueue|SessionLease|ControllerLease|RuntimeObservedState|
#                  PENDING_TURN_QUEUE|SESSION_LEASE|CONTROLLER_LEASE|RUNTIME_OBSERVED_STATE|8 张|8张
#    范围: design/turn-scheduler-*.md + design/diagrams/**/*.puml + design/diagrams/**/*.drawio

# 2) drawio XML 合法性（PowerShell）
[xml]$x = Get-Content -LiteralPath "design\diagrams\drawio\turn-scheduler-hld.drawio" -Encoding UTF8 -Raw  # hld=7 页
[xml]$x = Get-Content -LiteralPath "design\diagrams\drawio\turn-scheduler-lld.drawio" -Encoding UTF8 -Raw  # lld=9 页

# 3) drawio 悬空引用：grep 已删节点 id（如 p5-n4|p6-queue|p6-slea|p6-clea|p6-ros）

# 4) puml 配对：@startuml 与 @enduml 各应为 16（lld 9 + hld 7）

# 5) 编号连续：design.md §4.2.1~4.2.5；lld.md §3.1.1~3.1.5（5 表；原 3.1.1 turn_ready_outbox 整节已删，后续小节顺延）
```

## 7. 渲染命令（用户本机有环境时）

```bash
java -jar plantuml.jar design/diagrams/plantuml/turn-scheduler-hld.puml
java -jar plantuml.jar design/diagrams/plantuml/turn-scheduler-lld.puml
# drawio: draw.io desktop 打开导出 PNG/SVG/PDF
```

## 8. 容量与参数假设（写文档时引用）

745 会话、稳态 20~50 turns/s、峰值 <150；心跳/轮询节奏见 design.md §16（写放大：心跳 ×2 行同表）；同步提交通道 RPC = 峰值 turns/s × 每 Turn 2 RPC（submit + update），~300 RPC/s；INFRA_RETRY 写放大按重试率 ~5% 计（design.md §16.3，决策 D16）。

## 9. 开放事项

- [ ] 可选：三文档 DDL 细节跨文档统一（见 §4，未被要求；lld.md/hld.md 已随 v2 同步提交通道改造完成，风格差异仍按 §4 有意保留）。
- [ ] `archive/turn-scheduler-diagrams.drawio` 早期草稿确认删除（保留仅供追溯）。
