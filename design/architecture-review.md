# TurnScheduler v2 架构审阅意见（architecture-review）

> 审阅对象：`design/turn-scheduler-design.md`（v2.0，1766 行）
> 审阅视角：系统架构师（正确性 / 协议闭合 / 运维闭环）
> 审阅日期：2026-09-13
> 性质：**意见稿，非权威设计**——供修订 design.md 前逐条裁决；采纳后并入 §19 决策记录或归档
>
> **采纳状态（已裁决）：R1~R11 建议 + C3 已全部采纳，作为 D17~D27 并入 design.md §19；A/B/C 类问题对应修订已落地 design.md（见 §8 各条"待修订位置"）。本稿保留作追溯依据；如需归档请另行确认。**

---

## 1. 审阅结论摘要

| 维度 | 评价 |
|---|---|
| 核心骨架 | 扎实：同步提交通道（D1/D15）、commit 分界重试（D9~D12）、三类 Lease、Gateway/会话管理双权威分离清晰 |
| 分布式互斥 | 正确：SKIP LOCKED + 唯一键 CAS + epoch/fencing 兜底，"判死 ≠ 接管"红线正确且反复强调 |
| 薄弱接缝 | 三处：① 终态回写路径（对会话管理）② Runtime 事件与凭证协议（对 Runtime）③ 人工干预与配额（对用户/运维） |
| 总体判定 | 骨架可进入修订闭环；A 级 5 项建议在 v1.1 前消除，B 级 4 项必须闭合协议后才可开发 Runtime 对接 |

问题分级统计：**A 级（设计缺陷）5 项、B 级（协议空洞）4 项、C 级（容量/运维）3 项、D 级（次要）4 项**，共 16 项。

---

## 2. 问题总览矩阵

| 级别 | # | 主题 | 位置 | 影响 | 建议动作 |
|---|---|---|---|---|---|
| A | A1 | 终态 update 内嵌 DB 事务（远程 RPC 夹在本地事务中） | §6.8 / §12 | 双写窗口扩大、分歧状态无法对齐（会话管理可能永久 RUNNING） | update 移出事务 + 巡检补发终态 |
| A | A2 | 接管后旧 Worker 的 commit 无 fencing 隔离 | §6.7 / §4.2.5 / §4.1.1 | 镜像污染、会话管理接受旧 Attempt 滞留写入 | commit 携带 epoch + 活跃 attempt 校验 |
| A | A3 | "活动但永不结束"无强制超时 | §15.6.3（判据 2）/ §9.2 | 死循环 Agent Loop 永不收敛、资源白占 | 巡检按 spec.deadline 判据 4 |
| A | A4 | "可人工干预"目标未落地 | §1.1（承诺）/ §18 | 无提权、强制失败、重排入口，运维裸奔 | admin 运维接口 + 干预审计 |
| A | A5 | USER_RETRY 与 INFRA_RETRY 配额相互消耗 | §4.1.1（attempt_number）/ §12.3 | 基础设施抖动耗尽用户重试机会 | 配额分列计数 |
| B | B1 | Runtime 事件推送目标寻址未定义（接管后 SSE 推给谁） | §11.2 / §12.1 | 接管后事件断流、无法续读 | 明确推/拉模型或 holder 发现协议 |
| B | B2 | fencing token 生命周期未定义 | §7.3 | 慢执行 token 过期语义不清 | token 有效期 = Invocation 生命周期 + 代际号校验 |
| B | B3 | 孤儿快照无 GC（Phase 3 成功 / Phase 4 回滚） | §6.4 | 会话管理侧快照正文泄漏 | 快照引用计数 / 终态延迟清理 |
| B | B4 | L1 提前接管缺 K8s 节点级证据的获取组件 | §12.1 | L1 承诺 10~30s 无兑现路径 | EventWatcher 或注明降级为纯 Nacos 线索 |
| C | C1 | 终态行无归档清理策略 | §16.3（只有分区表） | 全年数十万行、查询与 vacuum 恶化 | 归档 Job + 保留周期 |
| C | C2 | 监控指标缺口 | §17.1 | 无 TTFT、Runtime 池健康、fencing 冲突、状态分歧 | 补指标 |
| C | C3 | F4 判定过度依赖 Nacos 瞬时视图 | §12 矩阵 F4 / §6.6 | 网络分区误挂起健康任务 | 加任务级证据（连续派发失败） |
| D | D1 | Admission 状态机缺 mermaid 图 | §5 | 核心队列状态机反而没有图 | 补 §5 新图 |
| D | D2 | 测试策略仅压测，无竞争/接管专项 | §16.7 | 双 Pod 同秒接管等风险无测试设计 | 补专项测试清单 |
| D | D3 | 无数据库迁移工具 | 全文 | 表结构变更无 Flyway/Liquibase 依托 | 补迁移策略 |
| D | D4 | oversubscription 资源检查无指标来源 | §14.5 | "本机 CPU/内存水位"来源未定义 | 明确 /actuator 端点 |

---

## 3. A 级：设计缺陷（建议 v1.1 前消除）

### A1. 终态 update 内嵌 DB 事务（远程 RPC 夹在本地事务中）

**现状**（§6.8）：Phase 7 在单个本地事务内依次执行——校验 Controller Lease → `markCompleted` → **调会话管理 update（远程 RPC）** → 释放 Session/Controller 租约 → COMMIT。

**问题**：
1. 事务持有行锁的时间 = RPC 网络不确定时长；update 超时/失败 → 整个事务回滚重来；
2. 逆向窗口：update 已在会话管理生效（Turn→COMPLETED），但本地 COMMIT 失败 → Gateway 侧 `runtime_invocation` 残留 `executing`、租约未释放 → **两库永久分歧**；
3. §12.1 接管 SQL 只选 `status in ('dispatched','executing')` 的 Invocation——一个 `status='completed'` 但终态未送达的行**不会被任何接管流程拾取**，会话管理 Turn 永久卡 RUNNING。

**建议**：
- 本地事务只落终态 + 标记 `sync_pending`（或复用 `fail_reason` 溯源字段语义），COMMIT **之后**异步调 update；
- RecoveryCoordinator 巡检新增判据 4：`status='completed'` 但未确认对齐会话管理的行 → 补发 update（幂等），直至对齐；
- 故障矩阵（§12）同步补一行 F9：终态回写失败。

### A2. 接管后旧 Worker 的 commit 无 fencing 隔离

**现状**（§6.7）：`insert into committed_content ... on conflict (invocation_id, committed_sequence) do nothing` **无任何 epoch/attempt 校验**；§4.1.1 commit 契约也未声明按活跃 attempt 过滤。

**问题**：旧 Pod 被接管（12.1）后，旧 Worker 可能仍存活并继续推送 committed 内容：
- `committed_content` 镜像被旧 Attempt 滞留写入污染；
- 会话管理 commit 按 message_id 幂等 → **权威侧也接受旧 Attempt 的写入**，等于绕过了"Attempt 终态后不可变"（D7/D5）。

**建议**：
- commit 请求必须携带 `attempt_id + controller_epoch`，Gateway 与会话管理双侧校验"attempt 为当前活跃 attempt 且 epoch 匹配当前 holder"；
- 被拒写入记为指标（fencing 冲突计数，见 C2）并丢弃。

### A3. "活动但永不结束"无强制超时

**现状**（§15.6.3）：判据 2 只测"事件水位停滞 5 分钟"（`updated_at` 阈值）；`execution_limits.deadline`（§9.2）只是 InvocationSpec 字段，无强制执行者。

**问题**：Agent Loop 死循环（不停收发事件、但永不产出终态）**推高 updated_at，永不命中判据 2** → 资源白占、Session 串行被卡死、容量被拖垮。

**建议**：巡检新增判据 4：`executing` 且 `created_at + spec.deadline < now()` → 按 commit 分界走 12.2 自动重试；或显式声明"由 Runtime 自觉执行 deadline + Runtime 侧 kill 契约"——二选一必须写明（推荐前者，Gateway 侧裁决与"判死≠接管"哲学一致）。

### A4. "可人工干预"目标未落地

**现状**：§1.1 目标明确承诺"调度状态可查询、可审计、可人工干预"，但全文无任何人工干预路径：
- `activation_priority` 字段存在（4.2.1）但**无设置/调整入口**；
- 无管理员"强制失败 / 重排 / 终止挂起任务"接口；
- 无干预审计轨迹。

**问题**：745 人规模一上线，运维对"卡死的 failed 前任务 / 误挂起 / 优先级调整"只能改库，违背目标且危险。

**建议**：补 admin 运维接口（提优先级、强制 failed、唤醒/重排 admission），每次干预落 `audit` 轨迹（干预者、动作、时间、对象）；与 §18 评审项 9（全局公平）联动。

### A5. USER_RETRY 与 INFRA_RETRY 配额相互消耗

**现状**：两种 retry 都走会话管理 `retry` 创建新 Attempt（`attempt_number+1`，§4.1.1）；§12.3 的"INFRA_RETRY 总次数达上限 N=3 → Turn failed，此后才允许用户重试"。

**问题**：若重试上限按 attempt_number 计算，**系统性基础设施故障（A2/A3 反复触发 INFRA_RETRY 3 次）会直接把用户的可用重试机会耗尽**——用户在一次与自己无关的故障后失去 ALL 修复机会。

**建议**：配额分列计数——`infra_retry_count` 独立字段（上限 3），USER_RETRY 配额单独维护；基础设施故障不得消耗用户配额。

---

## 4. B 级：协议空洞（开发 Runtime 对接前必须闭合）

| # | 空洞 | 具体缺口 | 建议 |
|---|---|---|---|
| B1 | Runtime 事件推送目标寻址 | §11.2 定义事件为 Runtime→Gateway 的 SSE；接管（12.1）后旧 SSE 断连，**事件该推给哪个新 Pod**未定义（新 Pod"重新构建连接"只是 Gateway 侧视角） | 二选一写明：① 推模式——Runtime 按 lease holder 发现（查 Nacos holder_pod 的 ip:port）重新建立 SSE；② 拉模式——改为 Gateway 主动长连/轮询拉取事件，天然跟 holder 走（推荐，免除 Runtime 端发现逻辑） |
| B2 | fencing token 生命周期 | §7.3 仅"Runtime 内部管理"一句，无有效期/续期机制；与 lease_duration=120s（§7.4）关系不明；慢执行（单次 LLM >120s）时 token 是否提前失效未答 | token 有效期 = Invocation 生命周期（不按固定 duration 过期），代际 = `controller_epoch`（每次接管递增即失效旧 token）；拒写事件计入 fencing 冲突指标 |
| B3 | 孤儿快照 GC | §6.4 Phase 3 存正文成功、Phase 4 本地事务回滚 → 会话管理侧 snapshot 正文**无引用方**，无清理机制 | 快照接口返回引用状态 + 按 Attempt 终态延迟清理（如终态后 TTL 保留再删），或 Attempt 终态时显式通知回收 |
| B4 | L1 提前接管缺 K8s 节点级证据组件 | §12.1 L1 承诺"Nacos 心跳超时 + K8s 节点级证据（Pod 被驱逐 / 节点 NotReady）→ 10~30s 接管"，但全文无任何组件读取 K8s 事件 | 补 EventWatcher/Informer 组件订阅 Pod 驱逐与节点状态；或明确"v1.0 不实现 L1，接管一律走 L2 租约过期"（保守但自洽） |

---

## 5. C 级：容量与运维缺口

| # | 缺口 | 现状 | 建议 |
|---|---|---|---|
| C1 | 终态数据生命周期未定义（"直接删 vs 转历史表"一刀切） | §4.2 五表 / §16.3 | 直接删破坏 `turn_admission` 幂等台账（重放防护失效）；全留活表膨胀、审计无分层 | 按表职责分层（见下）：admission 冷热分层 + 回执带终态；invocation/spec 转历史；镜像可删；lease 保持现状 |
| C2 | 监控指标缺口 | §17.1 无以下关键指标：TTFT/端到端延迟；Runtime 池健康（健康实例数、worker 占用率——F4 的判据来源）；fencing 冲突计数；**Gateway 与会话管理状态分歧数**（A1 可观测性前提） | 补 4~5 项指标进 §17.1 |
| C3 | F4 判定依赖 Nacos 瞬时视图 | §12 F4"无可用 Runtime"以 Nacos 健康列表为准，与 2.4"接管不可依赖 Nacos"的原则存在张力 | 加任务级证据：连续 N 次派发失败（或 readiness 过滤后仍选不出实例）才置 paused/suspended |

### C1. 终态数据生命周期未定义（"直接删 vs 转历史表"一刀切）

**现状**：§4.2 五表只定义 DDL 与流转，未定义 Turn 终态后各行数据何去何从；§16.3 只谈"分区表"治索引退化，无终态行归档/清理策略。

**核心论证——不能一刀切，按表职责分层**：

| 表 | 职责 | 直接删的后果 | 推荐处理 |
|---|---|---|---|
| `turn_admission` | 调度队列 + **幂等台账**（8.1 `on conflict` 吸收重放） | **删行 = 重放防护失效**：前端重放 submit → 会话管理幂等返回同回执 → 落账 `on conflict` 不再命中 → 插入新 queued 行 → **已完成的 Turn 被重新调度执行，外部副作用重放** | **冷热分层，不物理删除**：终态行按保留周期（如 30 天，覆盖重放窗口）转归档表；活表只留未终态 + 重放窗口内的终态行 |
| `runtime_invocation` | 执行工单 + 观测 | 丢执行事实（谁执行了、error_code、耗时），审计/排障残缺 | **转历史表**（同 admission 周期）：审计价值高；观测字段反正纯观测 |
| `invocation_spec` | 不可变执行计划 | 丢"这次按什么版本/预算执行的"审计证据 | **转历史表**：随 invocation 1:1 归档 |
| `committed_content` | 确认内容**镜像**（权威在会话管理，D2） | 可重建，删了不丢业务数据 | **可删**（或保留短周期供本地排障）；不涉及重放防护 |
| `lease_info` | 三类租约 | controller 行完成即 `delete`（6.8）；session 行 `on conflict do update` 每 Session 恒一行空壳 | **现状已正确，无需归档**：session 空壳保留（uk 占位 + "该 Session 曾存在"痕迹），controller 完成即删 |

**重放防护上移（admission 行归档/删除的前提）**：

现防线 = 前端 request 幂等键（Redis，**可丢**）+ 会话管理 submit 幂等 + Gateway `on conflict`。防线 1 可丢、防线 3 依赖行存在 → 行归档即破防。

- **建议（决策 R11）**：把防线 3 从"行存在性"改为"**终态拒绝**"——落账前查该 `(session_id, turn_id, attempt_id)` 是否已有终态记录：有则直接返回已终态结果，**不再回队**（不产生新 queued 行）。
- 实现：turn_admission 终态行转归档后，落账路径对"回执对应的 turn 已终态"做显式检查（可查归档表，或由会话管理回执携带 turn 终态标志，二选一）；与 A1 的终态异步化联动，统一走"对齐检查"。

**落地**：

- 归档 Job：终态行按保留周期（建议 admission 30~90 天 / invocation+spec 180~365 天）批量迁归档表（`turn_admission_archive` / `runtime_invocation_archive` / `invocation_spec_archive`，结构同活表 + `archived_at`）；
- 查询层按需 union 活表 + 归档（getTurn 历史回看，11.3）；
- 与 D3 同步规划迁移/归档脚本载体（Flyway/Liquibase + 定时 Job）。

> 判定一句话：**admission 是台账必须冷热分层（不能直接删）；invocation/spec 转历史做审计；committed_content 可删（镜像）；lease_info 现状已自清理。**

---

## 6. D 级：次要项

| # | 项 | 说明 |
|---|---|---|
| D1 | Admission 状态机缺 mermaid 图 | §5.1/5.2/5.3 有 Turn/Attempt/Invocation 图，唯独 4.2.1 的核心队列状态机（queued→activating→activated→dispatched→终态+suspended）没有图 |
| D2 | 测试策略仅压测 | §16.7 无"双 Pod 同秒接管""SKIP LOCKED 两 Pod 同时竞争""RPC 超时注入"专项测试设计 |
| D3 | 无数据库迁移工具 | 表结构变更无 Flyway/Liquibase 依托，与 §15.4 连接池等实现细节不对齐 |
| D4 | oversubscription 资源检查无指标来源 | §14.5"本机 CPU/内存水位允许"未指明读取哪个 /actuator 指标 |

---

## 7. 修订优先级路线图

| 阶段 | 修订项 | 理由 |
|---|---|---|
| v1.1 前必修 | A1、A2、A3、R11 | 故障正确性：分歧状态、镜像污染、死循环卡死、重放防护（R11 是 admission 归档的前提，先于归档落地） |
| 开发 Runtime 对接前 | B1、B2 | 协议闭合：Runtime 无法实现未定义的推送/凭证语义 |
| v1.1 | A4、A5、C1、C2、C3 | 产品与运维闭环（C1 归档 Job 依赖 R11 先落地） |
| 随改随补 | D1~D4 | 低风险增量 |

---

## 8. 建议决策记录（供逐条采纳，风格对齐 design.md §19）

> 采纳后移入 design.md §19（编号延续 D17 起）；每条注明修订位置。

| # | 决策建议 | 内容 | 待修订位置 |
|---|---|---|---|
| R1 | 终态回写异步化 | Phase 7 本地事务只落终态 + `sync_pending`，COMMIT 后异步调 update；巡检判据 4 补发终态直至对齐 | 6.8 / 12.1 / 15.6.3 |
| R2 | commit fencing 校验 | commit 携带 attempt_id + controller_epoch，Gateway 与会话管理双侧校验活跃 attempt + 当前 holder | 6.7 / 4.1.1 / 4.2.5 |
| R3 | deadline 强制超时 | 巡检判据 4：`executing` 且超 spec.deadline → 按 commit 分界走自动重试 | 15.6.3 / 9.2 |
| R4 | 人工干预接口 | admin 提优先级 / 强制 failed / 唤醒重排，全部落 audit 轨迹 | 11.3 / 18 / 19 |
| R5 | 重试配额隔离 | `infra_retry_count` 独立计数（上限 3），USER_RETRY 配额独立 | 4.1.1 / 12.3 |
| R6 | Runtime 事件拉模型 | 事件改为 Gateway 主动长连/轮询拉取（跟 holder 走），免 Runtime 端发现逻辑 | 11.2 / 12.1 |
| R7 | fencing token 生命周期 | token 有效期 = Invocation 生命周期，代际 = controller_epoch | 7.3 |
| R8 | 快照孤儿回收 | Attempt 终态时显式回收或终态后 TTL 清理 | 6.4 / 9.1 |
| R9 | L1 降级或补组件 | v1.0 不实现 L1，接管一律 L2 租约过期（或补 EventWatcher） | 12.1 |
| R10 | 终态行归档 | 终态行按保留周期迁移归档分区 | 16.3 / 16.6 |
| R11 | 数据生命周期分层 | admission 冷热分层（不物理删除，重放防护上移为"终态拒绝"）；invocation/spec 转历史表（审计）；committed_content 可删（镜像可重建）；lease_info 保持现状 | 4.2.5 / 8.1 / 16.3 |