# TurnScheduler 详细设计说明书（LLD）

> 版本：v1.0 ｜ 阶段：详细设计 ｜ 上游文档：`turn-scheduler-hld.md`（概要设计）
> 本文档面向编码实施与单元/集成测试；与 HLD 配套，内容以可实现的精度描述各模块处理、数据库 DDL、关键 SQL、时序、状态机与类设计。

---

## 1. 引言

### 1.1 编写目的

在概要设计（HLD）批准的基础上，将 TurnScheduler 的每个功能模块细化到可编码程度：

- 模块级处理流程、输入输出、异常处理；
- 数据库完整 DDL、索引、事务边界、关键竞争 SQL；
- 核心流程时序、状态机转换、并发与一致性机制；
- 类与接口定义、测试要点、监控埋点。

### 1.2 范围

实现范围：TurnScheduler 调度侧服务（Spring Boot），含 5 张 Gateway 侧表、三类 Lease、7 阶段调度流水线、流式 Delta 推送、故障恢复。不含 Gateway 会话管理侧实现（仅接口对接）。跨库接口表 `turn_ready_outbox`（OUTBOX，位于会话管理库）由会话管理侧建表，本模块通过消费接口（E1）claim/ack 消费，并在 3.1 给出**参考 DDL 与消费契约**以保证两侧落地一致。

### 1.3 引用文档

| 编号 | 文档 |
|---|---|
| [1] | turn-scheduler-hld.md（概要设计） |
| [2] | turn-scheduler-design.md（v2 技术方案） |
| [3] | 统一 Agent Runtime 与 Runtime Driver 技术方案 |
| [4] | 专题 02：Session、Turn 与 Context |

### 1.4 模块清单（对应 HLD 4.4）

| 模块 | 类名（Java） | 职责 |
|---|---|---|
| M0 实时事件路由 | RuntimeEventRouter | 消费 Runtime SSE 事件流并路由到 Confirm/Complete |
| M1 准入 | TurnAdmissionService | Inbox 幂等准入 |
| M2 排队激活 | TurnQueueActivationService | SKIP LOCKED 竞争串行激活 |
| M3 冻结 | TurnContextFreezeService | 冻结 ContextSnapshot 与 InvocationSpec |
| M4 创建/租约 | TurnExecutionService | 创建 Invocation、获取 Session/Controller Lease |
| M5 调度 | RuntimeDispatchService | Nacos 选型 + Worker 容量路由 + 下拨 |
| M6 确认 | TurnConfirmService | Committed 幂等累积 + Delta 推送 |
| M7 完成 | TurnCompletionService | 终态判定、Session 推进 |
| M8 心跳 | LeaseHeartbeatService | 三类租约周期续约 |
| M9 恢复 | TurnRecoveryService | 接管、中断、卡死检测 |
| M10 推送 | TurnStreamPushService | Redis Stream 写入 + 活跃 Attempt 过滤 |
| M11 路由 | ShardRouter | session_id 分片选择（v1 N=1） |

---

## 2. 模块详细设计

### 2.1 调度主循环（SchedulingLoop）

所有 Pod 独立运行，无选举，靠 DB 竞争。

```text
while (running) {
    // 1. 消费 Outbox：claim 一批（PENDING→CLAIMED，30s 租约）
    List<TurnReadyMessage> msgs = outboxClient.claim(batchSize);
    for (TurnReadyMessage m : msgs) {
        TurnAdmissionService.admit(m)
        outboxClient.ack(m.id);   // 先 Inbox 落账，后 ack
    }

    // 2. 激活排队
    TurnQueueActivationService.pollAndActivate()

    // 3. 处理可调度 Invocation
    TurnExecutionService.processNextInvocation()

    // 4. 心跳
    LeaseHeartbeatService.refreshAll()

    sleep(poll_interval)
}
```

| 配置项 | 默认值 |
|---|---|
| poll_interval | 200ms |
| outbox_batch_size | 50 |
| outbox_claim_lease | 30s（claim 后未 ack 超时回 PENDING） |
| activation_batch_size | 10 |
| dispatch_batch_size | 10 |

### 2.2 TurnAdmissionService（M1）

**消息定义（对应 turn_ready_outbox 一行，跨库消息契约）：**

```java
public record TurnReadyMessage(
    long    outboxId,       // turn_ready_outbox.id
    String  sessionId,
    String  turnId,
    String  attemptId,
    String  contextRef,     // 可空：冻结阶段再构建
    String  payload         // TurnReady 完整消息体
) {}
```

**处理流程：**

```text
1. claim：outboxClient.claim(batchSize)
   -> 会话管理库内 UPDATE ... WHERE status='PENDING' SKIP LOCKED
      SET status='CLAIMED', claimed_by=:pod, claim_expire_at=now()+30s RETURNING *
2. 对每条消息 m：
   a. inbox 幂等键 = (session_id, turn_id, attempt_id)
   b. INSERT INTO turn_admission (..., status='WAITING') ... ON CONFLICT DO NOTHING
      // INBOX + queue 合一：落账即入队，不再二次写 pending_turn_queue
   c. 若插入成功（新 Attempt）：已入队（WAITING），凭 admission 行参与 SKIP LOCKED 竞争
   d. ack：outboxClient.ack(m.outboxId)   // 先落账后 ack，m 置 DONE
```

**输入输出：**

| 方向 | 数据 | 说明 |
|---|---|---|
| 入 | TurnReadyMessage { outboxId, session_id, turn_id, attempt_id, context_ref, payload } | 会话管理事务写出的 OUTBOX 消息 |
| 出 | admission_id（admission 行即队列元素） | 后续流程凭 admission_id 追踪 |

**异常处理：**

| 异常 | 处理 |
|---|---|
| claim 失败（接口超时） | 下一轮重试（消息仍在 PENDING，无副作用） |
| admit 阶段失败 | **不 ack**，claim 租约 30s 后自动回 PENDING，其他 Pod 重领 |
| 入队冲突 | ON CONFLICT 静默跳过，视为已准入 |
| ack 失败 | 消息停留 CLAIMED，租约过期回 PENDING 后被重领；Inbox 幂等保证重领不产生重复数据 |

### 2.3 TurnQueueActivationService（M2）

**核心竞争 SQL（串行激活，INBOX+queue 合一后直接竞争 admission 行）：**

```sql
update turn_admission a
set status = 'ACTIVATING',
    activated_at = now(),
    activated_pod = :pod
where a.id = (
    select a2.id
    from turn_admission a2
    where a2.status = 'WAITING'
      and not exists (
          select 1 from runtime_invocation ri
          where ri.session_id = a2.session_id
            and ri.status in ('ADMITTED','FREEZING','FROZEN','ACTIVATING','ACTIVATED','DISPATCHED','EXECUTING')
      )
    order by a2.priority, a2.created_at
    limit 1
    for update skip locked
)
returning a.*;
```

**要点：**

- 「同 Session 无活跃 Invocation」是激活前提（会话级串行）；
- `FOR UPDATE SKIP LOCKED` 保证并发 Pod 各取各的行，不互相阻塞；
- 激活后状态 ACTIVATING，短暂过渡 → ACTIVATED（状态机见第 6 章）。

### 2.4 TurnContextFreezeService（M3）

**处理流程：**

```text
对 ACTIVATED 的 admission:
  1. 加载会话上下文（从 Gateway 侧上下文引用，冻结快照）
  2. 冻结 InvocationSpec（尽量拷贝，杜绝可变外部引用）
  3. 写入 invocation_spec（context_snapshot jsonb + spec jsonb）
  4. 将 admission 置 FREEZING -> FROZEN
```

**防篡改原则：** ContextSnapshot 与 Spec 一经生成不可修改；异常时整条路径回滚至当前 Attempt。

### 2.5 TurnExecutionService（M4）

**创建 Invocation 并获取双层租约（单事务脚本，租约统一写入 lease_info）：**

```sql
begin;

insert into runtime_invocation (session_id, turn_id, attempt_id, admission_id,
                                status, priority, committed_seq, init_seq, spec_rev)
values (:session_id, :turn_id, :attempt_id, :admission_id,
        'ADMITTED', :priority, 0, 0, :spec_rev);

-- Session Execution Lease（串行执行权，lease_type='session'）
insert into lease_info (lease_type, session_id, invocation_id, holder_pod, lease_until)
values ('session', :session_id, :invocation_id, :pod, now() + interval '30 second');

-- Controller Lease（控制权，lease_type='controller'）
insert into lease_info (lease_type, session_id, invocation_id, controller_epoch, holder_pod, lease_until)
values ('controller', :session_id, :invocation_id, 1, :pod, now() + interval '60 second');

commit;
```

**租约刷新（M8 心跳，两语句）：**

```sql
update lease_info set lease_until = now() + interval '30 second'
where lease_type = 'session' and session_id = :s and invocation_id = :i and holder_pod = :pod;

update lease_info set lease_until = now() + interval '60 second'
where lease_type = 'controller' and session_id = :s and invocation_id = :i and controller_epoch = :epoch and holder_pod = :pod;
```

> 心跳用 `holder_pod + controller_epoch` 双重匹配，保证只续自己的、只续当前代。

### 2.6 RuntimeDispatchService（M5）

**选型流程：**

```text
1. 从 Nacos 拉取 Runtime 实例列表（健康实例：心跳不过期 + readiness=true）
2. 按剩余 Worker 容量排序（实例上报的 capacity + running_count）
3. 无容量上报时 round-robin
4. 选择实例，下拨执行
5. 更新 runtime_invocation: status=INVOCATION_DISPATCHED, runtime_replica_id=:replica
6. 游程开始（fencing_token 继承）
```

**下拨数据（SchedulingContext）：**

```text
{
  invocation_id,
  fencing_token,
  context_snapshot（冻结的会话上下文）,
  spec（冻结的 InvocationSpec）,
  event_target（SSE 回调地址）,
  session_id, turn_id, attempt_id
}
```

### 2.7 RuntimeEventRouter（M0）+ TurnConfirmService（M6）

**事件路由：**

| 事件类型 | 路由目标 | 处理 |
|---|---|---|
| assistant_output_delta | TurnStreamPushService | 写 Redis Stream（体验层） |
| assistant_content_committed | TurnConfirmService | 幂等写 committed_content |
| runtime_event（tool 等） | 记录 + 推送 | runtime_invocation 观测字段推进（event_sequence / progress_note） |
| invocation_completed | TurnCompletionService | 终态判定 |
| invocation_failed | TurnCompletionService | INTERRUPTED |

**幂等确认 SQL（关键）：**

```sql
insert into committed_content (session_id, turn_id, invocation_id, seq,
                               content_type, payload, committed_by, created_at)
values (:session_id, :turn_id, :invocation_id, :seq, :content_type, :payload, :by, now())
on conflict (invocation_id, seq) do nothing;
```

**水位推进：**

```sql
update runtime_invocation ri
set committed_seq = :new_seq
where ri.invocation_id = :id and ri.committed_seq = :old_seq;
```

> 乐观锁：期望旧水位匹配才推进，防止重复/乱序确认把水位回拨。

### 2.8 TurnCompletionService（M7）

**终态条件：**

```text
确认内容追平 init_seq + 收到 invocation_completed
  -> status = COMPLETED，写会话侧终态事件，释放租约
或：失败/中断判定
  -> status = INTERRUPTED，保留已确认内容，可重试
```

**完成 SQL：**

```sql
update runtime_invocation set status = 'COMPLETED', completed_at = now()
where invocation_id = :id and status in ('DISPATCHED','EXECUTING','PAUSED');

delete from lease_info where lease_type = 'session' and invocation_id = :id;
delete from lease_info where lease_type = 'controller' and invocation_id = :id;
```

### 2.9 TurnRecoveryService（M9）

**接管（Gateway Pod 崩溃场景）：**

```text
扫描 lease_info 中 lease_type='controller' 且超期（lease_until < now()）的行
  -> 重新执行创建/租约流程（同 2.5），但 controller_epoch = 旧 + 1
  -> 新 fencing token 下发 Runtime
  -> Runtime 侧旧请求因 fencing 过期被拒
```

**中断（Runtime 失联）：**

```text
扫描 executing 但 runtime_invocation.updated_at 停滞（观测字段不再推进）+ Runtime 心跳超时
  -> Attempt 标 INTERRUPTED，通知前端可重试
  -> 不拼接：新 Attempt 新租约新 Watermark
```

**卡死检测 SQL（判据）：**

```sql
-- Controller 心跳停滞（lease_type='controller'）
select session_id, invocation_id, holder_pod
from lease_info
where lease_type = 'controller'
  and lease_until < now() - interval '1 minute'
  and exists (select 1 from runtime_invocation ri
              where ri.invocation_id = lease_info.invocation_id
                and ri.status in ('DISPATCHED','EXECUTING'));

-- Runtime 事件水位停滞（观测字段并入 runtime_invocation）
select ri.invocation_id, ri.event_sequence, ri.updated_at
from runtime_invocation ri
where ri.updated_at < now() - interval '5 minutes'
  and ri.status in ('DISPATCHED','EXECUTING');
```

### 2.10 TurnStreamPushService（M10）

**写入：**

```text
XADD turn_events:<session_id> * event_json
  事件 = { session_id, turn_id, attempt_id, sequence, type, payload }
TTL: EXPIRE turn_events:<session_id> 600
```

**推送（活跃 Attempt 过滤）：**

```text
持有 SSE 连接者，按 client_watermark 消费:
  XRANGE turn_events:<session_id> <watermark+1> +
过滤: attempt_id == 当前活跃 attempt 才推送
控制事件: attempt 切换时推 stream_reset（见 11.3 契约）
```

### 2.11 ShardRouter（M11）

```text
shard_id = Math.floorMod(session_id.hashCode(), N)   // N 配置化，v1 = 1
数据源选择：AbstractRoutingDataSource 按 shard_id 切换
约束：同 Session 同 shard；无跨 shard 事务
```

---

## 3. 数据库详细设计

### 3.1 表结构与 DDL

**gateway 侧（调度模块拥有，5 张）。** 另含 1 张会话管理库接口表（3.1.1，OUTBOX，本模块只消费不建表）。

#### 3.1.1 turn_ready_outbox（会话管理库 · OUTBOX，接口契约）

> 由会话管理侧建表，本模块仅在消费接口（E1）中读写。此处给出参考 DDL，用于双方对齐字段与状态语义。

```sql
create table turn_ready_outbox (
    id               bigserial primary key,
    session_id       uuid      not null,
    turn_id          uuid      not null,
    attempt_id       uuid      not null,        -- Attempt 粒度：重试产生新 Attempt = 新消息
    context_ref      uuid,
    payload          jsonb     not null,
    status           varchar   not null default 'PENDING',  -- PENDING/CLAIMED/DONE/DEAD
    claimed_by       varchar,
    claim_expire_at  timestamptz,
    attempt_count    int       not null default 0,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    unique (session_id, turn_id, attempt_id)    -- 写入侧幂等
);

create index idx_outbox_pending       on turn_ready_outbox (id) where status = 'PENDING';
create index idx_outbox_claim_expiry  on turn_ready_outbox (claim_expire_at) where status = 'CLAIMED';
```

**claim 语义（会话管理库内单语句，消费接口内部逻辑）：**

```sql
update turn_ready_outbox o
set status = 'CLAIMED',
    claimed_by = :pod,
    claim_expire_at = now() + interval '30 seconds',
    attempt_count = attempt_count + 1,
    updated_at = now()
where o.id in (
    select id from turn_ready_outbox
    where status = 'PENDING'
    order by id
    for update skip locked
    limit :batch_size
)
returning o.id, o.session_id, o.turn_id, o.attempt_id, o.context_ref, o.payload;
```

**ack 语义：**

```sql
update turn_ready_outbox
set status = 'DONE', updated_at = now()
where id in (:ids) and claimed_by = :pod and status = 'CLAIMED';
```

**租约回收（会话管理侧后台任务）：** `UPDATE ... SET status='PENDING', claimed_by=NULL WHERE status='CLAIMED' AND claim_expire_at < now();`

#### 3.1.2 turn_admission（INBOX + 调度队列，幂等消费台账）

> **表合并说明（方案A）：** 原 `pending_turn_queue` 并入本表——INBOX 幂等落账（admitted）与入队（waiting）合一，`priority` 与排队语义由 admission 行自身承载，消除双写窗口；Phase 2 SKIP LOCKED 直接竞争本表 `status='WAITING'` 的行。

```sql
create table turn_admission (
    id            bigserial primary key,            -- 调度侧主键
    outbox_id     bigint,                           -- OUTBOX 消息溯源（ack 后回填，可空）
    session_id    uuid      not null,               -- 会话（分片键）
    turn_id       uuid      not null,
    attempt_id    uuid      not null,
    context_ref   uuid      not null,               -- 冻结上下文引用
    status        varchar   not null default 'WAITING',  -- 状态机：WAITING(入队即准入) -> FREEZING -> FROZEN -> ACTIVATING -> ACTIVATED -> COMPLETED / DEAD / CANCELLED
    priority      int       not null default 0,     -- 调度优先级（原 queue 语义）
    admitted_at   timestamptz,
    frozen_at     timestamptz,
    activated_at  timestamptz,
    activated_pod varchar,                          -- 激活者（SKIP LOCKED 竞争获胜的 Pod）
    completed_at  timestamptz,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now(),
    unique (session_id, turn_id, attempt_id)        -- Inbox 幂等键（OUTBOX 消息幂等）
);

create index idx_admission_session on turn_admission (session_id, created_at desc);
create index idx_admission_claim    on turn_admission (priority, created_at) where status in ('WAITING','ACTIVATING');
```

#### 3.1.3 runtime_invocation（执行工单 + Runtime 观测）

> **表合并说明（方案A）：** 原 `runtime_observed_state` 并入本表（1:1 冗余）——`runtime_status / event_sequence / progress_note` 观测字段由 Runtime 事件单调推进，只用于健康感知与卡死判定，不参与状态机。

```sql
create table runtime_invocation (
    id                bigserial primary key,
    session_id        uuid      not null,
    turn_id           uuid      not null,
    attempt_id        uuid      not null,
    admission_id      bigint    not null references turn_admission(id),
    status            varchar   not null,   -- ADMITTED/FREEZING/FROZEN/ACTIVATING/ACTIVATED/DISPATCHED/EXECUTING/PAUSED/COMPLETED/TERMINATED/INTERRUPTED/FAILED
    priority          int       not null default 0,
    committed_seq     bigint    not null default 0,
    init_seq          bigint    not null default 0,
    spec_rev          bigint    not null default 1,
    runtime_replica_id varchar,
    fencing_token     bigint,
    runtime_status    varchar,                    -- 观测：Runtime 侧执行状态
    event_sequence    bigint    not null default 0, -- 观测：事件水位（单调）
    progress_note     varchar,                    -- 观测：最近进度说明
    started_at        timestamptz,
    completed_at      timestamptz,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    unique (session_id, attempt_id)
);

create index idx_invocation_session  on runtime_invocation (session_id, created_at desc);
create index idx_invocation_status   on runtime_invocation (status) where status in ('ACTIVATING','ACTIVATED','DISPATCHED','EXECUTING');
create index idx_invocation_pending  on runtime_invocation (priority, id) where status in ('FREEZING','FROZEN','ACTIVATING','ACTIVATED');
create index idx_invocation_stuck    on runtime_invocation (updated_at) where status in ('DISPATCHED','EXECUTING') and event_sequence >= 0;
```

#### 3.1.4 invocation_spec

```sql
create table invocation_spec (
    invocation_id     bigint primary key references runtime_invocation(id),
    context_snapshot  jsonb not null,
    spec              jsonb not null,
    created_at        timestamptz not null default now(),
    rev               bigint not null default 1
);
```

#### 3.1.5 lease_info（统一租约：Session + Controller）

> **表合并说明（方案A）：** 原 `session_lease` 与 `controller_lease` 合并为一张表，以 `lease_type` 区分；Runtime 侧 fencing token 为运行时内存语义（见 6.2），不在此落库。

```sql
create table lease_info (
    id                bigserial primary key,
    lease_type        varchar   not null,           -- 'session' / 'controller'
    session_id        uuid      not null,
    invocation_id     bigint    not null references runtime_invocation(id),
    controller_epoch  bigint    not null default 1, -- 仅 controller 类型使用，接管时 +1
    holder_pod        varchar   not null,
    lease_until       timestamptz not null,
    updated_at        timestamptz not null default now(),
    unique (lease_type, session_id),                -- session 型：同 Session 串行唯一
    unique (lease_type, invocation_id)              -- controller 型：单控制者
);
create index idx_lease_expiry on lease_info (lease_until) where lease_until < now();
```

#### 3.1.6 committed_content

```sql
create table committed_content (
    id             bigserial primary key,
    session_id     uuid      not null,
    turn_id        uuid      not null,
    invocation_id  bigint    not null references runtime_invocation(id),
    seq            bigint    not null,
    content_type   varchar   not null,               -- text/delta/file_ref/tool_event
    payload        text      not null,
    committed_by   varchar   not null default 'runtime',
    created_at     timestamptz not null default now(),
    unique (invocation_id, seq)
);
create index idx_committed_turn on committed_content (turn_id, seq);
create index idx_committed_session on committed_content (session_id, created_at desc);
```

### 3.2 索引设计总结

| 表 | 关键索引 | 目的 |
|---|---|---|
| turn_ready_outbox | (id) where PENDING、(claim_expire_at) where CLAIMED | claim/租约回收扫描（会话管理库） |
| turn_admission | unique(session_id,turn_id,attempt_id)；idx_admission_claim (priority,created_at) where WAITING/ACTIVATING | Inbox 幂等 + SKIP LOCKED 竞争扫描 |
| runtime_invocation | (status,q) partial；idx_invocation_stuck (updated_at) where DISPATCHED/EXECUTING | 调度待办扫描 + 卡死检测 |
| lease_info | unique(lease_type,session_id)、unique(lease_type,invocation_id)、(lease_until) where < now() | 串行约束 + 单控制者 + 租约回收 |
| committed_content | unique(invocation_id,seq) | 幂等确认 |
| 全部分片表 | session_id 前缀 | 分片/分区路由 |

### 3.3 事务边界规则

| 规则 | 说明 |
|---|---|
| 长事务禁止 | 任何 SQL 不跨越网络调用（Runtime 下拨在事务外） |
| claim 单语句 | 使用 CTE + UPDATE ... RETURNING 原子完成 |
| 事务内只写 PG | 不混 Redis/Nacos 写 |
| 隔离级别 | READ COMMITTED（SKIP LOCKED 已足够） |

### 3.4 分区分片

- v1：Router N=1 + 分区表按 hash(session_id) 分区（解决行数膨胀）；
- 演进：N=2/4（见 HLD 9.2），路由只改配置与数据迁移，代码不变；
- 分片唯一约束：唯一键必须含 session_id（已满足所有表设计）。

## 4. 核心流程时序图

### 4.1 提交与准入

```mermaid
sequenceDiagram
    participant SM as 会话管理
    participant SMPG as 会话管理库<br/>(turn_ready_outbox)
    participant TS as TurnScheduler
    participant TSPG as Gateway库<br/>(turn_admission)

    SM->>SMPG: [T] 写 Turn + 写 turn_ready_outbox（同事务）
    TS->>SMPG: claim(batchSize, podId)&#xa;PENDING→CLAIMED（SKIP LOCKED + 30s 租约）
    SMPG-->>TS: TurnReadyMessage[]（outbox_id 等）
    loop 每条消息
        TS->>TSPG: INSERT turn_admission (status=WAITING)&#xa;ON CONFLICT DO NOTHING&#xa;幂等键 (session_id, turn_id, attempt_id)&#xa;INBOX+queue 合一：落账即入队
        alt 新 Attempt
            TS->>TSPG: 已入队，凭 admission 行参与后续竞争
        else 重复消息
            TS->>TSPG: 静默跳过（Inbox 幂等）
        end
        TS->>SMPG: ack(outbox_id, podId)&#xa;CLAIMED→DONE（先落账后 ack）
    end
    Note over TS,SMPG: claim 后崩溃 → 租约30s过期回 PENDING → 其他 Pod 重领
```

### 4.2 调度与执行

```mermaid
sequenceDiagram
    participant TS as TurnScheduler
    participant PG as PostgreSQL
    participant RT as Agent Runtime

    TS->>PG: UPDATE turn_admission SKIP LOCKED 激活（WAITING→ACTIVATING）
    TS->>PG: 冻结 ContextSnapshot + InvocationSpec
    TS->>PG: [TX] 创建 runtime_invocation + lease_info（session + controller）
    TS->>PG: 置 status=DISPATCHED, runtime_replica_id
    TS->>RT: 下拨 SchedulingContext(fencing_token, spec)
    RT-->>TS: SSE: assistant_output_delta
    TS->>PG: 幂等写 committed_content（seq 去重）
    TS->>TS: XADD Redis Stream（Delta 体验层）
    RT-->>TS: SSE: invocation_completed
    TS->>PG: 终态判定 COMPLETED + 释放 lease_info 双层租约
```

### 4.3 故障接管（Pod 崩溃）

```mermaid
sequenceDiagram
    participant OA as 旧 Pod(Controller)
    participant PG as PostgreSQL
    participant NB as 新 Pod(Controller)
    participant RT as Agent Runtime

    OA->>PG: 心跳续约（30s/60s）...
    OA-->xPG: 宕机，停止心跳
    PG-->>NB: 检测 lease_info controller 过期
    NB->>PG: controller_epoch = 旧epoch + 1 重获控制权
    NB->>PG: 生成新 fencing_token
    NB->>RT: 重新下拨（新 fencing_token）
    RT-->>NB: 旧请求 fencing 过期被拒，接受新请求
    NB->>PG: 从 committed_seq 水位继续累积
```

### 4.4 流式推送（Attempt 屏蔽）

```mermaid
sequenceDiagram
    participant RT as Agent Runtime
    participant TS as TurnScheduler
    participant RD as Redis Stream
    participant GW as Gateway(SSE)
    participant UI as 前端

    RT-->>TS: SSE: delta(attempt=2)
    TS->>RD: XADD turn_events:s7 {attempt_id:2, seq:41}
    TS->>GW: 推送（仅 attempt_id==活跃 attempt 的事件）
    GW->>UI: SSE: {turn_id, seq, type, payload}
    Note over TS,GW: 用户 retryTurn 后 attempt 切换
    TS->>GW: SSE: stream_reset {from_sequence: 50}
    GW->>UI: 清空渲染缓冲，接收新回答
```

---

## 5. 状态机详细设计

### 5.1 Turn 状态（会话管理侧）

```mermaid
stateDiagram-v2
    [*] --> WAITING : 提交
    WAITING --> RUNNING : 首个 Attempt 开始
    RUNNING --> COMPLETED : 正式回答完成
    RUNNING --> INTERRUPTED : 中断可重试
    INTERRUPTED --> RUNNING : 用户重试(新 Attempt)
    COMPLETED --> [*]
    INTERRUPTED --> [*]
```

### 5.2 Attempt 状态（会话管理侧）

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> EXECUTING
    EXECUTING --> COMPLETED
    EXECUTING --> INTERRUPTED
    INTERRUPTED --> [*]
    COMPLETED --> [*]
```

### 5.3 RuntimeInvocation 状态（调度侧）

```mermaid
stateDiagram-v2
    [*] --> ADMITTED : 准入
    ADMITTED --> FREEZING : 开始冻结
    FREEZING --> FROZEN : 冻结完成
    FREEZING --> TERMINATED : 冻结失败
    FROZEN --> ACTIVATING : 排队激活
    ACTIVATING --> ACTIVATED : 激活成功
    ACTIVATED --> DISPATCHED : 下拨 Runtime
    DISPATCHED --> EXECUTING : Runtime 开始执行
    EXECUTING --> PAUSED : 条件等待
    PAUSED --> EXECUTING : 条件满足
    EXECUTING --> COMPLETED : 完成事件
    EXECUTING --> INTERRUPTED : 失败/失联
    PAUSED --> INTERRUPTED : 接管/超时
    TERMINATED --> [*]
    COMPLETED --> [*]
    INTERRUPTED --> [*]
```

### 5.4 状态转换防护

| 禁止转换 | 防护手段 |
|---|---|
| 非 own 状态推进 | 所有 UPDATE 带 `status in (...)` 条件 + `holder_pod/controller_epoch` 匹配 |
| COMPLETED 后仍写内容 | 确认写入以 commit_seq 乐观锁推进，终态后写不生效 |
| 旧控制器接管 | controller_epoch 单调递增，低 epoch 写入被拒 |
| 双活 in-flight | SKIP LOCKED 队列激活 + session 唯一租约 + Runtime fencing |

---

## 6. 并发与一致性详细设计

### 6.1 分布式互斥（SKIP LOCKED）

- 队列激活与状态推进全部单语句原子完成；
- `FOR UPDATE SKIP LOCKED`：每个 Pod 拿到属于自己的行，失败/竞争不阻塞；
- 结果校验：`UPDATE ... RETURNING` 返回空 = 未竞争成功，下一轮再试。

### 6.2 三类 Lease 语义矩阵

| Lease | 用途 | 时长 | 刷新者 | 过期后果 |
|---|---|---|---|---|
| lease_info（lease_type='session'） | 同 Session 串行唯一 | 30s | 未来执行该 Session 的 Pod | 其他 Pod 可开启新 Invocation |
| lease_info（lease_type='controller'） | Invocation 控制权（写权限） | 60s | 持控制权 Pod | 其他 Pod 接管（epoch+1） |
| runtime fencing_token | 执行权防呆 | 30s（Runtime 侧） | Runtime | Runtime 拒绝陈旧下拨 |

### 6.3 Fencing Token 机制

- 每次下拨携带当前 controller_epoch 对应的 token；
- 接管后 token 变更；Runtime 收到 token 不匹配的重复请求直接拒绝；
- 旧 Pod 即使网络恢复，也因 token 失配无法再写入 Runtime 与 PG（epoch 校验）。

### 6.4 幂等设计

| 层面 | 幂等键 | 机制 |
|---|---|---|
| Admission | (session_id, turn_id, attempt_id) | UNIQUE + ON CONFLICT DO NOTHING（INBOX 吸收 OUTBOX 重复投递） |
| Committed | (invocation_id, seq) | UNIQUE + ON CONFLICT DO NOTHING |
| 水位推进 | committed_seq 乐观锁 | 期望旧值匹配才更新 |
| Lease 获取 | 写入前检查占用 | unique(lease_type,session_id) / unique(lease_type,invocation_id) |

### 6.5 双写窗口

- 场景：确认内容写入后 Pod 崩溃，尚未回执 Runtime；
- 结论：内容已入 committed_content（权威），Runtime 重发仅被幂等吸收；终态事件重复到达以状态条件过滤。

---

## 7. 类设计

### 7.1 领域模型类图

```mermaid
classDiagram
    class Turn {
        +UUID id
        +UUID sessionId
        +TurnStatus status
        +List~Attempt~ attempts
    }
    class Attempt {
        +UUID id
        +int seq
        +AttemptStatus status
    }
    class RuntimeInvocation {
        +long id
        +UUID sessionId
        +UUID turnId
        +UUID attemptId
        +InvocationStatus status
        +long committedSeq
        +long initSeq
        +long fencingToken
    }
    class InvocationSpec {
        +long invocationId
        +JsonNode contextSnapshot
        +JsonNode spec
    }
    class LeaseInfo {
        +LeaseType leaseType
        +UUID sessionId
        +long invocationId
        +long controllerEpoch
        +String holderPod
        +Instant leaseUntil
    }
    class CommittedContent {
        +long id
        +UUID turnId
        +long seq
        +String content
    }

    Turn "1" --> "N" Attempt
    Attempt "1" --> "1" RuntimeInvocation
    RuntimeInvocation "1" --> "1" InvocationSpec
    RuntimeInvocation "1" --> "2" LeaseInfo
    RuntimeInvocation "1" --> "N" CommittedContent
```

### 7.2 服务层类图

```mermaid
classDiagram
    class SchedulingLoop {
        -TurnAdmissionService admission
        -TurnQueueActivationService activation
        -TurnExecutionService execution
        -LeaseHeartbeatService heartbeat
        +runLoop() void
    }
    class TurnAdmissionService {
        +admit(TurnReadyMessage) AdmissionResult
    }
    class OutboxConsumer {
        +claim(int batchSize) List~TurnReadyMessage~
        +ack(long outboxId) void
    }
    class TurnQueueActivationService {
        +pollAndActivate() int
    }
    class TurnContextFreezeService {
        +freeze(Admission) InvocationSpec
    }
    class TurnExecutionService {
        +createInvocationWithLeases() long
    }
    class RuntimeDispatchService {
        +dispatch(Invocation, RuntimeInstance) void
    }
    class RuntimeEventRouter {
        +onDelta(DeltaEvent) void
        +onCommitted(CommittedEvent) void
        +onCompleted(CompletionEvent) void
    }
    class TurnConfirmService {
        +confirmCommitted(CommittedEvent) void
    }
    class TurnCompletionService {
        +complete(Invocation) void
    }
    class LeaseHeartbeatService {
        +refreshAll() void
    }
    class TurnRecoveryService {
        +recoverExpiredLeases() void
        +detectStalled() void
    }
    class TurnStreamPushService {
        +pushDeltaToStream(DeltaEvent) void
        +pushToClient(StreamEvent) void
    }
    class RuntimeInstanceRegistry {
        +healthyRuntimes() List~RuntimeInstance~
    }
    class ShardRouter {
        +resolveShard(UUID sessionId) DataSource
    }

    SchedulingLoop --> TurnAdmissionService
    TurnAdmissionService --> OutboxConsumer
    SchedulingLoop --> TurnQueueActivationService
    SchedulingLoop --> TurnExecutionService
    SchedulingLoop --> LeaseHeartbeatService
    TurnExecutionService --> RuntimeDispatchService
    RuntimeDispatchService --> RuntimeInstanceRegistry
    RuntimeEventRouter --> TurnConfirmService
    RuntimeEventRouter --> TurnCompletionService
    TurnConfirmService --> TurnStreamPushService
    TurnRecoveryService --> TurnExecutionService
    TurnExecutionService --> ShardRouter
```

### 7.3 关键接口签名（Java）

```java
public interface TurnSchedulerPorts {
    // 准入（M1）+ Outbox 消费
    AdmissionResult admit(TurnReadyMessage msg);
    List<TurnReadyMessage> claimOutbox(int batchSize);

    // 激活（M2）
    int pollAndActivate();

    // 创建与租约（M4）
    long createInvocationWithLeases(UUID sessionId, UUID turnId, UUID attemptId, int priority);

    // 调度（M5）
    void dispatch(long invocationId, RuntimeInstance target);

    // 确认（M6）
    void confirmCommitted(CommittedEvent evt);

    // 完成（M7）
    void complete(long invocationId, CompletionReason reason);

    // 心跳（M8）
    void refreshAllLeases();

    // 恢复（M9）
    int recoverExpired();     // 返回接管数
    int detectStalled();      // 返回中断数

    // 推送（M10）
    void pushDelta(DeltaEvent evt);
}
```

---

## 8. 异常处理设计

### 8.1 异常分类与策略

| 类别 | 代表异常 | 策略 |
|---|---|---|
| 重试可恢复 | DB 连接抖动、Nacos 暂不可用 | 指数退避重试（3 次，上限 10s） |
| 竞争未胜 | SKIP LOCKED 空结果 | 静默，下一轮重试 |
| 数据一致性 | 乐观锁冲突 | 重新读取，按新状态决策（接管/放弃） |
| 不可恢复 | Spec 冻结失败、死信 | 记录 + 置死信 + 告警，人工介入 |
| Runtime 异常 | 下拨失败、执行超时 | 按事故分级：租约回收 → 中断 Attempt → 通知前端重试 |

### 8.2 重试矩阵

| 操作 | 重试次数 | 退避 | 上限时长 |
|---|---|---|---|
| Outbox 消费 | 无限（幂等） | 200ms 平滑 | - |
| 激活竞争 | 无限（循环） | 下一轮 | - |
| Spec 冻结 | 3 | 指数 100ms/1s/10s | 30s 内 |
| Runtime 下拨 | 2 | 1s/5s | 换实例重试 |

### 8.3 死信处理

- 死信条件：Admission 持续冻结失败 / Queue 行损坏无法激活；
- 处理：状态置 DEAD + 死信表记录 + 告警；不阻塞同 Session（同 Session 只卡受影响 Turn）。

---

## 9. 安全设计

| 项 | 设计 |
|---|---|
| Runtime 双向认证 | 下拨令牌校验 + fencing token（防重放/防陈旧） |
| Nacos 访问 | 命名空间隔离 + 鉴权 |
| Secret 管理 | K8s Secret 注入 API Key，日志脱敏（不可打印密钥） |
| SQL 注入 | 全部参数化（JdbcTemplate ? 占位符 / MyBatis #{}） |
| Redis | 独立 namespace，仅存 Delta 体验数据（TTL 自洁） |
| 审计 | Invocation 全生命周期 + 终态原因字段，可追溯 |

---

## 10. 测试设计要点

| 层 | 用例 |
|---|---|
| 单元 | 状态机转换、幂等键、乐观锁推进、ShardRouter 哈希 |
| 并发 | 100 Producer × 25 Pod × 745 Session 竞争激活；SKIP LOCKED 无死锁 |
| 故障 | Pod 宕机接管、Runtime 失联中断、lease 过期兜底、双活不可能性 |
| 流式 | attempt 切换 stream_reset、断线续读水位、旧 attempt 事件不串流 |
| 集成 | Gateway Outbox → 调度 → Runtime → 确认 → 完成全链路 |
| 压测 | 稳态 50 turns/s、峰值 150、Redis 积压、PG 连接水位 |
| 分片 | N=1/2 切换路由一致性、session 同 shard 约束 |

---

## 11. 监控埋点详细设计

### 11.1 指标埋点

| 指标 | 类型 | 标签 |
|---|---|---|
| ts_turns_total | Counter | outcome={admitted,completed,interrupted} |
| ts_turns_per_second | Gauge | - |
| ts_lock_contention_rate | Gauge | queue / invocation |
| ts_lease_duration_seconds | Histogram | lease=session/controller |
| ts_takeover_total | Counter | reason={l0,l1,l2} |
| ts_queue_depth | Gauge | status=waiting |
| ts_delta_latency_ms | Histogram | p50/p95/p99 |
| ts_stream_backlog | Gauge | session 维度聚合 |

### 11.2 告警规则

| 告警 | 表达式示例 | 级别 |
|---|---|---|
| 接管风暴 | rate(ts_takeover_total[5m]) > 10 | P1 |
| 锁竞争异常 | ts_lock_contention_rate > 0.5 持续 5m | P2 |
| 中断率过高 | rate(interrupted)/rate(completed) > 0.1 | P1 |
| 队列积压 | ts_queue_depth > 1000 | P2 |
| Delta 延迟 | p95 > 500ms | P2 |

### 11.3 日志规范

- 每条 Invocation 生命周期打点（创建/下拨/确认/完成/中断，含 session_id、invocation_id）；
- 接管与恢复操作打 INFO+ 原因字段（l0/l1/l2）；
- 任何「理论不发生」的异常（双活、epoch 回退）打 ERROR 并触发 P0。

---

*—— 详细设计正文结束 ——*