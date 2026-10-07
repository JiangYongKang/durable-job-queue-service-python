# Durable Job Queue Service (Python)

一套**本地可运行**的持久化任务队列与消费位点管理服务。基于 FastAPI + SQLite（标准库），
不依赖任何外部服务、真实凭据或付费资源；任务状态与消费者位点落盘于本地 SQLite 文件，
进程重启后状态完整保留。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run pytest           # 运行全部行为测试（日志含关键输入与判定依据）
uv run python main.py   # 启动服务，默认 127.0.0.1:8000，文档见 /docs
```

## 投递语义（Delivery Semantics）

- **At-least-once**：任务以"租约（lease）"形式投递。消费者在租约有效期内必须
  `ack`（成功）或 `nack`（失败）；逾期未确认的任务会被自动重新入队并再次投递。
- **不并发重复执行**：同一任务同一时刻只租给一个消费者（租约 token + 过期时间），
  其他消费者在租约有效期内拿不到它；崩溃消费者的旧租约在重新投递后自动失效
  （旧 token 的 ack/nack 会被 `LEASE_MISMATCH` 拒绝）。
- **尝试计数恰好一次**：每次投递（lease）计为一次 attempt；租约过期重投不会重复累加。
- **乱序完成（out-of-order completion）**：多消费者并发时，任何一个任务处理完就
  **立即**进入终态——成功即 `SUCCEEDED`（含处理结果，可查询），确定失败即 `DEAD`
  （含原因）。已完成的任务**不会**因为序号在前的任务还没处理完而卡在中间状态。
- **完成即终态、终态即隔离**：任务一旦进入终态，绝不会再被投递、不会再被执行，
  其结果也不会被任何迟到的确认覆盖。

## 消费位点（Checkpoint）：连续完成前沿

每个消费者组维护一个已提交 offset，语义为**连续完成前沿（contiguous completion
frontier）**：

- 从当前位点往后，凡是**连续**处于终态（`SUCCEEDED`/`DEAD`）的任务，位点一路推进；
  碰到第一个还没处理完的"空洞"就停在空洞前面。
- 空洞被补完（ack 成功或 nack 进 DEAD）时，位点**自动**向后衔接已经在空洞之后
  完成的那批任务——不需要重跑，也不需要人工干预。
- 底线不变：位点**绝不倒退**、**绝不跳过**非终态任务。乱序确认、重复确认、
  对已完成任务的再确认，都不会让位点倒退或错误前进。

`ack` 的校验与结果：

| 情形 | 结果 |
|---|---|
| 租约有效、offset 匹配 | `OK`：任务立即 `SUCCEEDED`，位点推进到连续完成前沿 |
| 同一租约重复 ack（如响应丢失后重试） | `OK`：幂等空操作，状态与位点均不变 |
| 重投后旧租约的迟到 ack / 伪造 token | `LEASE_MISMATCH`：识别并拒绝，最终结果不被污染 |
| `result_offset <= 当前位点` | `CHECKPOINT_REGRESSION`（回退，拒绝） |
| `result_offset` 与被 ack 任务的 offset 不符或超过已分配最大 offset | `CHECKPOINT_OUT_OF_ORDER`（拒绝） |
| 租约已过期/不存在，或对 DEAD 任务 ack | `NOT_LEASED`（终态结果不可推翻） |

`ack` 可携带可选的 `result`（dict），随任务落盘，之后通过 `GET /jobs/{id}` 查询。

## 与旧版严格顺序语义的兼容范围

- **行为变化**：旧版要求"ack 之前所有更早 offset 的任务都已进入终态"，否则
  `ack` 被 `CHECKPOINT_OUT_OF_ORDER` 拒绝、任务滞留 `LEASED`；新版改为任务
  立即终态化、位点按连续前沿推进。旧版中"因前序任务未完成而被拒的 ack"，
  在新版会被接受——这正是本次要补的并发完成语义。
- **保持不变**：幂等提交/去重、有界退避重试、租约互斥与过期重投、迟到确认拒绝、
  位点不回退/不跳空、终态不再投递、进程重启后状态与位点完整保留。
- **存储兼容**：旧版 SQLite 文件打开时自动迁移（新增 `result` 列），已有任务与
  位点不受影响。

## 幂等去重规则

提交 `POST /jobs` 携带 `idempotency_key` 与 `payload`，返回可区分的结果：

| 情形 | 结果 | 说明 |
|---|---|---|
| 首次提交 | `ACCEPTED` (200) | 创建任务，分配单调递增的 offset |
| 窗口内同 key 同 payload | `DUPLICATE` (200) | 返回已存在的 job_id，不产生重复执行 |
| 同 key 不同 payload | `CONFLICT` (409) | 参数冲突，**绝不静默覆盖**已存在任务 |
| 窗口外复用同 key | `REJECTED` (422) | 需更换新 key，拒绝静默覆盖 |
| 非法参数（空 key / 非 dict payload / max_attempts < 1） | `REJECTED` (422) | 附具体原因 |

## 失败重试与终态

- 可重试失败（`nack retryable=true`）：按**有界指数退避**重新可见，
  `delay = min(backoff_base * 2^(attempts-1), backoff_max)`。
- 达到 `max_attempts` 次投递后仍失败：进入终态 `DEAD`，原因 `RETRIES_EXHAUSTED`。
- 不可重试失败（`retryable=false`）：立即进入 `DEAD`，原因 `NON_RETRYABLE`。
- 成功 `ack`：进入终态 `SUCCEEDED`，原因 `SUCCEEDED`。
- 终态任务**可查询、不再投递、不再重试**，`last_error` 保留最后一次失败原因。

状态机：`PENDING → LEASED → (SUCCEEDED | FAILED_RETRYABLE → LEASED … | DEAD)`，
租约超时路径：`LEASED → PENDING`（重新投递）。

## 配置项

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `QUEUE_DB_PATH` | `jobqueue.db` | SQLite 存储路径（`:memory:` 为纯内存） |
| `QUEUE_DEDUP_WINDOW_SECONDS` | `3600` | 幂等去重窗口 |
| `QUEUE_MAX_ATTEMPTS` | `3` | 默认最大投递次数 |
| `QUEUE_BACKOFF_BASE_SECONDS` | `1` | 退避基数 |
| `QUEUE_BACKOFF_MAX_SECONDS` | `60` | 退避上限 |
| `QUEUE_VISIBILITY_TIMEOUT_SECONDS` | `30` | 租约可见性超时（崩溃重投时延） |

## HTTP API 摘要

- `POST /jobs` 提交任务（幂等）
- `GET /jobs` / `GET /jobs/{job_id}` 查询任务（含终态、失败原因与处理结果 `result`）
- `POST /leases` 按消费者组租出下一个任务
- `POST /jobs/{id}/ack` 确认成功（可携带 `result`），任务立即终态化并推进连续完成前沿
- `POST /jobs/{id}/nack` 报告失败（可指定是否可重试）
- `GET /checkpoints/{group}` 查询消费位点
- `POST /maintenance/requeue-expired` 手动触发过期租约重投

## 验证方法

```bash
uv run pytest                              # 27 个行为测试，日志打印关键输入与判定依据
uv run pytest -k retry                     # 只看重试/终态场景
uv run pytest tests/test_out_of_order.py   # 只看乱序完成/位点前沿/迟到确认场景

# 并发用例稳定性验证（连跑 20 遍应全部通过）
for i in $(seq 1 20); do
  uv run pytest tests/test_out_of_order.py::TestConcurrentOutOfOrder \
                tests/test_queue.py::TestRedelivery -q | tail -1
done
```

测试覆盖：重复提交（去重命中/冲突/非法）、有界退避、重试耗尽进入 DEAD、
不可重试失败、位点推进/回退/越界/伪造租约、消费中断后的重新投递、
**乱序完成立即终态化且结果可查**、**空洞补完后位点自动衔接**、
**重投后旧租约的迟到确认被拒且最终结果不被污染**、**完成租约的重复 ack 幂等**、
**DEAD 视为完成并放行位点**、已完成任务不再投递、
8 线程并发乱序确认收敛、成功/失败混合并发收敛、
以及进程重启（重开 SQLite 文件）后状态与位点保留。
