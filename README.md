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

## 消费位点（Checkpoint）约束

每个消费者组（consumer_group）维护一个已提交 offset，仅当 `ack` 通过全部校验才推进：

- `result_offset <= 当前位点` → `CHECKPOINT_REGRESSION`（回退，拒绝）
- `result_offset` 与被 ack 任务的 offset 不符、超过已分配的最大 offset、
  或之前仍有非终态任务 → `CHECKPOINT_OUT_OF_ORDER`（越界/乱序，拒绝）
- 租约不属于该消费者或 token 不符 → `LEASE_MISMATCH`；租约已过期/不存在 → `NOT_LEASED`

被拒绝的 ack 不会改变任务状态，也不会移动位点。

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
- `GET /jobs` / `GET /jobs/{job_id}` 查询任务（含终态与失败原因）
- `POST /leases` 按消费者组租出下一个任务
- `POST /jobs/{id}/ack` 确认成功并推进位点
- `POST /jobs/{id}/nack` 报告失败（可指定是否可重试）
- `GET /checkpoints/{group}` 查询消费位点
- `POST /maintenance/requeue-expired` 手动触发过期租约重投

## 验证方法

```bash
uv run pytest            # 17 个行为测试，日志打印关键输入与判定依据
uv run pytest -k retry   # 只看重试/终态场景
```

测试覆盖：重复提交（去重命中/冲突/非法）、有界退避、重试耗尽进入 DEAD、
不可重试失败、位点推进/回退/乱序/伪造租约、消费中断后的重新投递、
8 线程并发消费不重复执行、以及进程重启（重开 SQLite 文件）后状态与位点保留。
