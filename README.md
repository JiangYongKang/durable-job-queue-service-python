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
  其他消费者在租约有效期内拿不到它；崩溃消费者的旧租约在重新投递后自动失效。
- **终态不可变**：任务一旦进入终态（`SUCCEEDED` / `DEAD`）就绝不再被投递、绝不再执行第二次；
  无论旧 token 的迟到确认还是对同一任务的重复确认，都会被明确拒绝：
  - 租约已易主（重投后旧消费者来确认）→ `LEASE_MISMATCH`
  - 任务已被新消费者确认到终态（僵尸确认、重复 ack/nack）→ `ALREADY_COMPLETED`
- **尝试计数恰好一次**：每次投递（lease）计为一次 attempt；租约过期重投不会重复累加。

## 乱序完成与连续位点语义（Completion & Checkpoint）

多个消费者并发消费时，**任务终态与位点推进相互解耦**：

1. **完成即定论**：一个任务处理完（`ack` 成功或不可重试/重试耗尽的 `nack`），
   立即进入对应终态，与序号更小的任务是否完成无关：
   - 成功 → `SUCCEEDED`，处理结果随 ack 的 `result` 字段落库，立即可查
     （`GET /jobs/{id}` 返回 `result`）；
   - 确定失败 → `DEAD`（`NON_RETRYABLE` / `RETRIES_EXHAUSTED`），`last_error` 保留原因。
   不会再出现"后面的任务早处理完，却因前面有任务未完成而一直悬在处理中"的情况。
2. **位点 = 连续完成边界**：每次有任务落终态后，位点从当前值 +1 开始沿 offset 扫描，
   只要对应任务已是终态（成功或 DEAD 都算）就连续推进，**碰到第一个未完成的"空洞"立即停住**。
3. **空洞补完自动衔接**：卡住位点的任务后来完成（含重试后成功）后，位点一次性自动越过
   之前早已处理好的那批任务，**无需重跑、无需人工干预**。
4. **底线不变**：位点永不跳过未完成任务、永不倒退。乱序确认、重复确认、重放确认
   都不会让位点倒退或错误前进。可重试失败（`FAILED_RETRYABLE`）不是终态，仍占住空洞，
   直到重试后落终态才参与推进。

### 与原严格顺序行为的兼容范围

- `ack` 的 `result_offset` 参数改为**可选**：不传时按新语义工作（推荐）；
  仍传时会保留原有校验——`result_offset <= 当前位点` 返回 `CHECKPOINT_REGRESSION`、
  与被 ack 任务 offset 不符或越界返回 `CHECKPOINT_OUT_OF_ORDER`，但它**不再决定位点**，
  位点一律由连续完成边界自动推进。因此：
  - 原来"按 offset 顺序逐个 ack"的消费方式行为完全不变（边界与顺序确认结果一致）；
  - 原来"必须等前面全部终态才能 ack 后面"的限制解除，乱序 ack 直接成功且结果可查。
- `nack` 新增可选关键字参数 `consumer_group`：任务在该次 nack 进入终态（DEAD）时
  一并参与连续位点推进；不传则行为与旧版一致（只落任务状态）。HTTP 接口对应
  `NackRequest.consumer_group` 可选字段。
- 旧版本 SQLite 库文件可直接打开：缺少 `result` 列时会自动补列（`ALTER TABLE`），
  历史任务的 `result` 为 `null`。

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

每个消费者组（consumer_group）维护一个已提交 offset（含义：该组**连续完成**的最大 offset）。
位点在每次 ack/nack 落终态后由系统自动推进到连续完成边界（见上一节），无需调用方计算。

以下确认会被拒绝，且**不改变任何任务状态、不移动位点**：

- 任务不存在 → `NOT_FOUND`
- 任务已在终态（重复确认 / 重投后的僵尸确认）→ `ALREADY_COMPLETED`
- 租约不属于该消费者或 token 不符 → `LEASE_MISMATCH`；租约已过期 → `NOT_LEASED`
- 传了旧版 `result_offset` 且 `<= 当前位点` → `CHECKPOINT_REGRESSION`（回退，拒绝）
- 传了旧版 `result_offset` 且与被 ack 任务 offset 不符或越界 → `CHECKPOINT_OUT_OF_ORDER`

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
- `GET /jobs` / `GET /jobs/{id}` 查询任务（含终态、失败原因与成功 `result`）
- `POST /leases` 按消费者组租出下一个任务
- `POST /jobs/{id}/ack` 确认成功（请求体可选 `result` 落处理结果；`result_offset`
  仅为兼容旧客户端的可选校验项），成功后自动推进连续完成位点
- `POST /jobs/{id}/nack` 报告失败（可指定 `retryable`；可选 `consumer_group`，
  进入 DEAD 终态时一并推进位点）
- `GET /checkpoints/{group}` 查询消费位点（连续完成边界）
- `POST /maintenance/requeue-expired` 手动触发过期租约重投

## 验证方法

```bash
uv run pytest                      # 全部行为测试（收集到 74 个用例），日志打印关键输入与判定依据
uv run pytest -k out_of_order      # 乱序完成 / 空洞补完 / 迟到确认等确定性场景
uv run pytest -k concurrent        # 并发乱序、僵尸确认竞争、成功+DEAD 混合（重复多轮）
```

并发稳定性：并发相关用例均用 `@pytest.mark.parametrize` 重复 10~20 轮
（`Barrier` 让多消费者同时起跑、按 offset 反向延时制造乱序），可再外层连跑确认不是碰运气：

```bash
for i in $(seq 1 20); do uv run pytest -q 2>/dev/null | tail -1; done
```

测试覆盖：

- 重复提交（去重命中/冲突/非法）、有界退避、重试耗尽进入 DEAD、不可重试失败；
- 位点推进/回退/乱序/伪造租约（旧 17 个用例零回归）；
- **乱序完成**：大 offset 先成功立即 SUCCEEDED、结果可查，位点停在空洞前；
- **空洞补完自动衔接**：卡住的任务完成后位点一次性越过早已完成的批次，无重跑
  （attempts 仍为 1）；DEAD 任务同样算终态、参与边界；
- **重投后迟到确认**：旧消费者的僵尸 ack 被 `LEASE_MISMATCH` / `ALREADY_COMPLETED`
  拒绝，新消费者定下的结果不被覆盖，任务不再投递、不执行第二次；
- 重复 ack 被 `ALREADY_COMPLETED` 拒绝且位点不动、旧版 `result_offset` 校验仍生效；
- 并发：8 消费者抢 20 任务不重复、30 任务 8 消费者乱序完成 ×20 轮位点必达 N、
  僵尸/新确认竞争 ×20 轮恰有一个 OK、成功+DEAD 混合 ×10 轮；
- 消费中断重投、进程重启（重开 SQLite 文件）状态与位点保留。

### 本地手工复现乱序完成

```bash
QUEUE_DB_PATH=demo.db uv run python main.py     # 起服务
curl -s localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"idempotency_key":"j1","payload":{}}'      # offset 1
curl -s localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"idempotency_key":"j2","payload":{}}'      # offset 2
# 分别租给 c1/c2 后，先确认 offset 2（lease_token 以实际返回为准）：
curl -s localhost:8000/leases -H 'content-type: application/json' -d '{"consumer_group":"g","consumer_id":"c1"}'
curl -s localhost:8000/leases -H 'content-type: application/json' -d '{"consumer_group":"g","consumer_id":"c2"}'
curl -s localhost:8000/jobs/<id2>/ack  -H 'content-type: application/json' \
  -d '{"consumer_group":"g","consumer_id":"c2","lease_token":"<token2>","result":{"v":2}}'
curl -s localhost:8000/checkpoints/g    # offset 仍为 0：空洞在 offset 1，但 j2 已 succeeded 且 result 可查
curl -s localhost:8000/jobs/<id1>/ack  -H 'content-type: application/json' \
  -d '{"consumer_group":"g","consumer_id":"c1","lease_token":"<token1>","result":{"v":1}}'
curl -s localhost:8000/checkpoints/g    # offset 自动跳到 2，j2 没有重跑
```
