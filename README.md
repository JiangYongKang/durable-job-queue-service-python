# durable-job-queue-service-python

本地可运行的持久化任务队列与消费位点管理服务。全部状态落盘到本地 SQLite 文件，
不依赖任何外部服务、真实凭据或付费资源；进程重启后任务状态与消费位点自动恢复。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run pytest -v        # 运行全部测试（日志打印关键输入与判定依据）
uv run python main.py   # 启动 HTTP 服务，默认 127.0.0.1:8000
```

HTTP 接口示例：

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"dedup_key": "order-1", "payload": "{\"amount\":100}"}'
curl -X POST 'localhost:8000/jobs/lease?consumer_id=c1&limit=1'
curl -X POST 'localhost:8000/jobs/<job_id>/ack?consumer_id=c1'
curl -X POST localhost:8000/offset/commit -H 'content-type: application/json' \
     -d '{"seq": 1, "verified": true}'
```

## 投递语义

- **至少一次投递（at-least-once）**：任务只有被 `ack` 才进入 `SUCCEEDED` 终态；
  消费者崩溃、超时或未确认时，租约过期后任务回到 `PENDING` 被重新投递。
- **不并发双执行**：`lease` 在单连接串行写与队列锁内完成状态翻转，
  同一任务在租约有效期内只会投递给一个消费者（有多线程竞争测试佐证）。
- **状态机**：`PENDING -> LEASED -> SUCCEEDED`（成功终态）；
  `LEASED -> PENDING`（可重试失败 / 租约过期回收）；`LEASED -> DEAD`（失败终态）。
  终态任务不再被投递，但可查询（含 `attempts` 与 `last_error`）。

## 去重规则（提交幂等）

以 `dedup_key` 为业务标识，在去重窗口（默认 3600s）内判定，结果四选一、可区分：

| 结果 | 条件 | 行为 |
|---|---|---|
| `CREATED` | 窗口内无同 key 任务 | 新建入队，分配单调递增 `seq` |
| `DUPLICATE` | 同 key 且 payload 哈希一致 | 返回原任务 ID，不重复入队 |
| `CONFLICT` | 同 key 但 payload 不一致 | 拒绝，原任务不被覆盖 |
| `REJECTED` | 空 key / 空 payload / `max_attempts < 1` | 拒绝并说明原因 |

窗口过期后同 key 可再次提交为新任务。任何情况下都不会静默覆盖已存在任务。

## 重试规则

- 仅 `retryable=True` 的失败会重试；`attempts` 只在 `fail` 时累加一次，
  租约过期回收（消费者中断）不累加，避免计数重复。
- 退避为**有界指数退避**：`min(base * 2^(attempts-1), max_backoff)`，
  默认 base=0.1s、上限 10s（测试中用 1s/60s）。
- `attempts` 达到 `max_attempts` 或失败为不可重试时，任务进入 `DEAD` 终态，
  `last_error` 区分记录「可重试但次数耗尽」与「不可重试失败」，可查询、不丢弃、不无限重试。

## 位点推进约束

- 每个任务有单调递增 `seq`；消费位点按消费者组持久化，重启后从上次提交处恢复。
- `commit(seq, verified)` 仅在 `verified=True`（结果校验通过）时受理；
  且只允许严格推进到 `current + 1`：
  - `seq <= current` → 回退/重复提交，拒绝（`OffsetError` / HTTP 409）；
  - `seq > current + 1` → 乱序/越界跳号，拒绝；
  - `verified=False` → 拒绝。

## 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `JOBQUEUE_DB`（环境变量） | `./jobqueue.db` | SQLite 文件路径，`:memory:` 为纯内存 |
| `dedup_window_seconds` | 3600 | 幂等去重窗口 |
| `base_backoff_seconds` / `max_backoff_seconds` | 0.1 / 10 | 退避基数与上限 |
| `lease_seconds` | 30 | 租约有效期，过期任务被回收重投 |
| `max_attempts`（每次提交） | 3 | 最大尝试次数 |

## 测试覆盖

`tests/` 下 17 个用例，日志打印关键输入与判定依据：

- `test_submit_dedup.py`：重复提交去重、参数冲突、非法提交、窗口过期；
- `test_retry.py`：重试耗尽显 DEAD、退避有界、不可重试直接终态、ack 终态；
- `test_offset.py`：未校验拒绝、位点回退拒绝、乱序越界拒绝、位点跨重启恢复；
- `test_redelivery.py`：处理中崩溃后重投、并发不双执行（含 8 线程竞争）、
  旧租约持有者 ack 被拒、任务状态跨进程重启恢复。
