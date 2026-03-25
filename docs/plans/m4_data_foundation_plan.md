# M4 执行计划：数据录制、标准化 ETL 与 Qlib Provider 构建

## 0. 前置结论

M0-M3 已经把 **规则引擎、VeighNa 兼容网关、trade_server 运行时契约、OMS 冒烟链路** 打稳。下一阶段不要直接跳到模型训练或信号审批，而是先把 **交易侧产生的数据** 变成 **研究侧可重复消费的数据资产**。

M4 的唯一目标是打通下面这条链路：

```text
OpenCTP / mock 行情回调
  -> trade_server 录制
  -> append-only raw market data
  -> 标准化 ETL
  -> corporate action / adjustment factor
  -> Qlib provider_uri
  -> pyqlib 读取冒烟通过
```

M4 完成后，M5 才进入 Qlib baseline workflow（Alpha158 + 基线模型 + daily inference）。

---

## 1. M4 范围冻结

### In Scope
- SSE/SZSE 现金股票 + ETF
- Tick 录制
- 1 分钟 Bar 聚合
- 日线标准化输出
- 复权/调整因子最小闭环
- Qlib provider 导出
- 数据质量检查（DQ）
- 单日样本回放

### Out of Scope
- 北交所
- 可转债、融资融券、期权
- 商业数据源深度接入
- 历史全市场大规模回填
- ClickHouse 引入
- 模型训练、信号审批、执行任务发布

---

## 2. 设计原则

1. **原始数据不可变**：raw 层 append-only，不做就地覆盖。
2. **标准层可重建**：standard 层可以根据 raw + 主数据 + corporate action 重新生成。
3. **研究交易解耦**：Qlib 只读 provider 目录，不依赖 trade_server 内存态。
4. **数据与规则分离**：交易规则仍由 M1 规则引擎负责，ETL 不重复维护规则口径。
5. **时区固定**：全系统时间戳统一使用 `Asia/Shanghai` 语义，持久化保存 `exchange_ts` 与 `received_ts`。
6. **仓库优先本地可运行**：本阶段优先 Parquet + PostgreSQL manifest；不强依赖 Docker/ClickHouse。

---

## 3. 目标产物

M4 结束时，仓库里必须出现并可运行这些能力：

### 3.1 录制能力
- trade_server 启动后可挂接 `DataRecorder` 或自研录制 sink。
- 从 mock gateway / OpenCTP sim 回调进入的 `TickData` 能落为 raw 文件。
- 录制过程写 manifest，记录文件路径、交易日、symbol、记录条数、hash、来源 gateway。

### 3.2 标准化能力
- raw tick -> standardized tick parquet
- standardized tick -> 1m bars parquet
- 1m bars -> daily bars parquet
- 可按交易日、symbol、exchange 增量重建

### 3.3 corporate action / adjustment factor
- 有独立 schema 保存分红、送转、拆合股、配股等事件
- 能输出最小可用 adjustment factor（前复权或 Qlib 兼容字段）
- 因子可按 `instrument_key + effective_date` 查询

### 3.4 Qlib provider
- 能将标准化日线/分钟线 + instruments + calendars + adjustment factors 导出到 `data/qlib_bin/`
- 产出可以被 `qlib.init(provider_uri=..., region="cn")` 打开
- 至少 1 个 symbol、2 个交易日、1m + 1d 数据能被 Qlib 冒烟读取

### 3.5 DQ + 回放
- 对录制文件执行 DQ 检查并生成报告
- 支持从 raw/standard 样本重放出 TickData/BarData 事件序列用于测试

---

## 4. 建议目录增量

在现有 monorepo 下新增：

```text
apps/
  trade_server/
    app/
      recording/
        recorder_service.py
        sinks.py
        manifests.py
      replay/
        tick_replay.py
        bar_replay.py
      tests/
        test_recording_pipeline.py
        test_replay_sample.py

libs/
  marketdata/
    schemas.py
    raw_store.py
    manifest_store.py
    standardize.py
    bars.py
    corporate_actions.py
    adjustments.py
    qlib_export.py
    dq.py
    symbol_mapping.py
    tests/
      test_standardize_ticks.py
      test_bar_builder.py
      test_adjustment_factor.py
      test_qlib_export.py
      test_dq_checks.py

infra/
  sql/
    postgres/
      002_market_data.sql

scripts/
  run_recorder_smoke.py
  build_standard_data.py
  export_qlib_provider.py
  run_data_dq.py
  replay_sample.py

data/
  raw/
    market_ticks/
  standard/
    ticks/
    bars_1m/
    bars_1d/
  corporate_actions/
  qlib_bin/
  manifests/
  dq_reports/

docs/
  adr/
    0004_m4_data_foundation.md
  runbooks/
    recorder_and_qlib_export.md
```

---

## 5. 数据契约（必须先冻结）

### 5.1 Raw Tick Schema

文件层建议使用 Parquet，分区建议：

```text
data/raw/market_ticks/trade_date=YYYY-MM-DD/exchange=SSE/symbol=600000/*.parquet
```

字段：

- `instrument_key: str`
- `symbol: str`
- `exchange: str`
- `vt_symbol: str`
- `gateway_name: str`
- `exchange_ts: datetime64[ns, Asia/Shanghai]`
- `received_ts: datetime64[ns, Asia/Shanghai]`
- `last_price: float64`
- `volume: float64`
- `turnover: float64`
- `open_interest: float64 | null`
- `bid_price_1..5: float64 | null`
- `ask_price_1..5: float64 | null`
- `bid_volume_1..5: float64 | null`
- `ask_volume_1..5: float64 | null`
- `limit_up: float64 | null`
- `limit_down: float64 | null`
- `source_seq: str | null`
- `recorded_at: datetime64[ns, Asia/Shanghai]`
- `raw_hash: str`

### 5.2 Standard 1m Bar Schema

- `instrument_key`
- `symbol`
- `exchange`
- `vt_symbol`
- `bar_dt`（分钟结束时间）
- `open`
- `high`
- `low`
- `close`
- `volume`
- `turnover`
- `trade_count`
- `vwap`
- `session_tag`（am/pm/auction/after_hours）
- `is_synthetic`（是否由不足样本插补，首版一般 false）
- `build_run_id`

### 5.3 Corporate Action Schema

- `instrument_key`
- `symbol`
- `exchange`
- `action_type`（cash_dividend / stock_split / reverse_split / rights_issue / bonus_share）
- `ex_date`
- `effective_date`
- `cash_per_share`
- `share_ratio`
- `rights_price`
- `source`
- `source_hash`
- `loaded_at`

### 5.4 Adjustment Factor Schema

- `instrument_key`
- `trade_date`
- `adj_factor`
- `adj_mode`（forward / backward）
- `source_run_id`

### 5.5 Manifest Schema

PostgreSQL 只存元信息，不存大体量 tick：

- `recording_runs`
- `raw_file_manifest`
- `standard_file_manifest`
- `dq_reports`
- `corporate_actions`
- `adjustment_factors`

---

## 6. 实现顺序

### Step 1：录制层

#### 目标
把 trade_server 已有链路中的 `TickData` 稳定落到 raw 层。

#### 要求
- 不改动现有 gateway 回调契约。
- 录制逻辑订阅 EventEngine 或 recorder sink，不侵入 mapper/state。
- 每个文件写入后更新 manifest。
- 允许 mock gateway 直接驱动录制测试。

#### 建议实现
- `apps/trade_server/app/recording/recorder_service.py`
  - 负责注册事件监听、轮转文件、flush、异常日志。
- `apps/trade_server/app/recording/sinks.py`
  - `ParquetTickSink`
  - `ParquetBarSink`（可先留接口）
- `libs/marketdata/raw_store.py`
  - append writer
  - path resolver
  - hash helper
- `libs/marketdata/manifest_store.py`
  - PostgreSQL upsert / file fallback

#### 验收
- 输入 100 条 mock tick，raw 文件存在、manifest 存在、条数一致。
- 相同 tick 重放不会导致同一批文件重复写两次（按 run_id 或 file rotation 规则校验）。

---

### Step 2：标准化 ETL

#### 目标
把 raw tick 变成标准化 tick 与 1m/daily bars。

#### 要求
- 清洗重复值
- 保持时间单调
- 丢弃交易时段外脏数据（但保留原始文件）
- 1m bar 聚合时按 M1 规则引擎提供的 phase/session 切分
- 处理中午休市
- 允许增量重跑某一天某个 symbol

#### 建议实现
- `libs/marketdata/standardize.py`
  - `load_raw_ticks()`
  - `normalize_ticks()`
  - `filter_session_ticks()`
- `libs/marketdata/bars.py`
  - `build_1m_bars()`
  - `build_daily_bars_from_1m()`
- `scripts/build_standard_data.py`
  - `--trade-date`
  - `--symbol`
  - `--all`
  - `--rebuild`

#### 验收
- 给定一段 mock/raw 数据，生成的 1m bars 的 OHLCV 正确。
- 午间休市不产生非法分钟 bar。
- 竞价时段、盘后固定价格交易暂按规则引擎 phase 标记，首版不强行聚成普通连续竞价 bar。

---

### Step 3：Corporate Action 与 Adjustment Factor

#### 目标
形成 Qlib 可用的最小复权闭环。

#### 要求
- corporate action 数据先支持仓库 bootstrap + 手工样例
- 不依赖真实供应商即可跑通 factor 生成
- 输出 `adjustment_factors` 到 parquet + PostgreSQL manifest

#### 建议实现
- `libs/marketdata/corporate_actions.py`
- `libs/marketdata/adjustments.py`
- `scripts/build_standard_data.py --with-adjustment`

#### 验收
- 样例分红/送转事件能推导出 trade_date 序列上的 adj_factor。
- 同一 instrument 的 factor 序列单调可解释。
- 源数据变化时支持重建。

---

### Step 4：Qlib Provider 导出

#### 目标
把标准化数据导出为 Qlib 可读目录。

#### 要求
- provider 构建独立于交易进程
- 明确 symbol mapping：`instrument_key -> qlib_symbol`
- 目录结构和字段满足 `pyqlib` 读取要求
- 支持日线与分钟线至少一种先落地；推荐本阶段同时导出 1d 和 1min

#### 建议实现
- `libs/marketdata/symbol_mapping.py`
  - `to_qlib_symbol()`
  - `from_qlib_symbol()`
- `libs/marketdata/qlib_export.py`
  - `export_calendars()`
  - `export_instruments()`
  - `export_features_1d()`
  - `export_features_1min()`
  - `export_adjustments()`
- `scripts/export_qlib_provider.py`

#### 验收
- `pyqlib` smoke test 能通过：
  - `qlib.init(provider_uri=..., region="cn")`
  - 读取一个 symbol 的 1d/1min 样本数据成功
- 导出目录可重复构建；同输入下输出 hash 稳定。

---

### Step 5：DQ 与 Replay

#### 目标
保证数据可用、可解释、可重放。

#### DQ 检查至少包含
- 主键重复
- 时间倒序
- 负价格/负成交量
- 涨跌停价带外异常（容忍配置可调）
- 交易时段外记录
- symbol / instrument_key 映射缺失
- bar 聚合前后体量不守恒
- corporate action 日期落在非交易日

#### Replay 要求
- 能从 raw tick parquet 读回并重建 `TickData`
- 能从标准 1m bars 读回并重建 `BarData`
- 用于单测和策略回放，不要求此阶段接入 vn.py 回放 GUI

#### 建议实现
- `libs/marketdata/dq.py`
- `apps/trade_server/app/replay/tick_replay.py`
- `apps/trade_server/app/replay/bar_replay.py`
- `scripts/run_data_dq.py`
- `scripts/replay_sample.py`

#### 验收
- 生成 DQ report 文件和 manifest 记录。
- replay 样本可在单测里还原出稳定事件顺序。

---

## 7. SQL 增量建议

新增 `infra/sql/postgres/002_market_data.sql`，至少包含：

```sql
create table if not exists recording_runs (
  run_id text primary key,
  source_gateway text not null,
  mode text not null,
  started_at timestamptz not null,
  finished_at timestamptz,
  status text not null,
  notes text
);

create table if not exists raw_file_manifest (
  file_id text primary key,
  run_id text not null references recording_runs(run_id),
  trade_date date not null,
  exchange text not null,
  symbol text not null,
  instrument_key text not null,
  record_count integer not null,
  file_path text not null,
  file_hash text not null,
  created_at timestamptz not null default now()
);

create table if not exists standard_file_manifest (
  file_id text primary key,
  build_run_id text not null,
  layer text not null,
  trade_date date,
  exchange text,
  symbol text,
  instrument_key text,
  record_count integer not null,
  file_path text not null,
  file_hash text not null,
  created_at timestamptz not null default now()
);

create table if not exists corporate_actions (
  id bigserial primary key,
  instrument_key text not null,
  exchange text not null,
  symbol text not null,
  action_type text not null,
  ex_date date not null,
  effective_date date not null,
  cash_per_share double precision,
  share_ratio double precision,
  rights_price double precision,
  source text not null,
  source_hash text not null,
  loaded_at timestamptz not null default now()
);

create table if not exists adjustment_factors (
  instrument_key text not null,
  trade_date date not null,
  adj_factor double precision not null,
  adj_mode text not null,
  source_run_id text not null,
  primary key (instrument_key, trade_date, adj_mode)
);

create table if not exists dq_reports (
  report_id text primary key,
  layer text not null,
  trade_date date,
  scope text not null,
  status text not null,
  issue_count integer not null,
  report_path text not null,
  created_at timestamptz not null default now()
);
```

---

## 8. CLI 契约

本阶段建议固定这些脚本入口：

```powershell
.\.venv\Scripts\python.exe -m scripts.run_recorder_smoke
.\.venv\Scripts\python.exe -m scripts.build_standard_data --trade-date 2026-03-25
.\.venv\Scripts\python.exe -m scripts.build_standard_data --trade-date 2026-03-25 --symbol 600000.SSE --rebuild
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1d
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1min
.\.venv\Scripts\python.exe -m scripts.run_data_dq --trade-date 2026-03-25
.\.venv\Scripts\python.exe -m scripts.replay_sample --input data/standard/bars_1m/.../sample.parquet
```

`dev.ps1` 建议增加：
- `record-smoke`
- `build-data`
- `export-qlib`
- `dq`
- `replay-sample`

---

## 9. 测试计划

### 单元测试
- raw path resolver
- parquet writer/reader
- duplicate tick normalization
- 1m bar OHLCV 聚合
- midday break session split
- adjustment factor 计算
- qlib symbol mapping
- dq issue detection

### 集成测试
- mock gateway -> recorder -> raw parquet -> manifest
- raw parquet -> standard parquet -> daily bar
- standard parquet -> qlib export -> pyqlib smoke read
- raw/standard sample replay -> TickData/BarData 序列一致

### 验收标准
- `ruff check .`
- `mypy apps gateways libs scripts`
- `pytest -q`
- 新增测试通过后总测试数上升且无 flaky
- 至少 1 个端到端数据样本能从录制一路走到 qlib 冒烟读取

---

## 10. 非功能要求

- 不引入 Docker 硬依赖
- 不引入 ClickHouse
- 所有 bulk data 默认走 Parquet，不落 PostgreSQL 大表
- 所有脚本支持 `--dry-run` 或等价验证模式（如果适用）
- 所有导出流程产出 manifest 和 hash
- 所有 build/export 任务带 `run_id`

---

## 11. M4 完成定义（Definition of Done）

只有同时满足下列条件，M4 才算完成：

1. mock gateway 事件能录制为 raw parquet
2. raw parquet 能标准化为 1m / 1d 数据
3. corporate action 样例能生成 adjustment factor
4. Qlib provider 导出成功
5. `pyqlib` 冒烟读取成功
6. DQ 报告生成成功
7. replay sample 单测通过
8. 文档新增 ADR + runbook
9. CI/本地命令全绿

---

## 12. M4 结束后的唯一下一步

M4 完成后，下一阶段只做 **M5：Qlib baseline workflow**，不提前开 signal_service 或 execution planner。

M5 的目标会是：
- provider_uri 接入
- Alpha158 或等价基线特征
- baseline model training
- daily inference
- Recorder/MLflow 归档
- `prediction` / `approved_target_weight` 导出

