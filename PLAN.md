# M0-M2 顺序实施计划

## Summary
- 当前仓库根目录直接作为 monorepo 根目录，不再额外套 `aquant/` 子目录。
- 本地开发按 Windows + `.venv\Scripts\python.exe` 3.11.9 设计，先做 M0，再做 M1，再做 M2，不提前铺后续里程碑代码。
- M1/M2 采用 adapter/mock-first：规则和网关接口先稳定，真实数据源与 OpenCTP SDK 后接。

## Key Changes
### M0
- 创建顶层目录 `apps/`、`gateways/`、`libs/`、`infra/`、`scripts/`、`data/`、`docs/`、`.github/`，但只实装 M0-M2 会用到的内容，不生成后续阶段的大量空模块。
- 根目录新增 `pyproject.toml`，使用 `setuptools` 作为构建后端，统一 Python 3.11，配置 `pytest`、`ruff`、`mypy`、`pre-commit`。
- 新增 `libs/common/logging.py`，采用标准 `logging` 封装项目日志初始化；仅补最少公共工具，不提前抽象业务层。
- 新增 PowerShell 开发入口 `scripts/dev.ps1`，固定本地命令为 `lint`、`test`、`bootstrap`、`up`；同时保留 `Makefile` 作为 CI/Unix 兼容层，映射同一套动作。
- 新增 `.env.example`、`compose.yaml`、基础 `README.md`、`docs/adr/ADR_TEMPLATE.md`、环境启动 runbook。
- `main.py` 的 PyCharm 示例脚本在 M0 中移除，不保留无关样板。

### M1
- 新增 `libs/schemas/master_data.py` 与 `libs/schemas/trading.py`，定义 `Instrument`、`MarketRuleSnapshot`、`CostProfile`、`OrderRequest`、`AccountSnapshot`、`MarketSnapshot`、`ValidationResult`。
- 新增 `libs/rules_engine/calendar.py`、`market_rules.py`、`product_classifier.py`、`cost_model.py`、`order_validation.py`。
- 在 `data/master/bootstrap/` 放版本化种子数据，包含交易所日历、板块规则快照、ETF 结算类型覆盖表、默认成本模板。
- 在 `infra/sql/postgres/001_master_data.sql` 定义 `instruments`、`market_rules`、`cost_profiles` 三张表，当前阶段直接用 SQL，不上 ORM/migration 框架。
- 新增 `scripts/load_master_data.py`，读取 bootstrap CSV/JSON，经 Pydantic 校验后执行 PostgreSQL upsert；无数据库时支持 `--validate-only`。
- 规则口径固定为数据驱动：交易时段、禁撤单窗口、涨跌停价带来自 `market_rules` 快照；ETF T+0/T+1 由 `settlement_type` 主数据决定，不允许按代码前缀猜测；A 股买入按 100 股整数手校验，零股卖出单独放行。

### M2
- 只新增 `gateways/vnpy_openctpsec/`，不提前实现 `trade_server`。
- 包内固定包含 `gateway.py`、`md_api.py`、`td_api.py`、`mapper.py`、`state.py`、`contract_loader.py`、`reconnect.py`、`errors.py`。
- `gateway.py` 暴露 `OpenCTPSecGateway`；`md_api.py`/`td_api.py` 只负责底层 adapter 包装；VeighNa 类型映射只放 `mapper.py`；订单幂等和乱序处理只放 `state.py`。
- `state.py` 订单状态机同时按本地单号和柜台单号索引，必须支持重复回报去重、回报先于本地下单确认、断线重连后的未完结订单恢复。
- `reconnect.py` 负责订阅恢复和未完成订单同步；真实 OpenCTP SDK 接入通过 adapter 协议实现，不直接写死动态库或账号配置。
- M2 代码完成标准是 mock 驱动的完整事件链可测；真实 OpenCTP sim 作为后续接线验收，不把 SDK 缺失变成代码结构上的阻塞。

## Public Interfaces
- M1 对外公共函数固定为 `is_trade_day`、`get_sessions`、`is_cancel_allowed`、`get_price_limit`、`get_lot_size`、`is_t0_allowed`、`supports_after_hours_fixed_price`、`calc_cost`、`validate_order`。
- M1 CLI 固定为 `.\.venv\Scripts\python.exe -m scripts.load_master_data --validate-only` 和 `--apply`。
- M2 网关公共方法固定为 `connect`、`subscribe`、`send_order`、`cancel_order`、`query_account`、`query_position`、`query_contract`、`close`。
- M2 adapter 协议固定覆盖登录、行情、订单、成交、资金、持仓、错误、重连事件，保证后续替换真实 OpenCTP 实现时不改网关主结构。

## Test Plan
- M0：通过 `scripts/dev.ps1` 跑 `ruff`、`mypy`、`pytest`；补充导入冒烟测试、配置文件存在性测试、PowerShell 命令入口测试。
- M1：覆盖深市禁撤单窗口、交易日判断、100 股整数手买入、零股卖出、ETF T+0/T+1 分类、涨跌停价带、最低佣金、卖出税费、规则按日期快照查询；规则引擎覆盖率目标大于 90%。
- M1：补 loader 测试，覆盖重复证券、规则有效期重叠、非法字段、无数据库时 `--validate-only` 行为。
- M2：补 mapper 映射测试、状态机幂等测试、重复回报测试、部分成交到全成测试、拒单测试、回报先于 ack 测试、重连恢复测试。
- M2：使用 mock adapter 做最小联调，覆盖登录、订阅、下单、撤单、资金查询、持仓查询、回报转换。

## Assumptions
- 当前仓库 `C:\Users\whq\PycharmProjects\veighnaProject` 直接作为 monorepo 根目录。
- 项目统一使用 `.venv\Scripts\python.exe` 3.11.9，shell 默认 `python` 3.10.5 不参与项目命令。
- 本地验收优先 Windows/PowerShell；`Makefile` 仅为 CI/Unix 兼容，不要求你当前机器先装 `make`。
- Docker 当前不在 PATH，所以 M0 先交付 compose 与脚本，真实 `up` 验收等 Docker 可用后再补。
- 主数据先采用仓库内版本化 bootstrap 文件；真实数据供应商接入只替换 loader adapter。
- OpenCTP 先做 adapter/mock-first；真实 SDK、仿真账号、动态库路径不在当前阶段写死。
- Git 目前有 `safe.directory` 限制；后续如需依赖 git 命令，需要先把仓库加入 safe directory。
