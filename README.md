# A-Share Alpha Engine

A standalone A-share cross-sectional multi-factor research project focused on:

- stock selection
- portfolio construction
- backtesting
- daily review
- paper trading

中文名：`A股阿尔法引擎`

## What This Project Does

这个项目不是“单只股票择时脚本”，而是一套完整的组合研究框架。

它每天做的事情可以概括成一句话：

`先从 A 股里筛出一批可交易、流动性足够的股票，再用多因子打分，最后低频、分批地持有排名靠前的一篮子股票。`

当前主线已经覆盖：

- 全市场基础股票池过滤
- 多因子计算与截面标准化
- 行业 / 市值 / 板块中性化
- 低频慢换仓组合管理
- 回测与参数优化
- 收盘后复盘
- 模拟账户连续跟踪

## Current Strategy Snapshot

当前主线更偏向：

- `短期价格行为`
- `20 日换手`
- `20 日波动`
- `EP`
- `ROE_TTM`

组合层强调：

- 分散持仓
- 更慢调仓
- 最小持有交易日
- 每次调仓限制新增仓位数量

这套策略不是纯追涨模型，也不是纯价值模型，更像：

`截面因子选股 + 低频组合管理 + 收盘后决策执行`

## Repository Layout

```text
src/qstrategy_v2/        Core strategy package
docs/                    Research, execution, and process documents
tests/                   Lightweight verification tests
config/                  Static config files
reports/current_baseline Representative baseline backtest outputs
reports/paper_accounts/  Paper account snapshots
reports/daily_reviews/   Daily review examples
```

## Quick Start

Install:

```bash
python3 -m pip install -e .
```

Required environment variables:

```bash
export TUSHARE_TOKEN=...
export EASTMONEY_APIKEY=...
```

Common commands:

```bash
qstrategy-mf --help
qstrategy-daily-review --help
```

Run a backtest:

```bash
PYTHONPATH=src python3 -m qstrategy_v2.cli \
  --start-date 2024-09-01 \
  --end-date 2026-03-20 \
  --top-n 18 \
  --buffer-rank 30 \
  --rebalance-interval-trade-days 10 \
  --min-holding-trade-days 10 \
  --max-new-positions-per-rebalance 2 \
  --output-dir reports/run_2024_09_to_2026_03_20
```

Generate a daily review:

```bash
PYTHONPATH=src python3 -m qstrategy_v2.daily_workflow \
  --signal-report reports/current_baseline/latest_backtest.json \
  --trade-date 2026-03-25 \
  --top-n 18 \
  --paper-account-name paper_current_baseline_500k
```

## Representative Outputs

- Baseline signal snapshot: `reports/current_baseline/latest_backtest.md`
- Paper account snapshot: `reports/paper_accounts/paper_current_baseline_500k.md`
- Daily review example: `reports/daily_reviews/2026-03-25_review.md`

## Docs

- [Version Roadmap](docs/version_roadmap.md)
- [Repository Workflow](docs/repository_workflow.md)
- [Multi-Factor Research Notes](docs/multifactor_v2.md)
- [Parameter Optimization Plan](docs/parameter_optimization_plan.md)
- [Live Validation Plan](docs/live_validation_plan.md)
- [Daily Review SOP](docs/daily_post_close_review_sop.md)

## Current Status

这个仓库已经不是概念验证，而是一个可以持续迭代的研究底座：

- core code is independent
- historical backtests can run
- parameter search can run
- daily review drafts can be generated
- paper accounts can be tracked continuously

下一阶段最适合继续推进的是：

- 在 `2024-09` 之后的新市场环境里继续做参数和风险优化
- 加强双数据源降级与稳定性检查
- 逐步把研究盘、模拟盘、实盘验证流程完全打通
