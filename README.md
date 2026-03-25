# A股截面多因子策略 V2

这个仓库是从原工作区中独立抽出的 A 股截面多因子量化策略项目，目标是把“研究、回测、复盘、模拟跟踪”沉淀成一个可持续迭代的独立代码库。

当前主线不是旧的题材龙头观察机，而是新的 `qstrategy_v2`：

- 全市场基础过滤与股票池构建
- 多因子打分与截面中性化
- 低频慢换仓组合管理
- 历史回测、参数搜索、日常复盘
- 纸面账户连续跟踪

## 当前策略思路

一句话概括：

`先从 A 股里筛出可交易且流动性足够的股票，再用多因子给它们打分，最后低频、分批地持有排名靠前的一篮子股票。`

当前主线的核心特征：

- 股票池：过滤 `ST`、停牌、次新、无量、一字板等异常样本
- 因子：短期价格行为、20 日换手、20 日波动、`EP`、`ROE_TTM`
- 处理：去极值、板块内标准化、行业/市值/板块中性化
- 组合：`Top N` 持仓、缓冲区卖出、固定调仓频率、最小持有交易日
- 执行：考虑手续费、滑点、整手约束

## 目录结构

```text
src/qstrategy_v2/        核心策略代码
docs/                    研究与执行文档
tests/                   最小测试集
config/                  主题/配置文件
reports/current_baseline 示例基线回测输出
reports/paper_accounts/  示例纸面账户快照
reports/daily_reviews/   示例每日复盘
```

## 快速开始

安装：

```bash
python3 -m pip install -e .
```

需要的环境变量：

```bash
export TUSHARE_TOKEN=...
export EASTMONEY_APIKEY=...
```

常用命令：

```bash
qstrategy-mf --help
qstrategy-daily-review --help
```

回测示例：

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

每日复盘示例：

```bash
PYTHONPATH=src python3 -m qstrategy_v2.daily_workflow \
  --signal-report reports/current_baseline/latest_backtest.json \
  --trade-date 2026-03-25 \
  --top-n 18 \
  --paper-account-name paper_current_baseline_500k
```

## 当前保留的代表性产出

- 基线信号快照：`reports/current_baseline/latest_backtest.md`
- 纸面账户快照：`reports/paper_accounts/paper_current_baseline_500k.md`
- 每日复盘样例：`reports/daily_reviews/2026-03-25_review.md`

## 当前状态

这个仓库已经具备：

- 独立的多因子代码骨架
- 可运行的回测与优化流程
- 每日复盘草稿生成
- 模拟账户连续跟踪

接下来更适合继续做的是：

- 在 `2024-09` 之后的新市场环境下继续调优
- 逐步加强风险控制与实盘验证流程
- 接入更稳的双数据源降级逻辑
