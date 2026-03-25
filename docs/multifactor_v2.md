# A股截面多因子策略 V2

这套 `v2` 目录是和现有 `qstrategy` 主观多头策略并行存在的新框架，目标是承接《A股截面多因子量化选股模型》需求，不侵入旧策略。

## 当前已落地

- 独立包：`src/qstrategy_v2/`
- 数据接口抽象：`HistoricalDataProvider`
- `TushareHistoryProvider` 已接入 `trade_cal / stock_basic / daily / daily_basic / stk_limit / suspend_d / index_classify / index_member_all / income / fina_indicator / disclosure_date`
- 基础股票池过滤：北交所 / ST / 次新 / 停牌 / 无量 / 一字板
- 核心因子入口：`1M_Reversal`、`Turnover_20D`、`Volatility_20D`、`Price_Vol_Corr`、`EP`、`ROE_TTM`、`SUE`
- 截面预处理：MAD 去极值、Z-score、对数总市值 + 申万一级行业哑变量中性化
- 组合层：Top 50 等权、80 名缓冲卖出、T+1 持仓约束接口
- 组合控制：支持因子权重、最小持有天数、按交易日频率调仓
- 回测层：手续费、印花税、滑点、日频净值推进
- 调试入口：支持 `--universe-limit` 做小样本真数据联调
- 财报缓存预热：支持 `--warm-financial-cache` 先预拉首日股票池的财报数据
- 分段回测：支持 `--segment-trade-days`，按交易日分段运行但保留组合状态连续性
- 离线缓存构建：支持 `--build-financial-cache-only`，先打财报缓存再单独跑回测
- 市场缓存构建：支持 `--build-market-cache-only`，先打日线/日频缓存再跑老年份回测
- 参数入口：支持 `--exclude-factors`、`--factor-weights`、`--min-holding-days`、`--rebalance-interval-trade-days`

## 当前研究结论

- `price_volume_corr` 在 `2019 Q2-Q3` 明显偏负贡献，当前默认研究基线建议先剔除。
- 仅靠放宽 `TopN` 或 `buffer` 没有改善，问题不主要在持仓数量。
- 单纯加最小持有天数改善有限，真正有效的是把调仓从日频降到固定交易日频率。
- 当前最佳已知研究基线：
  - `exclude_factors=price_volume_corr`
  - `factor_weights=one_month_reversal=0.75,turnover_20d=0.5,volatility_20d=0.75,ep=1.5,roe_ttm=1.5,sue=0.75`
  - `min_holding_days=10`
  - `rebalance_interval_trade_days=5`
  - `u50 / Top15 / Buffer25`

## 关键验证结果

- `2019 Q2`：
  - 原始 `no_pvcorr` 基线净值 `725663.4397`
  - 防御版周频调仓净值 `910578.7836`
- `2019 Q3`：
  - 原始 `no_pvcorr` 基线净值 `623685.2915`
  - 防御版周频调仓净值 `901547.5755`
- `2019 H2` 连续版：
  - 防御版周频调仓净值 `844685.4903`
  - 平均日换手约 `0.3803`
- `2019` 全年连续版：
  - 防御版周频调仓净值 `1130867.8334`
  - 平均日换手约 `0.3866`
- `2020` 分季度：
  - `Q1` 净值 `960522.8608`
  - `Q2` 净值 `1000822.1215`
  - `Q3` 净值 `979422.9072`
  - `Q4` 净值 `907776.7826`
- `2020` 全年连续版：
  - 防御版周频调仓净值 `828727.0083`
  - 平均日换手约 `0.3663`
  - 说明当前基线已解决高换手问题，但仍未跨年份稳定

## 因子诊断结论

- `2019` 与 `2020` 的同口径因子诊断已完成：
  - `2019` 与 `2020` 的详细诊断结果来自原研究工作区
  - 独立仓库当前仅保留主线基线样例，不再携带完整历史实验目录
- 结论：
  - `sue` 在 `2020` 转为负贡献，`mean_ic` 和 `mean_spread` 都为负。
  - `one_month_reversal` 在 `2020` 仍为正，但强度明显弱于 `2019`。
  - `volatility_20d` 在 `2020` 反而更强，说明低波特征在新环境中更有效。
  - `roe_ttm` 在 `2020` 几乎没有贡献，适合降权而不是继续高配。

## 诊断驱动版候选

- 新候选参数：
  - `exclude_factors=price_volume_corr,sue`
  - `factor_weights=one_month_reversal=1.25,turnover_20d=0.75,volatility_20d=1.5,ep=1.25,roe_ttm=0.5`
  - `min_holding_days=10`
  - `rebalance_interval_trade_days=5`
  - `u50 / Top15 / Buffer25`
- 年度结果：
  - `2019`：从 `1130867.8334` 降到 `1085361.5728`
  - `2020`：从 `828727.0083` 提升到 `941743.3740`
  - 说明这版更均衡，但会牺牲部分 `2019` 超额收益来换取 `2020` 稳定性

## 当前还缺

- Tushare 实盘级字段联调和权限核验
- 更完整的历史 ST 状态判定
- 更严格的复权口径确认
- 更贴近实盘的成交撮合规则

## 推荐下一步

1. 在 `2019-2020` 两年连续区间上对比“防御版基线”和“诊断驱动版候选”。
2. 再把 `u50` 扩到 `u100`，检查结论是样本池问题还是因子问题。
3. 输出分年度表现、换手率、分段回撤、行业暴露和因子贡献诊断。
