from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

from .eastmoney import EastmoneyClient, EastmoneyError

from .backtest import CrossSectionalBacktester
from .config import DEFAULT_BOARD_FACTOR_WEIGHTS, DEFAULT_FACTOR_WEIGHTS, MultiFactorConfig
from .data import (
    DataProviderError,
    EastmoneyHistoryProvider,
    ProviderNotReadyError,
    TushareHistoryProvider,
)
from .diagnostics import FactorDiagnosticsRunner, write_factor_diagnostic_outputs
from .optimizer import (
    parse_int_list,
    run_grid_search,
    run_walk_forward,
    write_grid_search_outputs,
    write_walk_forward_outputs,
)
from .paper_account import (
    fetch_live_quotes_for_targets,
    initialize_equal_weight_account,
    save_paper_account,
)
from .reporting import write_backtest_outputs
from .review import write_daily_review_draft


def parse_factor_weights(raw: str) -> dict[str, float]:
    if not raw.strip():
        return {}
    weights: dict[str, float] = {}
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                f"Invalid factor weight '{item}'. Expected name=value pairs."
            )
        factor_name, value = item.split("=", maxsplit=1)
        weights[factor_name.strip()] = float(value.strip())
    return weights


def parse_board_factor_weights(raw: str) -> dict[str, dict[str, float]]:
    if not raw.strip():
        return {}
    parsed: dict[str, dict[str, float]] = {}
    for board_chunk in raw.split("|"):
        item = board_chunk.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(
                f"Invalid board factor weights '{item}'. Expected board:name=value pairs."
            )
        board, weights_raw = item.split(":", maxsplit=1)
        parsed[board.strip()] = parse_factor_weights(weights_raw.strip())
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the A-share multi-factor strategy v2.")
    parser.add_argument("--start-date", default="2019-01-01", help="Backtest start date.")
    parser.add_argument("--end-date", default="2025-12-31", help="Backtest end date.")
    parser.add_argument("--top-n", type=int, default=50, help="Target holdings count.")
    parser.add_argument(
        "--buffer-rank",
        type=int,
        default=80,
        help="Sell only when held names fall below this rank.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/multifactor_v2",
        help="Directory for backtest outputs.",
    )
    parser.add_argument(
        "--universe-limit",
        type=int,
        default=None,
        help="Optional limit for smoke-testing on a subset of the universe.",
    )
    parser.add_argument(
        "--industry-filter-top-n",
        type=int,
        default=None,
        help="Optional industry trend filter: keep only the top N industries by average lookback return.",
    )
    parser.add_argument(
        "--industry-filter-min-momentum",
        type=float,
        default=None,
        help="Optional industry trend filter: keep only industries above this average lookback return threshold.",
    )
    parser.add_argument(
        "--provider",
        choices=["tushare", "eastmoney"],
        default="tushare",
        help="Historical data provider.",
    )
    parser.add_argument(
        "--warm-financial-cache",
        action="store_true",
        help="Warm the financial cache for the initial universe before running backtest.",
    )
    parser.add_argument(
        "--segment-trade-days",
        type=int,
        default=None,
        help="Run the backtest in contiguous trade-date segments while preserving portfolio state.",
    )
    parser.add_argument(
        "--build-financial-cache-only",
        action="store_true",
        help="Only build the financial cache for the initial universe and exit.",
    )
    parser.add_argument(
        "--build-market-cache-only",
        action="store_true",
        help="Only build the market daily cache for the requested date range and exit.",
    )
    parser.add_argument(
        "--exclude-factors",
        default="",
        help="Comma-separated factor names to exclude, e.g. sue,price_volume_corr",
    )
    parser.add_argument(
        "--factor-weights",
        default="",
        help="Comma-separated factor weights, e.g. ep=1.5,roe_ttm=1.5,turnover_20d=0.5",
    )
    parser.add_argument(
        "--board-factor-weights",
        default="",
        help=(
            "Board-specific weights, e.g. "
            "main:ep=1.5,roe_ttm=1.0|gem:one_month_reversal=1.5,volatility_20d=1.5|"
            "star:one_month_reversal=1.75,volatility_20d=1.75,ep=0.5"
        ),
    )
    parser.add_argument(
        "--min-holding-days",
        type=int,
        default=0,
        help="Minimum holding days before selling a ranked position below the buffer.",
    )
    parser.add_argument(
        "--min-holding-trade-days",
        type=int,
        default=None,
        help="Minimum holding period measured in trade days. Overrides --min-holding-days when set.",
    )
    parser.add_argument(
        "--rebalance-interval-trade-days",
        type=int,
        default=1,
        help="Only rebalance every N trade days while still marking portfolio daily.",
    )
    parser.add_argument(
        "--max-new-positions-per-rebalance",
        type=int,
        default=None,
        help="Cap the number of newly opened positions at each rebalance.",
    )
    parser.add_argument(
        "--analyze-factors-only",
        action="store_true",
        help="Only run factor diagnostics and write factor diagnostic outputs.",
    )
    parser.add_argument(
        "--diagnostic-horizon-trade-days",
        type=int,
        default=None,
        help="Forward holding horizon used for factor diagnostics. Defaults to rebalance interval.",
    )
    parser.add_argument(
        "--diagnostic-board",
        choices=["main", "gem", "star"],
        default=None,
        help="Optional board filter for factor diagnostics.",
    )
    parser.add_argument(
        "--generate-daily-review",
        action="store_true",
        help="Generate a post-close daily review draft from the latest strategy state.",
    )
    parser.add_argument(
        "--daily-review-output-dir",
        default="reports/daily_reviews",
        help="Directory for generated daily review drafts.",
    )
    parser.add_argument(
        "--init-paper-account",
        action="store_true",
        help="Initialize a paper account from the latest target holdings using Eastmoney live quotes.",
    )
    parser.add_argument(
        "--paper-account-name",
        default="paper_500k",
        help="Paper account name.",
    )
    parser.add_argument(
        "--paper-account-initial-capital",
        type=float,
        default=500000.0,
        help="Initial capital for the paper account.",
    )
    parser.add_argument(
        "--paper-account-output-dir",
        default="reports/paper_accounts",
        help="Directory for paper account snapshots.",
    )
    parser.add_argument(
        "--optimize-grid-only",
        action="store_true",
        help="Run the first-round portfolio parameter grid search and exit.",
    )
    parser.add_argument(
        "--walk-forward-only",
        action="store_true",
        help="Run walk-forward validation and exit.",
    )
    parser.add_argument(
        "--grid-search-output-dir",
        default="reports/multifactor_v2_grid_search",
        help="Directory for grid search outputs.",
    )
    parser.add_argument(
        "--walk-forward-output-dir",
        default="reports/multifactor_v2_walk_forward",
        help="Directory for walk-forward outputs.",
    )
    parser.add_argument(
        "--walk-forward-train-months",
        type=int,
        default=24,
        help="Training window length in months for walk-forward validation.",
    )
    parser.add_argument(
        "--walk-forward-test-months",
        type=int,
        default=3,
        help="Test window length in months for walk-forward validation.",
    )
    parser.add_argument(
        "--walk-forward-step-months",
        type=int,
        default=3,
        help="Rolling step length in months for walk-forward validation.",
    )
    parser.add_argument(
        "--grid-top-n-values",
        default="12,15,18",
        help="Comma-separated Top N candidates for grid search.",
    )
    parser.add_argument(
        "--grid-buffer-rank-values",
        default="20,25,30",
        help="Comma-separated buffer rank candidates for grid search.",
    )
    parser.add_argument(
        "--grid-rebalance-interval-values",
        default="5,7,10",
        help="Comma-separated rebalance interval candidates for grid search.",
    )
    parser.add_argument(
        "--grid-min-holding-trade-day-values",
        default="10,15",
        help="Comma-separated min holding trade day candidates for grid search.",
    )
    parser.add_argument(
        "--grid-max-new-position-values",
        default="2,3,4",
        help="Comma-separated max new position candidates for grid search.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.rebalance_interval_trade_days <= 0:
        parser.error("--rebalance-interval-trade-days must be > 0")
    if args.diagnostic_horizon_trade_days is not None and args.diagnostic_horizon_trade_days <= 0:
        parser.error("--diagnostic-horizon-trade-days must be > 0")
    if args.walk_forward_train_months <= 0:
        parser.error("--walk-forward-train-months must be > 0")
    if args.walk_forward_test_months <= 0:
        parser.error("--walk-forward-test-months must be > 0")
    if args.walk_forward_step_months <= 0:
        parser.error("--walk-forward-step-months must be > 0")
    excluded_factors = tuple(
        item.strip()
        for item in args.exclude_factors.split(",")
        if item.strip()
    )
    factor_weights = dict(DEFAULT_FACTOR_WEIGHTS)
    factor_weights.update(parse_factor_weights(args.factor_weights))
    board_factor_weights = {
        board: dict(weights)
        for board, weights in DEFAULT_BOARD_FACTOR_WEIGHTS.items()
    }
    parsed_board_weights = parse_board_factor_weights(args.board_factor_weights)
    for board, weights in parsed_board_weights.items():
        board_factor_weights.setdefault(board, {})
        board_factor_weights[board].update(weights)
    config = MultiFactorConfig(
        top_n=args.top_n,
        buffer_rank=args.buffer_rank,
        min_holding_days=args.min_holding_days,
        min_holding_trade_days=args.min_holding_trade_days,
        rebalance_interval_trade_days=args.rebalance_interval_trade_days,
        max_new_positions_per_rebalance=args.max_new_positions_per_rebalance,
        output_dir=Path(args.output_dir),
        universe_limit=args.universe_limit,
        industry_filter_top_n=args.industry_filter_top_n,
        industry_filter_min_momentum=args.industry_filter_min_momentum,
        excluded_factors=excluded_factors,
        factor_weights=factor_weights,
        board_factor_weights=board_factor_weights,
    )
    try:
        if args.provider == "tushare":
            provider = TushareHistoryProvider.from_env()
        else:
            client = EastmoneyClient.from_env()
            provider = EastmoneyHistoryProvider(client)
        backtester = CrossSectionalBacktester(provider=provider, config=config)
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
        if args.build_market_cache_only:
            warmed = backtester.prepare_market_cache(
                start_date=start_date,
                end_date=end_date,
            )
            print(f"Warmed market cache for {warmed} trade dates.")
            return 0
        if args.build_financial_cache_only:
            warmed = backtester.prepare_financial_cache(
                start_date=start_date,
                end_date=end_date,
            )
            print(f"Warmed financial cache for {warmed} stocks.")
            return 0
        if args.analyze_factors_only:
            diagnostics = FactorDiagnosticsRunner(provider=provider, config=config)
            report = diagnostics.run(
                start_date=start_date,
                end_date=end_date,
                horizon_trade_days=args.diagnostic_horizon_trade_days,
                board=args.diagnostic_board,
            )
            json_path, md_path = write_factor_diagnostic_outputs(report, config.output_dir)
            print(f"Generated factor diagnostic report: {md_path}")
            print(f"Generated factor diagnostic data:   {json_path}")
            return 0
        if args.optimize_grid_only:
            results = run_grid_search(
                provider=provider,
                base_config=config,
                start_date=start_date,
                end_date=end_date,
                top_n_values=parse_int_list(args.grid_top_n_values),
                buffer_rank_values=parse_int_list(args.grid_buffer_rank_values),
                rebalance_interval_values=parse_int_list(args.grid_rebalance_interval_values),
                min_holding_trade_day_values=parse_int_list(args.grid_min_holding_trade_day_values),
                max_new_position_values=parse_int_list(args.grid_max_new_position_values),
            )
            json_path, md_path = write_grid_search_outputs(
                results,
                Path(args.grid_search_output_dir),
            )
            print(f"Generated grid search report: {md_path}")
            print(f"Generated grid search data:   {json_path}")
            if results:
                best = results[0]
                print(
                    "Best candidate: "
                    f"top_n={best.candidate.top_n}, "
                    f"buffer_rank={best.candidate.buffer_rank}, "
                    f"rebalance={best.candidate.rebalance_interval_trade_days}, "
                    f"min_hold={best.candidate.min_holding_trade_days}, "
                    f"max_new={best.candidate.max_new_positions_per_rebalance}"
                )
                print(
                    "Best metrics: "
                    f"return={best.metrics.total_return:.2%}, "
                    f"max_drawdown={best.metrics.max_drawdown:.2%}, "
                    f"avg_turnover={best.metrics.avg_turnover:.4f}, "
                    f"score={best.metrics.score:.4f}"
                )
            return 0
        if args.walk_forward_only:
            walk_forward_output_dir = Path(args.walk_forward_output_dir)

            def checkpoint(report) -> None:
                write_walk_forward_outputs(
                    report,
                    walk_forward_output_dir,
                )
                if report.windows:
                    latest = report.windows[-1]
                    print(
                        "Completed window: "
                        f"train={latest.train_start.isoformat()}~{latest.train_end.isoformat()} "
                        f"test={latest.test_start.isoformat()}~{latest.test_end.isoformat()} "
                        f"oos_return={latest.test_metrics.total_return:.2%} "
                        f"oos_drawdown={latest.test_metrics.max_drawdown:.2%}"
                    )

            report = run_walk_forward(
                provider=provider,
                base_config=config,
                start_date=start_date,
                end_date=end_date,
                train_months=args.walk_forward_train_months,
                test_months=args.walk_forward_test_months,
                step_months=args.walk_forward_step_months,
                top_n_values=parse_int_list(args.grid_top_n_values),
                buffer_rank_values=parse_int_list(args.grid_buffer_rank_values),
                rebalance_interval_values=parse_int_list(args.grid_rebalance_interval_values),
                min_holding_trade_day_values=parse_int_list(args.grid_min_holding_trade_day_values),
                max_new_position_values=parse_int_list(args.grid_max_new_position_values),
                progress_callback=checkpoint,
            )
            json_path, md_path = write_walk_forward_outputs(
                report,
                walk_forward_output_dir,
            )
            print(f"Generated walk-forward report: {md_path}")
            print(f"Generated walk-forward data:   {json_path}")
            if report.windows:
                print(
                    "Aggregate out-of-sample: "
                    f"return={report.aggregate_test_metrics.total_return:.2%}, "
                    f"max_drawdown={report.aggregate_test_metrics.max_drawdown:.2%}, "
                    f"avg_turnover={report.aggregate_test_metrics.avg_turnover:.4f}, "
                    f"score={report.aggregate_test_metrics.score:.4f}"
                )
            return 0
        if args.segment_trade_days:
            result = backtester.run_segmented(
                start_date=start_date,
                end_date=end_date,
                segment_trade_days=args.segment_trade_days,
                warm_financial_cache=args.warm_financial_cache,
            )
        else:
            if args.warm_financial_cache:
                warmed = backtester.prepare_financial_cache(
                    start_date=start_date,
                    end_date=end_date,
                )
                print(f"Warmed financial cache for {warmed} stocks.")
            result = backtester.run(
                start_date=start_date,
                end_date=end_date,
            )
        json_path, md_path = write_backtest_outputs(result, config.output_dir)
        paper_account_state = None
        paper_account_json_path = None
        paper_account_md_path = None
        if args.init_paper_account:
            eastmoney_client = EastmoneyClient.from_env()
            next_trade_dates = provider.list_trade_dates(
                start_date=end_date,
                end_date=date.fromordinal(end_date.toordinal() + 14),
            )
            paper_trade_date = end_date
            for candidate in next_trade_dates:
                if candidate > end_date:
                    paper_trade_date = candidate
                    break
            quotes = fetch_live_quotes_for_targets(
                eastmoney_client,
                result.latest_holdings,
            )
            paper_account_state = initialize_equal_weight_account(
                account_name=args.paper_account_name,
                trade_date=paper_trade_date,
                strategy_date=end_date,
                initial_capital=args.paper_account_initial_capital,
                targets=result.latest_holdings,
                quotes=quotes,
                buy_fee_rate=config.buy_fee_rate,
                slippage_rate=config.slippage_rate,
            )
            paper_account_json_path, paper_account_md_path = save_paper_account(
                paper_account_state,
                Path(args.paper_account_output_dir),
            )
        review_path = None
        if args.generate_daily_review:
            review_path = write_daily_review_draft(
                result=result,
                provider=provider,
                config=config,
                start_date=start_date,
                trade_date=end_date,
                output_dir=Path(args.daily_review_output_dir),
                paper_account=paper_account_state,
            )
    except DataProviderError as exc:
        print(f"[data-provider-error] {exc}", file=sys.stderr)
        return 2
    except ProviderNotReadyError as exc:
        print(f"[provider-not-ready] {exc}", file=sys.stderr)
        return 2
    except EastmoneyError as exc:
        print(f"[eastmoney-error] {exc}", file=sys.stderr)
        return 2

    print(f"Generated v2 report: {md_path}")
    print(f"Generated v2 data:   {json_path}")
    if args.init_paper_account:
        print(f"Generated paper account report: {paper_account_md_path}")
        print(f"Generated paper account data:   {paper_account_json_path}")
    if args.generate_daily_review:
        print(f"Generated daily review draft: {review_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
