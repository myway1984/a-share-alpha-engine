from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import json

from .eastmoney import EastmoneyClient, EastmoneyError

from .config import MultiFactorConfig
from .data import DataProviderError, ProviderNotReadyError, TushareHistoryProvider
from .models import BacktestResult, FactorObservation, HoldingSummary, Order, PortfolioSnapshot
from .paper_account import (
    build_quote_targets_from_account,
    fetch_live_quotes_for_targets,
    initialize_equal_weight_account,
    load_paper_account,
    LiveQuote,
    mark_to_market_account,
    save_paper_account,
)
from .review import write_daily_review_draft


DEFAULT_SIGNAL_REPORT = (
    "reports/multifactor_v2_2026_ytd_slowrebalance_3_u50/latest_backtest.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the daily post-close review workflow from the latest strategy signal report."
    )
    parser.add_argument(
        "--signal-report",
        default=DEFAULT_SIGNAL_REPORT,
        help="Path to the latest strategy report JSON.",
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="Trade date for the daily review and simulated account. Defaults to today.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=15,
        help="Number of target holdings to use from the signal report.",
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
        "--daily-review-output-dir",
        default="reports/daily_reviews",
        help="Directory for daily review drafts.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    signal_report = Path(args.signal_report)
    if not signal_report.exists():
        parser.error(f"--signal-report not found: {signal_report}")

    trade_date = date.fromisoformat(args.trade_date) if args.trade_date else date.today()

    try:
        report = json.loads(signal_report.read_text(encoding="utf-8"))
        strategy_date = _infer_strategy_date(report)
        provider = TushareHistoryProvider.from_env()
        strategy_snapshots = {
            snapshot.code: snapshot
            for snapshot in provider.get_daily_snapshots(strategy_date)
        }
        holdings, rankings, orders = _build_targets(
            report=report,
            provider=provider,
            strategy_date=strategy_date,
            top_n=args.top_n,
        )
        account_path = Path(args.paper_account_output_dir) / f"{args.paper_account_name}.json"
        client = EastmoneyClient.from_env()
        if account_path.exists():
            existing_account = load_paper_account(account_path)
            if existing_account.positions:
                quote_targets = build_quote_targets_from_account(existing_account)
                try:
                    quotes = fetch_live_quotes_for_targets(client, quote_targets)
                except EastmoneyError:
                    quotes = {}
                quotes = _fill_missing_quotes_from_snapshots(
                    targets=quote_targets,
                    quotes=quotes,
                    snapshots=strategy_snapshots.values(),
                )
                account = mark_to_market_account(
                    state=existing_account,
                    trade_date=trade_date,
                    strategy_date=strategy_date,
                    quotes=quotes,
                )
            else:
                try:
                    quotes = fetch_live_quotes_for_targets(client, holdings)
                except EastmoneyError:
                    quotes = {}
                quotes = _fill_missing_quotes_from_snapshots(
                    targets=holdings,
                    quotes=quotes,
                    snapshots=strategy_snapshots.values(),
                )
                account = initialize_equal_weight_account(
                    account_name=args.paper_account_name,
                    trade_date=trade_date,
                    strategy_date=strategy_date,
                    initial_capital=args.paper_account_initial_capital,
                    targets=holdings,
                    quotes=quotes,
                    buy_fee_rate=0.0002,
                    slippage_rate=0.002,
                )
        else:
            try:
                quotes = fetch_live_quotes_for_targets(client, holdings)
            except EastmoneyError:
                quotes = {}
            quotes = _fill_missing_quotes_from_snapshots(
                targets=holdings,
                quotes=quotes,
                snapshots=strategy_snapshots.values(),
            )
            account = initialize_equal_weight_account(
                account_name=args.paper_account_name,
                trade_date=trade_date,
                strategy_date=strategy_date,
                initial_capital=args.paper_account_initial_capital,
                targets=holdings,
                quotes=quotes,
                buy_fee_rate=0.0002,
                slippage_rate=0.002,
            )
        paper_json_path, paper_md_path = save_paper_account(
            account,
            Path(args.paper_account_output_dir),
        )
        result = BacktestResult(
            daily_nav=[
                PortfolioSnapshot(
                    trade_date=date.fromisoformat(item["trade_date"]),
                    nav=float(item["nav"]),
                    turnover=float(item["turnover"]),
                    holdings=int(item["holdings"]),
                )
                for item in report.get("daily_nav", [])
            ],
            latest_rankings=rankings,
            latest_orders=orders,
            latest_holdings=holdings,
        )
        review_path = write_daily_review_draft(
            result=result,
            provider=provider,
            config=MultiFactorConfig(top_n=args.top_n, rebalance_interval_trade_days=5),
            start_date=date(strategy_date.year, 1, 1),
            trade_date=trade_date,
            output_dir=Path(args.daily_review_output_dir),
            paper_account=account,
        )
    except (DataProviderError, ProviderNotReadyError, EastmoneyError) as exc:
        print(f"[daily-workflow-error] {exc}")
        return 2

    print(f"Signal report:               {signal_report}")
    print(f"Strategy date:              {strategy_date.isoformat()}")
    print(f"Review trade date:          {trade_date.isoformat()}")
    print(f"Generated paper account:    {paper_md_path}")
    print(f"Generated paper account js: {paper_json_path}")
    print(f"Generated daily review:     {review_path}")
    print(f"Paper account nav:          {account.nav:.2f}")
    print(f"Paper account pnl:          {account.pnl:.2f} ({account.pnl_pct:.2%})")
    print(f"Paper account holdings:     {len(account.positions)} / {account.target_count}")
    return 0


def _infer_strategy_date(report: dict) -> date:
    daily_nav = report.get("daily_nav") or []
    if daily_nav:
        return date.fromisoformat(daily_nav[-1]["trade_date"])
    latest_orders = report.get("latest_orders") or []
    if latest_orders:
        return date.fromisoformat(latest_orders[-1]["trade_date"])
    raise ValueError("Signal report does not contain daily_nav or latest_orders.")


def _build_targets(
    report: dict,
    provider: TushareHistoryProvider,
    strategy_date: date,
    top_n: int,
) -> tuple[list[HoldingSummary], list[FactorObservation], list[Order]]:
    snapshots = {snapshot.code: snapshot for snapshot in provider.get_daily_snapshots(strategy_date)}
    rankings: list[FactorObservation] = []
    latest_rankings = report.get("latest_rankings") or []
    if latest_rankings:
        source_items = latest_rankings[:top_n]
    else:
        source_items = [
            {
                "code": item["code"],
                "name": item["name"],
                "score": item.get("total_score", 0.0) or 0.0,
                "processed_factors": {},
            }
            for item in (report.get("latest_holdings") or [])[:top_n]
        ]
    for item in source_items:
        snapshot = snapshots.get(item["code"])
        rankings.append(
            FactorObservation(
                code=item["code"],
                name=item["name"],
                trade_date=strategy_date,
                industry_l1=snapshot.industry_l1 if snapshot else None,
                total_market_cap=snapshot.total_market_cap if snapshot else None,
                board=snapshot.board if snapshot else None,
                processed_factors=item.get("processed_factors", {}),
                total_score=float(item.get("score", 0.0)),
            )
        )
    holdings = [
        HoldingSummary(
            code=ranking.code,
            name=ranking.name,
            weight=1.0 / len(rankings) if rankings else 0.0,
            board=ranking.board,
            industry_l1=ranking.industry_l1,
            total_score=ranking.total_score,
        )
        for ranking in rankings
    ]
    orders = [
        Order(
            trade_date=date.fromisoformat(item["trade_date"]),
            code=item["code"],
            side=item["side"],
            from_weight=float(item["from_weight"]),
            to_weight=float(item["to_weight"]),
            reason=item["reason"],
        )
        for item in report.get("latest_orders", [])
    ]
    return holdings, rankings, orders


def _fill_missing_quotes_from_snapshots(
    targets: list[HoldingSummary],
    quotes: dict[str, LiveQuote],
    snapshots,
) -> dict[str, LiveQuote]:
    completed = dict(quotes)
    snapshot_map = {snapshot.code: snapshot for snapshot in snapshots}
    for target in targets:
        if target.code in completed:
            continue
        snapshot = snapshot_map.get(target.code)
        if snapshot is None or snapshot.close_price is None:
            continue
        reference_open = snapshot.open_price if snapshot.open_price is not None else snapshot.close_price
        completed[target.code] = LiveQuote(
            code=target.code,
            name=target.name,
            latest_price=snapshot.close_price,
            open_price=reference_open,
        )
    return completed


if __name__ == "__main__":
    raise SystemExit(main())
