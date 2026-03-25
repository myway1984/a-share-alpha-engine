from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import replace
from datetime import date
from pathlib import Path
from collections import deque
from itertools import product
import json

from .backtest import CrossSectionalBacktester
from .config import MultiFactorConfig
from .models import BacktestResult, PortfolioSnapshot


@dataclass(slots=True)
class GridSearchCandidate:
    top_n: int
    buffer_rank: int
    rebalance_interval_trade_days: int
    min_holding_trade_days: int | None
    max_new_positions_per_rebalance: int | None


@dataclass(slots=True)
class GridSearchMetrics:
    start_nav: float
    end_nav: float
    total_return: float
    max_drawdown: float
    avg_turnover: float
    positive_month_ratio: float
    score: float
    trade_days: int


@dataclass(slots=True)
class GridSearchResult:
    candidate: GridSearchCandidate
    metrics: GridSearchMetrics


@dataclass(slots=True)
class PreparedTradeDay:
    trade_date: date
    close_map: dict[str, float]
    rankings: list


def parse_int_list(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(int(item))
    return values


def run_grid_search(
    provider,
    base_config: MultiFactorConfig,
    start_date: date,
    end_date: date,
    top_n_values: list[int],
    buffer_rank_values: list[int],
    rebalance_interval_values: list[int],
    min_holding_trade_day_values: list[int],
    max_new_position_values: list[int],
) -> list[GridSearchResult]:
    prepared_days, initial_previous_closes = prepare_grid_search_inputs(
        provider=provider,
        base_config=base_config,
        start_date=start_date,
        end_date=end_date,
    )
    results: list[GridSearchResult] = []
    for values in product(
        top_n_values,
        buffer_rank_values,
        rebalance_interval_values,
        min_holding_trade_day_values,
        max_new_position_values,
    ):
        candidate = GridSearchCandidate(
            top_n=values[0],
            buffer_rank=values[1],
            rebalance_interval_trade_days=values[2],
            min_holding_trade_days=values[3],
            max_new_positions_per_rebalance=values[4],
        )
        if not is_valid_candidate(candidate):
            continue
        result = simulate_candidate(
            candidate=candidate,
            base_config=base_config,
            prepared_days=prepared_days,
            initial_previous_closes=initial_previous_closes,
        )
        metrics = summarize_backtest(result)
        results.append(GridSearchResult(candidate=candidate, metrics=metrics))

    results.sort(
        key=lambda item: (
            item.metrics.score,
            item.metrics.total_return,
            -item.metrics.max_drawdown,
        ),
        reverse=True,
    )
    return results


def prepare_grid_search_inputs(
    provider,
    base_config: MultiFactorConfig,
    start_date: date,
    end_date: date,
) -> tuple[list[PreparedTradeDay], dict[str, float]]:
    backtester = CrossSectionalBacktester(provider=provider, config=base_config)
    state, trade_dates = backtester._build_initial_state(start_date=start_date, end_date=end_date)
    history_window: deque[list] = deque(state.history_window, maxlen=base_config.lookback_days)
    prepared_days: list[PreparedTradeDay] = []
    for trade_date in trade_dates:
        snapshots = provider.get_daily_snapshots(trade_date)
        history_window.append(snapshots)
        close_map = {
            snapshot.code: snapshot.close_price
            for snapshot in snapshots
            if snapshot.close_price is not None
        }
        universe = backtester.selector.select(snapshots)
        universe = backtester.selector.apply_limit(universe)
        universe = backtester.selector.apply_industry_trend_filter(
            universe,
            history_window=list(history_window),
        )
        raw_observations = backtester.factor_engine.build_cross_section(
            trade_date=trade_date,
            universe=universe,
            provider=provider,
            history_window=list(history_window),
        )
        rankings = backtester.preprocessor.transform(raw_observations)
        prepared_days.append(
            PreparedTradeDay(
                trade_date=trade_date,
                close_map=close_map,
                rankings=rankings,
            )
        )
    return prepared_days, dict(state.previous_closes)


def simulate_candidate(
    candidate: GridSearchCandidate,
    base_config: MultiFactorConfig,
    prepared_days: list[PreparedTradeDay],
    initial_previous_closes: dict[str, float],
) -> BacktestResult:
    config = replace(
        base_config,
        top_n=candidate.top_n,
        buffer_rank=candidate.buffer_rank,
        rebalance_interval_trade_days=candidate.rebalance_interval_trade_days,
        min_holding_trade_days=candidate.min_holding_trade_days,
        max_new_positions_per_rebalance=candidate.max_new_positions_per_rebalance,
    )
    portfolio_manager = CrossSectionalBacktester(provider=None, config=config).portfolio_manager
    nav = config.initial_capital
    previous_closes = dict(initial_previous_closes)
    positions = {}
    daily_nav: list[PortfolioSnapshot] = []
    latest_rankings = []
    latest_orders = []
    trade_days_processed = 0

    for prepared in prepared_days:
        portfolio_return = 0.0
        for code, position in positions.items():
            current_close = prepared.close_map.get(code)
            previous_close = previous_closes.get(code)
            if not current_close or not previous_close:
                continue
            portfolio_return += position.weight * (current_close / previous_close - 1.0)
        nav *= 1.0 + portfolio_return

        latest_rankings = prepared.rankings
        latest_orders = []
        turnover = 0.0
        should_rebalance = (
            trade_days_processed % config.rebalance_interval_trade_days == 0
        )
        if should_rebalance:
            positions, latest_orders, turnover = portfolio_manager.rebalance(
                trade_date=prepared.trade_date,
                ranked=prepared.rankings,
                current_positions=positions,
                trade_day_index=trade_days_processed,
            )
            nav -= transaction_cost(nav=nav, orders=latest_orders, config=config)
        previous_closes = prepared.close_map
        daily_nav.append(
            PortfolioSnapshot(
                trade_date=prepared.trade_date,
                nav=nav,
                turnover=turnover,
                holdings=len(positions),
            )
        )
        trade_days_processed += 1

    return BacktestResult(
        daily_nav=daily_nav,
        latest_rankings=latest_rankings,
        latest_orders=latest_orders,
        latest_holdings=[],
    )


def is_valid_candidate(candidate: GridSearchCandidate) -> bool:
    if candidate.buffer_rank < candidate.top_n + 5:
        return False
    if candidate.rebalance_interval_trade_days <= 0:
        return False
    if candidate.min_holding_trade_days is not None:
        if candidate.min_holding_trade_days < candidate.rebalance_interval_trade_days:
            return False
    if candidate.max_new_positions_per_rebalance is not None:
        if candidate.max_new_positions_per_rebalance <= 0:
            return False
        if candidate.max_new_positions_per_rebalance > max(1, candidate.top_n // 3):
            return False
    return True


def summarize_backtest(result: BacktestResult) -> GridSearchMetrics:
    if not result.daily_nav:
        return GridSearchMetrics(
            start_nav=0.0,
            end_nav=0.0,
            total_return=0.0,
            max_drawdown=0.0,
            avg_turnover=0.0,
            positive_month_ratio=0.0,
            score=float("-inf"),
            trade_days=0,
        )
    navs = result.daily_nav
    start_nav = navs[0].nav
    end_nav = navs[-1].nav
    total_return = end_nav / start_nav - 1.0 if start_nav else 0.0
    avg_turnover = sum(snapshot.turnover for snapshot in navs) / len(navs)
    max_drawdown = compute_max_drawdown([snapshot.nav for snapshot in navs])
    positive_month_ratio = compute_positive_month_ratio(result)
    score = (
        total_return * 0.4
        - max_drawdown * 0.3
        - avg_turnover * 0.15
        + positive_month_ratio * 0.15
    )
    return GridSearchMetrics(
        start_nav=start_nav,
        end_nav=end_nav,
        total_return=total_return,
        max_drawdown=max_drawdown,
        avg_turnover=avg_turnover,
        positive_month_ratio=positive_month_ratio,
        score=score,
        trade_days=len(navs),
    )


def compute_max_drawdown(navs: list[float]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for nav in navs:
        peak = max(peak, nav)
        if peak <= 0:
            continue
        drawdown = 1.0 - nav / peak
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown


def compute_positive_month_ratio(result: BacktestResult) -> float:
    monthly_navs: dict[str, tuple[float, float]] = {}
    for snapshot in result.daily_nav:
        key = snapshot.trade_date.strftime("%Y-%m")
        start_nav, _ = monthly_navs.get(key, (snapshot.nav, snapshot.nav))
        monthly_navs[key] = (start_nav, snapshot.nav)
    if not monthly_navs:
        return 0.0
    positive_months = 0
    for start_nav, end_nav in monthly_navs.values():
        if start_nav > 0 and end_nav / start_nav - 1.0 > 0:
            positive_months += 1
    return positive_months / len(monthly_navs)


def transaction_cost(nav: float, orders, config: MultiFactorConfig) -> float:
    buy_turnover = sum(
        max(order.to_weight - order.from_weight, 0.0)
        for order in orders
    )
    sell_turnover = sum(
        max(order.from_weight - order.to_weight, 0.0)
        for order in orders
    )
    total_rate = (
        buy_turnover * (config.buy_fee_rate + config.slippage_rate)
        + sell_turnover * (config.sell_fee_rate + config.slippage_rate)
    )
    return nav * total_rate


def write_grid_search_outputs(
    results: list[GridSearchResult],
    output_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest_grid_search.json"
    md_path = output_dir / "latest_grid_search.md"
    payload = {
        "results": [
            {
                "candidate": asdict(item.candidate),
                "metrics": {
                    "start_nav": round(item.metrics.start_nav, 4),
                    "end_nav": round(item.metrics.end_nav, 4),
                    "total_return": round(item.metrics.total_return, 6),
                    "max_drawdown": round(item.metrics.max_drawdown, 6),
                    "avg_turnover": round(item.metrics.avg_turnover, 6),
                    "positive_month_ratio": round(item.metrics.positive_month_ratio, 6),
                    "score": round(item.metrics.score, 6),
                    "trade_days": item.metrics.trade_days,
                },
            }
            for item in results
        ]
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_grid_search_markdown(results), encoding="utf-8")
    return json_path, md_path


def render_grid_search_markdown(results: list[GridSearchResult]) -> str:
    lines = ["# 组合层参数网格搜索摘要", ""]
    if not results:
        lines.append("- 暂无结果。")
        return "\n".join(lines) + "\n"
    best = results[0]
    lines.append(
        f"- 最优组合：`top_n={best.candidate.top_n}` / "
        f"`buffer_rank={best.candidate.buffer_rank}` / "
        f"`rebalance_interval_trade_days={best.candidate.rebalance_interval_trade_days}` / "
        f"`min_holding_trade_days={best.candidate.min_holding_trade_days}` / "
        f"`max_new_positions_per_rebalance={best.candidate.max_new_positions_per_rebalance}`"
    )
    lines.append(
        f"- 最优结果：收益 `{best.metrics.total_return:.2%}` | "
        f"最大回撤 `{best.metrics.max_drawdown:.2%}` | "
        f"平均日换手 `{best.metrics.avg_turnover:.4f}` | "
        f"月度正收益占比 `{best.metrics.positive_month_ratio:.2%}` | "
        f"综合分 `{best.metrics.score:.4f}`"
    )
    lines.append("")
    lines.append("## Top 10")
    lines.append("")
    for index, item in enumerate(results[:10], start=1):
        lines.append(
            f"{index}. `top_n={item.candidate.top_n}` / "
            f"`buffer_rank={item.candidate.buffer_rank}` / "
            f"`rebalance={item.candidate.rebalance_interval_trade_days}` / "
            f"`min_hold={item.candidate.min_holding_trade_days}` / "
            f"`max_new={item.candidate.max_new_positions_per_rebalance}` | "
            f"收益 `{item.metrics.total_return:.2%}` | "
            f"回撤 `{item.metrics.max_drawdown:.2%}` | "
            f"换手 `{item.metrics.avg_turnover:.4f}` | "
            f"月胜率 `{item.metrics.positive_month_ratio:.2%}` | "
            f"分数 `{item.metrics.score:.4f}`"
        )
    lines.append("")
    return "\n".join(lines) + "\n"
