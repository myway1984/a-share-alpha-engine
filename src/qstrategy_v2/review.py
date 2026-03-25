from __future__ import annotations

from collections import defaultdict
from datetime import date
from datetime import timedelta
from pathlib import Path

from .config import MultiFactorConfig
from .data import HistoricalDataProvider
from .models import BacktestResult, DailySnapshot, FactorObservation, HoldingSummary
from .paper_account import PaperAccountState


def write_daily_review_draft(
    result: BacktestResult,
    provider: HistoricalDataProvider,
    config: MultiFactorConfig,
    start_date: date,
    trade_date: date,
    output_dir: Path,
    paper_account: PaperAccountState | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    review_path = output_dir / f"{trade_date.isoformat()}_review.md"

    trade_dates = provider.list_trade_dates(
        start_date=max(start_date - timedelta(days=10), date.fromordinal(1)),
        end_date=trade_date,
    )
    previous_trade_date = trade_dates[-2] if len(trade_dates) >= 2 else None
    next_trade_dates = provider.list_trade_dates(
        start_date=trade_date + timedelta(days=1),
        end_date=trade_date + timedelta(days=14),
    )
    next_trade_date = next_trade_dates[0] if next_trade_dates else None

    todays_snapshots = provider.get_daily_snapshots(trade_date)
    previous_snapshots = (
        provider.get_daily_snapshots(previous_trade_date)
        if previous_trade_date is not None
        else []
    )

    content = render_daily_review_draft(
        result=result,
        config=config,
        trade_date=trade_date,
        next_trade_date=next_trade_date,
        todays_snapshots=todays_snapshots,
        previous_snapshots=previous_snapshots,
        paper_account=paper_account,
    )
    review_path.write_text(content, encoding="utf-8")
    return review_path


def render_daily_review_draft(
    result: BacktestResult,
    config: MultiFactorConfig,
    trade_date: date,
    next_trade_date: date | None,
    todays_snapshots: list[DailySnapshot],
    previous_snapshots: list[DailySnapshot],
    paper_account: PaperAccountState | None = None,
) -> str:
    market = _build_market_summary(todays_snapshots, previous_snapshots)
    holdings = result.latest_holdings
    holdings_by_code = {holding.code: holding for holding in holdings}
    target_codes = {obs.code for obs in result.latest_rankings[: config.top_n]}
    buy_orders = [order for order in result.latest_orders if order.side == "BUY"]
    sell_orders = [order for order in result.latest_orders if order.side == "SELL"]
    buy_reason_by_code = {order.code: order.reason for order in buy_orders}
    sell_reason_by_code = {order.code: order.reason for order in sell_orders}
    latest_nav = result.daily_nav[-1] if result.daily_nav else None
    is_rebalance_window = bool(result.daily_nav) and (
        (len(result.daily_nav) - 1) % config.rebalance_interval_trade_days == 0
    )

    lines = [
        "# A股多因子策略 V2 每日收盘后复盘草稿",
        "",
        "## 1. 基本信息",
        "",
        f"- 交易日：`{trade_date.isoformat()}`",
        f"- 下一交易日：`{next_trade_date.isoformat()}`" if next_trade_date else "- 下一交易日：`待确认`",
        f"- 是否数据完整：`{'是' if todays_snapshots else '否'}`",
        f"- 是否为模型调仓窗口：`{'是' if is_rebalance_window else '否'}`",
        f"- 是否触发模型调仓：`{'是' if result.latest_orders else '否'}`",
        "- 当日结论：`待填写`",
        "",
        "## 2. 大盘复盘",
        "",
        "- 指数表现：",
        "  - 上证：`待补东方财富/指数数据`",
        "  - 深成：`待补东方财富/指数数据`",
        "  - 创业板：`待补东方财富/指数数据`",
        "  - 科创 50：`待补东方财富/指数数据`",
        f"- 全市场成交额：`{_fmt_amount(market['total_amount'])}`",
        f"- 涨跌家数：`上涨 {market['advancers']} / 下跌 {market['decliners']} / 平盘 {market['flat']}`",
        "- 涨停 / 跌停数量：`待补东方财富行情统计`",
        "- 北向资金：`待补东方财富资金数据`",
        "- 今日市场环境判断：`待填写`",
        "",
        "## 3. 板块复盘",
        "",
        f"- 最强行业前 5：{_fmt_ranked_groups(market['top_industries'])}",
        f"- 最弱行业前 5：{_fmt_ranked_groups(market['bottom_industries'])}",
        f"- 板块成交额分布：{_fmt_board_amounts(market['board_amounts'])}",
        "- 今日核心主线：`待填写`",
        "- 今日退潮方向：`待填写`",
        "- 是否出现新的强叙事：`待填写`",
        "- 是否出现明显风险扩散：`待填写`",
        "",
        "## 4A. 模拟账户表现",
        "",
    ]
    if paper_account is not None:
        lines.extend(
            [
                f"- 账户名：`{paper_account.account_name}`",
                f"- 最新净值：`{paper_account.nav:.2f}`",
                f"- 账户收益：`{paper_account.pnl:.2f}` | `{paper_account.pnl_pct:.2%}`",
                f"- 现金：`{paper_account.cash:.2f}`",
                f"- 持仓市值：`{paper_account.market_value:.2f}`",
                f"- 实际建仓只数：`{len(paper_account.positions)}` / 目标 `{paper_account.target_count}`",
                f"- 模拟持仓摘要：{_fmt_paper_positions(paper_account)}",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "- 模拟账户尚未接入：`待补`",
                "",
            ]
        )
    lines.extend(
        [
        "## 4B. 当前组合复盘",
        "",
        f"- 组合净值：`{latest_nav.nav:.4f}`" if latest_nav else "- 组合净值：`待补`",
        f"- 当日收益：`{_fmt_pct(_daily_return(result))}`",
        "- 相对基准表现：`待填写`",
        f"- 当前持仓数：`{len(holdings)}`",
        "- 是否触发风控：`待填写`",
        "",
        "### 持仓检查表",
        "",
        "|股票|名称|权重|板块|行业|是否仍在目标组合|动作|",
        "|---|---|---|---|---|---|---|",
    ])
    for holding in holdings:
        action = "新纳入" if holding.code in buy_reason_by_code else "继续持有"
        lines.append(
            f"|{holding.code}|{holding.name}|{holding.weight:.2%}|{holding.board or '-'}|"
            f"{holding.industry_l1 or '-'}|{'是' if holding.code in target_codes else '否'}|{action}|"
        )
    if not holdings:
        lines.append("|-|-|-|-|-|-|-|")

    lines.extend(
        [
            "",
            "## 5. 模型候选审核",
            "",
            f"- 今日目标前 {config.top_n}：{_fmt_code_list(result.latest_rankings[: config.top_n])}",
            f"- 前 5 核心股票：{_fmt_code_list(result.latest_rankings[:5])}",
            f"- 今日拟买入：{_fmt_order_codes(buy_orders)}",
            f"- 今日拟卖出：{_fmt_order_codes(sell_orders)}",
            f"- 今日观察名单：{_fmt_code_list(result.latest_rankings[config.top_n : config.top_n + 5])}",
            "",
            "### 拟换入理由",
            "",
            "|股票|名称|板块|行业|主要拉分因子|模型原因|是否可执行|",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for order in buy_orders:
        ranking = _find_ranking(result.latest_rankings, order.code)
        holding = holdings_by_code.get(order.code)
        lines.append(
            f"|{order.code}|{ranking.name if ranking else order.code}|{holding.board if holding else '-'}|"
            f"{holding.industry_l1 if holding else '-'}|{_top_factor_names(ranking)}|{order.reason}|待填写|"
        )
    if not buy_orders:
        lines.append("|-|-|-|-|-|今日未触发新增调仓|是|")

    lines.extend(
        [
            "",
            "### 拟换出理由",
            "",
            "|股票|名称|原因|模型原因|资讯原因|是否立即执行|",
            "|---|---|---|---|---|---|",
        ]
    )
    for order in sell_orders:
        lines.append(
            f"|{order.code}|{order.code}|调出目标组合|{sell_reason_by_code.get(order.code, order.reason)}|待填写|待填写|"
        )
    if not sell_orders:
        lines.append("|-|-|今日无卖出指令|模型未触发|待填写|否|")

    lines.extend(
        [
            "",
            "## 6. 资讯复盘",
            "",
            "### 市场级资讯",
            "",
            "- 宏观 / 政策：`待用东方财富资讯搜索补充`",
            "- 监管 / 规则：`待用东方财富资讯搜索补充`",
            "- 盘后重点新闻：`待用东方财富资讯搜索补充`",
            "",
            "### 持仓级资讯",
            "",
            f"- 当前持仓：{_fmt_holdings_for_news(holdings)}",
            "- 持仓重大公告：`待填写`",
            "- 持仓负面风险：`待填写`",
            "- 持仓正面催化：`待填写`",
            "",
            "### 候选级资讯",
            "",
            f"- 拟买入候选：{_fmt_order_codes(buy_orders)}",
            "- 拟买入个股公告：`待填写`",
            "- 拟买入个股新闻 / 研报：`待填写`",
            "- 是否存在一票否决项：`待填写`",
            "",
            "## 7. 风险标记",
            "",
            "- 数据风险：`待填写`",
            "- 交易风险：`待填写`",
            "- 板块过度集中风险：`待填写`",
            "- 个股流动性风险：`待填写`",
            "- 事件性风险：`待填写`",
            "",
            "## 8. 次日执行计划",
            "",
            f"- 是否执行调仓：`{'是' if result.latest_orders else '否'}`",
            f"- 计划买入：{_fmt_order_codes(buy_orders)}",
            f"- 计划卖出：{_fmt_order_codes(sell_orders)}",
            "- 不执行原因：`待填写`",
            "- 盘前需要再次确认的事项：`待填写`",
            "",
            "## 9. 复盘结论",
            "",
            "- 今日一句话结论：`待填写`",
            "- 明日执行原则：`待填写`",
            "- 需要跟踪的重点变量：`待填写`",
            "",
        ]
    )
    return "\n".join(lines)


def _build_market_summary(
    todays_snapshots: list[DailySnapshot],
    previous_snapshots: list[DailySnapshot],
) -> dict[str, object]:
    previous_close_map = {
        snapshot.code: snapshot.close_price
        for snapshot in previous_snapshots
        if snapshot.close_price is not None
    }
    advancers = 0
    decliners = 0
    flat = 0
    total_amount = 0.0
    industry_returns: dict[str, list[float]] = defaultdict(list)
    board_amounts: dict[str, float] = defaultdict(float)

    for snapshot in todays_snapshots:
        total_amount += snapshot.amount or 0.0
        board_amounts[snapshot.board or "unknown"] += snapshot.amount or 0.0
        prev_close = previous_close_map.get(snapshot.code)
        if prev_close and snapshot.close_price:
            daily_return = snapshot.close_price / prev_close - 1.0
            if daily_return > 0:
                advancers += 1
            elif daily_return < 0:
                decliners += 1
            else:
                flat += 1
            if snapshot.industry_l1:
                industry_returns[snapshot.industry_l1].append(daily_return)

    ranked_industries = sorted(
        (
            (industry, sum(returns) / len(returns))
            for industry, returns in industry_returns.items()
            if returns
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    return {
        "advancers": advancers,
        "decliners": decliners,
        "flat": flat,
        "total_amount": total_amount,
        "top_industries": ranked_industries[:5],
        "bottom_industries": list(reversed(ranked_industries[-5:])),
        "board_amounts": sorted(board_amounts.items(), key=lambda item: item[1], reverse=True),
    }


def _fmt_amount(value: float) -> str:
    if value >= 100_000_000:
        return f"{value / 100_000_000:.2f} 亿元"
    if value >= 10_000:
        return f"{value / 10_000:.2f} 万元"
    return f"{value:.2f} 元"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "待补"
    return f"{value:.2%}"


def _fmt_ranked_groups(items: list[tuple[str, float]]) -> str:
    if not items:
        return "`待补`"
    return "；".join(f"`{name} {score:.2%}`" for name, score in items)


def _fmt_board_amounts(items: list[tuple[str, float]]) -> str:
    if not items:
        return "`待补`"
    return "；".join(f"`{board} {_fmt_amount(amount)}`" for board, amount in items)


def _fmt_code_list(observations: list[FactorObservation]) -> str:
    if not observations:
        return "`无`"
    return "、".join(f"`{obs.code} {obs.name}`" for obs in observations)


def _fmt_order_codes(orders) -> str:
    if not orders:
        return "`无`"
    return "、".join(f"`{order.code}`" for order in orders)


def _fmt_holdings_for_news(holdings: list[HoldingSummary]) -> str:
    if not holdings:
        return "`无`"
    return "、".join(f"`{holding.code} {holding.name}`" for holding in holdings[:15])


def _fmt_paper_positions(account: PaperAccountState) -> str:
    if not account.positions:
        return "`无`"
    return "、".join(
        f"`{position.code} {position.name} {position.unrealized_pnl_pct:.2%}`"
        for position in account.positions[:10]
    )


def _top_factor_names(observation: FactorObservation | None) -> str:
    if observation is None or not observation.processed_factors:
        return "-"
    ranked = sorted(
        observation.processed_factors.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    return " / ".join(name for name, value in ranked[:3] if value > 0) or ranked[0][0]


def _find_ranking(
    observations: list[FactorObservation],
    code: str,
) -> FactorObservation | None:
    for observation in observations:
        if observation.code == code:
            return observation
    return None


def _daily_return(result: BacktestResult) -> float | None:
    if len(result.daily_nav) < 2:
        return None
    prev = result.daily_nav[-2].nav
    latest = result.daily_nav[-1].nav
    if prev == 0:
        return None
    return latest / prev - 1.0
