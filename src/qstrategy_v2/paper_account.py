from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
import math
import time

from .eastmoney import EastmoneyClient
from .eastmoney import EastmoneyError

from .models import HoldingSummary


@dataclass(slots=True)
class LiveQuote:
    code: str
    name: str
    latest_price: float | None
    open_price: float | None


@dataclass(slots=True)
class PaperAccountPosition:
    code: str
    name: str
    board: str | None
    industry_l1: str | None
    shares: int
    entry_price: float
    latest_price: float
    market_value: float
    cost_basis: float
    unrealized_pnl: float
    unrealized_pnl_pct: float


@dataclass(slots=True)
class PaperTrade:
    trade_date: str
    code: str
    name: str
    side: str
    shares: int
    reference_price: float
    execution_price: float
    gross_amount: float
    fee: float
    total_cash_flow: float
    slippage_rate: float


@dataclass(slots=True)
class PaperAccountState:
    account_name: str
    trade_date: str
    strategy_date: str
    initial_capital: float
    cash: float
    market_value: float
    nav: float
    total_cost: float
    pnl: float
    pnl_pct: float
    target_count: int
    positions: list[PaperAccountPosition]
    trades: list[PaperTrade]


def fetch_live_quotes(client: EastmoneyClient, identifiers: list[str]) -> dict[str, LiveQuote]:
    quotes: dict[str, LiveQuote] = {}
    chunk_size = 4
    for index in range(0, len(identifiers), chunk_size):
        batch = identifiers[index : index + chunk_size]
        query = "、".join(batch) + " 最新价 开盘价"
        payload = client.query_data(query)
        for table in payload.get("dataTableDTOList") or []:
            parsed = _parse_quote_table(table)
            quotes.update(parsed)
    return quotes


def build_quote_identifiers(targets: list[HoldingSummary]) -> list[str]:
    identifiers: list[str] = []
    for target in targets:
        name = target.name.strip()
        if name:
            identifiers.append(f"{target.code}{name}")
        else:
            identifiers.append(target.code)
    return identifiers


def fetch_live_quotes_for_targets(
    client: EastmoneyClient,
    targets: list[HoldingSummary],
) -> dict[str, LiveQuote]:
    identifiers = build_quote_identifiers(targets)
    quotes = fetch_live_quotes(client, identifiers)
    missing_targets = [target for target in targets if target.code not in quotes]
    for target in missing_targets:
        try:
            fallback_quotes = fetch_live_quotes(
                client,
                [f"{target.code}{target.name}", target.name, target.code],
            )
        except EastmoneyError as exc:
            if "请求频率过高" not in str(exc):
                continue
            time.sleep(2.0)
            try:
                fallback_quotes = fetch_live_quotes(
                    client,
                    [f"{target.code}{target.name}", target.name, target.code],
                )
            except EastmoneyError:
                continue
        quote = fallback_quotes.get(target.code)
        if quote is not None:
            quotes[target.code] = quote
    return quotes


def initialize_equal_weight_account(
    account_name: str,
    trade_date: date,
    strategy_date: date,
    initial_capital: float,
    targets: list[HoldingSummary],
    quotes: dict[str, LiveQuote],
    buy_fee_rate: float,
    slippage_rate: float,
    lot_size: int = 100,
) -> PaperAccountState:
    if not targets:
        raise ValueError("No targets provided for paper account initialization.")
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive.")
    if lot_size <= 0:
        raise ValueError("lot_size must be positive.")

    cash = initial_capital
    per_name_budget = initial_capital / len(targets)
    positions: list[PaperAccountPosition] = []
    trades: list[PaperTrade] = []

    for target in targets:
        quote = quotes.get(target.code)
        if quote is None or quote.latest_price is None:
            continue
        # Pre-open or partially updated sessions may not expose a usable open price yet.
        # In that case, fall back to the latest available quote so the paper portfolio
        # can still be initialized and tracked from the current signal set.
        reference_price = quote.open_price if quote.open_price is not None else quote.latest_price
        execution_price = reference_price * (1.0 + slippage_rate)
        cash_per_share = execution_price * (1.0 + buy_fee_rate)
        raw_shares = math.floor(per_name_budget / cash_per_share / lot_size) * lot_size
        shares = int(raw_shares)
        if shares <= 0:
            continue
        gross_amount = shares * execution_price
        fee = gross_amount * buy_fee_rate
        total_cash_flow = gross_amount + fee
        if total_cash_flow > cash:
            shares = int(math.floor(cash / cash_per_share / lot_size) * lot_size)
            if shares <= 0:
                continue
            gross_amount = shares * execution_price
            fee = gross_amount * buy_fee_rate
            total_cash_flow = gross_amount + fee
        market_value = shares * quote.latest_price
        cost_basis = total_cash_flow
        unrealized_pnl = market_value - cost_basis
        unrealized_pnl_pct = unrealized_pnl / cost_basis if cost_basis else 0.0
        cash -= total_cash_flow
        positions.append(
            PaperAccountPosition(
                code=target.code,
                name=target.name,
                board=target.board,
                industry_l1=target.industry_l1,
                shares=shares,
                entry_price=execution_price,
                latest_price=quote.latest_price,
                market_value=market_value,
                cost_basis=cost_basis,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct,
            )
        )
        trades.append(
            PaperTrade(
                trade_date=trade_date.isoformat(),
                code=target.code,
                name=target.name,
                side="BUY",
                shares=shares,
                reference_price=reference_price,
                execution_price=execution_price,
                gross_amount=gross_amount,
                fee=fee,
                total_cash_flow=total_cash_flow,
                slippage_rate=slippage_rate,
            )
        )

    market_value = sum(position.market_value for position in positions)
    total_cost = initial_capital - cash
    nav = cash + market_value
    pnl = nav - initial_capital
    pnl_pct = pnl / initial_capital if initial_capital else 0.0
    return PaperAccountState(
        account_name=account_name,
        trade_date=trade_date.isoformat(),
        strategy_date=strategy_date.isoformat(),
        initial_capital=initial_capital,
        cash=cash,
        market_value=market_value,
        nav=nav,
        total_cost=total_cost,
        pnl=pnl,
        pnl_pct=pnl_pct,
        target_count=len(targets),
        positions=positions,
        trades=trades,
    )


def build_quote_targets_from_account(state: PaperAccountState) -> list[HoldingSummary]:
    return [
        HoldingSummary(
            code=position.code,
            name=position.name,
            weight=(position.market_value / state.market_value) if state.market_value else 0.0,
            board=position.board,
            industry_l1=position.industry_l1,
            total_score=0.0,
        )
        for position in state.positions
    ]


def mark_to_market_account(
    state: PaperAccountState,
    trade_date: date,
    strategy_date: date,
    quotes: dict[str, LiveQuote],
) -> PaperAccountState:
    updated_positions: list[PaperAccountPosition] = []
    for position in state.positions:
        quote = quotes.get(position.code)
        latest_price = quote.latest_price if quote and quote.latest_price is not None else position.latest_price
        market_value = position.shares * latest_price
        unrealized_pnl = market_value - position.cost_basis
        unrealized_pnl_pct = unrealized_pnl / position.cost_basis if position.cost_basis else 0.0
        updated_positions.append(
            PaperAccountPosition(
                code=position.code,
                name=position.name,
                board=position.board,
                industry_l1=position.industry_l1,
                shares=position.shares,
                entry_price=position.entry_price,
                latest_price=latest_price,
                market_value=market_value,
                cost_basis=position.cost_basis,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct,
            )
        )

    market_value = sum(position.market_value for position in updated_positions)
    nav = state.cash + market_value
    pnl = nav - state.initial_capital
    pnl_pct = pnl / state.initial_capital if state.initial_capital else 0.0
    return PaperAccountState(
        account_name=state.account_name,
        trade_date=trade_date.isoformat(),
        strategy_date=strategy_date.isoformat(),
        initial_capital=state.initial_capital,
        cash=state.cash,
        market_value=market_value,
        nav=nav,
        total_cost=state.total_cost,
        pnl=pnl,
        pnl_pct=pnl_pct,
        target_count=state.target_count,
        positions=updated_positions,
        trades=state.trades,
    )


def save_paper_account(state: PaperAccountState, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{state.account_name}.json"
    md_path = output_dir / f"{state.account_name}.md"
    payload = {
        "account_name": state.account_name,
        "trade_date": state.trade_date,
        "strategy_date": state.strategy_date,
        "initial_capital": round(state.initial_capital, 4),
        "cash": round(state.cash, 4),
        "market_value": round(state.market_value, 4),
        "nav": round(state.nav, 4),
        "total_cost": round(state.total_cost, 4),
        "pnl": round(state.pnl, 4),
        "pnl_pct": round(state.pnl_pct, 6),
        "target_count": state.target_count,
        "positions": [
            {
                "code": position.code,
                "name": position.name,
                "board": position.board,
                "industry_l1": position.industry_l1,
                "shares": position.shares,
                "entry_price": round(position.entry_price, 6),
                "latest_price": round(position.latest_price, 6),
                "market_value": round(position.market_value, 4),
                "cost_basis": round(position.cost_basis, 4),
                "unrealized_pnl": round(position.unrealized_pnl, 4),
                "unrealized_pnl_pct": round(position.unrealized_pnl_pct, 6),
            }
            for position in state.positions
        ],
        "trades": [asdict(trade) for trade in state.trades],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_paper_account_markdown(state), encoding="utf-8")
    return json_path, md_path


def load_paper_account(path: Path) -> PaperAccountState:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return PaperAccountState(
        account_name=payload["account_name"],
        trade_date=payload["trade_date"],
        strategy_date=payload["strategy_date"],
        initial_capital=float(payload["initial_capital"]),
        cash=float(payload["cash"]),
        market_value=float(payload["market_value"]),
        nav=float(payload["nav"]),
        total_cost=float(payload["total_cost"]),
        pnl=float(payload["pnl"]),
        pnl_pct=float(payload["pnl_pct"]),
        target_count=int(payload["target_count"]),
        positions=[
            PaperAccountPosition(
                code=item["code"],
                name=item["name"],
                board=item.get("board"),
                industry_l1=item.get("industry_l1"),
                shares=int(item["shares"]),
                entry_price=float(item["entry_price"]),
                latest_price=float(item["latest_price"]),
                market_value=float(item["market_value"]),
                cost_basis=float(item["cost_basis"]),
                unrealized_pnl=float(item["unrealized_pnl"]),
                unrealized_pnl_pct=float(item["unrealized_pnl_pct"]),
            )
            for item in payload.get("positions", [])
        ],
        trades=[
            PaperTrade(
                trade_date=item["trade_date"],
                code=item["code"],
                name=item["name"],
                side=item["side"],
                shares=int(item["shares"]),
                reference_price=float(item["reference_price"]),
                execution_price=float(item["execution_price"]),
                gross_amount=float(item["gross_amount"]),
                fee=float(item["fee"]),
                total_cash_flow=float(item["total_cash_flow"]),
                slippage_rate=float(item["slippage_rate"]),
            )
            for item in payload.get("trades", [])
        ],
    )


def render_paper_account_markdown(state: PaperAccountState) -> str:
    lines = [
        "# 模拟账户快照",
        "",
        f"- 账户名：`{state.account_name}`",
        f"- 快照日期：`{state.trade_date}`",
        f"- 信号日期：`{state.strategy_date}`",
        f"- 初始资金：`{state.initial_capital:.2f}`",
        f"- 最新净值：`{state.nav:.2f}`",
        f"- 账户收益：`{state.pnl:.2f}` | `{state.pnl_pct:.2%}`",
        f"- 现金：`{state.cash:.2f}`",
        f"- 持仓市值：`{state.market_value:.2f}`",
        f"- 实际建仓只数：`{len(state.positions)}` / 目标 `{state.target_count}`",
        "",
        "## 持仓",
        "",
    ]
    for position in state.positions:
        lines.append(
            f"- `{position.code}` {position.name} | {position.shares} 股 | 成本 `{position.entry_price:.4f}` | "
            f"现价 `{position.latest_price:.4f}` | 浮盈 `{position.unrealized_pnl:.2f}` ({position.unrealized_pnl_pct:.2%})"
        )
    lines.append("")
    lines.append("## 成交")
    lines.append("")
    for trade in state.trades:
        lines.append(
            f"- `{trade.code}` {trade.name} | 买入 `{trade.shares}` 股 | 参考价 `{trade.reference_price:.4f}` | "
            f"成交价 `{trade.execution_price:.4f}` | 手续费 `{trade.fee:.2f}`"
        )
    lines.append("")
    lines.append("## 假设")
    lines.append("")
    lines.append("- 按次日开盘价加买入滑点成交。")
    lines.append("- 手续费按策略当前 `buy_fee_rate` 计算。")
    lines.append("- 当前模拟按 `100` 股整手近似处理。")
    return "\n".join(lines) + "\n"


def _parse_quote_table(table: dict) -> dict[str, LiveQuote]:
    parsed: dict[str, LiveQuote] = {}
    table_payload = table.get("table") or {}
    latest_prices = table_payload.get("f2") or []
    open_prices = table_payload.get("f17") or []
    if not latest_prices and not open_prices:
        return parsed
    headers = table_payload.get("headName") or []
    entity_codes = table.get("entityCodes") or []
    for index, code_info in enumerate(entity_codes):
        code = str(code_info).split(".")[0]
        header = headers[index] if index < len(headers) else code
        name = header.split("(", maxsplit=1)[0].strip() if "(" in header else str(header).strip()
        parsed[code] = LiveQuote(
            code=code,
            name=name.strip(),
            latest_price=_to_float(latest_prices[index]) if index < len(latest_prices) else None,
            open_price=_to_float(open_prices[index]) if index < len(open_prices) else None,
        )
    return parsed


def _to_float(value) -> float | None:
    try:
        if value in (None, "", "-", "--"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
