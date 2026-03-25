from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass(slots=True)
class DailySnapshot:
    code: str
    name: str
    trade_date: date
    exchange: str
    industry_l1: str | None
    listed_days: int | None
    is_st: bool
    is_suspended: bool
    open_price: float | None
    close_price: float | None
    volume: float | None
    amount: float | None
    turnover_rate: float | None
    total_market_cap: float | None
    limit_up_price: float | None = None
    limit_down_price: float | None = None
    pe_ttm: float | None = None
    roe_ttm: float | None = None
    board: str | None = None


@dataclass(slots=True)
class QuarterlyReport:
    code: str
    announce_date: date
    report_period: date
    single_quarter_net_profit: float | None
    roe_ttm: float | None = None


@dataclass(slots=True)
class FactorObservation:
    code: str
    name: str
    trade_date: date
    industry_l1: str | None
    total_market_cap: float | None
    board: str | None = None
    raw_factors: dict[str, float | None] = field(default_factory=dict)
    processed_factors: dict[str, float] = field(default_factory=dict)
    total_score: float = 0.0


@dataclass(slots=True)
class Position:
    code: str
    weight: float
    buy_date: date
    buy_trade_index: int = 0


@dataclass(slots=True)
class HoldingSummary:
    code: str
    name: str
    weight: float
    board: str | None
    industry_l1: str | None
    total_score: float | None = None


@dataclass(slots=True)
class Order:
    trade_date: date
    code: str
    side: str
    from_weight: float
    to_weight: float
    reason: str


@dataclass(slots=True)
class PortfolioSnapshot:
    trade_date: date
    nav: float
    turnover: float
    holdings: int


@dataclass(slots=True)
class SegmentSummary:
    start_date: date
    end_date: date
    trade_days: int
    ending_nav: float
    holdings: int


@dataclass(slots=True)
class BacktestState:
    positions: dict[str, Position] = field(default_factory=dict)
    nav: float = 0.0
    previous_closes: dict[str, float] = field(default_factory=dict)
    history_window: list[list[DailySnapshot]] = field(default_factory=list)
    trade_days_processed: int = 0


@dataclass(slots=True)
class BacktestResult:
    daily_nav: list[PortfolioSnapshot]
    latest_rankings: list[FactorObservation]
    latest_orders: list[Order]
    latest_holdings: list[HoldingSummary] = field(default_factory=list)
    segment_summaries: list[SegmentSummary] = field(default_factory=list)
