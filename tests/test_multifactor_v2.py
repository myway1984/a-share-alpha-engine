from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from qstrategy_v2.backtest import CrossSectionalBacktester
from qstrategy_v2.config import DEFAULT_FACTOR_DIRECTIONS
from qstrategy_v2.cli import parse_board_factor_weights, parse_factor_weights
from qstrategy_v2.config import MultiFactorConfig
from qstrategy_v2.data import infer_board
from qstrategy_v2.diagnostics import (
    FactorDiagnosticsReport,
    FactorDiagnosticsRunner,
    FactorDiagnosticSummary,
    assess_factor_health,
    quantile_return_spread,
    render_factor_diagnostics_markdown,
    spearman_rank_corr,
)
from qstrategy_v2.models import (
    BacktestResult,
    DailySnapshot,
    FactorObservation,
    HoldingSummary,
    PortfolioSnapshot,
    Position,
    QuarterlyReport,
)
from qstrategy_v2.optimizer import (
    GridSearchCandidate,
    compute_max_drawdown,
    compute_positive_month_ratio,
    is_valid_candidate,
    parse_int_list,
    run_grid_search,
)
from qstrategy_v2.portfolio import PortfolioManager
from qstrategy_v2.preprocessing import CrossSectionPreprocessor, winsorize_mad, zscore, zscore_by_board
from qstrategy_v2.paper_account import LiveQuote, initialize_equal_weight_account, mark_to_market_account
from qstrategy_v2.review import write_daily_review_draft
from qstrategy_v2.universe import UniverseSelector


def build_snapshot(**overrides) -> DailySnapshot:
    payload = {
        "code": "600000",
        "name": "浦发银行",
        "trade_date": date(2026, 3, 20),
        "exchange": "SSE",
        "industry_l1": "银行",
        "listed_days": 500,
        "is_st": False,
        "is_suspended": False,
        "open_price": 10.0,
        "close_price": 10.2,
        "volume": 100_000.0,
        "amount": 1_000_000.0,
        "turnover_rate": 0.03,
        "total_market_cap": 100_000_000_000.0,
        "limit_up_price": 11.0,
        "limit_down_price": 9.0,
        "pe_ttm": 8.0,
        "roe_ttm": 0.12,
        "board": "main",
    }
    payload.update(overrides)
    return DailySnapshot(**payload)


def test_universe_selector_filters_basic_rules() -> None:
    config = MultiFactorConfig()
    selector = UniverseSelector(config)
    snapshots = [
        build_snapshot(code="830001"),
        build_snapshot(code="600001", is_st=True),
        build_snapshot(code="600002", listed_days=20),
        build_snapshot(code="600003", is_suspended=True),
        build_snapshot(code="600004", volume=0.0),
        build_snapshot(code="600005", open_price=11.0),
        build_snapshot(code="600006"),
    ]

    selected = selector.select(snapshots)

    assert [item.code for item in selected] == ["600006"]


def test_universe_selector_apply_limit_uses_liquidity_and_board_quotas() -> None:
    config = MultiFactorConfig(universe_limit=4)
    selector = UniverseSelector(config)
    snapshots = [
        build_snapshot(code="600001", board="main", amount=90.0),
        build_snapshot(code="600002", board="main", amount=80.0),
        build_snapshot(code="600003", board="main", amount=70.0),
        build_snapshot(code="300001", exchange="SZSE", board="gem", amount=60.0),
        build_snapshot(code="300002", exchange="SZSE", board="gem", amount=50.0),
        build_snapshot(code="688001", exchange="SSE", board="star", amount=40.0),
    ]

    limited = selector.apply_limit(snapshots)

    assert len(limited) == 4
    assert {item.board for item in limited} == {"main", "gem", "star"}
    assert [item.code for item in limited[:2]] == ["600001", "600002"]


def test_universe_selector_apply_industry_trend_filter_keeps_stronger_industries() -> None:
    config = MultiFactorConfig(top_n=2, lookback_days=2, industry_filter_top_n=1)
    selector = UniverseSelector(config)
    current = [
        build_snapshot(code="600001", industry_l1="强行业", close_price=12.0),
        build_snapshot(code="600002", industry_l1="强行业", close_price=11.0),
        build_snapshot(code="600003", industry_l1="弱行业", close_price=8.0),
        build_snapshot(code="600004", industry_l1="弱行业", close_price=7.5),
    ]
    history_window = [
        [
            build_snapshot(code="600001", industry_l1="强行业", close_price=10.0),
            build_snapshot(code="600002", industry_l1="强行业", close_price=10.0),
            build_snapshot(code="600003", industry_l1="弱行业", close_price=10.0),
            build_snapshot(code="600004", industry_l1="弱行业", close_price=10.0),
        ],
        current,
    ]

    filtered = selector.apply_industry_trend_filter(current, history_window)

    assert {item.industry_l1 for item in filtered} == {"强行业"}
    assert {item.code for item in filtered} == {"600001", "600002"}


def test_preprocessing_handles_outlier_and_standardizes() -> None:
    raw = {"a": 1.0, "b": 1.1, "c": 0.9, "d": 9.0}
    winsorized = winsorize_mad(raw)
    standardized = zscore(winsorized)

    assert winsorized["d"] < raw["d"]
    assert round(sum(standardized.values()), 8) == 0.0


def test_portfolio_manager_uses_buffer_rule() -> None:
    config = MultiFactorConfig(top_n=2, buffer_rank=3)
    manager = PortfolioManager(config)
    trade_date = date(2026, 3, 20)
    ranked = [
        FactorObservation(code="A", name="A", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=5.0),
        FactorObservation(code="B", name="B", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=4.0),
        FactorObservation(code="C", name="C", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=3.0),
        FactorObservation(code="D", name="D", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=2.0),
    ]
    current_positions = {
        "C": Position(code="C", weight=0.5, buy_date=date(2026, 3, 19)),
        "D": Position(code="D", weight=0.5, buy_date=date(2026, 3, 19)),
    }

    next_positions, orders, _ = manager.rebalance(
        trade_date=trade_date,
        ranked=ranked,
        current_positions=current_positions,
        trade_day_index=1,
    )

    assert sorted(next_positions) == ["A", "C"]
    assert any(order.code == "D" and order.side == "SELL" for order in orders)
    assert any(order.code == "A" and order.side == "BUY" for order in orders)


def test_portfolio_manager_respects_min_holding_days_for_ranked_names() -> None:
    config = MultiFactorConfig(top_n=2, buffer_rank=2, min_holding_days=5)
    manager = PortfolioManager(config)
    trade_date = date(2026, 3, 20)
    ranked = [
        FactorObservation(code="A", name="A", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=5.0),
        FactorObservation(code="B", name="B", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=4.0),
        FactorObservation(code="C", name="C", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=3.0),
    ]
    current_positions = {
        "C": Position(code="C", weight=0.5, buy_date=date(2026, 3, 18)),
        "D": Position(code="D", weight=0.5, buy_date=date(2026, 3, 18)),
    }

    next_positions, orders, _ = manager.rebalance(
        trade_date=trade_date,
        ranked=ranked,
        current_positions=current_positions,
        trade_day_index=2,
    )

    assert sorted(next_positions) == ["A", "C"]
    assert not any(order.code == "C" and order.side == "SELL" for order in orders)
    assert any(order.code == "D" and order.side == "SELL" for order in orders)


def test_portfolio_manager_respects_min_holding_trade_days() -> None:
    config = MultiFactorConfig(top_n=2, buffer_rank=2, min_holding_trade_days=3)
    manager = PortfolioManager(config)
    trade_date = date(2026, 3, 20)
    ranked = [
        FactorObservation(code="A", name="A", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=5.0),
        FactorObservation(code="B", name="B", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=4.0),
        FactorObservation(code="C", name="C", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=3.0),
    ]
    current_positions = {
        "C": Position(code="C", weight=0.5, buy_date=date(2026, 3, 17), buy_trade_index=0),
        "D": Position(code="D", weight=0.5, buy_date=date(2026, 3, 17), buy_trade_index=0),
    }

    next_positions, orders, _ = manager.rebalance(
        trade_date=trade_date,
        ranked=ranked,
        current_positions=current_positions,
        trade_day_index=2,
    )

    assert "C" in next_positions
    assert not any(order.code == "C" and order.side == "SELL" for order in orders)


def test_portfolio_manager_limits_new_positions_per_rebalance() -> None:
    config = MultiFactorConfig(top_n=3, buffer_rank=3, max_new_positions_per_rebalance=1)
    manager = PortfolioManager(config)
    trade_date = date(2026, 3, 20)
    ranked = [
        FactorObservation(code="A", name="A", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=6.0),
        FactorObservation(code="B", name="B", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=5.0),
        FactorObservation(code="C", name="C", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=4.0),
        FactorObservation(code="D", name="D", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=3.0),
        FactorObservation(code="E", name="E", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=2.0),
    ]
    current_positions = {
        "C": Position(code="C", weight=1 / 3, buy_date=date(2026, 3, 1), buy_trade_index=0),
        "D": Position(code="D", weight=1 / 3, buy_date=date(2026, 3, 1), buy_trade_index=0),
        "E": Position(code="E", weight=1 / 3, buy_date=date(2026, 3, 1), buy_trade_index=0),
    }

    next_positions, orders, _ = manager.rebalance(
        trade_date=trade_date,
        ranked=ranked,
        current_positions=current_positions,
        trade_day_index=10,
    )

    assert len([order for order in orders if order.side == "BUY"]) == 1
    assert len([order for order in orders if order.side == "SELL"]) == 1
    assert sorted(next_positions) == ["A", "C", "D"]


def test_portfolio_manager_bootstraps_initial_portfolio_before_staggering() -> None:
    config = MultiFactorConfig(top_n=3, buffer_rank=3, max_new_positions_per_rebalance=1)
    manager = PortfolioManager(config)
    trade_date = date(2026, 3, 20)
    ranked = [
        FactorObservation(code="A", name="A", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=6.0),
        FactorObservation(code="B", name="B", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=5.0),
        FactorObservation(code="C", name="C", trade_date=trade_date, industry_l1="电子", total_market_cap=1.0, total_score=4.0),
    ]

    next_positions, orders, _ = manager.rebalance(
        trade_date=trade_date,
        ranked=ranked,
        current_positions={},
        trade_day_index=0,
    )

    assert sorted(next_positions) == ["A", "B", "C"]
    assert len([order for order in orders if order.side == "BUY"]) == 3


def test_preprocessor_uses_factor_weights_in_total_score() -> None:
    config = MultiFactorConfig(
        factor_weights={
            "one_month_reversal": 1.0,
            "turnover_20d": 0.5,
            "volatility_20d": 1.0,
            "price_volume_corr": 1.0,
            "ep": 2.0,
            "roe_ttm": 1.0,
            "sue": 1.0,
        }
    )
    preprocessor = CrossSectionPreprocessor(config)
    observations = [
        FactorObservation(
            code="A",
            name="A",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            raw_factors={"ep": 1.0, "turnover_20d": 0.0},
        ),
        FactorObservation(
            code="B",
            name="B",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            raw_factors={"ep": 0.0, "turnover_20d": 1.0},
        ),
        FactorObservation(
            code="C",
            name="C",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            raw_factors={"ep": 0.5, "turnover_20d": 0.5},
        ),
    ]

    ranked = preprocessor.transform(observations)

    assert ranked[0].code == "A"
    assert ranked[-1].code == "B"


def test_preprocessor_uses_board_specific_factor_weights() -> None:
    config = MultiFactorConfig(
        factor_weights={
            "one_month_reversal": 1.0,
            "turnover_20d": 1.0,
            "volatility_20d": 1.0,
            "price_volume_corr": 1.0,
            "ep": 1.0,
            "roe_ttm": 1.0,
            "sue": 1.0,
        },
        board_factor_weights={
            "main": {"ep": 2.0, "turnover_20d": 0.5},
            "gem": {},
            "star": {},
        },
    )
    preprocessor = CrossSectionPreprocessor(config)
    observations = [
        FactorObservation(
            code="A",
            name="A",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            board="main",
            raw_factors={"ep": 1.0, "turnover_20d": 0.0},
        ),
        FactorObservation(
            code="B",
            name="B",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            board="main",
            raw_factors={"ep": 0.0, "turnover_20d": 1.0},
        ),
        FactorObservation(
            code="C",
            name="C",
            trade_date=date(2026, 3, 20),
            industry_l1="电子",
            total_market_cap=None,
            board="main",
            raw_factors={"ep": 0.5, "turnover_20d": 0.5},
        ),
    ]

    ranked = preprocessor.transform(observations)

    assert ranked[0].code == "A"
    assert ranked[-1].code == "B"


def test_parse_factor_weights_parses_name_value_pairs() -> None:
    weights = parse_factor_weights("ep=1.5, roe_ttm=1.25, turnover_20d=0.5")

    assert weights == {
        "ep": 1.5,
        "roe_ttm": 1.25,
        "turnover_20d": 0.5,
    }


def test_parse_board_factor_weights_parses_multiple_boards() -> None:
    weights = parse_board_factor_weights(
        "main:ep=1.25,roe_ttm=1.0|gem:one_month_reversal=1.5|star:volatility_20d=1.75,ep=0.5"
    )

    assert weights == {
        "main": {"ep": 1.25, "roe_ttm": 1.0},
        "gem": {"one_month_reversal": 1.5},
        "star": {"volatility_20d": 1.75, "ep": 0.5},
    }


def test_infer_board_distinguishes_main_gem_and_star() -> None:
    assert infer_board("600000", "SSE") == "main"
    assert infer_board("300750", "SZSE") == "gem"
    assert infer_board("688001", "SSE") == "star"


def test_spearman_rank_corr_detects_positive_monotonic_relation() -> None:
    value = spearman_rank_corr(
        [
            (1.0, 0.01),
            (2.0, 0.02),
            (3.0, 0.03),
            (4.0, 0.04),
        ]
    )

    assert value is not None
    assert round(value, 6) == 1.0


def test_quantile_return_spread_uses_top_minus_bottom_buckets() -> None:
    spread = quantile_return_spread(
        [
            (5.0, 0.10),
            (4.0, 0.08),
            (3.0, 0.03),
            (2.0, -0.02),
            (1.0, -0.05),
        ]
    )

    assert spread is not None
    assert round(spread, 6) == 0.15


def test_zscore_by_board_normalizes_within_board_groups() -> None:
    observations = [
        FactorObservation(code="A", name="A", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="main"),
        FactorObservation(code="B", name="B", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="main"),
        FactorObservation(code="C", name="C", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="main"),
        FactorObservation(code="D", name="D", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="star"),
        FactorObservation(code="E", name="E", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="star"),
        FactorObservation(code="F", name="F", trade_date=date(2026, 3, 20), industry_l1="电子", total_market_cap=1.0, board="star"),
    ]
    values = {"A": 1.0, "B": 2.0, "C": 3.0, "D": 10.0, "E": 11.0, "F": 12.0}

    standardized = zscore_by_board(observations, values)

    assert round(standardized["A"], 6) == -1.224745
    assert round(standardized["C"], 6) == 1.224745
    assert round(standardized["D"], 6) == -1.224745
    assert round(standardized["F"], 6) == 1.224745


class FakeProvider:
    def __init__(self, snapshots_by_date: dict[date, list[DailySnapshot]]) -> None:
        self.snapshots_by_date = snapshots_by_date

    def list_trade_dates(self, start_date: date, end_date: date) -> list[date]:
        return [
            trade_date
            for trade_date in sorted(self.snapshots_by_date)
            if start_date <= trade_date <= end_date
        ]

    def get_daily_snapshots(self, trade_date: date) -> list[DailySnapshot]:
        return self.snapshots_by_date[trade_date]

    def get_price_history(
        self, code: str, end_date: date, lookback_days: int
    ) -> list[DailySnapshot]:
        trade_dates = [
            trade_date
            for trade_date in sorted(self.snapshots_by_date)
            if trade_date <= end_date
        ]
        selected_dates = trade_dates[-lookback_days:]
        history: list[DailySnapshot] = []
        for trade_date in selected_dates:
            for snapshot in self.snapshots_by_date[trade_date]:
                if snapshot.code == code:
                    history.append(snapshot)
        return history

    def get_financial_reports(
        self, code: str, end_date: date, limit: int = 12
    ) -> list[QuarterlyReport]:
        return []

    def warm_financial_cache(self, codes: list[str], end_date: date) -> int:
        return len(codes)

    def warm_market_cache(self, start_date: date, end_date: date) -> int:
        return len(self.list_trade_dates(start_date, end_date))


def test_backtest_rebalances_only_on_configured_interval() -> None:
    dates = [date(2026, 3, 18), date(2026, 3, 19), date(2026, 3, 20)]
    snapshots_by_date = {
        dates[0]: [
            build_snapshot(code="A", name="A", trade_date=dates[0], pe_ttm=2.0, close_price=10.0),
            build_snapshot(code="B", name="B", trade_date=dates[0], pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="C", name="C", trade_date=dates[0], pe_ttm=8.0, close_price=10.0),
        ],
        dates[1]: [
            build_snapshot(code="A", name="A", trade_date=dates[1], pe_ttm=2.0, close_price=10.0),
            build_snapshot(code="B", name="B", trade_date=dates[1], pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="C", name="C", trade_date=dates[1], pe_ttm=8.0, close_price=10.0),
        ],
        dates[2]: [
            build_snapshot(code="A", name="A", trade_date=dates[2], pe_ttm=8.0, close_price=10.0),
            build_snapshot(code="B", name="B", trade_date=dates[2], pe_ttm=2.0, close_price=10.0),
            build_snapshot(code="C", name="C", trade_date=dates[2], pe_ttm=4.0, close_price=10.0),
        ],
    }
    config = MultiFactorConfig(
        lookback_days=1,
        top_n=1,
        buffer_rank=1,
        rebalance_interval_trade_days=2,
        factor_directions={name: direction for name, direction in DEFAULT_FACTOR_DIRECTIONS.items()},
        excluded_factors=tuple(name for name in DEFAULT_FACTOR_DIRECTIONS if name != "ep"),
    )
    backtester = CrossSectionalBacktester(provider=FakeProvider(snapshots_by_date), config=config)

    result = backtester.run(start_date=dates[0], end_date=dates[-1])

    assert [snapshot.turnover for snapshot in result.daily_nav] == [1.0, 0.0, 2.0]
    assert [holding.code for holding in result.latest_holdings] == ["B"]


def test_write_daily_review_draft_includes_holdings_and_orders() -> None:
    dates = [date(2026, 3, 18), date(2026, 3, 19), date(2026, 3, 20)]
    snapshots_by_date = {
        dates[0]: [
            build_snapshot(code="A", name="A", trade_date=dates[0], pe_ttm=2.0, close_price=10.0, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[0], pe_ttm=4.0, close_price=10.0, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[0], pe_ttm=8.0, close_price=10.0, amount=80.0),
        ],
        dates[1]: [
            build_snapshot(code="A", name="A", trade_date=dates[1], pe_ttm=2.0, close_price=10.0, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[1], pe_ttm=4.0, close_price=10.0, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[1], pe_ttm=8.0, close_price=10.0, amount=80.0),
        ],
        dates[2]: [
            build_snapshot(code="A", name="A", trade_date=dates[2], pe_ttm=8.0, close_price=10.0, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[2], pe_ttm=2.0, close_price=10.5, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[2], pe_ttm=4.0, close_price=9.8, amount=80.0),
        ],
    }
    config = MultiFactorConfig(
        lookback_days=1,
        top_n=1,
        buffer_rank=1,
        rebalance_interval_trade_days=2,
        factor_directions={name: direction for name, direction in DEFAULT_FACTOR_DIRECTIONS.items()},
        excluded_factors=tuple(name for name in DEFAULT_FACTOR_DIRECTIONS if name != "ep"),
    )
    provider = FakeProvider(snapshots_by_date)
    backtester = CrossSectionalBacktester(provider=provider, config=config)
    result = backtester.run(start_date=dates[0], end_date=dates[-1])

    with TemporaryDirectory() as tmpdir:
        path = write_daily_review_draft(
            result=result,
            provider=provider,
            config=config,
            start_date=dates[0],
            trade_date=dates[-1],
            output_dir=Path(tmpdir),
        )
        content = path.read_text(encoding="utf-8")

    assert "A股多因子策略 V2 每日收盘后复盘草稿" in content
    assert "当前持仓数：`1`" in content
    assert "`B B`" in content


def test_initialize_equal_weight_account_applies_fees_slippage_and_lot_size() -> None:
    targets = [
        HoldingSummary(code="600001", name="A", weight=0.5, board="main", industry_l1="电子"),
        HoldingSummary(code="300001", name="B", weight=0.5, board="gem", industry_l1="电力设备"),
    ]
    quotes = {
        "600001": LiveQuote(code="600001", name="A", latest_price=10.5, open_price=10.0),
        "300001": LiveQuote(code="300001", name="B", latest_price=20.8, open_price=20.0),
    }

    state = initialize_equal_weight_account(
        account_name="paper_test",
        trade_date=date(2026, 3, 23),
        strategy_date=date(2026, 3, 20),
        initial_capital=100_000.0,
        targets=targets,
        quotes=quotes,
        buy_fee_rate=0.0002,
        slippage_rate=0.002,
        lot_size=100,
    )

    assert len(state.positions) == 2
    assert all(position.shares % 100 == 0 for position in state.positions)
    assert state.total_cost < 100_000.0
    assert state.nav > 100_000.0


def test_mark_to_market_account_updates_existing_positions_without_rebuilding() -> None:
    targets = [
        HoldingSummary(code="600001", name="A", weight=0.5, board="main", industry_l1="电子"),
        HoldingSummary(code="300001", name="B", weight=0.5, board="gem", industry_l1="电力设备"),
    ]
    initial_quotes = {
        "600001": LiveQuote(code="600001", name="A", latest_price=10.5, open_price=10.0),
        "300001": LiveQuote(code="300001", name="B", latest_price=20.8, open_price=20.0),
    }
    state = initialize_equal_weight_account(
        account_name="paper_test",
        trade_date=date(2026, 3, 23),
        strategy_date=date(2026, 3, 20),
        initial_capital=100_000.0,
        targets=targets,
        quotes=initial_quotes,
        buy_fee_rate=0.0002,
        slippage_rate=0.002,
        lot_size=100,
    )
    updated = mark_to_market_account(
        state=state,
        trade_date=date(2026, 3, 24),
        strategy_date=date(2026, 3, 20),
        quotes={
            "600001": LiveQuote(code="600001", name="A", latest_price=11.0, open_price=10.8),
            "300001": LiveQuote(code="300001", name="B", latest_price=19.0, open_price=19.5),
        },
    )

    assert updated.trade_date == "2026-03-24"
    assert updated.cash == state.cash
    assert [position.shares for position in updated.positions] == [position.shares for position in state.positions]
    assert updated.trades == state.trades
    assert updated.nav != state.nav


def test_factor_diagnostics_runner_filters_by_board() -> None:
    dates = [
        date(2026, 3, 18),
        date(2026, 3, 19),
        date(2026, 3, 20),
        date(2026, 3, 21),
    ]
    snapshots_by_date = {
        dates[0]: [
            build_snapshot(code="600001", name="M1", trade_date=dates[0], board="main", pe_ttm=2.0, close_price=10.0),
            build_snapshot(code="600002", name="M2", trade_date=dates[0], board="main", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="600003", name="M3", trade_date=dates[0], board="main", pe_ttm=8.0, close_price=10.0),
            build_snapshot(code="688001", name="S1", trade_date=dates[0], board="star", pe_ttm=2.0, close_price=10.0),
            build_snapshot(code="688002", name="S2", trade_date=dates[0], board="star", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="688003", name="S3", trade_date=dates[0], board="star", pe_ttm=8.0, close_price=10.0),
        ],
        dates[1]: [
            build_snapshot(code="600001", name="M1", trade_date=dates[1], board="main", pe_ttm=2.0, close_price=10.5),
            build_snapshot(code="600002", name="M2", trade_date=dates[1], board="main", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="600003", name="M3", trade_date=dates[1], board="main", pe_ttm=8.0, close_price=9.5),
            build_snapshot(code="688001", name="S1", trade_date=dates[1], board="star", pe_ttm=2.0, close_price=10.5),
            build_snapshot(code="688002", name="S2", trade_date=dates[1], board="star", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="688003", name="S3", trade_date=dates[1], board="star", pe_ttm=8.0, close_price=9.5),
        ],
        dates[2]: [
            build_snapshot(code="600001", name="M1", trade_date=dates[2], board="main", pe_ttm=2.0, close_price=11.0),
            build_snapshot(code="600002", name="M2", trade_date=dates[2], board="main", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="600003", name="M3", trade_date=dates[2], board="main", pe_ttm=8.0, close_price=9.0),
            build_snapshot(code="688001", name="S1", trade_date=dates[2], board="star", pe_ttm=2.0, close_price=11.0),
            build_snapshot(code="688002", name="S2", trade_date=dates[2], board="star", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="688003", name="S3", trade_date=dates[2], board="star", pe_ttm=8.0, close_price=9.0),
        ],
        dates[3]: [
            build_snapshot(code="600001", name="M1", trade_date=dates[3], board="main", pe_ttm=2.0, close_price=11.5),
            build_snapshot(code="600002", name="M2", trade_date=dates[3], board="main", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="600003", name="M3", trade_date=dates[3], board="main", pe_ttm=8.0, close_price=8.5),
            build_snapshot(code="688001", name="S1", trade_date=dates[3], board="star", pe_ttm=2.0, close_price=11.5),
            build_snapshot(code="688002", name="S2", trade_date=dates[3], board="star", pe_ttm=4.0, close_price=10.0),
            build_snapshot(code="688003", name="S3", trade_date=dates[3], board="star", pe_ttm=8.0, close_price=8.5),
        ],
    }
    config = MultiFactorConfig(
        lookback_days=1,
        rebalance_interval_trade_days=1,
        factor_directions={name: direction for name, direction in DEFAULT_FACTOR_DIRECTIONS.items()},
        excluded_factors=tuple(name for name in DEFAULT_FACTOR_DIRECTIONS if name != "ep"),
    )
    runner = FactorDiagnosticsRunner(provider=FakeProvider(snapshots_by_date), config=config)

    report = runner.run(start_date=dates[1], end_date=dates[-1], horizon_trade_days=1, board="star")

    assert report.board == "star"
    assert len(report.rebalance_dates) == 2


def test_assess_factor_health_classifies_strength() -> None:
    strong = assess_factor_health(
        FactorDiagnosticSummary(
            factor_name="ep",
            mean_ic=0.04,
            ic_ir=0.8,
            positive_ic_rate=0.6,
            mean_spread=0.02,
            positive_spread_rate=0.7,
            observation_count=12,
            average_coverage=50.0,
        )
    )
    weak = assess_factor_health(
        FactorDiagnosticSummary(
            factor_name="roe_ttm",
            mean_ic=-0.01,
            ic_ir=-0.2,
            positive_ic_rate=0.4,
            mean_spread=-0.02,
            positive_spread_rate=0.3,
            observation_count=12,
            average_coverage=50.0,
        )
    )

    assert strong.health == "强"
    assert strong.action == "保留主线"
    assert weak.health == "弱"
    assert weak.action == "考虑降权/剔除"


def test_render_factor_diagnostics_markdown_includes_summary_sections() -> None:
    report = FactorDiagnosticsReport(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 1),
        horizon_trade_days=5,
        board=None,
        rebalance_dates=[date(2026, 1, 10), date(2026, 1, 20)],
        summaries=[
            FactorDiagnosticSummary(
                factor_name="ep",
                mean_ic=0.04,
                ic_ir=0.8,
                positive_ic_rate=0.6,
                mean_spread=0.02,
                positive_spread_rate=0.7,
                observation_count=12,
                average_coverage=50.0,
            ),
            FactorDiagnosticSummary(
                factor_name="roe_ttm",
                mean_ic=-0.01,
                ic_ir=-0.2,
                positive_ic_rate=0.4,
                mean_spread=-0.02,
                positive_spread_rate=0.3,
                observation_count=12,
                average_coverage=50.0,
            ),
        ],
    )

    markdown = render_factor_diagnostics_markdown(report)

    assert "## 总结" in markdown
    assert "## 健康度判断" in markdown
    assert "## 使用建议" in markdown
    assert "保留主线" in markdown


def test_parse_int_list_parses_csv_values() -> None:
    assert parse_int_list("12, 15,18") == [12, 15, 18]


def test_grid_search_candidate_constraints_filter_invalid_combinations() -> None:
    assert is_valid_candidate(
        GridSearchCandidate(
            top_n=15,
            buffer_rank=25,
            rebalance_interval_trade_days=5,
            min_holding_trade_days=10,
            max_new_positions_per_rebalance=3,
        )
    )
    assert not is_valid_candidate(
        GridSearchCandidate(
            top_n=15,
            buffer_rank=18,
            rebalance_interval_trade_days=5,
            min_holding_trade_days=10,
            max_new_positions_per_rebalance=3,
        )
    )
    assert not is_valid_candidate(
        GridSearchCandidate(
            top_n=15,
            buffer_rank=25,
            rebalance_interval_trade_days=7,
            min_holding_trade_days=5,
            max_new_positions_per_rebalance=3,
        )
    )


def test_compute_max_drawdown_and_positive_month_ratio() -> None:
    navs = [1.0, 1.2, 0.9, 1.1]
    assert round(compute_max_drawdown(navs), 6) == 0.25

    result = BacktestResult(
        daily_nav=[
            PortfolioSnapshot(trade_date=date(2026, 1, 2), nav=1.0, turnover=0.0, holdings=1),
            PortfolioSnapshot(trade_date=date(2026, 1, 31), nav=1.1, turnover=0.0, holdings=1),
            PortfolioSnapshot(trade_date=date(2026, 2, 3), nav=1.1, turnover=0.0, holdings=1),
            PortfolioSnapshot(trade_date=date(2026, 2, 28), nav=1.0, turnover=0.0, holdings=1),
        ],
        latest_rankings=[],
        latest_orders=[],
    )
    assert round(compute_positive_month_ratio(result), 6) == 0.5


def test_run_grid_search_returns_ranked_results() -> None:
    dates = [date(2026, 3, 18), date(2026, 3, 19), date(2026, 3, 20)]
    snapshots_by_date = {
        dates[0]: [
            build_snapshot(code="A", name="A", trade_date=dates[0], pe_ttm=2.0, close_price=10.0, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[0], pe_ttm=4.0, close_price=10.0, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[0], pe_ttm=8.0, close_price=10.0, amount=80.0),
        ],
        dates[1]: [
            build_snapshot(code="A", name="A", trade_date=dates[1], pe_ttm=2.0, close_price=10.5, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[1], pe_ttm=4.0, close_price=10.0, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[1], pe_ttm=8.0, close_price=9.5, amount=80.0),
        ],
        dates[2]: [
            build_snapshot(code="A", name="A", trade_date=dates[2], pe_ttm=2.0, close_price=11.0, amount=100.0),
            build_snapshot(code="B", name="B", trade_date=dates[2], pe_ttm=4.0, close_price=10.0, amount=90.0),
            build_snapshot(code="C", name="C", trade_date=dates[2], pe_ttm=8.0, close_price=9.0, amount=80.0),
        ],
    }
    config = MultiFactorConfig(
        lookback_days=1,
        top_n=1,
        buffer_rank=6,
        rebalance_interval_trade_days=1,
        factor_directions={name: direction for name, direction in DEFAULT_FACTOR_DIRECTIONS.items()},
        excluded_factors=tuple(name for name in DEFAULT_FACTOR_DIRECTIONS if name != "ep"),
    )
    results = run_grid_search(
        provider=FakeProvider(snapshots_by_date),
        base_config=config,
        start_date=dates[0],
        end_date=dates[-1],
        top_n_values=[1, 2],
        buffer_rank_values=[6, 7],
        rebalance_interval_values=[1],
        min_holding_trade_day_values=[1],
        max_new_position_values=[1],
    )

    assert results
    assert results == sorted(results, key=lambda item: item.metrics.score, reverse=True)
