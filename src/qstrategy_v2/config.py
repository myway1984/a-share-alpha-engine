from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_FACTOR_DIRECTIONS = {
    "one_month_reversal": -1.0,
    "turnover_20d": -1.0,
    "volatility_20d": -1.0,
    "price_volume_corr": 1.0,
    "ep": 1.0,
    "roe_ttm": 1.0,
    "sue": 1.0,
}

DEFAULT_FACTOR_WEIGHTS = {
    "one_month_reversal": 1.0,
    "turnover_20d": 1.0,
    "volatility_20d": 1.0,
    "price_volume_corr": 1.0,
    "ep": 1.0,
    "roe_ttm": 1.0,
    "sue": 1.0,
}

DEFAULT_BOARD_FACTOR_WEIGHTS = {
    "main": {},
    "gem": {},
    "star": {},
}


@dataclass(slots=True)
class MultiFactorConfig:
    lookback_days: int = 20
    min_listing_days: int = 180
    top_n: int = 50
    buffer_rank: int = 80
    min_holding_days: int = 0
    min_holding_trade_days: int | None = None
    rebalance_interval_trade_days: int = 1
    max_new_positions_per_rebalance: int | None = None
    universe_limit: int | None = None
    industry_filter_top_n: int | None = None
    industry_filter_min_momentum: float | None = None
    initial_capital: float = 1_000_000.0
    buy_fee_rate: float = 0.0002
    sell_fee_rate: float = 0.0012
    slippage_rate: float = 0.002
    output_dir: Path = Path("reports/multifactor_v2")
    factor_directions: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_FACTOR_DIRECTIONS)
    )
    factor_weights: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_FACTOR_WEIGHTS)
    )
    board_factor_weights: dict[str, dict[str, float]] = field(
        default_factory=lambda: {
            board: dict(weights)
            for board, weights in DEFAULT_BOARD_FACTOR_WEIGHTS.items()
        }
    )
    excluded_factors: tuple[str, ...] = ()

    def active_factor_names(self) -> list[str]:
        excluded = set(self.excluded_factors)
        return [
            factor_name
            for factor_name in self.factor_directions
            if factor_name not in excluded
        ]

    def factor_weight(self, factor_name: str, board: str | None = None) -> float:
        if board:
            board_weights = self.board_factor_weights.get(board) or {}
            if factor_name in board_weights:
                return board_weights[factor_name]
        return self.factor_weights.get(factor_name, 1.0)
