from __future__ import annotations

from collections import deque
from datetime import date

from .config import MultiFactorConfig
from .data import HistoricalDataProvider
from .factors import FactorEngine
from .models import (
    BacktestResult,
    BacktestState,
    HoldingSummary,
    PortfolioSnapshot,
    Position,
    SegmentSummary,
)
from .portfolio import PortfolioManager
from .preprocessing import CrossSectionPreprocessor
from .universe import UniverseSelector


class CrossSectionalBacktester:
    def __init__(
        self,
        provider: HistoricalDataProvider,
        config: MultiFactorConfig,
    ) -> None:
        self.provider = provider
        self.config = config
        self.selector = UniverseSelector(config)
        self.factor_engine = FactorEngine(config)
        self.preprocessor = CrossSectionPreprocessor(config)
        self.portfolio_manager = PortfolioManager(config)

    def prepare_financial_cache(self, start_date: date, end_date: date) -> int:
        all_trade_dates = self.provider.list_trade_dates(start_date, end_date)
        if not all_trade_dates:
            return 0
        anchor_date = all_trade_dates[0]
        return self.prepare_financial_cache_for_trade_date(
            trade_date=anchor_date,
            end_date=end_date,
        )

    def prepare_financial_cache_for_trade_date(self, trade_date: date, end_date: date) -> int:
        snapshots = self.provider.get_daily_snapshots(trade_date)
        universe = self.selector.select(snapshots)
        universe = self.selector.apply_limit(universe)
        universe = self.selector.apply_industry_trend_filter(
            universe,
            history_window=None,
        )
        codes = [snapshot.code for snapshot in universe]
        return self.provider.warm_financial_cache(codes, end_date=end_date)

    def prepare_market_cache(self, start_date: date, end_date: date) -> int:
        warmup_start = start_date
        if self.config.lookback_days > 1:
            warmup_start = date.fromordinal(start_date.toordinal() - self.config.lookback_days * 3)
        return self.provider.warm_market_cache(start_date=warmup_start, end_date=end_date)

    def run(self, start_date: date, end_date: date) -> BacktestResult:
        state, trade_dates = self._build_initial_state(start_date=start_date, end_date=end_date)
        return self._run_trade_dates(trade_dates=trade_dates, state=state)

    def run_segmented(
        self,
        start_date: date,
        end_date: date,
        segment_trade_days: int,
        warm_financial_cache: bool = False,
    ) -> BacktestResult:
        if segment_trade_days <= 0:
            raise ValueError("segment_trade_days must be > 0")
        state, trade_dates = self._build_initial_state(start_date=start_date, end_date=end_date)
        daily_nav: list[PortfolioSnapshot] = []
        latest_rankings = []
        latest_orders = []
        latest_holdings = []
        segment_summaries: list[SegmentSummary] = []

        for segment_start in range(0, len(trade_dates), segment_trade_days):
            segment_dates = trade_dates[segment_start : segment_start + segment_trade_days]
            if warm_financial_cache and segment_dates:
                self.prepare_financial_cache_for_trade_date(
                    trade_date=segment_dates[0],
                    end_date=segment_dates[-1],
                )
            partial = self._run_trade_dates(trade_dates=segment_dates, state=state)
            daily_nav.extend(partial.daily_nav)
            latest_rankings = partial.latest_rankings
            latest_orders = partial.latest_orders
            latest_holdings = partial.latest_holdings
            if partial.daily_nav:
                latest_snapshot = partial.daily_nav[-1]
                segment_summaries.append(
                    SegmentSummary(
                        start_date=segment_dates[0],
                        end_date=segment_dates[-1],
                        trade_days=len(segment_dates),
                        ending_nav=latest_snapshot.nav,
                        holdings=latest_snapshot.holdings,
                    )
                )

        return BacktestResult(
            daily_nav=daily_nav,
            latest_rankings=latest_rankings,
            latest_orders=latest_orders,
            latest_holdings=latest_holdings,
            segment_summaries=segment_summaries,
        )

    def _build_initial_state(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[BacktestState, list[date]]:
        warmup_start = start_date
        if self.config.lookback_days > 1:
            warmup_start = date.fromordinal(start_date.toordinal() - self.config.lookback_days * 3)
        all_trade_dates = self.provider.list_trade_dates(warmup_start, end_date)
        trade_dates = [trade_date for trade_date in all_trade_dates if trade_date >= start_date]
        history_window: deque[list] = deque(maxlen=self.config.lookback_days)
        previous_closes: dict[str, float] = {}
        for trade_date in all_trade_dates:
            if trade_date >= start_date:
                break
            snapshots = self.provider.get_daily_snapshots(trade_date)
            history_window.append(snapshots)
            previous_closes = {
                snapshot.code: snapshot.close_price
                for snapshot in snapshots
                if snapshot.close_price is not None
            }
        state = BacktestState(
            positions={},
            nav=self.config.initial_capital,
            previous_closes=previous_closes,
            history_window=list(history_window),
            trade_days_processed=0,
        )
        return state, trade_dates

    def _run_trade_dates(
        self,
        trade_dates: list[date],
        state: BacktestState,
    ) -> BacktestResult:
        daily_nav: list[PortfolioSnapshot] = []
        latest_rankings = []
        latest_orders = []
        latest_holdings = []
        history_window: deque[list] = deque(state.history_window, maxlen=self.config.lookback_days)
        latest_snapshot_map = {}

        for trade_date in trade_dates:
            snapshots = self.provider.get_daily_snapshots(trade_date)
            history_window.append(snapshots)
            latest_snapshot_map = {snapshot.code: snapshot for snapshot in snapshots}
            close_map = {
                snapshot.code: snapshot.close_price
                for snapshot in snapshots
                if snapshot.close_price is not None
            }

            portfolio_return = 0.0
            for code, position in state.positions.items():
                current_close = close_map.get(code)
                previous_close = state.previous_closes.get(code)
                if not current_close or not previous_close:
                    continue
                portfolio_return += position.weight * (current_close / previous_close - 1.0)
            state.nav *= 1.0 + portfolio_return

            universe = self.selector.select(snapshots)
            universe = self.selector.apply_limit(universe)
            universe = self.selector.apply_industry_trend_filter(
                universe,
                history_window=list(history_window),
            )
            raw_observations = self.factor_engine.build_cross_section(
                trade_date=trade_date,
                universe=universe,
                provider=self.provider,
                history_window=list(history_window),
            )
            latest_rankings = self.preprocessor.transform(raw_observations)
            latest_orders = []
            turnover = 0.0
            should_rebalance = (
                state.trade_days_processed % self.config.rebalance_interval_trade_days == 0
            )
            if should_rebalance:
                state.positions, latest_orders, turnover = self.portfolio_manager.rebalance(
                    trade_date=trade_date,
                    ranked=latest_rankings,
                    current_positions=state.positions,
                    trade_day_index=state.trade_days_processed,
                )
                state.nav -= self._transaction_cost(state.nav, latest_orders)
            state.previous_closes = close_map
            daily_nav.append(
                PortfolioSnapshot(
                    trade_date=trade_date,
                    nav=state.nav,
                    turnover=turnover,
                    holdings=len(state.positions),
                )
            )
            state.trade_days_processed += 1

        latest_rankings_by_code = {obs.code: obs for obs in latest_rankings}
        latest_holdings = [
            HoldingSummary(
                code=position.code,
                name=latest_snapshot_map.get(position.code).name if latest_snapshot_map.get(position.code) else position.code,
                weight=position.weight,
                board=latest_snapshot_map.get(position.code).board if latest_snapshot_map.get(position.code) else None,
                industry_l1=latest_snapshot_map.get(position.code).industry_l1 if latest_snapshot_map.get(position.code) else None,
                total_score=latest_rankings_by_code.get(position.code).total_score if latest_rankings_by_code.get(position.code) else None,
            )
            for position in sorted(
                state.positions.values(),
                key=lambda item: item.weight,
                reverse=True,
            )
        ]
        state.history_window = list(history_window)
        return BacktestResult(
            daily_nav=daily_nav,
            latest_rankings=latest_rankings,
            latest_orders=latest_orders,
            latest_holdings=latest_holdings,
        )

    def _transaction_cost(self, nav: float, orders) -> float:
        buy_turnover = sum(
            max(order.to_weight - order.from_weight, 0.0)
            for order in orders
        )
        sell_turnover = sum(
            max(order.from_weight - order.to_weight, 0.0)
            for order in orders
        )
        total_rate = (
            buy_turnover * (self.config.buy_fee_rate + self.config.slippage_rate)
            + sell_turnover * (self.config.sell_fee_rate + self.config.slippage_rate)
        )
        return nav * total_rate
