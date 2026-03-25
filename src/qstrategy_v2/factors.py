from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from math import sqrt
from statistics import mean, pstdev

from .config import MultiFactorConfig
from .data import HistoricalDataProvider
from .models import DailySnapshot, FactorObservation, QuarterlyReport


class FactorEngine:
    def __init__(self, config: MultiFactorConfig) -> None:
        self.config = config

    def build_cross_section(
        self,
        trade_date: date,
        universe: Iterable[DailySnapshot],
        provider: HistoricalDataProvider,
        history_window: list[list[DailySnapshot]] | None = None,
    ) -> list[FactorObservation]:
        observations: list[FactorObservation] = []
        universe_list = list(universe)
        history_by_code = self._build_history_by_code(universe_list, history_window)

        for snapshot in universe_list:
            history = history_by_code.get(snapshot.code)
            if history is None:
                history = provider.get_price_history(
                    snapshot.code,
                    end_date=trade_date,
                    lookback_days=self.config.lookback_days,
                )
            if len(history) < self.config.lookback_days:
                continue
            reports = provider.get_financial_reports(snapshot.code, trade_date, limit=12)
            raw_factors = {
                "one_month_reversal": self._one_month_reversal(history),
                "turnover_20d": self._turnover_20d(history),
                "volatility_20d": self._volatility_20d(history),
                "price_volume_corr": self._price_volume_corr(history),
                "ep": self._ep(snapshot),
                "roe_ttm": self._latest_roe_ttm(reports, trade_date),
                "sue": self._sue(reports, trade_date),
            }
            observations.append(
                FactorObservation(
                    code=snapshot.code,
                    name=snapshot.name,
                    trade_date=trade_date,
                    industry_l1=snapshot.industry_l1,
                    total_market_cap=snapshot.total_market_cap,
                    board=snapshot.board,
                    raw_factors=raw_factors,
                )
            )
        return observations

    def _build_history_by_code(
        self,
        universe: list[DailySnapshot],
        history_window: list[list[DailySnapshot]] | None,
    ) -> dict[str, list[DailySnapshot]]:
        if not history_window:
            return {}
        target_codes = {snapshot.code for snapshot in universe}
        history_by_code: dict[str, list[DailySnapshot]] = {code: [] for code in target_codes}
        for day_snapshots in history_window:
            for snapshot in day_snapshots:
                if snapshot.code in history_by_code:
                    history_by_code[snapshot.code].append(snapshot)
        return history_by_code

    @staticmethod
    def _one_month_reversal(history: list[DailySnapshot]) -> float | None:
        start_price = history[0].close_price
        end_price = history[-1].close_price
        if not start_price or not end_price:
            return None
        return end_price / start_price - 1.0

    @staticmethod
    def _turnover_20d(history: list[DailySnapshot]) -> float | None:
        values = [item.turnover_rate for item in history if item.turnover_rate is not None]
        if not values:
            return None
        return mean(values)

    @staticmethod
    def _volatility_20d(history: list[DailySnapshot]) -> float | None:
        returns: list[float] = []
        for previous, current in zip(history, history[1:]):
            if not previous.close_price or not current.close_price:
                continue
            returns.append(current.close_price / previous.close_price - 1.0)
        if len(returns) < 2:
            return None
        return pstdev(returns)

    @staticmethod
    def _price_volume_corr(history: list[DailySnapshot]) -> float | None:
        prices = [item.close_price for item in history if item.close_price is not None]
        volumes = [item.volume for item in history if item.volume is not None]
        size = min(len(prices), len(volumes))
        if size < 3:
            return None
        return pearson_corr(prices[-size:], volumes[-size:])

    @staticmethod
    def _ep(snapshot: DailySnapshot) -> float | None:
        if snapshot.pe_ttm is None or snapshot.pe_ttm <= 0:
            return None
        return 1.0 / snapshot.pe_ttm

    @staticmethod
    def _sue(reports: list[QuarterlyReport], trade_date: date) -> float | None:
        visible_reports = [
            report
            for report in reports
            if report.announce_date <= trade_date and report.single_quarter_net_profit is not None
        ]
        if len(visible_reports) < 5:
            return None
        visible_reports.sort(key=lambda item: item.report_period)
        series_by_period = {
            (report.report_period.year, report.report_period.month): report
            for report in visible_reports
        }
        yoy_diffs: list[float] = []
        for report in visible_reports:
            prior = series_by_period.get((report.report_period.year - 1, report.report_period.month))
            if prior is None or prior.single_quarter_net_profit is None:
                continue
            yoy_diffs.append(report.single_quarter_net_profit - prior.single_quarter_net_profit)
        if len(yoy_diffs) < 2:
            return None
        latest_diff = yoy_diffs[-1]
        trailing = yoy_diffs[-8:]
        if len(trailing) < 2:
            return None
        std = pstdev(trailing)
        if std == 0:
            return None
        return latest_diff / std

    @staticmethod
    def _latest_roe_ttm(reports: list[QuarterlyReport], trade_date: date) -> float | None:
        visible_reports = [
            report
            for report in reports
            if report.announce_date <= trade_date and report.roe_ttm is not None
        ]
        if not visible_reports:
            return None
        visible_reports.sort(key=lambda item: (item.announce_date, item.report_period))
        return visible_reports[-1].roe_ttm


def pearson_corr(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = mean(xs)
    mean_y = mean(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / sqrt(var_x * var_y)
