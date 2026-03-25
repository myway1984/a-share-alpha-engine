from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
from statistics import mean, pstdev

from .config import MultiFactorConfig
from .data import HistoricalDataProvider
from .factors import FactorEngine
from .models import DailySnapshot
from .preprocessing import CrossSectionPreprocessor
from .universe import UniverseSelector


@dataclass(slots=True)
class FactorDiagnosticSummary:
    factor_name: str
    mean_ic: float
    ic_ir: float | None
    positive_ic_rate: float
    mean_spread: float
    positive_spread_rate: float
    observation_count: int
    average_coverage: float


@dataclass(slots=True)
class FactorDiagnosticsReport:
    start_date: date
    end_date: date
    horizon_trade_days: int
    board: str | None
    rebalance_dates: list[date]
    summaries: list[FactorDiagnosticSummary]


class FactorDiagnosticsRunner:
    def __init__(self, provider: HistoricalDataProvider, config: MultiFactorConfig) -> None:
        self.provider = provider
        self.config = config
        self.selector = UniverseSelector(config)
        self.factor_engine = FactorEngine(config)
        self.preprocessor = CrossSectionPreprocessor(config)

    def run(
        self,
        start_date: date,
        end_date: date,
        horizon_trade_days: int | None = None,
        board: str | None = None,
    ) -> FactorDiagnosticsReport:
        horizon = horizon_trade_days or self.config.rebalance_interval_trade_days
        if horizon <= 0:
            raise ValueError("horizon_trade_days must be > 0")

        warmup_start = start_date
        if self.config.lookback_days > 1:
            warmup_start = date.fromordinal(start_date.toordinal() - self.config.lookback_days * 3)
        all_trade_dates = self.provider.list_trade_dates(warmup_start, end_date)
        analysis_dates = [trade_date for trade_date in all_trade_dates if trade_date >= start_date]

        history_window: deque[list[DailySnapshot]] = deque(maxlen=self.config.lookback_days)
        for trade_date in all_trade_dates:
            if trade_date >= start_date:
                break
            history_window.append(self.provider.get_daily_snapshots(trade_date))

        daily_snapshots: dict[date, list[DailySnapshot]] = {}
        rebalance_dates: list[date] = []
        ic_by_factor: dict[str, list[float]] = defaultdict(list)
        spread_by_factor: dict[str, list[float]] = defaultdict(list)
        coverage_by_factor: dict[str, list[int]] = defaultdict(list)

        factor_names = [*self.config.active_factor_names(), "total_score"]

        for idx, trade_date in enumerate(analysis_dates):
            snapshots = daily_snapshots.setdefault(
                trade_date,
                self.provider.get_daily_snapshots(trade_date),
            )
            history_window.append(snapshots)
            if idx % self.config.rebalance_interval_trade_days != 0:
                continue
            next_idx = idx + horizon
            if next_idx >= len(analysis_dates):
                break
            future_date = analysis_dates[next_idx]
            future_snapshots = daily_snapshots.setdefault(
                future_date,
                self.provider.get_daily_snapshots(future_date),
            )
            rebalance_dates.append(trade_date)
            current_close_map = {
                snapshot.code: snapshot.close_price
                for snapshot in snapshots
                if snapshot.close_price is not None
            }
            future_close_map = {
                snapshot.code: snapshot.close_price
                for snapshot in future_snapshots
                if snapshot.close_price is not None
            }

            universe = self.selector.select(snapshots)
            if board:
                universe = [snapshot for snapshot in universe if snapshot.board == board]
            universe = self.selector.apply_limit(universe)
            universe = self.selector.apply_industry_trend_filter(
                universe,
                history_window=list(history_window),
            )
            observations = self.factor_engine.build_cross_section(
                trade_date=trade_date,
                universe=universe,
                provider=self.provider,
                history_window=list(history_window),
            )
            if board:
                observations = [obs for obs in observations if obs.board == board]
            ranked = self.preprocessor.transform(observations)
            realized_returns = {
                obs.code: future_close_map[obs.code] / current_close_map[obs.code] - 1.0
                for obs in ranked
                if obs.code in current_close_map
                and obs.code in future_close_map
                and current_close_map[obs.code]
            }
            if len(realized_returns) < 5:
                continue

            for factor_name in factor_names:
                exposures = _collect_exposures(ranked, factor_name)
                paired = [
                    (exposures[code], realized_returns[code])
                    for code in exposures
                    if code in realized_returns
                ]
                if len(paired) < 5:
                    continue
                ic = spearman_rank_corr(paired)
                spread = quantile_return_spread(paired)
                if ic is not None:
                    ic_by_factor[factor_name].append(ic)
                if spread is not None:
                    spread_by_factor[factor_name].append(spread)
                coverage_by_factor[factor_name].append(len(paired))

        summaries: list[FactorDiagnosticSummary] = []
        for factor_name in factor_names:
            ic_values = ic_by_factor.get(factor_name, [])
            spread_values = spread_by_factor.get(factor_name, [])
            coverage_values = coverage_by_factor.get(factor_name, [])
            if not ic_values or not spread_values or not coverage_values:
                continue
            ic_std = pstdev(ic_values) if len(ic_values) >= 2 else 0.0
            summaries.append(
                FactorDiagnosticSummary(
                    factor_name=factor_name,
                    mean_ic=mean(ic_values),
                    ic_ir=(mean(ic_values) / ic_std) if ic_std > 0 else None,
                    positive_ic_rate=sum(1 for value in ic_values if value > 0) / len(ic_values),
                    mean_spread=mean(spread_values),
                    positive_spread_rate=(
                        sum(1 for value in spread_values if value > 0) / len(spread_values)
                    ),
                    observation_count=len(ic_values),
                    average_coverage=mean(coverage_values),
                )
            )
        summaries.sort(key=lambda item: item.mean_ic, reverse=True)
        return FactorDiagnosticsReport(
            start_date=start_date,
            end_date=end_date,
            horizon_trade_days=horizon,
            board=board,
            rebalance_dates=rebalance_dates,
            summaries=summaries,
        )


def _collect_exposures(ranked, factor_name: str) -> dict[str, float]:
    if factor_name == "total_score":
        return {
            obs.code: obs.total_score
            for obs in ranked
            if obs.total_score != float("-inf")
        }
    return {
        obs.code: obs.processed_factors[factor_name]
        for obs in ranked
        if factor_name in obs.processed_factors
    }


def average_ranks(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    idx = 0
    while idx < len(ordered):
        end_idx = idx
        while end_idx + 1 < len(ordered) and ordered[end_idx + 1][1] == ordered[idx][1]:
            end_idx += 1
        average_rank = (idx + end_idx + 2) / 2.0
        for item_idx in range(idx, end_idx + 1):
            original_idx, _ = ordered[item_idx]
            ranks[original_idx] = average_rank
        idx = end_idx + 1
    return ranks


def spearman_rank_corr(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    rank_x = average_ranks(xs)
    rank_y = average_ranks(ys)
    mean_x = mean(rank_x)
    mean_y = mean(rank_y)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(rank_x, rank_y))
    var_x = sum((x - mean_x) ** 2 for x in rank_x)
    var_y = sum((y - mean_y) ** 2 for y in rank_y)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / (var_x * var_y) ** 0.5


def quantile_return_spread(pairs: list[tuple[float, float]], quantile: float = 0.2) -> float | None:
    if len(pairs) < 5:
        return None
    ordered = sorted(pairs, key=lambda item: item[0], reverse=True)
    bucket_size = max(1, int(len(ordered) * quantile))
    top_bucket = ordered[:bucket_size]
    bottom_bucket = ordered[-bucket_size:]
    if not top_bucket or not bottom_bucket:
        return None
    return mean(item[1] for item in top_bucket) - mean(item[1] for item in bottom_bucket)


def write_factor_diagnostic_outputs(
    report: FactorDiagnosticsReport,
    output_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest_factor_diagnostics.json"
    md_path = output_dir / "latest_factor_diagnostics.md"
    payload = {
        "start_date": report.start_date.isoformat(),
        "end_date": report.end_date.isoformat(),
        "horizon_trade_days": report.horizon_trade_days,
        "board": report.board,
        "rebalance_dates": [item.isoformat() for item in report.rebalance_dates],
        "summaries": [
            {
                "factor_name": item.factor_name,
                "mean_ic": round(item.mean_ic, 6),
                "ic_ir": round(item.ic_ir, 6) if item.ic_ir is not None else None,
                "positive_ic_rate": round(item.positive_ic_rate, 6),
                "mean_spread": round(item.mean_spread, 6),
                "positive_spread_rate": round(item.positive_spread_rate, 6),
                "observation_count": item.observation_count,
                "average_coverage": round(item.average_coverage, 2),
            }
            for item in report.summaries
        ],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_factor_diagnostics_markdown(report), encoding="utf-8")
    return json_path, md_path


def render_factor_diagnostics_markdown(report: FactorDiagnosticsReport) -> str:
    lines = ["# A股多因子诊断摘要", ""]
    lines.append(
        f"- 区间：`{report.start_date.isoformat()}` ~ `{report.end_date.isoformat()}` | "
        f"持有周期：`{report.horizon_trade_days}` 个交易日 | "
        f"样本期数：`{len(report.rebalance_dates)}` | "
        f"板块：`{report.board or 'all'}`"
    )
    lines.append("")
    lines.append("## 因子表现")
    lines.append("")
    for item in report.summaries:
        ic_ir = f"{item.ic_ir:.4f}" if item.ic_ir is not None else "n/a"
        lines.append(
            f"- `{item.factor_name}` | mean_ic `{item.mean_ic:.4f}` | "
            f"ic_ir `{ic_ir}` | positive_ic `{item.positive_ic_rate:.2%}` | "
            f"mean_spread `{item.mean_spread:.4f}` | positive_spread `{item.positive_spread_rate:.2%}` | "
            f"coverage `{item.average_coverage:.1f}`"
        )
    return "\n".join(lines) + "\n"
