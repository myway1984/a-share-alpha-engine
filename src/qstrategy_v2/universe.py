from __future__ import annotations

from collections import Counter
from statistics import mean

from .config import MultiFactorConfig
from .models import DailySnapshot


class UniverseSelector:
    def __init__(self, config: MultiFactorConfig) -> None:
        self.config = config

    def select(self, snapshots: list[DailySnapshot]) -> list[DailySnapshot]:
        return [snapshot for snapshot in snapshots if self._is_eligible(snapshot)]

    def apply_limit(self, snapshots: list[DailySnapshot]) -> list[DailySnapshot]:
        if self.config.universe_limit is None or len(snapshots) <= self.config.universe_limit:
            return snapshots

        ranked = sorted(
            snapshots,
            key=lambda snapshot: (
                snapshot.amount or 0.0,
                snapshot.total_market_cap or 0.0,
                snapshot.volume or 0.0,
            ),
            reverse=True,
        )
        limit = self.config.universe_limit
        grouped: dict[str, list[DailySnapshot]] = {}
        for snapshot in ranked:
            board = snapshot.board or "unknown"
            grouped.setdefault(board, []).append(snapshot)

        board_counts = Counter(snapshot.board or "unknown" for snapshot in ranked)
        quotas = {board: 0 for board in grouped}
        if limit >= len(grouped):
            for board in quotas:
                quotas[board] = 1
            remaining = limit - len(grouped)
        else:
            remaining = limit

        board_order = sorted(
            grouped,
            key=lambda board: (
                board_counts[board],
                grouped[board][0].amount or 0.0,
            ),
            reverse=True,
        )
        fractional_parts: list[tuple[float, str]] = []
        total = len(ranked)
        for board in board_order:
            share = board_counts[board] / total * remaining if total > 0 else 0.0
            extra = min(len(grouped[board]) - quotas[board], int(share))
            quotas[board] += extra
            fractional_parts.append((share - int(share), board))

        assigned = sum(quotas.values())
        leftovers = limit - assigned
        for _, board in sorted(fractional_parts, reverse=True):
            if leftovers <= 0:
                break
            if quotas[board] >= len(grouped[board]):
                continue
            quotas[board] += 1
            leftovers -= 1

        selected: list[DailySnapshot] = []
        selected_codes: set[str] = set()
        for board in board_order:
            for snapshot in grouped[board][: quotas[board]]:
                selected.append(snapshot)
                selected_codes.add(snapshot.code)

        if len(selected) < limit:
            for snapshot in ranked:
                if snapshot.code in selected_codes:
                    continue
                selected.append(snapshot)
                selected_codes.add(snapshot.code)
                if len(selected) >= limit:
                    break

        return selected[:limit]

    def apply_industry_trend_filter(
        self,
        snapshots: list[DailySnapshot],
        history_window: list[list[DailySnapshot]] | None,
    ) -> list[DailySnapshot]:
        if not snapshots:
            return snapshots
        if self.config.industry_filter_top_n is None and self.config.industry_filter_min_momentum is None:
            return snapshots
        if not history_window:
            return snapshots

        history_by_code: dict[str, list[DailySnapshot]] = {snapshot.code: [] for snapshot in snapshots}
        for day_snapshots in history_window:
            for snapshot in day_snapshots:
                if snapshot.code in history_by_code:
                    history_by_code[snapshot.code].append(snapshot)

        industry_returns: dict[str, list[float]] = {}
        for snapshot in snapshots:
            industry = snapshot.industry_l1
            history = history_by_code.get(snapshot.code, [])
            if not industry or len(history) < self.config.lookback_days:
                continue
            start_price = history[0].close_price
            end_price = history[-1].close_price
            if not start_price or not end_price:
                continue
            industry_returns.setdefault(industry, []).append(end_price / start_price - 1.0)

        if not industry_returns:
            return snapshots

        ranked_industries = sorted(
            (
                (industry, mean(returns))
                for industry, returns in industry_returns.items()
                if returns
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        if not ranked_industries:
            return snapshots

        selected_industries = {industry for industry, _ in ranked_industries}
        if self.config.industry_filter_top_n is not None:
            selected_industries = {
                industry
                for industry, _ in ranked_industries[: self.config.industry_filter_top_n]
            }
        if self.config.industry_filter_min_momentum is not None:
            selected_industries &= {
                industry
                for industry, score in ranked_industries
                if score >= self.config.industry_filter_min_momentum
            }
        if not selected_industries:
            return snapshots

        filtered = [
            snapshot
            for snapshot in snapshots
            if snapshot.industry_l1 in selected_industries
        ]
        minimum_required = max(
            self.config.top_n,
            min(5, max(1, len(snapshots) // 2)),
        )
        if len(filtered) < minimum_required:
            return snapshots
        return filtered

    def _is_eligible(self, snapshot: DailySnapshot) -> bool:
        if snapshot.code.startswith(("8", "9")):
            return False
        if snapshot.is_st:
            return False
        if (snapshot.listed_days or 0) < self.config.min_listing_days:
            return False
        if snapshot.is_suspended:
            return False
        if not snapshot.volume or snapshot.volume <= 0:
            return False
        if (
            snapshot.open_price is not None
            and snapshot.limit_up_price is not None
            and abs(snapshot.open_price - snapshot.limit_up_price) <= 1e-8
        ):
            return False
        if (
            snapshot.open_price is not None
            and snapshot.limit_down_price is not None
            and abs(snapshot.open_price - snapshot.limit_down_price) <= 1e-8
        ):
            return False
        return True
