from __future__ import annotations

from datetime import date

from .config import MultiFactorConfig
from .models import FactorObservation, Order, Position


class PortfolioManager:
    def __init__(self, config: MultiFactorConfig) -> None:
        self.config = config

    def rebalance(
        self,
        trade_date: date,
        ranked: list[FactorObservation],
        current_positions: dict[str, Position],
        trade_day_index: int,
    ) -> tuple[dict[str, Position], list[Order], float]:
        rank_map = {obs.code: idx for idx, obs in enumerate(ranked, start=1)}
        next_positions: dict[str, Position] = {}
        orders: list[Order] = []
        sell_candidates: list[tuple[float, str, Position]] = []
        target_count = min(self.config.top_n, len(ranked)) if self.config.top_n > 0 else 0

        for code, position in current_positions.items():
            rank = rank_map.get(code)
            can_keep = rank is not None and rank <= self.config.buffer_rank
            if position.buy_date == trade_date:
                can_keep = True
            held_calendar_days = (trade_date - position.buy_date).days
            held_trade_days = max(0, trade_day_index - position.buy_trade_index)
            if self.config.min_holding_trade_days is not None:
                under_min_holding = held_trade_days < self.config.min_holding_trade_days
            else:
                under_min_holding = held_calendar_days < self.config.min_holding_days
            if not can_keep and rank is not None and under_min_holding:
                can_keep = True
            if can_keep:
                next_positions[code] = position
            else:
                sell_rank = float("inf") if rank is None else float(rank)
                sell_candidates.append((sell_rank, code, position))

        buy_candidates = [obs for obs in ranked if obs.code not in next_positions]
        available_slots = max(0, target_count - len(next_positions))
        max_new = self.config.max_new_positions_per_rebalance
        if not current_positions:
            buy_budget = available_slots
        elif max_new is None:
            buy_budget = available_slots
        else:
            buy_budget = min(available_slots, max_new)
        buy_budget = min(buy_budget, len(buy_candidates))

        required_sells = max(0, len(next_positions) + len(sell_candidates) + buy_budget - target_count)
        sell_candidates.sort(key=lambda item: item[0], reverse=True)
        selected_sells = sell_candidates[:required_sells]
        kept_candidates = sell_candidates[required_sells:]

        for _, code, position in kept_candidates:
            next_positions[code] = position

        for _, code, position in selected_sells:
            orders.append(
                Order(
                    trade_date=trade_date,
                    code=code,
                    side="SELL",
                    from_weight=position.weight,
                    to_weight=0.0,
                    reason="rank_below_buffer",
                )
            )

        target_weight = 1.0 / target_count if target_count > 0 else 0.0
        for obs in buy_candidates[:buy_budget]:
            previous_weight = current_positions.get(obs.code).weight if obs.code in current_positions else 0.0
            next_positions[obs.code] = Position(
                code=obs.code,
                weight=target_weight,
                buy_date=trade_date,
                buy_trade_index=trade_day_index,
            )
            orders.append(
                Order(
                    trade_date=trade_date,
                    code=obs.code,
                    side="BUY",
                    from_weight=previous_weight,
                    to_weight=target_weight,
                    reason="rank_in_top_bucket",
                )
            )

        for code, position in list(next_positions.items()):
            next_positions[code] = Position(
                code=code,
                weight=target_weight,
                buy_date=position.buy_date,
                buy_trade_index=position.buy_trade_index,
            )

        turnover = sum(abs(order.to_weight - order.from_weight) for order in orders)
        return next_positions, orders, turnover
