from __future__ import annotations

from math import log
from statistics import mean, median, pstdev

from .config import MultiFactorConfig
from .models import FactorObservation


class CrossSectionPreprocessor:
    def __init__(self, config: MultiFactorConfig) -> None:
        self.config = config

    def transform(self, observations: list[FactorObservation]) -> list[FactorObservation]:
        processed_by_factor: dict[str, dict[str, float]] = {}
        factor_names = self.config.active_factor_names()

        for factor_name in factor_names:
            raw_values = {
                obs.code: value
                for obs in observations
                if (value := obs.raw_factors.get(factor_name)) is not None
            }
            if len(raw_values) < 3:
                continue
            winsorized = winsorize_by_board(observations, raw_values)
            standardized = zscore_by_board(observations, winsorized)
            neutralized = neutralize(
                observations=observations,
                standardized_values=standardized,
            )
            aligned = {
                code: residual * self.config.factor_directions[factor_name]
                for code, residual in neutralized.items()
            }
            processed_by_factor[factor_name] = aligned

        for obs in observations:
            obs.processed_factors = {
                factor_name: values[obs.code]
                for factor_name, values in processed_by_factor.items()
                if obs.code in values
            }
            if obs.processed_factors:
                total_weight = sum(
                    self.config.factor_weight(factor_name, obs.board)
                    for factor_name in obs.processed_factors
                )
                if total_weight > 0:
                    weighted_sum = sum(
                        value * self.config.factor_weight(factor_name, obs.board)
                        for factor_name, value in obs.processed_factors.items()
                    )
                    obs.total_score = weighted_sum / total_weight
                else:
                    obs.total_score = float("-inf")
            else:
                obs.total_score = float("-inf")

        observations.sort(key=lambda item: item.total_score, reverse=True)
        return observations


def winsorize_mad(values: dict[str, float]) -> dict[str, float]:
    center = median(values.values())
    mad = median(abs(value - center) for value in values.values())
    if mad == 0:
        return dict(values)
    lower = center - 3.0 * mad
    upper = center + 3.0 * mad
    return {code: min(max(value, lower), upper) for code, value in values.items()}


def winsorize_by_board(
    observations: list[FactorObservation],
    values: dict[str, float],
) -> dict[str, float]:
    grouped_codes = group_codes_by_board(observations, values)
    winsorized = dict(values)
    for codes in grouped_codes.values():
        group_values = {code: values[code] for code in codes}
        if len(group_values) < 3:
            continue
        winsorized.update(winsorize_mad(group_values))
    return winsorized


def zscore(values: dict[str, float]) -> dict[str, float]:
    avg = mean(values.values())
    std = pstdev(values.values())
    if std == 0:
        return {code: 0.0 for code in values}
    return {code: (value - avg) / std for code, value in values.items()}


def zscore_by_board(
    observations: list[FactorObservation],
    values: dict[str, float],
) -> dict[str, float]:
    global_standardized = zscore(values)
    grouped_codes = group_codes_by_board(observations, values)
    standardized = dict(global_standardized)
    for codes in grouped_codes.values():
        group_values = {code: values[code] for code in codes}
        if len(group_values) < 3:
            continue
        standardized.update(zscore(group_values))
    return standardized


def group_codes_by_board(
    observations: list[FactorObservation],
    values: dict[str, float],
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for obs in observations:
        if obs.code not in values:
            continue
        board = obs.board or "unknown"
        grouped.setdefault(board, []).append(obs.code)
    return grouped


def neutralize(
    observations: list[FactorObservation],
    standardized_values: dict[str, float],
) -> dict[str, float]:
    eligible = [
        obs
        for obs in observations
        if obs.code in standardized_values and obs.total_market_cap and obs.total_market_cap > 0
    ]
    if len(eligible) < 3:
        return dict(standardized_values)

    industries = sorted(
        {
            obs.industry_l1
            for obs in eligible
            if obs.industry_l1
        }
    )
    industry_to_idx = {industry: idx for idx, industry in enumerate(industries[1:], start=0)}
    boards = sorted(
        {
            obs.board
            for obs in eligible
            if obs.board
        }
    )
    board_to_idx = {board: idx for idx, board in enumerate(boards[1:], start=0)}

    x_rows: list[list[float]] = []
    y_values: list[float] = []
    codes: list[str] = []
    for obs in eligible:
        row = [1.0, log(obs.total_market_cap)]
        dummies = [0.0] * len(industry_to_idx)
        if obs.industry_l1 in industry_to_idx:
            dummies[industry_to_idx[obs.industry_l1]] = 1.0
        row.extend(dummies)
        board_dummies = [0.0] * len(board_to_idx)
        if obs.board in board_to_idx:
            board_dummies[board_to_idx[obs.board]] = 1.0
        row.extend(board_dummies)
        x_rows.append(row)
        y_values.append(standardized_values[obs.code])
        codes.append(obs.code)

    beta = ordinary_least_squares(x_rows, y_values)
    if beta is None:
        return dict(standardized_values)

    residuals = dict(standardized_values)
    for code, row, y in zip(codes, x_rows, y_values):
        fitted = sum(weight * value for weight, value in zip(beta, row))
        residuals[code] = y - fitted
    return residuals


def ordinary_least_squares(x_rows: list[list[float]], y_values: list[float]) -> list[float] | None:
    if not x_rows:
        return None
    width = len(x_rows[0])
    xtx = [[0.0 for _ in range(width)] for _ in range(width)]
    xty = [0.0 for _ in range(width)]
    for row, y in zip(x_rows, y_values):
        for i in range(width):
            xty[i] += row[i] * y
            for j in range(width):
                xtx[i][j] += row[i] * row[j]
    return solve_linear_system(xtx, xty)


def solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float] | None:
    size = len(vector)
    augmented = [row[:] + [vector[idx]] for idx, row in enumerate(matrix)]
    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(augmented[row][col]))
        if abs(augmented[pivot][col]) < 1e-10:
            return None
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        pivot_value = augmented[col][col]
        augmented[col] = [value / pivot_value for value in augmented[col]]
        for row in range(size):
            if row == col:
                continue
            factor = augmented[row][col]
            augmented[row] = [
                current - factor * pivot_item
                for current, pivot_item in zip(augmented[row], augmented[col])
            ]
    return [augmented[row][-1] for row in range(size)]
