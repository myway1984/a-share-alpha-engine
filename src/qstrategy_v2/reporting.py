from __future__ import annotations

from pathlib import Path
import json

from .models import BacktestResult


def write_backtest_outputs(result: BacktestResult, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest_backtest.json"
    md_path = output_dir / "latest_backtest.md"

    payload = {
        "daily_nav": [
            {
                "trade_date": snapshot.trade_date.isoformat(),
                "nav": round(snapshot.nav, 4),
                "turnover": round(snapshot.turnover, 4),
                "holdings": snapshot.holdings,
            }
            for snapshot in result.daily_nav
        ],
        "latest_rankings": [
            {
                "code": obs.code,
                "name": obs.name,
                "score": round(obs.total_score, 6),
                "processed_factors": {
                    name: round(value, 6)
                    for name, value in obs.processed_factors.items()
                },
            }
            for obs in result.latest_rankings[:50]
        ],
        "latest_orders": [
            {
                "trade_date": order.trade_date.isoformat(),
                "code": order.code,
                "side": order.side,
                "from_weight": round(order.from_weight, 6),
                "to_weight": round(order.to_weight, 6),
                "reason": order.reason,
            }
            for order in result.latest_orders
        ],
        "latest_holdings": [
            {
                "code": holding.code,
                "name": holding.name,
                "weight": round(holding.weight, 6),
                "board": holding.board,
                "industry_l1": holding.industry_l1,
                "total_score": round(holding.total_score, 6) if holding.total_score is not None else None,
            }
            for holding in result.latest_holdings
        ],
        "segments": [
            {
                "start_date": segment.start_date.isoformat(),
                "end_date": segment.end_date.isoformat(),
                "trade_days": segment.trade_days,
                "ending_nav": round(segment.ending_nav, 4),
                "holdings": segment.holdings,
            }
            for segment in result.segment_summaries
        ],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(result), encoding="utf-8")
    return json_path, md_path


def render_markdown(result: BacktestResult) -> str:
    lines = ["# A股截面多因子策略 V2 回测摘要", ""]
    if result.daily_nav:
        latest = result.daily_nav[-1]
        lines.append(
            f"- 最新净值：`{latest.nav:.4f}` | 日期：`{latest.trade_date.isoformat()}` | 持仓数：`{latest.holdings}` | 换手：`{latest.turnover:.4f}`"
        )
    else:
        lines.append("- 暂无回测结果。")
    lines.append("")
    if result.segment_summaries:
        lines.append("## 分段摘要")
        lines.append("")
        for segment in result.segment_summaries:
            lines.append(
                f"- `{segment.start_date.isoformat()}` ~ `{segment.end_date.isoformat()}` | "
                f"交易日 `{segment.trade_days}` | 结束净值 `{segment.ending_nav:.4f}` | 持仓 `{segment.holdings}`"
            )
    lines.append("")
    lines.append("## 最新持仓")
    lines.append("")
    for holding in result.latest_holdings[:20]:
        score_text = "-" if holding.total_score is None else f"{holding.total_score:.4f}"
        lines.append(
            f"- `{holding.code}` {holding.name} | 权重 `{holding.weight:.2%}` | "
            f"板块 `{holding.board or '-'} ` | 行业 `{holding.industry_l1 or '-'} ` | score `{score_text}`"
        )
    lines.append("")
    lines.append("## 最新 Top 10")
    lines.append("")
    for obs in result.latest_rankings[:10]:
        lines.append(f"- `{obs.code}` {obs.name} | score `{obs.total_score:.4f}`")
    lines.append("")
    lines.append("## 最新调仓")
    lines.append("")
    for order in result.latest_orders[:20]:
        lines.append(
            f"- `{order.side}` `{order.code}` | `{order.from_weight:.2%}` -> `{order.to_weight:.2%}` | {order.reason}"
        )
    return "\n".join(lines) + "\n"
