from datetime import date

from qstrategy_v2.data import build_quarterly_reports, single_quarter_profit


def test_single_quarter_profit_uses_cumulative_deltas() -> None:
    cumulative = {
        "20240331": 10.0,
        "20240630": 30.0,
        "20240930": 45.0,
        "20241231": 80.0,
    }

    assert single_quarter_profit(date(2024, 3, 31), 10.0, cumulative) == 10.0
    assert single_quarter_profit(date(2024, 6, 30), 30.0, cumulative) == 20.0
    assert single_quarter_profit(date(2024, 9, 30), 45.0, cumulative) == 15.0
    assert single_quarter_profit(date(2024, 12, 31), 80.0, cumulative) == 35.0


def test_build_quarterly_reports_prefers_disclosure_actual_date() -> None:
    reports = build_quarterly_reports(
        code="600000",
        income_rows=[
            {
                "ts_code": "600000.SH",
                "ann_date": "20240429",
                "f_ann_date": "20240429",
                "end_date": "20240331",
                "report_type": "1",
                "n_income": 10.0,
            },
            {
                "ts_code": "600000.SH",
                "ann_date": "20240829",
                "f_ann_date": "20240829",
                "end_date": "20240630",
                "report_type": "1",
                "n_income": 30.0,
            },
        ],
        indicator_rows=[
            {"ts_code": "600000.SH", "ann_date": "20240429", "end_date": "20240331", "roe_dt": 0.12},
            {"ts_code": "600000.SH", "ann_date": "20240829", "end_date": "20240630", "roe_dt": 0.15},
        ],
        disclosure_rows=[
            {"ts_code": "600000.SH", "ann_date": "20240429", "end_date": "20240331", "actual_date": "20240430"},
            {"ts_code": "600000.SH", "ann_date": "20240829", "end_date": "20240630", "actual_date": "20240830"},
        ],
    )

    assert reports[0].announce_date.isoformat() == "2024-04-30"
    assert reports[1].single_quarter_net_profit == 20.0
    assert reports[1].roe_ttm == 0.15
