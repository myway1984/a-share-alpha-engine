from __future__ import annotations

from datetime import date
from datetime import timedelta
from collections import deque
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Protocol

import requests

from .eastmoney import EastmoneyClient

from .models import DailySnapshot, QuarterlyReport


class ProviderNotReadyError(RuntimeError):
    """Raised when the historical data provider is not wired up yet."""


class DataProviderError(RuntimeError):
    """Raised when the external market data provider returns an error."""


class HistoricalDataProvider(Protocol):
    def list_trade_dates(self, start_date: date, end_date: date) -> list[date]:
        ...

    def get_daily_snapshots(self, trade_date: date) -> list[DailySnapshot]:
        ...

    def get_price_history(
        self, code: str, end_date: date, lookback_days: int
    ) -> list[DailySnapshot]:
        ...

    def get_financial_reports(
        self, code: str, end_date: date, limit: int = 12
    ) -> list[QuarterlyReport]:
        ...

    def warm_financial_cache(self, codes: list[str], end_date: date) -> int:
        ...

    def warm_market_cache(self, start_date: date, end_date: date) -> int:
        ...


class TushareHistoryProvider:
    api_url = "http://api.tushare.pro"

    def __init__(
        self,
        token: str,
        cache_dir: Path | None = None,
        timeout: int = 30,
    ) -> None:
        if not token:
            raise DataProviderError("Missing TUSHARE_TOKEN.")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        self.cache_dir = cache_dir or Path(".cache/tushare")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._stock_basics: dict[str, dict[str, str]] | None = None
        self._industry_map: dict[str, str] | None = None
        self._trade_dates_cache: dict[tuple[str, str], list[date]] = {}
        self._daily_snapshots_cache: dict[str, list[DailySnapshot]] = {}
        self._price_history_cache: dict[tuple[str, str, int], list[DailySnapshot]] = {}
        self._financial_reports_cache: dict[str, list[QuarterlyReport]] = {}
        self._request_timestamps: deque[float] = deque()
        self._max_requests_per_minute = 180

    @classmethod
    def from_env(cls, cache_dir: Path | None = None) -> "TushareHistoryProvider":
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise DataProviderError("Missing TUSHARE_TOKEN. 请先在环境变量中设置。")
        return cls(token=token, cache_dir=cache_dir)

    def list_trade_dates(self, start_date: date, end_date: date) -> list[date]:
        key = (start_date.isoformat(), end_date.isoformat())
        if key in self._trade_dates_cache:
            return self._trade_dates_cache[key]
        payload = self._query(
            "trade_cal",
            params={
                "exchange": "",
                "start_date": to_tushare_date(start_date),
                "end_date": to_tushare_date(end_date),
                "is_open": "1",
            },
            fields="cal_date,is_open",
        )
        dates = sorted(
            date_from_tushare(row["cal_date"])
            for row in payload
            if str(row.get("is_open")) == "1"
        )
        self._trade_dates_cache[key] = dates
        return dates

    def get_daily_snapshots(self, trade_date: date) -> list[DailySnapshot]:
        cache_key = trade_date.isoformat()
        if cache_key in self._daily_snapshots_cache:
            return self._daily_snapshots_cache[cache_key]

        basics = self._load_stock_basics()
        industry_map = self._load_industry_map()
        tushare_trade_date = to_tushare_date(trade_date)

        daily_rows = self._query(
            "daily",
            params={"trade_date": tushare_trade_date},
            fields="ts_code,trade_date,open,close,vol,amount",
            use_cache=True,
        )
        daily_basic_rows = self._query(
            "daily_basic",
            params={"trade_date": tushare_trade_date},
            fields="ts_code,trade_date,turnover_rate,pe_ttm,total_mv",
            use_cache=True,
        )
        limit_rows = self._query(
            "stk_limit",
            params={"trade_date": tushare_trade_date},
            fields="ts_code,trade_date,up_limit,down_limit",
            use_cache=True,
        )
        suspend_rows = self._query(
            "suspend_d",
            params={"trade_date": tushare_trade_date, "suspend_type": "S"},
            fields="ts_code,trade_date,suspend_type",
            use_cache=True,
        )

        daily_map = {row["ts_code"]: row for row in daily_rows}
        daily_basic_map = {row["ts_code"]: row for row in daily_basic_rows}
        limit_map = {row["ts_code"]: row for row in limit_rows}
        suspended = {row["ts_code"] for row in suspend_rows}

        snapshots: list[DailySnapshot] = []
        for ts_code, row in daily_map.items():
            basic = basics.get(ts_code)
            if basic is None:
                continue
            code, exchange = split_ts_code(ts_code)
            daily_basic = daily_basic_map.get(ts_code, {})
            limit_row = limit_map.get(ts_code, {})
            stock_name = basic.get("name", code)
            snapshots.append(
                DailySnapshot(
                    code=code,
                    name=stock_name,
                    trade_date=trade_date,
                    exchange=exchange,
                    industry_l1=industry_map.get(ts_code),
                    listed_days=days_between(
                        date_from_tushare(basic["list_date"]),
                        trade_date,
                    ),
                    is_st=is_st_stock_name(stock_name),
                    is_suspended=ts_code in suspended,
                    open_price=to_float(row.get("open")),
                    close_price=to_float(row.get("close")),
                    volume=to_float(row.get("vol")),
                    amount=scaled_amount_to_yuan(row.get("amount")),
                    turnover_rate=percent_to_ratio(daily_basic.get("turnover_rate")),
                    total_market_cap=wan_to_yuan(daily_basic.get("total_mv")),
                    limit_up_price=to_float(limit_row.get("up_limit")),
                    limit_down_price=to_float(limit_row.get("down_limit")),
                    pe_ttm=to_float(daily_basic.get("pe_ttm")),
                    roe_ttm=None,
                    board=infer_board(code, exchange),
                )
            )

        self._daily_snapshots_cache[cache_key] = snapshots
        return snapshots

    def get_price_history(
        self, code: str, end_date: date, lookback_days: int
    ) -> list[DailySnapshot]:
        cache_key = (code, end_date.isoformat(), lookback_days)
        if cache_key in self._price_history_cache:
            return self._price_history_cache[cache_key]

        basics = self._load_stock_basics()
        ts_code = to_ts_code(code, basics)
        if ts_code is None:
            return []
        basic = basics[ts_code]
        industry_map = self._load_industry_map()
        start_date = end_date - timedelta(days=max(lookback_days * 4, 120))

        daily_rows = self._query(
            "daily",
            params={
                "ts_code": ts_code,
                "start_date": to_tushare_date(start_date),
                "end_date": to_tushare_date(end_date),
            },
            fields="ts_code,trade_date,open,close,vol,amount",
            use_cache=True,
        )
        daily_basic_rows = self._query(
            "daily_basic",
            params={
                "ts_code": ts_code,
                "start_date": to_tushare_date(start_date),
                "end_date": to_tushare_date(end_date),
            },
            fields="ts_code,trade_date,turnover_rate,pe_ttm,total_mv",
            use_cache=True,
        )
        limit_rows = self._query(
            "stk_limit",
            params={
                "ts_code": ts_code,
                "start_date": to_tushare_date(start_date),
                "end_date": to_tushare_date(end_date),
            },
            fields="ts_code,trade_date,up_limit,down_limit",
            use_cache=True,
        )
        daily_basic_by_date = {row["trade_date"]: row for row in daily_basic_rows}
        limit_by_date = {row["trade_date"]: row for row in limit_rows}
        stock_name = basic.get("name", code)
        history = [
            DailySnapshot(
                code=code,
                name=stock_name,
                trade_date=date_from_tushare(row["trade_date"]),
                exchange=split_ts_code(ts_code)[1],
                industry_l1=industry_map.get(ts_code),
                listed_days=days_between(date_from_tushare(basic["list_date"]), date_from_tushare(row["trade_date"])),
                is_st=is_st_stock_name(stock_name),
                is_suspended=False,
                open_price=to_float(row.get("open")),
                close_price=to_float(row.get("close")),
                volume=to_float(row.get("vol")),
                amount=scaled_amount_to_yuan(row.get("amount")),
                turnover_rate=percent_to_ratio(daily_basic_by_date.get(row["trade_date"], {}).get("turnover_rate")),
                total_market_cap=wan_to_yuan(daily_basic_by_date.get(row["trade_date"], {}).get("total_mv")),
                limit_up_price=to_float(limit_by_date.get(row["trade_date"], {}).get("up_limit")),
                limit_down_price=to_float(limit_by_date.get(row["trade_date"], {}).get("down_limit")),
                pe_ttm=to_float(daily_basic_by_date.get(row["trade_date"], {}).get("pe_ttm")),
                roe_ttm=None,
                board=infer_board(code, split_ts_code(ts_code)[1]),
            )
            for row in daily_rows
        ]
        history.sort(key=lambda item: item.trade_date)
        trimmed = history[-lookback_days:]
        self._price_history_cache[cache_key] = trimmed
        return trimmed

    def get_financial_reports(
        self, code: str, end_date: date, limit: int = 12
    ) -> list[QuarterlyReport]:
        if code not in self._financial_reports_cache:
            self._financial_reports_cache[code] = self._load_all_financial_reports(code)

        reports = [
            report
            for report in self._financial_reports_cache[code]
            if report.announce_date <= end_date
        ]
        reports.sort(key=lambda item: (item.announce_date, item.report_period))
        return reports[-limit:]

    def warm_financial_cache(self, codes: list[str], end_date: date) -> int:
        warmed = 0
        seen: set[str] = set()
        for code in codes:
            if code in seen:
                continue
            seen.add(code)
            self.get_financial_reports(code, end_date=end_date, limit=12)
            warmed += 1
        return warmed

    def warm_market_cache(self, start_date: date, end_date: date) -> int:
        trade_dates = self.list_trade_dates(start_date=start_date, end_date=end_date)
        for trade_date in trade_dates:
            self.get_daily_snapshots(trade_date)
        return len(trade_dates)

    def _load_all_financial_reports(self, code: str) -> list[QuarterlyReport]:
        basics = self._load_stock_basics()
        ts_code = to_ts_code(code, basics)
        if ts_code is None:
            return []
        income_rows = self._query(
            "income",
            params={"ts_code": ts_code},
            fields="ts_code,ann_date,f_ann_date,end_date,report_type,n_income",
            use_cache=True,
        )
        indicator_rows = self._query(
            "fina_indicator",
            params={"ts_code": ts_code},
            fields="ts_code,ann_date,end_date,roe_dt",
            use_cache=True,
        )
        disclosure_rows = self._query(
            "disclosure_date",
            params={"ts_code": ts_code},
            fields="ts_code,ann_date,end_date,actual_date",
            use_cache=True,
        )
        return build_quarterly_reports(
            code=code,
            income_rows=income_rows,
            indicator_rows=indicator_rows,
            disclosure_rows=disclosure_rows,
        )

    def _load_stock_basics(self) -> dict[str, dict[str, str]]:
        if self._stock_basics is not None:
            return self._stock_basics
        rows = self._query(
            "stock_basic",
            params={"list_status": "L"},
            fields="ts_code,symbol,name,list_date,exchange,market",
            use_cache=True,
        )
        self._stock_basics = {row["ts_code"]: row for row in rows}
        return self._stock_basics

    def _load_industry_map(self) -> dict[str, str]:
        if self._industry_map is not None:
            return self._industry_map

        industry_rows = self._query(
            "index_classify",
            params={"level": "L1", "src": "SW2021"},
            fields="index_code,industry_name",
            use_cache=True,
        )
        mapping: dict[str, str] = {}
        for industry in industry_rows:
            index_code = industry.get("index_code")
            industry_name = industry.get("industry_name")
            if not index_code or not industry_name:
                continue
            try:
                members = self._query(
                    "index_member_all",
                    params={"l1_code": index_code, "is_new": "Y"},
                    fields="ts_code,l1_name",
                    use_cache=True,
                )
            except DataProviderError:
                continue
            for member in members:
                if member.get("ts_code"):
                    mapping[member["ts_code"]] = member.get("l1_name") or str(industry_name)
        self._industry_map = mapping
        return self._industry_map

    def _query(
        self,
        api_name: str,
        params: dict[str, str],
        fields: str,
        use_cache: bool = False,
    ) -> list[dict[str, object]]:
        cache_path = self.cache_dir / f"{api_name}-{stable_key(params, fields)}.json"
        if use_cache and cache_path.exists():
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            return payload

        self._apply_rate_limit()
        response = self.session.post(
            self.api_url,
            json={
                "api_name": api_name,
                "token": self.token,
                "params": params,
                "fields": fields,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            if "每分钟最多访问" in str(payload.get("msg") or ""):
                time.sleep(60.0)
                return self._query(api_name, params, fields, use_cache=use_cache)
            raise DataProviderError(f"Tushare {api_name} error: {payload.get('msg')}")
        data = payload.get("data") or {}
        field_names = data.get("fields") or []
        items = data.get("items") or []
        records = [dict(zip(field_names, item)) for item in items]
        if use_cache:
            cache_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        return records

    def _apply_rate_limit(self) -> None:
        now = time.monotonic()
        while self._request_timestamps and now - self._request_timestamps[0] >= 60.0:
            self._request_timestamps.popleft()
        if len(self._request_timestamps) >= self._max_requests_per_minute:
            sleep_seconds = 60.0 - (now - self._request_timestamps[0]) + 0.05
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            now = time.monotonic()
            while self._request_timestamps and now - self._request_timestamps[0] >= 60.0:
                self._request_timestamps.popleft()
        self._request_timestamps.append(time.monotonic())


class EastmoneyHistoryProvider:
    """Thin placeholder for the historical provider that will be wired to Eastmoney data."""

    def __init__(self, client: EastmoneyClient) -> None:
        self.client = client

    def list_trade_dates(self, start_date: date, end_date: date) -> list[date]:
        raise ProviderNotReadyError(
            "Eastmoney 历史数据 provider 还未接通。下一步需要补齐交易日历、日线、行业和公告日财报接口。"
        )

    def get_daily_snapshots(self, trade_date: date) -> list[DailySnapshot]:
        raise ProviderNotReadyError(
            "Eastmoney 历史日线截面 provider 还未实现。"
        )

    def get_price_history(
        self, code: str, end_date: date, lookback_days: int
    ) -> list[DailySnapshot]:
        raise ProviderNotReadyError(
            "Eastmoney 个股历史行情 provider 还未实现。"
        )

    def get_financial_reports(
        self, code: str, end_date: date, limit: int = 12
    ) -> list[QuarterlyReport]:
        raise ProviderNotReadyError(
            "Eastmoney 公告日财报 provider 还未实现。"
        )

    def warm_financial_cache(self, codes: list[str], end_date: date) -> int:
        raise ProviderNotReadyError(
            "Eastmoney 公告日财报 provider 还未实现。"
        )

    def warm_market_cache(self, start_date: date, end_date: date) -> int:
        raise ProviderNotReadyError(
            "Eastmoney 历史日线截面 provider 还未实现。"
        )


def build_quarterly_reports(
    code: str,
    income_rows: list[dict[str, object]],
    indicator_rows: list[dict[str, object]],
    disclosure_rows: list[dict[str, object]],
) -> list[QuarterlyReport]:
    disclosure_map: dict[str, date] = {}
    for row in disclosure_rows:
        period = row.get("end_date")
        if not period:
            continue
        actual_date = row.get("actual_date") or row.get("ann_date")
        if not actual_date:
            continue
        disclosure_map[str(period)] = date_from_tushare(str(actual_date))

    income_by_period: dict[str, dict[str, object]] = {}
    for row in income_rows:
        period = row.get("end_date")
        if not period:
            continue
        key = str(period)
        current_ann = row.get("f_ann_date") or row.get("ann_date")
        previous_ann = income_by_period.get(key, {}).get("f_ann_date") or income_by_period.get(key, {}).get("ann_date")
        if previous_ann is None or str(current_ann or "") >= str(previous_ann or ""):
            income_by_period[key] = row

    roe_by_period: dict[str, float | None] = {}
    for row in indicator_rows:
        period = row.get("end_date")
        if not period:
            continue
        key = str(period)
        roe_by_period[key] = to_float(row.get("roe_dt"))

    cumulative_income_by_period = {
        period: to_float(row.get("n_income"))
        for period, row in income_by_period.items()
    }

    reports: list[QuarterlyReport] = []
    for period, row in income_by_period.items():
        report_period = date_from_tushare(period)
        announce_date = disclosure_map.get(period)
        if announce_date is None:
            raw_ann = row.get("f_ann_date") or row.get("ann_date")
            if not raw_ann:
                continue
            announce_date = date_from_tushare(str(raw_ann))
        reports.append(
            QuarterlyReport(
                code=code,
                announce_date=announce_date,
                report_period=report_period,
                single_quarter_net_profit=single_quarter_profit(
                    report_period=report_period,
                    cumulative_profit=cumulative_income_by_period.get(period),
                    cumulative_income_by_period=cumulative_income_by_period,
                ),
                roe_ttm=roe_by_period.get(period),
            )
        )

    reports.sort(key=lambda item: item.report_period)
    return reports


def single_quarter_profit(
    report_period: date,
    cumulative_profit: float | None,
    cumulative_income_by_period: dict[str, float | None],
) -> float | None:
    if cumulative_profit is None:
        return None
    month = report_period.month
    if month == 3:
        return cumulative_profit
    previous_period = {
        6: date(report_period.year, 3, 31),
        9: date(report_period.year, 6, 30),
        12: date(report_period.year, 9, 30),
    }.get(month)
    if previous_period is None:
        return cumulative_profit
    previous = cumulative_income_by_period.get(to_tushare_date(previous_period))
    if previous is None:
        return None
    return cumulative_profit - previous


def split_ts_code(ts_code: str) -> tuple[str, str]:
    code, _, suffix = ts_code.partition(".")
    exchange = {
        "SH": "SSE",
        "SZ": "SZSE",
        "BJ": "BSE",
    }.get(suffix, suffix or "UNKNOWN")
    return code, exchange


def infer_board(code: str, exchange: str) -> str:
    if exchange == "SSE":
        if code.startswith(("688", "689")):
            return "star"
        return "main"
    if exchange == "SZSE":
        if code.startswith(("300", "301")):
            return "gem"
        return "main"
    return "other"


def to_ts_code(code: str, basics: dict[str, dict[str, str]]) -> str | None:
    if "." in code:
        return code
    for ts_code in basics:
        if ts_code.startswith(f"{code}."):
            return ts_code
    return None


def to_tushare_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def date_from_tushare(value: str) -> date:
    return date.fromisoformat(f"{value[:4]}-{value[4:6]}-{value[6:8]}")


def percent_to_ratio(value: object) -> float | None:
    number = to_float(value)
    if number is None:
        return None
    return number / 100.0


def scaled_amount_to_yuan(value: object) -> float | None:
    number = to_float(value)
    if number is None:
        return None
    return number * 1000.0


def wan_to_yuan(value: object) -> float | None:
    number = to_float(value)
    if number is None:
        return None
    return number * 10_000.0


def to_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def days_between(start_date: date, end_date: date) -> int:
    return max((end_date - start_date).days, 0)


def is_st_stock_name(name: str) -> bool:
    upper_name = name.upper()
    return "ST" in upper_name


def stable_key(params: dict[str, str], fields: str) -> str:
    raw = json.dumps({"params": params, "fields": fields}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
