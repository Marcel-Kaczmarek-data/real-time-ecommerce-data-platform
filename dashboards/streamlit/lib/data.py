"""Data access for the dashboard: one shared GoldWarehouse, cached queries, global filters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from serving.warehouse import GoldWarehouse, TableNotAvailableError

CACHE_TTL_SECONDS = 60
ALL_CHANNELS = ["web", "mobile_app", "marketplace", "social_commerce"]

ISO3 = {
    "US": "USA",
    "CA": "CAN",
    "MX": "MEX",
    "BR": "BRA",
    "GB": "GBR",
    "IE": "IRL",
    "FR": "FRA",
    "DE": "DEU",
    "ES": "ESP",
    "IT": "ITA",
    "NL": "NLD",
    "BE": "BEL",
    "AT": "AUT",
    "PT": "PRT",
    "SE": "SWE",
    "DK": "DNK",
    "NO": "NOR",
    "FI": "FIN",
    "PL": "POL",
    "CZ": "CZE",
    "CH": "CHE",
    "JP": "JPN",
    "KR": "KOR",
    "IN": "IND",
    "SG": "SGP",
    "AU": "AUS",
    "NZ": "NZL",
    "ZA": "ZAF",
    "AE": "ARE",
    "TR": "TUR",
}


@st.cache_resource(show_spinner=False)
def warehouse() -> GoldWarehouse:
    return GoldWarehouse()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    return warehouse().query(sql, list(params))


def safe_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame | None:
    """Query with a friendly error state instead of a stack trace."""
    try:
        return query(sql, params)
    except TableNotAvailableError as exc:
        st.info(f"Data not available yet: {exc}. Run `make transform` (or wait for the Airflow DAGs).")
    except Exception as exc:  # surfaced, not swallowed
        st.error(f"Query failed: {type(exc).__name__}: {exc}")
    return None


@dataclass(frozen=True)
class Filters:
    start: date
    end: date
    countries: tuple[str, ...]
    channels: tuple[str, ...]
    categories: tuple[str, ...]

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    @property
    def prev_start(self) -> date:
        return self.start - timedelta(days=self.days)

    @property
    def prev_end(self) -> date:
        return self.start - timedelta(days=1)

    def in_list(self, column: str, values: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
        """SQL fragment + params for an optional IN filter (empty selection = all)."""
        if not values:
            return "TRUE", ()
        return f"{column} IN ({', '.join('?' for _ in values)})", tuple(values)

    def where_sales(
        self,
        date_col: str = "date",
        start: date | None = None,
        end: date | None = None,
        country_col: str | None = "country_code",
        channel_col: str | None = "sales_channel",
    ) -> tuple[str, tuple[Any, ...]]:
        parts = [f"{date_col} BETWEEN ? AND ?"]
        params: list[Any] = [start or self.start, end or self.end]
        if country_col:
            frag, p = self.in_list(country_col, self.countries)
            parts.append(frag)
            params.extend(p)
        if channel_col:
            frag, p = self.in_list(channel_col, self.channels)
            parts.append(frag)
            params.extend(p)
        return " AND ".join(parts), tuple(params)


def data_bounds() -> tuple[date, date]:
    df = safe_query("SELECT min(date) AS lo, max(date) AS hi FROM mart_daily_sales")
    if df is None or df.empty or pd.isna(df.loc[0, "lo"]):
        today = date.today()
        return today - timedelta(days=30), today
    return pd.Timestamp(df.loc[0, "lo"]).date(), pd.Timestamp(df.loc[0, "hi"]).date()


def options(sql: str) -> list[str]:
    df = safe_query(sql)
    return [] if df is None else [str(v) for v in df.iloc[:, 0].dropna().tolist()]


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0) or pd.isna(previous) or pd.isna(current):
        return None
    return current / previous - 1
