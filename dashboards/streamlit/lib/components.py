"""Reusable UI pieces: page header, KPI cards, sections, charts, export."""

from __future__ import annotations

import html
from collections.abc import Sequence

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib import theme


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="page-title">{html.escape(title)}</div><div class="page-sub">{html.escape(subtitle)}</div>',
        unsafe_allow_html=True,
    )


def section(title: str) -> None:
    st.markdown(f'<p class="section">{html.escape(title)}</p>', unsafe_allow_html=True)


def fmt_money(v: float | None, decimals: int = 0) -> str:
    if v is None or pd.isna(v):
        return "–"
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"${v / div:,.{max(decimals, 1) if div > 1e3 else decimals or 1}f}{suffix}"
    return f"${v:,.{decimals}f}"


def fmt_int(v: float | None) -> str:
    return "–" if v is None or pd.isna(v) else f"{v:,.0f}"


def fmt_pct(v: float | None, decimals: int = 1) -> str:
    return "–" if v is None or pd.isna(v) else f"{v * 100:.{decimals}f}%"


def kpi_card(
    label: str,
    value: str,
    delta: float | None = None,
    hint: str = "",
    good_when_up: bool = True,
    delta_is_points: bool = False,
) -> str:
    if delta is None or pd.isna(delta):
        delta_html = ""
    else:
        positive = delta > 0
        cls = "flat" if abs(delta) < 1e-9 else ("up" if positive == good_when_up else "down")
        arrow = "▲" if positive else ("▼" if delta < 0 else "■")
        text = f"{delta * 100:+.1f} pts" if delta_is_points else f"{delta * 100:+.1f}%"
        delta_html = f'<div class="kpi-delta {cls}">{arrow} {text} vs previous period</div>'
    hint_html = f'<div class="kpi-hint">{html.escape(hint)}</div>' if hint else ""
    return (
        f'<div class="kpi-card"><div class="kpi-label">{html.escape(label)}</div>'
        f'<div class="kpi-value">{html.escape(value)}</div>{delta_html}{hint_html}</div>'
    )


def kpi_row(cards: Sequence[str]) -> None:
    cols = st.columns(len(cards))
    for col, card in zip(cols, cards, strict=True):
        col.markdown(card, unsafe_allow_html=True)


def chart(fig: go.Figure, height: int = 320, key: str | None = None) -> None:
    fig.update_layout(height=height)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)


def line_chart(
    df: pd.DataFrame,
    x: str,
    ys: dict[str, str],
    title: str,
    colors: dict[str, str] | None = None,
    y_format: str = "$,.0f",
    fill_first: bool = False,
) -> go.Figure:
    """Lines on ONE shared axis (never a dual axis); ``ys`` maps column -> legend label."""
    prefix = "$" if y_format.startswith("$") else ""
    fmt = y_format.lstrip("$")
    fig = go.Figure()
    for i, (col, label) in enumerate(ys.items()):
        color = (colors or {}).get(col, theme.SERIES[i % len(theme.SERIES)])
        first_fill = fill_first and i == 0
        fig.add_trace(
            go.Scatter(
                x=df[x],
                y=df[col],
                name=label,
                mode="lines",
                line={"color": color, "width": 2},
                fill="tozeroy" if first_fill else None,
                fillcolor="rgba(57,135,229,0.12)" if first_fill else None,
                hovertemplate=f"{label}: {prefix}%{{y:{fmt}}}<extra></extra>",
            )
        )
    fig.update_layout(title=title, showlegend=len(ys) > 1)
    fig.update_yaxes(tickformat=fmt, tickprefix=prefix)
    if pd.api.types.is_datetime64_any_dtype(df[x]) or (len(df) and hasattr(df[x].iloc[0], "isoformat")):
        fig.update_xaxes(tickformat="%d %b")  # no second "year" tick line colliding with the legend
    return fig


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    horizontal: bool = False,
    value_format: str = "$,.0f",
    colors: list[str] | None = None,
) -> go.Figure:
    prefix = "$" if value_format.startswith("$") else ""
    fmt = value_format.lstrip("$")
    marker = {"color": colors or color or theme.SERIES[0], "cornerradius": 4}
    if horizontal:
        trace = go.Bar(
            x=df[y],
            y=df[x],
            orientation="h",
            marker=marker,
            hovertemplate=f"%{{y}}: {prefix}%{{x:{fmt}}}<extra></extra>",
        )
    else:
        trace = go.Bar(
            x=df[x], y=df[y], marker=marker, hovertemplate=f"%{{x}}: {prefix}%{{y:{fmt}}}<extra></extra>"
        )
    fig = go.Figure(trace)
    fig.update_layout(title=title, showlegend=False, hovermode="closest", bargap=0.35)
    if horizontal:
        fig.update_yaxes(autorange="reversed")
        fig.update_xaxes(
            tickformat=fmt, tickprefix=prefix, showgrid=True, gridcolor=theme.GRID, griddash="dot"
        )
    else:
        fig.update_yaxes(tickformat=fmt, tickprefix=prefix)
    return fig


def export_button(df: pd.DataFrame, name: str) -> None:
    st.download_button(
        "⬇ Export CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{name}.csv",
        mime="text/csv",
        key=f"export-{name}",
    )


def empty_state(message: str = "No data for the selected filters.") -> None:
    st.markdown(f'<div class="kpi-card muted">{html.escape(message)}</div>', unsafe_allow_html=True)


def status_badge(status: str) -> str:
    """Status = colour + icon + label (never colour alone)."""
    mapping = {
        "PASS": ("good", "✔"),
        "FAIL": ("critical", "✖"),
        "ERROR": ("serious", "⚠"),
        "Healthy": ("good", "✔"),
        "Low stock": ("warning", "▲"),
        "Stock-out": ("critical", "✖"),
        "Overstock": ("serious", "●"),
    }
    cls, icon = mapping.get(status, ("warning", "●"))
    return f'<span class="status-{cls}">{icon} {html.escape(status)}</span>'
