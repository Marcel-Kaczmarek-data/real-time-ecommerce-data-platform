"""Visual system for the dashboard (dark mode).

Colours are the validated reference palette's DARK steps (CVD-checked as a set):
categorical slots are assigned in fixed order by entity and never cycled;
status colours are reserved for state and always paired with a label.
"""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

SURFACE = "#1a1a19"
PAGE = "#0d0d0d"
INK = "#f0efec"
INK_2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
AXIS = "#383835"
BORDER = "rgba(255,255,255,0.10)"

SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c", "#9085e9", "#e66767"]
SEQUENTIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
GOOD, WARNING, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"
DELTA_UP = "#0ca30c"

# Stable colour per entity (colour follows the entity, never its rank).
CHANNEL_COLORS = {
    "web": SERIES[0],
    "mobile_app": SERIES[1],
    "marketplace": SERIES[2],
    "social_commerce": SERIES[3],
}
DEVICE_COLORS = {"mobile": SERIES[0], "desktop": SERIES[1], "tablet": SERIES[2]}
REGION_COLORS = {
    "North America": SERIES[0],
    "Europe": SERIES[1],
    "Asia Pacific": SERIES[2],
    "Latin America": SERIES[3],
    "Middle East & Africa": SERIES[4],
}

FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def register_plotly_template() -> None:
    template = go.layout.Template()
    template.layout = go.Layout(
        font={"family": FONT, "color": INK_2, "size": 12},
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        colorway=SERIES,
        margin={"l": 8, "r": 8, "t": 40, "b": 8},
        title={"font": {"size": 14, "color": INK}, "x": 0.0, "xanchor": "left"},
        xaxis={
            "gridcolor": GRID,
            "linecolor": AXIS,
            "zeroline": False,
            "tickfont": {"color": MUTED},
            "showgrid": False,
        },
        yaxis={
            "gridcolor": GRID,
            "linecolor": AXIS,
            "zeroline": False,
            "tickfont": {"color": MUTED},
            "griddash": "dot",
        },
        # Legend below the plot: never collides with the left-aligned title.
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.12,
            "x": 0,
            "font": {"color": INK_2},
            "bgcolor": "rgba(0,0,0,0)",
        },
        hoverlabel={"bgcolor": "#262624", "bordercolor": AXIS, "font": {"color": INK, "family": FONT}},
        hovermode="x unified",
    )
    template.data.scatter = [go.Scatter(line={"width": 2})]
    template.data.bar = [go.Bar(marker={"line": {"width": 0}})]
    pio.templates["ecom_dark"] = template
    pio.templates.default = "ecom_dark"


CSS = f"""
<style>
  .block-container {{ padding-top: 3.2rem; padding-bottom: 2rem; max-width: 1500px; }}
  [data-testid="stSidebar"] {{ background: {SURFACE}; border-right: 1px solid {BORDER}; }}
  h1, h2, h3 {{ font-family: {FONT}; letter-spacing: -0.01em; }}
  .page-title {{ font-size: 1.7rem; line-height: 1.2; font-weight: 650; color: {INK}; margin: 0; }}
  .page-sub {{ color: {MUTED}; font-size: 0.9rem; margin: 0.25rem 0 1rem 0; }}
  td {{ vertical-align: middle; }}
  .nowrap {{ white-space: nowrap; }}
  .kpi-card {{
      background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 14px;
      padding: 14px 16px 12px 16px; height: 100%;
  }}
  .kpi-label {{ color: {MUTED}; font-size: 0.78rem; text-transform: uppercase; letter-spacing: .04em; }}
  .kpi-value {{ color: {INK}; font-size: 1.55rem; font-weight: 650; margin-top: 4px; font-variant-numeric: tabular-nums; }}
  .kpi-delta {{ font-size: 0.8rem; margin-top: 2px; font-variant-numeric: tabular-nums; }}
  .kpi-delta.up {{ color: {DELTA_UP}; }}
  .kpi-delta.down {{ color: {CRITICAL}; }}
  .kpi-delta.flat {{ color: {MUTED}; }}
  .kpi-hint {{ color: {MUTED}; font-size: 0.72rem; }}
  .section {{ color: {INK}; font-weight: 600; font-size: 1.02rem; margin: 0.6rem 0 0.2rem 0; }}
  .pill {{ display: inline-block; padding: 2px 9px; border-radius: 999px; font-size: 0.74rem;
          border: 1px solid {BORDER}; color: {INK_2}; margin-right: 6px; }}
  .status-good {{ color: {GOOD}; }} .status-warning {{ color: {WARNING}; }}
  .status-serious {{ color: {SERIOUS}; }} .status-critical {{ color: {CRITICAL}; }}
  .profile-card {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 14px; padding: 16px; }}
  .profile-name {{ font-size: 1.25rem; font-weight: 650; color: {INK}; }}
  .muted {{ color: {MUTED}; }}
  div[data-testid="stDataFrame"] {{ border: 1px solid {BORDER}; border-radius: 10px; }}
</style>
"""
