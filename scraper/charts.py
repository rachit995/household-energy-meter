"""
Chart and table image generation for Telegram messages.
Uses matplotlib to render tables and charts as PNG images.
"""

import io
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
from datetime import date, datetime


# Dark theme colors
BG_COLOR = "#1a1a2e"
CARD_COLOR = "#16213e"
TEXT_COLOR = "#e0e0e0"
HEADER_COLOR = "#0f3460"
ACCENT_COLOR = "#e94560"
GREEN_COLOR = "#4ecca3"
GRID_COLOR = "#2a2a4a"
ROW_ALT = "#1b2840"


def _fig_to_bytes(fig):
    """Convert matplotlib figure to PNG bytes."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor(), edgecolor="none", pad_inches=0.3)
    plt.close(fig)
    buf.seek(0)
    return buf


def render_table_image(title, subtitle, headers, rows, col_alignments=None, highlights=None):
    """Render a data table as a styled PNG image.

    Args:
        title: Main title text
        subtitle: Description text below title
        headers: List of column header strings
        rows: List of lists (each row)
        col_alignments: List of 'left', 'right', 'center' per column
        highlights: Dict of {row_idx: color} for row highlighting
    Returns:
        BytesIO PNG image buffer
    """
    n_rows = len(rows)
    n_cols = len(headers)
    aligns = col_alignments or ["left"] * n_cols
    highlights = highlights or {}

    fig_height = 1.2 + 0.35 * n_rows + (0.4 if subtitle else 0)
    fig, ax = plt.subplots(figsize=(6, max(fig_height, 2.5)))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.axis("off")

    # Title and subtitle
    y_top = 0.95
    ax.text(0.5, y_top, title, transform=ax.transAxes, fontsize=14, fontweight="bold",
            color=TEXT_COLOR, ha="center", va="top", fontfamily="monospace")
    if subtitle:
        ax.text(0.5, y_top - 0.08, subtitle, transform=ax.transAxes, fontsize=9,
                color="#888888", ha="center", va="top", fontfamily="monospace")

    # Build table
    table = ax.table(cellText=rows, colLabels=headers, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.6)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(GRID_COLOR)
        cell.set_linewidth(0.5)
        cell.get_text().set_fontfamily("monospace")

        if row == 0:
            # Header row
            cell.set_facecolor(HEADER_COLOR)
            cell.get_text().set_color(TEXT_COLOR)
            cell.get_text().set_fontweight("bold")
        else:
            # Data rows
            if row in highlights:
                cell.set_facecolor(highlights[row])
            elif row % 2 == 0:
                cell.set_facecolor(ROW_ALT)
            else:
                cell.set_facecolor(CARD_COLOR)
            cell.get_text().set_color(TEXT_COLOR)

        # Alignment
        if col < len(aligns):
            ha = aligns[col]
            cell.get_text().set_ha(ha)
            if ha == "right":
                cell.PAD = 0.05
            elif ha == "left":
                cell.PAD = 0.05

    return _fig_to_bytes(fig)


def render_bar_chart(title, subtitle, labels, values, value_fmt="₹{:.0f}", color=GREEN_COLOR,
                     colors=None, bar_annotations=None, x_axis_fmt=None):
    """Render a horizontal bar chart as PNG image.

    Args:
        title: Main title
        subtitle: Description
        labels: List of bar labels
        values: List of numeric values
        value_fmt: Format string for value labels (ignored when bar_annotations set)
        color: Default bar color (used when ``colors`` is None)
        colors: Optional per-bar color list. When provided, overrides ``color``
                and disables the default max/min highlight (caller encodes
                meaning via the color list itself).
        bar_annotations: Optional per-bar annotation strings. Replaces the
                         ``value_fmt.format(val)`` label next to each bar.
        x_axis_fmt: Optional ``lambda x: str`` for x-axis tick formatting.
                    Defaults to ``₹{x:.0f}``.
    Returns:
        BytesIO PNG image buffer
    """
    n = len(labels)
    if n == 0 or not values:
        return None
    if colors is not None and len(colors) != n:
        raise ValueError(f"colors length {len(colors)} != labels length {n}")
    if bar_annotations is not None and len(bar_annotations) != n:
        raise ValueError(f"bar_annotations length {len(bar_annotations)} != labels length {n}")

    fig, ax = plt.subplots(figsize=(6, max(1.5 + 0.4 * n, 3)))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    float_vals = [float(v) for v in values]
    y_pos = range(n - 1, -1, -1)

    if colors is not None:
        bars = ax.barh(y_pos, float_vals, color=colors, height=0.6, edgecolor="none", alpha=0.85)
    else:
        bars = ax.barh(y_pos, float_vals, color=color, height=0.6, edgecolor="none", alpha=0.85)
        # Highlight max/min only when the caller didn't supply per-bar colors.
        max_idx = float_vals.index(max(float_vals))
        min_idx = float_vals.index(min(float_vals))
        bars[max_idx].set_color(ACCENT_COLOR)
        bars[min_idx].set_color("#3498db")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=10, color=TEXT_COLOR, fontfamily="monospace")
    ax.tick_params(axis="x", colors=TEXT_COLOR, labelsize=8)
    tick_fmt = x_axis_fmt if x_axis_fmt is not None else (lambda x: f"₹{x:.0f}")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: tick_fmt(x)))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.spines["left"].set_color(GRID_COLOR)
    ax.grid(axis="x", color=GRID_COLOR, alpha=0.3)

    # Value labels on bars
    for i, (bar, val) in enumerate(zip(bars, float_vals)):
        label = bar_annotations[i] if bar_annotations is not None else value_fmt.format(val)
        ax.text(bar.get_width() + max(float_vals) * 0.02, bar.get_y() + bar.get_height() / 2,
                label, va="center", ha="left", fontsize=9, color=TEXT_COLOR, fontfamily="monospace")

    # Title
    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR, fontfamily="monospace", y=0.98)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888", fontfamily="monospace", pad=10)

    ax.set_xlim(0, max(float_vals) * 1.2)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _fig_to_bytes(fig)


def render_spend_chart(title, subtitle, dates, spends, avg_line=None, highlight_date=None):
    """Render daily spend as a vertical bar chart with optional average line.

    Args:
        title: Main title
        subtitle: Description
        dates: List of date objects
        spends: List of spend amounts
        avg_line: Optional average value to draw as horizontal line
        highlight_date: Optional date to highlight (e.g. today) with a distinct color
    Returns:
        BytesIO PNG image buffer
    """
    n = len(dates)
    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    float_spends = [float(s) for s in spends]
    x = range(n)
    max_s, min_s = max(float_spends), min(float_spends)
    colors = []
    for d, s in zip(dates, float_spends):
        if highlight_date and d == highlight_date:
            colors.append("#f1c40f")  # gold for highlighted date
        elif s == max_s:
            colors.append(ACCENT_COLOR)
        elif s == min_s:
            colors.append("#3498db")
        else:
            colors.append(GREEN_COLOR)
    ax.bar(x, float_spends, color=colors, width=0.7, edgecolor="none", alpha=0.85)

    if avg_line:
        ax.axhline(y=float(avg_line), color=ACCENT_COLOR, linestyle="--", alpha=0.7, linewidth=1)
        ax.text(n - 0.5, float(avg_line), f"avg ₹{float(avg_line):.0f}", fontsize=8,
                color=ACCENT_COLOR, va="bottom", ha="right", fontfamily="monospace")

    labels = [d.strftime("%d") if isinstance(d, date) else str(d) for d in dates]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8, color=TEXT_COLOR, fontfamily="monospace", rotation=0)
    ax.tick_params(axis="y", colors=TEXT_COLOR, labelsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, p: f"₹{y:.0f}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.spines["left"].set_color(GRID_COLOR)
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.3)

    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR, fontfamily="monospace", y=0.98)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888", fontfamily="monospace", pad=8)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _fig_to_bytes(fig)


def render_donut_chart(title, subtitle, labels, values, colors=None):
    """Render a donut/pie chart as PNG image."""
    float_vals = [float(v) for v in values if v and float(v) > 0]
    valid_labels = [l for l, v in zip(labels, values) if v and float(v) > 0]
    if not float_vals:
        return None
    segment_colors = colors or [GREEN_COLOR, ACCENT_COLOR, "#888888", "#3498db"]
    valid_colors = segment_colors[:len(float_vals)]

    fig, ax = plt.subplots(figsize=(5, 4))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    total = sum(float_vals)
    wedges, texts, autotexts = ax.pie(
        float_vals, labels=None, autopct=lambda p: f"{p:.0f}%" if p > 3 else "",
        colors=valid_colors, wedgeprops=dict(width=0.4, edgecolor=BG_COLOR, linewidth=2),
        pctdistance=0.78, startangle=90,
    )
    for t in autotexts:
        t.set_color(TEXT_COLOR)
        t.set_fontsize(9)
        t.set_fontfamily("monospace")

    # Center text
    ax.text(0, 0, f"₹{total:,.0f}", ha="center", va="center", fontsize=16,
            fontweight="bold", color=TEXT_COLOR, fontfamily="monospace")

    # Legend
    legend_labels = [f"{l}: ₹{v:,.0f}" for l, v in zip(valid_labels, float_vals)]
    leg = ax.legend(wedges, legend_labels, loc="lower center", bbox_to_anchor=(0.5, -0.15),
                    ncol=min(len(valid_labels), 3), fontsize=9, frameon=False)
    for t in leg.get_texts():
        t.set_color(TEXT_COLOR)
        t.set_fontfamily("monospace")

    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR, fontfamily="monospace", y=0.97)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888", fontfamily="monospace", pad=12)

    fig.tight_layout(rect=[0, 0.05, 1, 0.92])
    return _fig_to_bytes(fig)


def render_line_chart(title, subtitle, dates, values, projection_dates=None,
                      projection_values=None, markers=None):
    """Render a line chart with optional projection and markers."""
    n = len(dates)
    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    float_vals = [float(v) if v is not None else None for v in values]

    ax.plot(range(n), float_vals, color=GREEN_COLOR, linewidth=2, marker="o",
            markersize=3, solid_capstyle="round", zorder=3)
    ax.fill_between(range(n), float_vals, alpha=0.1, color=GREEN_COLOR)

    all_labels = [d.strftime("%d") if isinstance(d, date) else str(d) for d in dates]
    if projection_dates and projection_values:
        proj_vals = [float(v) for v in projection_values]
        proj_x = range(n - 1, n - 1 + len(projection_dates))
        ax.plot(proj_x, proj_vals, color=ACCENT_COLOR, linewidth=1.5,
                linestyle="--", alpha=0.7, zorder=2)
        all_labels += [d.strftime("%d") if isinstance(d, date) else str(d) for d in projection_dates]

    all_vals = float_vals + ([float(v) for v in projection_values] if projection_values else [])
    if any(v is not None and v < 0 for v in all_vals):
        ax.axhline(y=0, color=ACCENT_COLOR, linewidth=0.8, alpha=0.5)

    if markers:
        for idx, label in markers.items():
            if idx < len(float_vals) and float_vals[idx] is not None:
                ax.annotate(label, (idx, float_vals[idx]), textcoords="offset points",
                            xytext=(0, 12), ha="center", fontsize=7, color=ACCENT_COLOR,
                            fontfamily="monospace", fontweight="bold")
                ax.plot(idx, float_vals[idx], marker="^", color=ACCENT_COLOR,
                        markersize=8, zorder=4)

    total_x = len(all_labels)
    step = max(1, total_x // 12)
    tick_positions = list(range(0, total_x, step))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([all_labels[i] for i in tick_positions], fontsize=8,
                       color=TEXT_COLOR, fontfamily="monospace")
    ax.tick_params(axis="y", colors=TEXT_COLOR, labelsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, p: f"₹{y:,.0f}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.spines["left"].set_color(GRID_COLOR)
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.3)

    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR, fontfamily="monospace", y=0.98)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888", fontfamily="monospace", pad=8)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _fig_to_bytes(fig)


def render_grouped_bars(title, subtitle, labels, group1_vals, group2_vals,
                        group1_label="This Week", group2_label="Last Week"):
    """Render grouped/side-by-side vertical bar chart."""
    import numpy as np
    n = len(labels)
    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    x = np.arange(n)
    width = 0.35
    g1 = [float(v) for v in group1_vals]
    g2 = [float(v) for v in group2_vals]

    bars1 = ax.bar(x - width / 2, g1, width, label=group1_label,
                   color=GREEN_COLOR, edgecolor="none", alpha=0.85)
    bars2 = ax.bar(x + width / 2, g2, width, label=group2_label,
                   color="#555577", edgecolor="none", alpha=0.6)

    for bar in bars1:
        if bar.get_height() > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"₹{bar.get_height():.0f}", ha="center", va="bottom",
                    fontsize=7, color=TEXT_COLOR, fontfamily="monospace")
    for bar in bars2:
        if bar.get_height() > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"₹{bar.get_height():.0f}", ha="center", va="bottom",
                    fontsize=7, color="#888888", fontfamily="monospace")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9, color=TEXT_COLOR, fontfamily="monospace")
    ax.tick_params(axis="y", colors=TEXT_COLOR, labelsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, p: f"₹{y:.0f}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.spines["left"].set_color(GRID_COLOR)
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.3)

    leg = ax.legend(fontsize=9, frameon=False, loc="upper right")
    for t in leg.get_texts():
        t.set_color(TEXT_COLOR)
        t.set_fontfamily("monospace")

    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR, fontfamily="monospace", y=0.98)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888", fontfamily="monospace", pad=8)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _fig_to_bytes(fig)


def render_time_profile_chart(title, subtitle, timestamps, values,
                              peak_ts=None, low_ts=None):
    """Render a power-vs-time-of-day line chart for the evening report.

    Unlike `render_line_chart` (which is hardcoded for rupee-denominated
    daily-spend charts with day-of-month X-ticks), this helper plots a
    continuous time axis with hour labels and a kW Y-axis.

    Args:
        title: Main title (e.g., "Today's Power Profile").
        subtitle: Short stats line (e.g., "Peak 2.8 kW · Low 0.2 kW · Avg 0.9 kW").
        timestamps: list of timezone-aware datetimes, sorted ascending.
        values: list of floats (kW), same length as `timestamps`. Callers
                should filter out NULL/None entries BEFORE calling — NULL
                means "unknown power", which would otherwise plot as a
                misleading dip to zero.
        peak_ts: optional datetime matching one of the timestamps — rendered
                 as a red up-triangle marker with a "Peak" label.
        low_ts: optional datetime — rendered as a green down-triangle with
                a "Low" label.

    Returns:
        BytesIO PNG buffer, ready for send_telegram_photo().
    """
    # Defensive: empty series would produce an empty chart. Callers should
    # check len() before invoking, but this keeps the function itself safe.
    if not timestamps or not values:
        fig, ax = plt.subplots(figsize=(7, 3.5))
        fig.set_facecolor(BG_COLOR)
        ax.set_facecolor(BG_COLOR)
        ax.text(0.5, 0.5, "No data yet", ha="center", va="center",
                color=TEXT_COLOR, fontsize=12, fontfamily="monospace",
                transform=ax.transAxes)
        ax.set_xticks([])
        ax.set_yticks([])
        fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR,
                     fontfamily="monospace", y=0.98)
        return _fig_to_bytes(fig)

    float_vals = [float(v) for v in values]

    fig, ax = plt.subplots(figsize=(7, 3.5))
    fig.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    # Plot the power curve. matplotlib handles datetime X coords natively.
    ax.plot(timestamps, float_vals, color=GREEN_COLOR, linewidth=2, marker="o",
            markersize=3, solid_capstyle="round", zorder=3)
    ax.fill_between(timestamps, float_vals, alpha=0.1, color=GREEN_COLOR)

    # Peak marker — red up-triangle, annotated above the point.
    if peak_ts is not None:
        try:
            idx = timestamps.index(peak_ts)
            ax.plot(peak_ts, float_vals[idx], marker="^", color=ACCENT_COLOR,
                    markersize=9, zorder=4)
            ax.annotate("Peak", (peak_ts, float_vals[idx]),
                        textcoords="offset points", xytext=(0, 12),
                        ha="center", fontsize=7, color=ACCENT_COLOR,
                        fontfamily="monospace", fontweight="bold")
        except ValueError:
            # peak_ts not in the series — silently skip rather than crash.
            pass

    # Low marker — green down-triangle, annotated below the point.
    if low_ts is not None:
        try:
            idx = timestamps.index(low_ts)
            ax.plot(low_ts, float_vals[idx], marker="v", color=GREEN_COLOR,
                    markersize=9, zorder=4)
            ax.annotate("Low", (low_ts, float_vals[idx]),
                        textcoords="offset points", xytext=(0, -16),
                        ha="center", fontsize=7, color=GREEN_COLOR,
                        fontfamily="monospace", fontweight="bold")
        except ValueError:
            pass

    # X-axis: hours across the day. AutoDateLocator picks sensible tick
    # spacing based on the total span (every 2-3 hours for a 24h chart).
    # DateFormatter produces "HH:MM" labels.
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=5, maxticks=12))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    # Y-axis: kW formatting with one decimal place.
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.1f} kW"))

    # Styling — match the rest of the chart family.
    ax.tick_params(axis="x", colors=TEXT_COLOR, labelsize=8)
    ax.tick_params(axis="y", colors=TEXT_COLOR, labelsize=8)
    for label in ax.get_xticklabels():
        label.set_fontfamily("monospace")
    for label in ax.get_yticklabels():
        label.set_fontfamily("monospace")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.spines["left"].set_color(GRID_COLOR)
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.3)

    # Add padding to Y-axis so the curve doesn't hug the frame.
    y_min = min(float_vals)
    y_max = max(float_vals)
    pad = max(0.1, (y_max - y_min) * 0.1)
    ax.set_ylim(max(0, y_min - pad), y_max + pad)

    fig.suptitle(title, fontsize=13, fontweight="bold", color=TEXT_COLOR,
                 fontfamily="monospace", y=0.98)
    if subtitle:
        ax.set_title(subtitle, fontsize=9, color="#888888",
                     fontfamily="monospace", pad=8)

    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _fig_to_bytes(fig)
