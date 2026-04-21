#!/usr/bin/env python3
"""
Energy Meter Scraper — SmartGridSoft mobile API backend.

Data acquisition uses the vendor's undocumented JSON API at
``http://103.105.155.227:86/WebServicesMeterData.svc/`` (no auth; bootstrap
IDs via ``scripts/bootstrap_ids.py``). Storage, alerts, charts, and Telegram
formatting are unchanged — the adapter in ``scraper/normalizer.py`` reproduces
the exact contract the HTML scraper used to return.
"""

import argparse
import math
import os
import sys
import logging
import time
from dotenv import load_dotenv
import requests
from storage import (save_daily, save_daily_costs, save_monthly, save_recharge, save_rates, extract_recharges,
                      save_portal_recharges, load_portal_recharges, detect_new_recharges, merge_portal_recharges_to_history,
                      cleanup_duplicate_recharges, load_daily_readings, load_historical_months,
                      claim_portal_recharge_notification, last_recharge_date,
                      # Phase 2: high-frequency readings + edge-triggered alerts
                      save_reading, load_readings, load_previous_reading,
                      get_alert_state, set_alert_state, clear_alert_state)
from charts import (render_table_image, render_bar_chart, render_spend_chart,
                     render_donut_chart, render_line_chart, render_grouped_bars,
                     render_time_profile_chart)
from api_client import SmartGridClient
from normalizer import normalize
from appliances import (
    HIGH_POWER_KW_THRESHOLD,
    SUSTAINED_LOAD_KW,
    NIGHT_ANOMALY_KW,
    BASELINE_KW,
    MAJOR_LOAD_FLOOR_KW,
    ALL_APPLIANCES,
)

load_dotenv()
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import re
import sentry_sdk

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Sentry — private error monitoring (public GHA logs can't show tracebacks)
# ---------------------------------------------------------------------------
_SECRET_PATTERNS = re.compile(
    r"postgres(?:ql)?://[^\s'\"]+|"        # DATABASE_URL (postgresql:// or postgres://)
    r"bot\d+:[A-Za-z0-9_-]+|"             # Telegram bot token
    r"password=[^\s&'\"]+|"                # libpq-style password=...
    r"Bearer\s+[A-Za-z0-9._-]+|"          # Authorization bearer tokens
    r"token=[A-Za-z0-9._-]+|"             # generic token= params
    r"api[_-]?key=[A-Za-z0-9._-]+"        # generic api_key= / apikey= params
)


def _scrub_strings(obj):
    """Recursively scrub secret patterns from every string in a nested
    dict/list structure. Covers exception values, breadcrumbs, extra,
    contexts, and frame local variables."""
    if isinstance(obj, str):
        return _SECRET_PATTERNS.sub("<redacted>", obj)
    if isinstance(obj, dict):
        return {k: _scrub_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub_strings(v) for v in obj]
    return obj


def _scrub_event(event, hint):
    """Walk the entire Sentry event payload and strip secrets."""
    return _scrub_strings(event)


sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),          # empty/unset → SDK disables itself
    environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
    release=os.getenv("GITHUB_SHA"),       # auto-set by GHA; tags errors by commit
    before_send=_scrub_event,
    traces_sample_rate=0,                  # no tracing — short-lived script
)


# Configure logging.
#
# Default level is INFO which intentionally SUPPRESSES financial and
# real-time-power values (balance, deductions, power draw, rate card).
# Those are emitted via `logger.debug()` so they don't appear in public
# GitHub Actions logs. Set LOG_LEVEL=DEBUG locally when you need full
# detail for troubleshooting.
_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
CONFIG = {
    "SITE_ID": os.getenv("SMARTGRID_SITE_ID", ""),
    "UNIT_ID": os.getenv("SMARTGRID_UNIT_ID", ""),
    "METER_ID": os.getenv("SMARTGRID_METER_ID", ""),
    "MONTHLY_BUDGET": Decimal(os.getenv("MONTHLY_BUDGET", "8000")),
}


def parse_decimal(value):
    """Parse string to Decimal, handling empty values.

    Kept for compatibility with a few remaining non-portal call sites; the
    API-path normalizer has its own ``_parse_decimal`` with the same
    semantics (module-local copy keeps ``normalizer.py`` importable without
    a circular dependency through ``scraper.py``).
    """
    if not value or value.strip() in ["", "-"]:
        return None
    try:
        cleaned = value.replace(",", "").strip()
        return Decimal(cleaned)
    except (ValueError, InvalidOperation):
        return None


def create_client():
    """Build a SmartGridClient from SMARTGRID_SITE_ID / UNIT_ID / METER_ID."""
    return SmartGridClient(
        site_id=CONFIG["SITE_ID"],
        unit_id=CONFIG["UNIT_ID"],
        meter_id=CONFIG["METER_ID"],
    )


def _fetch_historical_months(now=None, count=4):
    """Pad the 2 API-sourced monthly_consumption entries (prev + prev-prev)
    up to 6 months using our own ``monthly_summaries`` table.
    Returns a list safe to hand directly to ``normalize(..., historical_months=...)``."""
    anchor = (now or datetime.now(IST)).date()
    # prev-prev month in YYYY-MM (API covers everything at-or-after this).
    total_month = anchor.year * 12 + (anchor.month - 1) - 2
    year, month0 = divmod(total_month, 12)
    threshold = f"{year:04d}-{month0 + 1:02d}"
    try:
        return load_historical_months(threshold, count=count)
    except Exception as exc:
        logger.warning(f"Could not load historical months from DB: {exc}")
        return []


def scrape_meter_data(client):
    """Fetch the full meter snapshot via the SmartGridSoft JSON API and adapt
    it to the 13-tuple contract the rest of the scraper consumes.

    Optional endpoints that fail are silently None (logged by the client);
    critical endpoints raise and propagate out of ``main()``.
    """
    logger.info("Fetching meter data via API...")
    responses = client.fetch_all()
    historical = _fetch_historical_months()
    result = normalize(responses, historical_months=historical)
    logger.info(
        f"Daily readings: {len(result[6])} days; "
        f"Portal recharges: {len(result[11])} entries"
    )
    return result


def _build_daily_spends(daily_readings, start_date, end_date):
    """Build daily spend list from balance drops, filtering out recharge days.

    Spend on day X = balance[X] - balance[X+1] (opening balance drop).
    We need data for one day AFTER end_date to compute the last day's spend.
    """
    day_after = end_date + timedelta(days=1)
    relevant = [r for r in daily_readings if start_date <= r["date"] <= day_after]

    daily_spends = []
    for i in range(len(relevant) - 1):
        curr = relevant[i]
        nxt = relevant[i + 1]
        if curr["balance"] is not None and nxt["balance"] is not None and curr["date"] <= end_date:
            spend = curr["balance"] - nxt["balance"]
            dg = curr["dg_consume"] or Decimal("0")
            daily_spends.append({
                "date": curr["date"],
                "spend": spend,
                "dg_consume": dg,
                "is_recharge": spend < 0,
            })

    # Separate consumption days from recharge days
    consumption_days = [d for d in daily_spends if not d["is_recharge"]]
    return daily_spends, consumption_days


def compute_weekly_stats(daily_readings, week_start, week_end):
    """Compute stats for a given week (Mon-Sun) from daily readings"""
    daily_spends, consumption_days = _build_daily_spends(daily_readings, week_start, week_end)

    if not consumption_days:
        return None

    total = sum(d["spend"] for d in consumption_days)
    avg = total / len(consumption_days)
    highest = max(consumption_days, key=lambda d: d["spend"])
    lowest = min(consumption_days, key=lambda d: d["spend"])

    # DG days: check raw readings to catch all days in the week
    week_raw = [r for r in daily_readings if week_start <= r["date"] <= week_end]
    dg_days = sum(1 for r in week_raw if r["dg_consume"] and r["dg_consume"] > 0)

    # Balance trend: first and last reading in the week
    week_readings = [r for r in daily_readings if week_start <= r["date"] <= week_end and r["balance"] is not None]
    bal_start = week_readings[0]["balance"] if week_readings else None
    bal_end = week_readings[-1]["balance"] if week_readings else None

    return {
        "week_start": week_start,
        "week_end": week_end,
        "total": total,
        "avg": avg,
        "highest": highest,
        "lowest": lowest,
        "dg_days": dg_days,
        "days_count": len(daily_spends),
        "bal_start": bal_start,
        "bal_end": bal_end,
        "projected_monthly": avg * 30,
    }


def build_weekly_message(stats, prev_stats, balance, duration, last_sync=None, daily_readings=None):
    """Build the weekly Telegram message"""
    ws = stats["week_start"].strftime("%d %b")
    we = stats["week_end"].strftime("%d %b %Y")

    msg = (
        "📊 <b>Energy Monitor — Weekly Report</b>\n"
        f"📅 {ws} – {we}\n"
        f"🔄 Last Sync: {last_sync or '—'}\n\n"
        f"💰 Week Total: <b>₹{stats['total']:.0f}</b>\n"
        f"📈 Avg Daily: ₹{stats['avg']:.0f}\n\n"
        f"🔺 Highest: {stats['highest']['date'].strftime('%d %b')} — ₹{stats['highest']['spend']:.0f}\n"
        f"🔻 Lowest: {stats['lowest']['date'].strftime('%d %b')} — ₹{stats['lowest']['spend']:.0f}\n\n"
    )

    if stats["bal_start"] is not None and stats["bal_end"] is not None:
        drop = stats["bal_start"] - stats["bal_end"]
        msg += f"📉 Balance: ₹{stats['bal_start']:.0f} → ₹{stats['bal_end']:.0f} (−₹{drop:.0f})\n"

    if prev_stats and prev_stats["total"]:
        change = ((stats["total"] - prev_stats["total"]) / prev_stats["total"]) * 100
        arrow = "↑" if change > 0 else "↓"
        msg += f"📊 vs Last Week: {arrow}{abs(change):.0f}% (₹{prev_stats['total']:.0f} → ₹{stats['total']:.0f})\n"

    msg += (
        f"\n⚡ DG Usage: {stats['dg_days']} of {stats['days_count']} days\n"
        f"🗓️ Projected Monthly: ₹{stats['projected_monthly']:.0f}\n"
    )

    # Balance forecast (Feature 8)
    if daily_readings and balance:
        msg += _build_balance_forecast(balance, daily_readings)

    msg += f"\nDuration: {duration:.1f}s"
    return msg


def compute_monthly_stats(daily_readings, year, month):
    """Compute stats for a given month from daily readings"""
    import calendar

    days_in_month = calendar.monthrange(year, month)[1]
    month_start = date(year, month, 1)
    month_end = date(year, month, days_in_month)

    daily_spends, consumption_days = _build_daily_spends(daily_readings, month_start, month_end)

    if not consumption_days:
        return None

    total = sum(d["spend"] for d in consumption_days)
    avg = total / len(consumption_days)
    highest = max(consumption_days, key=lambda d: d["spend"])
    lowest = min(consumption_days, key=lambda d: d["spend"])

    # DG days: check raw readings (not just spend-computed days) to catch first day too
    month_readings = [r for r in daily_readings if month_start <= r["date"] <= month_end]
    dg_days = sum(1 for r in month_readings if r["dg_consume"] and r["dg_consume"] > 0)

    # Weekday vs weekend (consumption days only)
    weekday_spends = [d["spend"] for d in consumption_days if d["date"].weekday() < 5]
    weekend_spends = [d["spend"] for d in consumption_days if d["date"].weekday() >= 5]
    weekday_avg = sum(weekday_spends) / len(weekday_spends) if weekday_spends else Decimal("0")
    weekend_avg = sum(weekend_spends) / len(weekend_spends) if weekend_spends else Decimal("0")

    # First half vs second half (consumption days only)
    first_half = [d["spend"] for d in consumption_days if d["date"].day <= 15]
    second_half = [d["spend"] for d in consumption_days if d["date"].day > 15]
    first_half_avg = sum(first_half) / len(first_half) if first_half else Decimal("0")
    second_half_avg = sum(second_half) / len(second_half) if second_half else Decimal("0")

    # Most expensive week (consumption days only)
    week_totals = {}
    for d in consumption_days:
        week_num = (d["date"].day - 1) // 7 + 1
        week_totals[week_num] = week_totals.get(week_num, Decimal("0")) + d["spend"]
    most_expensive_week = max(week_totals.items(), key=lambda x: x[1]) if week_totals else (0, Decimal("0"))

    # Recharges in this month
    recharge_days = [d for d in daily_spends if d["is_recharge"]]
    recharge_count = len(recharge_days)
    recharge_total = sum(abs(d["spend"]) for d in recharge_days)

    # Balance trend (from all readings, including recharge days)
    relevant = [r for r in daily_readings if month_start <= r["date"] <= month_end and r["balance"] is not None]
    bal_start = relevant[0]["balance"] if relevant else None
    bal_end = relevant[-1]["balance"] if relevant else None

    return {
        "year": year,
        "month": month,
        "total": total,
        "avg": avg,
        "highest": highest,
        "lowest": lowest,
        "dg_days": dg_days,
        "days_count": len(month_readings),
        "weekday_avg": weekday_avg,
        "weekend_avg": weekend_avg,
        "first_half_avg": first_half_avg,
        "second_half_avg": second_half_avg,
        "most_expensive_week": most_expensive_week,
        "bal_start": bal_start,
        "bal_end": bal_end,
        "recharge_count": recharge_count,
        "recharge_total": recharge_total,
        "dg_total_cost": sum(d["spend"] for d in daily_spends if d.get("dg_consume") and d["dg_consume"] > 0 and not d["is_recharge"]),
        "weekday_pattern": _weekday_pattern(consumption_days),
    }


def _weekday_pattern(consumption_days):
    """Compute average spend per day of week."""
    from collections import defaultdict
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    by_day = defaultdict(list)
    for d in consumption_days:
        by_day[d["date"].weekday()].append(d["spend"])
    pattern = {}
    for i, name in enumerate(day_names):
        spends = by_day.get(i, [])
        if spends:
            pattern[name] = sum(spends) / len(spends)
    return pattern


def build_monthly_message(stats, prev_month, prev_prev_month, monthly_consumption, duration, last_sync=None, rate_card=None, balance=None, daily_readings=None):
    """Build the monthly Telegram message"""
    import calendar
    month_name = calendar.month_name[stats["month"]]

    pm = prev_month
    rc = rate_card or {}
    msg = (
        f"📆 <b>Energy Monitor — Monthly Report</b>\n"
        f"📅 {month_name} {stats['year']}\n"
        f"🔄 Last Sync: {last_sync or '—'}\n\n"
        f"💰 Total Spent: <b>₹{stats['total']:.0f}</b>\n"
        f"📈 Avg Daily: ₹{stats['avg']:.0f}\n"
        f"  EB: ₹{pm.get('eb') or '—'} | DG: ₹{pm.get('dg') or '—'}{_dg_premium_str(pm.get('dg'), rc)} | Fix: ₹{pm.get('fix_charge') or '—'}\n\n"
        f"🔺 Highest: {stats['highest']['date'].strftime('%d %b')} — ₹{stats['highest']['spend']:.0f}\n"
        f"🔻 Lowest: {stats['lowest']['date'].strftime('%d %b')} — ₹{stats['lowest']['spend']:.0f}\n\n"
    )

    # Month-over-month comparison
    ppm_total = prev_prev_month.get("total") if prev_prev_month else None
    if ppm_total and stats["total"]:
        change = ((pm.get("total", Decimal("0")) - ppm_total) / ppm_total) * 100
        arrow = "↑" if change > 0 else "↓"
        msg += f"📊 vs Previous Month: {arrow}{abs(change):.0f}% (₹{ppm_total} → ₹{pm.get('total') or '—'})\n\n"

    msg += (
        f"📅 Weekday avg: ₹{stats['weekday_avg']:.0f} | Weekend avg: ₹{stats['weekend_avg']:.0f}\n"
        f"📊 1st half avg: ₹{stats['first_half_avg']:.0f} | 2nd half avg: ₹{stats['second_half_avg']:.0f}\n"
        f"🏆 Most expensive week: Week {stats['most_expensive_week'][0]} (₹{stats['most_expensive_week'][1]:.0f})\n\n"
    )

    # Enhanced DG/Outage Summary (Feature 2)
    if stats["dg_days"] > 0 and stats.get("dg_total_cost"):
        eb_rate = rc.get("eb_rate")
        dg_rate = rc.get("dg_rate")
        dg_cost = stats["dg_total_cost"]
        msg += f"⚡ <b>DG/Outage Summary</b>\n"
        msg += f"  Days: {stats['dg_days']} of {stats['days_count']}\n"
        msg += f"  DG cost: ₹{dg_cost:.0f}\n"
        if eb_rate and dg_rate and dg_rate > 0:
            # Compute DG kVAh and premium
            dg_kvah = dg_cost / dg_rate
            eb_equiv = dg_kvah * eb_rate
            premium = dg_cost - eb_equiv
            pct_of_total = (premium / stats["total"]) * 100 if stats["total"] > 0 else 0
            msg += f"  EB-equivalent: ₹{eb_equiv:.0f}\n"
            msg += f"  <b>DG premium: ₹{premium:.0f}</b> ({pct_of_total:.1f}% of total bill)\n"
        msg += "\n"
    else:
        msg += f"⚡ DG Usage: {stats['dg_days']} of {stats['days_count']} days\n\n"

    if stats["bal_start"] is not None and stats["bal_end"] is not None:
        drop = stats["bal_start"] - stats["bal_end"]
        msg += f"📉 Balance: ₹{stats['bal_start']:.0f} → ₹{stats['bal_end']:.0f} (−₹{drop:.0f})\n"

    if stats.get("recharge_count", 0) > 0:
        msg += f"🔋 Recharges: {stats['recharge_count']} totaling ₹{stats['recharge_total']:.0f}\n"

    # Weekday pattern (sent as image separately)
    pattern = stats.get("weekday_pattern", {})
    if pattern:
        if stats["weekday_avg"] > 0 and stats["weekend_avg"] > 0:
            diff_pct = ((stats["weekend_avg"] - stats["weekday_avg"]) / stats["weekday_avg"]) * 100
            direction = "more" if diff_pct > 0 else "less"
            msg += f"\n📊 Weekdays: ₹{stats['weekday_avg']:.0f}/day | Weekends: ₹{stats['weekend_avg']:.0f}/day ({abs(diff_pct):.0f}% {direction})\n"

    if monthly_consumption:
        msg += "\n<b>Monthly Consumption (Last 6 Months)</b>\n"
        for m in monthly_consumption:
            msg += f"  {m['month']}: ₹{m['amount'] or '—'}\n"

    # Balance forecast (Feature 8)
    if daily_readings and balance:
        msg += _build_balance_forecast(balance, daily_readings)

    # Appliance guide and charts sent as images separately

    msg += f"\nDuration: {duration:.1f}s"
    return msg


def check_recharge_prediction(balance, daily_readings):
    """Predict when balance will hit ₹0 based on 7-day rolling average."""
    if balance is None or not daily_readings:
        return None

    today_date = date.today()
    week_ago = today_date - timedelta(days=7)
    _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date)

    if not consumption_days:
        return None

    avg_7day = sum(d["spend"] for d in consumption_days) / len(consumption_days)
    if avg_7day <= 0:
        return None

    days_remaining = int(balance / avg_7day)
    projected_zero = today_date + timedelta(days=days_remaining)

    return {
        "avg_7day": avg_7day,
        "days_remaining": days_remaining,
        "projected_zero": projected_zero,
    }


def build_recharge_alert(prediction, balance):
    """Build recharge prediction alert message."""
    p = prediction
    urgency = "🚨" if p["days_remaining"] <= 3 else "⚠️"

    return (
        f"{urgency} <b>Recharge Prediction</b>\n\n"
        f"💳 Balance: ₹{balance}\n"
        f"📉 Avg daily spend (7-day): ₹{p['avg_7day']:.0f}\n"
        f"⏰ Balance will reach ₹0 on: <b>{p['projected_zero'].strftime('%d %b %Y')}</b> (~{p['days_remaining']} day{'s' if p['days_remaining'] != 1 else ''})\n\n"
        "Please recharge soon!"
    )


def build_recharge_analysis(new_recharges, all_recharges, balance, daily_readings, effectiveness=None, now=None):
    """Build recharge analysis Telegram message when a new recharge is detected.

    ``effectiveness`` is the precomputed output of
    ``_compute_recharge_effectiveness``. When provided, the header frames the
    message as an "Early Top-up" if the newest recharge happened while
    substantial balance remained, mirroring the color-coded chart.

    ``now`` is an IST-aware datetime captured by the caller. Using a single
    ``now`` prevents off-by-one-day drift if the computation straddles
    midnight, and avoids the silent ``date.today()``-in-UTC bug on GHA
    runners.
    """
    newest = new_recharges[0]
    if now is None:
        now = datetime.now(IST)
    today_date = now.date()

    # Header — branches on whether the newest recharge was early.
    newest_info = effectiveness[0] if effectiveness else None
    header = (
        "🔋 <b>Recharge Detected!</b>\n\n"
        f"💳 ₹{newest['amount']:,.0f} via {newest.get('type', '—')}\n"
    )
    if newest_info and newest_info["is_early"] and newest_info["balance_before"] is not None:
        bal_before = newest_info["balance_before"]
        # Runway: use a 7-day rolling avg for the projection — the ongoing
        # recharge doesn't have its own closed burn-rate window yet.
        week_ago = today_date - timedelta(days=7)
        _, cd = _build_daily_spends(daily_readings or [], week_ago, today_date)
        recent_avg = (sum(float(d["spend"]) for d in cd) / len(cd)) if cd else None
        runway_str = ""
        if recent_avg and recent_avg > 0:
            runway_days = int(bal_before / recent_avg)
            runway_str = f" (~{runway_days} days runway)"
        header = (
            "🔋 <b>Early Top-up Detected!</b>\n\n"
            f"💳 ₹{newest['amount']:,.0f} via {newest.get('type', '—')}\n"
            f"💡 ₹{bal_before:,.0f} still on meter{runway_str}\n"
        )

    msg = header + "\n"

    # Previous recharge runtime — when effectiveness data is available, show
    # the honest "lasted Xd at ₹Y/day" line for the recharge that was just
    # superseded.
    if effectiveness and len(effectiveness) > 1:
        prev = effectiveness[1]
        if prev["effective_runtime"] is not None and prev["avg_daily_spend"] is not None:
            msg += (
                f"<b>Previous ₹{prev['amount']:,.0f}</b>\n"
                f"  Lasted: {prev['effective_runtime']:.0f}d at ₹{prev['avg_daily_spend']:.0f}/day\n\n"
            )

    # Compute intervals between consecutive recharges (sorted newest first)
    intervals = []
    for i in range(len(all_recharges) - 1):
        curr_date = all_recharges[i]["date"] if isinstance(all_recharges[i]["date"], date) else datetime.strptime(all_recharges[i]["date"], "%Y-%m-%d").date()
        next_date = all_recharges[i + 1]["date"] if isinstance(all_recharges[i + 1]["date"], date) else datetime.strptime(all_recharges[i + 1]["date"], "%Y-%m-%d").date()
        days = (curr_date - next_date).days
        intervals.append(days)

    # Last 10 recharges table with duration
    # Recharge table sent as image separately

    # Stats
    amounts = [float(r["amount"]) for r in all_recharges]
    avg_amount = sum(amounts) / len(amounts)

    msg += "<b>Stats</b>\n"
    msg += f"  Avg amount: ₹{avg_amount:,.0f}\n"

    if intervals:
        avg_interval = sum(intervals) / len(intervals)
        avg_cost_per_day = avg_amount / avg_interval if avg_interval > 0 else 0
        msg += f"  Avg interval: {avg_interval:.0f} days\n"
        msg += f"  Avg cost/day: ₹{avg_cost_per_day:,.0f}\n"

    # Trends (compare last 3 vs overall)
    has_freq_trend = len(intervals) >= 4
    has_amt_trend = len(amounts) >= 4
    if has_freq_trend or has_amt_trend:
        msg += f"\n📉 <b>Trends</b>\n"
        if has_freq_trend:
            recent_intervals = intervals[:3]
            recent_avg_interval = sum(recent_intervals) / len(recent_intervals)
            freq_arrow = "↑ more frequent" if recent_avg_interval < avg_interval else "↓ less frequent"
            msg += f"  Frequency: {freq_arrow} ({recent_avg_interval:.0f}d vs {avg_interval:.0f}d avg)\n"
        if has_amt_trend:
            recent_amounts = amounts[:3]
            recent_avg_amount = sum(recent_amounts) / len(recent_amounts)
            amt_arrow = "↑ higher" if recent_avg_amount > avg_amount else "↓ lower"
            msg += f"  Amount: {amt_arrow} (₹{recent_avg_amount:,.0f} vs ₹{avg_amount:,.0f} avg)\n"

    # Next recharge prediction
    if balance is not None and daily_readings:
        week_ago = today_date - timedelta(days=7)
        _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date)
        if consumption_days:
            avg_daily_spend = sum(d["spend"] for d in consumption_days) / len(consumption_days)
            if avg_daily_spend > 0:
                days_left = int(float(balance) / float(avg_daily_spend))
                next_date = today_date + timedelta(days=days_left)
                msg += (
                    f"\n🔮 This ₹{newest['amount']:,.0f} should last ~{days_left} days\n"
                    f"   Next recharge: ~{next_date.strftime('%d %b %Y')}\n"
                )

    return msg


# --- Constants for new features ---

APPLIANCE_GUIDE = [
    ("AC (1.5T inv)", 1.0, 8),       # 1.0 kW avg running, 8 hrs/day
    ("Geyser", 2.0, 0.5),             # 2.0 kW, 30 min
    ("Washing Machine", 0.5, 1),       # 0.5 kW, 1 hr
    ("Fridge", 0.15, 8),                # 0.15 kW, ~8 hrs effective (compressor cycles)
    ("LED TV (55\")", 0.1, 6),         # 0.1 kW, 6 hrs
]
POWER_FACTOR = 0.9

SEASON_HINTS = {
    "summer": ((4, 5, 6), "Higher AC usage typical for summer."),
    "monsoon": ((7, 8, 9), "Monsoon — moderate usage expected."),
    "winter": ((11, 12, 1, 2), "Heater usage may increase costs."),
    "moderate": ((3, 10), "Moderate season — usage should be stable."),
}


def _dg_premium_str(dg_cost, rate_card):
    """Return ' (premium: ₹X)' string if DG cost > 0, else ''."""
    if not dg_cost or dg_cost <= 0:
        return ""
    eb_rate = rate_card.get("eb_rate")
    dg_rate = rate_card.get("dg_rate")
    if not eb_rate or not dg_rate or dg_rate <= 0:
        return ""
    dg_kvah = dg_cost / dg_rate
    premium = dg_kvah * (dg_rate - eb_rate)
    return f" (premium: ₹{premium:.0f})"


def _build_wow_line(daily_readings):
    """Build week-over-week comparison line for evening message."""
    today_date = date.today()
    # Need at least Wednesday (3 days of current week)
    this_monday = today_date - timedelta(days=today_date.weekday())
    _, this_week = _build_daily_spends(daily_readings, this_monday, today_date)

    if len(this_week) < 3:
        return None

    # Compare same days of last week (e.g., Mon-Wed vs Mon-Wed) for fair comparison
    last_monday = this_monday - timedelta(days=7)
    last_same_day = today_date - timedelta(days=7)
    _, last_week = _build_daily_spends(daily_readings, last_monday, last_same_day)

    if not last_week:
        return None

    this_avg = sum(d["spend"] for d in this_week) / len(this_week)
    last_avg = sum(d["spend"] for d in last_week) / len(last_week)

    if last_avg <= 0:
        return None

    pct_change = ((this_avg - last_avg) / last_avg) * 100
    if abs(pct_change) < 5:
        return None

    arrow = "↑" if pct_change > 0 else "↓"
    return (
        f"\n📊 This week ({len(this_week)} days): ₹{this_avg:.0f}/day avg"
        f"\n   Last week: ₹{last_avg:.0f}/day — tracking {arrow}{abs(pct_change):.0f}%"
    )


def build_recharge_advisor(balance, daily_readings, now=None):
    """Prescriptive recharge advice: how much to recharge to last until month-end.

    Suppressed when a recharge happened in the last ``_ADVISOR_POST_RECHARGE_DAYS``
    (queried from the ``recharges`` table). The old signature took a
    ``new_recharges`` list from the evening path's in-memory detection; that
    path now runs in snapshot mode, so evening-time detection always returns
    ``[]``. Querying the persisted recharges table is the authoritative
    "was there a recent top-up" check and covers any caller.
    """
    import calendar

    if balance is None or balance <= 0:
        return None

    if now is None:
        now = datetime.now(IST)
    today_date = now.date()

    last_rc = last_recharge_date()
    if last_rc is not None and (today_date - last_rc).days <= _ADVISOR_POST_RECHARGE_DAYS:
        return None

    week_ago = today_date - timedelta(days=7)
    _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date)

    if len(consumption_days) < 3:
        return None

    avg_daily = sum(d["spend"] for d in consumption_days) / len(consumption_days)
    if avg_daily <= 0:
        return None

    days_remaining = int(float(balance) / float(avg_daily))
    if days_remaining >= 10:
        return None

    _, days_in_month = calendar.monthrange(today_date.year, today_date.month)
    days_until_end = days_in_month - today_date.day + 1  # +1 to include today

    if days_remaining >= days_until_end:
        return None

    total_needed = float(avg_daily) * days_until_end
    shortfall = total_needed - float(balance)
    rounded = math.ceil(shortfall / 500) * 500
    buffer_amount = rounded + 500
    buffer_days = int(buffer_amount / float(avg_daily))

    month_end = date(today_date.year, today_date.month, days_in_month)
    return (
        "💡 <b>Recharge Advisor</b>\n\n"
        f"📉 Balance: ₹{balance:,.0f} (~{days_remaining} days remaining)\n"
        f"📅 Month-end: {month_end.strftime('%d %b')} ({days_until_end} days away)\n\n"
        f"💳 <b>Recharge ₹{rounded:,} to last until {month_end.strftime('%d %b')}</b>\n"
        f"  Based on 7-day avg: ₹{avg_daily:.0f}/day\n\n"
        f"Tip: Round up — ₹{buffer_amount:,} gives ~{buffer_days} days buffer."
    )


def check_spending_trend(daily_readings):
    """Alert if current month pace differs significantly from last month."""
    today_date = date.today()
    if today_date.day < 5 or today_date.day not in (7, 14, 21, 28):
        return None

    prev_month_end = today_date.replace(day=1) - timedelta(days=1)
    year_month = f"{prev_month_end.year}-{prev_month_end.month:02d}"
    prev_readings_raw = load_daily_readings(year_month)
    if not prev_readings_raw:
        return None

    # Convert stored readings to have date objects
    from datetime import date as date_type
    prev_readings = []
    for r in prev_readings_raw:
        rd = r["date"]
        if isinstance(rd, str):
            rd = datetime.strptime(rd, "%Y-%m-%d").date()
        prev_readings.append({**r, "date": rd, "balance": Decimal(str(r["balance"])) if r.get("balance") is not None else None,
                              "dg_consume": Decimal(str(r["dg_consume"])) if r.get("dg_consume") is not None else None})

    current_start = today_date.replace(day=1)
    _, curr_consumption = _build_daily_spends(daily_readings, current_start, today_date)

    prev_start = date(prev_month_end.year, prev_month_end.month, 1)
    prev_compare_end = date(prev_month_end.year, prev_month_end.month, min(today_date.day, prev_month_end.day))
    _, prev_consumption = _build_daily_spends(prev_readings, prev_start, prev_compare_end)

    if len(curr_consumption) < 3 or len(prev_consumption) < 3:
        return None

    curr_avg = sum(d["spend"] for d in curr_consumption) / len(curr_consumption)
    prev_avg = sum(d["spend"] for d in prev_consumption) / len(prev_consumption)

    if prev_avg <= 0:
        return None

    pct_change = ((curr_avg - prev_avg) / prev_avg) * 100
    if abs(pct_change) < 20:
        return None

    import calendar
    prev_month_name = calendar.month_abbr[prev_month_end.month]
    curr_month_name = calendar.month_abbr[today_date.month]
    arrow = "↑" if pct_change > 0 else "↓"
    severity = "significantly " if abs(pct_change) > 30 else ""

    # Season hint
    hint = ""
    for season, (months, desc) in SEASON_HINTS.items():
        if today_date.month in months:
            hint = f"\n📈 {desc}"
            if season == "summer":
                hint += "\nTip: Each °C lower on AC saves ~₹15/day."
            break

    curr_total = sum(d["spend"] for d in curr_consumption)
    prev_total = sum(d["spend"] for d in prev_consumption)
    return (
        f"📊 <b>Spending Trend Alert</b>\n\n"
        f"{curr_month_name} {severity}tracking <b>{arrow}{abs(pct_change):.0f}%</b> vs {prev_month_name} (same period).\n"
        f"  {curr_month_name} ({today_date.day} days): ₹{curr_total:,.0f} avg ₹{curr_avg:.0f}/day\n"
        f"  {prev_month_name} (first {prev_compare_end.day} days): ₹{prev_total:,.0f} avg ₹{prev_avg:.0f}/day"
        f"{hint}"
    )


def _build_spend_chart_image(consumption_days, balance):
    """Build spend chart image for weekly/monthly reports. Returns BytesIO or None."""
    if not consumption_days or len(consumption_days) < 5:
        return None
    days = consumption_days[-14:]
    dates = [d["date"] for d in days]
    spends = [d["spend"] for d in days]
    avg = sum(float(s) for s in spends) / len(spends)

    avg_daily = avg
    days_left = int(float(balance) / avg_daily) if avg_daily > 0 and balance else 0
    zero_date = (date.today() + timedelta(days=days_left)).strftime("%d %b") if days_left > 0 else "—"

    return render_spend_chart(
        "Daily Spend",
        f"Avg ₹{avg:.0f}/day | Balance ₹{float(balance):,.0f} → ₹0 on ~{zero_date} (~{days_left}d)",
        dates, spends, avg_line=avg
    )


def _build_balance_forecast(balance, daily_readings):
    """Build balance forecast text for weekly/monthly reports."""
    if not balance or balance <= 0 or not daily_readings:
        return ""
    today_date = date.today()
    two_weeks_ago = today_date - timedelta(days=14)
    _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
    if len(consumption_days) < 5:
        return ""

    avg_daily = sum(d["spend"] for d in consumption_days) / len(consumption_days)
    if avg_daily <= 0:
        return ""

    days_left = int(float(balance) / float(avg_daily))
    zero_date = today_date + timedelta(days=days_left)

    return (
        f"\n🔮 <b>Balance Forecast</b>\n"
        f"💳 Current: ₹{balance:,.0f}\n"
        f"📉 At ₹{avg_daily:.0f}/day → ₹0 on ~{zero_date.strftime('%d %b')}"
        f" (~{days_left} days)\n"
    )


def _build_appliance_guide_image(eb_rate, fix_charge=None):
    """Build appliance cost guide as table image. Returns BytesIO or None."""
    if not eb_rate or eb_rate <= 0:
        return None
    rows = []
    for name, kw, hours in APPLIANCE_GUIDE:
        kwh = kw * hours
        kvah = kwh / POWER_FACTOR
        cost = kvah * float(eb_rate)
        hrs_label = f"{hours:.0f}hr" if hours >= 1 else f"{int(hours * 60)}m"
        rows.append([name, hrs_label, f"₹{cost:.0f}"])
    subtitle = f"At EB ₹{float(eb_rate):.2f}/kVAh · Power factor 0.9"
    if fix_charge and fix_charge > 0:
        subtitle += f" · On top of ₹{float(fix_charge):.0f}/day maintenance"
    return render_table_image(
        "Appliance Cost Guide", subtitle,
        ["Appliance", "Usage", "Cost/day"], rows,
        col_alignments=["left", "right", "right"],
    )


def _build_recharge_table_image(all_recharges, intervals):
    """Build recharge history as table image. Returns BytesIO or None."""
    if not all_recharges:
        return None
    rows = []
    for i, r in enumerate(all_recharges):
        r_date = r["date"] if isinstance(r["date"], date) else datetime.strptime(r["date"], "%Y-%m-%d").date()
        date_str = r_date.strftime("%d %b %y")
        if i == 0:
            days_since = (date.today() - r_date).days
            days_str = f"{days_since}d*"
        elif (i - 1) < len(intervals):
            days_str = f"{intervals[i - 1]}d"
        else:
            days_str = ""
        rows.append([f"₹{float(r['amount']):,.0f}", date_str, r.get("type", ""), days_str])
    return render_table_image(
        "Last 10 Recharges", "How long each recharge lasted (* = ongoing)",
        ["Amount", "Date", "Type", "Lasted"], rows,
        col_alignments=["right", "left", "left", "right"],
    )


def _build_weekday_chart_image(pattern):
    """Build weekday spending pattern as bar chart image. Returns BytesIO or None."""
    if not pattern:
        return None
    days = list(pattern.keys())
    vals = list(pattern.values())
    return render_bar_chart(
        "Weekday Spending Pattern",
        "Average daily spend by day of week",
        days, vals, value_fmt="₹{:.0f}",
    )


def _build_balance_runway_image(daily_readings, balance):
    """Chart 1: Balance line chart with projection to ₹0."""
    if not balance or balance <= 0 or not daily_readings:
        return None
    today_date = date.today()
    two_weeks_ago = today_date - timedelta(days=14)
    relevant = [r for r in daily_readings if two_weeks_ago <= r["date"] <= today_date and r["balance"] is not None]
    if len(relevant) < 5:
        return None

    dates = [r["date"] for r in relevant]
    balances = [r["balance"] for r in relevant]

    # Compute avg daily spend for projection
    _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
    if not consumption_days:
        return None
    avg_daily = float(sum(d["spend"] for d in consumption_days) / len(consumption_days))
    if avg_daily <= 0:
        return None

    # Project forward to ₹0
    days_left = int(float(balance) / avg_daily)
    proj_dates = [today_date + timedelta(days=i) for i in range(1, min(days_left + 2, 31))]
    proj_values = [float(balance) - avg_daily * i for i in range(1, len(proj_dates) + 1)]

    zero_date = (today_date + timedelta(days=days_left)).strftime("%d %b")
    return render_line_chart(
        "Balance Runway",
        f"₹{float(balance):,.0f} remaining → ₹0 on ~{zero_date} (~{days_left}d)",
        dates, balances,
        projection_dates=proj_dates, projection_values=proj_values,
    )


def _build_bill_split_image(deductions, rate_card, title="Bill Composition"):
    """Charts 2 & 4: Donut chart of EB/DG/Fix split."""
    eb = float(deductions.get("eb") or 0)
    dg = float(deductions.get("dg") or 0)
    fix = float(deductions.get("fix_charge") or 0)
    # Add pending fix charge if it's 0 (evening: not yet deducted)
    if fix == 0 and rate_card:
        fix = float(rate_card.get("fix_charge") or 0)

    if eb + dg + fix <= 0:
        return None

    total = eb + dg + fix
    labels = ["Electricity (EB)", "Generator (DG)", "Maintenance"]
    values = [eb, dg, fix]
    controllable = eb + dg
    subtitle = f"Controllable: ₹{controllable:,.0f} ({controllable/total*100:.0f}%) | Fixed: ₹{fix:,.0f} ({fix/total*100:.0f}%)"

    return render_donut_chart(title, subtitle, labels, values)


def _build_daily_spend_trend_image(daily_readings, rate_card=None):
    """Bar chart of last 14 days' total spend with today highlighted."""
    today_date = date.today()
    start = today_date - timedelta(days=13)
    _, consumption_days = _build_daily_spends(daily_readings, start, today_date)
    if len(consumption_days) < 3:
        return None

    dates = [d["date"] for d in consumption_days]
    spends = [d["spend"] for d in consumption_days]
    avg = sum(spends) / len(spends)
    max_d = consumption_days[max(range(len(spends)), key=lambda i: spends[i])]
    min_d = consumption_days[min(range(len(spends)), key=lambda i: spends[i])]
    subtitle = f"High: ₹{max_d['spend']:.0f} ({max_d['date'].strftime('%d %b')}) | Low: ₹{min_d['spend']:.0f} ({min_d['date'].strftime('%d %b')})"

    return render_spend_chart(
        "Daily Spending — Last 14 Days",
        subtitle, dates, spends,
        avg_line=avg, highlight_date=today_date,
    )


def _build_week_vs_week_image(daily_readings):
    """Chart 3: Side-by-side bars for this week vs last week."""
    today_date = date.today()
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    this_monday = today_date - timedelta(days=today_date.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_same_day = today_date - timedelta(days=7)

    all_spends_this, this_week = _build_daily_spends(daily_readings, this_monday, today_date)
    all_spends_last, last_week = _build_daily_spends(daily_readings, last_monday, last_same_day)

    if len(this_week) < 3 or not last_week:
        return None

    # Build per-day lookup
    this_by_day = {d["date"].weekday(): float(d["spend"]) for d in this_week}
    last_by_day = {d["date"].weekday(): float(d["spend"]) for d in last_week}

    # Only show days that exist in this week
    labels, g1, g2 = [], [], []
    for i in range(7):
        if i in this_by_day:
            labels.append(day_names[i])
            g1.append(this_by_day[i])
            g2.append(last_by_day.get(i, 0))

    if not labels:
        return None

    this_total = sum(g1)
    last_total = sum(g2)
    pct = ((this_total - last_total) / last_total * 100) if last_total > 0 else 0
    arrow = "+" if pct > 0 else ""

    return render_grouped_bars(
        "Week vs Week",
        f"This week ₹{this_total:,.0f} ({arrow}{pct:.0f}%) vs last week ₹{last_total:,.0f}",
        labels, g1, g2,
    )


def _build_balance_journey_image(daily_readings, year, month):
    """Chart 5: Full month balance line with recharge markers."""
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]
    month_start = date(year, month, 1)
    month_end = date(year, month, days_in_month)

    relevant = sorted(
        [r for r in daily_readings if month_start <= r["date"] <= month_end and r["balance"] is not None],
        key=lambda r: r["date"]
    )
    if len(relevant) < 5:
        return None

    dates = [r["date"] for r in relevant]
    balances = [r["balance"] for r in relevant]

    # Detect recharge points (balance jumps up)
    markers = {}
    for i in range(1, len(relevant)):
        if float(balances[i]) > float(balances[i - 1]) + 100:
            amount = float(balances[i]) - float(balances[i - 1])
            markers[i] = f"+₹{amount:,.0f}"

    bal_start = float(balances[0])
    bal_end = float(balances[-1])
    month_name = calendar.month_abbr[month]
    recharge_count = len(markers)

    return render_line_chart(
        f"Balance Journey — {month_name} {year}",
        f"₹{bal_start:,.0f} → ₹{bal_end:,.0f} | {recharge_count} recharge{'s' if recharge_count != 1 else ''}",
        dates, balances, markers=markers,
    )


def _build_monthly_trend_image(monthly_consumption):
    """Chart 6: 6-month trend as vertical bars."""
    if not monthly_consumption or len(monthly_consumption) < 2:
        return None

    # Reverse so oldest is first (portal gives newest first)
    items = [m for m in reversed(monthly_consumption) if m.get("amount")]
    if len(items) < 2:
        return None

    labels = [m["month"] for m in items]
    values = [m["amount"] for m in items]
    avg = sum(float(v) for v in values) / len(values)

    return render_spend_chart(
        "Monthly Trend",
        f"Avg ₹{avg:,.0f}/month over {len(items)} months",
        labels, values, avg_line=avg,
    )


def _balance_before_recharge(recharge_date, daily_readings):
    """Opening balance on the recharge day = pre-recharge balance.

    Daily readings store ``amount_total`` as the portal's midnight snapshot,
    so the row dated ``recharge_date`` is the balance immediately before any
    same-day top-up. Returns None when the date isn't in daily_readings
    (e.g., recharge happened before our data window).

    Normalizes each row's ``date`` field to a ``date`` object before
    comparison — psycopg2 returns ``date`` but an in-memory normalizer result
    can be either, and a silent type mismatch would return None for every
    row and all bars would render ``bal ?``.
    """
    for r in daily_readings or []:
        r_date = r.get("date")
        if isinstance(r_date, str):
            try:
                r_date = datetime.strptime(r_date, "%Y-%m-%d").date()
            except ValueError:
                continue
        if r_date == recharge_date and r.get("balance") is not None:
            return float(r["balance"])
    return None


def _compute_recharge_effectiveness(all_recharges, daily_readings, now=None):
    """For each recharge, compute:
      - interval_days: calendar days until the next (newer) recharge, or
        days-since for the ongoing newest recharge.
      - avg_daily_spend: mean consumption-day spend within that window
        (recharge day itself excluded; `_build_daily_spends` already drops
        negative-spend rows i.e. top-up days).
      - effective_runtime: amount / avg_daily_spend — the days this recharge
        would have lasted at the observed burn rate. Industry-standard
        "energy purchased per recharge" metric that stays accurate across
        early top-ups.
      - balance_before: opening balance on the recharge day.
      - is_early: balance_before > 3 × classification_rate. ``classification_rate``
        is ``avg_daily_spend`` when available, else a 7-day rolling mean ending
        the day before the recharge. Falling back covers the ongoing newest
        recharge (no closed window) and back-to-back recharges where the window
        has < 3 consumption days.
      - is_negative: balance_before < 0 (recharged during grace period).

    Today-date baseline: takes an optional ``now`` (IST-aware datetime). Using
    a single caller-supplied ``now`` avoids mid-computation day rollover and
    the ``date.today()``-runs-in-UTC bug on GHA runners (where the wall-clock
    date is 5:30h ahead of IST during the early-morning hours).

    Returns list aligned with ``all_recharges`` (newest first). Fields are
    None when underlying data is missing — callers must check.
    """
    if now is None:
        now = datetime.now(IST)
    today_date = now.date()
    results = []

    for i, r in enumerate(all_recharges):
        r_date = r["date"] if isinstance(r["date"], date) else datetime.strptime(r["date"], "%Y-%m-%d").date()
        amount = float(r["amount"])

        if i == 0:
            interval_days = max((today_date - r_date).days, 0)
            window_end = today_date
            is_ongoing = True
        else:
            prev = all_recharges[i - 1]
            prev_date = prev["date"] if isinstance(prev["date"], date) else datetime.strptime(prev["date"], "%Y-%m-%d").date()
            interval_days = max((prev_date - r_date).days, 0)
            window_end = prev_date - timedelta(days=1)
            is_ongoing = False

        window_start = r_date + timedelta(days=1)
        if window_start <= window_end:
            _, consumption_days = _build_daily_spends(daily_readings or [], window_start, window_end)
        else:
            consumption_days = []

        if len(consumption_days) >= 3:
            avg_spend = sum(float(d["spend"]) for d in consumption_days) / len(consumption_days)
            effective_runtime = amount / avg_spend if avg_spend > 0 else None
        else:
            avg_spend = None
            effective_runtime = None

        balance_before = _balance_before_recharge(r_date, daily_readings)

        # For early-detection we need SOME burn rate. The ongoing recharge
        # (i=0) has no closed window, so fall back to a 7-day rolling mean
        # ending on the recharge day. Same fallback covers short-interval
        # recharges (< 3 consumption days) further back in history.
        classification_rate = avg_spend
        if classification_rate is None:
            rolling_start = r_date - timedelta(days=7)
            rolling_end = r_date - timedelta(days=1)
            if rolling_start <= rolling_end:
                _, rolling_days = _build_daily_spends(daily_readings or [], rolling_start, rolling_end)
                if len(rolling_days) >= 3:
                    classification_rate = sum(float(d["spend"]) for d in rolling_days) / len(rolling_days)

        is_early = False
        is_negative = False
        if balance_before is not None:
            if balance_before < -1:
                is_negative = True
            elif classification_rate is not None and balance_before > 3 * classification_rate:
                is_early = True

        results.append({
            "recharge": r,
            "date": r_date,
            "amount": amount,
            "interval_days": interval_days,
            "avg_daily_spend": avg_spend,
            "effective_runtime": effective_runtime,
            "balance_before": balance_before,
            "is_early": is_early,
            "is_negative": is_negative,
            "is_ongoing": is_ongoing,
        })

    return results


def _build_recharge_intervals_image(effectiveness):
    """Recharge Effectiveness chart.

    Bar length = effective runtime (``amount / avg_daily_spend`` during that
    recharge's window) — the honest "how long did each recharge last" answer
    that doesn't get distorted by early top-ups. Falls back to the raw
    interval with a ▵ marker when < 3 consumption days are available for
    burn-rate estimation.

    Per-bar color encodes the early-recharge signal:
      - teal   — normal: recharged close to empty
      - yellow — early top-up: balance > ~3 days of avg spend remained
      - red    — ran into grace: balance went negative before top-up

    Each bar annotation shows ``{runtime}d · ₹{balance_before} left`` so the
    user sees at a glance why a bar is yellow (how much was unused).

    The newest (ongoing) recharge is skipped — no closed window to measure
    yet.

    Name kept as ``_build_recharge_intervals_image`` for backward compatibility
    with existing callers; the chart title and semantics are now the richer
    effectiveness view.
    """
    if not effectiveness:
        return None

    # Skip the ongoing newest recharge — no closed window to measure yet.
    closed = [e for e in effectiveness if not e["is_ongoing"]]
    if not closed:
        return None

    labels, values, bar_colors, annotations = [], [], [], []
    for e in closed:
        r_date = e["date"]
        amt = e["amount"]
        amt_label = f"₹{amt/1000:.0f}K" if amt >= 1000 else f"₹{amt:.0f}"
        labels.append(f"{amt_label} {r_date.strftime('%d %b')}")

        if e["effective_runtime"] is not None:
            runtime = e["effective_runtime"]
            marker = ""
        else:
            # Fallback: use raw interval when burn rate isn't computable.
            runtime = e["interval_days"]
            marker = "▵ "
        values.append(runtime)

        if e["is_negative"]:
            bar_colors.append("#e74c3c")  # red
        elif e["is_early"]:
            bar_colors.append("#f1c40f")  # yellow
        else:
            bar_colors.append("#4ecca3")  # teal

        bal = e["balance_before"]
        if bal is None:
            bal_str = "bal ?"
        elif bal < -1:
            bal_str = f"₹{bal:,.0f} (grace)"
        elif bal > 1:
            bal_str = f"₹{bal:,.0f} left"
        else:
            bal_str = "empty"
        annotations.append(f"{marker}{runtime:.0f}d · {bal_str}")

    runtimes = [e["effective_runtime"] for e in closed if e["effective_runtime"] is not None]
    if runtimes:
        avg_runtime = sum(runtimes) / len(runtimes)
        coverage_note = f" ({len(runtimes)}/{len(closed)} with burn rate)" if len(runtimes) < len(closed) else ""
        subtitle = f"Effective runtime per recharge · Avg {avg_runtime:.0f}d{coverage_note}"
    else:
        subtitle = "Days between recharges (burn rate unavailable)"

    return render_bar_chart(
        "Recharge Effectiveness",
        subtitle,
        labels, values,
        colors=bar_colors,
        bar_annotations=annotations,
        x_axis_fmt=lambda x: f"{int(x)}d",
    )


def check_grace_period(balance, grace_credit, rate_card=None):
    """Check if balance is negative or will go negative after tonight's fix charge."""
    if balance is None:
        return None

    fix = float((rate_card or {}).get("fix_charge") or 0)

    if balance < 0:
        used = abs(float(balance))
        limit = float(grace_credit) if grace_credit else 1500
        return (
            "🚨 <b>Grace Period Active!</b>\n\n"
            f"💳 Balance: <b>₹{balance}</b>\n"
            f"Using grace credit: ₹{used:.0f} of ₹{limit:.0f}\n"
            f"Remaining grace: ₹{limit - used:.0f}\n\n"
            "Recharge immediately to avoid disconnection!"
        )

    if fix > 0 and float(balance) < fix:
        projected = float(balance) - fix
        return (
            "⚠️ <b>Grace Period Warning!</b>\n\n"
            f"💳 Balance: ₹{balance}\n"
            f"Tonight's maintenance: ₹{fix:.0f}\n"
            f"Projected balance after: <b>₹{projected:.0f}</b>\n\n"
            "Balance will go negative tonight. Recharge now!"
        )

    return None


def check_consumption_spike(today_deductions, daily_readings, rate_card=None):
    """Alert if today's consumption is >50% above 7-day average."""
    today_total = today_deductions.get("total")
    if not today_total or today_total <= 0:
        return None

    # Add pending fix charge (deducted at ~11 PM, not reflected at 10 PM)
    fix = Decimal(str((rate_card or {}).get("fix_charge") or 0))
    adjusted_today = today_total + fix

    threshold = float(os.getenv("SPIKE_THRESHOLD", "1.5"))
    today_date = date.today()
    week_ago = today_date - timedelta(days=7)
    _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date - timedelta(days=1))

    if not consumption_days:
        return None

    avg_7day = sum(d["spend"] for d in consumption_days) / len(consumption_days)
    if avg_7day <= 0:
        return None

    if adjusted_today > avg_7day * Decimal(str(threshold)):
        pct = ((adjusted_today - avg_7day) / avg_7day) * 100
        return (
            "🚨 <b>Consumption Spike Alert!</b>\n\n"
            f"Today's spend: ₹{adjusted_today:.0f}\n"
            f"7-day average: ₹{avg_7day:.0f}\n"
            f"Spike: ↑{pct:.0f}% above normal\n\n"
            "Check for appliances left running."
        )
    return None


def check_dg_usage(today_deductions, rate_card, daily_readings=None):
    """Alert when DG is used (expensive — 4.5x EB rate)."""
    dg_cost = today_deductions.get("dg")
    if not dg_cost or dg_cost <= 0:
        return None

    eb_rate = rate_card.get("eb_rate")
    dg_rate = rate_card.get("dg_rate")
    if eb_rate and dg_rate and dg_rate > 0:
        dg_kvah = dg_cost / dg_rate
        eb_equivalent = dg_kvah * eb_rate
        premium = dg_cost - eb_equivalent
        ratio = dg_rate / eb_rate
        msg = (
            "⚡ <b>DG Usage Detected!</b>\n\n"
            f"DG cost today: ₹{dg_cost}\n"
            f"Same usage on EB would cost: ₹{eb_equivalent:.2f}\n"
            f"Premium paid: ₹{premium:.2f} ({ratio:.1f}x EB rate)"
        )

        # Month-to-date DG context (Feature 2)
        if daily_readings:
            today_date = date.today()
            month_start = today_date.replace(day=1)
            month_readings = [r for r in daily_readings if month_start <= r["date"] <= today_date]
            dg_days_mtd = sum(1 for r in month_readings if r.get("dg_consume") and r["dg_consume"] > 0)
            dg_kvah_mtd = sum(float(r["dg_consume"]) for r in month_readings if r.get("dg_consume") and r["dg_consume"] > 0)
            dg_premium_mtd = dg_kvah_mtd * (float(dg_rate) - float(eb_rate))
            msg += f"\n\n📊 <b>This month</b>: {dg_days_mtd} DG days, total premium: ₹{dg_premium_mtd:.0f}"

        return msg

    return (
        "⚡ <b>DG Usage Detected!</b>\n\n"
        f"DG cost today: ₹{dg_cost}"
    )


def check_rate_changes(rate_card):
    """Alert if any rate changed from last known rates."""
    from storage import load_rates
    last = load_rates()
    if not last:
        return None  # First run, no comparison

    changes = []
    labels = {"eb_rate": "EB Rate", "dg_rate": "DG Rate", "fix_charge": "Fix Charge"}
    units = {"eb_rate": "per kVAh", "dg_rate": "per kVAh", "fix_charge": "per day"}

    for key, label in labels.items():
        old_val = Decimal(str(last.get(key, 0)))
        new_val = rate_card.get(key)
        if new_val is not None and old_val != new_val and old_val > 0:
            pct = ((new_val - old_val) / old_val) * 100
            arrow = "↑" if pct > 0 else "↓"
            changes.append(f"{label}: ₹{old_val} → ₹{new_val} {units[key]} ({arrow}{abs(pct):.0f}%)")

    if changes:
        return "🔔 <b>Rate Change Detected!</b>\n\n" + "\n".join(changes)
    return None


def check_fix_charge_anomaly(prev_day_deductions, rate_card):
    """Alert if yesterday's fix charge is significantly higher than expected.
    Uses prev_day because today's fix charge is ₹0 at 10 PM (deducted at ~11 PM)."""
    prev_fix = prev_day_deductions.get("fix_charge")
    daily_rate = rate_card.get("fix_charge")

    if not prev_fix or not daily_rate or daily_rate <= 0:
        return None

    # 20% tolerance for rounding
    if prev_fix > daily_rate * Decimal("1.2"):
        excess = prev_fix - daily_rate
        return (
            "🔔 <b>Fix Charge Anomaly!</b>\n\n"
            f"Yesterday's fix charge: ₹{prev_fix}\n"
            f"Expected daily rate: ₹{daily_rate}\n"
            f"Excess: ₹{excess:.2f}\n\n"
            "Possible forced deduction by maintenance."
        )
    return None


def send_telegram_photo(photo_buf, caption=""):
    """Send a photo via Telegram Bot API. Returns True on success, False on
    config-missing / network / HTTP errors. Exceptions are always swallowed."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set, skipping photo")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    files = {"photo": ("chart.png", photo_buf, "image/png")}

    try:
        resp = requests.post(url, data=data, files=files, timeout=15)
        if resp.ok:
            logger.info("Telegram photo sent")
            return True
        # Only log status code — resp.text might echo the URL (with the
        # embedded bot token) back; this would leak into public GHA logs.
        logger.warning(f"Telegram photo error: HTTP {resp.status_code}")
        return False
    except Exception as e:
        # Exception messages from `requests` often contain the full URL
        # including the bot token. Log only the exception type.
        logger.warning(f"Failed to send Telegram photo: {type(e).__name__}")
        return False


def send_telegram_message(text):
    """Send a message via Telegram Bot API. Returns True on success, False on
    config-missing / network / HTTP errors. Exceptions are always swallowed."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set, skipping notification")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.ok:
            logger.info("Telegram notification sent")
            return True
        # resp.text may echo the URL (with bot token) — log only status.
        logger.warning(f"Telegram API error: HTTP {resp.status_code}")
        return False
    except Exception as e:
        # Exception message may contain the URL with bot token.
        logger.warning(f"Failed to send Telegram message: {type(e).__name__}")
        return False


def _budget_line(current_month, rate_card=None):
    """Build budget tracking line if current month total is available."""
    cm_total = current_month.get("total")
    budget = CONFIG["MONTHLY_BUDGET"]
    if cm_total and cm_total > 0 and budget > 0:
        # Add pending fix charge (deducted at ~11 PM, not reflected at 10 PM)
        fix = Decimal(str((rate_card or {}).get("fix_charge") or 0))
        adjusted = cm_total + fix
        pct = (adjusted / budget) * 100
        icon = "🟢" if pct < 50 else "🟡" if pct < 75 else "🟠" if pct < 90 else "🔴"
        return f"{icon} Budget: ₹{adjusted:.0f} of ₹{budget} ({pct:.0f}%)\n"
    return ""


def _yesterday_comparison(prev_day, daily_readings, rate_card):
    """Compare yesterday's electricity (EB+DG) against recent history."""
    eb = prev_day.get("eb")
    dg = prev_day.get("dg")
    if not eb and not dg:
        return ""
    yesterday_elec = (eb or Decimal("0")) + (dg or Decimal("0"))
    if yesterday_elec <= 0:
        return ""

    yesterday_date = date.today() - timedelta(days=1)
    week_ago = yesterday_date - timedelta(days=7)
    _, consumption_days = _build_daily_spends(daily_readings, week_ago, yesterday_date)
    if len(consumption_days) < 2:
        return ""

    # Use stored costs if available, otherwise approximate from balance drop - fix charge
    fix_rate = Decimal(str((rate_card or {}).get("fix_charge") or 0))
    historical = [d for d in consumption_days if d["date"] < yesterday_date]
    if not historical:
        return ""

    # Load stored daily readings to get exact eb_cost/dg_cost if available
    stored_by_date = {}
    for month_offset in [0, 1]:
        ym = (yesterday_date.replace(day=1) - timedelta(days=30 * month_offset)).strftime("%Y-%m")
        stored = load_daily_readings(ym)
        if stored:
            for r in stored:
                d = r["date"] if isinstance(r["date"], date) else r["date"]
                stored_by_date[d] = r

    elec_spends = []
    for d in historical:
        date_key = d["date"].isoformat()
        r = stored_by_date.get(date_key)
        if r and r.get("eb_cost") is not None:
            elec = Decimal(str(r.get("eb_cost") or 0)) + Decimal(str(r.get("dg_cost") or 0))
        else:
            elec = max(d["spend"] - fix_rate, Decimal("0"))
        elec_spends.append(elec)

    avg_elec = sum(elec_spends) / len(elec_spends)

    parts = [f"  Electricity (EB+DG): ₹{yesterday_elec:.0f}"]
    if avg_elec > 0:
        pct = ((yesterday_elec - avg_elec) / avg_elec) * 100
        arrow = "↑" if pct > 0 else "↓"
        parts.append(f"  vs {len(historical)}d avg: ₹{avg_elec:.0f} ({arrow}{abs(pct):.0f}%)")

    # Same weekday last week
    same_weekday_indices = [i for i, d in enumerate(historical) if d["date"].weekday() == yesterday_date.weekday()]
    if same_weekday_indices:
        last_week_elec = elec_spends[same_weekday_indices[-1]]
        day_name = yesterday_date.strftime("%a")
        parts.append(f"  vs last {day_name}: ₹{last_week_elec:.0f}")

    return "\n".join(parts) + "\n"


def build_morning_message(balance, current_month, prev_day, prev_month, monthly_consumption, duration, last_sync=None, rate_card=None, daily_readings=None):
    """Build the morning Telegram message"""
    cm = current_month
    pd = prev_day
    pm = prev_month
    rc = rate_card or {}
    now_ist = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")

    msg = (
        "⚡ <b>Energy Monitor — Morning Report</b>\n"
        f"📅 {now_ist}\n"
        f"🔄 Last Sync: {last_sync or '—'}\n\n"
        f"💳 Recharge Left: <b>₹{balance or '—'}</b>\n"
    )
    msg += _budget_line(cm, rc)
    msg += (
        "\n<b>Yesterday's Deductions</b>\n"
        f"  Total: ₹{pd.get('total') or '—'}\n"
        f"  EB: ₹{pd.get('eb') or '—'}\n"
        f"  DG: ₹{pd.get('dg') or '—'}{_dg_premium_str(pd.get('dg'), rc)}\n"
        f"  Fix Charge: ₹{pd.get('fix_charge') or '—'}\n"
    )

    # Yesterday's electricity comparison
    if daily_readings:
        comp = _yesterday_comparison(pd, daily_readings, rc)
        if comp:
            msg += comp

    msg += (
        "\n<b>Current Month Deductions</b>\n"
        f"  Total: ₹{cm.get('total') or '—'}\n"
        f"  EB: ₹{cm.get('eb') or '—'}\n"
        f"  DG: ₹{cm.get('dg') or '—'}{_dg_premium_str(cm.get('dg'), rc)}\n"
        f"  Fix Charge: ₹{cm.get('fix_charge') or '—'}\n"
    )

    # Balance runway using last 7 days of actual daily spends
    if balance and balance > 0 and daily_readings:
        today_date = date.today()
        week_ago = today_date - timedelta(days=7)
        _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date)
        if consumption_days:
            avg_daily = sum(d["spend"] for d in consumption_days) / len(consumption_days)
            if avg_daily > 0:
                days_remaining = int(balance / avg_daily)
                runway_date = today_date + timedelta(days=days_remaining)
                msg += f"\n📊 <b>Balance Runway: ~{days_remaining} days</b> (till ~{runway_date.strftime('%d %b')})"
                msg += f"\n  (Avg daily spend: ₹{avg_daily:.0f}, last {len(consumption_days)}d)"

    msg += f"\n\nDuration: {duration:.1f}s"
    return msg


def _attribute_daily_cost(readings_20min, rate_card):
    """Split today's cost into coarse buckets by integrating power over
    20-min intervals and pricing each interval using its own source.

    At 20-min granularity, cost attribution cannot identify specific
    appliances reliably. This function deliberately uses only three
    buckets to stay honest:

      - baseline: power in [0, MAJOR_LOAD_FLOOR_KW] — fridge + small loads
      - major:    power >= MAJOR_LOAD_FLOOR_KW — ACs / geysers, not
                  separated (a ~2 kW delta could be either at this
                  granularity).
      - other:    gaps (interval > 30 min or either endpoint NULL).

    Pricing:
      - Use apparent_power_kva when available (tariff is ₹/kVAh).
      - Fall back to active_power_kw when apparent is NULL (approximates
        power_factor 1.0, under-estimating cost by ~5%).
      - Select per-interval rate from `source`: "Generator" → dg_rate
        (when set), else eb_rate. Gaps go into 'other' priced similarly.

    Returns:
        {
          "baseline": float (INR),
          "major":    float,
          "other":    float,
          "total_attributed": float,
          "confidence": "medium" | "low",   # "low" if other/total > 0.25
        }

    Returns all zeros (confidence="medium") if `readings_20min` is empty,
    `rate_card` is None/empty, or neither eb_rate nor dg_rate is set.
    """
    zeros = {
        "baseline": 0.0,
        "major": 0.0,
        "other": 0.0,
        "total_attributed": 0.0,
        "confidence": "medium",
    }
    if not readings_20min or not rate_card:
        return zeros

    eb = rate_card.get("eb_rate")
    dg = rate_card.get("dg_rate")
    if not eb and not dg:
        return zeros
    eb = float(eb) if eb else None
    dg = float(dg) if dg else None

    rows = sorted(readings_20min, key=lambda r: r["recorded_at"])

    baseline = 0.0
    major = 0.0
    other = 0.0

    for i in range(len(rows) - 1):
        a = rows[i]
        b = rows[i + 1]
        dt_h = (b["recorded_at"] - a["recorded_at"]).total_seconds() / 3600.0

        # Rate selection per interval — use start-of-interval source.
        src = a.get("source")
        if src == "Generator" and dg:
            rate = dg
        elif eb:
            rate = eb
        else:
            rate = dg  # only dg set, no eb — price everything at dg

        # Apparent power matches the ₹/kVAh tariff; active is a fallback
        # when apparent wasn't reported (approximates PF=1.0).
        a_pow = a.get("apparent_power_kva")
        if a_pow is None:
            a_pow = a.get("active_power_kw")
        b_pow = b.get("apparent_power_kva")
        if b_pow is None:
            b_pow = b.get("active_power_kw")

        # Gap / NULL → attribute entire interval to "other" at the
        # interval's source-aware rate.
        if dt_h > 0.5 or a_pow is None or b_pow is None:
            # Use a nominal 0.3 kVA load (baseline-ish) when either
            # endpoint's power is unknown; for pure time-gap intervals
            # where both endpoints have power, use endpoint-a's value.
            if a_pow is None and b_pow is None:
                nom = 0.3
            elif a_pow is None:
                nom = float(b_pow)
            else:
                nom = float(a_pow)
            other += nom * dt_h * rate
            continue

        kvah = float(a_pow) * dt_h
        cost = kvah * rate

        # Load-type bucket uses active_power_kw (the threshold is about
        # load type, not billing). Fall back to apparent if active is
        # missing.
        load_kw = a.get("active_power_kw")
        if load_kw is None:
            load_kw = a.get("apparent_power_kva") or 0.0
        if float(load_kw) < MAJOR_LOAD_FLOOR_KW:
            baseline += cost
        else:
            major += cost

    total = baseline + major + other
    if total == 0:
        confidence = "medium"
    elif other / total > 0.25:
        confidence = "low"
    else:
        confidence = "medium"

    return {
        "baseline": baseline,
        "major": major,
        "other": other,
        "total_attributed": total,
        "confidence": confidence,
    }


def build_evening_message(balance, current_month, today, duration, last_sync=None, daily_readings=None, rate_card=None, readings_20min=None):
    """Build the evening Telegram message with today's spending and balance runway"""
    td = today
    rc = rate_card or {}
    cm_total = current_month.get("total")
    day_of_month = date.today().day
    now_ist = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")

    msg = (
        "🌙 <b>Energy Monitor — Evening Report</b>\n"
        f"📅 {now_ist}\n"
        f"🔄 Last Sync: {last_sync or '—'}\n\n"
        f"💳 Recharge Left: <b>₹{balance or '—'}</b>\n"
    )
    msg += _budget_line(current_month, rc)
    # Show fix charge from rate card if not yet deducted (report at 10 PM, fix charge at ~11 PM)
    fix_display = td.get('fix_charge')
    if not fix_display and rc.get("fix_charge"):
        fix_display = f"~{rc['fix_charge']}"
        pending_fix = True
    else:
        pending_fix = False

    # Adjust total to include pending fix charge
    total_display = td.get('total')
    if pending_fix and total_display and rc.get("fix_charge"):
        total_display = total_display + Decimal(str(rc["fix_charge"]))
        total_display = f"~{total_display}"

    msg += (
        "\n<b>Today's Deductions</b>\n"
        f"  Total: ₹{total_display or '—'}\n"
        f"  EB: ₹{td.get('eb') or '—'}\n"
        f"  DG: ₹{td.get('dg') or '—'}{_dg_premium_str(td.get('dg'), rc)}\n"
        f"  Fix Charge: ₹{fix_display or '—'}\n"
    )

    # Cost breakdown — coarse buckets; gracefully no-op if no readings or rates.
    if readings_20min and rc:
        attrib = _attribute_daily_cost(readings_20min, rc)
        if attrib["total_attributed"] > 0:
            msg += (
                "\n<b>Cost Breakdown (approx)</b>\n"
                f"  🏠 Baseline: ₹{attrib['baseline']:.0f}\n"
                f"  🌡️ Major loads: ₹{attrib['major']:.0f}  "
                f"<i>(ACs + geysers)</i>\n"
                f"  ❓ Other / gaps: ₹{attrib['other']:.0f}\n"
            )
            if attrib["confidence"] == "low":
                msg += "  <i>(low confidence — significant snapshot gaps today)</i>\n"

    # Balance runway using last 7 days of actual daily spends
    if balance and balance > 0 and daily_readings:
        today_date = date.today()
        week_ago = today_date - timedelta(days=7)
        _, consumption_days = _build_daily_spends(daily_readings, week_ago, today_date)
        if consumption_days:
            avg_daily = sum(d["spend"] for d in consumption_days) / len(consumption_days)
            if avg_daily > 0:
                days_remaining = int(balance / avg_daily)
                runway_date = today_date + timedelta(days=days_remaining)
                msg += f"\n📊 <b>Balance Runway: ~{days_remaining} days</b> (till ~{runway_date.strftime('%d %b')})"
                msg += f"\n  (Avg daily spend: ₹{avg_daily:.0f}, last {len(consumption_days)}d)"

    # Week-over-week comparison (Feature 4)
    if daily_readings:
        wow_line = _build_wow_line(daily_readings)
        if wow_line:
            msg += wow_line

    msg += f"\n\nDuration: {duration:.1f}s"
    return msg


def _source_display(source_text):
    """Convert portal source changeover text to user-friendly display."""
    if not source_text or not source_text.strip():
        return None, None  # (display_text, is_dg)
    s = source_text.strip()
    if s.lower() in ("full load", "eb", "grid"):
        return "EB (Grid)", False
    else:
        return f"DG ({s})", True


def _appliance_hint(active_power_kw):
    """Describe likely appliance load for current power reading.

    Uses the known appliance inventory (see scraper/appliances.py).
    Returns a short parenthetical suffix for report strings. Language
    stays generic because at 20-min granularity we cannot reliably
    distinguish one major load from another.
    """
    if active_power_kw is None:
        return ""
    kw = float(active_power_kw) - BASELINE_KW
    if kw < 0.2:
        return " (baseline — fridge, lights, router)"
    if kw >= 3.8:
        return " (two ACs + geyser, or 3+ major loads)"
    if kw >= 3.4:
        return " (two major loads running)"
    if kw >= 1.8:
        return " (one AC or geyser heating)"
    if kw >= 1.4:
        return " (likely one AC)"
    return " (small appliance)"


def _project_daily_spend(today_deductions, rate_card):
    """Extrapolate today's full-day spend from partial data.

    Returns (partial_spend, projected_full_day) or (None, None).
    """
    partial = today_deductions.get("total")
    if not partial or partial <= 0:
        return None, None

    now_ist = datetime.now(IST)
    fraction = (now_ist.hour * 60 + now_ist.minute) / (24 * 60)
    if fraction < 0.1:
        return None, None

    rc = rate_card or {}
    fix = Decimal(str(rc.get("fix_charge") or 0))
    fix_in_partial = today_deductions.get("fix_charge") or Decimal("0")
    eb_dg_partial = partial - fix_in_partial
    projected_eb_dg = eb_dg_partial / Decimal(str(fraction))
    projected_full = projected_eb_dg + fix

    return partial, projected_full


def build_afternoon_message(balance, current_month, today, duration, last_sync=None,
                             daily_readings=None, rate_card=None, electrical_params=None):
    """Build the afternoon check-in Telegram message. Short and actionable."""
    import calendar
    rc = rate_card or {}
    ep = electrical_params or {}
    now_ist = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")

    msg = (
        "☀️ <b>Energy Monitor — Afternoon Check-in</b>\n"
        f"📅 {now_ist}\n"
        f"🔄 Last Sync: {last_sync or '—'}\n\n"
    )

    # 1. Current power draw + appliance hint
    active_kw = ep.get("active_power_kw")
    if active_kw is not None and active_kw > 0:
        hint = _appliance_hint(active_kw)
        msg += f"⚡ Live Draw: <b>{float(active_kw):.1f} kW</b>{hint}\n"
    elif active_kw is not None:
        msg += "⚡ Live Draw: 0 kW (meter idle)\n"
    else:
        msg += "⚡ Live Draw: — (meter not synced)\n"

    # 2. DG/EB source status
    source_text = ep.get("source")
    source_display, is_dg = _source_display(source_text)
    if source_display:
        if is_dg:
            dg_rate = rc.get("dg_rate")
            eb_rate = rc.get("eb_rate")
            premium_note = ""
            if dg_rate and eb_rate:
                premium_note = f" — ₹{dg_rate}/kVAh vs EB ₹{eb_rate}/kVAh"
            msg += f"🔴 Source: <b>{source_display}</b>{premium_note}\n"
            msg += "  💡 <i>Consider deferring heavy appliances until EB returns</i>\n"
        else:
            msg += f"🟢 Source: {source_display}\n"
    msg += "\n"

    # 3. Balance + today's spend so far + projection
    msg += f"💳 Balance: <b>₹{balance or '—'}</b>\n"
    partial, projected = _project_daily_spend(today, rc)
    if partial:
        msg += f"📊 Today so far: ₹{partial:.0f}"
        if projected:
            msg += f" → projected ₹{projected:.0f} by EOD"
        msg += "\n"
    else:
        msg += f"📊 Today so far: ₹{today.get('total') or '—'}\n"

    # 4. Budget pace — one line
    budget = CONFIG["MONTHLY_BUDGET"]
    cm_total = current_month.get("total")
    day_of_month = date.today().day
    days_in_month = calendar.monthrange(date.today().year, date.today().month)[1]
    if cm_total and cm_total > 0 and budget > 0:
        pct = (cm_total / budget) * 100
        day_pct = (day_of_month / days_in_month) * 100
        pace = "ahead" if float(pct) > float(day_pct) + 5 else "behind" if float(pct) < float(day_pct) - 5 else "on track"
        icon = "🟢" if pace != "ahead" else "🟠"
        msg += f"{icon} Day {day_of_month} of {days_in_month}: ₹{cm_total:.0f} of ₹{budget} ({pct:.0f}%) — {pace}\n"

    # 5. Spike alert — today partial already exceeds yesterday's full total
    if daily_readings and partial:
        yesterday = date.today() - timedelta(days=1)
        _, yesterday_spends = _build_daily_spends(daily_readings, yesterday, yesterday)
        if yesterday_spends:
            yesterday_total = yesterday_spends[0]["spend"]
            if yesterday_total > 0 and partial > yesterday_total:
                pct_over = ((partial - yesterday_total) / yesterday_total) * 100
                msg += f"\n🚨 <b>Already ₹{partial:.0f} today — exceeds yesterday's ₹{yesterday_total:.0f} (+{pct_over:.0f}%)</b>\n"

    msg += f"\nDuration: {duration:.1f}s"
    return msg


# =============================================================================
# Phase 2: snapshot mode, edge-triggered alert engine, load profile chart
# =============================================================================
#
# Tunables for the alert engine. Centralized here so they're easy to adjust
# once we have a few weeks of real data and can calibrate thresholds.

HIGH_POWER_COOLDOWN_HOURS = 1       # Don't re-fire high_power within this window

SUSTAINED_LOAD_MIN_SAMPLES = 5       # of 6 expected samples in 2h window (20-min cadence; 10-of-12 was the 10-min-cadence era)
SUSTAINED_LOAD_COOLDOWN_HOURS = 4    # Longer cooldown — AC can run hours
# Require the earliest valid sample in the window to cover at least this
# much of the 2h span. Without this, 10 samples clustered in the last 100
# min would trigger "sustained 2+ hours" on false premises.
SUSTAINED_LOAD_MIN_SPAN_MINUTES = 105

NIGHT_ANOMALY_COOLDOWN_HOURS = 2

# Sanity cap — the scraped portal can glitch and return implausibly high
# values (e.g. 9999 kW). Reject anything above this in the alert engine so
# one bad sample doesn't fire every alert and latch sustained_load.
MAX_PLAUSIBLE_POWER_KW = 50.0

STALE_SYNC_WARN_MINUTES = 20          # Log a WARN if portal's last_sync is older than this
RECHARGE_JUMP_THRESHOLD = 500         # Balance jump in a single 20-min window ≥ this triggers save_recharge

# Sync-stall alert: the meter occasionally loses comms with the vendor portal
# and `last_sync` freezes for hours (20h stall observed on 2026-04-16). While
# frozen, snapshot runs would otherwise store duplicate rows with stuck values
# and mislead the alert engine. We detect stall by comparing the current
# `last_sync` to the previous reading's; fields get NULLed in storage and an
# escalating Telegram alert fires on exponential backoff.
SYNC_STALL_ALERT_MINUTES = 60
SYNC_STALL_BACKOFF_HOURS = [2, 4, 8, 16, 32]  # minimum gap since last fire; last value repeats on saturation

# Smart Recharge Advisor: skip the advisory message if the user topped up
# within this many days (queried from the `recharges` table). Covers the
# "just recharged, don't nag" case without depending on in-memory state.
_ADVISOR_POST_RECHARGE_DAYS = 2


def parse_last_sync(raw):
    """Parse the portal's last_sync string (e.g. '16-04-2026 09:21:27') to
    an IST-aware datetime. Returns None on failure.

    Kept separate from scrape_meter_data so callers can decide whether to
    save the parsed value, use it for staleness checks, or just display the
    raw string.
    """
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw.strip(), "%d-%m-%Y %H:%M:%S")
        return dt.replace(tzinfo=IST)
    except (ValueError, TypeError):
        return None


def _humanize_duration(td):
    """Render a timedelta as '1d 4h', '3h 12m', or '45m'. Used in stall
    alert messages; precision beyond minutes isn't useful for a 60min+
    alert threshold."""
    total = int(td.total_seconds())
    if total < 0:
        total = 0
    days = total // 86400
    hours = (total % 86400) // 3600
    mins = (total % 3600) // 60
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins and not days:
        parts.append(f"{mins}m")
    return " ".join(parts) if parts else "0m"


def _detect_sync_stall(last_sync_dt, prev_reading):
    """Return True when the portal's meter sync time hasn't advanced since
    the previous snapshot. Treats the current reading as stuck repeat data.

    Returns False when we can't determine (no parseable last_sync, no prev
    reading, or prev.last_sync is NULL) — callers then save the row as-is.
    """
    if last_sync_dt is None or prev_reading is None:
        return False
    prev_sync = prev_reading.get("last_sync")
    if prev_sync is None:
        return False
    return last_sync_dt == prev_sync


def _check_sync_stall_alerts(last_sync_dt, now, prev_reading, balance):
    """Fire exponential-backoff stall alerts and resume notifications.

    Stall (last_sync unchanged from prev AND staleness ≥ threshold):
      - First detection: fire, set alert_state with fire_count=1, stuck_since,
        balance_at_stall.
      - Re-fire only after SYNC_STALL_BACKOFF_HOURS[fire_count-1] has elapsed
        since last_fired_at. Ladder caps at the last entry (32h by default).

    Resume (previously stuck state exists AND sync has advanced):
      - Send resume Telegram with duration + balance delta.
      - Clear alert_state so the next stall fires a fresh first-alert.

    No-op when last_sync is unparseable or when we have no prev reading.
    """
    is_stalled = _detect_sync_stall(last_sync_dt, prev_reading)
    state = get_alert_state("sync_stuck")

    if state and not is_stalled:
        # First-run / cold-start guard: if we have no prior reading to compare
        # against, we can't actually confirm the sync advanced. Orphaned
        # sync_stuck state from a prior deployment (or manual test) would
        # otherwise fire a spurious resume with garbage context. Silently
        # clear and return — the next snapshot with a proper prev_reading
        # will correctly classify.
        if prev_reading is None or last_sync_dt is None:
            clear_alert_state("sync_stuck")
            logger.info("sync_stuck state cleared without alert (no prev reading to confirm resume)")
            return

        ctx = state.get("context") or {}
        stuck_since_iso = ctx.get("stuck_since")
        balance_at_stall = ctx.get("balance_at_stall")

        stuck_since_dt = None
        if stuck_since_iso:
            try:
                stuck_since_dt = datetime.fromisoformat(stuck_since_iso)
            except ValueError:
                stuck_since_dt = None

        duration_str = _humanize_duration(now - stuck_since_dt) if stuck_since_dt else "unknown"
        stuck_since_str = stuck_since_dt.strftime("%d %b %H:%M") if stuck_since_dt else "unknown"

        delta_line = ""
        if balance_at_stall is not None and balance is not None:
            try:
                prev_bal = Decimal(str(balance_at_stall))
                curr_bal = Decimal(str(balance))
                delta = curr_bal - prev_bal
                sign = "+" if delta >= 0 else ""
                delta_line = (
                    f"Balance: ₹{prev_bal:.2f} → ₹{curr_bal:.2f} "
                    f"(Δ {sign}₹{delta:.2f})\n"
                )
            except (InvalidOperation, ValueError):
                delta_line = ""

        msg = (
            "✅ <b>Meter sync resumed</b>\n\n"
            f"Stuck since: {stuck_since_str} IST\n"
            f"Blind window: {duration_str}\n"
            f"{delta_line}"
        )
        # Only clear alert_state when the resume notification actually made it
        # to Telegram. If send fails (network, 429, outage), leave state intact
        # so the next snapshot retries the resume. The backoff ladder keeps
        # the retry cost bounded.
        if send_telegram_message(msg):
            clear_alert_state("sync_stuck")
            logger.info(f"sync_stuck resumed after {duration_str}")
        else:
            logger.warning("sync_stuck resume Telegram failed; leaving state for retry")
        return

    if not is_stalled or last_sync_dt is None:
        return

    staleness = now - last_sync_dt
    if staleness < timedelta(minutes=SYNC_STALL_ALERT_MINUTES):
        return

    ctx = (state.get("context") if state else None) or {}
    fire_count = int(ctx.get("fire_count", 0))

    if state:
        idx = min(max(fire_count - 1, 0), len(SYNC_STALL_BACKOFF_HOURS) - 1)
        required_gap = timedelta(hours=SYNC_STALL_BACKOFF_HOURS[idx])
        if (now - state["last_fired_at"]) < required_gap:
            return

    new_fire_count = fire_count + 1
    next_idx = min(new_fire_count - 1, len(SYNC_STALL_BACKOFF_HOURS) - 1)
    next_gap_h = SYNC_STALL_BACKOFF_HOURS[next_idx]

    stuck_since_iso = ctx.get("stuck_since") or last_sync_dt.isoformat()
    balance_at_stall = ctx.get("balance_at_stall")
    if balance_at_stall is None and prev_reading is not None:
        prev_bal = prev_reading.get("balance")
        if prev_bal is not None:
            balance_at_stall = str(prev_bal)

    msg = (
        "⚠️ <b>Meter sync frozen</b>\n\n"
        f"Last update: {last_sync_dt.strftime('%d %b %H:%M')} IST\n"
        f"Stale for: {_humanize_duration(staleness)}\n"
        f"Notification #{new_fire_count} · next check in {next_gap_h}h"
    )
    send_telegram_message(msg)

    set_alert_state("sync_stuck", now, {
        "fire_count": new_fire_count,
        "stuck_since": stuck_since_iso,
        "balance_at_stall": balance_at_stall,
    })
    logger.warning(f"sync_stuck alert #{new_fire_count} fired (stale {_humanize_duration(staleness)})")


def _run_alert_engine(current_reading, now):
    """Run all 3 edge-triggered alert checks on the current reading.

    Each check:
      1. Skips if active_power_kw is NULL (can't evaluate unknown power).
      2. Skips if its own cooldown is still active (alert already fired
         recently for this condition).
      3. Fires only on a false→true *transition* — checks the previous
         reading's value and only alerts if that was below the threshold.
         `load_previous_reading` enforces a `max_age` guard (25 min by
         default) so a missed cron slot doesn't silently suppress an
         alert by pinning us to a stale "previous" reading.
      4. On fire: sends a Telegram message AND records the cooldown in
         alert_state so we won't re-fire for the next N hours.

    `current_reading` is a dict (same shape as load_readings rows):
        {recorded_at, last_sync, active_power_kw, ..., balance}

    `now` is the current timezone-aware datetime — passed in so test code
    can freeze time without monkey-patching.
    """
    check_high_power_alert(current_reading, now)
    check_sustained_load_alert(current_reading, now)
    check_night_anomaly_alert(current_reading, now)


def check_high_power_alert(current, now):
    """Fire when power crosses HIGH_POWER_KW_THRESHOLD from below."""
    power = current.get("active_power_kw")
    if power is None or power < HIGH_POWER_KW_THRESHOLD or power > MAX_PLAUSIBLE_POWER_KW:
        return

    # Cooldown: have we already fired within the last hour?
    state = get_alert_state("high_power")
    if state and (now - state["last_fired_at"]) < timedelta(hours=HIGH_POWER_COOLDOWN_HOURS):
        return

    # Transition check — was the PREVIOUS reading also above threshold?
    # If yes, this isn't a new edge; condition has been sustained and we
    # should wait for the cooldown to handle spam. If no (or no prev within
    # max_age), this is a fresh edge and we fire.
    #
    # IMPORTANT: use `is not None` explicitly. Truthy checks treat `0.0`
    # (a valid low-draw reading) as falsy, and also conflate NULL (unknown
    # power — we cannot determine if threshold was exceeded) with "below
    # threshold". NULL prev should NOT suppress the alert; treating NULL as
    # "fresh edge" is the safer choice when we lack information.
    prev = load_previous_reading(current["recorded_at"])
    prev_power = prev.get("active_power_kw") if prev else None
    if prev_power is not None and float(prev_power) >= HIGH_POWER_KW_THRESHOLD:
        return

    msg = f"⚡ Heavy power draw: {float(power):.1f} kW"
    send_telegram_message(msg)
    set_alert_state("high_power", now, {"power_kw": float(power)})


def check_sustained_load_alert(current, now):
    """Fire when power has been ≥ SUSTAINED_LOAD_KW for 2+ hours continuously.

    Edge-triggered: once fired, will NOT re-fire until the condition
    actually breaks (power drops below threshold in at least one valid
    reading). A simple cooldown alone would re-fire every 4h during the
    same uninterrupted AC session, which is spam.

    Validity rules:
      - At least SUSTAINED_LOAD_MIN_SAMPLES valid (non-NULL) samples in the
        2h window. NULLs are gaps; we require enough data to be confident.
      - Valid samples must span at least SUSTAINED_LOAD_MIN_SPAN_MINUTES
        of the 2h window — 10 samples all in the last 100 min isn't enough
        to claim "sustained 2 hours".
      - All valid samples must be ≥ threshold AND ≤ MAX_PLAUSIBLE_POWER_KW
        (a single glitch reading doesn't latch us in an alert state).
    """
    power = current.get("active_power_kw")
    if power is None or power < SUSTAINED_LOAD_KW or power > MAX_PLAUSIBLE_POWER_KW:
        return

    window_start = now - timedelta(hours=2)
    window = load_readings(window_start, now + timedelta(seconds=1))
    valid = [r for r in window if r.get("active_power_kw") is not None]
    if len(valid) < SUSTAINED_LOAD_MIN_SAMPLES:
        # First 2h after deploy (or after a DB wipe) — silently warm up.
        logger.info(f"sustained_load: only {len(valid)}/{SUSTAINED_LOAD_MIN_SAMPLES} samples, skipping check")
        return

    # Span check — earliest valid sample must cover most of the 2h window.
    earliest = valid[0]["recorded_at"]
    span_minutes = (now - earliest).total_seconds() / 60
    if span_minutes < SUSTAINED_LOAD_MIN_SPAN_MINUTES:
        logger.info(f"sustained_load: valid samples only span {span_minutes:.0f} min, skipping")
        return

    # All valid samples must be at or above threshold — and also sane
    # (reject single glitchy samples that latch us in alert state).
    for r in valid:
        p = float(r["active_power_kw"])
        if p < SUSTAINED_LOAD_KW or p > MAX_PLAUSIBLE_POWER_KW:
            return

    # Edge-triggered guard: if alert_state has a recent fire AND the window
    # has been continuously above threshold since then, this is the SAME
    # session — don't re-fire until the condition breaks.
    state = get_alert_state("sustained_load")
    if state:
        fired_at = state["last_fired_at"]
        # If any valid sample between fired_at and now dropped below
        # threshold, the condition broke — we can fire again. (The loop
        # above already verified all are >= threshold, so if the earliest
        # sample is after fired_at, the condition has been continuously
        # true since the last fire.)
        if earliest >= fired_at - timedelta(minutes=15):
            return  # Same session, don't re-fire.
        # Additional cooldown safety net — even if the condition broke and
        # re-triggered quickly, enforce min gap between alerts.
        if (now - fired_at) < timedelta(hours=SUSTAINED_LOAD_COOLDOWN_HOURS):
            return

    avg = sum(float(r["active_power_kw"]) for r in valid) / len(valid)
    msg = f"⏱ Sustained load > {SUSTAINED_LOAD_KW} kW for 2+ hours (avg {avg:.1f} kW)"
    send_telegram_message(msg)
    set_alert_state("sustained_load", now, {"avg_kw": avg, "samples": len(valid)})


def check_night_anomaly_alert(current, now):
    """Fire if power > NIGHT_ANOMALY_KW between 00:00 and 05:00 IST.

    Useful for catching "AC left on overnight" or "something unexpected
    running while everyone's asleep". Edge-triggered + cooldown-guarded
    to avoid spamming across a sustained late-night AC session.
    """
    ts_ist = current["recorded_at"].astimezone(IST) if current.get("recorded_at") else now.astimezone(IST)
    if not (0 <= ts_ist.hour < 5):
        return

    power = current.get("active_power_kw")
    if power is None or power < NIGHT_ANOMALY_KW or power > MAX_PLAUSIBLE_POWER_KW:
        return

    state = get_alert_state("night_anomaly")
    if state and (now - state["last_fired_at"]) < timedelta(hours=NIGHT_ANOMALY_COOLDOWN_HOURS):
        return

    # Transition check — same rationale as high_power. Use `is not None`
    # explicitly so 0.0 (valid low-draw) isn't conflated with NULL (unknown).
    prev = load_previous_reading(current["recorded_at"])
    prev_power = prev.get("active_power_kw") if prev else None
    if prev_power is not None and float(prev_power) >= NIGHT_ANOMALY_KW:
        return

    msg = f"🌙 Unusual night draw: {float(power):.1f} kW at {ts_ist:%H:%M}"
    send_telegram_message(msg)
    set_alert_state("night_anomaly", now, {"power_kw": float(power)})


def _snapshot_recharge_detect(prev_reading, current_balance, now, max_age=timedelta(hours=6)):
    """Detect a mid-day recharge by comparing this snapshot's balance to
    the previous reading's balance. If the jump is ≥ RECHARGE_JUMP_THRESHOLD,
    call save_recharge() (which handles its own dedup against portal data).

    Runs only in snapshot mode — the 3x/day report runs already detect
    balance-jump recharges from daily_readings, but at 20-min granularity
    we can catch them near-real-time and surface the balance change in any
    alert context that might need it.

    ``max_age`` bounds how old ``prev_reading`` can be before we skip
    detection. The caller loads a 24h lookback for stall detection and
    shares that reading here; we re-apply a tighter 6h bound so a
    post-outage first run doesn't misattribute a multi-day gap to an
    instantaneous recharge. Portal-side authoritative recharges will still
    be merged via ``_process_portal_recharges``.
    """
    if prev_reading is None or current_balance is None:
        return
    prev_balance = prev_reading.get("balance")
    if prev_balance is None:
        return

    prev_ts = prev_reading.get("recorded_at")
    if prev_ts is not None and (now - prev_ts) > max_age:
        return

    jump = float(current_balance) - float(prev_balance)
    if jump < RECHARGE_JUMP_THRESHOLD:
        return

    # save_recharge handles ±₹1 tolerance dedup and portal cross-matching
    # (±2 days). It's safe to call even if this recharge was already
    # detected from daily-level data.
    save_recharge(now.date(), jump, float(prev_balance), float(current_balance))


def _process_portal_recharges(portal_recharges, balance, daily_readings, now=None):
    """Detect new portal recharges vs stored snapshot, send the analysis
    Telegram (with recharge-table and effectiveness images), then merge +
    dedup into the unified ``recharges`` table and refresh the portal
    snapshot.

    Runs at snapshot cadence so a recharge appears in Telegram within one
    20-min window of the vendor publishing it. No mode gating — the first
    run after vendor-publish wins the atomic claim and notifies;
    subsequent runs see ``stored == current`` and no-op.

    Concurrency: Two overlapping GHA runs can both observe the same
    ``stored`` and ``detect_new_recharges`` result. Atomic de-duplication
    is handled by ``claim_portal_recharge_notification`` which races a
    ``RETURNING id`` INSERT on the ``recharges`` UNIQUE (date, amount)
    constraint — only the winner sends the notification.
    """
    if not portal_recharges:
        return

    stored = load_portal_recharges()
    new_recharges = detect_new_recharges(portal_recharges, stored)

    if new_recharges:
        newest = new_recharges[0]
        if claim_portal_recharge_notification(newest):
            effectiveness = _compute_recharge_effectiveness(portal_recharges, daily_readings, now=now)

            analysis_msg = build_recharge_analysis(
                new_recharges, portal_recharges, balance, daily_readings,
                effectiveness=effectiveness, now=now,
            )
            send_telegram_message(analysis_msg)

            table_intervals = []
            for i in range(len(portal_recharges) - 1):
                d1 = portal_recharges[i]["date"] if isinstance(portal_recharges[i]["date"], date) else datetime.strptime(portal_recharges[i]["date"], "%Y-%m-%d").date()
                d2 = portal_recharges[i + 1]["date"] if isinstance(portal_recharges[i + 1]["date"], date) else datetime.strptime(portal_recharges[i + 1]["date"], "%Y-%m-%d").date()
                table_intervals.append((d1 - d2).days)

            recharge_img = _build_recharge_table_image(portal_recharges, table_intervals)
            if recharge_img:
                send_telegram_photo(recharge_img)
            effectiveness_img = _build_recharge_intervals_image(effectiveness)
            if effectiveness_img:
                send_telegram_photo(effectiveness_img)
        else:
            # Another concurrent runner already claimed the notification —
            # still refresh local state so we stay in sync, then exit.
            logger.info(
                f"portal recharge on {newest.get('date')} already claimed by another run; skipping notification"
            )

    merge_portal_recharges_to_history(portal_recharges)
    cleanup_duplicate_recharges()
    save_portal_recharges(portal_recharges)


def _build_load_profile_image(today_readings_ist):
    """Build the 24h power profile chart for the evening report.

    Filters out rows with NULL active_power_kw (gaps, not zeros) and
    returns None if there aren't enough valid samples to draw a
    meaningful chart.
    """
    MIN_SAMPLES_FOR_CHART = 6  # ~1 hour of 20-min data; arbitrary but avoids empty charts

    pts = [(r["recorded_at"], float(r["active_power_kw"]))
           for r in today_readings_ist
           if r.get("active_power_kw") is not None]

    if len(pts) < MIN_SAMPLES_FOR_CHART:
        logger.info(f"load profile chart: only {len(pts)} valid samples, skipping")
        return None

    timestamps = [t for t, _ in pts]
    values = [v for _, v in pts]

    peak_idx = values.index(max(values))
    low_idx = values.index(min(values))
    peak_kw = values[peak_idx]
    low_kw = values[low_idx]
    avg_kw = sum(values) / len(values)

    subtitle = f"Peak {peak_kw:.1f} kW · Low {low_kw:.1f} kW · Avg {avg_kw:.1f} kW"
    return render_time_profile_chart(
        title="Today's Power Profile",
        subtitle=subtitle,
        timestamps=timestamps,
        values=values,
        peak_ts=timestamps[peak_idx],
        low_ts=timestamps[low_idx],
    )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--evening", action="store_true", help="Run evening report mode")
    parser.add_argument("--afternoon", action="store_true", help="Run afternoon check-in mode")
    parser.add_argument("--snapshot", action="store_true",
                        help="High-frequency snapshot mode (20-min cron). Saves a reading, "
                             "runs the alert engine, skips the report flow.")
    parser.add_argument("--weekly", action="store_true", help="Include weekly report")
    parser.add_argument("--monthly", action="store_true", help="Include monthly report")
    args = parser.parse_args()

    # Mode dispatch — CLI flag wins; otherwise fall back to SCRAPER_MODE env
    # (used by GitHub Actions cron routing).
    mode_env = os.getenv("SCRAPER_MODE", "morning")
    if args.snapshot or mode_env == "snapshot":
        mode = "snapshot"
    elif args.evening:
        mode = "evening"
    elif args.afternoon:
        mode = "afternoon"
    else:
        mode = mode_env
    send_weekly = args.weekly or os.getenv("WEEKLY") == "true"
    send_monthly = args.monthly or os.getenv("MONTHLY") == "true"

    start_time = time.time()

    logger.info("=" * 60)
    logger.info(f"Starting Energy Meter Scraper ({mode} mode)")
    logger.info("=" * 60)

    if not (CONFIG["SITE_ID"] and CONFIG["UNIT_ID"] and CONFIG["METER_ID"]):
        logger.error(
            "SMARTGRID_SITE_ID, SMARTGRID_UNIT_ID, and SMARTGRID_METER_ID must be set. "
            "Run scripts/bootstrap_ids.py once to resolve them from tower/flat."
        )
        sys.exit(1)

    # --- Sentry Cron Monitoring -------------------------------------------
    # Tracks that each scheduled mode actually runs on time. If a cron tick
    # is missed (GHA outage, quota exhaustion, runner failure), Sentry alerts
    # before the user has to notice that Telegram went silent.
    # Schedules match .github/workflows/scraper.yml (UTC).
    from sentry_sdk.crons import monitor as sentry_monitor
    _CRON_CONFIGS = {
        "snapshot":  {"schedule": {"type": "crontab", "value": "5,25,45 * * * *"}, "checkin_margin": 10, "max_runtime": 5},
        "morning":   {"schedule": {"type": "crontab", "value": "0 1 * * *"},               "checkin_margin": 10, "max_runtime": 5},
        "afternoon": {"schedule": {"type": "crontab", "value": "0 12 * * *"},              "checkin_margin": 10, "max_runtime": 5},
        "evening":   {"schedule": {"type": "crontab", "value": "30 16 * * *"},             "checkin_margin": 10, "max_runtime": 5},
    }
    _monitor_ctx = sentry_monitor(
        monitor_slug=f"energy-{mode}",
        monitor_config=_CRON_CONFIGS.get(mode),
    )
    _monitor_ctx.__enter__()

    try:
        # Client session is intentionally closed as soon as scrape_meter_data
        # returns. Every downstream call (storage, alerts, Telegram, charts)
        # operates on the materialized 13-tuple — nothing re-fetches. If you
        # add a feature that re-polls an endpoint later in main(), move this
        # `with` block to wrap the wider try-body or you'll hit a closed
        # session.
        with create_client() as client:
            balance, current_month, prev_day, prev_month, monthly_consumption, today, daily_readings, prev_prev_month, last_sync, rate_card, grace_credit, portal_recharges, electrical_params = scrape_meter_data(client)
        duration = time.time() - start_time

        # ------------------------------------------------------------------
        # Phase 2: persist an instantaneous snapshot on EVERY run.
        # The data is already in hand — saving it costs one INSERT. This
        # means reports and snapshots share the same historical record.
        # ------------------------------------------------------------------
        now_ist = datetime.now(IST)
        last_sync_dt = parse_last_sync(last_sync)

        # Detect sync stall BEFORE save by comparing the portal's meter-sync
        # time to the most recent prior reading's. When the meter loses comms
        # with the vendor, `last_sync` freezes for hours (20h observed once)
        # and the portal keeps returning the same stale snapshot. Persisting
        # those as valid rows latches the sustained-load alert, pollutes the
        # 24h power-profile chart, and misattributes balance jumps. The fix:
        # write a heartbeat row but NULL the derived fields when stuck.
        prev_reading = load_previous_reading(now_ist, max_age=timedelta(hours=24))
        is_sync_stalled = _detect_sync_stall(last_sync_dt, prev_reading)

        if is_sync_stalled:
            # Heartbeat row only: recorded_at + last_sync preserved for audit.
            save_reading(now_ist, last_sync_dt, last_sync, None, None)
        else:
            save_reading(now_ist, last_sync_dt, last_sync, electrical_params, balance)

        # Stall alert (exponential backoff) + resume notification.
        _check_sync_stall_alerts(last_sync_dt, now_ist, prev_reading, balance)

        # Staleness log — debug breadcrumb for the shorter 20-min window.
        # Real alerting with cooldown/backoff lives in _check_sync_stall_alerts.
        if last_sync_dt is not None:
            staleness = now_ist - last_sync_dt
            if staleness > timedelta(minutes=STALE_SYNC_WARN_MINUTES):
                logger.warning(f"Portal last_sync is {staleness} old — meter may be offline")

        # Shared in-hand view. During stalls the derived fields are None so
        # the alert engine and recharge-detect helpers correctly treat the
        # reading as "data absent" rather than a valid sample at 0.
        current_power = None if is_sync_stalled else (electrical_params or {}).get("active_power_kw")
        current_balance = None if is_sync_stalled else balance

        # Snapshot mode short-circuit: save the reading, run the alert
        # engine, maybe record a mid-day recharge, process any new portal
        # recharges, then exit without any report flow or daily aggregate
        # churn.
        if mode == "snapshot":
            # Mid-day recharge detection from balance jump. Skipped during
            # stalls — the stuck balance would produce a spurious jump on
            # resume. The portal-recharge path below is authoritative.
            if not is_sync_stalled:
                _snapshot_recharge_detect(prev_reading, balance, now_ist)

            # Portal recharge analysis moved from evening to snapshot: once
            # the vendor publishes a new BindRecharge entry, it surfaces in
            # Telegram within one 20-min window instead of waiting for 22:00.
            # Pass the stall-aware `current_balance` (None during stalls) so
            # the analysis message doesn't quote a stale balance, and a
            # single caller-captured `now_ist` so mid-computation midnight
            # rollovers and UTC-vs-IST drift can't bite.
            _process_portal_recharges(portal_recharges, current_balance, daily_readings, now=now_ist)

            current_reading = {
                "recorded_at": now_ist,
                "active_power_kw": current_power,
                "balance": current_balance,
            }
            _run_alert_engine(current_reading, now_ist)

            duration = time.time() - start_time
            _monitor_ctx.__exit__(None, None, None)
            sentry_sdk.flush(timeout=5)
            logger.info(f"Snapshot finished in {duration:.2f}s")
            return

        # ------------------------------------------------------------------
        # Report modes below (morning / afternoon / evening) keep the
        # existing save-everything + send-reports flow.
        # ------------------------------------------------------------------

        # Save historical data
        if daily_readings:
            save_daily(daily_readings)
            # Detect and save recharges
            for r in extract_recharges(daily_readings):
                save_recharge(r["date"], r["amount"], r["balance_before"], r["balance_after"])
        if rate_card.get("eb_rate") is not None:
            save_rates(rate_card)

        # Save yesterday's cost breakdown (exact values from portal)
        if prev_day.get("eb") is not None:
            yesterday = date.today() - timedelta(days=1)
            save_daily_costs(yesterday, prev_day.get("eb"), prev_day.get("dg"), prev_day.get("fix_charge"))

        # Run the alert engine in report modes too — same data is in hand,
        # running checks here gives us extra coverage at 6:30 / 17:30 / 22:00
        # in case a snapshot run missed the edge. Stall-aware: derived
        # fields are None when sync is frozen.
        current_reading = {
            "recorded_at": now_ist,
            "active_power_kw": current_power,
            "balance": current_balance,
        }
        _run_alert_engine(current_reading, now_ist)

        # Phase 2 evening cost attribution needs today's 20-min readings
        # BEFORE build_evening_message is called. Same list is reused for
        # the 24-hour load-profile chart below — no double-query.
        today_readings = None
        if mode == "evening":
            today_start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
            today_readings = load_readings(today_start_ist, now_ist + timedelta(seconds=1))

        if mode == "evening":
            msg = build_evening_message(balance, current_month, today, duration, last_sync, daily_readings=daily_readings, rate_card=rate_card, readings_20min=today_readings)
        elif mode == "afternoon":
            msg = build_afternoon_message(balance, current_month, today, duration, last_sync, daily_readings=daily_readings, rate_card=rate_card, electrical_params=electrical_params)
        else:
            msg = build_morning_message(balance, current_month, prev_day, prev_month, monthly_consumption, duration, last_sync, rate_card=rate_card, daily_readings=daily_readings)

        send_telegram_message(msg)

        # Evening chart images
        if mode == "evening" and daily_readings:
            trend_img = _build_daily_spend_trend_image(daily_readings, rate_card)
            if trend_img:
                send_telegram_photo(trend_img)

            # Phase 2: 24-hour power profile chart, built from the
            # high-frequency `readings` table populated by snapshot runs.
            # `today_readings` was loaded above (before build_evening_message)
            # so the Cost Breakdown block and this chart share one query.
            profile_img = _build_load_profile_image(today_readings)
            if profile_img:
                send_telegram_photo(profile_img)

        # Anomaly detection (run on every scrape)
        rate_change_msg = check_rate_changes(rate_card)
        if rate_change_msg:
            send_telegram_message(rate_change_msg)

        # Evening-specific anomaly checks (today's data is complete by 10 PM)
        if mode == "evening" and daily_readings:
            spike_msg = check_consumption_spike(today, daily_readings, rate_card)
            if spike_msg:
                send_telegram_message(spike_msg)

            dg_msg = check_dg_usage(today, rate_card, daily_readings)
            if dg_msg:
                send_telegram_message(dg_msg)

            fix_msg = check_fix_charge_anomaly(prev_day, rate_card)
            if fix_msg:
                send_telegram_message(fix_msg)

        # Portal recharge analysis is now handled in snapshot mode so new
        # recharges surface within 20min of the vendor publishing them —
        # see _process_portal_recharges() called from the snapshot branch.

        # Smart Recharge Advisor (evening only). Suppression after a recent
        # top-up now queries the recharges table directly (see
        # build_recharge_advisor) — the old in-memory new_recharges handshake
        # no longer applies once portal recharge processing moved to snapshot
        # mode.
        if mode == "evening" and balance is not None and balance > 0 and daily_readings:
            advisor_msg = build_recharge_advisor(balance, daily_readings, now=now_ist)
            if advisor_msg:
                send_telegram_message(advisor_msg)

        # Spending Trend Alert (Feature 3 — evening only, days 7/14/21/28)
        if mode == "evening" and daily_readings:
            trend_msg = check_spending_trend(daily_readings)
            if trend_msg:
                send_telegram_message(trend_msg)

        # Weekly report (Monday mornings or --weekly flag)
        if send_weekly and daily_readings:
            today_date = date.today()
            # Last week: Monday to Sunday
            last_monday = today_date - timedelta(days=today_date.weekday() + 7)
            last_sunday = last_monday + timedelta(days=6)
            # Week before that (for comparison)
            prev_monday = last_monday - timedelta(days=7)
            prev_sunday = last_monday - timedelta(days=1)

            stats = compute_weekly_stats(daily_readings, last_monday, last_sunday)
            prev_stats = compute_weekly_stats(daily_readings, prev_monday, prev_sunday)

            if stats:
                weekly_msg = build_weekly_message(stats, prev_stats, balance, duration, last_sync, daily_readings=daily_readings)
                send_telegram_message(weekly_msg)
                # Weekly chart images
                two_weeks_ago = today_date - timedelta(days=14)
                _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
                if balance and consumption_days:
                    spend_img = _build_spend_chart_image(consumption_days, balance)
                    if spend_img:
                        send_telegram_photo(spend_img)
                wow_img = _build_week_vs_week_image(daily_readings)
                if wow_img:
                    send_telegram_photo(wow_img)
            else:
                logger.warning("Not enough data for weekly report")

        # Monthly report (1st of month or --monthly flag)
        if send_monthly and daily_readings:
            today_date = date.today()
            # Previous month
            first_of_this_month = today_date.replace(day=1)
            last_month_end = first_of_this_month - timedelta(days=1)
            prev_year, prev_mo = last_month_end.year, last_month_end.month

            m_stats = compute_monthly_stats(daily_readings, prev_year, prev_mo)
            if m_stats:
                save_monthly(m_stats, prev_month)
                monthly_msg = build_monthly_message(m_stats, prev_month, prev_prev_month, monthly_consumption, duration, last_sync, rate_card=rate_card, balance=balance, daily_readings=daily_readings)
                send_telegram_message(monthly_msg)
                # Monthly chart images
                pattern = m_stats.get("weekday_pattern", {})
                if pattern:
                    weekday_img = _build_weekday_chart_image(pattern)
                    if weekday_img:
                        send_telegram_photo(weekday_img)
                bill_img = _build_bill_split_image(prev_month, rate_card, title=f"Bill Composition — {last_month_end.strftime('%b %Y')}")
                if bill_img:
                    send_telegram_photo(bill_img)
                journey_img = _build_balance_journey_image(daily_readings, prev_year, prev_mo)
                if journey_img:
                    send_telegram_photo(journey_img)
                trend_img = _build_monthly_trend_image(monthly_consumption)
                if trend_img:
                    send_telegram_photo(trend_img)
                two_weeks_ago = today_date - timedelta(days=14)
                _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
                if balance and consumption_days:
                    spend_img = _build_spend_chart_image(consumption_days, balance)
                    if spend_img:
                        send_telegram_photo(spend_img)
                appliance_img = _build_appliance_guide_image(rate_card.get("eb_rate"), fix_charge=rate_card.get("fix_charge"))
                if appliance_img:
                    send_telegram_photo(appliance_img)
            else:
                logger.warning("Not enough data for monthly report")

        # Grace period check (balance negative)
        grace_msg = check_grace_period(balance, grace_credit, rate_card)
        if grace_msg:
            send_telegram_message(grace_msg)

        # Smart recharge prediction (replaces static threshold)
        if balance is not None and balance >= 0 and daily_readings:
            prediction = check_recharge_prediction(balance, daily_readings)
            if prediction and prediction["days_remaining"] <= 7:
                send_telegram_message(build_recharge_alert(prediction, balance))

    except Exception as e:
        # psycopg2 / requests exceptions can include the full connection
        # string (DATABASE_URL with password) or the Telegram URL (with bot
        # token) in their str representation. Telegram message history is
        # synced across the user's devices and third-party cloud, so even
        # a "private chat" is not a safe secrets sink. Send ONLY the
        # exception type and a short static hint — full traceback goes to
        # Sentry (private dashboard) where it's scrubbed of secrets.
        logger.error(f"Fatal error: {type(e).__name__}")
        _monitor_ctx.__exit__(type(e), e, e.__traceback__)
        sentry_sdk.capture_exception(e)
        duration = time.time() - start_time
        send_telegram_message(
            "🚨 <b>Energy Scraper — Failed</b>\n\n"
            f"Error type: {type(e).__name__}\n"
            f"Duration: {duration:.1f}s\n"
            f"Check Sentry dashboard for full traceback."
        )
        sentry_sdk.flush(timeout=5)
        sys.exit(1)

    _monitor_ctx.__exit__(None, None, None)
    sentry_sdk.flush(timeout=5)
    logger.info(f"Scraper finished in {duration:.2f} seconds")


if __name__ == "__main__":
    main()
