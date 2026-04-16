#!/usr/bin/env python3
"""
Energy Meter Scraper for Smartgridsoft Portal
Uses direct HTTP requests + BeautifulSoup (no browser needed)
"""

import argparse
import math
import os
import sys
import logging
import time
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from storage import (save_daily, save_daily_costs, save_monthly, save_recharge, save_rates, extract_recharges,
                      save_portal_recharges, load_portal_recharges, detect_new_recharges, merge_portal_recharges_to_history,
                      cleanup_duplicate_recharges, load_daily_readings,
                      # Phase 2: high-frequency readings + edge-triggered alerts
                      save_reading, load_readings, load_previous_reading,
                      get_alert_state, set_alert_state)
from charts import (render_table_image, render_bar_chart, render_spend_chart,
                     render_donut_chart, render_line_chart, render_grouped_bars,
                     render_time_profile_chart)

load_dotenv()
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

IST = timezone(timedelta(hours=5, minutes=30))

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
    "LOGIN_URL": "https://www.smartgridsoft.in/WebsitePages/Login.aspx",
    "METER_URL": "https://www.smartgridsoft.in/WebsitePages/MyMeter.aspx",
    "COMPANY": os.getenv("SMARTGRID_COMPANY", ""),
    "USERNAME": os.getenv("SMARTGRID_USERNAME", ""),
    "PASSWORD": os.getenv("SMARTGRID_PASSWORD", ""),
    "LOW_BALANCE_THRESHOLD": Decimal(os.getenv("LOW_BALANCE_THRESHOLD", "800")),
    "MONTHLY_BUDGET": Decimal(os.getenv("MONTHLY_BUDGET", "8000")),
}

# Map company names to their dropdown option values
COMPANY_IDS = {
    "Olive County": "53",
    "Bharat City": "21",
    "Gulshan Ikebana": "23",
    "Exotica Dreamville": "24",
    "Cherry County": "26",
}


def parse_decimal(value):
    """Parse string to Decimal, handling empty values"""
    if not value or value.strip() in ["", "-"]:
        return None
    try:
        cleaned = value.replace(",", "").strip()
        return Decimal(cleaned)
    except (ValueError, InvalidOperation):
        return None


def create_session():
    """Create an HTTP session with browser-like headers"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": CONFIG["LOGIN_URL"],
    })
    return session


def get_hidden_fields(soup):
    """Extract ASP.NET hidden form fields from a page"""
    return {
        "__VIEWSTATE": soup.find("input", {"name": "__VIEWSTATE"})["value"],
        "__VIEWSTATEGENERATOR": soup.find("input", {"name": "__VIEWSTATEGENERATOR"})["value"],
        "__EVENTVALIDATION": soup.find("input", {"name": "__EVENTVALIDATION"})["value"],
    }


def login(session):
    """Login to Smartgridsoft portal via HTTP POST"""
    logger.info("Logging in...")

    company_id = COMPANY_IDS.get(CONFIG["COMPANY"])
    if not company_id:
        raise ValueError(f"Unknown company: {CONFIG['COMPANY']}. Known: {list(COMPANY_IDS.keys())}")

    # GET login page
    r = session.get(CONFIG["LOGIN_URL"])
    soup = BeautifulSoup(r.text, "html.parser")

    # Step 1: Company dropdown postback (updates __VIEWSTATE and __EVENTVALIDATION)
    payload = get_hidden_fields(soup)
    payload.update({
        "__EVENTTARGET": "ddlsocietyname",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "ddlsocietyname": company_id,
        "txtusername": "",
        "txtpassword": "",
    })
    r2 = session.post(CONFIG["LOGIN_URL"], data=payload)
    soup2 = BeautifulSoup(r2.text, "html.parser")

    # Step 2: Submit login form with credentials
    payload2 = get_hidden_fields(soup2)
    payload2.update({
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "ddlsocietyname": company_id,
        "txtusername": CONFIG["USERNAME"],
        "txtpassword": CONFIG["PASSWORD"],
        "btnsignin": "Sign In",
    })
    r3 = session.post(CONFIG["LOGIN_URL"], data=payload2, allow_redirects=True)

    # Check for login failure (alert script in response)
    if "Login" in r3.url:
        soup3 = BeautifulSoup(r3.text, "html.parser")
        for script in soup3.find_all("script"):
            if "alert" in script.get_text():
                raise ValueError("Login failed: Invalid username or password")
        raise ValueError("Login failed: Still on login page")

    logger.info("Login successful!")
    return session


def scrape_meter_data(session):
    """Fetch MyMeter page and extract all data fields"""
    logger.info("Fetching meter data...")
    r = session.get(CONFIG["METER_URL"])
    soup = BeautifulSoup(r.text, "html.parser")

    def get_field(field_id):
        el = soup.find(id=field_id)
        return parse_decimal(el.get_text(strip=True)) if el else None

    def get_text(field_id):
        el = soup.find(id=field_id)
        return el.get_text(strip=True) if el else None

    # Recharge balance
    balance = get_field("ContentPlaceHolder1_lblbalance")
    logger.debug(f"Recharge balance: ₹{balance}")

    # Last sync time from server
    last_sync = get_text("ContentPlaceHolder1_lbldatetime")
    logger.debug(f"Last sync: {last_sync}")

    # Current month deductions
    current_month = {
        "total": get_field("ContentPlaceHolder1_lbl_cmdtotal"),
        "eb": get_field("ContentPlaceHolder1_lbl_cmdeb"),
        "dg": get_field("ContentPlaceHolder1_lbl_cmddg"),
        "fix_charge": get_field("ContentPlaceHolder1_lbl_cmdfixc"),
    }
    logger.debug(f"Current month deductions: {current_month}")

    # Previous day deductions
    prev_day = {
        "total": get_field("ContentPlaceHolder1_lbl_pddtotal"),
        "eb": get_field("ContentPlaceHolder1_lbl_pddeb"),
        "dg": get_field("ContentPlaceHolder1_lbl_pdddg"),
        "fix_charge": get_field("ContentPlaceHolder1_lbl_pddfixc"),
    }
    logger.debug(f"Previous day deductions: {prev_day}")

    # Previous month deductions
    prev_month = {
        "total": get_field("ContentPlaceHolder1_lbl_pmdtotal"),
        "eb": get_field("ContentPlaceHolder1_lbl_pmdeb"),
        "dg": get_field("ContentPlaceHolder1_lbl_pmddg"),
        "fix_charge": get_field("ContentPlaceHolder1_lbl_pmdfixc"),
    }
    logger.debug(f"Previous month deductions: {prev_month}")

    # Monthly consumption history (last 6 months)
    monthly_consumption = []
    month_fields = [
        ("ContentPlaceHolder1_lblmonth_one", "ContentPlaceHolder1_lblmonth_one_amount"),
        ("ContentPlaceHolder1_lblmonth_two", "ContentPlaceHolder1_lblmonth_two_amount"),
        ("ContentPlaceHolder1_lblmonth_three", "ContentPlaceHolder1_lblmonth_three_amount"),
        ("ContentPlaceHolder1_lblmonth_four", "ContentPlaceHolder1_lblmonth_four_amount"),
        ("ContentPlaceHolder1_lblmonth_five", "ContentPlaceHolder1_lblmonth_five_amount"),
        ("ContentPlaceHolder1_lblmonth_six", "ContentPlaceHolder1_lblmonth_six_amount"),
    ]
    for name_id, amount_id in month_fields:
        name = get_text(name_id)
        amount = get_field(amount_id)
        if name:
            monthly_consumption.append({"month": name, "amount": amount})
    logger.debug(f"Monthly consumption: {monthly_consumption}")

    # Previous-previous month deductions (for month-over-month comparison)
    prev_prev_month = {
        "total": get_field("ContentPlaceHolder1_lbl_ppmdtotal"),
        "eb": get_field("ContentPlaceHolder1_lbl_ppmdeb"),
        "dg": get_field("ContentPlaceHolder1_lbl_ppmddg"),
        "fix_charge": get_field("ContentPlaceHolder1_lbl_ppmdfixc"),
    }
    logger.debug(f"Previous-previous month deductions: {prev_prev_month}")

    # Current day deductions
    today = {
        "total": get_field("ContentPlaceHolder1_lbl_cdd_total"),
        "eb": get_field("ContentPlaceHolder1_lbl_cddeb"),
        "dg": get_field("ContentPlaceHolder1_lbl_cdddg"),
        "fix_charge": get_field("ContentPlaceHolder1_lblcddfixc"),
    }
    logger.debug(f"Current day deductions: {today}")

    # Grace credit limit
    grace_text = get_text("ContentPlaceHolder1_lbl_g_credit")
    grace_credit = parse_decimal(grace_text.replace("INR", "").strip()) if grace_text else None
    logger.debug(f"Grace credit: ₹{grace_credit}")

    # Rate card
    rate_card = {
        "eb_rate": get_field("ContentPlaceHolder1_lbl_arebrate"),
        "dg_rate": get_field("ContentPlaceHolder1_lbl_ardgrate"),
        "fix_charge": get_field("ContentPlaceHolder1_lbl_arfixcrate"),
    }
    logger.debug(f"Rate card: {rate_card}")

    # Daily readings from current + previous month tables (for weekly report)
    daily_readings = scrape_daily_readings(soup)
    logger.info(f"Daily readings: {len(daily_readings)} days scraped")

    # Last 10 recharges from portal
    portal_recharges = scrape_recharges(soup)
    logger.info(f"Portal recharges: {len(portal_recharges)} entries scraped")

    # Electrical parameters (real-time power draw, source changeover)
    electrical_params = scrape_electrical_params(soup)
    logger.debug(f"Electrical params: {electrical_params}")

    return balance, current_month, prev_day, prev_month, monthly_consumption, today, daily_readings, prev_prev_month, last_sync, rate_card, grace_credit, portal_recharges, electrical_params


def parse_date(date_str):
    """Parse DD-MM-YYYY date string"""
    try:
        return datetime.strptime(date_str.strip(), "%d-%m-%Y").date()
    except ValueError:
        return None


def scrape_daily_readings(soup):
    """Parse daily readings from current month (Table19) and previous month (Table20) tables"""
    readings = []

    for table_id in ["Table19", "Table20"]:
        table = soup.find("table", id=table_id)
        if not table:
            continue
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            reading_date = parse_date(cells[0].get_text(strip=True))
            if not reading_date:
                continue
            readings.append({
                "date": reading_date,
                "eb_reading": parse_decimal(cells[1].get_text(strip=True)),
                "eb_consume": parse_decimal(cells[2].get_text(strip=True)),
                "dg_reading": parse_decimal(cells[3].get_text(strip=True)),
                "dg_consume": parse_decimal(cells[4].get_text(strip=True)),
                "balance": parse_decimal(cells[5].get_text(strip=True)),
            })

    # Sort by date ascending (oldest first)
    readings.sort(key=lambda r: r["date"])
    return readings


def scrape_recharges(soup):
    """Parse the 'Last 10 Recharges' table from the portal page."""
    recharges = []
    # Find the title bar containing "Last 10 Recharges"
    for title_div in soup.find_all("div", class_="title-bar"):
        if "Last 10 Recharges" in title_div.get_text():
            box = title_div.find_parent("div", class_="box")
            if not box:
                continue
            table = box.find("table")
            if not table:
                continue
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                amount = parse_decimal(cells[0].get_text(strip=True))
                recharge_date = parse_date(cells[1].get_text(strip=True))
                recharge_type = cells[2].get_text(strip=True)
                if amount and recharge_date:
                    recharges.append({
                        "date": recharge_date,
                        "amount": amount,
                        "type": recharge_type,
                    })
            break
    # Sort by date descending (newest first)
    recharges.sort(key=lambda r: r["date"], reverse=True)
    return recharges


def scrape_electrical_params(soup):
    """Extract real-time electrical parameters and source changeover from portal."""
    def get_field(field_id):
        el = soup.find(id=field_id)
        return parse_decimal(el.get_text(strip=True)) if el else None

    def get_text(field_id):
        el = soup.find(id=field_id)
        return el.get_text(strip=True) if el else None

    return {
        "active_power_kw": get_field("ContentPlaceHolder1_lblActivePower"),
        "apparent_power_kva": get_field("ContentPlaceHolder1_lblApparentPower"),
        "current_amp": get_field("ContentPlaceHolder1_lblCurrent"),
        "voltage_ln": get_field("ContentPlaceHolder1_lblVoltageLN"),
        "voltage_ll": get_field("ContentPlaceHolder1_lblVoltageLL"),
        "power_factor": get_field("ContentPlaceHolder1_lblPF"),
        "frequency_hz": get_field("ContentPlaceHolder1_lblFrequency"),
        "source": get_text("ContentPlaceHolder1_lbl_chgsrc"),
    }


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


def build_recharge_analysis(new_recharges, all_recharges, balance, daily_readings):
    """Build recharge analysis Telegram message when a new recharge is detected."""
    newest = new_recharges[0]
    today_date = date.today()

    msg = (
        "🔋 <b>Recharge Detected!</b>\n\n"
        f"💳 ₹{newest['amount']:,.0f} via {newest.get('type', '—')}\n\n"
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


def build_recharge_advisor(balance, daily_readings, new_recharges):
    """Prescriptive recharge advice: how much to recharge to last until month-end."""
    import calendar

    if new_recharges:
        return None
    if balance is None or balance <= 0:
        return None

    today_date = date.today()
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


def _build_recharge_intervals_image(all_recharges, intervals):
    """Chart 7: How long each recharge lasted as horizontal bars."""
    if not all_recharges or not intervals:
        return None

    labels = []
    values = []
    # Row 0 is newest (ongoing), show intervals[0..N-2] for rows 1..N-1
    for i in range(1, len(all_recharges)):
        if (i - 1) >= len(intervals):
            break
        r = all_recharges[i]
        r_date = r["date"] if isinstance(r["date"], date) else datetime.strptime(r["date"], "%Y-%m-%d").date()
        amt = float(r["amount"])
        amt_label = f"₹{amt/1000:.0f}K" if amt >= 1000 else f"₹{amt:.0f}"
        labels.append(f"{amt_label} {r_date.strftime('%d %b')}")
        values.append(intervals[i - 1])

    if not values:
        return None

    avg_interval = sum(values) / len(values)
    return render_bar_chart(
        "Recharge Intervals",
        f"How long each recharge lasted · Avg {avg_interval:.0f} days",
        labels, values, value_fmt="{:.0f}d", color="#4ecca3",
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
    """Send a photo via Telegram Bot API."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set, skipping photo")
        return

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
        else:
            # Only log status code — resp.text might echo the URL (with the
            # embedded bot token) back; this would leak into public GHA logs.
            logger.warning(f"Telegram photo error: HTTP {resp.status_code}")
    except Exception as e:
        # Exception messages from `requests` often contain the full URL
        # including the bot token. Log only the exception type.
        logger.warning(f"Failed to send Telegram photo: {type(e).__name__}")


def send_telegram_message(text):
    """Send a message via Telegram Bot API"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set, skipping notification")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.ok:
            logger.info("Telegram notification sent")
        else:
            # resp.text may echo the URL (with bot token) — log only status.
            logger.warning(f"Telegram API error: HTTP {resp.status_code}")
    except Exception as e:
        # Exception message may contain the URL with bot token.
        logger.warning(f"Failed to send Telegram message: {type(e).__name__}")


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


def build_evening_message(balance, current_month, today, duration, last_sync=None, daily_readings=None, rate_card=None):
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
    """Return a short appliance hint based on current power draw."""
    if active_power_kw is None:
        return ""
    kw = float(active_power_kw)
    if kw >= 3.0:
        return " (AC + geyser likely on)"
    elif kw >= 2.0:
        return " (AC or geyser may be on)"
    elif kw >= 1.0:
        return " (AC or heavy appliance)"
    elif kw >= 0.3:
        return " (normal — fridge, lights)"
    elif kw > 0:
        return " (standby load)"
    return ""


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

HIGH_POWER_KW_THRESHOLD = 2.5       # Single-reading spike trigger (kW)
HIGH_POWER_COOLDOWN_HOURS = 1       # Don't re-fire high_power within this window

SUSTAINED_LOAD_KW = 2.5              # Sustained-load trigger (kW)
SUSTAINED_LOAD_MIN_SAMPLES = 10      # of 12 expected samples in 2h window
SUSTAINED_LOAD_COOLDOWN_HOURS = 4    # Longer cooldown — AC can run hours
# Require the earliest valid sample in the window to cover at least this
# much of the 2h span. Without this, 10 samples clustered in the last 100
# min would trigger "sustained 2+ hours" on false premises.
SUSTAINED_LOAD_MIN_SPAN_MINUTES = 105

NIGHT_ANOMALY_KW = 1.0                # Threshold between 00:00-05:00 IST
NIGHT_ANOMALY_COOLDOWN_HOURS = 2

# Sanity cap — the scraped portal can glitch and return implausibly high
# values (e.g. 9999 kW). Reject anything above this in the alert engine so
# one bad sample doesn't fire every alert and latch sustained_load.
MAX_PLAUSIBLE_POWER_KW = 50.0

STALE_SYNC_WARN_MINUTES = 20          # Log a WARN if portal's last_sync is older than this
RECHARGE_JUMP_THRESHOLD = 500         # Balance jump in a single 10-min window ≥ this triggers save_recharge


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


def _snapshot_recharge_detect(prev_reading, current_balance, now):
    """Detect a mid-day recharge by comparing this snapshot's balance to
    the previous reading's balance. If the jump is ≥ RECHARGE_JUMP_THRESHOLD,
    call save_recharge() (which handles its own dedup against portal data).

    Runs only in snapshot mode — the 3x/day report runs already detect
    balance-jump recharges from daily_readings, but at 10-min granularity
    we can catch them near-real-time and surface the balance change in any
    alert context that might need it.
    """
    if prev_reading is None or current_balance is None:
        return
    prev_balance = prev_reading.get("balance")
    if prev_balance is None:
        return

    jump = float(current_balance) - float(prev_balance)
    if jump < RECHARGE_JUMP_THRESHOLD:
        return

    # save_recharge handles ±₹1 tolerance dedup and portal cross-matching
    # (±2 days). It's safe to call even if this recharge was already
    # detected from daily-level data.
    save_recharge(now.date(), jump, float(prev_balance), float(current_balance))


def _build_load_profile_image(today_readings_ist):
    """Build the 24h power profile chart for the evening report.

    Filters out rows with NULL active_power_kw (gaps, not zeros) and
    returns None if there aren't enough valid samples to draw a
    meaningful chart.
    """
    MIN_SAMPLES_FOR_CHART = 6  # ~1 hour of 10-min data; arbitrary but avoids empty charts

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
                        help="High-frequency snapshot mode (10-min cron). Saves a reading, "
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

    if not CONFIG["USERNAME"] or not CONFIG["PASSWORD"]:
        logger.error("SMARTGRID_USERNAME and SMARTGRID_PASSWORD must be set!")
        sys.exit(1)

    try:
        session = create_session()
        login(session)

        balance, current_month, prev_day, prev_month, monthly_consumption, today, daily_readings, prev_prev_month, last_sync, rate_card, grace_credit, portal_recharges, electrical_params = scrape_meter_data(session)
        duration = time.time() - start_time

        # ------------------------------------------------------------------
        # Phase 2: persist an instantaneous snapshot on EVERY run.
        # The data is already in hand — saving it costs one INSERT. This
        # means reports and snapshots share the same historical record.
        # ------------------------------------------------------------------
        now_ist = datetime.now(IST)
        last_sync_dt = parse_last_sync(last_sync)
        save_reading(now_ist, last_sync_dt, last_sync, electrical_params, balance)

        # Staleness log (not an alert — just a debug breadcrumb).
        if last_sync_dt is not None:
            staleness = now_ist - last_sync_dt
            if staleness > timedelta(minutes=STALE_SYNC_WARN_MINUTES):
                logger.warning(f"Portal last_sync is {staleness} old — meter may be offline")

        # Snapshot mode short-circuit: save the reading, run the alert
        # engine, maybe record a mid-day recharge, then exit without any
        # report flow or daily aggregate churn.
        if mode == "snapshot":
            # Mid-day recharge detection from balance jump.
            prev_reading = load_previous_reading(now_ist, max_age=timedelta(hours=6))
            _snapshot_recharge_detect(prev_reading, balance, now_ist)

            # Edge-triggered alerts. The "current" view matches the shape
            # that load_readings returns (dict keyed by column names) —
            # build a minimal one from in-hand values so we don't round-trip
            # the DB for the reading we just wrote.
            current_reading = {
                "recorded_at": now_ist,
                "active_power_kw": (electrical_params or {}).get("active_power_kw"),
                "balance": balance,
            }
            _run_alert_engine(current_reading, now_ist)

            duration = time.time() - start_time
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
        # in case a snapshot run missed the edge.
        current_reading = {
            "recorded_at": now_ist,
            "active_power_kw": (electrical_params or {}).get("active_power_kw"),
            "balance": balance,
        }
        _run_alert_engine(current_reading, now_ist)

        if mode == "evening":
            msg = build_evening_message(balance, current_month, today, duration, last_sync, daily_readings=daily_readings, rate_card=rate_card)
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
            # Query today's data in IST — we want a calendar-day window,
            # not the last 24 rolling hours.
            today_start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
            today_readings = load_readings(today_start_ist, now_ist + timedelta(seconds=1))
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

        # Recharge analysis (evening: detect new recharges and send analysis)
        new_recharges = []
        if portal_recharges:
            stored = load_portal_recharges()
            new_recharges = detect_new_recharges(portal_recharges, stored)

            if new_recharges and mode == "evening":
                analysis_msg = build_recharge_analysis(
                    new_recharges, portal_recharges, balance, daily_readings
                )
                send_telegram_message(analysis_msg)
                # Send recharge table as image
                intervals = []
                for i in range(len(portal_recharges) - 1):
                    d1 = portal_recharges[i]["date"] if isinstance(portal_recharges[i]["date"], date) else datetime.strptime(portal_recharges[i]["date"], "%Y-%m-%d").date()
                    d2 = portal_recharges[i+1]["date"] if isinstance(portal_recharges[i+1]["date"], date) else datetime.strptime(portal_recharges[i+1]["date"], "%Y-%m-%d").date()
                    intervals.append((d1 - d2).days)
                recharge_img = _build_recharge_table_image(portal_recharges, intervals)
                if recharge_img:
                    send_telegram_photo(recharge_img)
                intervals_img = _build_recharge_intervals_image(portal_recharges, intervals)
                if intervals_img:
                    send_telegram_photo(intervals_img)

            merge_portal_recharges_to_history(portal_recharges)
            cleanup_duplicate_recharges()
            save_portal_recharges(portal_recharges)

        # Smart Recharge Advisor (Feature 1 — evening only, separate message)
        if mode == "evening" and balance is not None and balance > 0 and daily_readings:
            advisor_msg = build_recharge_advisor(balance, daily_readings, new_recharges)
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
        # exception type and a short static hint — check the raw log
        # locally (LOG_LEVEL=DEBUG + `uv run scraper.py`) for detail.
        logger.error(f"Fatal error: {type(e).__name__}")
        duration = time.time() - start_time
        send_telegram_message(
            "🚨 <b>Energy Scraper — Failed</b>\n\n"
            f"Error type: {type(e).__name__}\n"
            f"Duration: {duration:.1f}s\n"
            f"Check Action logs for details."
        )
        sys.exit(1)

    logger.info(f"Scraper finished in {duration:.2f} seconds")


if __name__ == "__main__":
    main()
