#!/usr/bin/env python3
"""
Test script: sends ALL message types + chart images to Telegram using real portal data.
Usage: uv run python scraper/test_messages.py
"""

import sys
import time
import os
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from datetime import date, datetime, timedelta
from decimal import Decimal
from scraper import (
    create_client, scrape_meter_data, send_telegram_message, send_telegram_photo,
    build_morning_message, build_afternoon_message, build_evening_message, build_weekly_message,
    build_monthly_message, build_recharge_analysis, build_recharge_advisor,
    check_spending_trend, check_consumption_spike, check_dg_usage,
    check_fix_charge_anomaly, check_rate_changes, check_grace_period,
    check_recharge_prediction, build_recharge_alert,
    compute_weekly_stats, compute_monthly_stats,
    _build_daily_spends, _build_spend_chart_image, _build_weekday_chart_image,
    _build_appliance_guide_image, _build_recharge_table_image,
    _build_balance_runway_image, _build_bill_split_image,
    _build_week_vs_week_image, _build_balance_journey_image,
    _build_monthly_trend_image, _build_recharge_intervals_image,
    SEASON_HINTS,
)
from storage import load_portal_recharges, detect_new_recharges


def send(label, msg):
    if msg:
        tagged = f"🧪 <b>[TEST] {label}</b>\n{'─' * 25}\n\n{msg}"
        send_telegram_message(tagged)
        print(f"  ✓ Sent: {label} ({len(tagged)} chars)")
    else:
        print(f"  ✗ Skipped: {label} (no data)")


def send_img(label, img_buf):
    if img_buf:
        send_telegram_photo(img_buf, caption=f"🧪 [TEST] {label}")
        print(f"  ✓ Sent image: {label}")
    else:
        print(f"  ✗ Skipped image: {label} (no data)")


def main():
    print("Fetching meter data via API...")
    start = time.time()
    with create_client() as client:
        balance, current_month, prev_day, prev_month, monthly_consumption, today, daily_readings, prev_prev_month, last_sync, rate_card, grace_credit, portal_recharges, electrical_params = scrape_meter_data(client)
    duration = time.time() - start
    print(f"Scraped in {duration:.1f}s — balance: ₹{balance}, {len(daily_readings)} daily readings, {len(portal_recharges)} recharges\n")

    today_date = date.today()

    # === MORNING ===
    print("--- Morning Report ---")
    send("Morning Report", build_morning_message(
        balance, current_month, prev_day, prev_month, monthly_consumption,
        duration, last_sync, rate_card=rate_card
    ))

    # === AFTERNOON ===
    print("\n--- Afternoon Check-in ---")
    send("Afternoon Check-in", build_afternoon_message(
        balance, current_month, today, duration, last_sync,
        daily_readings=daily_readings, rate_card=rate_card, electrical_params=electrical_params
    ))

    # === EVENING ===
    print("\n--- Evening Report ---")
    send("Evening Report", build_evening_message(
        balance, current_month, today, duration, last_sync,
        daily_readings=daily_readings, rate_card=rate_card
    ))
    send_img("Balance Runway", _build_balance_runway_image(daily_readings, balance))
    send_img("Today's Bill Split", _build_bill_split_image(today, rate_card, "Today's Bill Split"))

    # === WEEKLY ===
    print("\n--- Weekly Report ---")
    last_monday = today_date - timedelta(days=today_date.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    prev_monday = last_monday - timedelta(days=7)
    prev_sunday = last_monday - timedelta(days=1)
    stats = compute_weekly_stats(daily_readings, last_monday, last_sunday)
    prev_stats = compute_weekly_stats(daily_readings, prev_monday, prev_sunday)
    if stats:
        send("Weekly Report", build_weekly_message(
            stats, prev_stats, balance, duration, last_sync, daily_readings=daily_readings
        ))
        two_weeks_ago = today_date - timedelta(days=14)
        _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
        if balance and consumption_days:
            send_img("Weekly Spend Chart", _build_spend_chart_image(consumption_days, balance))
        send_img("Week vs Week", _build_week_vs_week_image(daily_readings))
    else:
        print("  ✗ Skipped: Weekly Report (not enough data)")

    # === MONTHLY ===
    print("\n--- Monthly Report ---")
    first_of_month = today_date.replace(day=1)
    last_month_end = first_of_month - timedelta(days=1)
    m_stats = compute_monthly_stats(daily_readings, last_month_end.year, last_month_end.month)
    if m_stats:
        send("Monthly Report", build_monthly_message(
            m_stats, prev_month, prev_prev_month, monthly_consumption,
            duration, last_sync, rate_card=rate_card, balance=balance,
            daily_readings=daily_readings
        ))
        send_img("Weekday Pattern", _build_weekday_chart_image(m_stats.get("weekday_pattern", {})))
        send_img("Bill Composition", _build_bill_split_image(
            prev_month, rate_card, f"Bill Composition — {last_month_end.strftime('%b %Y')}"
        ))
        send_img("Balance Journey", _build_balance_journey_image(
            daily_readings, last_month_end.year, last_month_end.month
        ))
        send_img("6-Month Trend", _build_monthly_trend_image(monthly_consumption))
        two_weeks_ago = today_date - timedelta(days=14)
        _, consumption_days = _build_daily_spends(daily_readings, two_weeks_ago, today_date)
        if balance and consumption_days:
            send_img("Monthly Spend Chart", _build_spend_chart_image(consumption_days, balance))
        send_img("Appliance Guide", _build_appliance_guide_image(
            rate_card.get("eb_rate"), fix_charge=rate_card.get("fix_charge")
        ))
    else:
        print("  ✗ Skipped: Monthly Report (not enough data)")

    # === RECHARGE ANALYSIS ===
    print("\n--- Recharge Analysis ---")
    if portal_recharges:
        fake_new = [{"date": portal_recharges[0]["date"], "amount": float(portal_recharges[0]["amount"]), "type": portal_recharges[0].get("type", "")}]
        send("Recharge Analysis", build_recharge_analysis(
            fake_new, portal_recharges, balance, daily_readings
        ))
        intervals = []
        for i in range(len(portal_recharges) - 1):
            d1 = portal_recharges[i]["date"] if isinstance(portal_recharges[i]["date"], date) else datetime.strptime(portal_recharges[i]["date"], "%Y-%m-%d").date()
            d2 = portal_recharges[i+1]["date"] if isinstance(portal_recharges[i+1]["date"], date) else datetime.strptime(portal_recharges[i+1]["date"], "%Y-%m-%d").date()
            intervals.append((d1 - d2).days)
        send_img("Recharge Table", _build_recharge_table_image(portal_recharges, intervals))
        send_img("Recharge Intervals", _build_recharge_intervals_image(portal_recharges, intervals))

    # === ALERTS ===
    print("\n--- Alerts ---")
    send("Recharge Advisor", build_recharge_advisor(Decimal("1200"), daily_readings, []))

    # Spending Trend (force — bypass day check)
    from storage import load_daily_readings as ldr
    import calendar
    prev_month_end = today_date.replace(day=1) - timedelta(days=1)
    year_month = f"{prev_month_end.year}-{prev_month_end.month:02d}"
    prev_readings_raw = ldr(year_month)
    if prev_readings_raw:
        prev_readings = []
        for r in prev_readings_raw:
            rd = r["date"]
            if isinstance(rd, str):
                rd = datetime.strptime(rd, "%Y-%m-%d").date()
            prev_readings.append({**r, "date": rd,
                "balance": Decimal(str(r["balance"])) if r.get("balance") is not None else None,
                "dg_consume": Decimal(str(r["dg_consume"])) if r.get("dg_consume") is not None else None})
        current_start = today_date.replace(day=1)
        _, curr_consumption = _build_daily_spends(daily_readings, current_start, today_date)
        prev_start = date(prev_month_end.year, prev_month_end.month, 1)
        prev_compare_end = date(prev_month_end.year, prev_month_end.month, min(today_date.day, prev_month_end.day))
        _, prev_consumption = _build_daily_spends(prev_readings, prev_start, prev_compare_end)
        if len(curr_consumption) >= 3 and len(prev_consumption) >= 3:
            curr_avg = sum(d["spend"] for d in curr_consumption) / len(curr_consumption)
            prev_avg = sum(d["spend"] for d in prev_consumption) / len(prev_consumption)
            if prev_avg > 0:
                pct_change = ((curr_avg - prev_avg) / prev_avg) * 100
                arrow = "↑" if pct_change > 0 else "↓"
                severity = "significantly " if abs(pct_change) > 30 else ""
                hint = ""
                for season, (months, desc) in SEASON_HINTS.items():
                    if today_date.month in months:
                        hint = f"\n📈 {desc}"
                        break
                curr_total = sum(d["spend"] for d in curr_consumption)
                prev_total = sum(d["spend"] for d in prev_consumption)
                send("Spending Trend", (
                    f"📊 <b>Spending Trend Alert</b>\n\n"
                    f"{calendar.month_abbr[today_date.month]} {severity}tracking <b>{arrow}{abs(pct_change):.0f}%</b> vs {calendar.month_abbr[prev_month_end.month]}.\n"
                    f"  This month: ₹{curr_total:,.0f} avg ₹{curr_avg:.0f}/day\n"
                    f"  Last month: ₹{prev_total:,.0f} avg ₹{prev_avg:.0f}/day{hint}"
                ))

    send("Consumption Spike", check_consumption_spike(today, daily_readings, rate_card))
    send("DG Usage", check_dg_usage(today, rate_card, daily_readings))
    send("Fix Charge Anomaly", check_fix_charge_anomaly(prev_day, rate_card))
    send("Rate Change", check_rate_changes(rate_card))
    send("Grace Period", check_grace_period(balance, grace_credit, rate_card))

    if balance is not None and balance >= 0 and daily_readings:
        prediction = check_recharge_prediction(balance, daily_readings)
        if prediction:
            send("Recharge Prediction", build_recharge_alert(prediction, balance))
        else:
            print("  ✗ Skipped: Recharge Prediction (no prediction)")

    print(f"\nDone! Check your Telegram.")


if __name__ == "__main__":
    main()
