"""Adapt raw SmartGridSoft API responses into the 13-tuple shape that the rest
of the scraper consumes. Preserves exact semantics, types, and ordering of the
original HTML-derived output so ``storage.py``, ``charts.py``, ``alerts``, and
the Telegram message builders remain unchanged.

Contract of the 13-tuple (positional, unpacked at scraper.py:2044):
    balance, current_month, prev_day, prev_month, monthly_consumption, today,
    daily_readings, prev_prev_month, last_sync, rate_card, grace_credit,
    portal_recharges, electrical_params
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))

_DEDUCTION_KEYS = ("_Total", "_EB", "_DG", "_FixC")
_DEDUCTION_OUTPUT = ("total", "eb", "dg", "fix_charge")

_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def _parse_decimal(value: Any) -> Decimal | None:
    """Tolerant Decimal coercion that mirrors scraper.parse_decimal.

    Accepts str, int, float, Decimal, or None. Returns None for empty / '-' /
    unparseable input.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if not isinstance(value, str):
        return None
    v = value.strip()
    if v in ("", "-"):
        return None
    try:
        return Decimal(v.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _parse_date_ddmmyyyy(value: str | None) -> date | None:
    """Parse the DD-MM-YYYY format used by BindRecharge."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d-%m-%Y").date()
    except (ValueError, AttributeError):
        return None


def _parse_date_yymmdd(value: str | None) -> date | None:
    """Parse the YY-MM-DD format used by CurrentMonthAllUnitView."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def _first_row(resp: dict | None, result_key: str) -> dict | None:
    if not resp:
        return None
    rows = resp.get(result_key)
    if not rows:
        return None
    return rows[0]


def _rows(resp: dict | None, result_key: str) -> list[dict]:
    if not resp:
        return []
    return list(resp.get(result_key) or [])


def _extract_deduction(resp: dict | None, result_key: str, field_prefix: str) -> dict:
    """Map {field_prefix}_Total / _EB / _DG / _FixC into the
    ``{total, eb, dg, fix_charge}`` contract."""
    row = _first_row(resp, result_key)
    out: dict[str, Decimal | None] = {k: None for k in _DEDUCTION_OUTPUT}
    if not row:
        return out
    for src_suffix, dst_key in zip(_DEDUCTION_KEYS, _DEDUCTION_OUTPUT):
        out[dst_key] = _parse_decimal(row.get(f"{field_prefix}{src_suffix}"))
    return out


def _diff(a: Decimal | None, b: Decimal | None) -> Decimal | None:
    if a is None or b is None:
        return None
    return a - b


def _month_label(when: date) -> str:
    """'April-2026' — mirrors the HTML portal's hyphenated format verbatim."""
    return f"{_MONTH_NAMES[when.month - 1]}-{when.year}"


def _shift_month(anchor: date, delta_months: int) -> date:
    """Return a date in the month that is ``delta_months`` away from anchor.

    We keep day=1 so the result is always valid regardless of anchor.day.
    """
    total_month = anchor.year * 12 + (anchor.month - 1) + delta_months
    year, month0 = divmod(total_month, 12)
    return date(year, month0 + 1, 1)


def extract_balance(meter_basic: dict | None) -> Decimal | None:
    row = _first_row(meter_basic, "MeterBasicDataResult")
    return _parse_decimal(row.get("lblbalance")) if row else None


def extract_last_sync(meter_basic: dict | None) -> str | None:
    row = _first_row(meter_basic, "MeterBasicDataResult")
    return row.get("DateTimeData") if row else None


def extract_grace_credit(op_params: dict | None) -> Decimal | None:
    row = _first_row(op_params, "BindOperationalParametersResult")
    if not row:
        return None
    raw = row.get("g_credit")
    if raw is None:
        return None
    # If the vendor ever returns a bare numeric (int/float/Decimal) instead of
    # the usual "1500 INR" string, don't crash the whole scrape.
    if isinstance(raw, str):
        raw = raw.replace("INR", "").strip()
    return _parse_decimal(raw)


def extract_rate_card(rates: dict | None) -> dict:
    row = _first_row(rates, "BindApplicableRatesResult")
    if not row:
        return {"eb_rate": None, "dg_rate": None, "fix_charge": None}
    return {
        "eb_rate": _parse_decimal(row.get("eb_rate")),
        "dg_rate": _parse_decimal(row.get("dg_rate")),
        "fix_charge": _parse_decimal(row.get("fix_crate_rate")),
    }


def extract_portal_recharges(recharge: dict | None) -> list[dict]:
    """Return recharges sorted newest-first, matching the HTML scraper."""
    rows = _rows(recharge, "BindRechargeResult")
    out: list[dict] = []
    for row in rows:
        amt = _parse_decimal(row.get("RechargeAmount"))
        dt = _parse_date_ddmmyyyy(row.get("RechargeDate"))
        if amt is None or dt is None:
            continue
        out.append({
            "date": dt,
            "amount": amt,
            "type": (row.get("RechargeTransactionType") or "").strip(),
        })
    out.sort(key=lambda r: r["date"], reverse=True)
    return out


# Map the BindSourceRunning flag to the portal-literal strings that
# ``_source_display()`` at ``scraper.py:1609`` expects. "Full Load" hits the
# EB branch (case-insensitive match); anything else falls into the DG branch
# as ``DG (<text>)``. We control the text on both sides here.
_SOURCE_RUNNING_LABEL = {"0": "Full Load", "1": "Generator"}


def extract_electrical_params(electric: dict | None, source_running: dict | None) -> dict:
    """Live power snapshot + real-time source.

    ``source`` is derived from ``BindSourceRunning.SourceRunning_Val``. We
    avoid ``BindSourceChageover`` because its DG-mode string was never
    sampled during reverse engineering and could silently mis-route through
    ``_source_display()``'s EB-token allowlist.
    """
    ep_row = _first_row(electric, "BindElectricParameterResult")
    out: dict[str, Any] = {
        "active_power_kw": None,
        "apparent_power_kva": None,
        "current_amp": None,
        "voltage_ln": None,
        "voltage_ll": None,
        "power_factor": None,
        "frequency_hz": None,
        "source": None,
    }
    if ep_row:
        out["active_power_kw"] = _parse_decimal(ep_row.get("ElectricParameter_ActivePower"))
        out["apparent_power_kva"] = _parse_decimal(ep_row.get("ElectricParameter_ApparentPower"))
        out["current_amp"] = _parse_decimal(ep_row.get("ElectricParameter_Current"))
        out["voltage_ln"] = _parse_decimal(ep_row.get("ElectricParameter_VoltageLN"))
        out["voltage_ll"] = _parse_decimal(ep_row.get("ElectricParameter_VoltageLL"))
        out["power_factor"] = _parse_decimal(ep_row.get("ElectricParameter_PF"))
        out["frequency_hz"] = _parse_decimal(ep_row.get("ElectricParameter_Frequency"))
    src_row = _first_row(source_running, "BindSourceRunningResult")
    if src_row:
        raw = src_row.get("SourceRunning_Val")
        # Vendor consistently returns a string today, but guard against a
        # future bare int/float the same way extract_grace_credit does.
        val = raw.strip() if isinstance(raw, str) else ("" if raw is None else str(raw))
        out["source"] = _SOURCE_RUNNING_LABEL.get(val)
    return out


def extract_daily_readings(
    previous_view: dict | None,
    current_view: dict | None,
) -> list[dict]:
    """Merge prev+current month rows, sort ascending by date, synthesize
    ``eb_consume`` / ``dg_consume`` from day-over-day meter-read diffs.

    **Consume semantics**: each API row is an *opening* snapshot at midnight,
    so ``unit_s1[X]`` is the cumulative read at the START of day X. Consume
    DURING day X is therefore ``unit_s1[X+1] - unit_s1[X]``. The row for the
    newest (in-progress) day has no next-day baseline → consume=None.

    ``balance`` is ``amount_total`` (the portal's daily opening balance), which
    ``_build_daily_spends()`` at ``scraper.py:350`` diffs to compute spend.
    """
    staged: dict[date, dict] = {}
    for resp, key in (
        (previous_view, "PreviousMonthAllUnitViewResult"),
        (current_view, "CurrentMonthAllUnitViewResult"),
    ):
        for row in _rows(resp, key):
            d = _parse_date_yymmdd(row.get("date"))
            if d is None:
                continue
            staged[d] = {
                "date": d,
                "eb_reading": _parse_decimal(row.get("unit_s1")),
                "dg_reading": _parse_decimal(row.get("unit_s2")),
                "balance": _parse_decimal(row.get("amount_total")),
            }
    ordered = sorted(staged.values(), key=lambda r: r["date"])
    out: list[dict] = []
    for i, row in enumerate(ordered):
        nxt = ordered[i + 1] if i + 1 < len(ordered) else None
        if nxt is None:
            eb_consume, dg_consume = None, None
        else:
            eb_consume = _diff(nxt["eb_reading"], row["eb_reading"])
            dg_consume = _diff(nxt["dg_reading"], row["dg_reading"])
        out.append({
            "date": row["date"],
            "eb_reading": row["eb_reading"],
            "eb_consume": eb_consume,
            "dg_reading": row["dg_reading"],
            "dg_consume": dg_consume,
            "balance": row["balance"],
        })
    return out


def extract_monthly_consumption(
    previous_month_deduction: dict | None,
    prev_prev_month_deduction: dict | None,
    now: datetime | None = None,
    historical: list[dict] | None = None,
) -> list[dict]:
    """Newest-first list of ``{"month": str, "amount": Decimal|None}`` dicts.

    Matches the HTML portal's "Monthly Consumption History" panel: **excludes
    the current month**, starts at the previous complete month, and uses the
    hyphenated label format (e.g. ``"March-2026"``). The API surfaces 2 past
    months directly; caller passes ``historical`` rows from the
    ``monthly_summaries`` table to pad out to 6. ``_build_monthly_trend_image()``
    at ``scraper.py:1168`` reverses this list, so ordering matters.
    """
    anchor = (now or datetime.now(IST)).date()
    api_months = [
        (_month_label(_shift_month(anchor, -1)),
         _extract_deduction(previous_month_deduction, "BindPreviousMonthDeductionResult", "PreviousMonthDeduction")["total"]),
        (_month_label(_shift_month(anchor, -2)),
         _extract_deduction(prev_prev_month_deduction, "BindPreviousToPreviousMonthDeductionResult", "PreviousToPreviousMonthDeduction")["total"]),
    ]
    api_list = [{"month": m, "amount": a} for m, a in api_months]
    seen = {m["month"] for m in api_list}
    padded = list(api_list)
    for row in historical or []:
        label = row.get("month")
        if label and label not in seen:
            padded.append({"month": label, "amount": row.get("amount")})
            seen.add(label)
        if len(padded) >= 6:
            break
    return padded[:6]


def normalize(
    responses: dict[str, dict | None],
    *,
    now: datetime | None = None,
    historical_months: list[dict] | None = None,
) -> tuple:
    """Transform fetched API responses into the 13-tuple that
    ``scrape_meter_data()`` used to return. Preserves field order, types
    (Decimal / str / date / None), and sort order of list fields.
    """
    balance = extract_balance(responses.get("MeterBasicData"))
    last_sync = extract_last_sync(responses.get("MeterBasicData"))
    current_month = _extract_deduction(
        responses.get("BindCurrentMonthDeduction"),
        "BindCurrentMonthDeductionResult", "CurrentMonthDeduction",
    )
    prev_day = _extract_deduction(
        responses.get("BindPreviousDayDeduction"),
        "BindPreviousDayDeductionResult", "PreviousDayDeduction",
    )
    prev_month = _extract_deduction(
        responses.get("BindPreviousMonthDeduction"),
        "BindPreviousMonthDeductionResult", "PreviousMonthDeduction",
    )
    prev_prev_month = _extract_deduction(
        responses.get("BindPreviousToPreviousMonthDeduction"),
        "BindPreviousToPreviousMonthDeductionResult", "PreviousToPreviousMonthDeduction",
    )
    today = _extract_deduction(
        responses.get("BindCurrentDayDeduction"),
        "BindCurrentDayDeductionResult", "CurrentDayDeduction",
    )
    monthly_consumption = extract_monthly_consumption(
        responses.get("BindPreviousMonthDeduction"),
        responses.get("BindPreviousToPreviousMonthDeduction"),
        now=now,
        historical=historical_months,
    )
    daily_readings = extract_daily_readings(
        responses.get("PreviousMonthAllUnitView"),
        responses.get("CurrentMonthAllUnitView"),
    )
    rate_card = extract_rate_card(responses.get("BindApplicableRates"))
    grace_credit = extract_grace_credit(responses.get("BindOperationalParameters"))
    portal_recharges = extract_portal_recharges(responses.get("BindRecharge"))
    electrical_params = extract_electrical_params(
        responses.get("BindElectricParameter"),
        responses.get("BindSourceRunning"),
    )
    return (
        balance,
        current_month,
        prev_day,
        prev_month,
        monthly_consumption,
        today,
        daily_readings,
        prev_prev_month,
        last_sync,
        rate_card,
        grace_credit,
        portal_recharges,
        electrical_params,
    )
