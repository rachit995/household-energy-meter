"""
Historical data storage — Neon Postgres backend.

Function signatures and return shapes are preserved from the JSON era so
scraper.py and test_messages.py work unchanged.
"""

import logging
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)

_conn = None


def _get_conn():
    """Module-level lazy singleton. One connection per process."""
    global _conn
    if _conn is None or _conn.closed:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        _conn = psycopg2.connect(
            database_url,
            sslmode="require",
            connect_timeout=10,
        )
    return _conn


def _to_iso(d):
    """Coerce date/datetime/str → ISO date string."""
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat() if isinstance(d, date) and not isinstance(d, datetime) else d.date().isoformat()
    return str(d)


def _as_float(v):
    """Coerce Decimal/None/number → float or None."""
    if v is None:
        return None
    return float(v)


# -----------------------------------------------------------------------------
# Daily readings
# -----------------------------------------------------------------------------

def save_daily(daily_readings):
    """Upsert daily readings.

    Rules (enforced in the ON CONFLICT clause):
      - eb_reading, eb_consume, dg_reading, dg_consume: max-preserve.
        Portal resets these to 0 at midnight, so a fresh 0 must never clobber
        the real end-of-day value we captured earlier.
      - balance: updated to latest EXCEPT when the new value is NULL, or
        an exact 0 while the existing value is non-zero. Only these two
        specific cases are midnight-reset artefacts. We deliberately do
        NOT guard against "large drops" — a legitimate grace-period
        activation (e.g. 150 → -1500) or DG-heavy day (5000 → 100) must
        pass through, otherwise `check_grace_period` and the recharge
        runway alerts both misfire.
      - updated_at: set to now() on every upsert for audit/debug.
    """
    if not daily_readings:
        return

    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            for r in daily_readings:
                d = r["date"] if isinstance(r["date"], date) else datetime.strptime(r["date"], "%Y-%m-%d").date()
                cur.execute(
                    """
                    INSERT INTO daily_readings
                        (date, eb_reading, eb_consume, dg_reading, dg_consume, balance, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (date) DO UPDATE SET
                        eb_reading = CASE
                            WHEN daily_readings.eb_reading IS NULL THEN EXCLUDED.eb_reading
                            WHEN EXCLUDED.eb_reading IS NULL THEN daily_readings.eb_reading
                            ELSE GREATEST(daily_readings.eb_reading, EXCLUDED.eb_reading)
                        END,
                        eb_consume = CASE
                            WHEN daily_readings.eb_consume IS NULL THEN EXCLUDED.eb_consume
                            WHEN EXCLUDED.eb_consume IS NULL THEN daily_readings.eb_consume
                            ELSE GREATEST(daily_readings.eb_consume, EXCLUDED.eb_consume)
                        END,
                        dg_reading = CASE
                            WHEN daily_readings.dg_reading IS NULL THEN EXCLUDED.dg_reading
                            WHEN EXCLUDED.dg_reading IS NULL THEN daily_readings.dg_reading
                            ELSE GREATEST(daily_readings.dg_reading, EXCLUDED.dg_reading)
                        END,
                        dg_consume = CASE
                            WHEN daily_readings.dg_consume IS NULL THEN EXCLUDED.dg_consume
                            WHEN EXCLUDED.dg_consume IS NULL THEN daily_readings.dg_consume
                            ELSE GREATEST(daily_readings.dg_consume, EXCLUDED.dg_consume)
                        END,
                        -- Balance preserve rule: reject only the two
                        -- specific midnight-reset artefacts — NULL from
                        -- portal, or exact 0 while we have a non-zero
                        -- value stored. Large legitimate drops (grace
                        -- period, heavy DG day) must pass through.
                        balance = CASE
                            WHEN EXCLUDED.balance IS NULL THEN daily_readings.balance
                            WHEN EXCLUDED.balance = 0
                                 AND daily_readings.balance IS NOT NULL
                                 AND daily_readings.balance <> 0
                                THEN daily_readings.balance
                            ELSE EXCLUDED.balance
                        END,
                        updated_at = now()
                    """,
                    (
                        d,
                        r.get("eb_reading"),
                        r.get("eb_consume"),
                        r.get("dg_reading"),
                        r.get("dg_consume"),
                        r.get("balance"),
                    ),
                )
    logger.info(f"Saved {len(daily_readings)} daily readings")


def save_daily_costs(date_obj, eb_cost, dg_cost, fix_charge):
    """Save cost breakdown for a single day. Never overwrites non-zero with zero OR NULL."""
    if isinstance(date_obj, str):
        d = datetime.strptime(date_obj, "%Y-%m-%d").date()
    elif isinstance(date_obj, date):
        d = date_obj
    else:
        logger.warning(f"Unsupported date type for save_daily_costs: {type(date_obj)}")
        return False

    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            # Ensure row exists first (UPDATE returns 0 rows otherwise)
            cur.execute("SELECT 1 FROM daily_readings WHERE date = %s", (d,))
            if not cur.fetchone():
                logger.warning(f"No daily reading for {d}, cannot save costs")
                return False

            cur.execute(
                """
                UPDATE daily_readings SET
                    eb_cost = CASE
                        WHEN eb_cost IS NOT NULL AND eb_cost <> 0
                             AND (%s IS NULL OR %s = 0) THEN eb_cost
                        ELSE %s
                    END,
                    dg_cost = CASE
                        WHEN dg_cost IS NOT NULL AND dg_cost <> 0
                             AND (%s IS NULL OR %s = 0) THEN dg_cost
                        ELSE %s
                    END,
                    fix_charge_cost = CASE
                        WHEN fix_charge_cost IS NOT NULL AND fix_charge_cost <> 0
                             AND (%s IS NULL OR %s = 0) THEN fix_charge_cost
                        ELSE %s
                    END,
                    updated_at = now()
                WHERE date = %s
                """,
                (
                    eb_cost, eb_cost, eb_cost,
                    dg_cost, dg_cost, dg_cost,
                    fix_charge, fix_charge, fix_charge,
                    d,
                ),
            )
    logger.info(f"Saved daily costs for {d}")
    return True


def load_daily_readings(year_month):
    """Load daily readings for a given month (e.g., '2026-03').
    Returns list of dicts with ISO string dates and float values, or None if no rows.
    """
    try:
        start = datetime.strptime(year_month + "-01", "%Y-%m-%d").date()
    except ValueError:
        return None

    # End = first day of next month
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)

    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT date, eb_reading, eb_consume, dg_reading, dg_consume,
                   balance, eb_cost, dg_cost, fix_charge_cost
            FROM daily_readings
            WHERE date >= %s AND date < %s
            ORDER BY date
            """,
            (start, end),
        )
        rows = cur.fetchall()

    if not rows:
        return None

    return [
        {
            "date": row["date"].isoformat(),
            "eb_reading": _as_float(row["eb_reading"]),
            "eb_consume": _as_float(row["eb_consume"]),
            "dg_reading": _as_float(row["dg_reading"]),
            "dg_consume": _as_float(row["dg_consume"]),
            "balance": _as_float(row["balance"]),
            "eb_cost": _as_float(row["eb_cost"]),
            "dg_cost": _as_float(row["dg_cost"]),
            "fix_charge_cost": _as_float(row["fix_charge_cost"]),
        }
        for row in rows
    ]


# -----------------------------------------------------------------------------
# Monthly summaries
# -----------------------------------------------------------------------------

def save_monthly(stats, prev_month_deductions):
    """Save monthly summary. Write-once per month."""
    month_key = f"{stats['year']}-{stats['month']:02d}"
    pm = prev_month_deductions or {}

    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monthly_summaries
                    (month, total, eb, dg, fix_charge, avg_daily,
                     highest_date, highest_amount, lowest_date, lowest_amount,
                     dg_days, days_count, weekday_avg, weekend_avg)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (month) DO NOTHING
                """,
                (
                    month_key,
                    float(pm.get("total") or stats["total"]),
                    float(pm.get("eb") or 0),
                    float(pm.get("dg") or 0),
                    float(pm.get("fix_charge") or 0),
                    float(stats["avg"]),
                    stats["highest"]["date"],
                    float(stats["highest"]["spend"]),
                    stats["lowest"]["date"],
                    float(stats["lowest"]["spend"]),
                    stats["dg_days"],
                    stats["days_count"],
                    float(stats.get("weekday_avg", 0)),
                    float(stats.get("weekend_avg", 0)),
                ),
            )
            if cur.rowcount:
                logger.info(f"Saved monthly summary for {month_key}")
            else:
                logger.info(f"Monthly summary already exists for {month_key}, skipping")


# -----------------------------------------------------------------------------
# Recharges (balance-jump + portal-sourced, unified table)
# -----------------------------------------------------------------------------

def save_recharge(recharge_date, amount, balance_before, balance_after):
    """Append a balance-jump recharge. ₹1 tolerance dedup, cross-match portal ±2 days."""
    if isinstance(recharge_date, str):
        d = datetime.strptime(recharge_date, "%Y-%m-%d").date()
    else:
        d = recharge_date
    amt = float(amount)

    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            # Exact-day dedup with ₹1 tolerance
            cur.execute(
                "SELECT 1 FROM recharges WHERE date = %s AND abs(amount - %s) < 1 LIMIT 1",
                (d, amt),
            )
            if cur.fetchone():
                logger.info(f"Recharge on {d} already recorded, skipping")
                return

            # Cross-match: skip if a portal recharge within ±2 days shadows
            # this balance jump.
            #
            # Note the ASYMMETRIC tolerance (`amount >= jump - 1`, not
            # `abs(diff) < 1`). This is intentional: balance-jump detection
            # is fuzzy (underestimates recharge amount when consumption
            # happens on the same day), while portal recharges are
            # authoritative. If a portal recharge of ₹5000 happened within
            # ±2 days of a detected balance jump of ₹4500, the jump is
            # almost certainly the same recharge minus some consumption.
            # A symmetric ±₹1 rule would create duplicate entries.
            cur.execute(
                """
                SELECT date, amount FROM recharges
                WHERE source = 'portal'
                  AND abs(date - %s) <= 2
                  AND amount >= %s - 1
                LIMIT 1
                """,
                (d, amt),
            )
            shadow = cur.fetchone()
            if shadow:
                logger.info(
                    f"Recharge on {d} (₹{amt:.0f}) shadowed by portal recharge on {shadow[0]} (₹{float(shadow[1]):.0f}), skipping"
                )
                return

            cur.execute(
                """
                INSERT INTO recharges (date, amount, balance_before, balance_after, source)
                VALUES (%s, %s, %s, %s, NULL)
                ON CONFLICT (date, amount) DO NOTHING
                """,
                (d, amt, float(balance_before), float(balance_after)),
            )
    logger.debug(f"Saved recharge: ₹{amt} on {d}")


def merge_portal_recharges_to_history(portal_recharges):
    """Append portal recharges into the unified recharges table.
    ₹1 tolerance dedup. Sets source='portal', balance_before/after=NULL."""
    if not portal_recharges:
        logger.info("No portal recharges to merge")
        return

    conn = _get_conn()
    added = 0
    with conn:
        with conn.cursor() as cur:
            for r in portal_recharges:
                r_date = r["date"]
                if isinstance(r_date, str):
                    r_date = datetime.strptime(r_date, "%Y-%m-%d").date()
                r_amount = float(r["amount"])

                cur.execute(
                    """
                    INSERT INTO recharges (date, amount, balance_before, balance_after, source)
                    SELECT %s, %s, NULL, NULL, 'portal'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM recharges
                        WHERE date = %s AND abs(amount - %s) < 1
                    )
                    ON CONFLICT (date, amount) DO NOTHING
                    """,
                    (r_date, r_amount, r_date, r_amount),
                )
                if cur.rowcount:
                    added += 1

    if added > 0:
        logger.info(f"Merged {added} portal recharges into recharges history")
    else:
        logger.info("No new portal recharges to merge")


def cleanup_duplicate_recharges():
    """Remove balance-jump recharges (source IS NULL) shadowed by a portal
    entry within ±2 days.

    Same asymmetric tolerance as save_recharge's cross-match: a portal
    recharge with amount >= (balance-jump - ₹1) shadows the jump. Portal
    is authoritative; balance-jump detection under-estimates when
    consumption occurs on the recharge day.
    """
    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM recharges r
                WHERE r.source IS NULL
                  AND EXISTS (
                      SELECT 1 FROM recharges p
                      WHERE p.source = 'portal'
                        AND abs(p.date - r.date) <= 2
                        AND p.amount >= r.amount - 1
                  )
                """
            )
            removed = cur.rowcount
    if removed:
        logger.info(f"Cleaned up {removed} duplicate recharge(s)")
    else:
        logger.info("No duplicate recharges to clean up")


# -----------------------------------------------------------------------------
# Portal recharges (last-10 snapshot, full replace on each run)
# -----------------------------------------------------------------------------

def save_portal_recharges(recharges):
    """Full replace of portal_recharges table. Atomic DELETE + INSERTs."""
    today = date.today()
    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM portal_recharges")
            for r in recharges:
                r_date = r["date"]
                if isinstance(r_date, str):
                    r_date = datetime.strptime(r_date, "%Y-%m-%d").date()
                cur.execute(
                    """
                    INSERT INTO portal_recharges (date, amount, type, last_updated)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (r_date, float(r["amount"]), r.get("type", ""), today),
                )
    logger.info(f"Saved {len(recharges)} portal recharges")


def load_portal_recharges():
    """Load stored portal recharges.

    Returns None when table is empty (first-run sentinel — prevents
    detect_new_recharges from flagging every row as new).
    Otherwise returns list of dicts with ISO string dates and float amounts.
    """
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT date, amount, type FROM portal_recharges ORDER BY date DESC")
        rows = cur.fetchall()

    if not rows:
        return None

    return [
        {
            "date": row["date"].isoformat(),
            "amount": float(row["amount"]),
            "type": row["type"] or "",
        }
        for row in rows
    ]


# -----------------------------------------------------------------------------
# Rates (append-only on change)
# -----------------------------------------------------------------------------

def save_rates(rates):
    """Append rate card entry only if rates differ from the latest stored row."""
    new_eb = float(rates.get("eb_rate") or 0)
    new_dg = float(rates.get("dg_rate") or 0)
    new_fix = float(rates.get("fix_charge") or 0)

    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT eb_rate, dg_rate, fix_charge FROM rates ORDER BY id DESC LIMIT 1"
            )
            last = cur.fetchone()
            if last:
                if (float(last[0]) == new_eb and
                        float(last[1]) == new_dg and
                        float(last[2]) == new_fix):
                    return  # No change

            cur.execute(
                """
                INSERT INTO rates (date, eb_rate, dg_rate, fix_charge)
                VALUES (%s, %s, %s, %s)
                """,
                (date.today(), new_eb, new_dg, new_fix),
            )
    logger.debug(f"Saved rate change: eb={new_eb} dg={new_dg} fix={new_fix}")


def load_rates():
    """Return the latest rates as a dict, or None if the table is empty.
    Values are floats, compatible with Decimal(str(v)) in scraper.py."""
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT date, eb_rate, dg_rate, fix_charge FROM rates ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()

    if not row:
        return None
    return {
        "date": row["date"].isoformat(),
        "eb_rate": float(row["eb_rate"]),
        "dg_rate": float(row["dg_rate"]),
        "fix_charge": float(row["fix_charge"]),
    }


# -----------------------------------------------------------------------------
# Pure functions (no I/O) — unchanged from JSON era
# -----------------------------------------------------------------------------

def detect_new_recharges(current, stored):
    """Find recharges in current that aren't in stored. Uses ±₹1 tolerance."""
    if stored is None:
        return []  # First run — don't treat all as new
    current_normalized = [
        {
            **r,
            "date": r["date"].isoformat() if isinstance(r["date"], date) else r["date"],
            "amount": float(r["amount"]),
        }
        for r in current
    ]
    new = []
    for r in current_normalized:
        already = any(
            s["date"] == r["date"] and abs(s["amount"] - r["amount"]) < 1
            for s in stored
        )
        if not already:
            new.append(r)
    return new


def extract_recharges(daily_readings):
    """Detect recharge events from daily readings (balance jumps up)."""
    readings = sorted(daily_readings, key=lambda r: r["date"])
    recharges = []

    for i in range(len(readings) - 1):
        curr = readings[i]
        nxt = readings[i + 1]
        if curr["balance"] is not None and nxt["balance"] is not None:
            diff = curr["balance"] - nxt["balance"]
            if diff < 0:  # Balance went up = recharge
                recharges.append({
                    "date": nxt["date"],
                    "amount": abs(diff),
                    "balance_before": nxt["balance"] - abs(diff),
                    "balance_after": nxt["balance"],
                })

    return recharges


# =============================================================================
# Phase 2: High-frequency readings + edge-triggered alert cooldowns
# =============================================================================
#
# The functions below support 10-minute snapshot polling. Each snapshot writes
# one row to `readings` with the live electrical parameters and balance. The
# alert engine reads recent rows to detect edges (false→true transitions) and
# records cooldowns in `alert_state` to avoid re-firing alerts while a
# condition persists.
#
# See migrations/002_readings_and_alert_state.sql for table definitions.


def save_reading(recorded_at, last_sync, last_sync_raw, electrical_params, balance):
    """Insert one row into `readings`.

    Every field is persisted as-is — including NULLs. Electrical parameters
    are NULL when the portal hasn't reported them (common); the alert engine
    must skip NULL readings, not treat them as 0.

    Args:
        recorded_at: timezone-aware datetime, scrape wall-clock time.
        last_sync: parsed datetime of portal's meter-sync time, or None if
                   unparseable (raw value still saved in last_sync_raw).
        last_sync_raw: original portal string (e.g., "16-04-2026 09:21:27"),
                       kept for debugging format drift.
        electrical_params: dict returned by scrape_electrical_params() with
                           keys: active_power_kw, apparent_power_kva,
                           current_amp, voltage_ln, voltage_ll, power_factor,
                           frequency_hz, source. Any value may be None.
        balance: Decimal/float/None — account balance at scrape time.
    """
    ep = electrical_params or {}
    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO readings (
                    recorded_at, last_sync, last_sync_raw,
                    active_power_kw, apparent_power_kva, current_amp,
                    voltage_ln, voltage_ll, power_factor, frequency_hz,
                    source, balance
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    recorded_at,
                    last_sync,
                    last_sync_raw,
                    ep.get("active_power_kw"),
                    ep.get("apparent_power_kva"),
                    ep.get("current_amp"),
                    ep.get("voltage_ln"),
                    ep.get("voltage_ll"),
                    ep.get("power_factor"),
                    ep.get("frequency_hz"),
                    ep.get("source"),
                    balance,
                ),
            )
    # Balance + power values are private; emit at DEBUG so they never leak
    # into public GitHub Actions logs.
    logger.debug(f"Saved reading @ {recorded_at.isoformat()} (power={ep.get('active_power_kw')}kW, balance={balance})")
    logger.info("Saved reading")


def load_readings(start, end):
    """Fetch readings in [start, end) ordered by recorded_at ASC.

    Used by:
      - Evening report's 24h power-profile chart
      - Sustained-load alert check (last 2h window)

    Returns:
        list of dicts with keys matching readings columns. Decimal/date values
        are returned as-is (caller decides whether to coerce to float/str).
        Empty list when no rows match.
    """
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, recorded_at, last_sync, active_power_kw, apparent_power_kva,
                   current_amp, voltage_ln, voltage_ll, power_factor, frequency_hz,
                   source, balance
            FROM readings
            WHERE recorded_at >= %s AND recorded_at < %s
            ORDER BY recorded_at ASC
            """,
            (start, end),
        )
        return [dict(r) for r in cur.fetchall()]


def load_previous_reading(before_ts, max_age=timedelta(minutes=25)):
    """Return the most recent reading strictly before `before_ts`,
    but only if it is within `max_age` of `before_ts`. Returns None
    otherwise.

    The max_age guard is critical for edge detection: if the previous
    reading is too old (e.g., GHA cron skipped a run, or DB was down
    briefly), we should NOT treat the current reading as a continuation
    of that stale sample. Instead we treat it as a fresh edge — better
    to potentially re-fire an alert than to silently suppress after a
    data gap.

    Args:
        before_ts: timezone-aware datetime — we want readings strictly
                   earlier than this.
        max_age: timedelta — max gap allowed. Default 25 min tolerates
                 one missed 10-min cron slot plus GitHub Actions'
                 well-documented 5-15 min schedule-trigger jitter.

    Returns:
        dict (same shape as load_readings rows), or None.
    """
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, recorded_at, last_sync, active_power_kw, apparent_power_kva,
                   current_amp, voltage_ln, voltage_ll, power_factor, frequency_hz,
                   source, balance
            FROM readings
            WHERE recorded_at < %s
              AND recorded_at >= %s
            ORDER BY recorded_at DESC
            LIMIT 1
            """,
            (before_ts, before_ts - max_age),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_alert_state(alert_type):
    """Return the last-fire record for an alert type, or None.

    Args:
        alert_type: one of 'high_power', 'sustained_load', 'night_anomaly'.

    Returns:
        dict with keys `last_fired_at` (datetime) and `context` (dict or
        None), or None if this alert has never fired.
    """
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT alert_type, last_fired_at, context FROM alert_state WHERE alert_type = %s",
            (alert_type,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def set_alert_state(alert_type, fired_at, context=None):
    """Upsert the last-fire record for an alert type.

    The JSONB column requires psycopg2's Json adapter — passing a raw dict
    would raise 'can't adapt type dict'. We wrap here so callers don't
    need to know.

    Args:
        alert_type: one of 'high_power', 'sustained_load', 'night_anomaly'.
        fired_at: timezone-aware datetime when the alert fired.
        context: optional dict of metadata (e.g., {'power_kw': 3.1}). Stored
                 as JSONB. None stores SQL NULL.
    """
    json_context = Json(context) if context is not None else None
    conn = _get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_state (alert_type, last_fired_at, context)
                VALUES (%s, %s, %s)
                ON CONFLICT (alert_type) DO UPDATE SET
                    last_fired_at = EXCLUDED.last_fired_at,
                    context       = EXCLUDED.context
                """,
                (alert_type, fired_at, json_context),
            )
    # Context contains power/balance values — emit at DEBUG only.
    logger.debug(f"alert_state[{alert_type}] fired @ {fired_at.isoformat()} context={context}")
    logger.info(f"alert_state[{alert_type}] fired")
