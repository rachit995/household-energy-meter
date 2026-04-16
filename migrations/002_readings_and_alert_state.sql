-- Migration 002: High-frequency readings + alert cooldown state
--
-- Adds two tables to support Phase 2 (10-min power sampling + edge-triggered alerts):
--
--   1. `readings`     — one row per ~10-min scrape, stores instantaneous electrical
--                       parameters and balance. Used for:
--                         - 24h power-profile chart in evening report
--                         - Edge detection for alerts (compare current vs previous reading)
--                         - Sustained-load window queries (last N readings in a time range)
--                         - Historical baseline analysis (future — see icebox.md)
--
--   2. `alert_state`  — cooldown tracker keyed by alert type. Prevents re-firing an
--                       alert while the triggering condition persists. Populated by
--                       `set_alert_state()` in storage.py when an alert is sent.
--
-- Idempotent: all CREATE statements use IF NOT EXISTS, safe to re-run.

-- ---------------------------------------------------------------------------
-- readings: 10-minute snapshots of real-time meter data
-- ---------------------------------------------------------------------------
-- Row size: ~100 bytes. At 144 rows/day × 365 days = ~5 MB/year.
-- Neon free tier storage limit: 512 MB → ~100 years of headroom.
--
-- All electrical-parameter columns are NULLABLE: the portal frequently returns
-- empty values when the physical meter hasn't synced recently. We persist NULL
-- rather than defaulting to 0 (which would corrupt analytics — NULL means
-- "unknown", not "no draw").
CREATE TABLE IF NOT EXISTS readings (
    id                 BIGSERIAL   PRIMARY KEY,
    recorded_at        TIMESTAMPTZ NOT NULL,      -- Wall-clock at scrape time (authoritative for ordering/alert windows)
    last_sync          TIMESTAMPTZ,               -- Portal's meter-sync timestamp, parsed to datetime (may be NULL if unparseable)
    last_sync_raw      TEXT,                      -- Verbatim portal value — kept for debugging and format audits
    active_power_kw    REAL,                      -- Real-time active power draw (kW). NULL when portal hasn't reported.
    apparent_power_kva REAL,                      -- Apparent power (kVA)
    current_amp        REAL,                      -- Line current (A)
    voltage_ln         REAL,                      -- Line-to-neutral voltage (V)
    voltage_ll         REAL,                      -- Line-to-line voltage (V)
    power_factor       REAL,                      -- Power factor (0-1). Useful for load-type hints (motor vs resistive).
    frequency_hz       REAL,                      -- Grid frequency (Hz)
    source             TEXT,                      -- Power source: 'EB (...)', 'DG (...)', 'Full Load', etc.
    balance            NUMERIC(10,2)              -- Prepaid balance at snapshot time. NUMERIC for financial precision.
);

-- DESC index supports the most frequent access pattern: "give me the N most
-- recent readings" (e.g., previous-reading lookup for edge detection,
-- 2h-window scan for sustained-load alerts).
CREATE INDEX IF NOT EXISTS idx_readings_recorded_at ON readings (recorded_at DESC);


-- ---------------------------------------------------------------------------
-- alert_state: one row per alert type, tracks last fire time
-- ---------------------------------------------------------------------------
-- Max expected rows: ~3 (one per alert type). Tiny table.
--
-- Used by the edge-triggered alert engine to enforce per-type cooldowns so
-- that an alert doesn't re-fire on every 10-min run while the underlying
-- condition persists.
--
-- `context` JSONB stores structured metadata about the last fire (e.g., the
-- power level, duration, sample count). Informational — not required for
-- cooldown logic.
CREATE TABLE IF NOT EXISTS alert_state (
    alert_type      TEXT        PRIMARY KEY,     -- 'high_power', 'sustained_load', 'night_anomaly'
    last_fired_at   TIMESTAMPTZ NOT NULL,        -- UTC timestamp when the alert last fired
    context         JSONB                        -- Optional metadata (must be wrapped with psycopg2.extras.Json on write)
);
