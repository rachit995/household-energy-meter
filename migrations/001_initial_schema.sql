-- Migration tracker (must exist first; migrate.py creates it separately if missing)
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_readings (
    date            DATE PRIMARY KEY,
    eb_reading      NUMERIC(10,2),
    eb_consume      NUMERIC(10,2),
    dg_reading      NUMERIC(10,2),
    dg_consume      NUMERIC(10,2),
    balance         NUMERIC(10,2),
    eb_cost         NUMERIC(10,2),
    dg_cost         NUMERIC(10,2),
    fix_charge_cost NUMERIC(10,2),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monthly_summaries (
    month          TEXT PRIMARY KEY,
    total          NUMERIC(10,2) NOT NULL,
    eb             NUMERIC(10,2) NOT NULL,
    dg             NUMERIC(10,2) NOT NULL,
    fix_charge     NUMERIC(10,2) NOT NULL,
    avg_daily      NUMERIC(10,3) NOT NULL,
    highest_date   DATE NOT NULL,
    highest_amount NUMERIC(10,2) NOT NULL,
    lowest_date    DATE NOT NULL,
    lowest_amount  NUMERIC(10,2) NOT NULL,
    dg_days        INTEGER NOT NULL,
    days_count     INTEGER NOT NULL,
    weekday_avg    NUMERIC(10,2) DEFAULT 0,
    weekend_avg    NUMERIC(10,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS recharges (
    id             SERIAL PRIMARY KEY,
    date           DATE NOT NULL,
    amount         NUMERIC(10,2) NOT NULL,
    balance_before NUMERIC(10,2),
    balance_after  NUMERIC(10,2),
    source         TEXT,
    UNIQUE(date, amount)
);

CREATE TABLE IF NOT EXISTS portal_recharges (
    id           SERIAL PRIMARY KEY,
    date         DATE NOT NULL,
    amount       NUMERIC(10,2) NOT NULL,
    type         TEXT,
    last_updated DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS rates (
    id         SERIAL PRIMARY KEY,
    date       DATE NOT NULL,
    eb_rate    NUMERIC(10,4) NOT NULL,
    dg_rate    NUMERIC(10,4) NOT NULL,
    fix_charge NUMERIC(10,4) NOT NULL
);
