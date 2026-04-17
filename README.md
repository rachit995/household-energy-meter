# Energy Monitor

A self-hosted energy meter monitoring system that reads a prepaid electricity meter via the vendor's mobile-app JSON API and sends reports via Telegram.

## Stack

| Component | Technology | Hosting |
|-----------|------------|---------|
| Scraper | Python + requests (undocumented vendor JSON API) | GitHub Actions (cron) |
| Notifications | Telegram Bot API | — |
| Error Monitoring | Sentry (cron monitors + exception capture) | Sentry free tier |
| Data Storage | Neon Postgres | Neon free tier |
| Python Deps | uv | Local / CI |

**Total cost: $0/month** — GitHub Actions free tier (unlimited for public repos) + Neon free tier.

## Reports

| Mode | Schedule | Content |
|------|----------|---------|
| Snapshot | Every 20 min (`:05,:25,:45`) | Saves instantaneous power + balance to `readings`; runs real-time alerts; no Telegram unless an alert fires |
| Morning | 6:30 AM IST daily | Balance, yesterday's deductions vs 7d avg & last week, current month, balance runway |
| Afternoon | 5:30 PM IST daily | Live power draw, DG/EB source, today's spend + projection, budget pace |
| Evening | 10:00 PM IST daily | Balance, today's deductions, balance runway, 14-day spend trend chart, 24h power profile chart |
| Weekly | Monday morning | Week total, highest/lowest day, WoW comparison, DG days |
| Monthly | 1st of month | Full month stats, weekday pattern, recharges, 6-month history |

## Alerts

**Daily alerts** (run during scheduled reports):

| Alert | Trigger | When |
|-------|---------|------|
| Rate Change | EB/DG/fix rate changes | Every run |
| Grace Period | Balance goes negative | Every run |
| Recharge Prediction | Balance < 7 days of spend | Every run |
| Budget | 50/75/90% of monthly budget | Every run (in message) |
| Consumption Spike | Today > 150% of 7-day avg | Evening |
| DG Usage | DG cost > 0 today | Evening |
| Fix Charge Anomaly | Fix charge > 120% of daily rate | Evening |
| Recharge Analysis | New recharge detected in portal | Evening |
| Spending Trend | Periodic trend check (days 7/14/21/28) | Evening |
| Partial Day Spike | Today's partial spend already exceeds yesterday | Afternoon |

**Real-time alerts** (run every 20-min snapshot — edge-triggered with cooldowns):

| Alert | Trigger | Cooldown |
|-------|---------|----------|
| Heavy Power Draw | Power crosses 2.5 kW | 1 hour |
| Sustained Load | Power ≥ 2.5 kW for 2+ hours | 4 hours |
| Night Anomaly | Power > 1.0 kW between 00:00–05:00 IST | 2 hours |

## Setup

### 1. Create a Neon Postgres Database

1. Sign up at [neon.tech](https://neon.tech) (free tier)
2. Create a new project
3. Copy the connection string (ends with `?sslmode=require`)

### 2. Create a Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Create a new bot and copy the token
3. Start a chat with your bot and get your chat ID via [@userinfobot](https://t.me/userinfobot)

### 3. Initialize the Database

```bash
uv sync                              # Install deps
cp .env.example .env                 # Fill in IDs + DATABASE_URL

uv run python migrations/migrate.py  # Create tables
```

### 4. Bootstrap the Meter IDs

The vendor API needs three identifiers on every call: `SMARTGRID_SITE_ID` (society), `SMARTGRID_UNIT_ID`, and `SMARTGRID_METER_ID`. They're static per flat and only need to be resolved once. The bootstrap script derives all three from your society/tower/flat:

```bash
uv run python scripts/bootstrap_ids.py \
  --society "<your society>" --tower "<tower>" --flat "<flat>"
```

Copy the three `SMARTGRID_*` lines it prints into `.env` and into the GitHub secrets below.

### 5. Configure GitHub Repository

Public repos have public Action logs. To avoid leaking identifying values, every secret below is stored as an **encrypted Secret** (auto-masked in logs) rather than a Variable (plaintext in logs).

Go to **Settings** > **Secrets and variables** > **Actions** and add these under **Secrets**:

| Secret | Value |
|--------|-------|
| `SMARTGRID_SITE_ID` | Society ID from `bootstrap_ids.py` |
| `SMARTGRID_UNIT_ID` | Unit ID from `bootstrap_ids.py` |
| `SMARTGRID_METER_ID` | Meter ID from `bootstrap_ids.py` |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |
| `DATABASE_URL` | Neon Postgres connection string |
| `SENTRY_DSN` | *(optional)* Sentry project DSN for private error monitoring + cron health |

### 6. Enable GitHub Actions

The workflow runs automatically on schedule. To test manually:

1. Go to **Actions** tab
2. Click **Energy Meter Scraper**
3. Click **Run workflow** and select a mode (morning/afternoon/evening)

## Local Development

```bash
uv sync                                      # Install deps
cp .env.example .env                         # Configure credentials + DATABASE_URL

uv run python migrations/migrate.py          # Create/update tables
uv run python scraper/scraper.py             # Morning report
uv run python scraper/scraper.py --afternoon # Afternoon check-in
uv run python scraper/scraper.py --evening   # Evening report (includes 24h power profile chart)
uv run python scraper/scraper.py --snapshot  # 20-min snapshot + edge-triggered alerts
uv run python scraper/scraper.py --weekly    # + Weekly report
uv run python scraper/scraper.py --monthly   # + Monthly report

uv run python scraper/test_messages.py       # Send all message types
```

## Project Structure

```
energy-monitor/
├── .github/workflows/
│   └── scraper.yml           # Cron: morning + afternoon + evening + weekly + monthly
├── migrations/
│   ├── 001_initial_schema.sql
│   └── migrate.py            # Run pending migrations against DATABASE_URL
├── scripts/
│   ├── bootstrap_ids.py      # One-shot: society/tower/flat → SITE/UNIT/METER IDs
│   └── probe_api.py          # Dump raw API responses for fixture capture
├── scraper/
│   ├── scraper.py            # Orchestrator + Telegram messages + alert engine
│   ├── api_client.py         # Vendor JSON API client (13 endpoints)
│   ├── normalizer.py         # API → internal contract adapter
│   ├── storage.py            # Postgres persistence layer
│   ├── charts.py             # Matplotlib chart/table image generators
│   └── test_messages.py      # Live-integration harness for all message types
├── pyproject.toml            # Python project config (uv)
├── uv.lock
└── .python-version           # Python version pin (3.11)
```

## Data Storage

Data lives in Neon Postgres across 7 tables plus a migration tracker.

### `daily_readings`

One row per day. Meter readings, consumption, balance, and cost breakdown.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE (PK) | Calendar date |
| `eb_reading` | NUMERIC(10,2) | EB cumulative meter reading (kWh) |
| `eb_consume` | NUMERIC(10,2) | EB consumption for the day (kWh) |
| `dg_reading` | NUMERIC(10,2) | DG cumulative meter reading (kWh) |
| `dg_consume` | NUMERIC(10,2) | DG consumption for the day (kWh) |
| `balance` | NUMERIC(10,2) | Account balance at last scrape (₹) |
| `eb_cost` | NUMERIC(10,2) | EB electricity cost for the day (₹) |
| `dg_cost` | NUMERIC(10,2) | DG electricity cost for the day (₹) |
| `fix_charge_cost` | NUMERIC(10,2) | Daily fixed charge (₹) |
| `updated_at` | TIMESTAMPTZ | Last write timestamp |

**Upsert rules**: meter readings use max-preserve (portal resets to 0 at midnight, never overwrite non-zero with lower value). Balance always updates to latest. Cost fields never overwrite non-zero with zero or NULL.

### `monthly_summaries`

One row per completed month. Write-once; never overwritten.

### `recharges`

Unified recharge history with tolerance-based deduplication (±₹1). Portal-sourced entries shadow balance-jump detections within ±2 days.

### `portal_recharges`

Snapshot of the portal's "Last 10 Recharges" table. Full replace on each run.

### `rates`

Rate card history (EB rate, DG rate, daily fix charge). Append-only, only when rates change.

### `readings` (Phase 2)

Instantaneous snapshots — one row per 20-minute scrape. Columns include `recorded_at`, `active_power_kw`, `apparent_power_kva`, `current_amp`, `voltage_ln`/`voltage_ll`, `power_factor`, `frequency_hz`, `source`, and `balance`. Electrical parameters are NULL when the portal hasn't reported — stored as NULL rather than 0 (the alert engine skips NULL readings instead of treating them as "no draw"). Append-only. Indexed on `recorded_at DESC`.

### `alert_state` (Phase 2)

One row per alert type. Stores `last_fired_at` and a JSONB `context` (e.g., last-fired power level). Used by the edge-triggered alert engine to enforce per-type cooldowns across independent GHA runs.

### Migrations

New schema changes go into `migrations/NNN_description.sql`. Run `uv run python migrations/migrate.py` to apply pending migrations. Applied versions are tracked in the `schema_migrations` table.

## License

MIT
