# Energy Monitor - Claude Code Guide

## Project Overview

This is an energy meter monitoring system that:
1. **Fetches** data from the SmartGridSoft prepaid-meter vendor's undocumented JSON API (reverse-engineered from their Android app; no auth required once three meter IDs are bootstrapped)
2. **Snapshots** instantaneous power draw every 10 minutes (Phase 2)
3. **Notifies** via Telegram bot (morning, afternoon, evening, weekly, monthly reports)
4. **Alerts** on anomalies — daily (spikes, DG usage, rate changes, low balance) and real-time (heavy draw, sustained load, night anomalies)
5. **Stores** historical data in Neon Postgres
6. **Runs** on GitHub Actions cron (free tier, unlimited for public repos)

## Tech Stack

| Component | Technology | Hosting |
|-----------|------------|---------|
| Scraper | Python + requests (JSON API client) + Sentry | GitHub Actions (cron) |
| Notifications | Telegram Bot API | — |
| Data Storage | Neon Postgres | Neon free tier |
| Python Deps | uv | Local / CI |

## Project Structure

```
energy-monitor/
├── .github/workflows/
│   └── scraper.yml             # Cron: morning + afternoon + evening + weekly + monthly
├── migrations/
│   ├── 001_initial_schema.sql  # Schema definitions (numbered SQL files)
│   └── migrate.py              # Runs pending migrations against DATABASE_URL
├── scripts/
│   ├── bootstrap_ids.py        # One-shot: resolve SITE/UNIT/METER IDs from tower+flat
│   └── probe_api.py            # Dump raw API responses to tests/fixtures/
├── scraper/
│   ├── scraper.py              # Main orchestrator + Telegram messages + alerts
│   ├── api_client.py           # SmartGridSoft JSON API client (13 endpoints)
│   ├── normalizer.py           # API → 13-tuple adapter; preserves HTML-era contract
│   ├── storage.py              # Postgres persistence layer (psycopg2)
│   ├── charts.py               # Matplotlib chart/table image generators
│   └── test_messages.py        # Live-integration harness for all message types
├── tests/
│   ├── fixtures/               # Captured API responses (21 endpoints)
│   ├── golden/                 # Pinned Telegram message strings
│   ├── test_api_client.py
│   ├── test_normalizer.py
│   └── test_messages_golden.py
├── pyproject.toml              # Python project config (uv)
├── uv.lock                     # Python dependency lockfile
├── .python-version             # Python version pin (3.11)
└── .env.example                # Environment template
```

## Reports & Snapshots

| Mode | Schedule | Content |
|------|----------|---------|
| Snapshot | Every 10 min (`:05,:15,:25,:35,:45,:55`) | Saves instantaneous power draw + balance to `readings` table; runs edge-triggered alerts; no Telegram unless an alert fires |
| Morning | 6:30 AM IST daily | Balance, yesterday's deductions vs 7d avg & last week, current month, balance runway |
| Afternoon | 5:30 PM IST daily | Live power draw, DG/EB source, today's spend + projection, budget pace |
| Evening | 10:00 PM IST daily | Balance, today's deductions, balance runway, 14-day spend trend chart, **24h power profile chart** (Phase 2) |
| Weekly | Monday morning | Week total, highest/lowest day, WoW comparison, DG days |
| Monthly | 1st of month | Full month stats, weekday pattern, recharges, 6-month history |

## Alerts

**Daily alerts** (run during scheduled reports):

| Alert | Trigger | Frequency |
|-------|---------|-----------|
| Recharge Prediction | Balance < 7 days of spend | Every run |
| Grace Period | Balance goes negative | Every run |
| Consumption Spike | Today > 150% of 7-day avg | Evening |
| DG Usage | DG cost > 0 today | Evening |
| Rate Change | EB/DG/fix rate changes | Every run |
| Fix Charge Anomaly | Fix charge > 120% of daily rate | Evening |
| Recharge Analysis | New recharge detected in portal | Evening |
| Budget | 50/75/90% of monthly budget | Morning + Evening |

**Real-time alerts** (Phase 2 — edge-triggered, run every snapshot):

| Alert | Trigger | Cooldown |
|-------|---------|----------|
| High Power Draw | Power crosses 2.5 kW from below | 1 hour |
| Sustained Load | Power ≥ 2.5 kW continuously for 2+ hours (≥ 10 of 12 samples valid) | 4 hours |
| Night Anomaly | Power > 1.0 kW between 00:00 and 05:00 IST | 2 hours |

Cooldowns are stored in `alert_state` table so re-firing is suppressed across independent GHA runs. Thresholds are centralised as constants at the top of `scraper.py`'s Phase 2 section — easy to calibrate once real data is collected.

## Data Storage

Data lives in Neon Postgres. 7 tables plus a `schema_migrations` tracker.

### Tables

**Phase 1 (daily-level aggregates):**
- **`daily_readings`** (PK `date`) — meter readings, consumption, balance, cost breakdown per day
- **`monthly_summaries`** (PK `month`) — write-once per completed month
- **`recharges`** (UNIQUE `(date, amount)`) — unified recharge history (balance-jump + portal-sourced), ±₹1 tolerance dedup
- **`portal_recharges`** — snapshot of portal's "Last 10 Recharges", full replace each run
- **`rates`** — rate card history, append-only on change

**Phase 2 (10-min high-frequency data):**
- **`readings`** (BIGSERIAL PK, indexed on `recorded_at DESC`) — one row per snapshot run with instantaneous power, voltage, current, PF, frequency, source, balance. NULL power values are preserved (not defaulted to 0).
- **`alert_state`** (PK `alert_type`) — last-fire timestamp + JSONB context per alert type; used by the edge-triggered alert engine to enforce cooldowns across stateless GHA runs.

### Integrity Rules (enforced in `storage.py`)

- `daily_readings`: max-preserve for all 4 meter fields (portal resets to 0 at midnight, never overwrite non-zero with lower value). Balance uses midnight-reset guard: existing balance is kept if the new value is NULL, 0, or < 10% of existing. Cost fields never overwrite non-zero with zero or NULL.
- `monthly_summaries`: `ON CONFLICT DO NOTHING` on month — write-once.
- `recharges`: ±₹1 tolerance dedup, portal entries shadow balance-jump within ±2 days.
- `portal_recharges`: full replace in a single transaction (atomic DELETE + INSERTs).
- `rates`: SELECT-compare-INSERT (SERIAL PK — `ON CONFLICT DO NOTHING` would never fire).
- `readings`: append-only. Electrical params preserved as NULL when the portal hasn't reported (common — the alert engine treats NULL as "skip", not zero).
- `alert_state`: upsert by `alert_type`. JSONB context stored via `psycopg2.extras.Json()` wrapper. Truncate the table to force-re-enable all alerts.

### Migrations

New schema changes go into `migrations/NNN_description.sql`. Run `uv run python migrations/migrate.py` to apply pending migrations. Applied versions are tracked in `schema_migrations`.

## Environment Variables

### GitHub Secrets (PROD environment)

All identifying values go in **Secrets** (auto-masked in public action logs) — not Variables, which render as plaintext. This matters because the repo is public. The API itself has no auth; the three IDs below effectively identify a meter publicly on the vendor's server, but as secrets they at least stay out of action logs.

Runtime secrets:
- `SMARTGRID_SITE_ID` — society identifier from `GetSocietyName`
- `SMARTGRID_UNIT_ID` — from `GetLogin`
- `SMARTGRID_METER_ID` — from `GetLogin`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DATABASE_URL` — Neon Postgres connection string

Bootstrap-only (local `.env`, not needed in GHA):
- `SMARTGRID_COMPANY` / `SMARTGRID_TOWER` / `SMARTGRID_FLAT` — used by `scripts/bootstrap_ids.py` to resolve the three runtime IDs above

### Optional (env vars or .env)

- `LOG_LEVEL` - Logging verbosity (default: INFO). Set to `DEBUG` locally to see balance/power values in logs; leave at INFO for CI since those values would land in public logs.
- `SENTRY_DSN` - Sentry project DSN for private error monitoring (leave empty to disable; free tier at sentry.io, 5K errors/month)
- `SENTRY_ENVIRONMENT` - Sentry environment tag (default: `production`)
- `MONTHLY_BUDGET` - Monthly budget for tracking (default: 8000)
- `SPIKE_THRESHOLD` - Consumption spike multiplier (default: 1.5)

## Local Development

```bash
uv sync --extra dev                          # Install runtime + test deps
cp .env.example .env                         # Configure IDs + DATABASE_URL + Telegram

# First-time only: resolve the three meter IDs from tower + flat.
uv run python scripts/bootstrap_ids.py --society "Your Society" --tower "A" --flat "101"

uv run python migrations/migrate.py          # Create/update tables
uv run python scraper/scraper.py             # Morning report
uv run python scraper/scraper.py --afternoon # Afternoon check-in
uv run python scraper/scraper.py --evening   # Evening report (includes 24h power profile chart)
uv run python scraper/scraper.py --snapshot  # 10-min snapshot + edge-triggered alert check
uv run python scraper/scraper.py --weekly    # + Weekly report
uv run python scraper/scraper.py --monthly   # + Monthly report

uv run python scraper/test_messages.py       # Send all message types
uv run pytest tests/ -v                      # Unit + contract + golden tests
```

## Troubleshooting

### Scraper fails
- Check GitHub Actions logs
- Verify credentials in GitHub secrets/variables, including `DATABASE_URL`
- Test locally with `uv run python scraper/scraper.py`

### Telegram not sending
- Verify `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set
- Ensure the bot has been started (send /start to it)

### Database errors
- Check Neon dashboard for project status (free tier suspends compute after 5 min idle; cold start adds ~1s)
- Verify `DATABASE_URL` ends with `?sslmode=require`
- Run `uv run python migrations/migrate.py` — it's idempotent and will apply any pending schema changes

### Data shows zero
- Portal resets values around midnight — morning run at 6:30 AM should have correct data
- Storage layer uses max-preserve on all meter fields, protecting against midnight resets
