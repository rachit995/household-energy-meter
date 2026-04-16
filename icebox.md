# Icebox — Future Ideas

Parked features and ideas that aren't part of the current roadmap. Graduate to a real plan when priorities shift.

## Away Mode

**Idea**: Telegram slash commands (`/away`, `/home`, `/status`) to toggle alerting on any power draw above baseline — useful when travelling.

**Why parked**: GitHub Actions cron can't listen for Telegram webhooks (stateless). Polling for bot updates at each run means up to 10-min delay for a command to take effect. Better to build this when we have a persistent compute runtime (Oracle VM, Cloudflare Worker, etc.) — or accept the delay if the feature becomes critical.

**Graduation trigger**: User explicitly travelling and wants push-button away toggle.

## AC Season Toggle

**Idea**: Suppress "night anomaly" alerts during summer months (AC may legitimately run all night). Either manual toggle (`/summer`, `/winter`) or auto-detect from sustained overnight draw patterns.

**Why parked**: Adds complexity before we know whether false positives are actually a problem. Phase 2 alerts should run for a few weeks first to see real false-positive rates.

**Graduation trigger**: User reports getting night alerts for known AC-on nights.

## Baseline-Exceeded Alert (per time-of-day)

**Idea**: Alert when current power exceeds 2× the median power for this hour-of-day + day-of-week. Catches "unusual load pattern" without per-appliance identification.

**Why parked**: Needs 2-3 weeks of stored `readings` data to compute reliable baselines. Phase 2 should ship without it; add once we have the data.

**Graduation trigger**: 3+ weeks of 10-min readings accumulated.

## Power Factor Load Classification

**Idea**: Use PF delta on power step-ups to classify loads:
- PF drop > 0.05 → motor load (AC, compressor)
- PF stable/rising → resistive load (geyser, OTG, heater)

Useful as a supplementary hint in alert messages ("heavy motor load detected").

**Why parked**: PF signatures are diluted at whole-apartment level; needs empirical tuning. Ship core alerts first, add this as a v2 enhancement.

## Weather Correlation

**Idea**: Pull outdoor temperature from a free weather API, correlate with AC usage. Use temperature as a regressor for "expected consumption" — catches anomalies more accurately than a flat 7-day average.

**Why parked**: External API dependency + seasonal model complexity. Current 7-day rolling avg is good enough.

## Meter Offline Detection

**Idea**: If `last_sync` from the portal is > 1 hour stale, alert — means the physical meter has lost connectivity.

**Why parked**: Nice to have, not urgent. Portal usually recovers on its own within a few hours.

## Persistent Compute (Oracle Cloud VM)

**Idea**: Move from GitHub Actions cron to an always-on process on Oracle Always-Free ARM VM. Enables:
- Session reuse (fewer portal logins)
- In-memory alert state
- Telegram webhook listener for away mode
- Sub-10-min polling if needed
- Anti-idle keepalive to prevent VM reclamation

**Why parked**: GHA free tier on public repo is sufficient for current 10-min polling. Revisit if portal rate-limits us or we need real-time features.

## Daily JSON Export → Git Backup

**Idea**: Nightly cron exports Neon to JSON, commits to a backup branch/repo. Belt-and-suspenders redundancy in case Neon free tier changes.

**Why parked**: Phase 1 explicitly moved off JSON-in-git; re-adding it would be regression. Neon has their own backup options if redundancy becomes critical.
