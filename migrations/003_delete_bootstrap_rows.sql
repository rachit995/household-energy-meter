-- Migration 003: Delete seed/bootstrap rows from initial table population
--
-- 14 rows (ids 24-37) were inserted during initial setup with a placeholder
-- active_power_kw value and NULL last_sync. They pollute histograms and
-- load-profile analysis. Using exact IDs (not a predicate on last_sync /
-- active_power_kw) to avoid matching legitimate NULL-power rows created
-- by later sync stalls.

DELETE FROM readings WHERE id IN (24,25,26,27,28,29,30,31,32,33,34,35,36,37);
