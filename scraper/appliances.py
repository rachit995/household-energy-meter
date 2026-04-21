"""Appliance catalog + derived alert thresholds.

Centralises the household's known loads so alert thresholds and
cost attribution have narrative justification instead of magic
numbers. When appliances change, update this file — downstream
thresholds follow.

Thresholds are tuned empirically from a rolling window of observed
snapshot data in the `readings` table. If they feel too noisy /
too silent in practice, adjust here.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Appliance:
    name: str
    rated_w: int
    cycles: bool              # True for thermostat-controlled loads
    notes: str = ""


# Inventory. Update when an appliance is added / replaced.
AC_1    = Appliance("AC 1", 1920, False, "1-star non-inverter, 1.5 ton")
AC_2    = Appliance("AC 2", 1920, False, "1-star non-inverter, 1.5 ton")
# AC_3 rated_w is inferred from the model's nameplate lineage; label
# photo still pending. Keep 1600 W internal-only — do not use it in
# user-facing copy until confirmed.
AC_3    = Appliance("AC 3", 1600, False, "3-star non-inverter, 1.5 ton (nameplate pending)")
GEYSER_1 = Appliance("Geyser 1", 2000, True, "15 L storage, thermostat cycles")
GEYSER_2 = Appliance("Geyser 2", 2000, True, "15 L storage, thermostat cycles")
FRIDGE   = Appliance("Fridge", 60, True, "Large frost-free, ~40-60 W avg, ~270 W defrost")
TV       = Appliance("TV", 120, False, "OLED, typical viewing 80-200 W")

# Infrequent-use appliances — inventory only, nameplate pending. Do NOT use
# these in threshold derivation (they run too rarely to shift the baseline
# or major-load floor).
WASHING_MACHINE = Appliance("Washing machine", 500, True,
                            "Infrequent use, cycles during wash/rinse/spin")
IRON            = Appliance("Iron", 1400, True,
                            "Infrequent, short bursts, thermostat cycles")
MICROWAVE       = Appliance("Microwave", 1200, False,
                            "Infrequent, short bursts")

ALL_APPLIANCES = [AC_1, AC_2, AC_3,
                  GEYSER_1, GEYSER_2, FRIDGE, TV,
                  WASHING_MACHINE, IRON, MICROWAVE]


# Baseline: fridge + router + small always-on. Empirical quiet-floor
# lands around 0.2 kW; use that as the baseline-floor assumption.
BASELINE_KW = 0.20

# Power draw above this (but below MAX_PLAUSIBLE_POWER_KW) is attributable
# to at least one "major load" turning on — AC, geyser, or similar. Used
# for coarse-bucket cost attribution. Set just above the largest baseline
# excursion typically observed.
MAJOR_LOAD_FLOOR_KW = 0.80


# --- Alert thresholds grounded in empirical observation ---
#
# Fires when power crosses this from below. The prior 2.5 kW was too
# low — it fired on any normal evening with a single AC running.
# 3.5 kW corresponds to "two major loads overlapping" (AC + geyser,
# or two ACs) which is what we actually want to flag.
HIGH_POWER_KW_THRESHOLD = 3.5

# Fires when power stays >= this for 2+ hours. Prior 2.5 kW would
# fire every evening (single AC + baseline). 3.0 kW means "two major
# loads running together for hours" — genuinely actionable.
SUSTAINED_LOAD_KW = 3.0

# Fires between 00:00 and 05:00 IST if power > this. Prior 1.0 kW was
# too low to be useful when an AC running alone overnight is a normal
# state. 2.5 kW corresponds to "two major loads at night" — unusual
# enough to warrant the alert.
#
# NOTE: does NOT fix the midnight-boundary blind spot (if load is
# already above threshold at 00:00, prev-transition check suppresses
# the alert). Separate feature, deferred.
NIGHT_ANOMALY_KW = 2.5
