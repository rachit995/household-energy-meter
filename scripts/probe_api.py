"""One-shot probe of the SmartGridSoft mobile API.

Usage:
    uv run python scripts/probe_api.py --society "Kruti" --tower "A" --flat "101"

Or resolve IDs only (no endpoint sweep):
    uv run python scripts/probe_api.py --society "Kruti" --tower "A" --flat "101" --ids-only

Outputs raw JSON for every probed endpoint to tests/fixtures/<EndpointName>.json
and prints a summary so we can validate API shape against the current HTML scraper's
expectations before committing to the rewrite.

Uses stdlib urllib only — no repo deps required.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "http://103.105.155.227:86/WebServicesMeterData.svc"
FIXTURES_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
TIMEOUT = 30

# Endpoints we care about for parity with the current HTML scraper,
# plus a few probes to resolve unknowns (grace credit, source history).
PROBE_ENDPOINTS = [
    "MeterBasicData",
    "BindElectricParameter",
    "BindSourceRunning",
    "BindCurrentDayDeduction",
    "BindCurrentMonthDeduction",
    "BindPreviousDayDeduction",
    "BindPreviousMonthDeduction",
    "BindPreviousToPreviousMonthDeduction",
    "BindApplicableRates",
    "BindRecharge",
    "CurrentMonthAllUnitView",
    "PreviousMonthAllUnitView",
    "BindOperationalParameters",
    "BindSourceChageover",  # typo is intentional — that's the real endpoint name
    "BindSanctionLoad",
    "BindMeterInformation",
    "BindFreeMonthlyUnits",
    "BindTemperProtection",
    "BindHappyHours",
    "BindHappyDays",
    "BindOnlieRecharge",  # also a typo in the vendor API
]


def get_json(url: str) -> dict | list:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def resolve_society(fragment: str) -> tuple[str, str]:
    print(f"[1/3] GET {BASE_URL}/GetSocietyName ...", flush=True)
    data = get_json(f"{BASE_URL}/GetSocietyName")
    societies = data.get("GetSocietyNameResult", []) if isinstance(data, dict) else []
    if not societies:
        sys.exit("No societies returned — server may be down.")
    frag = fragment.lower()
    matches = [s for s in societies if frag in (s.get("Society_Name") or "").lower()]
    if not matches:
        print(f"No society matched '{fragment}'. First 20 available:", file=sys.stderr)
        for s in societies[:20]:
            print(f"  {s.get('Society_Id'):>6}  {s.get('Society_Name')}", file=sys.stderr)
        print(f"... ({len(societies)} total)", file=sys.stderr)
        sys.exit(1)
    if len(matches) > 1:
        print(f"Multiple societies matched '{fragment}':", file=sys.stderr)
        for s in matches[:20]:
            print(f"  {s.get('Society_Id'):>6}  {s.get('Society_Name')}", file=sys.stderr)
        sys.exit("Refine --society.")
    m = matches[0]
    print(f"      -> {m['Society_Name']} (Society_Id={m['Society_Id']})", flush=True)
    return m["Society_Id"], m["Society_Name"]


def resolve_ids(site_id: str, tower: str, flat: str) -> dict:
    url = f"{BASE_URL}/GetLogin/{urllib.parse.quote(site_id)}/{urllib.parse.quote(tower)}/{urllib.parse.quote(flat)}"
    print(f"[2/3] GET {url}", flush=True)
    data = get_json(url)
    results = data.get("GetLoginResult", []) if isinstance(data, dict) else []
    if not results or not results[0].get("Meter_Id"):
        sys.exit(f"Login failed — no Meter_Id returned. Raw: {json.dumps(data)[:300]}")
    row = results[0]
    print(
        f"      -> Site_Id={row.get('Site_Id')}  Unit_Id={row.get('Unit_Id')}  "
        f"Meter_Id={row.get('Meter_Id')}  Name={row.get('Name')}",
        flush=True,
    )
    return row


def probe(site_id: str, unit_id: str, meter_id: str) -> dict[str, dict]:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict] = {}
    for name in PROBE_ENDPOINTS:
        url = f"{BASE_URL}/{name}/{site_id}/{unit_id}/{meter_id}"
        t0 = time.time()
        try:
            data = get_json(url)
            elapsed = time.time() - t0
            status = "OK"
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            data = {"__error__": str(e)}
            elapsed = time.time() - t0
            status = "ERR"
        fixture_path = FIXTURES_DIR / f"{name}.json"
        fixture_path.write_text(json.dumps(data, indent=2))
        result_key = next((k for k in (data if isinstance(data, dict) else {}) if k.endswith("Result")), None)
        rows = len(data.get(result_key, [])) if result_key and isinstance(data.get(result_key), list) else "?"
        print(f"  {status}  {elapsed*1000:>6.0f}ms  rows={rows:<3}  {name}", flush=True)
        results[name] = data
    return results


def summarize(site_id: str, login_row: dict, probes: dict[str, dict]) -> None:
    # Runtime scraper uses Society_Id (from GetSocietyName) as its SiteId path
    # parameter, NOT the login payload's Site_Id. Print the correct one so
    # anyone running `probe_api.py --ids-only` doesn't cache the wrong value.
    print("\n=== Summary ===")
    print(f"SMARTGRID_SITE_ID={site_id}")
    print(f"SMARTGRID_UNIT_ID={login_row.get('Unit_Id')}")
    print(f"SMARTGRID_METER_ID={login_row.get('Meter_Id')}")

    meter_basic = probes.get("MeterBasicData", {})
    mb_rows = meter_basic.get("MeterBasicDataResult", [])
    if mb_rows:
        r = mb_rows[0]
        print("\n-- MeterBasicData[0] --")
        for k, v in r.items():
            print(f"  {k}: {v!r}")

    ep = probes.get("BindElectricParameter", {})
    ep_rows = ep.get("BindElectricParameterResult", [])
    if ep_rows:
        print("\n-- BindElectricParameter[0] (live power) --")
        for k, v in ep_rows[0].items():
            print(f"  {k}: {v!r}")

    op = probes.get("BindOperationalParameters", {})
    op_rows = op.get("BindOperationalParametersResult", [])
    if op_rows:
        print("\n-- BindOperationalParameters[0] (grace_credit search) --")
        for k, v in op_rows[0].items():
            print(f"  {k}: {v!r}")

    rc = probes.get("BindRecharge", {})
    rc_rows = rc.get("BindRechargeResult", [])
    print(f"\n-- BindRecharge: {len(rc_rows)} rows --")
    for row in rc_rows[:3]:
        print(f"  {row}")

    errors = {k: v for k, v in probes.items() if isinstance(v, dict) and "__error__" in v}
    if errors:
        print(f"\n-- Errors ({len(errors)}) --")
        for k, v in errors.items():
            print(f"  {k}: {v['__error__']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--society", required=True, help="Case-insensitive substring of Society_Name")
    ap.add_argument("--tower", required=True)
    ap.add_argument("--flat", required=True)
    ap.add_argument("--ids-only", action="store_true", help="Resolve IDs only, skip endpoint sweep")
    args = ap.parse_args()

    site_id, _ = resolve_society(args.society)
    login_row = resolve_ids(site_id, args.tower, args.flat)

    if args.ids_only:
        print("\nSet these as GitHub secrets:")
        print(f"  SMARTGRID_SITE_ID={site_id}")
        print(f"  SMARTGRID_UNIT_ID={login_row.get('Unit_Id')}")
        print(f"  SMARTGRID_METER_ID={login_row.get('Meter_Id')}")
        return

    print(f"[3/3] Probing {len(PROBE_ENDPOINTS)} endpoints (fixtures -> {FIXTURES_DIR.relative_to(Path.cwd())}/)...", flush=True)
    # Downstream endpoints expect Society_Id as the SiteId path param, NOT
    # the login response's Site_Id. Use the value resolved from GetSocietyName.
    probes = probe(site_id, login_row["Unit_Id"], login_row["Meter_Id"])
    summarize(site_id, login_row, probes)


if __name__ == "__main__":
    main()
