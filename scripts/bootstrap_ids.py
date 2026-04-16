"""Resolve SmartGridSoft meter IDs for GitHub-secret configuration.

The vendor API at 103.105.155.227:86 has no auth but does need three IDs on
every call: (SiteId, UnitId, MeterId). They're static per flat and only need
to be looked up once. Run this script after every move/onboarding; store the
output as GitHub secrets; the scraper never logs in again.

Usage:
    uv run python scripts/bootstrap_ids.py --society "<your society>" --tower "<tower>" --flat "<flat>"

Caveats:
- ``--society`` is a case-insensitive substring matched against GetSocietyName.
- The API returns a ``Site_Id`` inside the login payload, but every downstream
  endpoint uses the ``Society_Id`` from GetSocietyName as its SiteId path
  parameter. This script prints the correct one.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "http://103.105.155.227:86/WebServicesMeterData.svc"
TIMEOUT = 30


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def resolve_society_id(fragment: str) -> tuple[str, str]:
    data = _get(f"{BASE_URL}/GetSocietyName")
    societies = data.get("GetSocietyNameResult", []) if isinstance(data, dict) else []
    if not societies:
        sys.exit("GetSocietyName returned no rows — server may be down.")
    needle = fragment.lower()
    matches = [s for s in societies if needle in (s.get("Society_Name") or "").lower()]
    if not matches:
        print(f"No society matched '{fragment}'. First 20 available:", file=sys.stderr)
        for s in societies[:20]:
            print(f"  {s.get('Society_Id'):>6}  {s.get('Society_Name')}", file=sys.stderr)
        sys.exit(1)
    if len(matches) > 1:
        print(f"Multiple societies matched '{fragment}':", file=sys.stderr)
        for s in matches[:20]:
            print(f"  {s.get('Society_Id'):>6}  {s.get('Society_Name')}", file=sys.stderr)
        sys.exit("Refine --society with a more specific substring.")
    m = matches[0]
    return m["Society_Id"], m["Society_Name"]


def resolve_unit_meter(site_id: str, tower: str, flat: str) -> tuple[str, str, str]:
    url = f"{BASE_URL}/GetLogin/{urllib.parse.quote(site_id)}/{urllib.parse.quote(tower)}/{urllib.parse.quote(flat)}"
    data = _get(url)
    rows = data.get("GetLoginResult", []) if isinstance(data, dict) else []
    if not rows or not rows[0].get("Meter_Id"):
        sys.exit(f"Login failed — tower/flat not found. Raw: {json.dumps(data)[:300]}")
    row = rows[0]
    return row["Unit_Id"], row["Meter_Id"], row.get("Name", "")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--society", required=True, help="Case-insensitive substring of Society_Name")
    ap.add_argument("--tower", required=True)
    ap.add_argument("--flat", required=True)
    args = ap.parse_args()

    try:
        site_id, society_name = resolve_society_id(args.society)
        unit_id, meter_id, tenant_name = resolve_unit_meter(site_id, args.tower, args.flat)
    except urllib.error.URLError as exc:
        sys.exit(f"Network error contacting {BASE_URL}: {exc}")

    print(f"# Society : {society_name}")
    print(f"# Flat    : {args.tower}/{args.flat} ({tenant_name or '—'})")
    print()
    print(f"SMARTGRID_SITE_ID={site_id}")
    print(f"SMARTGRID_UNIT_ID={unit_id}")
    print(f"SMARTGRID_METER_ID={meter_id}")
    print()
    print("# Add these three as GitHub secrets. No other SMARTGRID_* secret")
    print("# is needed at runtime once the API rewrite lands.")


if __name__ == "__main__":
    main()
