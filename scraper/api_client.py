"""SmartGridSoft mobile API client.

Reverse-engineered from the vendor's Android app (com.smartgridsoft.kruti.SGS v1.36).
The server at 103.105.155.227:86 exposes a WCF service with no auth; knowing the
triple (SiteId, UnitId, MeterId) is sufficient to read all meter data.

Each endpoint is tagged critical or optional. Critical failures raise ApiError;
optional failures return None after retries so a single flaky endpoint cannot
take down the whole scrape.
"""
from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "http://103.105.155.227:86/WebServicesMeterData.svc"

CRITICAL_ENDPOINTS = frozenset({
    "MeterBasicData",
    "BindElectricParameter",
    "BindCurrentDayDeduction",
    "BindCurrentMonthDeduction",
    "CurrentMonthAllUnitView",
})
# ``PreviousMonthAllUnitView`` is intentionally optional even though the
# monthly report technically needs it on day 1 of each month. Making it
# critical for every cron tick would be an availability regression — the
# snapshot/morning/afternoon/evening paths all work off current-month data.
# When the monthly report does run, historical data is already persisted in
# ``daily_readings`` from prior scrapes and read back via ``load_daily_readings``.


class ApiError(RuntimeError):
    pass


class SmartGridClient:
    def __init__(
        self,
        site_id: str,
        unit_id: str,
        meter_id: str,
        base_url: str = BASE_URL,
        timeout: tuple[float, float] = (10.0, 30.0),
    ) -> None:
        self.site_id = str(site_id)
        self.unit_id = str(unit_id)
        self.meter_id = str(meter_id)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Accept": "application/json"})

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "SmartGridClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint}/{self.site_id}/{self.unit_id}/{self.meter_id}"

    def _get(self, endpoint: str, critical: bool | None = None) -> dict[str, Any] | None:
        if critical is None:
            critical = endpoint in CRITICAL_ENDPOINTS
        url = self._url(endpoint)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            if critical:
                raise ApiError(f"{endpoint} failed: {exc}") from exc
            logger.warning("optional endpoint %s failed, continuing with None: %s", endpoint, exc)
            return None
        # Critical endpoints must return a dict envelope. Bare JSON `null` or
        # a list would otherwise slip past downstream `.get(...)` and produce
        # silent None fields throughout the scrape.
        if critical and not isinstance(data, dict):
            raise ApiError(f"{endpoint} returned non-dict JSON: {type(data).__name__}")
        return data

    def _get_critical(self, endpoint: str) -> dict[str, Any]:
        """Type-narrowing wrapper for critical endpoints. ``_get`` is typed
        ``dict | None`` for the optional case, but on the critical path it
        either returns a dict or raises — the ``assert`` defends against a
        stale ``CRITICAL_ENDPOINTS`` set (e.g. an endpoint rename that skips
        updating the constant)."""
        data = self._get(endpoint, critical=True)
        assert data is not None, f"{endpoint} classified critical but returned None"
        return data

    # --- Critical endpoints (raise on failure) ---
    def meter_basic_data(self) -> dict[str, Any]:
        return self._get_critical("MeterBasicData")

    def electric_parameter(self) -> dict[str, Any]:
        return self._get_critical("BindElectricParameter")

    def current_day_deduction(self) -> dict[str, Any]:
        return self._get_critical("BindCurrentDayDeduction")

    def current_month_deduction(self) -> dict[str, Any]:
        return self._get_critical("BindCurrentMonthDeduction")

    def current_month_all_unit_view(self) -> dict[str, Any]:
        return self._get_critical("CurrentMonthAllUnitView")

    # --- Optional endpoints (return None on failure) ---
    def previous_month_all_unit_view(self) -> dict[str, Any] | None:
        return self._get("PreviousMonthAllUnitView")

    def previous_day_deduction(self) -> dict[str, Any] | None:
        return self._get("BindPreviousDayDeduction")

    def previous_month_deduction(self) -> dict[str, Any] | None:
        return self._get("BindPreviousMonthDeduction")

    def previous_to_previous_month_deduction(self) -> dict[str, Any] | None:
        return self._get("BindPreviousToPreviousMonthDeduction")

    def applicable_rates(self) -> dict[str, Any] | None:
        return self._get("BindApplicableRates")

    def recharge(self) -> dict[str, Any] | None:
        return self._get("BindRecharge")

    def operational_parameters(self) -> dict[str, Any] | None:
        return self._get("BindOperationalParameters")

    def source_running(self) -> dict[str, Any] | None:
        """Live source flag (``"0"`` = EB / Full Load, ``"1"`` = DG / Generator).

        We use this rather than ``BindSourceChageover`` because the latter was
        only probed in EB mode; its DG-mode string is unverified and can't be
        trusted to match ``_source_display()``'s `"full load"/"eb"/"grid"`
        EB-branch token set.
        """
        return self._get("BindSourceRunning")

    def fetch_all(self) -> dict[str, dict[str, Any] | None]:
        """Fetch every endpoint the normalizer consumes.

        Critical calls run first so their exceptions propagate before we spend
        time on optionals. Returns a dict keyed by endpoint name, with None for
        optional endpoints that failed.
        """
        return {
            "MeterBasicData": self.meter_basic_data(),
            "BindElectricParameter": self.electric_parameter(),
            "BindCurrentDayDeduction": self.current_day_deduction(),
            "BindCurrentMonthDeduction": self.current_month_deduction(),
            "CurrentMonthAllUnitView": self.current_month_all_unit_view(),
            "PreviousMonthAllUnitView": self.previous_month_all_unit_view(),
            "BindPreviousDayDeduction": self.previous_day_deduction(),
            "BindPreviousMonthDeduction": self.previous_month_deduction(),
            "BindPreviousToPreviousMonthDeduction": self.previous_to_previous_month_deduction(),
            "BindApplicableRates": self.applicable_rates(),
            "BindRecharge": self.recharge(),
            "BindOperationalParameters": self.operational_parameters(),
            "BindSourceRunning": self.source_running(),
        }
