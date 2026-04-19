"""
Fetches real FHIR R4 clinical bundles from the public HAPI FHIR server
and writes them as batch files (mimicking daily EHR drops).

Server: https://hapi.fhir.org/baseR4
- Free, no auth required
- Contains synthetic but structurally-correct FHIR resources
- Resources: Patient, Condition, MedicationRequest, Observation, DiagnosticReport

This replaces ``data_generators/synthetic/ehr_generator.py`` with REAL
FHIR-structured data.

Polling strategy:
- Fetch N patients with their conditions, medications, and observations
- Write as JSON bundles to ``<settings.ehr_batch_dir>/YYYY-MM-DD/``
- Each batch = 1 day of hospital EHR export
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from fhir.resources.bundle import Bundle

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_failed,
    records_processed,
    start_metrics_server,
)

log = get_logger(__name__)

FHIR_HEADERS = {"Accept": "application/fhir+json"}


def _to_dict(resource: Any) -> dict:
    """Pydantic v1/v2 compatible serializer for fhir.resources models."""
    if hasattr(resource, "model_dump"):
        return resource.model_dump(mode="json", exclude_none=True)
    return json.loads(resource.json(exclude_none=True))


def _parse_bundle(text: str) -> Bundle:
    if hasattr(Bundle, "model_validate_json"):
        return Bundle.model_validate_json(text)
    return Bundle.parse_raw(text)


class FHIRBatchProducer:
    def __init__(self, output_dir: str | None = None, request_delay: float = 0.2):
        self.base_url = settings.hapi_fhir_base_url.rstrip("/")
        self.output_dir = Path(output_dir or settings.ehr_batch_dir)
        self.request_delay = request_delay

    # ── HTTP ───────────────────────────────────────────────────────────────
    @retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
    def _request(self, url: str, params: dict | None = None) -> str:
        """Single HTTP GET against the FHIR server. Raises on non-2xx."""
        resp = requests.get(url, params=params or {}, headers=FHIR_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _get_bundle(self, path: str, params: dict | None = None) -> Bundle | None:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            text = self._request(url, params)
        except requests.RequestException as exc:
            records_failed.labels(layer="bronze", source="fhir", reason="api_error").inc()
            log.error("HAPI FHIR fetch failed",
                      extra={"extra_data": {"url": url, "error": str(exc)}})
            return None
        time.sleep(self.request_delay)
        return _parse_bundle(text)

    @staticmethod
    def _entries(bundle: Bundle | None) -> list:
        return [e.resource for e in (bundle.entry or [])] if bundle else []

    # ── Resource fetchers ──────────────────────────────────────────────────
    def fetch_patients(self, count: int = 50) -> list:
        bundle = self._get_bundle("Patient", {"_count": count})
        return self._entries(bundle)

    def fetch_conditions_for_patient(self, patient_id: str) -> list:
        bundle = self._get_bundle(
            "Condition",
            {"patient": patient_id, "_count": 20},
        )
        return self._entries(bundle)

    def fetch_medications_for_patient(self, patient_id: str) -> list:
        bundle = self._get_bundle(
            "MedicationRequest",
            {"patient": patient_id, "_count": 20},
        )
        return self._entries(bundle)

    def fetch_observations_for_patient(self, patient_id: str) -> list:
        bundle = self._get_bundle(
            "Observation",
            {"patient": patient_id, "_count": 20},
        )
        return self._entries(bundle)

    # ── Batch generation ───────────────────────────────────────────────────
    def generate_daily_batch(
        self,
        date_str: str | None = None,
        patient_count: int = 50,
    ) -> Path:
        """Generate one day's EHR batch from FHIR server."""
        date_str = date_str or datetime.utcnow().strftime("%Y-%m-%d")
        batch_dir = self.output_dir / date_str
        batch_dir.mkdir(parents=True, exist_ok=True)

        log.info("FHIR batch starting",
                 extra={"extra_data": {"date": date_str, "patients": patient_count}})

        patients = self.fetch_patients(patient_count)
        bundles: list[dict] = []
        for patient in patients:
            pid = getattr(patient, "id", None)
            if not pid:
                continue
            conditions = self.fetch_conditions_for_patient(pid)
            medications = self.fetch_medications_for_patient(pid)
            observations = self.fetch_observations_for_patient(pid)
            bundles.append({
                "patient": _to_dict(patient),
                "conditions":   [_to_dict(c) for c in conditions],
                "medications":  [_to_dict(m) for m in medications],
                "observations": [_to_dict(o) for o in observations],
            })
            records_processed.labels(layer="bronze", source="fhir").inc()

        out_path = batch_dir / "ehr_batch.json"
        out_path.write_text(json.dumps({
            "batch_date": date_str,
            "patient_count": len(bundles),
            "patients": bundles,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "hapi_fhir_r4",
        }, indent=2, default=str))

        log.info("FHIR batch written",
                 extra={"extra_data": {
                     "path": str(out_path),
                     "patients": len(bundles),
                 }})
        return out_path

    def run(self, patient_count: int = 50) -> None:
        try:
            self.generate_daily_batch(patient_count=patient_count)
        except KeyboardInterrupt:
            log.info("FHIR batch interrupted")


def main():
    start_metrics_server(8002)
    FHIRBatchProducer().run()


if __name__ == "__main__":
    main()
