"""REST-style reference data client (SOURCE 5: regions, warehouses, exchange rates).

Two transports behind one interface:

* `http://...`  - a real HTTP API (the mock server in `api/reference_api.py` in Docker),
                  with retries + exponential backoff on 5xx/429/connection errors and
                  page-by-page fetching.
* `file://...`  - the generated response documents on disk (default for local runs / CI),
                  so the pipeline never depends on a network service to be reproducible.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx

from ...config import REPO_ROOT
from ...logging_utils import get_logger
from .base import ReadResult, SourceError, records_to_string_table

log = get_logger("ingestion.api")

RESOURCES = {
    "regions": {"path": "/reference/regions", "file": "regions.json", "dated": False},
    "warehouses": {"path": "/reference/warehouses", "file": "warehouses.json", "dated": False},
    "exchange_rates": {
        "path": "/reference/exchange-rates",
        "file": "exchange_rates_{as_of}.json",
        "dated": True,
    },
}


class ReferenceApiClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        max_retries: int = 4,
        backoff: float = 0.5,
        page_size: int = 2000,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.page_size = page_size

    # ------------------------------------------------------------------ public
    def fetch(self, resource: str, as_of: str | None = None) -> ReadResult:
        if resource not in RESOURCES:
            raise SourceError(f"unknown reference resource '{resource}'")
        spec = RESOURCES[resource]
        if spec["dated"] and not as_of:
            raise SourceError(f"resource '{resource}' requires as_of")
        if self.base_url.startswith("file://"):
            records, meta = self._fetch_file(spec, as_of)
        elif self.base_url.startswith("http://") or self.base_url.startswith("https://"):
            records, meta = self._fetch_http(spec, as_of)
        else:
            raise SourceError(f"unsupported REFERENCE_API_URL scheme: {self.base_url}")
        table = records_to_string_table(records)
        return ReadResult(table=table, stats={"resource": resource, "as_of": as_of, **meta})

    # ------------------------------------------------------------------- file
    def _fetch_file(self, spec: dict[str, Any], as_of: str | None) -> tuple[list[dict], dict]:
        root = self.base_url[len("file://") :]
        base = Path(root)
        if not base.is_absolute():
            base = (REPO_ROOT / base).resolve()
        path = base / spec["file"].format(as_of=as_of)
        if not path.exists():
            raise SourceError(f"reference API document not found: {path}")
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        return doc.get("data", []), {
            "transport": "file",
            "source": str(path),
            "declared_count": doc.get("count"),
        }

    # ------------------------------------------------------------------- http
    def _fetch_http(self, spec: dict[str, Any], as_of: str | None) -> tuple[list[dict], dict]:
        records: list[dict] = []
        page = 1
        pages_fetched = 0
        with httpx.Client(timeout=self.timeout) as client:
            while True:
                params = {"page": page, "page_size": self.page_size}
                if as_of:
                    params["as_of"] = as_of
                doc = self._get_with_retry(client, self.base_url + spec["path"], params)
                records.extend(doc.get("data", []))
                pages_fetched += 1
                if page >= int(doc.get("total_pages", 1)):
                    break
                page += 1
        return records, {"transport": "http", "source": self.base_url + spec["path"], "pages": pages_fetched}

    def _get_with_retry(self, client: httpx.Client, url: str, params: dict) -> dict:
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = client.get(url, params=params)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise httpx.HTTPStatusError(
                        f"retryable status {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp.json()
            except (httpx.TransportError, httpx.HTTPStatusError) as e:
                if attempt > self.max_retries:
                    raise SourceError(f"reference API failed after {attempt - 1} retries: {url} ({e})") from e
                delay = self.backoff * (2 ** (attempt - 1))
                log.warning(
                    "reference API call failed, retrying",
                    url=url,
                    attempt=attempt,
                    delay_s=delay,
                    error=str(e),
                )
                time.sleep(delay)
