"""Mock reference-data REST API (SOURCE 5) - simulates a third-party service.

Serves the generated response documents from data/raw/api with pagination, and can be
made flaky (REFERENCE_API_FAIL_RATE=0.3 -> ~30% of calls return 503) to exercise the
ingestion client's retry/backoff path. Used by docker compose; local runs read the same
documents through the file:// transport.

    uvicorn api.reference_api:app --port 8081
"""

from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataforge.config import get_settings  # noqa: E402

app = FastAPI(
    title="Mock Reference API",
    version="1.0",
    description="Synthetic regions / warehouses / exchange-rates feed",
)
FAIL_RATE = float(os.environ.get("REFERENCE_API_FAIL_RATE", "0"))
_rng = random.Random(42)


def _doc(name: str) -> dict:
    path = get_settings().raw_dir / "api" / name
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{name} not generated yet")
    return json.loads(path.read_text(encoding="utf-8"))


def _page(data: list, page: int, page_size: int) -> dict:
    total_pages = max(1, -(-len(data) // page_size))
    start = (page - 1) * page_size
    return {
        "data": data[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "count": len(data),
        "synthetic": True,
    }


def _maybe_fail():
    if FAIL_RATE and _rng.random() < FAIL_RATE:
        raise HTTPException(status_code=503, detail="simulated upstream outage")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reference/regions")
def regions(page: int = Query(1, ge=1), page_size: int = Query(2000, ge=1, le=5000)):
    _maybe_fail()
    return _page(_doc("regions.json")["data"], page, page_size)


@app.get("/reference/warehouses")
def warehouses(page: int = Query(1, ge=1), page_size: int = Query(2000, ge=1, le=5000)):
    _maybe_fail()
    return _page(_doc("warehouses.json")["data"], page, page_size)


@app.get("/reference/exchange-rates")
def exchange_rates(
    as_of: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(2000, ge=1, le=5000),
):
    _maybe_fail()
    try:
        doc = _doc(f"exchange_rates_{as_of}.json")
    except HTTPException:
        return JSONResponse(
            {
                "data": [],
                "page": 1,
                "page_size": page_size,
                "total_pages": 1,
                "count": 0,
                "synthetic": True,
                "note": f"no rates batch for {as_of}",
            }
        )
    return _page(doc["data"], page, page_size)
