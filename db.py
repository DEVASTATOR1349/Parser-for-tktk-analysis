"""
API cost tracking stub.
Logs to console now; will write to 'Расходы API' sheet once base pipeline is validated.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

log = logging.getLogger("scout.db")


def create_api_cost_event(
    *,
    client_config: dict | None = None,
    provider: str = "",
    service: str = "",
    operation: str = "",
    actor_or_model: str = "",
    purpose: str = "",
    status: str = "",
    external_run_id: str | None = None,
    local_run_id: int | None = None,
    local_query_id: int | None = None,
    request_count: int = 0,
    result_count: int = 0,
    usage_usd: float = 0.0,
    usage_units: float | None = None,
    unit_type: str = "",
    error_text: str | None = None,
    request_json: Any = None,
    response_json: Any = None,
    metadata_json: Any = None,
    started_at: Any = None,
    finished_at: Any = None,
) -> None:
    client_name = (client_config or {}).get("name", "?")
    usd = float(usage_usd or 0)
    log.info(
        "API cost [%s] %s/%s status=%s results=%d usd=%.4f",
        client_name,
        provider,
        service,
        status,
        int(result_count or 0),
        usd,
    )
