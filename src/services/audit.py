import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def log_audit_event(
    db: AsyncSession,
    *,
    event_type: str,
    interaction_id: str,
    session_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    job_type: Optional[str] = None,
    job_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist a structured audit event.

    This is intentionally tiny: a single INSERT, best-effort logging on failure.
    """

    payload: Dict[str, Any] = data or {}
    if session_id is not None:
        payload.setdefault("session_id", session_id)

    try:
        await db.execute(
            text(
                """
                INSERT INTO audit_events (
                    interaction_id,
                    customer_id,
                    event_type,
                    job_type,
                    job_id,
                    data
                )
                VALUES (
                    :interaction_id,
                    :customer_id,
                    :event_type,
                    :job_type,
                    :job_id,
                    :data
                )
                """
            ),
            {
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "event_type": event_type,
                "job_type": job_type,
                "job_id": job_id,
                "data": json.dumps(payload),
            },
        )
        await db.commit()
    except Exception:
        logger.exception(
            "audit_event_insert_failed",
            extra={"interaction_id": interaction_id, "event_type": event_type},
        )
