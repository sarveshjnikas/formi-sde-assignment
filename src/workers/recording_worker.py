import asyncio
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.audit import log_audit_event
from src.services.jobs import (
    Job,
    claim_next_job,
    dead_letter_job,
    mark_job_failed,
    mark_job_succeeded,
)
from src.services.recording import fetch_and_upload_recording_with_polling
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


def _retry_delay_seconds(attempt: int) -> int:
    """Exponential backoff capped to 60s.

    attempt is 1-based (first failure => attempt=1).
    """

    return min(60, 2 ** min(attempt, 6))


async def process_one_recording_job(db: AsyncSession, *, job: Job) -> None:
    payload = job.payload or {}

    interaction_id = UUID(payload["interaction_id"])
    call_sid = payload.get("call_sid") or ""
    exotel_account_id = payload.get("exotel_account_id") or ""

    await log_audit_event(
        db,
        event_type="job_started",
        interaction_id=str(interaction_id),
        customer_id=str(job.customer_id) if job.customer_id else None,
        job_type=job.job_type,
        job_id=str(job.id),
        data={"lane": job.lane},
    )

    try:
        s3_key = await fetch_and_upload_recording_with_polling(
            interaction_id=str(interaction_id),
            call_sid=call_sid,
            exotel_account_id=exotel_account_id,
        )

        if not s3_key:
            raise RuntimeError("recording_unavailable_after_polling")

        await mark_job_succeeded(db, job_id=job.id)
        await log_audit_event(
            db,
            event_type="job_succeeded",
            interaction_id=str(interaction_id),
            customer_id=str(job.customer_id) if job.customer_id else None,
            job_type=job.job_type,
            job_id=str(job.id),
            data={"s3_key": s3_key},
        )

    except Exception as e:
        error = str(e)
        next_attempt = job.attempts + 1

        await log_audit_event(
            db,
            event_type="job_failed",
            interaction_id=str(interaction_id),
            customer_id=str(job.customer_id) if job.customer_id else None,
            job_type=job.job_type,
            job_id=str(job.id),
            data={"error": error, "attempt": next_attempt},
        )

        if next_attempt >= job.max_attempts:
            await dead_letter_job(
                db,
                job_id=job.id,
                interaction_id=job.interaction_id,
                customer_id=job.customer_id,
                job_type=job.job_type,
                payload=payload,
                error=error,
            )
            await log_audit_event(
                db,
                event_type="job_dead_lettered",
                interaction_id=str(interaction_id),
                customer_id=str(job.customer_id) if job.customer_id else None,
                job_type=job.job_type,
                job_id=str(job.id),
                data={"error": error},
            )
            return

        await mark_job_failed(
            db,
            job_id=job.id,
            error=error,
            retry_in_seconds=_retry_delay_seconds(next_attempt),
        )


async def run_once(*, worker_id: str) -> bool:
    """Claim and process at most one recording job. Returns True if a job ran."""

    async with async_session_factory() as db:
        job = await claim_next_job(db, job_type="recording", worker_id=worker_id)
        if not job:
            return False

        await process_one_recording_job(db, job=job)
        return True


async def run_forever(*, worker_id: str, idle_sleep_seconds: float = 1.0) -> None:
    while True:
        did_work = await run_once(worker_id=worker_id)
        if not did_work:
            await asyncio.sleep(idle_sleep_seconds)


def _configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level)


if __name__ == "__main__":
    _configure_logging()
    wid = os.getenv("WORKER_ID") or f"recording-worker-{os.getpid()}"
    asyncio.run(run_forever(worker_id=wid))
