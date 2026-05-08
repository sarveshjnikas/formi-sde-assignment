import asyncio
import logging
import os
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
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


async def process_one_downstream_job(db: AsyncSession, *, job: Job) -> None:
    payload = job.payload or {}

    interaction_id = UUID(payload["interaction_id"])
    customer_id = job.customer_id

    await log_audit_event(
        db,
        event_type="job_started",
        interaction_id=str(interaction_id),
        customer_id=str(customer_id) if customer_id else None,
        job_type=job.job_type,
        job_id=str(job.id),
        data={"lane": job.lane},
    )

    try:
        await trigger_signal_jobs(
            interaction_id=str(interaction_id),
            session_id=payload.get("session_id", ""),
            campaign_id=payload.get("campaign_id", ""),
            analysis_result=payload.get("analysis_result") or {},
        )

        call_stage = payload.get("call_stage") or "processing"
        if payload.get("lead_id"):
            await update_lead_stage(
                lead_id=payload["lead_id"],
                interaction_id=str(interaction_id),
                call_stage=call_stage,
            )

        await mark_job_succeeded(db, job_id=job.id)
        await log_audit_event(
            db,
            event_type="job_succeeded",
            interaction_id=str(interaction_id),
            customer_id=str(customer_id) if customer_id else None,
            job_type=job.job_type,
            job_id=str(job.id),
            data={"call_stage": call_stage},
        )

    except Exception as e:
        error = str(e)
        next_attempt = job.attempts + 1

        await log_audit_event(
            db,
            event_type="job_failed",
            interaction_id=str(interaction_id),
            customer_id=str(customer_id) if customer_id else None,
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
                customer_id=str(customer_id) if customer_id else None,
                job_type=job.job_type,
                job_id=str(job.id),
                data={"error": error},
            )
            return

        await mark_job_failed(db, job_id=job.id, error=error)


async def run_once(*, worker_id: str) -> bool:
    async with async_session_factory() as db:
        job = await claim_next_job(db, job_type="downstream", worker_id=worker_id)
        if not job:
            return False

        await process_one_downstream_job(db, job=job)
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
    wid = os.getenv("WORKER_ID") or f"downstream-worker-{os.getpid()}"
    asyncio.run(run_forever(worker_id=wid))
