import asyncio
import logging
import os
from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.services.audit import log_audit_event
from src.services.jobs import (
    Job,
    claim_next_job,
    dead_letter_job,
    mark_job_failed,
    mark_job_succeeded,
    requeue_job,
)
from src.services.post_call_processor import PostCallContext, PostCallProcessor
from src.services.rate_limiter import acquire_customer_token_budget, acquire_llm_capacity
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


def _estimate_tokens(payload: dict) -> int:
    # Simple estimate until we track actual prompt sizing.
    return int(payload.get("estimated_tokens") or settings.LLM_AVG_TOKENS_PER_CALL)


async def process_one_llm_job(db: AsyncSession, *, job: Job) -> None:
    payload = job.payload or {}

    interaction_id = UUID(payload["interaction_id"])
    customer_id = job.customer_id

    tokens_est = _estimate_tokens(payload)
    if customer_id is not None:
        cust_decision = await acquire_customer_token_budget(
            db, customer_id=customer_id, tokens=tokens_est
        )
        if not cust_decision.allowed:
            await log_audit_event(
                db,
                event_type="job_deferred_customer_budget",
                interaction_id=str(interaction_id),
                customer_id=str(customer_id),
                job_type=job.job_type,
                job_id=str(job.id),
                data={"retry_after_seconds": cust_decision.retry_after_seconds},
            )
            await requeue_job(
                db, job_id=job.id, delay_seconds=cust_decision.retry_after_seconds
            )
            return

    decision = await acquire_llm_capacity(db, tokens=tokens_est)

    if not decision.allowed:
        await log_audit_event(
            db,
            event_type="job_deferred_rate_limit",
            interaction_id=str(interaction_id),
            customer_id=str(customer_id) if customer_id else None,
            job_type=job.job_type,
            job_id=str(job.id),
            data={"retry_after_seconds": decision.retry_after_seconds},
        )
        await requeue_job(db, job_id=job.id, delay_seconds=decision.retry_after_seconds)
        return

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
        ctx = PostCallContext(
            interaction_id=str(interaction_id),
            session_id=payload.get("session_id", ""),
            lead_id=payload.get("lead_id", ""),
            campaign_id=payload.get("campaign_id", ""),
            customer_id=str(customer_id) if customer_id else payload.get("customer_id", ""),
            agent_id=payload.get("agent_id", ""),
            call_sid=payload.get("call_sid", ""),
            transcript_text=payload.get("transcript_text", ""),
            conversation_data=payload.get("conversation_data", {}),
            additional_data=payload.get("additional_data", {}),
            ended_at=datetime.fromisoformat(payload["ended_at"]),
            exotel_account_id=payload.get("exotel_account_id"),
        )

        processor = PostCallProcessor()
        result = await processor.process_post_call(ctx, single_prompt=True)

        await mark_job_succeeded(db, job_id=job.id)
        await log_audit_event(
            db,
            event_type="job_succeeded",
            interaction_id=str(interaction_id),
            customer_id=str(customer_id) if customer_id else None,
            job_type=job.job_type,
            job_id=str(job.id),
            data={"tokens_used": result.tokens_used, "call_stage": result.call_stage},
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
        job = await claim_next_job(db, job_type="llm", worker_id=worker_id)
        if not job:
            return False

        await process_one_llm_job(db, job=job)
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
    wid = os.getenv("WORKER_ID") or f"llm-worker-{os.getpid()}"
    asyncio.run(run_forever(worker_id=wid))
