import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    id: UUID
    interaction_id: UUID
    customer_id: Optional[UUID]
    job_type: str
    lane: str
    status: str
    attempts: int
    max_attempts: int
    available_at: datetime
    claimed_at: Optional[datetime]
    lease_expires_at: Optional[datetime]
    claimed_by: Optional[str]
    payload: Dict[str, Any]
    last_error: Optional[str]


async def enqueue_job(
    db: AsyncSession,
    *,
    interaction_id: UUID,
    customer_id: Optional[UUID],
    job_type: str,
    lane: str,
    payload: Dict[str, Any],
    available_at: Optional[datetime] = None,
    max_attempts: int = 10,
) -> UUID:
    now = datetime.now(timezone.utc)
    available = available_at or now

    row = await db.execute(
        text(
            """
            INSERT INTO postcall_jobs (
                interaction_id,
                customer_id,
                job_type,
                lane,
                status,
                attempts,
                max_attempts,
                available_at,
                payload
            )
            VALUES (
                :interaction_id,
                :customer_id,
                :job_type,
                :lane,
                'queued',
                0,
                :max_attempts,
                :available_at,
                :payload
            )
            RETURNING id
            """
        ),
        {
            "interaction_id": str(interaction_id),
            "customer_id": str(customer_id) if customer_id else None,
            "job_type": job_type,
            "lane": lane,
            "max_attempts": max_attempts,
            "available_at": available,
            "payload": json.dumps(payload),
        },
    )
    job_id = UUID(str(row.scalar_one()))
    await db.commit()
    return job_id


async def claim_next_job(
    db: AsyncSession,
    *,
    job_type: str,
    worker_id: str,
    lease_seconds: int = 120,
    now: Optional[datetime] = None,
) -> Optional[Job]:
    """Atomically claim the next queued job.

    Uses a lease to recover from worker crashes: if a job is claimed but the
    worker dies, a separate watchdog can re-queue it after lease expiry.
    """

    ts = now or datetime.now(timezone.utc)
    lease_expires = ts + timedelta(seconds=lease_seconds)

    res = await db.execute(
        text(
            """
            WITH next_job AS (
                SELECT id
                FROM postcall_jobs
                WHERE
                    job_type = :job_type
                    AND status = 'queued'
                    AND available_at <= :now
                ORDER BY available_at ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE postcall_jobs j
            SET
                status = 'claimed',
                claimed_at = :now,
                lease_expires_at = :lease_expires_at,
                claimed_by = :claimed_by,
                updated_at = NOW()
            FROM next_job
            WHERE j.id = next_job.id
            RETURNING
                j.id,
                j.interaction_id,
                j.customer_id,
                j.job_type,
                j.lane,
                j.status,
                j.attempts,
                j.max_attempts,
                j.available_at,
                j.claimed_at,
                j.lease_expires_at,
                j.claimed_by,
                j.payload,
                j.last_error
            """
        ),
        {
            "job_type": job_type,
            "now": ts,
            "lease_expires_at": lease_expires,
            "claimed_by": worker_id,
        },
    )

    row = res.mappings().first()
    if not row:
        return None

    payload = row["payload"] or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {"_raw": payload}

    await db.commit()

    return Job(
        id=UUID(str(row["id"])),
        interaction_id=UUID(str(row["interaction_id"])),
        customer_id=UUID(str(row["customer_id"])) if row["customer_id"] else None,
        job_type=row["job_type"],
        lane=row["lane"],
        status=row["status"],
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        available_at=row["available_at"],
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        claimed_by=row["claimed_by"],
        payload=payload,
        last_error=row["last_error"],
    )


async def mark_job_succeeded(db: AsyncSession, *, job_id: UUID) -> None:
    await db.execute(
        text(
            """
            UPDATE postcall_jobs
            SET status = 'succeeded', updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {"job_id": str(job_id)},
    )
    await db.commit()


async def mark_job_failed(
    db: AsyncSession,
    *,
    job_id: UUID,
    error: str,
    retry_in_seconds: int = 60,
    now: Optional[datetime] = None,
) -> None:
    ts = now or datetime.now(timezone.utc)
    available_at = ts + timedelta(seconds=retry_in_seconds)

    await db.execute(
        text(
            """
            UPDATE postcall_jobs
            SET
                status = CASE
                    WHEN attempts + 1 >= max_attempts THEN 'dead_lettered'
                    ELSE 'queued'
                END,
                attempts = attempts + 1,
                last_error = :error,
                available_at = CASE
                    WHEN attempts + 1 >= max_attempts THEN available_at
                    ELSE :available_at
                END,
                claimed_at = NULL,
                lease_expires_at = NULL,
                claimed_by = NULL,
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {"job_id": str(job_id), "error": error, "available_at": available_at},
    )
    await db.commit()


async def dead_letter_job(
    db: AsyncSession,
    *,
    job_id: UUID,
    interaction_id: UUID,
    customer_id: Optional[UUID],
    job_type: str,
    payload: Dict[str, Any],
    error: str,
) -> None:
    await db.execute(
        text(
            """
            INSERT INTO dead_letters (
                job_id,
                interaction_id,
                customer_id,
                job_type,
                payload,
                error
            )
            VALUES (
                :job_id,
                :interaction_id,
                :customer_id,
                :job_type,
                :payload,
                :error
            )
            """
        ),
        {
            "job_id": str(job_id),
            "interaction_id": str(interaction_id),
            "customer_id": str(customer_id) if customer_id else None,
            "job_type": job_type,
            "payload": json.dumps(payload),
            "error": error,
        },
    )
    await db.execute(
        text(
            """
            UPDATE postcall_jobs
            SET status = 'dead_lettered', updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {"job_id": str(job_id)},
    )
    await db.commit()
