import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int


def _minute_window_start(now: datetime) -> datetime:
    ts = now.astimezone(timezone.utc)
    return ts.replace(second=0, microsecond=0)


async def acquire_llm_capacity(
    db: AsyncSession,
    *,
    tokens: int,
    now: datetime | None = None,
) -> RateLimitDecision:
    """Global RPM/TPM limiter backed by Postgres.

    Stores counters per UTC minute. Uses row-level locking to enforce limits
    across multiple workers.
    """

    ts = now or datetime.now(timezone.utc)
    window_start = _minute_window_start(ts)

    await db.execute(
        text(
            """
            INSERT INTO llm_rate_limit_windows (window_start, requests_used, tokens_used)
            VALUES (:window_start, 0, 0)
            ON CONFLICT (window_start) DO NOTHING
            """
        ),
        {"window_start": window_start},
    )

    row = await db.execute(
        text(
            """
            SELECT requests_used, tokens_used
            FROM llm_rate_limit_windows
            WHERE window_start = :window_start
            FOR UPDATE
            """
        ),
        {"window_start": window_start},
    )
    current = row.mappings().one()

    req_used = int(current["requests_used"])
    tok_used = int(current["tokens_used"])

    max_rpm = int(settings.LLM_REQUESTS_PER_MINUTE)
    max_tpm = int(settings.LLM_TOKENS_PER_MINUTE)

    allowed = (req_used + 1) <= max_rpm and (tok_used + tokens) <= max_tpm

    if allowed:
        await db.execute(
            text(
                """
                UPDATE llm_rate_limit_windows
                SET requests_used = requests_used + 1,
                    tokens_used = tokens_used + :tokens,
                    updated_at = NOW()
                WHERE window_start = :window_start
                """
            ),
            {"window_start": window_start, "tokens": int(tokens)},
        )
        await db.commit()
        return RateLimitDecision(allowed=True, retry_after_seconds=0)

    # Not allowed: compute seconds until next window.
    next_window = window_start + timedelta(minutes=1)
    retry_after = max(1, int((next_window - ts).total_seconds()))
    await db.commit()
    return RateLimitDecision(allowed=False, retry_after_seconds=retry_after)


async def acquire_customer_token_budget(
    db: AsyncSession,
    *,
    customer_id: UUID,
    tokens: int,
    now: datetime | None = None,
) -> RateLimitDecision:
    """Per-customer token budget limiter backed by Postgres."""

    ts = now or datetime.now(timezone.utc)
    window_start = _minute_window_start(ts)

    # Determine limit: DB override if present, otherwise env default.
    limit_row = await db.execute(
        text(
            """
            SELECT tokens_per_minute_limit
            FROM customer_llm_budgets
            WHERE customer_id = :customer_id
            """
        ),
        {"customer_id": str(customer_id)},
    )
    limit_value = limit_row.scalar_one_or_none()
    limit = int(limit_value) if limit_value is not None else int(
        settings.CUSTOMER_TOKENS_PER_MINUTE_DEFAULT
    )

    await db.execute(
        text(
            """
            INSERT INTO customer_llm_budget_windows (customer_id, window_start, tokens_used)
            VALUES (:customer_id, :window_start, 0)
            ON CONFLICT (customer_id, window_start) DO NOTHING
            """
        ),
        {"customer_id": str(customer_id), "window_start": window_start},
    )

    row = await db.execute(
        text(
            """
            SELECT tokens_used
            FROM customer_llm_budget_windows
            WHERE customer_id = :customer_id AND window_start = :window_start
            FOR UPDATE
            """
        ),
        {"customer_id": str(customer_id), "window_start": window_start},
    )
    current = row.mappings().one()
    used = int(current["tokens_used"])

    allowed = (used + tokens) <= limit
    if allowed:
        await db.execute(
            text(
                """
                UPDATE customer_llm_budget_windows
                SET tokens_used = tokens_used + :tokens,
                    updated_at = NOW()
                WHERE customer_id = :customer_id AND window_start = :window_start
                """
            ),
            {
                "customer_id": str(customer_id),
                "window_start": window_start,
                "tokens": int(tokens),
            },
        )
        await db.commit()
        return RateLimitDecision(allowed=True, retry_after_seconds=0)

    next_window = window_start + timedelta(minutes=1)
    retry_after = max(1, int((next_window - ts).total_seconds()))
    await db.commit()
    return RateLimitDecision(allowed=False, retry_after_seconds=retry_after)
