import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

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
