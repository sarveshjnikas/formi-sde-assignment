import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID

from src.config import settings
from src.services.rate_limiter import RateLimitDecision, acquire_customer_token_budget
from src.services.jobs import Job


class _FakeResult:
    def __init__(self, scalar=None, mapping=None):
        self._scalar = scalar
        self._mapping = mapping

    def scalar_one_or_none(self):
        return self._scalar

    def mappings(self):
        return self

    def one(self):
        return self._mapping


class FakeCustomerBudgetSession:
    """Minimal AsyncSession-like stub for acquire_customer_token_budget tests."""

    def __init__(self):
        self.budgets = {}  # customer_id -> tokens_per_minute_limit
        self.windows = {}  # (customer_id, window_start) -> tokens_used

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}

        if "SELECT tokens_per_minute_limit" in sql and "FROM customer_llm_budgets" in sql:
            cid = params["customer_id"]
            return _FakeResult(scalar=self.budgets.get(cid))

        if "INSERT INTO customer_llm_budget_windows" in sql:
            key = (params["customer_id"], params["window_start"])
            self.windows.setdefault(key, 0)
            return _FakeResult()

        if "SELECT tokens_used" in sql and "FROM customer_llm_budget_windows" in sql and "FOR UPDATE" in sql:
            key = (params["customer_id"], params["window_start"])
            used = self.windows.setdefault(key, 0)
            return _FakeResult(mapping={"tokens_used": used})

        if "UPDATE customer_llm_budget_windows" in sql and "tokens_used = tokens_used +" in sql:
            key = (params["customer_id"], params["window_start"])
            self.windows[key] = self.windows.get(key, 0) + int(params["tokens"])
            return _FakeResult()

        raise AssertionError(f"Unexpected SQL in FakeCustomerBudgetSession: {sql}")

    async def commit(self):
        return None


@pytest.mark.asyncio
async def test_acquire_customer_token_budget_enforces_limit(monkeypatch):
    monkeypatch.setattr(settings, "CUSTOMER_TOKENS_PER_MINUTE_DEFAULT", 10)

    db = FakeCustomerBudgetSession()
    customer_id = UUID("00000000-0000-0000-0000-0000000000aa")

    now = datetime(2026, 5, 8, 12, 0, 10, tzinfo=timezone.utc)

    d1 = await acquire_customer_token_budget(db, customer_id=customer_id, tokens=6, now=now)
    assert d1.allowed is True

    d2 = await acquire_customer_token_budget(db, customer_id=customer_id, tokens=5, now=now)
    assert d2.allowed is False
    assert d2.retry_after_seconds > 0


@pytest.mark.asyncio
async def test_llm_worker_defers_when_customer_over_budget(monkeypatch):
    import src.workers.llm_worker as llm_worker

    async def _deny_budget(*args, **kwargs):
        return RateLimitDecision(allowed=False, retry_after_seconds=30)

    monkeypatch.setattr(llm_worker, "acquire_customer_token_budget", _deny_budget)

    # Global limiter should not matter if customer budget already denies.
    llm_worker.acquire_llm_capacity = AsyncMock()

    llm_worker.log_audit_event = AsyncMock()
    llm_worker.requeue_job = AsyncMock()

    processor = AsyncMock()
    monkeypatch.setattr(
        llm_worker,
        "PostCallProcessor",
        lambda: type("P", (), {"process_post_call": processor})(),
    )

    job = Job(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        interaction_id=UUID("00000000-0000-0000-0000-000000000002"),
        customer_id=UUID("00000000-0000-0000-0000-0000000000aa"),
        job_type="llm",
        lane="cold",
        status="claimed",
        attempts=0,
        max_attempts=3,
        available_at=datetime.now(timezone.utc),
        claimed_at=None,
        lease_expires_at=None,
        claimed_by=None,
        payload={
            "interaction_id": "00000000-0000-0000-0000-000000000002",
            "session_id": "s",
            "lead_id": "l",
            "campaign_id": "c",
            "agent_id": "a",
            "call_sid": "sid",
            "transcript_text": "hi",
            "conversation_data": {"transcript": []},
            "additional_data": {},
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "exotel_account_id": "ex",
        },
        last_error=None,
    )

    db = AsyncMock()

    await llm_worker.process_one_llm_job(db, job=job)

    assert processor.await_count == 0
    llm_worker.requeue_job.assert_awaited_once()
