import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

from src.config import settings
from src.services.rate_limiter import RateLimitDecision, acquire_llm_capacity
from src.services.jobs import Job


class _FakeResult:
    def __init__(self, scalar=None, mapping=None):
        self._scalar = scalar
        self._mapping = mapping

    def scalar_one_or_none(self):
        return self._scalar

    def scalar_one(self):
        return self._scalar

    def mappings(self):
        return self

    def one(self):
        return self._mapping


class FakeLimiterSession:
    """Minimal AsyncSession-like stub for acquire_llm_capacity tests."""

    def __init__(self):
        self.windows = {}  # window_start -> {requests_used, tokens_used}

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}

        if "INSERT INTO llm_rate_limit_windows" in sql:
            ws = params["window_start"]
            self.windows.setdefault(ws, {"requests_used": 0, "tokens_used": 0})
            return _FakeResult()

        if "SELECT requests_used, tokens_used" in sql and "FOR UPDATE" in sql:
            ws = params["window_start"]
            cur = self.windows.setdefault(ws, {"requests_used": 0, "tokens_used": 0})
            return _FakeResult(mapping=cur)

        if "UPDATE llm_rate_limit_windows" in sql and "requests_used = requests_used + 1" in sql:
            ws = params["window_start"]
            cur = self.windows.setdefault(ws, {"requests_used": 0, "tokens_used": 0})
            cur["requests_used"] += 1
            cur["tokens_used"] += int(params["tokens"])
            return _FakeResult()

        raise AssertionError(f"Unexpected SQL in FakeLimiterSession: {sql}")

    async def commit(self):
        return None


@pytest.mark.asyncio
async def test_acquire_llm_capacity_enforces_rpm(monkeypatch):
    monkeypatch.setattr(settings, "LLM_REQUESTS_PER_MINUTE", 5)
    monkeypatch.setattr(settings, "LLM_TOKENS_PER_MINUTE", 10_000)

    db = FakeLimiterSession()
    now = datetime(2026, 5, 8, 12, 0, 10, tzinfo=timezone.utc)

    for _ in range(5):
        d = await acquire_llm_capacity(db, tokens=1, now=now)
        assert d.allowed is True

    d = await acquire_llm_capacity(db, tokens=1, now=now)
    assert d.allowed is False
    assert d.retry_after_seconds > 0


@pytest.mark.asyncio
async def test_llm_worker_defers_when_rate_limited(monkeypatch):
    # Import module so monkeypatch targets the bound symbols it uses.
    import src.workers.llm_worker as llm_worker

    # Make limiter always deny.
    async def _deny(*args, **kwargs):
        return RateLimitDecision(allowed=False, retry_after_seconds=30)

    monkeypatch.setattr(llm_worker, "acquire_llm_capacity", _deny)
    monkeypatch.setattr(llm_worker, "log_audit_event", AsyncMock())
    monkeypatch.setattr(llm_worker, "requeue_job", AsyncMock())

    processor = AsyncMock()
    monkeypatch.setattr(llm_worker, "PostCallProcessor", lambda: SimpleNamespace(process_post_call=processor))

    job = Job(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        interaction_id=UUID("00000000-0000-0000-0000-000000000002"),
        customer_id=None,
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
            "customer_id": "cust",
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
