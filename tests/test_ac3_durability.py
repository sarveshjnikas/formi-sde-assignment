import pytest
from datetime import datetime, timezone

from src.services.jobs import requeue_stale_claims


class _FakeResult:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class FakeSession:
    def __init__(self, rowcount=0):
        self._rowcount = rowcount
        self.executed = []

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "UPDATE postcall_jobs" in sql and "lease_expires_at <" in sql:
            return _FakeResult(rowcount=self._rowcount)
        raise AssertionError(f"Unexpected SQL: {sql}")

    async def commit(self):
        return None


@pytest.mark.asyncio
async def test_requeue_stale_claims_returns_count():
    db = FakeSession(rowcount=3)
    now = datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc)
    n = await requeue_stale_claims(db, now=now)
    assert n == 3
