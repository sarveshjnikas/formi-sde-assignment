import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from uuid import UUID

from src.services.jobs import Job


@pytest.mark.asyncio
async def test_llm_worker_emits_deferred_event_on_rate_limit(monkeypatch):
    import src.workers.llm_worker as llm_worker

    async def _allow_budget(*args, **kwargs):
        return type("D", (), {"allowed": True, "retry_after_seconds": 0})()

    async def _deny_global(*args, **kwargs):
        return type("D", (), {"allowed": False, "retry_after_seconds": 12})()

    monkeypatch.setattr(llm_worker, "acquire_customer_token_budget", _allow_budget)
    monkeypatch.setattr(llm_worker, "acquire_llm_capacity", _deny_global)

    llm_worker.log_audit_event = AsyncMock()
    llm_worker.requeue_job = AsyncMock()

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

    event_types = [
        call.kwargs["event_type"] for call in llm_worker.log_audit_event.await_args_list
    ]
    assert "job_deferred_rate_limit" in event_types


@pytest.mark.asyncio
async def test_recording_worker_emits_started_and_failed(monkeypatch):
    import src.workers.recording_worker as recording_worker

    recording_worker.log_audit_event = AsyncMock()
    recording_worker.fetch_and_upload_recording_with_polling = AsyncMock(return_value=None)

    job = Job(
        id=UUID("00000000-0000-0000-0000-000000000010"),
        interaction_id=UUID("00000000-0000-0000-0000-000000000020"),
        customer_id=None,
        job_type="recording",
        lane="cold",
        status="claimed",
        attempts=0,
        max_attempts=2,
        available_at=datetime.now(timezone.utc),
        claimed_at=None,
        lease_expires_at=None,
        claimed_by=None,
        payload={
            "interaction_id": "00000000-0000-0000-0000-000000000020",
            "call_sid": "sid",
            "exotel_account_id": "acc",
        },
        last_error=None,
    )

    recording_worker.mark_job_failed = AsyncMock()
    recording_worker.dead_letter_job = AsyncMock()
    recording_worker.mark_job_succeeded = AsyncMock()

    db = AsyncMock()
    await recording_worker.process_one_recording_job(db, job=job)

    event_types = [
        call.kwargs["event_type"]
        for call in recording_worker.log_audit_event.await_args_list
    ]
    assert "job_started" in event_types
    assert "job_failed" in event_types
