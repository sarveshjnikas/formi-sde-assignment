import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_recording_poller_retries_then_succeeds(monkeypatch):
    import src.services.recording as recording

    sleeps = []

    async def _fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(recording.asyncio, "sleep", _fake_sleep)

    calls = {"n": 0}

    async def _fake_fetch_url(call_sid, account_id):
        calls["n"] += 1
        if calls["n"] < 3:
            return None
        return "https://example.com/rec.mp3"

    async def _fake_upload(url, interaction_id):
        assert url.startswith("https://")
        return f"recordings/{interaction_id}.mp3"

    monkeypatch.setattr(recording, "_fetch_exotel_recording_url", _fake_fetch_url)
    monkeypatch.setattr(recording, "_upload_to_s3", _fake_upload)

    s3_key = await recording.fetch_and_upload_recording_with_polling(
        interaction_id="i-1",
        call_sid="sid",
        exotel_account_id="acc",
        max_wait_seconds=120,
        initial_delay_seconds=2.0,
        max_delay_seconds=15.0,
    )

    assert s3_key == "recordings/i-1.mp3"
    assert calls["n"] == 3
    assert sleeps == [2.0, 4.0]


@pytest.mark.asyncio
async def test_recording_poller_times_out(monkeypatch):
    import src.services.recording as recording

    async def _fake_fetch_url(call_sid, account_id):
        return None

    monkeypatch.setattr(recording, "_fetch_exotel_recording_url", _fake_fetch_url)
    monkeypatch.setattr(recording.asyncio, "sleep", AsyncMock())

    s3_key = await recording.fetch_and_upload_recording_with_polling(
        interaction_id="i-2",
        call_sid="sid",
        exotel_account_id="acc",
        max_wait_seconds=3,
        initial_delay_seconds=1.0,
        max_delay_seconds=2.0,
    )

    assert s3_key is None
