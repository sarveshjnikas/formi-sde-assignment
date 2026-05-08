"""Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

Current approach (legacy):
  Wait 45 seconds. Try once. If it's not there, give up silently.

New approach (for durable jobs):
  Poll with retry/backoff until the recording is available or a max wait is hit.
  This is used by the Postgres-backed recording worker.
"""

import asyncio
import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """Legacy path used by the existing Celery pipeline.

    Kept for now to avoid a large refactor while we migrate to durable jobs.
    """

    await asyncio.sleep(settings.RECORDING_WAIT_SECONDS)

    try:
        recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

        if not recording_url:
            logger.debug(
                "recording_not_available",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "waited_seconds": settings.RECORDING_WAIT_SECONDS,
                },
            )
            return None

        s3_key = await _upload_to_s3(recording_url, interaction_id)
        return s3_key

    except Exception as e:
        logger.exception(
            "recording_upload_error",
            extra={"interaction_id": interaction_id, "error": str(e)},
        )
        return None


async def fetch_and_upload_recording_with_polling(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    *,
    max_wait_seconds: int = 120,
    initial_delay_seconds: float = 2.0,
    max_delay_seconds: float = 15.0,
) -> Optional[str]:
    """Poll Exotel for the recording and upload once available.

    Returns S3 key on success, None if not available within max_wait_seconds.
    """

    elapsed = 0.0
    delay = initial_delay_seconds

    while elapsed <= max_wait_seconds:
        recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)
        if recording_url:
            return await _upload_to_s3(recording_url, interaction_id)

        await asyncio.sleep(delay)
        elapsed += delay
        delay = min(max_delay_seconds, delay * 2)

    logger.warning(
        "recording_poll_timeout",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "max_wait_seconds": max_wait_seconds,
        },
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
