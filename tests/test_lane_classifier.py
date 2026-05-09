import pytest
from src.services.lane_classifier import classify_lane


@pytest.mark.parametrize("transcript_key,expected_lane", [
    ("rebook_confirmed", "hot"),
    ("demo_booked", "hot"),
    ("escalation_needed", "hot"),
    ("not_interested", "cold"),
    ("callback_requested", "cold"),
    ("already_purchased", "cold"),
    ("hinglish_ambiguous", "cold"),
    ("short_call_hangup", "skip"),
])
def test_classify_lane_matches_fixture(transcript_key, expected_lane, sample_transcripts):
    data = sample_transcripts[transcript_key]
    transcript = data["transcript"]
    transcript_text = "\n".join(
        f"{t['role']}: {t['content']}" for t in transcript
    )
    turn_count = len(transcript)
    result = classify_lane(transcript_text=transcript_text, turn_count=turn_count)
    assert result == expected_lane, (
        f"{transcript_key}: expected lane={expected_lane!r}, got {result!r}"
    )