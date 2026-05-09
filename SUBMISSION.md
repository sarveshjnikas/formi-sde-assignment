# Post-Call Processing Pipeline — Design Document

**Author:** Sarvesh Nikas
**Date:** 10/05/2026

---

## 1. Assumptions

<!-- _State every assumption you made about the business, system, or environment. Be specific. These will be discussed in the follow-up._ -->

1. The LLM provider’s limits (requests/minute and tokens/minute) are strict, and if we exceed them we’ll get 429s—so the right fix is to schedule/throttle requests, not just “retry later.”
2. Every call interaction belongs to one customer, and I should be able to trace/attribute any LLM token spend back to that customer and that interaction (and campaign/session where available).
3. Very short calls (fewer than 4 turns) aren’t worth sending to the LLM, but they still need the system to update the lead/dashboard and trigger any basic downstream actions.
4. Exotel recordings often show up late and unpredictably, so a fixed 45-second wait is unreliable; polling with backoff for a limited time is acceptable, and failures must be visible and retryable.
5. Workers and Redis/Celery can restart or crash at any time, so I can’t rely on in-memory tasks; durable job state must live in Postgres, with leases so stuck work can be recovered.
6. It’s okay if some processing is delayed during bursts (minutes, not hours) as long as nothing is lost and I can always see what happened via audit logs and a dead-letter queue.

---

## 2. Problem Diagnosis

1. No per-customer token budgeting. A large campaign from one customer can consume most of the LLM capacity and worker time, causing other customers’ calls to be delayed (no fairness/isolation).
2. Recording handling is brittle. The system waits a fixed 45 seconds for the recording and then tries once; recordings that appear later are effectively skipped with no retry or visibility.
3. Poor traceability. There is no clear, step-by-step audit trail per interaction (enqueued → recording → LLM → downstream actions), making debugging slow and guessy.
4. Retries are unreliable and inconsistent. Failures are retried via both Celery retry and a separate Redis retry queue; they don’t coordinate (duplicate processing risk) and both depend on Redis (Redis failure can lose work).
5. No dead-letter handling. After retry_exhausted, the system drops the job instead of persisting it to a dead-letter queue/store for investigation and replay.
6. No LLM rate-limit awareness. LLM providers enforce RPM/TPM limits; the current system sends requests immediately during bursts, which would hit rate limits (429) and create a growing backlog.

---

## 3. Architecture Overview

<!-- _End-to-end flow from call-end webhook to completed analysis. Include a diagram._ -->

```
                 Exotel (telephony)
                        |
                        | POST /session/{sid}/interaction/{iid}/end
                        v
+------------------------------------------------------+
| FastAPI Ingest                                       |
| - mark interaction ENDED                             |
| - choose lane: skip / cold / hot                     |
| - in ONE DB transaction:                             |
|     * insert jobs (recording, llm?, downstream)       |
|     * insert audit_event("interaction_ended")         |
| - return 200                                         |
+------------------------------------------------------+
                        |
                        | Postgres (durable source of truth)
                        v
+--------------------+   +---------------------+   +----------------------+
| Recording Worker   |   | LLM Scheduler       |   | Downstream Worker     |
| (job_type=recording)|  | (job_type=llm)      |   | (job_type=downstream) |
| - claim job        |   | - claim llm jobs    |   | - claim job           |
| - poll Exotel w/   |   | - enforce RPM/TPM   |   | - update lead stage   |
|   backoff          |   | - enforce per-cust  |   | - trigger CRM/signal  |
| - upload S3        |   |   token budgets     |   | - can run without     |
| - write result     |   | - prioritize hot    |   |   recording           |
| - audit each step  |   | - lease + watchdog  |   | - audit each step     |
+--------------------+   +---------------------+   +----------------------+
           |                        |                         |
           v                        v                         v
     audit_events table       audit_events table        audit_events table

```

### Key design decisions

1. I made Postgres the source of truth for background work by writing each step as a durable job row (so work isn’t silently lost on restarts).
2. Split the pipeline into separate jobs (recording, LLM, downstream) so one slow/failing step doesn’t block everything else and each step can retry independently.
3. Added a global, rate-limit-aware LLM gate (RPM/TPM) before calling the LLM, so the system defers work instead of triggering 429s and runaway retries.
4. Enforced per-customer token budgets with atomic counters, so one customer’s big campaign can’t starve everyone else.
5. Added leases + stale-claim recovery for jobs, so if a worker crashes mid-job the work becomes runnable again automatically.
6. Implemented structured audit events for each stage, so I can trace an interaction from “ended” to “done/failed/dead-lettered” without guessing.
7. Added a dead-letter queue in Postgres instead of dropping failures, so exhausted jobs remain visible and replayable.

---

## 4. Rate Limit Management

LLM calls only happen from a durable `llm` job worker. Before calling the LLM, the worker must acquire capacity from a global rate limiter. If capacity is not available, the job is deferred (re-queued) instead of firing and receiving 429s.

### How I track rate limit usage

- I track global usage in Postgres using `llm_rate_limit_windows(window_start, requests_used, tokens_used)`.
- `acquire_llm_capacity(...)` locks the current minute row (`FOR UPDATE`) and atomically checks/increments RPM and TPM counters.

### What happens when the limit is hit (recovery, not crash)

- If the next request would exceed RPM/TPM for the current minute, `acquire_llm_capacity` returns `allowed=False` with `retry_after_seconds` = seconds until the next minute window.
- The LLM worker re-queues the job via `requeue_job(...)` with `available_at = now + retry_after_seconds`. This does not consume retry attempts because rate limiting is expected under bursty load.
- Audit event emitted: `job_deferred_rate_limit`.

### Token estimation vs actuals

- For scheduling, the worker uses a conservative token estimate (defaults to `LLM_AVG_TOKENS_PER_CALL`).
- After the (mock) provider returns `usage.total_tokens`, the worker records actual usage in the success audit event. In production, I would persist actual usage to a billing-grade ledger.

---

## 5. Per-Customer Token Budgeting

I enforce per-customer token budgets before the global limiter so one customer cannot starve others.

- Each customer has a tokens/minute limit. Default comes from `CUSTOMER_TOKENS_PER_MINUTE_DEFAULT` and can be overridden per customer via `customer_llm_budgets`.
- Per-minute usage is tracked in `customer_llm_budget_windows(customer_id, window_start, tokens_used)`.
- `acquire_customer_token_budget(...)` locks the customer+minute row (`FOR UPDATE`) and atomically checks/increments token usage.
- If the budget would be exceeded, the `llm` job is deferred to the next minute window via `requeue_job(...)`. Audit event emitted: `job_deferred_customer_budget`.

Current behavior is simple and predictable: unused headroom is not redistributed. With more time I would add fair-sharing that still preserves per-customer minimum guarantees.

---

## 6. Differentiated Processing

I model urgency using `lane` on `postcall_jobs`: `hot`, `cold`, `skip`.

- `skip`: turn count < 4 → no LLM job created, downstream still runs.
- `hot` vs `cold`: determined at ingest by `lane_classifier.classify_lane()`, a regex-based keyword classifier that runs synchronously on the transcript text before any jobs are enqueued. Cold overrides are checked first so phrases like "already booked" or "call me back" don't false-positive as hot.

In production, hot/cold would be refined by having the LLM return a `lane` field as part of its existing analysis response — zero additional token cost. The keyword classifier handles the skip gate (free, instant) and serves as the triage fallback when the LLM result isn't yet available.

Hot jobs are enqueued with `lane="hot"` on all three job types (recording, llm, downstream). The worker claim query orders by `available_at ASC` — prioritising hot over cold requires either a separate hot queue or adding `lane` to the ORDER BY. That is the next implementation step.

---

## 7. Recording Pipeline

Recording upload runs as a durable `recording` job and uses polling with backoff instead of a fixed `sleep(45s)`:

- `fetch_and_upload_recording_with_polling(...)` polls Exotel for up to `max_wait_seconds` (default 120s).
- Backoff starts at 2s and doubles up to a cap (default 15s).
- If available, it uploads and returns an S3 key.
- If not available within the window, the worker marks the job failed and re-queues with backoff retries; on exhaustion it is dead-lettered.

Visibility:
- Audit events emitted with `interaction_id`/`job_id`: `job_started`, `job_failed`, `job_succeeded`, `job_dead_lettered`.

---

## 8. Reliability & Durability

<!-- _How do you ensure no analysis result is permanently lost?_ -->

- All work is represented as rows in `postcall_jobs` (durable).
- Workers claim jobs atomically (`FOR UPDATE SKIP LOCKED`) and attach a lease (`lease_expires_at`) so crashes don’t stall work forever.
- Stale claimed jobs can be made runnable again via `requeue_stale_claims(...)`.
- When retries are exhausted, the full payload + error is preserved in `dead_letters` (durable DLQ) rather than dropped.
---

## 9. Auditability & Observability

<!-- _How would you debug a specific failed interaction 3 days after the fact?_ -->

### What you log (and what fields every log event includes)

- I persist structured audit events to `audit_events`.
- Every event includes: `interaction_id`, `event_type`, optional `job_type`, optional `job_id`, optional `customer_id`, plus a JSON `data` payload.
- This makes it easy to query “what happened to interaction X?” and see a step-by-step timeline across recording/LLM/downstream.

### Alert conditions

- Any `job_dead_lettered` event should be alertable (durable failure needing attention).
- Sustained `job_deferred_rate_limit` / `job_deferred_customer_budget` indicates capacity pressure and should alert before SLA impact.
---

## 10. Data Model

<!-- _Schema changes required. Show the SQL._ -->

```sql
-- Audit trail
CREATE TABLE audit_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL,
    customer_id UUID,
    event_type VARCHAR(100) NOT NULL,
    job_type VARCHAR(50),
    job_id UUID,
    data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Durable jobs
CREATE TABLE postcall_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL,
    customer_id UUID,
    job_type VARCHAR(50) NOT NULL,
    lane VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 10,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    claimed_by VARCHAR(100),
    payload JSONB NOT NULL DEFAULT '{}',
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dead letter queue
CREATE TABLE dead_letters (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL,
    interaction_id UUID NOT NULL,
    customer_id UUID,
    job_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    error TEXT NOT NULL,
    failed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Global LLM rate limit windows (per-minute)
CREATE TABLE llm_rate_limit_windows (
    window_start TIMESTAMPTZ PRIMARY KEY,
    requests_used INTEGER NOT NULL DEFAULT 0,
    tokens_used INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Per-customer budgets and per-minute usage windows
CREATE TABLE customer_llm_budgets (
    customer_id UUID PRIMARY KEY,
    tokens_per_minute_limit INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE customer_llm_budget_windows (
    customer_id UUID NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    tokens_used INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (customer_id, window_start)
);
```

---

## 11. Security

**Sensitive data in this system:**
- Call transcripts (contain conversation content, customer PII)
- Lead records (name, phone, email)
- Call recordings (audio files in S3)
- LLM API keys and Exotel credentials

**Protection strategy:**

*At rest:*
- Transcripts stored in `interactions.conversation_data` (JSONB): encrypt the column
  at the application layer using Fernet (symmetric, key from KMS/env secret) before
  writing, decrypt on read. Alternatively, use Postgres TDE or RDS encryption
  if managed infra is available.
- Recordings in S3: enable SSE-S3 (AES-256) on the bucket. For higher compliance
  requirements, use SSE-KMS with a customer-managed key.
- PII fields in `leads` (phone, email): consider field-level encryption or
  pseudonymisation if the platform needs to comply with DPDP/GDPR.

*In transit:*
- All API calls (Exotel webhook, LLM provider, CRM) over TLS 1.2+.
- Exotel webhook endpoint should validate a shared HMAC secret on every
  `POST /session/.../end` request to prevent replay or spoofed webhooks.
- Internal service communication (worker → Postgres, worker → Redis) over VPC
  private networking; no public exposure.

*In audit logs:*
- `audit_events.data` must never log raw transcript text or PII.
  Structured events store `interaction_id` for correlation; full transcript
  is retrieved from `interactions` table only when needed.
- LLM prompts (which contain the transcript) must not be logged at INFO level
  in production — only log `interaction_id` and token counts.

*Access control:*
- Per-customer data isolation: all queries filter by `customer_id`.
  Row-level security (Postgres RLS) is the right long-term enforcement mechanism,
  ensuring even a miscoded query cannot leak cross-customer data.
- Dead-letters table contains full payloads (including transcript) — access
  should be restricted to on-call engineers via IAM/RBAC, not available to
  all application service accounts.
---

## 12. API Interface

<!-- _Did you change the API contract (`POST /session/.../end`)? If yes, explain why. If no, explain why you kept it._ -->

I kept the API contract the same because it matches the telephony provider webhook expectations and needs to return quickly.

The main change is internal: on call-end the endpoint writes durable jobs (`recording`, `llm`, `downstream`) and audit events. The legacy Celery enqueue is behind a feature flag (`ENABLE_LEGACY_CELERY_PIPELINE`) to support a safe migration without double-processing.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What I Chose Instead |
|---|---|---|
| SQS/RabbitMQ as job queue instead of Postgres | Standard message queue; simpler worker code | Adds infra dependency; no queryable job state; harder to implement leases and stale-claim recovery without extra DB anyway |
| Redis for rate limit counters instead of Postgres | Lower latency per counter increment | Same failure mode as the thing we're replacing; Redis restart loses all in-flight rate limit state; Postgres gives durable, queryable windows |
| Token bucket instead of per-minute windows | Smoother distribution; avoids burst-at-window-boundary | More complex to implement atomically in Postgres; per-minute windows are simple, auditable, and match how LLM providers actually report limits |
| Separate classification LLM call before full analysis | Better accuracy for hot/cold triage | Doubles LLM requests for every call; keyword matching is fast, free, and good enough for triage — LLM can always reclassify on the full result |
| Keep Celery, fix its problems | Less code change | Celery's durability story requires persistent broker (RabbitMQ) and result backend changes anyway; Postgres job table gives us the same guarantee with fewer moving parts and full auditability |
| Fair-share unallocated budget headroom | Better utilisation of spare capacity | Adds coordination complexity (who gets the spare?); simple per-customer cap with platform-level spillover is easier to reason about and audit |

---

## 14. Known Weaknesses

- `requeue_stale_claims(...)` exists but needs a periodic watchdog runner in production.
- Rate limiting is per-minute windows (safe and simple) rather than a smoother token bucket over sub-minute intervals.
- Hot jobs are classified correctly at ingest but worker claim query orders by `available_at` only — lane is not yet in the ORDER BY, so hot jobs only get priority if they arrive earlier.
- Downstream job currently receives a placeholder `analysis_result: {}`; the production fix is the LLM worker updating `interactions.interaction_metadata` post-analysis, which the downstream worker then reads before triggering CRM/signal actions.
- The three job inserts in the endpoint (recording, llm, downstream) are committed separately rather than in a single transaction — a failure between inserts can leave partial job state. Production fix is wrapping all three in one `BEGIN/COMMIT`.
- `asyncio.create_task()` calls remain in the long-transcript path of the endpoint for signal_jobs and lead_stage. These are redundant (the downstream job handles both) and not durable across restarts. They should be removed in favour of relying solely on the downstream worker.
- Actual token usage from the LLM response is logged per-call but not written to an aggregate counter — "tokens used this hour by customer X" requires log scanning rather than a DB query.

---

## 15. What I Would Do With More Time

1. Add `CASE lane WHEN 'hot' THEN 0 ELSE 1 END ASC` to the `claim_next_job` ORDER BY so hot jobs are genuinely prioritised ahead of cold in the worker queue.
2. Wrap the three `enqueue_job` calls in the endpoint in a single DB transaction so partial job state is impossible on failure.
3. Remove the redundant `asyncio.create_task()` calls from the long-transcript path — downstream worker already handles signal_jobs and lead_stage durably.
4. Wire downstream worker to read LLM output from `interactions.interaction_metadata` rather than a placeholder payload.
5. Add a small watchdog process to run `requeue_stale_claims(...)` periodically and emit audit events for recovered jobs.
6. Persist actual token usage as a billing-grade ledger (customer/campaign/interaction) rather than only audit events.
7. Add a DB-backed end-to-end integration test that runs the workers against a real Postgres container.