# Booking Engine: Data Model and Concurrency Design

## Entities, attributes, and table structures

The executable definitions and keys are in `sql/01_schema.sql`. All tables use InnoDB.

| Entity/table | Attributes | Key relationships |
|---|---|---|
| `app_users` | `user_id`, `email`, `full_name`, `created_at` | Unique email; user owns bookings. |
| `theatres` | `theatre_id`, `theatre_name`, `city`, `address`, `created_at` | Theatre contains screens. |
| `screens` | `screen_id`, `theatre_id`, `screen_name`, `capacity` | Unique screen name per theatre. |
| `seats` | `seat_id`, `screen_id`, `row_label`, `seat_number`, `seat_type` | Unique position within a screen. |
| `movies` | `movie_id`, `title`, `duration_minutes`, `language_code`, `certificate` | Movie is shown in one or more shows. |
| `shows` | `show_id`, `screen_id`, `movie_id`, `show_date`, `starts_at`, `ends_at`, `base_price`, `status` | Unique screen/start time; show is one movie in one screen. |
| `show_seats` | `show_id`, `seat_id`, `price`, `status`, `hold_token`, `held_until`, `version` | Composite PK `(show_id, seat_id)`; one inventory row per seat per show. |
| `bookings` | `booking_id`, `booking_reference`, `user_id`, `show_id`, `hold_token`, `idempotency_key`, `request_hash`, `status`, `total_amount`, `expires_at`, `created_at` | One user/show per booking; unique reference, hold token, and idempotency key. |
| `booking_seats` | `booking_id`, `seat_id`, `price` | Composite PK `(booking_id, seat_id)`; stores booking-time price. |
| `payments` | `payment_id`, `booking_id`, `provider_payment_id`, `amount`, `status`, `provider_created_at`, `updated_at` | At most one payment per booking; unique provider payment id. |
| `payment_webhook_events` | `webhook_event_id`, `provider_event_id`, `provider_payment_id`, `event_type`, `payload`, `received_at`, `processed_at` | Unique provider event id is the webhook idempotency key. |

## Example rows

Representative rows from `sql/02_seed.sql` (auto-increment IDs shown as assigned in a fresh database):

| Table | Example row |
|---|---|
| `app_users` | `(1, 'aisha@example.com', 'Aisha Rao')` |
| `theatres` | `(1, 'PVR Orion Mall', 'Bengaluru', '26 Brigade Road')` |
| `screens` | `(1, 1, 'Screen 1', 6)` |
| `seats` | `(1, 1, 'A', 1, 'REGULAR')` |
| `movies` | `(1, 'The Last Monsoon', 142, 'EN', 'U/A')` |
| `shows` | `(1, 1, 1, '2026-10-01', '2026-10-01 10:00:00', '2026-10-01 12:22:00', 250.00, 'SCHEDULED')` |
| `show_seats` | `(1, 1, 250.00, 'AVAILABLE', NULL, NULL, 0)` |

## Normal forms

- **1NF:** Values are scalar; one booking seat and one show-seat inventory item occupy one row. There are no repeating seat arrays or comma-separated lists.
- **2NF:** Non-key attributes in composite-key tables depend on the complete key. `show_seats.price` depends on `(show_id, seat_id)`; `booking_seats.price` depends on `(booking_id, seat_id)`.
- **3NF:** Movie and theatre attributes live in their own relations rather than being copied to shows; seat placement lives in `seats`, not in every show inventory row. Booking totals are a deliberate transaction snapshot, not a value used to derive the current catalogue.
- **BCNF:** Each listed determinant is a candidate key: primary keys and declared unique keys cover user email, theatre/screen name, screen/seat position, screen/start time, booking reference/hold/idempotency token, booking-seat identity, payment booking/provider identity, and webhook event id. This is subject to the stated business rules (e.g. a screen cannot run two shows with the same start time).

`booking_seats` intentionally does not repeat `show_id`: it is functionally determined by `booking_id`. A booking's show is reached through `bookings`; the service inserts booking seats only after locking and validating inventory for that show. The stored seat price is a historical booking snapshot.

## Discovery queries (P2 and date picker)

Run `sql/03_queries.sql` in MySQL 8.0+ after initialization. The P2 query returns all scheduled shows at a selected theatre on a selected `show_date`, including movie, screen and start/end times. The recursive date query returns seven consecutive calendar dates starting today and an indicator for dates with scheduled shows. `show_date` is indexed with screen and status for the discovery access path.

## Hold lifecycle and no-double-book invariant

`POST /holds` puts a hold request on the Redis Stream and returns a request id; `GET /holds/requests/{request_id}` provides the result. A consumer group allows configurable worker threads/instances to share the burst; abandoned pending entries are reclaimed. The worker executes these steps:

1. Canonicalize the requested seat set and bind the UUID idempotency key to a SHA-256 request fingerprint.
2. Atomically claim all `seat:{show_id}:{seat_id}` Redis keys using Lua `SET NX` semantics and a TTL. If any key belongs to another active request, reject without changing SQL.
3. Start a short MySQL transaction and lock requested `show_seats` rows in ascending `seat_id` order using `SELECT ... FOR UPDATE`.
4. Require every requested inventory row to exist, the show to be scheduled, and each seat to be available or held-but-expired. A current live hold, booked seat, or blocked seat is rejected.
5. Update all rows under the same token and expiry, check the updated row count, insert one booking plus its booking-seat rows, and commit.
6. On transaction failure, roll back and release only Redis keys still owned by that token. On API/worker retry with the same key and same payload, return the already-created booking. Reusing a key with different parameters is rejected.

The Redis TTL frees fast-path leases automatically. MySQL remains authoritative: its row lock serializes competing writers, the composite primary key guarantees one inventory row for a show-seat pair, and expiration is checked using MySQL time before a hold is replaced. The app runs `release_expired_holds()` every 15 seconds to clean expired SQL state; no timing correctness depends on that cleanup being punctual. Payment confirmation also checks that the booking and each matching seat hold remain live.

This combines **pessimistic locking** (the authoritative SQL row lock) with **optimistic metadata** (`version` for clients/caches). Acquiring multiple seat locks in sorted order reduces deadlocks. External payment calls are never made inside the SQL transaction. On database transient errors, the stream message stays pending for retry; validation conflicts are acknowledged and returned to the requester.

## Payment webhook idempotency

The webhook endpoint can require an HMAC-SHA256 signature (`PAYMENT_WEBHOOK_SECRET`) over the exact raw HTTP body. A transaction inserts the unique provider event, locks the booking and associated inventory rows, validates status, expiry, hold-token ownership, and exact payment amount, inserts one provider payment, marks the booking confirmed and inventory booked, and marks the event processed. Duplicate provider event ids return success without repeating state changes. A late or mismatched payment is rejected; production handling should enqueue a refund/manual reconciliation rather than confirm seats.

## Verification and limits

`tests/concurrency_test.py` starts a configurable burst of concurrent HTTP hold requests against one fresh seat, expects exactly one hold and all other requests to conflict, then verifies request idempotency and duplicate webhook handling. It reports observed p95 and approximate throughput. Run with Docker using the steps in `README.md`.

This is a runnable educational reference, not a claim of production-scale capacity. It has no payment-provider SDK, distributed tracing, API authentication/rate limiting, multi-region consistency, durable refund workflow, or representative multi-seat/multi-show benchmark yet. Redis and MySQL are single-node Compose services. Scale validation should vary seat hotness and show count, then observe API p95/p99, queue lag, Redis memory, MySQL lock waits/deadlocks, and throughput under a sustained test.
