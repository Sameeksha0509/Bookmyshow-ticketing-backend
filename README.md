# High-Concurrency Booking Engine

A runnable MySQL + Redis reference backend for theatre show discovery, timed seat holds, queued booking requests, and idempotent payment webhooks.

## Run locally with Docker

Requirements: Docker Desktop (Linux containers) and Python 3.10+ on the host for the integration test.

```powershell
docker compose up --build -d
```

This starts MySQL 8.4, Redis 7.4, and the FastAPI service at `http://localhost:8000`. The database initializes from `sql/01_schema.sql` and `sql/02_seed.sql` on the first run. API health is available at `/health`; interactive API docs at `/docs`.

Each API instance runs four Redis Stream consumers by default (`QUEUE_WORKERS`). To scale workers horizontally, use multiple app instances behind a load balancer; the fixed local Compose port is intended for the single-instance development setup.

Run the 32-request hot-seat burst test from another terminal:

```powershell
python -m pip install -r tests/requirements.txt
python tests/concurrency_test.py
```

Set `BOOKING_TEST_WORKERS` to change the number of simultaneous requests. The test creates a fresh show/seat, expects exactly one winner, prints p95 and approximate burst throughput, retries the winner with the same idempotency key, and posts the same payment webhook twice.

For local development without Dockerized API, install `requirements.txt` and set `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, and `REDIS_URL` before running `uvicorn app:app --reload`.

## API overview

- `POST /holds` accepts `show_id`, `seat_ids`, `user_id`, and a client-generated UUID `idempotency_key`, returning a queue request id immediately.
- `GET /holds/requests/{request_id}` retrieves pending or completed hold processing results.
- `POST /webhooks/payments` accepts a successful payment event. Configure `PAYMENT_WEBHOOK_SECRET` to require an HMAC-SHA256 `X-Webhook-Signature` header over the compact JSON payload.
- `GET /health` checks the MySQL and Redis dependencies.

## Project files

- `app.py`: API, configurable Redis Stream consumers, Redis seat leases, transactional MySQL hold/payment operations, and 15-second expiry cleanup.
- `sql/01_schema.sql`: tables, constraints, indexes, and the expiry procedure.
- `sql/02_seed.sql`: sample users, theatres, screens, seats, movies, shows, and inventory.
- `sql/03_queries.sql`: P2 shows query, seven-day date-picker query, and seat map query.
- `docs/solution.md`: entities, sample rows, normalization, concurrency invariants, design trade-offs, and limitations.
- `tests/concurrency_test.py`: HTTP burst, seat exclusivity, hold idempotency, and duplicate webhook integration test.

## Concurrency model

Redis Stream queues burst requests and a consumer group distributes them across API instances. A Lua script atomically claims all requested Redis seat keys with a TTL, while MySQL `SELECT ... FOR UPDATE` and the `show_seats` primary key provide the durable correctness boundary. MySQL commits the seat state and booking together. If Redis is unavailable, new hold requests fail closed instead of bypassing the queue.

Redis key expiry releases the fast-path lease automatically. The database stores `held_until`; booking requests treat expired rows as reusable while holding the row lock. A cleanup thread runs `release_expired_holds()` every 15 seconds. Webhook processing locks the booking and inventory, validates the exact amount and unexpired ownership, and uses a unique event id to make retries idempotent.

## Clean up

```powershell
docker compose down -v
```

This removes the local database and Redis volumes.
