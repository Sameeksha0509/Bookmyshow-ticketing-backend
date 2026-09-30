"""HTTP-level burst test for seat locking, idempotency, and payment webhooks."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import hashlib
import hmac
import json
import os
import sys
import time
import uuid

import mysql.connector
import requests


DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3307")),
    "user": os.getenv("MYSQL_USER", "booking_app"),
    "password": os.getenv("MYSQL_PASSWORD", "booking_app_password"),
    "database": os.getenv("MYSQL_DATABASE", "booking_engine"),
}
API_URL = os.getenv("BOOKING_API_URL", "http://127.0.0.1:8000")
WORKERS = max(1, int(os.getenv("BOOKING_TEST_WORKERS", "32")))
REQUEST_TIMEOUT_SECONDS = 30


def submit_and_wait(request_body: dict) -> tuple[int, dict]:
    response = requests.post(f"{API_URL}/holds", json=request_body, timeout=10)
    if response.status_code != 202:
        return response.status_code, {"error": response.text}
    request_id = response.json()["request_id"]
    deadline = time.monotonic() + REQUEST_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        result = requests.get(f"{API_URL}/holds/requests/{request_id}", timeout=10)
        if result.status_code == 409:
            return 409, {"error": result.json().get("detail", result.text)}
        body = result.json()
        if body.get("request_status") == "COMPLETED":
            return 202, body
        time.sleep(0.05)
    return 504, {"error": "timed out waiting for queue result"}


def create_test_show() -> tuple[int, int]:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        start_minute = int(uuid.uuid4().hex[:4], 16) % 1440
        cursor.execute(
            """INSERT INTO shows
               (screen_id, movie_id, show_date, starts_at, ends_at, base_price)
               VALUES (2, 3, UTC_DATE() + INTERVAL 4 DAY,
                       DATE_ADD(UTC_DATE() + INTERVAL 4 DAY, INTERVAL %s MINUTE),
                       DATE_ADD(DATE_ADD(UTC_DATE() + INTERVAL 4 DAY, INTERVAL %s MINUTE),
                                INTERVAL 2 HOUR), 199.00)""",
            (start_minute, start_minute),
        )
        show_id = cursor.lastrowid
        cursor.execute(
            """INSERT INTO show_seats (show_id, seat_id, price)
               SELECT %s, seat_id, 199.00 FROM seats
               WHERE screen_id = 2 ORDER BY seat_id LIMIT 1""",
            (show_id,),
        )
        cursor.execute("SELECT seat_id FROM show_seats WHERE show_id = %s", (show_id,))
        seat_id = cursor.fetchone()[0]
        conn.commit()
        return show_id, seat_id
    finally:
        cursor.close()
        conn.close()


def attempt_hold(show_id: int, seat_id: int, worker_number: int) -> tuple[int, dict, float]:
    request_body = {
        "show_id": show_id,
        "seat_ids": [seat_id],
        "user_id": 1 if worker_number % 2 == 0 else 2,
        "idempotency_key": str(uuid.uuid4()),
    }
    started = time.perf_counter()
    try:
        status_code, body = submit_and_wait(request_body)
        return status_code, {"body": body, "request": request_body}, time.perf_counter() - started
    except requests.RequestException as error:
        return 0, {"body": {"error": str(error)}, "request": request_body}, time.perf_counter() - started


def verify_payment_idempotency(winner: dict) -> None:
    booking = winner["body"]
    request = winner["request"]
    replay_status, replay = submit_and_wait(request)
    assert replay_status == 202, replay
    assert replay["idempotent_replay"] is True

    mismatched_request = {**request, "user_id": 2 if request["user_id"] == 1 else 1}
    mismatch_status, mismatch = submit_and_wait(mismatched_request)
    assert mismatch_status == 409 and "idempotency key" in mismatch.get("error", ""), mismatch

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT total_amount FROM bookings WHERE booking_id = %s", (booking["booking_id"],))
        amount = str(Decimal(cursor.fetchone()[0]))
    finally:
        cursor.close()
        conn.close()

    webhook = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "event_type": "payment.succeeded",
        "provider_payment_id": f"pay_{uuid.uuid4().hex}",
        "booking_reference": booking["booking_reference"],
        "amount": amount,
    }
    raw_body = json.dumps(webhook, separators=(",", ":")).encode()
    headers = {"Content-Type": "application/json"}
    secret = os.getenv("PAYMENT_WEBHOOK_SECRET", "")
    if secret:
        headers["X-Webhook-Signature"] = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    invalid_payment = {**webhook, "event_id": f"evt_{uuid.uuid4().hex}", "amount": str(Decimal(amount) + Decimal("1.00"))}
    invalid_body = json.dumps(invalid_payment, separators=(",", ":")).encode()
    invalid_headers = dict(headers)
    if secret:
        invalid_headers["X-Webhook-Signature"] = hmac.new(secret.encode(), invalid_body, hashlib.sha256).hexdigest()
    invalid = requests.post(f"{API_URL}/webhooks/payments", data=invalid_body, headers=invalid_headers, timeout=20)
    assert invalid.status_code == 409, invalid.text

    first = requests.post(f"{API_URL}/webhooks/payments", data=raw_body, headers=headers, timeout=20)
    duplicate = requests.post(f"{API_URL}/webhooks/payments", data=raw_body, headers=headers, timeout=20)
    assert first.status_code == 200, first.text
    assert duplicate.status_code == 200 and duplicate.json()["duplicate"] is True, duplicate.text


def main() -> None:
    show_id, seat_id = create_test_show()
    burst_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(attempt_hold, show_id, seat_id, index) for index in range(WORKERS)]
        results = [future.result() for future in as_completed(futures)]

    winners = [result for result in results if result[0] == 202]
    conflicts = [result for result in results if result[0] == 409]
    errors = [result for result in results if result[0] not in (202, 409)]
    latencies_ms = sorted(result[2] * 1000 for result in results)
    p95 = latencies_ms[min(len(latencies_ms) - 1, int(len(latencies_ms) * 0.95))]
    elapsed = max(time.perf_counter() - burst_started, 0.001)
    throughput = len(results) / max(elapsed, 0.001)

    if len(winners) != 1 or len(conflicts) != WORKERS - 1 or errors:
        raise AssertionError(
            f"expected one winner and {WORKERS - 1} conflicts; got "
            f"{len(winners)} winners, {len(conflicts)} conflicts, {len(errors)} errors: {errors[:2]}"
        )
    verify_payment_idempotency(winners[0][1])

    print(f"PASS: {WORKERS} concurrent HTTP requests; 1 hold, {len(conflicts)} conflicts")
    print(f"HTTP p95 latency: {p95:.1f} ms; approximate burst throughput: {throughput:.1f} requests/s")
    print("PASS: same idempotency key replays booking; duplicate payment webhook is idempotent")


if __name__ == "__main__":
    try:
        main()
    except (mysql.connector.Error, requests.RequestException, AssertionError) as error:
        print(f"TEST FAILED: {error}", file=sys.stderr)
        sys.exit(1)
