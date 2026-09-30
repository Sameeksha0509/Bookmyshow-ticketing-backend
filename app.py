"""Queue-backed booking API with Redis leases and MySQL as source of truth."""

from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal
import hashlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from typing import Annotated

import mysql.connector
from fastapi import FastAPI, Header, HTTPException
from fastapi import Request as HttpRequest
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
import redis

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("booking-engine")

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "db"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "booking_app"),
    "password": os.getenv("MYSQL_PASSWORD", "booking_app_password"),
    "database": os.getenv("MYSQL_DATABASE", "booking_engine"),
    "connection_timeout": 5,
}
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
HOLD_TTL_SECONDS = int(os.getenv("HOLD_TTL_SECONDS", "600"))
QUEUE_WORKERS = max(1, int(os.getenv("QUEUE_WORKERS", "4")))
QUEUE = "booking:hold-requests"
GROUP = "booking-workers"
RESULT_TTL_SECONDS = 60

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
STOP_WORKER = threading.Event()

CLAIM_SEATS_LUA = """
for i, key in ipairs(KEYS) do
    local current = redis.call('GET', key)
    if current and current ~= ARGV[1] then return 0 end
end
for i, key in ipairs(KEYS) do redis.call('SET', key, ARGV[1], 'EX', ARGV[2]) end
return 1
"""
RELEASE_SEATS_LUA = """
for i, key in ipairs(KEYS) do
    if redis.call('GET', key) == ARGV[1] then redis.call('DEL', key) end
end
return 1
"""


class HoldRequest(BaseModel):
    show_id: int = Field(gt=0)
    seat_ids: list[int] = Field(min_length=1, max_length=10)
    user_id: int = Field(gt=0)
    idempotency_key: uuid.UUID


class WebhookRequest(BaseModel):
    event_id: str = Field(min_length=1, max_length=120)
    event_type: str
    provider_payment_id: str = Field(min_length=1, max_length=120)
    booking_reference: str = Field(min_length=1, max_length=12)
    amount: Decimal = Field(ge=0)


def db_connect():
    conn = mysql.connector.connect(**DB_CONFIG)
    # The hold path does a read before explicitly opening its write transaction.
    # Autocommit keeps that idempotency lookup from leaving an implicit transaction open.
    conn.autocommit = True
    return conn


def seat_lock_keys(show_id: int, seat_ids: list[int]) -> list[str]:
    return [f"seat:{show_id}:{seat_id}" for seat_id in sorted(set(seat_ids))]


def process_hold(payload: dict) -> dict:
    show_id = int(payload["show_id"])
    user_id = int(payload["user_id"])
    seat_ids = sorted(set(int(value) for value in payload["seat_ids"]))
    token = str(uuid.UUID(payload["idempotency_key"]))
    if len(seat_ids) != len(payload["seat_ids"]):
        raise ValueError("duplicate seat ids are not allowed")
    canonical_payload = {**payload, "seat_ids": seat_ids, "idempotency_key": token}
    request_hash = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    conn = db_connect()
    cursor = conn.cursor(dictionary=True)
    lock_keys = seat_lock_keys(show_id, seat_ids)
    acquired = False
    try:
        cursor.execute(
            "SELECT booking_id, booking_reference, status, request_hash FROM bookings WHERE idempotency_key = %s",
            (token,),
        )
        previous = cursor.fetchone()
        if previous:
            if previous["request_hash"] != request_hash:
                raise ValueError("idempotency key was already used for a different request")
            return {"booking_id": previous["booking_id"], "booking_reference": previous["booking_reference"], "status": previous["status"], "idempotent_replay": True}

        acquired = bool(redis_client.eval(CLAIM_SEATS_LUA, len(lock_keys), *lock_keys, token, HOLD_TTL_SECONDS))
        if not acquired:
            raise ValueError("one or more seats are currently being held")

        conn.start_transaction()
        cursor.execute("SELECT status, screen_id FROM shows WHERE show_id = %s", (show_id,))
        show = cursor.fetchone()
        if not show or show["status"] != "SCHEDULED":
            raise ValueError("show does not exist or is not scheduled")

        placeholders = ",".join(["%s"] * len(seat_ids))
        cursor.execute(
                        f"""SELECT ss.seat_id, ss.price, ss.status, ss.hold_token, ss.held_until,
                                            (ss.held_until > UTC_TIMESTAMP(6)) AS hold_is_live
                             FROM show_seats ss
                             JOIN seats se ON se.seat_id = ss.seat_id
                             WHERE ss.show_id = %s AND ss.seat_id IN ({placeholders})
                                 AND se.screen_id = %s
                             ORDER BY ss.seat_id FOR UPDATE""",
                        (show_id, *seat_ids, show["screen_id"]),
        )
        rows = cursor.fetchall()
        if len(rows) != len(seat_ids):
            raise ValueError("one or more seats do not belong to this show")
        if all(row["status"] == "HELD" and row["hold_token"] == token for row in rows):
            cursor.execute(
                "SELECT booking_id, booking_reference, status, request_hash FROM bookings WHERE idempotency_key = %s",
                (token,),
            )
            previous = cursor.fetchone()
            if previous:
                if previous["request_hash"] != request_hash:
                    raise ValueError("idempotency key was already used for a different request")
                conn.commit()
                return {"booking_id": previous["booking_id"], "booking_reference": previous["booking_reference"], "status": previous["status"], "idempotent_replay": True}
        if any(row["status"] in ("BOOKED", "BLOCKED") or (row["status"] == "HELD" and row["hold_is_live"]) for row in rows):
            raise ValueError("one or more seats are unavailable")

        cursor.execute(
            f"""UPDATE show_seats
                SET status = 'HELD', hold_token = %s,
                    held_until = UTC_TIMESTAMP(6) + INTERVAL %s SECOND,
                    version = version + 1
                WHERE show_id = %s AND seat_id IN ({placeholders})""",
            (token, HOLD_TTL_SECONDS, show_id, *seat_ids),
        )
        if cursor.rowcount != len(seat_ids):
            raise RuntimeError("seat inventory changed during hold")

        total = sum((row["price"] for row in rows), Decimal("0.00"))
        booking_reference = uuid.uuid4().hex[:12].upper()
        cursor.execute(
            """INSERT INTO bookings
               (booking_reference, user_id, show_id, hold_token, idempotency_key, request_hash,
                status, total_amount, expires_at)
               VALUES (%s, %s, %s, %s, %s, %s, 'PENDING_PAYMENT', %s,
                       UTC_TIMESTAMP(6) + INTERVAL %s SECOND)""",
            (booking_reference, user_id, show_id, token, token, request_hash, total, HOLD_TTL_SECONDS),
        )
        booking_id = cursor.lastrowid
        cursor.executemany(
            "INSERT INTO booking_seats (booking_id, seat_id, price) VALUES (%s, %s, %s)",
            [(booking_id, row["seat_id"], row["price"]) for row in rows],
        )
        conn.commit()
        return {"booking_id": booking_id, "booking_reference": booking_reference, "status": "PENDING_PAYMENT", "hold_expires_in_seconds": HOLD_TTL_SECONDS, "idempotent_replay": False}
    except Exception:
        conn.rollback()
        if acquired:
            redis_client.eval(RELEASE_SEATS_LUA, len(lock_keys), *lock_keys, token)
        raise
    finally:
        cursor.close()
        conn.close()


def process_payment_webhook(payload: dict, raw_payload: str) -> dict:
    conn = db_connect()
    cursor = conn.cursor(dictionary=True)
    try:
        conn.start_transaction()
        cursor.execute(
            """INSERT IGNORE INTO payment_webhook_events
               (provider_event_id, provider_payment_id, event_type, payload)
               VALUES (%s, %s, %s, %s)""",
            (payload["event_id"], payload["provider_payment_id"], payload["event_type"], raw_payload),
        )
        if cursor.rowcount == 0:
            conn.commit()
            return {"accepted": True, "duplicate": True}
        if payload["event_type"] != "payment.succeeded":
            raise ValueError("only payment.succeeded events are supported by this example")

        cursor.execute(
            """SELECT booking_id, show_id, hold_token, status, total_amount,
                      (expires_at > UTC_TIMESTAMP(6)) AS booking_is_live
               FROM bookings WHERE booking_reference = %s FOR UPDATE""",
            (payload["booking_reference"],),
        )
        booking = cursor.fetchone()
        if not booking:
            raise ValueError("booking not found")
        if Decimal(str(booking["total_amount"])) != Decimal(str(payload["amount"])):
            raise ValueError("payment amount does not match booking total")
        if booking["status"] != "PENDING_PAYMENT" or not booking["booking_is_live"]:
            raise ValueError("booking is no longer eligible for payment confirmation")

        cursor.execute(
            """SELECT ss.seat_id, ss.status, ss.hold_token, ss.held_until
               FROM show_seats ss JOIN booking_seats bs ON bs.seat_id = ss.seat_id
               WHERE ss.show_id = %s AND bs.booking_id = %s
               ORDER BY ss.seat_id FOR UPDATE""",
            (booking["show_id"], booking["booking_id"]),
        )
        inventory = cursor.fetchall()
        valid_seats = [
            seat for seat in inventory
            if seat["status"] == "HELD"
            and seat["hold_token"] == booking["hold_token"]
            and seat["held_until"] is not None
        ]
        cursor.execute("SELECT UTC_TIMESTAMP(6) AS now_utc")
        now_utc = cursor.fetchone()["now_utc"]
        if not inventory or len(valid_seats) != len(inventory) or any(seat["held_until"] <= now_utc for seat in valid_seats):
            raise ValueError("seat hold expired or no longer belongs to this booking")

        cursor.execute(
            """INSERT INTO payments
               (booking_id, provider_payment_id, amount, status, provider_created_at)
               VALUES (%s, %s, %s, 'SUCCEEDED', UTC_TIMESTAMP(6))""",
            (booking["booking_id"], payload["provider_payment_id"], booking["total_amount"]),
        )
        cursor.execute("UPDATE bookings SET status = 'CONFIRMED' WHERE booking_id = %s", (booking["booking_id"],))
        cursor.execute(
            """UPDATE show_seats ss JOIN booking_seats bs ON bs.seat_id = ss.seat_id
               SET ss.status = 'BOOKED', ss.held_until = NULL, ss.version = ss.version + 1
               WHERE ss.show_id = %s AND bs.booking_id = %s
                 AND ss.hold_token = %s AND ss.status = 'HELD'""",
            (booking["show_id"], booking["booking_id"], booking["hold_token"]),
        )
        cursor.execute(
            "UPDATE payment_webhook_events SET processed_at = UTC_TIMESTAMP(6) WHERE provider_event_id = %s",
            (payload["event_id"],),
        )
        conn.commit()
        try:
            redis_client.eval(
                RELEASE_SEATS_LUA,
                len(inventory),
                *[f"seat:{booking['show_id']}:{seat['seat_id']}" for seat in inventory],
                booking["hold_token"],
            )
        except redis.RedisError:
            logger.exception("Payment confirmed but Redis lease cleanup failed; TTL will release it")
        return {"accepted": True, "duplicate": False, "booking_status": "CONFIRMED"}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def worker_loop(consumer: str):
    try:
        redis_client.xgroup_create(QUEUE, GROUP, id="0", mkstream=True)
    except redis.ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise
    while not STOP_WORKER.is_set():
        try:
            reclaimed = redis_client.xautoclaim(QUEUE, GROUP, consumer, min_idle_time=5000, start_id="0-0", count=16)
            pending_entries = reclaimed[1] if reclaimed else []
            if pending_entries:
                batches = [(QUEUE, pending_entries)]
            else:
                batches = redis_client.xreadgroup(GROUP, consumer, {QUEUE: ">"}, count=16, block=1000)
            for _, entries in batches:
                for message_id, fields in entries:
                    try:
                        payload = json.loads(fields["payload"])
                        redis_client.set(f"booking:result:{fields['request_id']}", json.dumps({"request_status": "PROCESSING"}), ex=RESULT_TTL_SECONDS)
                        result = process_hold(payload)
                        redis_client.set(f"booking:result:{fields['request_id']}", json.dumps({**result, "request_status": "COMPLETED"}), ex=RESULT_TTL_SECONDS)
                        redis_client.xack(QUEUE, GROUP, message_id)
                    except ValueError as error:
                        redis_client.set(f"booking:result:{fields['request_id']}", json.dumps({"error": str(error), "request_status": "FAILED"}), ex=RESULT_TTL_SECONDS)
                        redis_client.xack(QUEUE, GROUP, message_id)
                    except Exception:
                        logger.exception("Queued hold failed")
                        # Leave the entry pending; XAUTOCLAIM retries after the idle timeout.
        except redis.RedisError:
            logger.exception("Redis queue unavailable")
            STOP_WORKER.wait(1)


def expiry_cleanup_loop():
    while not STOP_WORKER.wait(15):
        conn = None
        cursor = None
        try:
            conn = db_connect()
            cursor = conn.cursor()
            cursor.callproc("release_expired_holds")
            conn.commit()
        except mysql.connector.Error:
            logger.exception("Expired hold cleanup failed")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    STOP_WORKER.clear()
    workers = [
        threading.Thread(
            target=worker_loop,
            args=(f"worker-{uuid.uuid4()}",),
            daemon=True,
            name=f"hold-queue-worker-{index}",
        )
        for index in range(QUEUE_WORKERS)
    ]
    cleanup = threading.Thread(target=expiry_cleanup_loop, daemon=True, name="hold-expiry-cleanup")
    for worker in workers:
        worker.start()
    cleanup.start()
    yield
    STOP_WORKER.set()
    for worker in workers:
        worker.join(timeout=2)
    cleanup.join(timeout=2)


app = FastAPI(title="High-Concurrency Booking Engine", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    try:
        redis_client.ping()
        conn = db_connect()
        conn.close()
        return {"status": "ok"}
    except Exception as error:
        raise HTTPException(status_code=503, detail="dependency unavailable") from error


@app.post("/holds", status_code=202)
def create_hold(request: HoldRequest):
    request_id = str(uuid.uuid4())
    payload = request.model_dump(mode="json")
    try:
        redis_client.xadd(QUEUE, {"request_id": request_id, "payload": json.dumps(payload)}, maxlen=100000, approximate=True)
        return {"request_id": request_id, "request_status": "QUEUED"}
    except redis.RedisError as error:
        raise HTTPException(status_code=503, detail="booking queue is unavailable") from error


@app.get("/holds/requests/{request_id}")
def get_hold_request(request_id: uuid.UUID):
    try:
        result = redis_client.get(f"booking:result:{request_id}")
    except redis.RedisError as error:
        raise HTTPException(status_code=503, detail="booking queue is unavailable") from error
    if not result:
        return {"request_id": str(request_id), "request_status": "PENDING"}
    decoded = json.loads(result)
    if decoded.get("request_status") == "FAILED":
        raise HTTPException(status_code=409, detail=decoded["error"])
    return decoded


@app.post("/webhooks/payments")
async def payment_webhook(
    request: WebhookRequest,
    http_request: HttpRequest,
    signature: Annotated[str | None, Header(alias="X-Webhook-Signature")] = None,
):
    raw_payload = await http_request.body()
    secret = os.getenv("PAYMENT_WEBHOOK_SECRET", "")
    if secret:
        expected = hmac.new(secret.encode(), raw_payload, hashlib.sha256).hexdigest()
        if not signature or not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="invalid webhook signature")
    try:
        return process_payment_webhook(request.model_dump(mode="json"), raw_payload.decode())
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except mysql.connector.Error as error:
        raise HTTPException(status_code=503, detail="payment processing failed; retry webhook") from error
