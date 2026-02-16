"""
Load generator for the chat-db-app.

Simulates multiple users sending messages concurrently.
Configurable intensity from light to heavy load.

Under light load: app is healthy.
Under heavy load: naive patterns break (pool exhaustion, lock contention).

Environment variables:
    APP_URL: Base URL of the chat app (default: http://app:8000)
    NUM_USERS: Number of simulated concurrent users (default: 3)
    REQUEST_DELAY: Seconds between requests per user (default: 2.0)
    STREAM_RATIO: Fraction of requests that are streaming (default: 0.3)
    RAMP_UP_SECONDS: Time to ramp up to full load (default: 10)
    READ_RATIO: Fraction of non-streaming requests that read messages (default: 0.3)
    BURST_MODE: When true, each user fires concurrent writes to same conversation (default: false)
    BURST_CONCURRENCY: Parallel writes per burst when BURST_MODE is true (default: 1)
    SEARCH_ENABLED: Enable search requests (default: false)
    SEARCH_RATIO: Fraction of iterations that include a search request (default: 0.0)
    BROADCAST_ENABLED: Enable periodic broadcast notifications (default: false)
    BROADCAST_INTERVAL: Seconds between broadcasts (default: 30.0)
    BROADCAST_SERIALIZABLE: Use SERIALIZABLE isolation for broadcasts (default: false)
    POLL_ENABLED: Enable notification long-polling (default: false)
    POLL_RATIO: Fraction of users that long-poll (default: 0.0)
    UNREAD_CHECK_RATIO: Fraction of iterations checking unread count (default: 0.0)
    MARK_READ_RATIO: Fraction of iterations marking all read (default: 0.0)
    LIST_NOTIFS_RATIO: Fraction of iterations listing notifications (default: 0.0)
    MULTI_USER_COUNT: Number of users to create at startup (default: 1)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("loadgen")

APP_URL = os.environ.get("APP_URL", "http://app:8000")
NUM_USERS = int(os.environ.get("NUM_USERS", "3"))
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "2.0"))
STREAM_RATIO = float(os.environ.get("STREAM_RATIO", "0.3"))
RAMP_UP_SECONDS = float(os.environ.get("RAMP_UP_SECONDS", "10"))
READ_RATIO = float(os.environ.get("READ_RATIO", "0.3"))
BURST_MODE = os.environ.get("BURST_MODE", "false").lower() in ("true", "1", "yes")
BURST_CONCURRENCY = int(os.environ.get("BURST_CONCURRENCY", "1"))
SEARCH_ENABLED = os.environ.get("SEARCH_ENABLED", "false").lower() in ("true", "1", "yes")
SEARCH_RATIO = float(os.environ.get("SEARCH_RATIO", "0.0"))
BROADCAST_ENABLED = os.environ.get("BROADCAST_ENABLED", "false").lower() in ("true", "1", "yes")
BROADCAST_INTERVAL = float(os.environ.get("BROADCAST_INTERVAL", "30.0"))
BROADCAST_SERIALIZABLE = os.environ.get("BROADCAST_SERIALIZABLE", "false").lower() in ("true", "1", "yes")
POLL_ENABLED = os.environ.get("POLL_ENABLED", "false").lower() in ("true", "1", "yes")
POLL_RATIO = float(os.environ.get("POLL_RATIO", "0.0"))
UNREAD_CHECK_RATIO = float(os.environ.get("UNREAD_CHECK_RATIO", "0.0"))
MARK_READ_RATIO = float(os.environ.get("MARK_READ_RATIO", "0.0"))
LIST_NOTIFS_RATIO = float(os.environ.get("LIST_NOTIFS_RATIO", "0.0"))
MULTI_USER_COUNT = int(os.environ.get("MULTI_USER_COUNT", "1"))

SEARCH_TERMS = [
    "quantum", "database", "PostgreSQL", "connection", "deadlock",
    "ACID", "consistency", "Raft", "consensus", "microservices",
    "observer", "threads", "garbage", "race", "capital",
    "connection pool", "CAP theorem", "garbage collection",
    "kubernetes", "terraform", "monitoring",
]

SAMPLE_MESSAGES = [
    "What is the capital of France?",
    "Explain quantum computing in simple terms.",
    "Write a haiku about databases.",
    "How do connection pools work in PostgreSQL?",
    "What are the ACID properties?",
    "Describe the difference between SQL and NoSQL.",
    "What is a deadlock and how can it be prevented?",
    "Explain the CAP theorem.",
    "What is eventual consistency?",
    "How does Raft consensus work?",
    "What are the benefits of microservices?",
    "Explain the observer pattern.",
    "What is the difference between threads and processes?",
    "How does garbage collection work?",
    "What is a race condition?",
]


async def wait_for_app(client: httpx.AsyncClient) -> None:
    """Wait for the app to become healthy."""
    for attempt in range(60):
        try:
            resp = await client.get(f"{APP_URL}/health")
            if resp.status_code == 200:
                log.info("App is healthy")
                return
        except httpx.ConnectError:
            pass
        log.info(f"Waiting for app... (attempt {attempt + 1})")
        await asyncio.sleep(2)
    raise RuntimeError("App did not become healthy in time")


async def simulate_user(
    user_id: int, client: httpx.AsyncClient, assigned_user_ids: list[str] | None = None
) -> None:
    """Simulate a single user sending messages."""
    assigned_user_ids = assigned_user_ids or []
    # Stagger start times during ramp-up
    delay = (user_id / max(NUM_USERS, 1)) * RAMP_UP_SECONDS
    await asyncio.sleep(delay)

    log.info(f"User {user_id} starting")

    conversation_id = None

    while True:
        try:
            # Create a new conversation periodically
            if conversation_id is None or random.random() < 0.1:
                resp = await client.post(
                    f"{APP_URL}/api/conversations",
                    json={"title": f"User {user_id} conversation"},
                    timeout=30,
                )
                if resp.status_code == 200:
                    conversation_id = resp.json()["id"]
                    log.info(f"User {user_id}: created conversation {conversation_id[:8]}")
                else:
                    log.warning(f"User {user_id}: failed to create conversation: {resp.status_code}")
                    await asyncio.sleep(REQUEST_DELAY)
                    continue

            # Send a message (streaming or regular)
            message = random.choice(SAMPLE_MESSAGES)
            token_count = len(message.split()) * 2  # rough token estimate

            if random.random() < STREAM_RATIO:
                # Streaming request (holds transaction open)
                start = time.monotonic()
                resp = await client.post(
                    f"{APP_URL}/api/conversations/{conversation_id}/stream",
                    json={"content": message, "token_count": token_count},
                    timeout=60,
                )
                elapsed = time.monotonic() - start
                if resp.status_code == 200:
                    log.info(f"User {user_id}: stream response {elapsed:.1f}s")
                else:
                    log.warning(f"User {user_id}: stream failed {resp.status_code} ({elapsed:.1f}s)")
            else:
                # Regular message
                start = time.monotonic()
                resp = await client.post(
                    f"{APP_URL}/api/conversations/{conversation_id}/messages",
                    json={
                        "content": message,
                        "role": "user",
                        "token_count": token_count,
                    },
                    timeout=30,
                )
                elapsed = time.monotonic() - start
                if resp.status_code == 200:
                    log.info(f"User {user_id}: message sent {elapsed:.1f}s")
                else:
                    log.warning(f"User {user_id}: message failed {resp.status_code} ({elapsed:.1f}s)")

            # Also occasionally list conversations or read messages
            if random.random() < 0.2:
                await client.get(f"{APP_URL}/api/conversations", timeout=10)

            if random.random() < READ_RATIO and conversation_id:
                await client.get(
                    f"{APP_URL}/api/conversations/{conversation_id}/messages",
                    timeout=10,
                )

            if SEARCH_ENABLED and random.random() < SEARCH_RATIO:
                term = random.choice(SEARCH_TERMS)
                try:
                    resp = await client.get(
                        f"{APP_URL}/api/conversations/search",
                        params={"q": term},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        results = resp.json()
                        log.info(f"User {user_id}: search '{term}' returned {len(results)} results")
                    else:
                        log.warning(f"User {user_id}: search failed {resp.status_code}")
                except httpx.TimeoutException:
                    log.warning(f"User {user_id}: search timed out for '{term}'")

            # Notification interactions
            notif_user_id = assigned_user_ids[user_id % len(assigned_user_ids)] if assigned_user_ids else None

            if notif_user_id and random.random() < UNREAD_CHECK_RATIO:
                try:
                    resp = await client.get(
                        f"{APP_URL}/api/notifications/unread-count",
                        params={"user_id": notif_user_id},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        log.info(f"User {user_id}: unread count = {resp.json().get('unread_count', '?')}")
                except httpx.TimeoutException:
                    log.warning(f"User {user_id}: unread-count timed out")

            if notif_user_id and random.random() < MARK_READ_RATIO:
                try:
                    resp = await client.post(
                        f"{APP_URL}/api/notifications/mark-read",
                        params={"user_id": notif_user_id},
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        log.info(f"User {user_id}: marked read = {resp.json().get('marked_read', '?')}")
                except httpx.TimeoutException:
                    log.warning(f"User {user_id}: mark-read timed out")

            if notif_user_id and random.random() < LIST_NOTIFS_RATIO:
                try:
                    start = time.monotonic()
                    resp = await client.get(
                        f"{APP_URL}/api/notifications",
                        params={"user_id": notif_user_id},
                        timeout=60,
                    )
                    elapsed = time.monotonic() - start
                    if resp.status_code == 200:
                        notifs = resp.json()
                        log.info(f"User {user_id}: listed {len(notifs)} notifications ({elapsed:.1f}s)")
                    else:
                        log.warning(f"User {user_id}: list-notifications failed {resp.status_code}")
                except httpx.TimeoutException:
                    log.warning(f"User {user_id}: list-notifications timed out")

        except httpx.TimeoutException:
            log.warning(f"User {user_id}: request timed out")
        except httpx.ConnectError:
            log.warning(f"User {user_id}: connection error, retrying...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            log.error(f"User {user_id}: unexpected error: {e}")

        # Wait between requests
        jitter = random.uniform(0.5, 1.5)
        await asyncio.sleep(REQUEST_DELAY * jitter)


async def simulate_user_burst(user_id: int, client: httpx.AsyncClient) -> None:
    """Simulate a user firing concurrent writes to the same conversation.

    Each burst creates one conversation then fires BURST_CONCURRENCY
    parallel add_message requests, triggering read-modify-write races
    on the token counter.
    """
    delay = (user_id / max(NUM_USERS, 1)) * RAMP_UP_SECONDS
    await asyncio.sleep(delay)

    log.info(f"Burst user {user_id} starting (concurrency={BURST_CONCURRENCY})")

    while True:
        try:
            # Create a fresh conversation for each burst
            resp = await client.post(
                f"{APP_URL}/api/conversations",
                json={"title": f"Burst user {user_id} conversation"},
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning(f"Burst user {user_id}: failed to create conversation: {resp.status_code}")
                await asyncio.sleep(REQUEST_DELAY)
                continue

            conversation_id = resp.json()["id"]

            # Fire concurrent writes
            async def _send_one(idx: int) -> None:
                message = random.choice(SAMPLE_MESSAGES)
                token_count = len(message.split()) * 2
                start = time.monotonic()
                r = await client.post(
                    f"{APP_URL}/api/conversations/{conversation_id}/messages",
                    json={
                        "content": message,
                        "role": "user",
                        "token_count": token_count,
                    },
                    timeout=30,
                )
                elapsed = time.monotonic() - start
                if r.status_code == 200:
                    log.info(f"Burst user {user_id}[{idx}]: sent {elapsed:.1f}s")
                else:
                    log.warning(f"Burst user {user_id}[{idx}]: failed {r.status_code} ({elapsed:.1f}s)")

            await asyncio.gather(*[_send_one(i) for i in range(BURST_CONCURRENCY)])

        except httpx.TimeoutException:
            log.warning(f"Burst user {user_id}: request timed out")
        except httpx.ConnectError:
            log.warning(f"Burst user {user_id}: connection error, retrying...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            log.error(f"Burst user {user_id}: unexpected error: {e}")

        jitter = random.uniform(0.5, 1.5)
        await asyncio.sleep(REQUEST_DELAY * jitter)


async def broadcast_loop(client: httpx.AsyncClient) -> None:
    """Periodically broadcast notifications to all users."""
    endpoint = (
        f"{APP_URL}/api/notifications/broadcast-serializable"
        if BROADCAST_SERIALIZABLE
        else f"{APP_URL}/api/notifications/broadcast"
    )
    while True:
        try:
            start = time.monotonic()
            resp = await client.post(
                endpoint,
                json={"type": "system", "payload": {"message": f"broadcast at {time.time():.0f}"}},
                timeout=120,
            )
            elapsed = time.monotonic() - start
            if resp.status_code == 200:
                data = resp.json()
                log.info(f"Broadcast sent to {data.get('recipients', '?')} users ({elapsed:.1f}s)")
            else:
                log.warning(f"Broadcast failed: {resp.status_code} ({elapsed:.1f}s)")
        except httpx.TimeoutException:
            log.warning("Broadcast timed out")
        except Exception as e:
            log.error(f"Broadcast error: {e}")
        await asyncio.sleep(BROADCAST_INTERVAL)


async def poll_loop(
    poller_id: int, client: httpx.AsyncClient, user_id: str
) -> None:
    """Long-poll for notifications for a specific user."""
    since = None
    while True:
        try:
            params: dict[str, str] = {"user_id": user_id}
            if since:
                params["since"] = since
            start = time.monotonic()
            resp = await client.get(
                f"{APP_URL}/api/notifications/poll",
                params=params,
                timeout=60,
            )
            elapsed = time.monotonic() - start
            if resp.status_code == 200:
                notifs = resp.json()
                if notifs:
                    log.info(f"Poller {poller_id}: got {len(notifs)} notifications ({elapsed:.1f}s)")
                    # Update since to latest notification timestamp
                    since = notifs[-1].get("created_at", since)
                else:
                    log.info(f"Poller {poller_id}: poll timeout, no new notifications ({elapsed:.1f}s)")
            else:
                log.warning(f"Poller {poller_id}: poll failed {resp.status_code}")
        except httpx.TimeoutException:
            log.info(f"Poller {poller_id}: poll HTTP timeout")
        except httpx.ConnectError:
            log.warning(f"Poller {poller_id}: connection error, retrying...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Poller {poller_id}: error: {e}")
        await asyncio.sleep(0.5)


async def setup_multi_users(client: httpx.AsyncClient) -> list[str]:
    """Create multiple users for notification testing. Returns list of user IDs."""
    if MULTI_USER_COUNT <= 1:
        return []
    log.info(f"Creating {MULTI_USER_COUNT} users for multi-user mode...")
    user_ids = []
    for i in range(MULTI_USER_COUNT):
        uid = f"00000000-0000-4000-9000-{i:012d}"
        try:
            # Use a direct POST to create user via the app's default user endpoint
            # Actually, users are created via ensure_notification_users in the app
            # We just need to know the IDs — the preseed handles creation
            user_ids.append(uid)
        except Exception as e:
            log.warning(f"Failed to register user {i}: {e}")
    log.info(f"Registered {len(user_ids)} user IDs")
    return user_ids


async def main() -> None:
    mode = "burst" if BURST_MODE else "normal"
    search_info = f", search={SEARCH_RATIO:.0%}" if SEARCH_ENABLED else ""
    broadcast_info = f", broadcast every {BROADCAST_INTERVAL}s" if BROADCAST_ENABLED else ""
    poll_info = f", poll_ratio={POLL_RATIO}" if POLL_ENABLED else ""
    notif_info = ""
    if LIST_NOTIFS_RATIO > 0:
        notif_info += f", list_notifs={LIST_NOTIFS_RATIO}"
    if MARK_READ_RATIO > 0:
        notif_info += f", mark_read={MARK_READ_RATIO}"
    if UNREAD_CHECK_RATIO > 0:
        notif_info += f", unread_check={UNREAD_CHECK_RATIO}"
    multi_info = f", multi_user={MULTI_USER_COUNT}" if MULTI_USER_COUNT > 1 else ""
    log.info(
        f"Load generator starting: {NUM_USERS} users, {REQUEST_DELAY}s delay, "
        f"{STREAM_RATIO:.0%} streaming, read_ratio={READ_RATIO}, mode={mode}"
        f"{search_info}{broadcast_info}{poll_info}{notif_info}{multi_info}"
    )

    async with httpx.AsyncClient() as client:
        await wait_for_app(client)

        # Setup multi-user IDs
        assigned_user_ids = await setup_multi_users(client)

        tasks: list[asyncio.Task] = []

        # Choose user simulation function based on mode
        if BURST_MODE:
            for i in range(NUM_USERS):
                tasks.append(asyncio.create_task(simulate_user_burst(i, client)))
        else:
            for i in range(NUM_USERS):
                tasks.append(asyncio.create_task(
                    simulate_user(i, client, assigned_user_ids=assigned_user_ids)
                ))

        # Broadcast coroutine
        if BROADCAST_ENABLED:
            tasks.append(asyncio.create_task(broadcast_loop(client)))

        # Poll coroutines
        if POLL_ENABLED and assigned_user_ids:
            num_pollers = max(1, int(len(assigned_user_ids) * POLL_RATIO))
            for i in range(num_pollers):
                uid = assigned_user_ids[i % len(assigned_user_ids)]
                tasks.append(asyncio.create_task(poll_loop(i, client, uid)))

        # Run until cancelled
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("Load generator shutting down")


if __name__ == "__main__":
    asyncio.run(main())
