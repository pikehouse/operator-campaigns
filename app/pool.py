"""
Connection pool setup for the chat application.
"""

import asyncpg


async def create_pool(dsn: str) -> asyncpg.Pool:
    """Create an asyncpg connection pool."""
    pool = await asyncpg.create_pool(
        dsn,
        min_size=2,
    )
    return pool
