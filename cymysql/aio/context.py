# based on aiomysql
# https://github.com/aio-libs/aiomysql/blob/master/aiomysql/utils.py

from collections.abc import Coroutine
from typing import Any


class _ContextManager(Coroutine):

    __slots__ = ('_coro', '_obj')

    def __init__(self, coro: Any) -> None:
        self._coro = coro
        self._obj: Any = None

    def send(self, value: Any) -> Any:
        return self._coro.send(value)

    def throw(self, typ: Any, val: Any = None, tb: Any = None) -> Any:
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        return self._coro.close()

    @property
    def gi_frame(self) -> Any:
        return self._coro.gi_frame

    @property
    def gi_running(self) -> Any:
        return self._coro.gi_running

    @property
    def gi_code(self) -> Any:
        return self._coro.gi_code

    def __next__(self) -> Any:
        return self.send(None)

    def __iter__(self) -> Any:
        return self._coro.__await__()

    def __await__(self) -> Any:
        return self._coro.__await__()

    async def __aenter__(self) -> Any:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):

    __slots__ = ('_coro', '_conn', '_pool')

    def __init__(self, coro: Any, pool: Any) -> None:
        self._coro = coro
        self._conn: Any = None
        self._pool: Any = pool

    async def __aenter__(self) -> Any:
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type: Any, exc, tb) -> None:
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _PoolConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from pool) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool: Any, conn: Any) -> None:
        self._pool: Any = pool
        self._conn: Any = conn

    def __enter__(self) -> Any:
        assert self._conn
        return self._conn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    async def __aenter__(self) -> Any:
        assert not self._conn
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

