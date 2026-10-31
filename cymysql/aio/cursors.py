from collections.abc import Sequence
import sys
from typing import Any
import weakref

from ..cursors import Cursor, RE_INSERT_VALUES


class AsyncCursor(Cursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def __aenter__(self) -> 'AsyncCursor':
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        ret = await self.fetchone()
        if ret is not None:
            return ret
        else:
            raise StopAsyncIteration  # noqa

    async def __aexit__(self, exec: Any, value: Any, traceback: Any) -> None:
        await self.close()

    async def close(self) -> None:
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        if not self.connection:
            return
        try:
            while await self.nextset():
                pass
        except:
            pass

    async def _flush(self) -> None:
        if self._result:
            await self._result.read_rest_rowdata_packet()

    async def nextset(self) -> bool | None:
        ''' Get the next query set '''
        if self._executed:
            await self.fetchall()
        del self.messages[:]

        if not self._result or not self._result.has_next:
            return None
        connection = self._get_db()
        await connection.next_result()
        self._do_get_result()
        return True

    async def execute(self, query: str | bytes, args: Sequence[Any] | dict[str, Any] | Any | None = None) -> None:
        ''' Execute a query '''
        self._rowcount = None

        conn = self._get_db()
        last_cursor = getattr(conn, '_last_execute_cursor', None)
        if last_cursor is not None:
            c = last_cursor()
            if c is not None:
                await c._flush()

        if self.messages:
            self.messages.clear()

        if args is not None:
            query = query % self._escape_args(args, conn)

        try:
            await self._query(query)
        except:
            exc, value, tb = sys.exc_info()
            del tb
            self.messages.append((exc, value))
            self.errorhandler(exc, value)

        self._executed = query
        conn._last_execute_cursor = weakref.ref(self)

    async def _do_execute_many(
        self, prefix: str, values: str, postfix: str, args: Sequence[Any], max_stmt_length: int
    ) -> int:
        conn = self._get_db()
        escape = conn.escape
        batch_values: list[str] = []
        cur_len = len(prefix) + len(postfix)
        rowcount = 0

        for arg in args:
            if isinstance(arg, (tuple, list)):
                escaped_val = values % tuple(map(escape, arg))
            elif isinstance(arg, dict):
                escaped_val = values % {k: escape(v) for k, v in arg.items()}
            else:
                escaped_val = values % escape(arg)

            val_len = len(escaped_val) + (1 if batch_values else 0)
            if batch_values and cur_len + val_len > max_stmt_length:
                await self.execute(prefix + ",".join(batch_values) + postfix)
                if self.rowcount != -1:
                    rowcount += self.rowcount
                batch_values = [escaped_val]
                cur_len = len(prefix) + len(postfix) + len(escaped_val)
            else:
                batch_values.append(escaped_val)
                cur_len += val_len

        if batch_values:
            await self.execute(prefix + ",".join(batch_values) + postfix)
            if self.rowcount != -1:
                rowcount += self.rowcount

        self._result = None
        self._rowcount = rowcount
        return rowcount

    async def executemany(self, query: str | bytes, args: Sequence[Sequence[Any] | dict[str, Any] | Any]) -> int:
        ''' Run several data against one query '''
        del self.messages[:]

        if not args:
            self._rowcount = 0
            return 0

        conn = self._get_db()
        q_str = query if isinstance(query, str) else query.decode(conn.encoding)
        m = RE_INSERT_VALUES.match(q_str)
        if m:
            try:
                q_prefix = m.group(1) % ()
            except (TypeError, ValueError):
                q_prefix = m.group(1)
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ""
            return await self._do_execute_many(
                q_prefix, q_values, q_postfix, args, self.max_stmt_length
            )

        rowcount = 0
        for params in args:
            await self.execute(query, params)
            if self.rowcount != -1:
                rowcount += self.rowcount
        self._result = None
        self._rowcount = rowcount
        return rowcount

    async def callproc(self, procname: str, args: Sequence[Any] = ()) -> Sequence[Any]:
        """Execute stored procedure procname with args

        procname -- string, name of procedure to execute on server

        args -- Sequence of parameters to use with procedure

        Returns the original args.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.
        """
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            self._query(q)
            self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range(len(args))]))
        self._query(q)
        self._executed = q

        return args

    async def fetchone(self) -> tuple[Any, ...] | None:
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        return await self._result.fetchone()

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]] | None:
        ''' Fetch several rows '''
        self._check_executed()
        size = size or self.arraysize
        if self._result is None:
            return None
        return await self._result.fetchmany(size)

    async def fetchall(self) -> list[tuple[Any, ...]] | None:
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        return await self._result.fetchall()

    async def _query(self, q: str) -> None:
        conn = self._get_db()
        self._last_executed = q
        await conn.query(q)
        self._do_get_result()


class AsyncDictCursor(AsyncCursor):
    """A cursor which returns results as a dictionary"""

    async def execute(self, query: str | bytes, args: Sequence[Any] | dict[str, Any] | Any | None = None) -> Any:
        result = await super().execute(query, args)
        if self.description:
            self._fields = [field[0] for field in self.description]
        return result

    async def fetchone(self) -> dict[str, Any] | None:
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        r = await super().fetchone()
        if not r:
            return None
        return dict(zip(self._fields, r))

    async def fetchmany(self, size: int | None = None) -> tuple[dict[str, Any], ...] | None:
        ''' Fetch several rows '''
        self._check_executed()
        if self._result is None:
            return None
        result = [dict(zip(self._fields, r)) for r in await super().fetchmany(size)]
        return tuple(result)

    async def fetchall(self) -> tuple[dict[str, Any], ...] | None:
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        return tuple([
            dict(zip(self._fields, r)) for r in await super().fetchall()
        ])

