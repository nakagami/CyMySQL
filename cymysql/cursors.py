# -*- coding: utf-8 -*-
from collections.abc import Sequence
import re
import sys
from typing import TYPE_CHECKING, Any
import weakref

from cymysql.err import (
    Warning, Error, InterfaceError, DataError,
    DatabaseError, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ProgrammingError
)

if TYPE_CHECKING:
    from cymysql.connections import Connection

RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\b.+\bVALUES?\s*)"
    r"(\(\s*(?:%s|%\([^)]+\)s)\s*(?:,\s*(?:%s|%\([^)]+\)s)\s*)*\))"
    r"(\s*(?:AS\s+(?:`[^`]+`|\"[^\"]+\"|[0-9A-Za-z_]+)\s*(?:\(\s*(?:`[^`]+`|\"[^\"]+\"|[0-9A-Za-z_]+)\s*(?:,\s*(?:`[^`]+`|\"[^\"]+\"|[0-9A-Za-z_]+)\s*)*\))?\s*)?(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)


class Cursor(object):
    '''
    This is the object you use to interact with the database.
    '''
    def __init__(self, connection: 'Connection') -> None:
        '''
        Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        '''
        self.connection: Connection | None = connection
        self.arraysize: int = 1
        self._executed: str | None = None
        self.messages: list[tuple[Any, ...]] = []
        self._result: Any = None
        self._rowcount: int | None = None
        self.max_stmt_length: int = 1024 * 1024

    def __enter__(self) -> 'Cursor':
        return self

    def __iter__(self) -> Any:
        return iter(self.fetchone, None)

    def __exit__(self, exc: Any, value: Any, traceback: Any) -> None:
        self.close()

    def errorhandler(self, errorclass: type[Exception], errorvalue: Any) -> None:
        if self.connection:
            self.connection.errorhandler(self, errorclass, errorvalue)
        else:
            raise errorclass(errorvalue)

    @property
    def rowcount(self) -> int:
        if self._result and self._result.affected_rows is not None:
            return self._result.affected_rows
        if self._rowcount is not None:
            return self._rowcount
        return -1

    @property
    def description(self) -> list[tuple[Any, ...]] | tuple[tuple[Any, ...], ...] | None:
        return self._result.description if self._result else None

    @property
    def lastrowid(self) -> int | None:
        return self._result.insert_id if self._result else None

    def close(self) -> None:
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        if not self.connection:
            return
        try:
            while self.nextset():
                pass
        except:
            pass

        self.connection = None

    def _get_db(self) -> 'Connection':
        if not self.connection:
            self.errorhandler(ProgrammingError, (-1, "cursor closed"))
        return self.connection

    def _check_executed(self) -> None:
        if not self._executed:
            self.errorhandler(ProgrammingError, (-1, "execute() first"))

    def _flush(self) -> None:
        if self._result:
            self._result.read_rest_rowdata_packet()

    def setinputsizes(self, *args: Any) -> None:
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args: Any) -> None:
        """Does nothing, required by DB API."""

    def nextset(self) -> bool | None:
        ''' Get the next query set '''
        if self._executed:
            self.fetchall()
        del self.messages[:]

        if not self._result or not self._result.has_next:
            return None
        connection = self._get_db()
        connection.next_result()
        self._do_get_result()
        return True

    def _escape_args(self, args: Any, conn: Any) -> Any:
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return dict((key, conn.escape(val)) for (key, val) in args.items())
        else:
            return conn.escape(args)

    def execute(self, query: str | bytes, args: Sequence[Any] | dict[str, Any] | Any | None = None) -> None:
        ''' Execute a query '''
        self._rowcount = None

        conn = self._get_db()
        if hasattr(conn, '_last_execute_cursor') and not conn._last_execute_cursor() is None:
            conn._last_execute_cursor()._flush()

        encoding = conn.encoding
        del self.messages[:]

        if not isinstance(query, str):
            query = query.decode(encoding)

        if args is not None:
            query = query % self._escape_args(args, conn)

        try:
            self._query(query)
        except:
            exc, value, tb = sys.exc_info()
            del tb
            self.messages.append((exc, value))
            self.errorhandler(exc, value)

        self._executed = query
        conn._last_execute_cursor = weakref.ref(self)

    def _do_execute_many(
        self, prefix: str, values: str, postfix: str, args: Sequence[Any], max_stmt_length: int
    ) -> int:
        conn = self._get_db()
        sql = [prefix]
        cur_len = len(prefix) + len(postfix)
        rowcount = 0
        first = True

        for arg in args:
            escaped_val = values % self._escape_args(arg, conn)
            val_len = len(escaped_val) + (0 if first else 1)
            if not first and cur_len + val_len > max_stmt_length:
                self.execute("".join(sql) + postfix)
                if self.rowcount != -1:
                    rowcount += self.rowcount
                sql = [prefix, escaped_val]
                cur_len = len(prefix) + len(postfix) + len(escaped_val)
            else:
                if not first:
                    sql.append(",")
                sql.append(escaped_val)
                cur_len += val_len
                first = False

        if not first:
            self.execute("".join(sql) + postfix)
            if self.rowcount != -1:
                rowcount += self.rowcount

        self._result = None
        self._rowcount = rowcount
        return rowcount

    def executemany(self, query: str | bytes, args: Sequence[Sequence[Any] | dict[str, Any] | Any]) -> int:
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
            return self._do_execute_many(
                q_prefix, q_values, q_postfix, args, self.max_stmt_length
            )

        rowcount = 0
        for params in args:
            self.execute(query, params)
            if self.rowcount != -1:
                rowcount += self.rowcount
        self._result = None
        self._rowcount = rowcount
        return rowcount

    def callproc(self, procname: str, args: Sequence[Any] = ()) -> Sequence[Any]:
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
            if not isinstance(q, str):
                q = q.decode(conn.encoding)
            self._query(q)
            self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range(len(args))]))
        if not isinstance(q, str):
            q = q.decode(conn.encoding)
        self._query(q)
        self._executed = q

        return args

    def fetchone(self) -> tuple[Any, ...] | None:
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        return self._result.fetchone()

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]] | None:
        ''' Fetch several rows '''
        self._check_executed()
        size = size or self.arraysize
        if self._result is None:
            return None
        return self._result.fetchmany(size)

    def fetchall(self) -> list[tuple[Any, ...]] | None:
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        return self._result.fetchall()

    def _query(self, q: str) -> None:
        conn = self._get_db()
        self._last_executed = q
        conn.query(q)
        self._do_get_result()

    def _do_get_result(self) -> None:
        conn = self._get_db()
        self._result = conn._result

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError


class DictCursor(Cursor):
    """A cursor which returns results as a dictionary"""

    def execute(self, query: str | bytes, args: Sequence[Any] | dict[str, Any] | Any | None = None) -> Any:
        result = super(DictCursor, self).execute(query, args)
        if self.description:
            self._fields = [field[0] for field in self.description]
        return result

    def fetchone(self) -> dict[str, Any] | None:
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        r = super(DictCursor, self).fetchone()
        if not r:
            return None
        return dict(zip(self._fields, r))

    def fetchmany(self, size: int | None = None) -> tuple[dict[str, Any], ...] | None:
        ''' Fetch several rows '''
        self._check_executed()
        if self._result is None:
            return None
        result = [dict(zip(self._fields, r)) for r in super(DictCursor, self).fetchmany(size)]
        return tuple(result)

    def fetchall(self) -> tuple[dict[str, Any], ...] | None:
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        return tuple([
            dict(zip(self._fields, r)) for r in super(DictCursor, self).fetchall()
        ])

