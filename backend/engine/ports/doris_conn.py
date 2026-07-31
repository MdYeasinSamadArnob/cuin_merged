"""
CUIN v2 - Doris Connection Adapter

Wraps a pymysql connection behind the same chainable
`.execute(sql).fetchall()` / `.execute(sql).fetchone()` surface
DuckDB's Python API exposes -- this is what lets
engine.rules.compiler, engine.rules.precheck, and every dialect-driven
normalize/suppression/evidence module run UNCHANGED against either
engine. Nothing in those modules imports duckdb or pymysql directly;
they only call `con.execute(...)`.

Only the small subset of the DuckDB connection API those modules
actually use is implemented: execute (with optional `?`-style
positional params, translated to pymysql's `%s`), fetchall, fetchone,
executemany, close.
"""

import pymysql


class DorisCursorResult:
    """Returned by DorisConnection.execute() so `.fetchall()`/`.fetchone()` chain like DuckDB's API."""

    def __init__(self, cursor):
        self._cursor = cursor

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()


class DorisConnection:
    def __init__(self, host: str, port: int, user: str, password: str = "", database: str = None):
        self._conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, autocommit=True, connect_timeout=10,
        )
        self._cursor = self._conn.cursor()

    def execute(self, sql: str, params=None) -> DorisCursorResult:
        # DuckDB-style '?' positional placeholders -> pymysql's '%s'.
        # Every caller in engine.rules.* that binds params uses '?'
        # (written against DuckDB originally); this keeps that code
        # dialect-agnostic rather than special-casing Doris at each
        # call site.
        if params is not None:
            self._cursor.execute(sql.replace("?", "%s"), params)
        else:
            self._cursor.execute(sql)
        return DorisCursorResult(self._cursor)

    def executemany(self, sql: str, seq_of_params) -> None:
        self._cursor.executemany(sql.replace("?", "%s"), seq_of_params)

    def close(self) -> None:
        try:
            self._cursor.close()
        finally:
            self._conn.close()


def connect(host: str, port: int, user: str, password: str = "", database: str = None) -> DorisConnection:
    return DorisConnection(host=host, port=port, user=user, password=password, database=database)
