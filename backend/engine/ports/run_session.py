"""
CUIN v2 - Engine-agnostic run session

Resolves a completed run to a (connection, dialect) pair regardless of
which engine actually ran it, so api/routes_rules.py's precheck/
redecide/reblock and api/routes_search.py's search work identically
whether the run was DuckDB- or Doris-backed.

Before this module, both routes hardcoded `data/runs/{run_id}.duckdb`
and `DuckDbDialect()` directly -- so EVERY Doris-backed run 404'd on
these endpoints with "No persisted evidence database for run {id} --
... or used a non-DuckDB engine", the exact bug a Doris demo run hits
on the Settings UI's precheck/reblock/redecide and on search.

Two access modes, mirroring what routes_rules.py already had for
DuckDB:

- `open_run_readonly(run_id)`: a connection directly to the run's
  persisted tables, for read-only use (search). DuckDB:
  `duckdb.connect(path, read_only=True)`. Doris: connect to the run's
  own `cuin_run_{id}` database -- nothing else writes to a completed
  run's database, so this is read-only in practice even though Doris
  connections have no read_only flag to enforce it.
- `open_run_scratch(run_id, table_names)`: an ISOLATED copy of the
  requested tables that redecide/reblock can freely mutate with zero
  risk to the real run. DuckDB: in-memory DB + ATTACH-and-copy (the
  original routes_rules.py pattern, unchanged). Doris: a throwaway
  `cuin_scratch_{uuid}` database populated via `CREATE TABLE ... AS
  SELECT * FROM cuin_run_{id}.{table}` -- verified live that Doris
  supports cross-database qualified table references within one
  connection/session -- dropped on `.close()`.

A run's engine is read from services.run_service's `Run.engine` field
(added alongside this module); runs created before that field existed
default to "duckdb" and are resolved by the pre-existing file-presence
check, so old runs keep working exactly as before.
"""

import os
import uuid
from dataclasses import dataclass
from typing import Optional

import duckdb
from fastapi import HTTPException

from engine.ports.dialect import SqlDialect
from engine.ports.duckdb_dialect import DuckDbDialect


def get_run_engine(run_id: str) -> str:
    from services.run_service import get_run_service
    run = get_run_service().get_run(run_id)
    if run and run.engine:
        return run.engine
    return "duckdb"


@dataclass
class RunSession:
    """
    `con`: a duckdb.DuckDBPyConnection or engine.ports.doris_conn.DorisConnection
    -- both expose the same .execute(sql, params).fetchall()/.fetchone() surface,
    so callers never need to branch on `engine` to issue SQL.
    """
    con: object
    dialect: SqlDialect
    engine: str
    run_id: str
    _scratch_db: Optional[str] = None

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass
        if self._scratch_db:
            try:
                admin = _doris_admin_connect()
                with admin.cursor() as cur:
                    cur.execute(f"DROP DATABASE IF EXISTS {self._scratch_db}")
                admin.close()
            except Exception:
                pass


# ----------------------------------------------------------------------
# DuckDB
# ----------------------------------------------------------------------

def _duckdb_run_path(run_id: str) -> str:
    return f"data/runs/{run_id}.duckdb"


def _open_duckdb_readonly(run_id: str) -> RunSession:
    path = _duckdb_run_path(run_id)
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"No persisted evidence database for run {run_id} -- the run predates the "
                   "persistent-evidence upgrade or is still executing.",
        )
    try:
        con = duckdb.connect(path, read_only=True)
    except duckdb.IOException as e:
        raise HTTPException(
            status_code=409,
            detail=f"Run database for {run_id} is locked (the run may still be executing): {e}",
        )
    return RunSession(con=con, dialect=DuckDbDialect(), engine="duckdb", run_id=run_id)


def _open_duckdb_scratch(run_id: str, table_names: tuple) -> RunSession:
    path = _duckdb_run_path(run_id)
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"No persisted evidence database for run {run_id} -- the run predates the "
                   "persistent-evidence upgrade or is still executing.",
        )
    scratch = duckdb.connect(":memory:")
    scratch.execute("SET preserve_insertion_order = true")
    try:
        scratch.execute(f"ATTACH '{path}' AS src (READ_ONLY)")
        found_any = False
        # NOTE: the loop variable here must NOT be named `tables` --
        # DuckDB's Python API auto-scans the calling frame for a local
        # variable whose name matches an unresolved SQL identifier
        # ("replacement scans") and will try to bind THAT instead of
        # the real table, raising InvalidInputException. Existence is
        # checked by attempting the copy and catching CatalogException,
        # not information_schema -- `src.information_schema.tables` is
        # not valid cross-catalog syntax in DuckDB. Both findings
        # verified live originally wiring this into routes_rules.py.
        for one_table in table_names:
            try:
                scratch.execute(f"CREATE TABLE {one_table} AS SELECT * FROM src.{one_table}")
                found_any = True
            except duckdb.CatalogException:
                continue
        scratch.execute("DETACH src")
    except duckdb.IOException as e:
        scratch.close()
        raise HTTPException(
            status_code=409,
            detail=f"Run database for {run_id} is locked (the run may still be executing): {e}",
        )
    if not found_any:
        scratch.close()
        raise HTTPException(
            status_code=409,
            detail=f"Run {run_id} has none of the required tables persisted -- cannot preview.",
        )
    return RunSession(con=scratch, dialect=DuckDbDialect(), engine="duckdb", run_id=run_id)


# ----------------------------------------------------------------------
# Doris
# ----------------------------------------------------------------------

def _doris_admin_connect():
    import pymysql
    from api.config import settings
    return pymysql.connect(
        host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
        user=settings.DORIS_USER, password=settings.DORIS_PASSWORD,
        autocommit=True, connect_timeout=10,
    )


def doris_run_db_name(run_id: str) -> str:
    from pipeline.doris_orchestrator import _sanitize_db_name
    return _sanitize_db_name(run_id)


def doris_database_exists(db_name: str) -> bool:
    try:
        admin = _doris_admin_connect()
        try:
            with admin.cursor() as cur:
                cur.execute("SHOW DATABASES")
                return db_name in {row[0] for row in cur.fetchall()}
        finally:
            admin.close()
    except Exception:
        return False


def _open_doris_readonly(run_id: str) -> RunSession:
    from engine.ports.doris_conn import DorisConnection
    from engine.ports.doris_dialect import DorisDialect
    from api.config import settings

    db_name = doris_run_db_name(run_id)
    if not doris_database_exists(db_name):
        raise HTTPException(
            status_code=404,
            detail=f"No persisted Doris database for run {run_id} -- the run predates this "
                   "engine's evidence persistence or is still executing.",
        )
    con = DorisConnection(
        host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
        user=settings.DORIS_USER, password=settings.DORIS_PASSWORD, database=db_name,
    )
    return RunSession(con=con, dialect=DorisDialect(), engine="doris", run_id=run_id)


def _open_doris_scratch(run_id: str, table_names: tuple) -> RunSession:
    from engine.ports.doris_conn import DorisConnection
    from engine.ports.doris_dialect import DorisDialect
    from api.config import settings

    src_db = doris_run_db_name(run_id)
    if not doris_database_exists(src_db):
        raise HTTPException(
            status_code=404,
            detail=f"No persisted Doris database for run {run_id} -- the run predates this "
                   "engine's evidence persistence or is still executing.",
        )

    scratch_db = f"cuin_scratch_{uuid.uuid4().hex[:12]}"
    admin = _doris_admin_connect()
    found_any = False
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE {scratch_db}")
            for one_table in table_names:
                try:
                    cur.execute(f"CREATE TABLE {scratch_db}.{one_table} AS SELECT * FROM {src_db}.{one_table}")
                    found_any = True
                except Exception:
                    continue
    finally:
        admin.close()

    if not found_any:
        try:
            admin2 = _doris_admin_connect()
            with admin2.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {scratch_db}")
            admin2.close()
        except Exception:
            pass
        raise HTTPException(
            status_code=409,
            detail=f"Run {run_id} has none of the required tables persisted -- cannot preview.",
        )

    con = DorisConnection(
        host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
        user=settings.DORIS_USER, password=settings.DORIS_PASSWORD, database=scratch_db,
    )
    return RunSession(con=con, dialect=DorisDialect(), engine="doris", run_id=run_id, _scratch_db=scratch_db)


# ----------------------------------------------------------------------
# Public entry points
# ----------------------------------------------------------------------

def open_run_readonly(run_id: str) -> RunSession:
    if get_run_engine(run_id) == "doris":
        return _open_doris_readonly(run_id)
    return _open_duckdb_readonly(run_id)


def open_run_scratch(run_id: str, table_names: tuple) -> RunSession:
    if get_run_engine(run_id) == "doris":
        return _open_doris_scratch(run_id, table_names)
    return _open_duckdb_scratch(run_id, table_names)
