"""
CUIN v2 - Engine-agnostic run session

Resolves a completed Doris-backed run to a (connection, dialect) pair
for api/routes_rules.py's precheck/redecide/reblock and
api/routes_search.py's search.

Two access modes:

- `open_run_readonly(run_id)`: a connection directly to the run's own
  `cuin_run_{id}` database -- nothing else writes to a completed run's
  database, so this is read-only in practice even though Doris
  connections have no read_only flag to enforce it.
- `open_run_scratch(run_id, table_names)`: an ISOLATED copy of the
  requested tables that redecide/reblock can freely mutate with zero
  risk to the real run -- a throwaway `cuin_scratch_{uuid}` database
  populated via `CREATE TABLE ... AS SELECT * FROM cuin_run_{id}.
  {table}` -- verified live that Doris supports cross-database
  qualified table references within one connection/session -- dropped
  on `.close()`.

Doris-only: the earlier DuckDB-backed run path (`data/runs/
{run_id}.duckdb`, `DuckDbDialect()`) was removed once the DuckDB
pipeline engine was -- there was no legacy `.duckdb` run data left on
disk to preserve read access to at the time (see the commit that
wiped backend/data/runs/ to empty), so this was a clean break, not a
compat shim.
"""

import uuid
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException

from engine.ports.dialect import SqlDialect


def get_run_engine(run_id: str) -> str:
    from services.run_service import get_run_service
    run = get_run_service().get_run(run_id)
    if run and run.engine:
        return run.engine
    return "doris"


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
    return _open_doris_readonly(run_id)


def open_run_scratch(run_id: str, table_names: tuple) -> RunSession:
    return _open_doris_scratch(run_id, table_names)
