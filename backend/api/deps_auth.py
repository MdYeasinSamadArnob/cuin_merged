"""
CUIN v2 - Auth/RBAC FastAPI dependencies

Mounted per-router in api/main.py (dependencies=[Depends(...)] on each
app.include_router(...) call), NOT inside the route files themselves --
keeps every existing router's own code untouched. get_current_user is
the baseline "must be logged in" gate; require_menu(key)/require_superuser
build on top of it for menu-scoped and superuser-only routers.

Every one of these does a real Postgres round trip via
services.auth_service.load_current_user, which is blocking (psycopg2).
get_current_user runs on EVERY authenticated request in the app, so it
is wrapped in run_in_threadpool exactly like every other blocking call
in backend/api/ (see this session's sweep, e.g. routes_matches.py) --
skipping that here would be the single worst-case instance of that bug,
since it's not one endpoint but the gate in front of nearly all of them.
"""

import logging
from typing import Optional

import psycopg2
from fastapi import Depends, Header, HTTPException
from starlette.concurrency import run_in_threadpool

from api.config import settings
from services import auth_service
from services.auth_service import CurrentUser

logger = logging.getLogger(__name__)


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


def _sync_load_user_from_token(token: str) -> CurrentUser:
    try:
        payload = auth_service.decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired session -- please log in again.")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid session token.")

    conn = _pg()
    try:
        user = auth_service.load_current_user(conn, user_id)
    finally:
        conn.close()

    if not user:
        raise HTTPException(status_code=401, detail="Account not found or deactivated.")
    return user


async def get_current_user(authorization: Optional[str] = Header(None)) -> CurrentUser:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token> header.")
    token = authorization.split(" ", 1)[1].strip()
    return await run_in_threadpool(_sync_load_user_from_token, token)


def require_menu(menu_key: str):
    """
    Dependency factory -- Depends(require_menu("workbench")) on a
    router. Builds on Depends(get_current_user), which FastAPI caches
    per-request by callable identity, so a route that also injects
    get_current_user directly (e.g. to read current_user.display_name
    for an audit actor) costs one Postgres round trip total, not two.
    """
    async def _check(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not current_user.can_see(menu_key):
            raise HTTPException(status_code=403, detail=f"Your role does not have access to '{menu_key}'.")
        return current_user
    return _check


async def require_superuser(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required.")
    return current_user
