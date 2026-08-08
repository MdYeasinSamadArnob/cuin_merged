"""
CUIN v2 - Login for the internal admin app

POST /login and GET /me. No Depends(get_current_user) on this router
itself (obviously -- /login is how you GET a token; /me re-validates
one the frontend already has, e.g. on app load). Every OTHER router in
this backend requires a valid session -- see api/main.py's
include_router(..., dependencies=[Depends(get_current_user)]) calls.

Unrelated to the public bank-facing /api/v1 sub-app's own single-
shared-bearer-token auth (api/routes_public_identity_api.py) -- that
mechanism has no notion of a human identity and this router has
nothing to do with it.
"""

import logging

import psycopg2
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.config import settings
from api.deps_auth import get_current_user
from services import auth_service
from services.auth_service import CurrentUser

logger = logging.getLogger(__name__)

router = APIRouter()


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


class LoginRequest(BaseModel):
    email: str
    password: str


class UserProfile(BaseModel):
    id: str
    email: str
    display_name: str
    role_name: str
    is_superuser: bool
    menus: list[str]


class LoginResponse(BaseModel):
    access_token: str
    user: UserProfile


def _to_profile(user: CurrentUser) -> UserProfile:
    return UserProfile(
        id=user.id, email=user.email, display_name=user.display_name,
        role_name=user.role_name, is_superuser=user.is_superuser,
        menus=sorted(user.menus),
    )


def _sync_login(request: LoginRequest) -> LoginResponse:
    conn = _pg()
    try:
        user = auth_service.authenticate(conn, request.email, request.password)
    finally:
        conn.close()

    if not user:
        raise HTTPException(status_code=401, detail="Incorrect email or password.")

    return LoginResponse(access_token=auth_service.issue_token(user.id), user=_to_profile(user))


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    return await run_in_threadpool(_sync_login, request)


@router.get("/me", response_model=UserProfile)
async def me(current_user: CurrentUser = Depends(get_current_user)):
    """
    Re-validates a stored token and returns the user's CURRENT
    effective menus -- called on frontend app load so a permission
    change a superuser made since the last login takes effect without
    waiting for the JWT to expire.
    """
    return _to_profile(current_user)
