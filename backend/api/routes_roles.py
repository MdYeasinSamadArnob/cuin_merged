"""
CUIN v2 - Role Management (superuser-only)

CRUD for roles, users, and menu permissions. This whole router is
mounted with dependencies=[Depends(require_superuser)] at the
include_router() call in api/main.py -- every route here is
superuser-only regardless of a caller's own menu grants, independent
of the role_management menu_key check that also gates the frontend
page. See db/migrations/010_users_and_roles.sql for the schema.
"""

import logging
from typing import List, Optional

import psycopg2
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.config import settings
from services import auth_service
from services.auth_service import ALL_MENU_KEYS

logger = logging.getLogger(__name__)

router = APIRouter()


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


def _validate_menu_keys(menu_keys: List[str]) -> None:
    bad = set(menu_keys) - ALL_MENU_KEYS
    if bad:
        raise HTTPException(status_code=400, detail=f"Unknown menu key(s): {sorted(bad)}")


# ----------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------

class RoleOut(BaseModel):
    id: str
    name: str
    is_superuser: bool
    menu_keys: List[str]


class CreateRoleRequest(BaseModel):
    name: str
    menu_keys: List[str] = []


class UpdateRoleRequest(BaseModel):
    name: Optional[str] = None
    menu_keys: Optional[List[str]] = None  # full replace when provided


class UserOut(BaseModel):
    id: str
    email: str
    display_name: str
    role_id: str
    role_name: str
    is_active: bool
    last_login_at: Optional[str] = None
    menu_overrides: dict = {}


class CreateUserRequest(BaseModel):
    email: str
    password: str
    display_name: str
    role_id: str


class UpdateUserRequest(BaseModel):
    display_name: Optional[str] = None
    role_id: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None


class MenuOverrideRequest(BaseModel):
    menu_key: str
    granted: Optional[bool] = None  # None clears the override, reverting to the role's default


# ----------------------------------------------------------------------
# menu keys (fixed catalog, for the frontend to render checkboxes without
# hardcoding the list a second time)
# ----------------------------------------------------------------------

@router.get("/menu-keys")
async def list_menu_keys():
    return {"menu_keys": sorted(ALL_MENU_KEYS)}


# ----------------------------------------------------------------------
# Roles
# ----------------------------------------------------------------------

def _sync_list_roles():
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name, is_superuser FROM roles ORDER BY created_at")
        roles = cur.fetchall()
        out = []
        for role_id, name, is_superuser in roles:
            cur.execute("SELECT menu_key FROM role_menu_permissions WHERE role_id = %s", (role_id,))
            menu_keys = sorted(r[0] for r in cur.fetchall())
            out.append(RoleOut(id=str(role_id), name=name, is_superuser=is_superuser, menu_keys=menu_keys))
        return out
    finally:
        conn.close()


@router.get("", response_model=List[RoleOut])
async def list_roles():
    return await run_in_threadpool(_sync_list_roles)


def _sync_create_role(request: CreateRoleRequest) -> RoleOut:
    _validate_menu_keys(request.menu_keys)
    conn = _pg()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO roles (name, is_superuser) VALUES (%s, FALSE) RETURNING id",
                (request.name,),
            )
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=409, detail=f"A role named '{request.name}' already exists.")
        role_id = cur.fetchone()[0]
        for menu_key in request.menu_keys:
            cur.execute(
                "INSERT INTO role_menu_permissions (role_id, menu_key) VALUES (%s, %s)",
                (role_id, menu_key),
            )
        conn.commit()
        return RoleOut(id=str(role_id), name=request.name, is_superuser=False, menu_keys=sorted(request.menu_keys))
    finally:
        conn.close()


@router.post("", response_model=RoleOut)
async def create_role(request: CreateRoleRequest):
    return await run_in_threadpool(_sync_create_role, request)


def _sync_update_role(role_id: str, request: UpdateRoleRequest) -> RoleOut:
    if request.menu_keys is not None:
        _validate_menu_keys(request.menu_keys)
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name, is_superuser FROM roles WHERE id = %s", (role_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Role not found.")
        current_name, is_superuser = row

        if is_superuser and request.menu_keys is not None:
            # The Superuser role's is_superuser flag already bypasses the
            # menu system entirely (see CurrentUser.can_see) -- editing
            # its permission rows would be a no-op that looks like it did
            # something, so refuse it explicitly instead of silently
            # accepting a change with no effect.
            raise HTTPException(status_code=400, detail="The Superuser role's access can't be limited by menu permissions.")

        if request.name is not None and request.name != current_name:
            try:
                cur.execute("UPDATE roles SET name = %s, updated_at = NOW() WHERE id = %s", (request.name, role_id))
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                raise HTTPException(status_code=409, detail=f"A role named '{request.name}' already exists.")

        if request.menu_keys is not None:
            cur.execute("DELETE FROM role_menu_permissions WHERE role_id = %s", (role_id,))
            for menu_key in request.menu_keys:
                cur.execute(
                    "INSERT INTO role_menu_permissions (role_id, menu_key) VALUES (%s, %s)",
                    (role_id, menu_key),
                )

        conn.commit()

        cur.execute("SELECT name, is_superuser FROM roles WHERE id = %s", (role_id,))
        name, is_superuser = cur.fetchone()
        cur.execute("SELECT menu_key FROM role_menu_permissions WHERE role_id = %s", (role_id,))
        menu_keys = sorted(r[0] for r in cur.fetchall())
        return RoleOut(id=role_id, name=name, is_superuser=is_superuser, menu_keys=menu_keys)
    finally:
        conn.close()


@router.patch("/{role_id}", response_model=RoleOut)
async def update_role(role_id: str, request: UpdateRoleRequest):
    return await run_in_threadpool(_sync_update_role, role_id, request)


def _sync_delete_role(role_id: str):
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT is_superuser FROM roles WHERE id = %s", (role_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Role not found.")
        if row[0]:
            raise HTTPException(status_code=400, detail="The Superuser role can't be deleted.")

        cur.execute("SELECT COUNT(*) FROM users WHERE role_id = %s", (role_id,))
        n_users = cur.fetchone()[0]
        if n_users:
            raise HTTPException(
                status_code=409,
                detail=f"{n_users} user(s) still have this role -- reassign them to a different role first.",
            )

        cur.execute("DELETE FROM roles WHERE id = %s", (role_id,))
        conn.commit()
        return {"deleted": role_id}
    finally:
        conn.close()


@router.delete("/{role_id}")
async def delete_role(role_id: str):
    return await run_in_threadpool(_sync_delete_role, role_id)


# ----------------------------------------------------------------------
# Users
# ----------------------------------------------------------------------

def _sync_list_users() -> List[UserOut]:
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.id, u.email, u.display_name, u.role_id, r.name, u.is_active, u.last_login_at
            FROM users u JOIN roles r ON r.id = u.role_id
            ORDER BY u.created_at
            """
        )
        rows = cur.fetchall()
        out = []
        for uid, email, display_name, role_id, role_name, is_active, last_login_at in rows:
            cur.execute("SELECT menu_key, granted FROM user_menu_permission_overrides WHERE user_id = %s", (uid,))
            overrides = {k: g for k, g in cur.fetchall()}
            out.append(UserOut(
                id=str(uid), email=email, display_name=display_name, role_id=str(role_id),
                role_name=role_name, is_active=is_active,
                last_login_at=last_login_at.isoformat() if last_login_at else None,
                menu_overrides=overrides,
            ))
        return out
    finally:
        conn.close()


@router.get("/users", response_model=List[UserOut])
async def list_users():
    return await run_in_threadpool(_sync_list_users)


def _sync_create_user(request: CreateUserRequest) -> UserOut:
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM roles WHERE id = %s", (request.role_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Unknown role_id.")

        password_hash = auth_service.hash_password(request.password)
        try:
            cur.execute(
                """
                INSERT INTO users (email, password_hash, display_name, role_id, is_active)
                VALUES (%s, %s, %s, %s, TRUE) RETURNING id
                """,
                (request.email, password_hash, request.display_name, request.role_id),
            )
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=409, detail=f"A user with email '{request.email}' already exists.")
        user_id = cur.fetchone()[0]
        conn.commit()

        cur.execute("SELECT name FROM roles WHERE id = %s", (request.role_id,))
        role_name = cur.fetchone()[0]
        return UserOut(
            id=str(user_id), email=request.email, display_name=request.display_name,
            role_id=request.role_id, role_name=role_name, is_active=True,
        )
    finally:
        conn.close()


@router.post("/users", response_model=UserOut)
async def create_user(request: CreateUserRequest):
    return await run_in_threadpool(_sync_create_user, request)


def _sync_update_user(user_id: str, request: UpdateUserRequest) -> UserOut:
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT email, is_active FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found.")
        email, is_active = row

        if request.is_active is False:
            # Refuse to lock out the last active superuser -- otherwise
            # nobody could ever log back in to undo it.
            cur.execute("SELECT is_superuser FROM roles r JOIN users u ON u.role_id = r.id WHERE u.id = %s", (user_id,))
            this_user_is_superuser = cur.fetchone()[0]
            if this_user_is_superuser:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM users u JOIN roles r ON r.id = u.role_id
                    WHERE r.is_superuser AND u.is_active AND u.id != %s
                    """,
                    (user_id,),
                )
                remaining = cur.fetchone()[0]
                if remaining == 0:
                    raise HTTPException(status_code=400, detail="Can't deactivate the last active superuser.")

        if request.role_id is not None:
            cur.execute("SELECT 1 FROM roles WHERE id = %s", (request.role_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=400, detail="Unknown role_id.")
            cur.execute("UPDATE users SET role_id = %s, updated_at = NOW() WHERE id = %s", (request.role_id, user_id))
        if request.display_name is not None:
            cur.execute("UPDATE users SET display_name = %s, updated_at = NOW() WHERE id = %s", (request.display_name, user_id))
        if request.is_active is not None:
            cur.execute("UPDATE users SET is_active = %s, updated_at = NOW() WHERE id = %s", (request.is_active, user_id))
        if request.password is not None:
            cur.execute(
                "UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s",
                (auth_service.hash_password(request.password), user_id),
            )
        conn.commit()

        cur.execute(
            """
            SELECT u.email, u.display_name, u.role_id, r.name, u.is_active, u.last_login_at
            FROM users u JOIN roles r ON r.id = u.role_id WHERE u.id = %s
            """,
            (user_id,),
        )
        email, display_name, role_id, role_name, is_active, last_login_at = cur.fetchone()
        return UserOut(
            id=user_id, email=email, display_name=display_name, role_id=str(role_id),
            role_name=role_name, is_active=is_active,
            last_login_at=last_login_at.isoformat() if last_login_at else None,
        )
    finally:
        conn.close()


@router.patch("/users/{user_id}", response_model=UserOut)
async def update_user(user_id: str, request: UpdateUserRequest):
    return await run_in_threadpool(_sync_update_user, user_id, request)


def _sync_delete_user(user_id: str):
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT r.is_superuser FROM users u JOIN roles r ON r.id = u.role_id WHERE u.id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found.")
        if row[0]:
            cur.execute(
                """
                SELECT COUNT(*) FROM users u JOIN roles r ON r.id = u.role_id
                WHERE r.is_superuser AND u.id != %s
                """,
                (user_id,),
            )
            if cur.fetchone()[0] == 0:
                raise HTTPException(status_code=400, detail="Can't delete the last superuser.")
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()
        return {"deleted": user_id}
    finally:
        conn.close()


@router.delete("/users/{user_id}")
async def delete_user(user_id: str):
    return await run_in_threadpool(_sync_delete_user, user_id)


# ----------------------------------------------------------------------
# Per-user menu overrides
# ----------------------------------------------------------------------

def _sync_set_user_menu_override(user_id: str, request: MenuOverrideRequest):
    _validate_menu_keys([request.menu_key])
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found.")

        if request.granted is None:
            cur.execute(
                "DELETE FROM user_menu_permission_overrides WHERE user_id = %s AND menu_key = %s",
                (user_id, request.menu_key),
            )
        else:
            cur.execute(
                """
                INSERT INTO user_menu_permission_overrides (user_id, menu_key, granted)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, menu_key) DO UPDATE SET granted = EXCLUDED.granted
                """,
                (user_id, request.menu_key, request.granted),
            )
        conn.commit()
        return {"user_id": user_id, "menu_key": request.menu_key, "granted": request.granted}
    finally:
        conn.close()


@router.put("/users/{user_id}/menu-overrides")
async def set_user_menu_override(user_id: str, request: MenuOverrideRequest):
    return await run_in_threadpool(_sync_set_user_menu_override, user_id, request)
