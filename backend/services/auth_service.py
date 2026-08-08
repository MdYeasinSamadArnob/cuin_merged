"""
CUIN v2 - Auth service (password hashing, JWT, effective permissions)

Backs the internal admin app's login/RBAC system (migration
db/migrations/010_users_and_roles.sql, api/routes_auth.py,
api/deps_auth.py). Unrelated to the public bank-facing /api/v1
sub-app's own single-shared-bearer-token auth -- that mechanism has no
notion of a human identity and nothing here touches it.

All functions here are synchronous (bcrypt/PyJWT are both CPU-bound,
not I/O) except load_current_user, which does a real Postgres round
trip and is always called via run_in_threadpool by api/deps_auth.py --
never call it directly from an `async def` route.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt

from api.config import settings

logger = logging.getLogger(__name__)

JWT_ALGORITHM = "HS256"
JWT_EXPIRY = timedelta(hours=24)

# Matches the CHECK constraint on role_menu_permissions/
# user_menu_permission_overrides in migration 010, and
# frontend/src/components/organisms/layout/Sidebar.tsx's nav items 1:1.
ALL_MENU_KEYS = frozenset({
    "dashboard", "source_data", "ingestion_pipeline", "workbench",
    "graph", "api_docs", "settings", "role_management",
})


@dataclass
class CurrentUser:
    id: str
    email: str
    display_name: str
    role_id: str
    role_name: str
    is_superuser: bool
    menus: set = field(default_factory=set)

    def can_see(self, menu_key: str) -> bool:
        return self.is_superuser or menu_key in self.menus


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        # Malformed hash (shouldn't happen for a real row) -- fail closed, not raise.
        return False


def issue_token(user_id: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {"sub": user_id, "iat": now, "exp": now + JWT_EXPIRY}
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """Raises jwt.PyJWTError (expired, malformed, bad signature) -- callers convert to 401."""
    return jwt.decode(token, settings.SECRET_KEY, algorithms=[JWT_ALGORITHM])


def load_current_user(pg_conn, user_id: str) -> Optional[CurrentUser]:
    """
    Loads the user's identity plus their EFFECTIVE menu permission set,
    fresh from Postgres -- not cached in the JWT, so a superuser
    revoking a role's or a specific user's access takes effect on that
    user's very next request, not just at token expiry.

    Effective permissions = role's default grants, with any row in
    user_menu_permission_overrides for this user overriding that one
    menu_key in either direction. A superuser's is_superuser flag
    bypasses this entirely (see CurrentUser.can_see) -- ALL_MENU_KEYS
    is still populated here so a caller inspecting `.menus` directly
    (e.g. an admin UI listing "this user's menus") sees the true full
    set, not an empty one that only becomes correct via a separate
    is_superuser check.
    """
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT u.id, u.email, u.display_name, u.role_id, r.name, r.is_superuser, u.is_active
        FROM users u JOIN roles r ON r.id = u.role_id
        WHERE u.id = %s
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    uid, email, display_name, role_id, role_name, is_superuser, is_active = row
    if not is_active:
        return None

    if is_superuser:
        menus = set(ALL_MENU_KEYS)
    else:
        cur.execute("SELECT menu_key FROM role_menu_permissions WHERE role_id = %s", (role_id,))
        menus = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT menu_key, granted FROM user_menu_permission_overrides WHERE user_id = %s", (uid,))
        for menu_key, granted in cur.fetchall():
            if granted:
                menus.add(menu_key)
            else:
                menus.discard(menu_key)

    return CurrentUser(
        id=str(uid), email=email, display_name=display_name,
        role_id=str(role_id), role_name=role_name, is_superuser=is_superuser, menus=menus,
    )


def authenticate(pg_conn, email: str, password: str) -> Optional[CurrentUser]:
    """Verifies credentials, returns the CurrentUser on success (None on bad email/password/inactive)."""
    cur = pg_conn.cursor()
    cur.execute("SELECT id, password_hash FROM users WHERE email = %s AND is_active", (email,))
    row = cur.fetchone()
    if not row:
        return None
    user_id, password_hash = row
    if not verify_password(password, password_hash):
        return None
    cur.execute("UPDATE users SET last_login_at = NOW() WHERE id = %s", (user_id,))
    pg_conn.commit()
    return load_current_user(pg_conn, str(user_id))
