-- ============================================
-- CUIN v2 - Migration 010: Users, Roles, and Menu Permissions
-- ============================================
-- Login + role-based access control for the INTERNAL admin/workbench
-- app (frontend/). Single organization, not multi-tenant -- no table
-- here or anywhere else gets a tenant_id, and no existing table's
-- rows get scoped/filtered by this migration. This only gates WHO can
-- reach WHICH page (a fixed catalog of "menu keys", one per Sidebar
-- item) and WHO gets credited as `actor` in the audit trail (see
-- backend/api/routes_workbench.py, which after this migration derives
-- `actor` from the authenticated session instead of trusting a
-- client-supplied string).
--
-- Deliberately separate from, and unrelated to, the public bank-facing
-- /api/v1 sub-app's own single-shared-bearer-token auth (migration 008)
-- -- that mechanism has no notion of a human identity and is untouched
-- by this migration.
--
-- Four tables:
-- - roles: named roles (Superuser/Developer/Analyst by default, plus
--   any custom-named role a superuser creates later via the Role
--   Management screen). is_superuser is a hard flag, not just "has
--   every menu permission" -- it also bypasses the menu-permission
--   system entirely and is the only thing that unlocks the
--   role_management menu itself and the /roles admin endpoints.
-- - role_menu_permissions: which menu_keys a role grants by default.
-- - user_menu_permission_overrides: optional per-user override layered
--   on top of the role's default (a row here always wins over the
--   role's grant for that one menu_key, whichever direction it goes).
-- - users: login credentials (bcrypt password_hash, never plaintext)
--   plus which role they're assigned.
-- ============================================

CREATE TABLE IF NOT EXISTS roles (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name           VARCHAR(64) NOT NULL UNIQUE,
    is_superuser   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fixed catalog, enforced here (not a separate table) since it changes
-- only when a new top-level Sidebar item is added -- a CHECK constraint
-- keeps a typo'd menu_key from silently granting nothing. Matches
-- frontend/src/components/organisms/layout/Sidebar.tsx's nav items 1:1.
CREATE TABLE IF NOT EXISTS role_menu_permissions (
    role_id   UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    menu_key  VARCHAR(32) NOT NULL CHECK (menu_key IN (
        'dashboard', 'source_data', 'ingestion_pipeline', 'workbench',
        'graph', 'api_docs', 'settings', 'role_management'
    )),
    PRIMARY KEY (role_id, menu_key)
);

CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    password_hash   VARCHAR(255) NOT NULL,
    display_name    VARCHAR(128) NOT NULL,
    role_id         UUID NOT NULL REFERENCES roles(id),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_role ON users(role_id);

-- `granted` is explicit (not just "row present = granted") so a
-- superuser can override a role's default EITHER direction -- give one
-- Analyst extra access to `settings` without upgrading their whole
-- role, or take `graph` away from one specific Developer without
-- touching the Developer role for everyone else.
CREATE TABLE IF NOT EXISTS user_menu_permission_overrides (
    user_id   UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    menu_key  VARCHAR(32) NOT NULL CHECK (menu_key IN (
        'dashboard', 'source_data', 'ingestion_pipeline', 'workbench',
        'graph', 'api_docs', 'settings', 'role_management'
    )),
    granted   BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, menu_key)
);

-- ------------------------------------------------------------
-- Seed: 3 default roles + their default menu grants, all editable
-- later via the Role Management screen -- these are starting points,
-- not fixed policy.
-- ------------------------------------------------------------

INSERT INTO roles (id, name, is_superuser)
SELECT 'a0000000-0000-0000-0000-000000000001', 'Superuser', TRUE
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE id = 'a0000000-0000-0000-0000-000000000001');

INSERT INTO roles (id, name, is_superuser)
SELECT 'a0000000-0000-0000-0000-000000000002', 'Developer', FALSE
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE id = 'a0000000-0000-0000-0000-000000000002');

INSERT INTO roles (id, name, is_superuser)
SELECT 'a0000000-0000-0000-0000-000000000003', 'Analyst', FALSE
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE id = 'a0000000-0000-0000-0000-000000000003');

-- Superuser's is_superuser=TRUE flag already bypasses the menu system
-- entirely (see backend/api/deps_auth.py) -- these rows are seeded
-- anyway so the Role Management UI has something concrete to show/
-- toggle for the Superuser row too, not a blank permission list.
INSERT INTO role_menu_permissions (role_id, menu_key)
SELECT 'a0000000-0000-0000-0000-000000000001', m
FROM unnest(ARRAY['dashboard','source_data','ingestion_pipeline','workbench','graph','api_docs','settings','role_management']) AS m
WHERE NOT EXISTS (SELECT 1 FROM role_menu_permissions WHERE role_id = 'a0000000-0000-0000-0000-000000000001' AND menu_key = m);

INSERT INTO role_menu_permissions (role_id, menu_key)
SELECT 'a0000000-0000-0000-0000-000000000002', m
FROM unnest(ARRAY['dashboard','source_data','ingestion_pipeline','workbench','graph','api_docs','settings']) AS m
WHERE NOT EXISTS (SELECT 1 FROM role_menu_permissions WHERE role_id = 'a0000000-0000-0000-0000-000000000002' AND menu_key = m);

INSERT INTO role_menu_permissions (role_id, menu_key)
SELECT 'a0000000-0000-0000-0000-000000000003', m
FROM unnest(ARRAY['dashboard','workbench','graph']) AS m
WHERE NOT EXISTS (SELECT 1 FROM role_menu_permissions WHERE role_id = 'a0000000-0000-0000-0000-000000000003' AND menu_key = m);

-- Seed the one default superuser account. Password is "123456" --
-- ONLY the bcrypt hash below, never the plaintext, is stored anywhere
-- in this codebase. Change this password after first login in a real
-- deployment; there is no forced-reset-on-first-login flow (out of
-- scope for this migration).
INSERT INTO users (id, email, password_hash, display_name, role_id, is_active)
SELECT
    'b0000000-0000-0000-0000-000000000001',
    'era@erainfotechbd.com',
    '$2b$12$Qoj7nrhMHXXV8eInZHIM1eJCk5oYfeOtENnny9LkTRpPishpvDVYC',
    'Era Admin',
    'a0000000-0000-0000-0000-000000000001',
    TRUE
WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 'b0000000-0000-0000-0000-000000000001');

COMMENT ON TABLE roles IS
    'Named roles for the internal admin app (Superuser/Developer/Analyst seeded, more creatable via the Role Management screen). is_superuser bypasses the menu-permission system entirely.';
COMMENT ON TABLE role_menu_permissions IS
    'Which Sidebar menu_keys a role grants by default. menu_key values are a fixed catalog (CHECK constraint), one per top-level Sidebar item.';
COMMENT ON TABLE user_menu_permission_overrides IS
    'Optional per-user grant/revoke layered on top of their role''s default -- a row here always wins over the role for that menu_key.';
COMMENT ON TABLE users IS
    'Login accounts for the internal admin app. password_hash is bcrypt; the plaintext password is never stored or logged anywhere.';
