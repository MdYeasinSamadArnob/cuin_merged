"""
CUIN v2 - Public API sub-application

A separate FastAPI app, mounted at /api/v1 on the main app (see
api/main.py), so its Swagger UI (/api/v1/docs) and OpenAPI schema
(/api/v1/openapi.json) show ONLY the bank-facing Identity Recognition
API -- never the ~20 internal admin/workbench routers the main app's
own /docs exposes. This is the one contract an outside bank
integration partner should ever see.

No lifespan/startup hooks needed here -- every route opens its own
Postgres/run-session connections per-request (see
routes_public_identity_api.py), so there's nothing to initialize at
sub-app boot.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes_public_identity_api import router as identity_router

PUBLIC_API_DESCRIPTION = """
Real-time identity screening for banking entity resolution.

Before opening a new account, submit the applicant's details (name +
at least one of mobile/email/document number/date of birth) to
`POST /identity/screen`. The response tells you whether this person
already exists in the resolved identity graph, whether they already
carry a confirmed Global ID, and -- when relevant -- ranked candidate
matches with field-by-field explainability, computed with the exact
same rules and thresholds the bank's own reviewers use.

### Authentication
Every endpoint except `/health` requires an `Authorization: Bearer <token>`
header. Click **Authorize** above and paste the token shown on the CUIN
control plane's API page (or fetched via `GET /admin/api-token` on the
internal admin API). It's a single global token for the whole API --
set `PUBLIC_API_BEARER_TOKEN` in the backend's `.env` to pin it to a
fixed value; otherwise a new one is generated every process restart.

### Versioning
This is `v1`, mounted at `/api/v1`. Breaking changes will ship as
`/api/v2` alongside it, never as a silent contract change here.
"""


def create_public_api() -> FastAPI:
    app = FastAPI(
        title="CUIN Identity Recognition API",
        version="1.0.0",
        description=PUBLIC_API_DESCRIPTION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    # Independent CORS policy from the main app -- can be tightened to
    # specific bank integration domains later without touching the
    # internal control plane's own (deliberately permissive, LAN-only)
    # CORS config.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    app.include_router(identity_router)
    return app
