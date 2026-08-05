"""
CUIN v2 Control Plane - FastAPI Application Entry Point

This is the main entry point for the CUIN v2 backend API.
It configures the FastAPI application with all routes, middleware,
and WebSocket support.
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api import db_init
from api.config import settings
from api.routes_admin import router as admin_router
from api.routes_candidates import router as candidates_router
from api.routes_graph import router as graph_router
from api.routes_matches import router as matches_router
from api.routes_metrics import router as metrics_router
from api.routes_review import router as review_router
from api.routes_runs import router as runs_router

# Import the module-level singleton, not the class -- api/routes_datasource.py
# and api/routes_runs.py import this SAME `ws_manager` from api.ws_events to
# broadcast progress. A previous version of this file instantiated its own
# `ConnectionManager()` here, so the /ws endpoint below registered clients on
# one instance while every broadcast went to a different, client-less one --
# live pipeline progress silently never reached the browser (API polling
# happened to mask it). Do not re-introduce a second instance.
from api.ws_events import ws_manager
from services.run_service import get_run_service

# Configure logging
# Setup logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("cuin-api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    logger.info("🚀 CUIN v2 Control Plane starting up...")
    logger.info(f"   Version: {settings.APP_VERSION}")
    logger.info(f"   Debug mode: {settings.DEBUG}")
    logger.info(f"   API URL: http://{settings.API_HOST}:{settings.API_PORT}")
    
    # Initialize Database
    db_init.init_db()
    
    # Hook up RunService to WebSockets
    run_service = get_run_service()
    
    async def ws_progress_callback(run_id: str, progress):
        await ws_manager.broadcast_stage_progress(
            run_id=run_id,
            stage=progress.stage.value,
            status=progress.status,
            message=progress.message,
            records_in=progress.records_in,
            records_out=progress.records_out,
            reduction_pct=progress.reduction_pct,
            duration_ms=progress.duration_ms,
            data=progress.data
        )
    
    run_service.set_progress_callback(ws_progress_callback)
    logger.info("✅ RunService hooked up to WebSockets with enhanced payload support")
    
    yield
    
    # Shutdown
    logger.info("👋 CUIN v2 Control Plane shutting down...")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
## CUIN v2 - Identity Intelligence Platform

Production-grade identity resolution for banking with:
- **Deterministic rules** for safety
- **Probabilistic scoring** for scale  
- **Bounded AI agents** for assisted judgment

### Key Features
- Multi-pass blocking with explainability
- Deterministic rule-based confidence scoring
- Three-tier decision engine (Auto-Link / Review / Reject)
- Maker-checker review workflow
- Tamper-evident audit trail
- Postgres-backed identity graph visualization

### API Sections
- `/runs` - Manage ER pipeline runs
- `/candidates` - View candidate pairs
- `/matches` - Scores and decisions
- `/review` - Human review queue
- `/audit` - Audit trail and compliance
- `/metrics` - KPIs and statistics
- `/graph` - Identity graph and clusters
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Configure TrustedHost
from fastapi.middleware.trustedhost import TrustedHostMiddleware
app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["*"]
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Health Check Endpoints
# ============================================

@app.get("/health", tags=["Health"])
async def health_check() -> dict:
    """
    Basic health check endpoint.
    Returns service status and version.
    """
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/health/ready", tags=["Health"])
async def readiness_check() -> dict:
    """
    Readiness check - verifies all dependencies are available.
    Used by Kubernetes/container orchestrators.
    """
    # Check Postgres (via SQLAlchemy or raw connection)
    db_ready = False
    try:
        # Simple TCP check or import connection logic if available
        # For now, let's assume if we can import and connect it's good
        # We don't have a global db session exposed clearly here yet, so let's do a quick connect
        import psycopg2
        conn = psycopg2.connect(settings.DATABASE_URL)
        conn.close()
        db_ready = True
    except Exception as e:
        logger.warning(f"Database Health Check Failed: {e}")
        db_ready = False

    checks = {
        "api": True,
        "database": db_ready,
    }
    
    all_ready = all(checks.values())
    
    return JSONResponse(
        status_code=200 if all_ready else 503,
        content={
            "ready": all_ready,
            "checks": checks,
            "timestamp": datetime.utcnow().isoformat(),
        }
    )


@app.get("/health/live", tags=["Health"])
async def liveness_check() -> dict:
    """
    Liveness check - basic check that the service is running.
    Used by Kubernetes/container orchestrators.
    """
    return {"alive": True}


# ============================================
# Include Routers
# ============================================

app.include_router(runs_router, prefix="/runs", tags=["Runs"])
app.include_router(candidates_router, prefix="/candidates", tags=["Candidates"])
app.include_router(matches_router, prefix="/matches", tags=["Matches"])
app.include_router(review_router, prefix="/review", tags=["Review"])
app.include_router(metrics_router, prefix="/metrics", tags=["Metrics"])
app.include_router(graph_router, prefix="/graph", tags=["Graph"])

from api.routes_admin import router as admin_router
app.include_router(admin_router, prefix="/admin", tags=["Admin"])

from api.routes_datasource import router as datasource_router
app.include_router(datasource_router, prefix="/datasource", tags=["Datasource"])

from api.routes_rules import router as rules_router
app.include_router(rules_router, prefix="/rules", tags=["Rules"])

from api.routes_search import router as search_router
app.include_router(search_router, prefix="/search", tags=["Search"])

from api.routes_schema import router as schema_router
app.include_router(schema_router, prefix="/datasource/schema", tags=["Schema"])

# Raw source data viewer -- lets a bank officer browse the raw,
# un-normalized source Parquet file directly, before running any
# pipeline. See engine/ports/doris_raw_preview.py's module docstring.
from api.routes_data_viewer import router as data_viewer_router
app.include_router(data_viewer_router, prefix="/data-viewer", tags=["Data Viewer"])

# Entity resolution workbench (Stage 4 of the plan) -- purely additive,
# does not touch/redirect any /graph, /explorer, /matches, or legacy
# /review endpoint. See api/routes_workbench.py's module docstring.
from api.routes_workbench import router as workbench_router
app.include_router(workbench_router, prefix="/workbench", tags=["Workbench"])

# Identity Graph 360 v2 -- purely additive, mounted at /graph/v2 (never
# collides with the legacy /graph/* router below it, which stays
# untouched for /explorer, /pipeline, and /runs/[id]). See
# api/routes_graph_v2.py's module docstring.
from api.routes_graph_v2 import router as graph_v2_router
app.include_router(graph_v2_router, prefix="/graph/v2", tags=["Graph V2"])

# Public, bank-facing Identity Recognition API -- a SEPARATE
# sub-application (own Swagger UI at /api/v1/docs, own OpenAPI schema),
# not another include_router() on this app. A bank integration partner
# should only ever see this one contract, never the ~20 internal
# admin/workbench routers above. See api/public_api.py.
from api.public_api import create_public_api
app.mount("/api/v1", create_public_api())


# ============================================
# WebSocket Endpoint
# ============================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time pipeline updates.
    
    Events emitted:
    - STAGE_PROGRESS: Pipeline stage completion
    - RUN_COMPLETE: Run finished
    - REVIEW_ITEM: New review item created
    """
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; actual events are pushed from pipeline
            data = await websocket.receive_text()
            # Echo for ping/pong or handle client commands
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info("WebSocket client disconnected")


# ============================================
# Root Endpoint
# ============================================

@app.get("/", tags=["Root"])
async def root() -> dict:
    """
    Root endpoint - API information.
    """
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
    )
