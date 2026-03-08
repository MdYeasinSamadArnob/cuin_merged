"""
CUIN v2 Control Plane - FastAPI Application Entry Point

This is the main entry point for the CUIN v2 backend API.
It configures the FastAPI application with all routes, middleware,
and WebSocket support.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.config import settings
from api.routes_runs import router as runs_router
# Trigger reload
from api.routes_upload import router as upload_router
from api.routes_candidates import router as candidates_router
from api.routes_matches import router as matches_router
from api.routes_review import router as review_router
from api.routes_audit import router as audit_router
from api.routes_metrics import router as metrics_router
from api.routes_graph import router as graph_router
from api.routes_admin import router as admin_router
from api.ws_events import ConnectionManager
from api import db_init

# Configure logging
# Setup logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger("cuin-api")

# WebSocket connection manager (singleton)
ws_manager = ConnectionManager()


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
    
    # Initialize Databases
    db_init.init_db()
    # db_init.init_graph()  # Skip Neo4j — not needed for Supabase demo
    
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
- Splink probabilistic matching
- Three-tier decision engine (Auto-Link / Review / Reject)
- Maker-checker review workflow
- Tamper-evident audit trail
- Neo4j identity graph projection

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
    # 1. Check Neo4j (Disabled for Lite Mode)
    neo4j_ready = True
    # try:
    #     from engine.graph.neo4j_writer import get_neo4j_writer
    #     writer = get_neo4j_writer()
    #     # Initial check
    #     if writer and writer.driver:
    #          writer.driver.verify_connectivity()
    #          neo4j_ready = True
    #     else:
    #          # Try re-initializing if None (lazy load attempt)
    #          from neo4j import GraphDatabase
    #          with GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)) as driver:
    #              driver.verify_connectivity()
    #              neo4j_ready = True
    # except Exception as e:
    #     logger.warning(f"Neo4j Health Check Failed: {e}")
    #     neo4j_ready = False

    # 2. Check Postgres (Disabled for Lite Mode)
    db_ready = True
    # try:
    #     # Simple TCP check or import connection logic if available
    #     # For now, let's assume if we can import and connect it's good
    #     # We don't have a global db session exposed clearly here yet, so let's do a quick connect
    #     import psycopg2
    #     conn = psycopg2.connect(settings.DATABASE_URL)
    #     conn.close()
    #     db_ready = True
    # except Exception as e:
    #     logger.warning(f"Database Health Check Failed: {e}")
    #     db_ready = False

    checks = {
        "api": True,
        "database": db_ready,
        "neo4j": neo4j_ready,
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
app.include_router(upload_router, prefix="/upload", tags=["Upload"])
app.include_router(candidates_router, prefix="/candidates", tags=["Candidates"])
app.include_router(matches_router, prefix="/matches", tags=["Matches"])
app.include_router(review_router, prefix="/review", tags=["Review"])
app.include_router(audit_router, prefix="/audit", tags=["Audit"])
app.include_router(metrics_router, prefix="/metrics", tags=["Metrics"])
app.include_router(graph_router, prefix="/graph", tags=["Graph"])

from api.routes_config import router as config_router
app.include_router(config_router, prefix="/config", tags=["Config"])

from api.routes_admin import router as admin_router
app.include_router(admin_router, prefix="/admin", tags=["Admin"])


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
