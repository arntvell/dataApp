from contextlib import asynccontextmanager
import secrets
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn
from datetime import datetime
import logging
import os
import base64
from api.endpoints import router as api_router
from api.dashboard import router as dashboard_router
from api.budget import router as budget_router
from api.stock import router as stock_router
from config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _run_migrations():
    """Apply schema changes and data fixes that create_all won't handle on existing tables."""
    from database.config import engine
    from sqlalchemy import text
    from data.category_groups import CATEGORY_GROUPS

    with engine.begin() as conn:
        # Column additions
        for sql in [
            "ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cancel_reason VARCHAR",
            "ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ",
            "ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS note TEXT",
            "ALTER TABLE sales_refunds ADD COLUMN IF NOT EXISTS note TEXT",
        ]:
            conn.execute(text(sql))

        # Backfill category_group for any rows where it is NULL.
        # Default to standard_category, then apply group overrides.
        conn.execute(text("""
            UPDATE category_mappings
            SET category_group = standard_category
            WHERE category_group IS NULL AND standard_category IS NOT NULL
        """))
        for std_cat, group in CATEGORY_GROUPS.items():
            conn.execute(text("""
                UPDATE category_mappings
                SET category_group = :group
                WHERE standard_category = :std_cat
                  AND category_group IS DISTINCT FROM :group
            """), {"group": group, "std_cat": std_cat})

        # Backfill total_discount for Sitoo orders where it was previously hardcoded to 0.
        # moneydiscount is stored ex-VAT per unit in sales_order_items.discount_amount;
        # multiply by 1.25 to produce an inc-VAT figure consistent with total_amount.
        conn.execute(text("""
            UPDATE sales_orders so
            SET total_discount = subq.order_discount
            FROM (
                SELECT order_id,
                       SUM(discount_amount * quantity) * 1.25 AS order_discount
                FROM sales_order_items
                GROUP BY order_id
            ) subq
            WHERE so.id = subq.order_id
              AND so.source_system = 'sitoo'
              AND so.total_discount = 0
              AND subq.order_discount > 0
        """))

    logger.info("Migrations applied")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    _run_migrations()
    if settings.SCHEDULER_ENABLED:
        from scheduler import start_scheduler
        start_scheduler()
        logger.info("Scheduler started")
    yield
    # Shutdown
    if settings.SCHEDULER_ENABLED:
        from scheduler import stop_scheduler
        stop_scheduler()
        logger.info("Scheduler stopped")


app = FastAPI(
    title="DataApp - Unified Business Intelligence Platform",
    description="Integration platform for Sitoo POS, Shopify, SameSystem, and Cin7 Core",
    version="2.0.0",
    lifespan=lifespan,
)


# ---- HTTP Basic Auth middleware ----
# Set APP_USERNAME and APP_PASSWORD env vars to enable.
# The /health endpoint is excluded so Railway/uptime monitors work.

APP_USERNAME = os.getenv("APP_USERNAME", "livid")
APP_PASSWORD = os.getenv("APP_PASSWORD", "")


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip auth for health check
        if request.url.path == "/health":
            return await call_next(request)

        auth = request.headers.get("Authorization")
        if auth and auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                username, password = decoded.split(":", 1)
                if (secrets.compare_digest(username, APP_USERNAME)
                        and secrets.compare_digest(password, APP_PASSWORD)):
                    return await call_next(request)
            except Exception:
                pass

        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="DataApp"'},
            content="Unauthorized",
        )


if APP_PASSWORD:
    app.add_middleware(BasicAuthMiddleware)
    logger.info("Basic auth enabled (set APP_PASSWORD to disable)")


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix="/api/v1")
app.include_router(dashboard_router, prefix="/api/v1")
app.include_router(budget_router, prefix="/api/v1")
app.include_router(stock_router, prefix="/api/v1")

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def root():
    return {
        "message": "DataApp API is running",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "dashboard": "/dashboard"
    }

@app.get("/dashboard")
async def serve_dashboard():
    """Serve the sales dashboard"""
    dashboard_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    return {"error": "Dashboard not found"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/v1/scheduler/status")
async def scheduler_status():
    """Get scheduler job status"""
    from scheduler import get_scheduler_status
    return get_scheduler_status()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
