from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from datetime import datetime
import logging
import os
from api.endpoints import router as api_router
from api.dashboard import router as dashboard_router
from api.budget import router as budget_router
from api.stock import router as stock_router
from config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
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

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Frontend URL
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
