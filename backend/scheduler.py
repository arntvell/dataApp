"""
APScheduler-based job scheduler for all sync pipelines.
Started/stopped via FastAPI lifespan events in main.py.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from config import settings

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="Europe/Oslo")


def _get_config() -> dict:
    return settings.get_connector_configs()


# ---- Job functions ----

def job_sales_incremental():
    """Incremental sync for Shopify + Sitoo sales (every 15 min)"""
    from pipelines.sales_sync import SalesSyncPipeline
    try:
        pipeline = SalesSyncPipeline(_get_config())
        pipeline.sync_incremental()
    except Exception as e:
        logger.error(f"Scheduled sales sync failed: {e}")


def job_samesystem_budgets():
    """Daily budget sync from SameSystem (06:00)"""
    from pipelines.budget_sync import BudgetSyncPipeline
    try:
        pipeline = BudgetSyncPipeline(_get_config())
        pipeline.sync_budgets()
    except Exception as e:
        logger.error(f"Scheduled budget sync failed: {e}")


def job_samesystem_worktime():
    """Daily worktime sync from SameSystem (06:30)"""
    from pipelines.budget_sync import BudgetSyncPipeline
    try:
        pipeline = BudgetSyncPipeline(_get_config())
        pipeline.sync_worktime()
    except Exception as e:
        logger.error(f"Scheduled worktime sync failed: {e}")


def job_cin7_stock():
    """Hourly stock level sync from Cin7"""
    from pipelines.stock_sync import StockSyncPipeline
    try:
        pipeline = StockSyncPipeline(_get_config())
        pipeline.sync_stock_levels()
    except Exception as e:
        logger.error(f"Scheduled stock sync failed: {e}")


def job_cin7_wholesale():
    """Wholesale order sync from Cin7 (every 4 hours)"""
    from pipelines.stock_sync import StockSyncPipeline
    try:
        pipeline = StockSyncPipeline(_get_config())
        pipeline.sync_wholesale_orders()
    except Exception as e:
        logger.error(f"Scheduled wholesale sync failed: {e}")


def job_cin7_purchases():
    """Purchase order sync from Cin7 (every 4 hours)"""
    from pipelines.stock_sync import StockSyncPipeline
    try:
        pipeline = StockSyncPipeline(_get_config())
        pipeline.sync_purchase_orders()
    except Exception as e:
        logger.error(f"Scheduled purchase sync failed: {e}")


# ---- Scheduler lifecycle ----

JOB_DEFAULTS = {
    "coalesce": True,
    "max_instances": 1,
    "misfire_grace_time": 300,
}


def start_scheduler():
    """Register all jobs and start the scheduler"""
    scheduler.configure(job_defaults=JOB_DEFAULTS)

    scheduler.add_job(
        job_sales_incremental,
        trigger=IntervalTrigger(minutes=15),
        id="sales_incremental",
        name="Sales incremental sync (Shopify+Sitoo)",
        replace_existing=True,
    )

    scheduler.add_job(
        job_samesystem_budgets,
        trigger=CronTrigger(hour=6, minute=0),
        id="samesystem_budgets",
        name="SameSystem daily budget sync",
        replace_existing=True,
    )

    scheduler.add_job(
        job_samesystem_worktime,
        trigger=CronTrigger(hour=6, minute=30),
        id="samesystem_worktime",
        name="SameSystem daily worktime sync",
        replace_existing=True,
    )

    scheduler.add_job(
        job_cin7_stock,
        trigger=IntervalTrigger(hours=1),
        id="cin7_stock",
        name="Cin7 stock level sync",
        replace_existing=True,
    )

    scheduler.add_job(
        job_cin7_wholesale,
        trigger=IntervalTrigger(hours=4),
        id="cin7_wholesale",
        name="Cin7 wholesale order sync",
        replace_existing=True,
    )

    scheduler.add_job(
        job_cin7_purchases,
        trigger=IntervalTrigger(hours=4),
        id="cin7_purchases",
        name="Cin7 purchase order sync",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))


def stop_scheduler():
    """Gracefully shutdown the scheduler"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def get_scheduler_status() -> dict:
    """Return status of all scheduled jobs"""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        })
    return {
        "running": scheduler.running,
        "job_count": len(jobs),
        "jobs": jobs,
    }
