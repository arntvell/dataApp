"""
Budget & worktime synchronization pipeline for SameSystem data.
Syncs budgets (sales + salary) and worktime/salary exports into raw schema.
"""

import logging
from typing import Dict, Any
from datetime import datetime, date, timedelta
from sqlalchemy import and_
from database.config import SessionLocal
from database.models import SameSystemBudget, SameSystemWorktime, SyncStatus
from connectors.samesystem_connector import SameSystemConnector

logger = logging.getLogger(__name__)


class BudgetSyncPipeline:
    """Sync SameSystem budgets and worktime data"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connector = SameSystemConnector(config.get("samesystem", {}))

    def _get_sync_status(self, db, source: str) -> SyncStatus:
        status = db.query(SyncStatus).filter(SyncStatus.source_system == source).first()
        if not status:
            status = SyncStatus(source_system=source)
            db.add(status)
            db.commit()
            db.refresh(status)
        return status

    def _update_sync_status(self, db, source: str, **kwargs):
        status = self._get_sync_status(db, source)
        for key, value in kwargs.items():
            if hasattr(status, key):
                setattr(status, key, value)
        db.commit()

    def sync_budgets(self, year: int = None, month: int = None):
        """
        Sync daily sales + salary budgets for all departments.
        Defaults to current year/month if not specified.
        """
        if year is None:
            year = datetime.now().year
        if month is None:
            month = datetime.now().month

        if not self.connector.authenticate():
            logger.error("SameSystem authentication failed")
            return

        db = SessionLocal()
        try:
            self._update_sync_status(db, "samesystem_budgets", sync_in_progress=True, last_error=None)
            total_upserted = 0

            for store_name, ctx_token in self.connector.departments.items():
                try:
                    # Fetch daily sales budgets
                    sales_budgets = self.connector.get_daily_sales_budgets(ctx_token, year, month)
                    for entry in sales_budgets:
                        total_upserted += self._upsert_budget(
                            db, store_name, entry, "sales", "daily"
                        )

                    # Fetch daily salary budgets
                    salary_budgets = self.connector.get_daily_salary_budgets(ctx_token, year, month)
                    for entry in salary_budgets:
                        total_upserted += self._upsert_budget(
                            db, store_name, entry, "salary", "daily"
                        )

                    logger.info(f"Budget sync for {store_name}: sales={len(sales_budgets)}, salary={len(salary_budgets)}")
                except Exception as e:
                    logger.error(f"Budget sync error for {store_name}: {e}")

            db.commit()
            self._update_sync_status(
                db, "samesystem_budgets",
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=total_upserted,
            )
            logger.info(f"Budget sync complete: {total_upserted} records upserted")

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, "samesystem_budgets", sync_in_progress=False, last_error=str(e))
            logger.error(f"Budget sync failed: {e}")
        finally:
            db.close()

    def sync_worktime(self, from_date: date = None, to_date: date = None):
        """
        Sync worktime/salary export data for all departments.
        Defaults to yesterday → today if not specified.
        """
        if from_date is None:
            from_date = date.today() - timedelta(days=1)
        if to_date is None:
            to_date = date.today()

        if not self.connector.authenticate():
            logger.error("SameSystem authentication failed")
            return

        db = SessionLocal()
        try:
            self._update_sync_status(db, "samesystem_worktime", sync_in_progress=True, last_error=None)
            total_upserted = 0

            for store_name, ctx_token in self.connector.departments.items():
                try:
                    # Fetch salary export (includes hours + cost)
                    salary_data = self.connector.get_salary_export(ctx_token, from_date, to_date)
                    for entry in salary_data:
                        total_upserted += self._upsert_worktime(db, store_name, entry)

                    # If salary export didn't include hours, fall back to worktime export
                    if not salary_data:
                        worktime_data = self.connector.get_worktime_export(ctx_token, from_date, to_date)
                        for entry in worktime_data:
                            total_upserted += self._upsert_worktime(db, store_name, entry)

                    logger.info(f"Worktime sync for {store_name}: {len(salary_data)} records")
                except Exception as e:
                    logger.error(f"Worktime sync error for {store_name}: {e}")

            db.commit()
            self._update_sync_status(
                db, "samesystem_worktime",
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=total_upserted,
            )
            logger.info(f"Worktime sync complete: {total_upserted} records upserted")

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, "samesystem_worktime", sync_in_progress=False, last_error=str(e))
            logger.error(f"Worktime sync failed: {e}")
        finally:
            db.close()

    # ---- Private helpers ----

    def _upsert_budget(self, db, store_name: str, entry: dict, budget_type: str, granularity: str) -> int:
        """Upsert a single budget record. Returns 1 if upserted, 0 on error."""
        try:
            entry_date = self._parse_date(entry.get("date") or entry.get("day"))
            if not entry_date:
                return 0

            amount = float(entry.get("amount", 0) or entry.get("value", 0) or 0)

            existing = db.query(SameSystemBudget).filter(
                and_(
                    SameSystemBudget.store == store_name,
                    SameSystemBudget.date == entry_date,
                    SameSystemBudget.budget_type == budget_type,
                    SameSystemBudget.granularity == granularity,
                )
            ).first()

            if existing:
                existing.amount = amount
            else:
                db.add(SameSystemBudget(
                    store=store_name,
                    date=entry_date,
                    budget_type=budget_type,
                    amount=amount,
                    granularity=granularity,
                ))
            return 1
        except Exception as e:
            logger.warning(f"Budget upsert error: {e}")
            return 0

    def _upsert_worktime(self, db, store_name: str, entry: dict) -> int:
        """Upsert a single worktime record. Returns 1 if upserted, 0 on error."""
        try:
            entry_date = self._parse_date(entry.get("date") or entry.get("day"))
            employee_id = str(entry.get("employee_id") or entry.get("employeeId") or entry.get("id", ""))
            if not entry_date or not employee_id:
                return 0

            hours = float(entry.get("hours_worked", 0) or entry.get("hours", 0) or 0)
            salary = float(entry.get("salary_cost", 0) or entry.get("salary", 0) or entry.get("cost", 0) or 0)

            existing = db.query(SameSystemWorktime).filter(
                and_(
                    SameSystemWorktime.store == store_name,
                    SameSystemWorktime.date == entry_date,
                    SameSystemWorktime.employee_id == employee_id,
                )
            ).first()

            if existing:
                existing.hours_worked = hours
                existing.salary_cost = salary
            else:
                db.add(SameSystemWorktime(
                    store=store_name,
                    employee_id=employee_id,
                    date=entry_date,
                    hours_worked=hours,
                    salary_cost=salary,
                ))
            return 1
        except Exception as e:
            logger.warning(f"Worktime upsert error: {e}")
            return 0

    @staticmethod
    def _parse_date(value) -> date:
        """Parse a date string or date object into a date"""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
