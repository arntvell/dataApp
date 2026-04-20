"""
Budget & worktime synchronization pipeline for SameSystem data.
Syncs budgets (sales + salary) and worktime/salary exports into raw schema.
"""

import logging
from collections import defaultdict
from typing import Dict, Any, List
from datetime import datetime, date, timedelta
from sqlalchemy import and_
from database.config import SessionLocal
from database.models import SameSystemBudget, SameSystemWorktime, SyncStatus
from connectors.samesystem_connector import SameSystemConnector

logger = logging.getLogger(__name__)


# SameSystem /export/calendar returns company-wide data regardless of which
# department's ctx_token you authenticate with — it does NOT scope to a
# specific store. To split the data per store, we map each record's
# `department.nr` field to a store name. Mapping confirmed by store
# managers (Butikksjef) on 2026-04-07. dept.nr 5 is not used.
DEPT_NR_TO_STORE: Dict[str, str] = {
    "1": "Livid Trondheim",
    "2": "Livid Oslo",
    "3": "Livid Bergen",
    "4": "Past Oslo",
    "6": "Livid Stavanger",
}
UNASSIGNED_STORE = "unassigned"


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
        Default from_date resumes from last successful sync (with 1 day
        overlap) and falls back to 30 days for cold start. The previous
        default of "yesterday → today" silently truncated history when the
        scheduler had been down longer than a day.
        """
        if to_date is None:
            to_date = date.today()

        if not self.connector.authenticate():
            logger.error("SameSystem authentication failed")
            return

        db = SessionLocal()
        try:
            if from_date is None:
                status = self._get_sync_status(db, "samesystem_worktime")
                if status.last_incremental_sync:
                    from_date = status.last_incremental_sync.date() - timedelta(days=1)
                else:
                    from_date = date.today() - timedelta(days=30)
                logger.info(f"samesystem_worktime: resuming from {from_date.isoformat()}")

            self._update_sync_status(db, "samesystem_worktime", sync_in_progress=True, last_error=None)
            total_upserted = 0

            # SameSystem /export/calendar is NOT scoped by ctx_token — every
            # ctx returns identical company-wide data. Fetch once, then route
            # each record to its store via DEPT_NR_TO_STORE.
            ctx_token = next(iter(self.connector.departments.values()))
            try:
                salary_data = self.connector.get_salary_export(ctx_token, from_date, to_date)
                if not salary_data:
                    salary_data = self.connector.get_worktime_export(ctx_token, from_date, to_date)
            except Exception as e:
                raise  # let outer except update sync_status with the error

            # Bucket records by store before aggregating, so the existing
            # (employee, date) aggregation runs correctly per-store.
            by_store: Dict[str, list] = defaultdict(list)
            unmapped_nrs: set = set()
            for entry in salary_data:
                nr = (entry.get("department") or {}).get("nr")
                if nr is None:
                    store_name = UNASSIGNED_STORE
                elif nr in DEPT_NR_TO_STORE:
                    store_name = DEPT_NR_TO_STORE[nr]
                else:
                    unmapped_nrs.add(nr)
                    store_name = UNASSIGNED_STORE
                by_store[store_name].append(entry)

            if unmapped_nrs:
                logger.warning(
                    f"Unmapped department.nr values routed to '{UNASSIGNED_STORE}': "
                    f"{sorted(unmapped_nrs)}"
                )

            for store_name, store_records in by_store.items():
                aggregated = self._aggregate_worktime(store_records)
                upserted_for_store = 0
                for (emp_id, entry_date), values in aggregated.items():
                    upserted_for_store += self._upsert_worktime_row(
                        db, store_name, emp_id, entry_date,
                        values["hours"], values["cost"],
                    )
                total_upserted += upserted_for_store
                logger.info(
                    f"Worktime sync for {store_name}: {len(store_records)} raw records "
                    f"→ {upserted_for_store} unique (employee,date) rows upserted"
                )

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

    def _aggregate_worktime(self, raw_records: List[dict]) -> Dict[tuple, dict]:
        """Group SameSystem /export/calendar records by (employee_id, date),
        summing paid hours and cost. The API can return multiple rows per
        employee per day (split shifts, multi-role, scheduled-vs-actual)."""
        agg: Dict[tuple, dict] = defaultdict(lambda: {"hours": 0.0, "cost": 0.0})
        for entry in raw_records or []:
            entry_date = self._parse_date(entry.get("date") or entry.get("day"))
            user = entry.get("user") or {}
            employee_id = str(user.get("id") or "")
            if not entry_date or not employee_id:
                continue

            # Sum paid hours across work events for this row.
            hours = 0.0
            for ev in (entry.get("events", {}) or {}).get("work", []) or []:
                hh_mm = (ev.get("total_hours") or {}).get("without_breaks") or "00:00"
                try:
                    h, m = hh_mm.split(":")
                    hours += int(h) + int(m) / 60.0
                except (ValueError, AttributeError):
                    continue

            cost = float(entry.get("cost", 0) or 0)

            bucket = agg[(employee_id, entry_date)]
            bucket["hours"] += hours
            bucket["cost"] += cost
        return agg

    def _upsert_worktime_row(self, db, store_name: str, employee_id: str,
                             entry_date: date, hours: float, salary: float) -> int:
        """Upsert one (store, date, employee) row. Returns 1 on success, 0 on error."""
        try:
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
