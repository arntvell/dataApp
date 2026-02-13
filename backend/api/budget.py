"""
Budget dashboard API endpoints.
Budget vs Actual and paygrade analysis using SameSystem + sales data.
"""

import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, cast, Date
from typing import Optional
from datetime import date, datetime
from database.config import get_db
from database.models import SameSystemBudget, SameSystemWorktime, SalesOrder

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard/budget", tags=["Budget"])


@router.get("/vs-actual")
async def get_budget_vs_actual(
    start_date: date = Query(default=None, description="Start date"),
    end_date: date = Query(default=None, description="End date"),
    store: str = Query(default=None, description="Filter by store name"),
    db: Session = Depends(get_db),
):
    """
    Budget vs Actual comparison.
    Joins samesystem_budgets with sales_orders by location+date.
    Returns budget_sales, actual_sales, achievement_pct, budget_salary, actual_salary, paygrade_pct per store.
    """
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    # Budget data filters
    budget_filters = [
        SameSystemBudget.date >= start_date,
        SameSystemBudget.date <= end_date,
        SameSystemBudget.granularity == "daily",
    ]
    if store:
        budget_filters.append(SameSystemBudget.store == store)

    # Aggregate budgets by store and type
    budget_data = db.query(
        SameSystemBudget.store,
        SameSystemBudget.budget_type,
        func.sum(SameSystemBudget.amount).label("total_amount"),
    ).filter(
        and_(*budget_filters)
    ).group_by(
        SameSystemBudget.store, SameSystemBudget.budget_type
    ).all()

    # Build budget lookup: {store: {sales: X, salary: Y}}
    budget_map = {}
    for row in budget_data:
        if row.store not in budget_map:
            budget_map[row.store] = {"sales": 0, "salary": 0}
        budget_map[row.store][row.budget_type] = float(row.total_amount or 0)

    # Actual sales per location
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    sales_filters = [
        SalesOrder.order_date >= start_dt,
        SalesOrder.order_date <= end_dt,
    ]
    if store:
        sales_filters.append(SalesOrder.location == store)

    actual_data = db.query(
        SalesOrder.location,
        func.sum(SalesOrder.total_amount).label("actual_sales"),
    ).filter(
        and_(*sales_filters)
    ).group_by(SalesOrder.location).all()

    actual_map = {row.location: float(row.actual_sales or 0) for row in actual_data}

    # Actual salary cost from worktime
    worktime_filters = [
        SameSystemWorktime.date >= start_date,
        SameSystemWorktime.date <= end_date,
    ]
    if store:
        worktime_filters.append(SameSystemWorktime.store == store)

    salary_data = db.query(
        SameSystemWorktime.store,
        func.sum(SameSystemWorktime.salary_cost).label("actual_salary"),
    ).filter(
        and_(*worktime_filters)
    ).group_by(SameSystemWorktime.store).all()

    salary_map = {row.store: float(row.actual_salary or 0) for row in salary_data}

    # Combine all stores
    all_stores = set(budget_map.keys()) | set(actual_map.keys()) | set(salary_map.keys())

    result = []
    for s in sorted(all_stores):
        budget_sales = budget_map.get(s, {}).get("sales", 0)
        budget_salary = budget_map.get(s, {}).get("salary", 0)
        actual_sales = actual_map.get(s, 0)
        actual_salary = salary_map.get(s, 0)

        achievement_pct = (actual_sales / budget_sales * 100) if budget_sales > 0 else 0
        paygrade_pct = (actual_salary / actual_sales * 100) if actual_sales > 0 else 0

        result.append({
            "store": s,
            "budget_sales": round(budget_sales, 2),
            "actual_sales": round(actual_sales, 2),
            "achievement_pct": round(achievement_pct, 1),
            "budget_salary": round(budget_salary, 2),
            "actual_salary": round(actual_salary, 2),
            "paygrade_pct": round(paygrade_pct, 1),
        })

    return result


@router.get("/paygrade")
async def get_paygrade(
    start_date: date = Query(default=None, description="Start date"),
    end_date: date = Query(default=None, description="End date"),
    db: Session = Depends(get_db),
):
    """
    Paygrade percentage = salary_cost / revenue * 100, per store.
    """
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    # Actual sales per location
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    actual_data = db.query(
        SalesOrder.location,
        func.sum(SalesOrder.total_amount).label("revenue"),
    ).filter(
        SalesOrder.order_date >= start_dt,
        SalesOrder.order_date <= end_dt,
    ).group_by(SalesOrder.location).all()

    actual_map = {row.location: float(row.revenue or 0) for row in actual_data}

    # Salary cost per store
    salary_data = db.query(
        SameSystemWorktime.store,
        func.sum(SameSystemWorktime.salary_cost).label("salary_cost"),
    ).filter(
        SameSystemWorktime.date >= start_date,
        SameSystemWorktime.date <= end_date,
    ).group_by(SameSystemWorktime.store).all()

    salary_map = {row.store: float(row.salary_cost or 0) for row in salary_data}

    all_stores = set(actual_map.keys()) | set(salary_map.keys())

    result = []
    for s in sorted(all_stores):
        revenue = actual_map.get(s, 0)
        salary = salary_map.get(s, 0)
        paygrade_pct = (salary / revenue * 100) if revenue > 0 else 0

        result.append({
            "store": s,
            "revenue": round(revenue, 2),
            "salary_cost": round(salary, 2),
            "paygrade_pct": round(paygrade_pct, 1),
        })

    return result
