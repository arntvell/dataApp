"""
Stock dashboard API endpoints.
Stock overview, per-product stock, and wholesale revenue from Cin7 data.
"""

import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import Optional
from datetime import date, datetime
from database.config import get_db
from database.models import Cin7Stock, Cin7Sale, Cin7SaleItem, Cin7Invoice, Cin7InvoiceItem

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard/stock", tags=["Stock"])


@router.get("/overview")
async def get_stock_overview(
    location: str = Query(default=None, description="Filter by warehouse location"),
    db: Session = Depends(get_db),
):
    """
    Stock overview: totals per location.
    Returns on_hand, allocated, available, on_order aggregated by location.
    """
    filters = []
    if location:
        filters.append(Cin7Stock.location == location)

    data = db.query(
        Cin7Stock.location,
        func.sum(Cin7Stock.on_hand).label("on_hand"),
        func.sum(Cin7Stock.allocated).label("allocated"),
        func.sum(Cin7Stock.available).label("available"),
        func.sum(Cin7Stock.on_order).label("on_order"),
        func.count(Cin7Stock.id).label("sku_count"),
    ).filter(
        and_(*filters) if filters else True
    ).group_by(
        Cin7Stock.location
    ).order_by(
        func.sum(Cin7Stock.on_hand).desc()
    ).all()

    return [
        {
            "location": row.location,
            "on_hand": float(row.on_hand or 0),
            "allocated": float(row.allocated or 0),
            "available": float(row.available or 0),
            "on_order": float(row.on_order or 0),
            "sku_count": row.sku_count,
        }
        for row in data
    ]


@router.get("/by-product")
async def get_stock_by_product(
    sku: str = Query(default=None, description="Filter by SKU (partial match)"),
    location: str = Query(default=None, description="Filter by location"),
    limit: int = Query(default=50, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Per-product stock levels, optionally filtered by SKU and/or location.
    """
    filters = []
    if sku:
        filters.append(Cin7Stock.sku.ilike(f"%{sku}%"))
    if location:
        filters.append(Cin7Stock.location == location)

    data = db.query(
        Cin7Stock.sku,
        Cin7Stock.location,
        Cin7Stock.on_hand,
        Cin7Stock.allocated,
        Cin7Stock.available,
        Cin7Stock.on_order,
    ).filter(
        and_(*filters) if filters else True
    ).order_by(
        Cin7Stock.on_hand.desc()
    ).limit(limit).all()

    return [
        {
            "sku": row.sku,
            "location": row.location,
            "on_hand": float(row.on_hand or 0),
            "allocated": float(row.allocated or 0),
            "available": float(row.available or 0),
            "on_order": float(row.on_order or 0),
        }
        for row in data
    ]


def _wholesale_filters(start_dt, end_dt):
    """Common filters for wholesale queries: date range + exclude webshop/internal"""
    return [
        Cin7Sale.order_date >= start_dt,
        Cin7Sale.order_date <= end_dt,
        Cin7Sale.sales_representative != "lividjeans",
        Cin7Sale.customer_name != "Livid Retail AS",
    ]


@router.get("/wholesale")
async def get_wholesale_revenue(
    start_date: date = Query(default=None, description="Start date"),
    end_date: date = Query(default=None, description="End date"),
    db: Session = Depends(get_db),
):
    """
    Wholesale revenue summary from Cin7 sales orders.
    Excludes webshop orders (sales_rep=lividjeans) and internal transfers (Livid Retail AS).
    """
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    filters = _wholesale_filters(start_dt, end_dt)

    # Aggregate wholesale sales
    totals = db.query(
        func.count(Cin7Sale.id).label("order_count"),
        func.coalesce(func.sum(Cin7Sale.total_amount), 0).label("total_revenue"),
    ).filter(*filters).first()

    # Top customers
    customers = db.query(
        Cin7Sale.customer_name,
        func.count(Cin7Sale.id).label("order_count"),
        func.sum(Cin7Sale.total_amount).label("revenue"),
    ).filter(*filters).group_by(
        Cin7Sale.customer_name
    ).order_by(
        func.sum(Cin7Sale.total_amount).desc()
    ).limit(10).all()

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "order_count": totals.order_count or 0,
        "total_revenue": float(totals.total_revenue or 0),
        "top_customers": [
            {
                "customer": row.customer_name or "Unknown",
                "order_count": row.order_count,
                "revenue": float(row.revenue or 0),
            }
            for row in customers
        ],
    }


@router.get("/wholesale/invoices")
async def get_wholesale_invoices(
    start_date: date = Query(default=None, description="Start date (invoice date)"),
    end_date: date = Query(default=None, description="End date (invoice date)"),
    db: Session = Depends(get_db),
):
    """
    Wholesale invoiced revenue — only what's actually been billed.
    Excludes webshop orders (sales_rep=lividjeans) and internal transfers (Livid Retail AS).
    """
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    # Join invoices → sales, filter by invoice date + wholesale-only
    totals = db.query(
        func.count(func.distinct(Cin7Invoice.id)).label("invoice_count"),
        func.coalesce(func.sum(Cin7Invoice.total), 0).label("invoiced_total"),
        func.coalesce(func.sum(Cin7Invoice.paid), 0).label("paid_total"),
    ).join(
        Cin7Sale, Cin7Invoice.sale_id == Cin7Sale.id
    ).filter(
        Cin7Invoice.invoice_date >= start_dt,
        Cin7Invoice.invoice_date <= end_dt,
        Cin7Sale.sales_representative != "lividjeans",
        Cin7Sale.customer_name != "Livid Retail AS",
    ).first()

    # Top customers by invoiced amount
    customers = db.query(
        Cin7Sale.customer_name,
        func.count(func.distinct(Cin7Invoice.id)).label("invoice_count"),
        func.sum(Cin7Invoice.total).label("invoiced"),
        func.sum(Cin7Invoice.paid).label("paid"),
    ).join(
        Cin7Sale, Cin7Invoice.sale_id == Cin7Sale.id
    ).filter(
        Cin7Invoice.invoice_date >= start_dt,
        Cin7Invoice.invoice_date <= end_dt,
        Cin7Sale.sales_representative != "lividjeans",
        Cin7Sale.customer_name != "Livid Retail AS",
    ).group_by(
        Cin7Sale.customer_name
    ).order_by(
        func.sum(Cin7Invoice.total).desc()
    ).limit(10).all()

    # Top invoiced products
    products = db.query(
        Cin7InvoiceItem.sku,
        Cin7InvoiceItem.product_name,
        func.sum(Cin7InvoiceItem.quantity).label("qty"),
        func.sum(Cin7InvoiceItem.line_total).label("revenue"),
    ).join(
        Cin7Invoice, Cin7InvoiceItem.invoice_id == Cin7Invoice.id
    ).join(
        Cin7Sale, Cin7Invoice.sale_id == Cin7Sale.id
    ).filter(
        Cin7Invoice.invoice_date >= start_dt,
        Cin7Invoice.invoice_date <= end_dt,
        Cin7Sale.sales_representative != "lividjeans",
        Cin7Sale.customer_name != "Livid Retail AS",
    ).group_by(
        Cin7InvoiceItem.sku, Cin7InvoiceItem.product_name
    ).order_by(
        func.sum(Cin7InvoiceItem.line_total).desc()
    ).limit(20).all()

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "invoice_count": totals.invoice_count or 0,
        "invoiced_total": float(totals.invoiced_total or 0),
        "paid_total": float(totals.paid_total or 0),
        "top_customers": [
            {
                "customer": row.customer_name or "Unknown",
                "invoice_count": row.invoice_count,
                "invoiced": float(row.invoiced or 0),
                "paid": float(row.paid or 0),
            }
            for row in customers
        ],
        "top_products": [
            {
                "sku": row.sku,
                "product_name": row.product_name,
                "quantity": float(row.qty or 0),
                "revenue": float(row.revenue or 0),
            }
            for row in products
        ],
    }
