"""
Stock dashboard API endpoints.
Stock overview, per-product stock, and wholesale revenue from Cin7 data.
"""

import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from typing import Optional
from datetime import date, datetime, timedelta
from database.config import get_db
from database.models import (
    Cin7Stock, Cin7Sale, Cin7SaleItem, Cin7Invoice, Cin7InvoiceItem,
    SalesOrder, SalesOrderItem, ProductMaster, ParentSkuMapping,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard/stock", tags=["Stock"])

# Physical locations we plan inventory across. Online is a sales channel
# fulfilled from this same physical stock — not its own stock location.
RETAIL_STORES = ["Livid Oslo", "Livid Bergen", "Livid Trondheim", "Livid Stavanger", "Past Løkka"]
WAREHOUSE = ["Livid Sentrallager"]
PHYSICAL_LOCATIONS = RETAIL_STORES + WAREHOUSE


def _since(days: int) -> datetime:
    return datetime.combine(date.today() - timedelta(days=days), datetime.min.time())


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


# ============== INVENTORY PLANNING (Stock tab) ==============
# Category/stock are joined to the product SSOT (product_master) by normalized SKU,
# since Cin7 SKUs and sales SKUs vary in case.

def _pm_sku(col):
    return func.upper(func.btrim(col))


@router.get("/locations-summary")
async def locations_summary(days: int = Query(30, description="Sales window for velocity/cover"),
                            db: Session = Depends(get_db)):
    """Per physical location: on-hand, available, units sold in the window, and days of cover."""
    since = _since(days)

    stock = {}
    for loc, oh, av in db.query(
        Cin7Stock.location, func.sum(Cin7Stock.on_hand), func.sum(Cin7Stock.available)
    ).filter(Cin7Stock.location.in_(PHYSICAL_LOCATIONS)).group_by(Cin7Stock.location):
        stock[loc] = (float(oh or 0), float(av or 0))

    sold = {}
    for loc, q in db.query(
        SalesOrder.location, func.sum(SalesOrderItem.quantity)
    ).join(SalesOrderItem, SalesOrderItem.order_id == SalesOrder.id).filter(
        SalesOrder.order_date >= since, SalesOrder.location.in_(RETAIL_STORES)
    ).group_by(SalesOrder.location):
        sold[loc] = int(q or 0)

    online = db.query(func.sum(SalesOrderItem.quantity)).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).filter(SalesOrder.order_date >= since, SalesOrder.source_system == 'shopify').scalar() or 0

    out = []
    for loc in PHYSICAL_LOCATIONS:
        oh, av = stock.get(loc, (0, 0))
        s = sold.get(loc, 0)
        daily = s / days if days else 0
        cover = round(av / daily, 1) if daily > 0 else None
        out.append({
            "location": loc, "on_hand": oh, "available": av, "sold": s,
            "daily": round(daily, 2), "days_cover": cover, "is_warehouse": loc in WAREHOUSE,
        })
    return {"days": days, "locations": out, "online_demand": int(online)}


@router.get("/matrix")
async def stock_matrix(days: int = Query(30), db: Session = Depends(get_db)):
    """Category-group x location on-hand matrix, plus units sold in the window per group."""
    since = _since(days)
    grp = func.coalesce(ProductMaster.category_group, 'Uncategorized')

    # on-hand per (group, location)
    cells = {}
    groups = {}
    for g, loc, oh in db.query(
        grp.label('g'), Cin7Stock.location, func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).outerjoin(
        ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)
    ).filter(Cin7Stock.location.in_(PHYSICAL_LOCATIONS)).group_by(grp, Cin7Stock.location):
        groups.setdefault(g, {"group": g, "cells": {}, "on_hand": 0, "sold": 0})
        groups[g]["cells"][loc] = float(oh or 0)
        groups[g]["on_hand"] += float(oh or 0)

    # sold per group (all stores + online) in window
    for g, q in db.query(grp.label('g'), func.sum(SalesOrderItem.quantity)).select_from(SalesOrderItem).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).outerjoin(ProductMaster, ProductMaster.sku == _pm_sku(SalesOrderItem.sku)).filter(
        SalesOrder.order_date >= since
    ).group_by(grp):
        groups.setdefault(g, {"group": g, "cells": {}, "on_hand": 0, "sold": 0})
        groups[g]["sold"] = int(q or 0)

    rows = sorted(groups.values(), key=lambda r: r["on_hand"], reverse=True)
    return {"days": days, "locations": PHYSICAL_LOCATIONS, "rows": rows}


@router.get("/matrix/products")
async def stock_matrix_products(category_group: str = Query(...),
                                days: int = Query(30), limit: int = Query(100),
                                db: Session = Depends(get_db)):
    """Products (by parent SKU) within a category group: per-location on-hand + sold in window."""
    since = _since(days)
    parent = func.coalesce(ParentSkuMapping.parent_sku, _pm_sku(Cin7Stock.sku))
    grp = func.coalesce(ProductMaster.category_group, 'Uncategorized')

    prods = {}
    for psku, name, loc, oh in db.query(
        parent.label('p'), func.min(ProductMaster.product_name), Cin7Stock.location, func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).outerjoin(
        ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)
    ).outerjoin(
        ParentSkuMapping, ParentSkuMapping.sku == _pm_sku(Cin7Stock.sku)
    ).filter(Cin7Stock.location.in_(PHYSICAL_LOCATIONS), grp == category_group).group_by(parent, Cin7Stock.location):
        prods.setdefault(psku, {"parent_sku": psku, "name": name, "cells": {}, "on_hand": 0, "sold": 0})
        prods[psku]["cells"][loc] = float(oh or 0)
        prods[psku]["on_hand"] += float(oh or 0)
        if name and not prods[psku]["name"]:
            prods[psku]["name"] = name

    rows = sorted(prods.values(), key=lambda r: r["on_hand"], reverse=True)[:limit]
    return {"category_group": category_group, "locations": PHYSICAL_LOCATIONS, "products": rows}


def _suggest_moves(by_loc, days):
    """Simple redistribution: move dead stock toward stores that are selling but low/out."""
    moves = []
    donors = sorted([l for l in by_loc if l["available"] > 0 and l["sold"] == 0],
                    key=lambda l: l["available"], reverse=True)
    receivers = sorted([l for l in by_loc if l["sold"] > 0 and l["available"] <= max(1, round(l["sold"] / 2))],
                       key=lambda l: l["sold"], reverse=True)
    pool = {d["location"]: d["available"] for d in donors}
    for r in receivers:
        need = max(1, r["sold"] - r["available"])  # bring up toward recent demand
        for d in donors:
            if need <= 0:
                break
            avail = pool.get(d["location"], 0)
            if avail <= 0 or d["location"] == r["location"]:
                continue
            qty = min(avail, need)
            if qty > 0:
                moves.append({"from": d["location"], "to": r["location"], "qty": int(qty)})
                pool[d["location"]] -= qty
                need -= qty
    return moves


@router.get("/product-detail")
async def product_detail(sku: str = Query(..., description="Parent or variant SKU"),
                         days: int = Query(30), db: Session = Depends(get_db)):
    """Per-location stock vs recent sales for a product, with suggested transfers."""
    since = _since(days)
    variants = [r.sku for r in db.query(ParentSkuMapping.sku).filter(ParentSkuMapping.parent_sku == sku).all()] or [sku]
    up = [v.upper().strip() for v in variants]

    stock = {}
    for loc, oh, av in db.query(
        Cin7Stock.location, func.sum(Cin7Stock.on_hand), func.sum(Cin7Stock.available)
    ).filter(_pm_sku(Cin7Stock.sku).in_(up), Cin7Stock.location.in_(PHYSICAL_LOCATIONS)).group_by(Cin7Stock.location):
        stock[loc] = (float(oh or 0), float(av or 0))

    sold = {}
    for loc, q in db.query(
        SalesOrder.location, func.sum(SalesOrderItem.quantity)
    ).join(SalesOrder, SalesOrderItem.order_id == SalesOrder.id).filter(
        _pm_sku(SalesOrderItem.sku).in_(up), SalesOrder.order_date >= since,
        SalesOrder.location.in_(RETAIL_STORES)
    ).group_by(SalesOrder.location):
        sold[loc] = int(q or 0)

    by_loc = []
    for loc in PHYSICAL_LOCATIONS:
        oh, av = stock.get(loc, (0, 0))
        by_loc.append({"location": loc, "on_hand": oh, "available": av, "sold": sold.get(loc, 0)})

    name = db.query(ProductMaster.product_name).filter(ProductMaster.sku.in_(up)).first()
    return {
        "sku": sku, "name": name[0] if name else sku, "days": days,
        "by_location": by_loc, "suggested_moves": _suggest_moves(by_loc, days),
    }


@router.get("/search")
async def stock_search(q: str = Query(..., min_length=2), limit: int = Query(20),
                       db: Session = Depends(get_db)):
    """Search products by SKU or name; returns parents with total physical on-hand."""
    parent = func.coalesce(ParentSkuMapping.parent_sku, _pm_sku(Cin7Stock.sku))
    like = f"%{q}%"
    rows = db.query(
        parent.label('p'), func.min(ProductMaster.product_name), func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).outerjoin(
        ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)
    ).outerjoin(
        ParentSkuMapping, ParentSkuMapping.sku == _pm_sku(Cin7Stock.sku)
    ).filter(
        Cin7Stock.location.in_(PHYSICAL_LOCATIONS),
        or_(Cin7Stock.sku.ilike(like), ProductMaster.product_name.ilike(like), parent.ilike(like)),
    ).group_by(parent).order_by(func.sum(Cin7Stock.on_hand).desc()).limit(limit).all()

    return [{"parent_sku": r[0], "name": r[1] or r[0], "on_hand": float(r[2] or 0)} for r in rows]
