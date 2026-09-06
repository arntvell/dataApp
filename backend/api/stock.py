"""
Stock dashboard API endpoints.
Stock overview, per-product stock, and wholesale revenue from Cin7 data.
"""

import logging
import re
from fastapi import APIRouter, Depends, Query, Body, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from typing import Optional
from datetime import date, datetime, timedelta
from database.config import get_db
from database.models import (
    Cin7Stock, Cin7Sale, Cin7SaleItem, Cin7Invoice, Cin7InvoiceItem,
    SalesOrder, SalesOrderItem, ProductMaster, ParentSkuMapping,
    RawShopifyProduct, AllocationPlan,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard/stock", tags=["Stock"])

# Physical locations we plan inventory across. Online is a sales channel
# fulfilled from this same physical stock — not its own stock location.
RETAIL_STORES = ["Livid Oslo", "Livid Bergen", "Livid Trondheim", "Livid Stavanger", "Past Løkka"]
WAREHOUSE = ["Livid Sentrallager"]
PHYSICAL_LOCATIONS = RETAIL_STORES + WAREHOUSE

# Locations the Allocate tab may draw stock FROM. Sentrallager is the default;
# the event/overflow locations hold real sellable goods (market and past-season
# stock lives there) but are invisible to every other planner, so they are opt-in.
ALLOCATION_SOURCES = WAREHOUSE + [
    "EVENTSALG", "MIDLERTIDIG LOKASJON", "VINTAGE NETT MELLOMLAGER", "Livid Kontor",
]

# True non-merchandise: production components, service/dummy SKUs and the returns
# holding bin. These must never reach an allocation plan whatever the filters say.
# (parent "S" is the 95k-unit "Buttons" style, which Cin7 leaves Uncategorized.)
NON_MERCH_PARENTS = {"S", "LIV-PCKUP", "GFTWRP", "LIV-SVD"}
NON_MERCH_CATEGORIES = {"BUTTON", "WRAPIN", "SAVED"}

# Shopify collection-season tags look like SS20 / FW24 (mirrors api.sale._SEASON_RE)
_SEASON_TAG_RE = re.compile(r"^(SS|FW|AW|HO|PRE|RESORT)\s?\d{2}$", re.IGNORECASE)

# Markdown tags Shopify carries on anything that has been put on sale, in any of the
# shapes the store has used over the years: SALE, SALE_FW25, SALESS23_F, PRESALE-SS26.
_SALE_TAG_RE = re.compile(r"^(PRE)?SALE", re.IGNORECASE)


# Size tokens, mirroring pipelines.product_sync._extract_parent. parent_sku_mappings
# is seeded from SALES history, so a SKU that never sold has no size_code — which is
# most of the Imperfect range. Derive it from the SKU instead of showing a dash.
_SIZE_TOKEN = r"\d{4}|XXS|XS|S|M|L|XL|XXL|2XL|3XL|OS|\d{1,2}"
_SIZE_EXACT_RE = re.compile(rf"^({_SIZE_TOKEN})$", re.IGNORECASE)
_SIZE_TAIL_RE = re.compile(rf"^(.+)-({_SIZE_TOKEN})$", re.IGNORECASE)

# Seconds/imperfect stock is a parallel range: IMP-LIV-<rest> mirrors LIV-<rest>.
IMPERFECT_PREFIX = "IMP-"
# "Barnes Japan Black 32/32*" / "Barnes Fade Bone, 3232*" -> "Barnes Japan Black"
_NAME_SIZE_TAIL_RE = re.compile(r"[\s,]*(\d{2}\s*/\s*\d{2}|\d{4})\s*\*?\s*$")


def _derive_size(sku, parent_sku):
    """Size for a variant whose parent_sku_mappings row is missing or sizeless."""
    if not sku:
        return None
    v = sku.strip()
    p = (parent_sku or "").strip()
    if p and len(v) > len(p) + 1 and v.upper().startswith(p.upper() + "-"):
        cand = v[len(p) + 1:]
        if _SIZE_EXACT_RE.match(cand):
            return cand.upper()
    m = _SIZE_TAIL_RE.match(v)
    return m.group(2).upper() if m else None


def _is_imperfect(parent_sku):
    return (parent_sku or "").upper().startswith(IMPERFECT_PREFIX)


def _clean_style_name(name):
    """Drop the per-variant size an Imperfect product name carries."""
    return _NAME_SIZE_TAIL_RE.sub("", (name or "").strip()).strip(" ,*") or (name or "")


def _non_merch(parent_sku, category_group):
    """Components / service SKUs / returns bin — never allocatable."""
    if (parent_sku or "").strip().upper() in NON_MERCH_PARENTS:
        return True
    return (category_group or "").strip().upper() in NON_MERCH_CATEGORIES


def _csv_param(value):
    """Comma-separated query param -> list of trimmed non-empty values."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


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
    """Size x location stock vs recent sales for a product, with suggested per-size transfers."""
    since = _since(days)
    pv = db.query(ParentSkuMapping.sku, ParentSkuMapping.size_code).filter(
        ParentSkuMapping.parent_sku == sku).all()
    if pv:
        variants = [r.sku for r in pv]
        size_of = {r.sku: (r.size_code or r.sku) for r in pv}
    else:
        variants = [sku]
        size_of = {sku: sku}
    up_to_variant = {v.upper().strip(): v for v in variants}
    ups = list(up_to_variant.keys())

    # stock per (variant, location)
    stock = {}
    for u, loc, oh, av in db.query(
        _pm_sku(Cin7Stock.sku), Cin7Stock.location,
        func.sum(Cin7Stock.on_hand), func.sum(Cin7Stock.available)
    ).filter(_pm_sku(Cin7Stock.sku).in_(ups), Cin7Stock.location.in_(PHYSICAL_LOCATIONS)).group_by(
        _pm_sku(Cin7Stock.sku), Cin7Stock.location
    ):
        v = up_to_variant.get(u)
        if v:
            stock[(v, loc)] = (float(oh or 0), float(av or 0))

    # sold per (variant, location), retail stores only
    sold = {}
    for s, loc, q in db.query(
        SalesOrderItem.sku, SalesOrder.location, func.sum(SalesOrderItem.quantity)
    ).join(SalesOrder, SalesOrderItem.order_id == SalesOrder.id).filter(
        SalesOrderItem.sku.in_(variants), SalesOrder.order_date >= since,
        SalesOrder.location.in_(RETAIL_STORES)
    ).group_by(SalesOrderItem.sku, SalesOrder.location):
        sold[(s, loc)] = int(q or 0)

    sizes, moves = [], []
    for v in sorted(variants, key=lambda x: str(size_of.get(x, x))):
        cells, sold_loc, per_loc = {}, {}, []
        tot_av = tot_sold = 0
        for loc in PHYSICAL_LOCATIONS:
            oh, av = stock.get((v, loc), (0, 0))
            sd = sold.get((v, loc), 0)
            cells[loc] = av
            sold_loc[loc] = sd
            tot_av += av
            tot_sold += sd
            per_loc.append({"location": loc, "available": av, "sold": sd})
        if tot_av == 0 and tot_sold == 0:
            continue  # hide sizes with no stock and no sales
        sizes.append({
            "sku": v, "size": size_of.get(v, v), "cells": cells, "sold": sold_loc,
            "total_available": tot_av, "total_sold": tot_sold,
        })
        for m in _suggest_moves(per_loc, days):
            moves.append({**m, "size": size_of.get(v, v), "sku": v})

    name = db.query(ProductMaster.product_name).filter(ProductMaster.sku.in_(ups)).first()
    return {
        "sku": sku, "name": name[0] if name else sku, "days": days,
        "locations": PHYSICAL_LOCATIONS, "sizes": sizes, "suggested_moves": moves,
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


@router.get("/central-allocation")
async def central_allocation(
    q: Optional[str] = Query(None, description="Search by SKU or product name"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    days: int = Query(365, description="Historical sales window per store"),
    season_id: Optional[int] = Query(None, description="Restrict to a Sale Planner season's on-sale styles"),
    locations: Optional[str] = Query(None, description="Comma-separated source locations; default Sentrallager"),
    category: Optional[str] = Query(None, description="Comma-separated category groups, e.g. Vintage,Knitwear"),
    collection: Optional[str] = Query(None, description="Shopify collection-season tag, e.g. FW25"),
    sale_tag: Optional[str] = Query(None, description="Shopify sale tag: 'any' for anything marked down, or an exact tag e.g. SALE_FW25"),
    include_noise: int = Query(0, description="1 = also show sale / imperfect / sample / consignment goods"),
    min_units: int = Query(0, ge=0, description="Only styles holding at least this many units at source"),
    max_sold: Optional[int] = Query(None, description="Only styles that sold at most this many units in the window"),
    db: Session = Depends(get_db),
):
    """
    Sellable stock at the chosen source location(s), variant/size level, with how much
    each retail store has sold and currently holds — the basis for allocating stock out
    to stores. Grouped by style, sorted by brand then name.

    Defaults to the central warehouse. Event/overflow locations can be added via
    `locations`. Production components and service SKUs are always excluded; sale,
    imperfect and sample goods are excluded unless `include_noise=1`.
    """
    from api.sale import _is_noise  # lazy import — sale.py imports from this module
    from database.models import SalePlanItem

    sources = [l for l in _csv_param(locations) if l in ALLOCATION_SOURCES] or [WAREHOUSE[0]]
    want_cats = {c.upper() for c in _csv_param(category)}
    want_coll = (collection or "").strip().upper().replace(" ", "")
    want_sale = (sale_tag or "").strip().upper().replace(" ", "")
    since = _since(days)

    # When scoped to a sale, drop styles the user explicitly excluded from that season.
    excluded_parents = set()
    if season_id is not None:
        excluded_parents = {
            p for (p,) in db.query(SalePlanItem.parent_sku).filter(
                SalePlanItem.season_id == season_id, SalePlanItem.included == False  # noqa: E712
            )
        }

    # How much sellable stock each selectable source holds — powers the location picker.
    source_options = []
    src_units = {loc: 0.0 for loc in ALLOCATION_SOURCES}
    for loc, av in db.query(Cin7Stock.location, func.sum(Cin7Stock.available)).filter(
        Cin7Stock.location.in_(ALLOCATION_SOURCES), Cin7Stock.available > 0
    ).group_by(Cin7Stock.location):
        src_units[loc] = float(av or 0)
    for loc in ALLOCATION_SOURCES:
        source_options.append({"location": loc, "units": src_units.get(loc, 0.0),
                               "is_warehouse": loc in WAREHOUSE})

    # Available stock per variant per source location, keyed on the raw Cin7 SKU
    # (the value we export for import back into the systems).
    avail_by_loc = {}
    wh_avail = {}
    for sku, loc, av in db.query(
        Cin7Stock.sku, Cin7Stock.location, func.sum(Cin7Stock.available)
    ).filter(Cin7Stock.location.in_(sources)).group_by(
        Cin7Stock.sku, Cin7Stock.location
    ).having(func.sum(Cin7Stock.available) > 0):
        v = float(av or 0)
        avail_by_loc.setdefault(sku, {})[loc] = v
        wh_avail[sku] = wh_avail.get(sku, 0.0) + v

    empty = {"stores": RETAIL_STORES, "warehouse": sources[0], "sources": sources,
             "source_options": source_options, "days": days, "brands": [],
             "categories": [], "collections": [], "sale_tags": [], "styles": [],
             "total_units": 0}
    if not wh_avail:
        return empty

    ups = list({s.upper().strip() for s in wh_avail})

    # Product info (only known products — drops components/services not in the SSOT)
    pm = {}
    for sku, parent, brnd, name, img, cat in db.query(
        ProductMaster.sku, ProductMaster.parent_sku, ProductMaster.sold_as_vendor,
        ProductMaster.product_name, ProductMaster.image_url, ProductMaster.category_group
    ).filter(ProductMaster.sku.in_(ups)):
        pm[sku] = (parent or sku, brnd, name, img, cat)

    # Size per variant
    size_of = {}
    for s, size in db.query(ParentSkuMapping.sku, ParentSkuMapping.size_code).filter(
        _pm_sku(ParentSkuMapping.sku).in_(ups)
    ):
        size_of[s.upper().strip()] = size

    # Collection-season tags per parent, from the Shopify product tags
    season_tags, sale_tag_map = {}, {}
    for parent, tags in db.query(ProductMaster.parent_sku, RawShopifyProduct.tags).join(
        RawShopifyProduct, ProductMaster.sku == _pm_sku(RawShopifyProduct.sku)
    ).filter(ProductMaster.sku.in_(ups), ProductMaster.parent_sku.isnot(None),
             RawShopifyProduct.tags.isnot(None)):
        for t in (tags or "").split(","):
            t = t.strip()
            if _SEASON_TAG_RE.match(t):
                season_tags.setdefault(parent, set()).add(t.upper().replace(" ", ""))
            if _SALE_TAG_RE.match(t):
                sale_tag_map.setdefault(parent, set()).add(t.upper().replace(" ", ""))

    # Per-store historical units sold (retail stores only)
    sold = {}
    for u, loc, qty in db.query(
        _pm_sku(SalesOrderItem.sku), SalesOrder.location, func.sum(SalesOrderItem.quantity)
    ).join(SalesOrder, SalesOrderItem.order_id == SalesOrder.id).filter(
        _pm_sku(SalesOrderItem.sku).in_(ups),
        SalesOrder.location.in_(RETAIL_STORES),
        SalesOrder.order_date >= since,
    ).group_by(_pm_sku(SalesOrderItem.sku), SalesOrder.location):
        sold.setdefault(u, {})[loc] = int(qty or 0)

    # Per-store current stock on hand (retail stores) — what each store already has
    store_stock = {}
    for u, loc, oh in db.query(
        _pm_sku(Cin7Stock.sku), Cin7Stock.location, func.sum(Cin7Stock.on_hand)
    ).filter(
        _pm_sku(Cin7Stock.sku).in_(ups), Cin7Stock.location.in_(RETAIL_STORES)
    ).group_by(_pm_sku(Cin7Stock.sku), Cin7Stock.location):
        store_stock.setdefault(u, {})[loc] = int(oh or 0)

    ql = q.lower().strip() if q else None
    styles = {}
    brands, categories, collections, sale_tags = set(), set(), set(), set()
    for sku, avail in wh_avail.items():
        u = sku.upper().strip()
        info = pm.get(u)
        if not info:
            continue  # not a known sellable product
        parent, brnd, name, img, cat = info
        brnd = brnd or "\u2014"
        if _non_merch(parent, cat):
            continue  # buttons / pickup / gift wrap / returns bin — never allocatable
        if not include_noise and _is_noise(parent, brnd):
            continue  # samples / B2B / consignment / sale + imperfect goods
        if season_id is not None and parent in excluded_parents:
            continue  # user pulled this style out of the sale

        # facets describe everything selectable under the current source + noise setting
        brands.add(brnd)
        if cat:
            categories.add(cat)
        tags = season_tags.get(parent) or set()
        collections.update(tags)
        stags = sale_tag_map.get(parent) or set()
        sale_tags.update(stags)

        if brand and brnd != brand:
            continue
        if want_cats and (cat or "").upper() not in want_cats:
            continue
        if want_coll and want_coll not in tags:
            continue
        if want_sale == "ANY":
            if not stags:
                continue          # never marked down in Shopify
        elif want_sale and want_sale not in stags:
            continue
        if ql and ql not in (name or "").lower() and ql not in sku.lower():
            continue

        st = styles.get(parent)
        if st is None:
            st = {"parent_sku": parent, "brand": brnd, "name": name or parent,
                  "image_url": img, "category": cat, "collections": sorted(tags),
                  "sale_tags": sorted(stags), "imperfect": _is_imperfect(parent),
                  "wh_total": 0.0, "sold_total": 0, "sizes": []}
            styles[parent] = st
        s_by_store = {s: sold.get(u, {}).get(s, 0) for s in RETAIL_STORES}
        stk_by_store = {s: store_stock.get(u, {}).get(s, 0) for s in RETAIL_STORES}
        st["sizes"].append({
            "sku": sku, "size": size_of.get(u) or _derive_size(sku, parent) or "\u2014",
            "wh_avail": avail,
            "by_location": avail_by_loc.get(sku, {}),
            "sold": s_by_store, "sold_total": sum(s_by_store.values()),
            "stock": stk_by_store,
        })
        st["wh_total"] += avail
        st["sold_total"] += sum(s_by_store.values())

    # Imperfect variants each carry their own size in product_name ("Barnes Japan
    # Black 32/32*"), so the style would be named after whichever variant was seen
    # first. Borrow the first-quality style's name instead: IMP-LIV-X mirrors LIV-X.
    imp_parents = [p for p in styles if _is_imperfect(p)]
    if imp_parents:
        twins = {p: p[len(IMPERFECT_PREFIX):] for p in imp_parents}
        twin_names = {}
        for par, nm in db.query(
            ProductMaster.parent_sku, func.min(ProductMaster.product_name)
        ).filter(ProductMaster.parent_sku.in_(list(twins.values()))).group_by(ProductMaster.parent_sku):
            twin_names[par] = nm
        for p in imp_parents:
            base = twin_names.get(twins[p]) or _clean_style_name(styles[p]["name"])
            styles[p]["name"] = f"{base} \u2014 Imperfect"

    out = [st for st in styles.values()
           if st["wh_total"] >= min_units and (max_sold is None or st["sold_total"] <= max_sold)]
    out.sort(key=lambda r: ((r["brand"] or "").lower(), (r["name"] or "").lower()))
    for st in out:
        st["sizes"].sort(key=lambda z: str(z["size"]))

    return {
        "stores": RETAIL_STORES, "warehouse": sources[0], "sources": sources,
        "source_options": source_options, "days": days,
        "brands": sorted(brands, key=lambda b: b.lower()),
        "categories": sorted(categories, key=lambda c: c.lower()),
        "collections": sorted(collections),
        "sale_tags": sorted(sale_tags),
        "styles": out, "total_units": sum(st["wh_total"] for st in out),
    }


# ============== SAVED ALLOCATION PLANS ==============
# The Allocate tab keeps a working draft in the browser (fast, no round-trip per
# keystroke). Saving pushes it here so a plan survives a different machine, a
# cleared cache, or a colleague picking the work up.

def _plan_summary(p: AllocationPlan):
    plan = p.plan or {}
    units = sum(int(q or 0) for byloc in plan.values() for q in byloc.values())
    lines = sum(len(byloc) for byloc in plan.values())
    return {
        "id": p.id, "name": p.name, "note": p.note,
        "skus": len(plan), "lines": lines, "units": units,
        "sources": p.sources or [],
        "updated_at": (p.updated_at or p.created_at).isoformat() if (p.updated_at or p.created_at) else None,
    }


@router.get("/allocation-plans")
async def list_allocation_plans(db: Session = Depends(get_db)):
    """Every saved plan, newest touched first."""
    plans = db.query(AllocationPlan).all()
    out = [_plan_summary(p) for p in plans]
    out.sort(key=lambda r: r["updated_at"] or "", reverse=True)
    return {"plans": out}


@router.get("/allocation-plan")
async def get_allocation_plan(name: str = Query(...), db: Session = Depends(get_db)):
    p = db.query(AllocationPlan).filter(AllocationPlan.name == name).first()
    if not p:
        raise HTTPException(status_code=404, detail="No saved plan by that name")
    return {**_plan_summary(p), "plan": p.plan or {}, "sku_info": p.sku_info or {}}


@router.put("/allocation-plan")
async def save_allocation_plan(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Create or overwrite a named plan. Body: {name, plan, sku_info?, sources?, note?}."""
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    plan = payload.get("plan")
    if not isinstance(plan, dict):
        raise HTTPException(status_code=400, detail="plan must be an object of {sku: {location: qty}}")
    # keep only positive integer quantities — a zero is an erased cell, not a line
    clean = {}
    for sku, byloc in plan.items():
        if not isinstance(byloc, dict):
            continue
        keep = {loc: int(q) for loc, q in byloc.items() if str(q).strip() not in ("", "None") and int(q) > 0}
        if keep:
            clean[sku] = keep
    p = db.query(AllocationPlan).filter(AllocationPlan.name == name).first()
    if not p:
        p = AllocationPlan(name=name)
        db.add(p)
    p.plan = clean
    p.sku_info = payload.get("sku_info") or {}
    p.sources = payload.get("sources") or []
    p.note = payload.get("note")
    db.commit()
    db.refresh(p)
    return {"ok": True, **_plan_summary(p)}


@router.post("/allocation-plan/delete")
async def delete_allocation_plan(payload: dict = Body(...), db: Session = Depends(get_db)):
    name = (payload.get("name") or "").strip()
    n = db.query(AllocationPlan).filter(AllocationPlan.name == name).delete()
    db.commit()
    return {"ok": True, "deleted": n}
