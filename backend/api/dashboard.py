"""
Sales Dashboard API Endpoints
Updated with date range filtering, location filters, categories, and YoY comparison
"""

import csv
import io
import logging
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, cast, Date
from typing import Optional, List
from datetime import datetime, date, timedelta
from database.config import get_db
from database.models import SalesOrder, SalesOrderItem, SalesRefund, CategoryMapping
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


# ============== RESPONSE MODELS ==============

class LocationMetrics(BaseModel):
    location: str
    revenue: float
    order_count: int
    item_count: int
    avg_order_value: float
    avg_item_value: float
    revenue_last_year: float = 0
    yoy_change: float = 0


class StaffMetrics(BaseModel):
    staff_id: Optional[str]
    staff_name: Optional[str]
    location: str
    revenue: float
    orders: int
    items_sold: int
    avg_order: float


class DailySummary(BaseModel):
    start_date: str
    end_date: str
    total_revenue: float
    total_refunded: float = 0
    total_orders: int
    total_items: int
    avg_order_value: float
    avg_item_value: float
    revenue_last_year: float = 0
    yoy_change: float = 0
    locations: List[LocationMetrics]


class ProductMetrics(BaseModel):
    sku: Optional[str]
    name: str
    category: Optional[str]
    quantity_sold: int
    revenue: float


class CategoryMetrics(BaseModel):
    category: str
    quantity_sold: int
    revenue: float
    order_count: int


# ============== HELPER FUNCTIONS ==============

def get_refunds_total(db: Session, start_date: date, end_date: date) -> float:
    """Get total refund amount for refunds processed within a date range"""
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    result = db.query(
        func.coalesce(func.sum(SalesRefund.amount), 0)
    ).filter(
        SalesRefund.refund_date >= start_dt,
        SalesRefund.refund_date <= end_dt
    ).scalar()
    return float(result or 0)


def get_date_filter(start_date: date, end_date: date):
    """Create date filter for queries"""
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    return and_(
        SalesOrder.order_date >= start_dt,
        SalesOrder.order_date <= end_dt
    )


def get_location_filter(location: str = None, source: str = None):
    """Create location/source filter"""
    filters = []
    if location and location not in ['ALL', 'All']:
        if location == 'Stores':
            filters.append(SalesOrder.source_system == 'sitoo')
        elif location == 'Online':
            filters.append(SalesOrder.source_system == 'shopify')
        else:
            filters.append(SalesOrder.location == location)
    if source == 'sitoo':
        filters.append(SalesOrder.source_system == 'sitoo')
    elif source == 'shopify':
        filters.append(SalesOrder.source_system == 'shopify')
    return and_(*filters) if filters else True


def get_comparison_period(start_date: date, end_date: date, compare_to: str):
    """Calculate comparison period dates
    
    Args:
        start_date: Current period start
        end_date: Current period end
        compare_to: "previous_period" or "previous_year"
        
    Returns:
        Tuple of (compare_start, compare_end)
    """
    if compare_to == "previous_period":
        period_length = (end_date - start_date).days + 1
        compare_end = start_date - timedelta(days=1)
        compare_start = compare_end - timedelta(days=period_length - 1)
    elif compare_to == "previous_year":
        compare_start = start_date.replace(year=start_date.year - 1)
        compare_end = end_date.replace(year=end_date.year - 1)
    else:
        return None, None
    return compare_start, compare_end


# ============== API ENDPOINTS ==============

@router.get("/summary")
async def get_summary(
    start_date: date = Query(default=None, description="Start date (defaults to today)"),
    end_date: date = Query(default=None, description="End date (defaults to today)"),
    target_date: date = Query(default=None, description="Single date (legacy, use start/end instead)"),
    db: Session = Depends(get_db)
):
    """Get sales summary for a date range with YoY comparison"""
    # Handle legacy single date parameter
    if target_date and not start_date:
        start_date = target_date
        end_date = target_date
    
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    date_filter = get_date_filter(start_date, end_date)
    
    # Calculate last year's date range
    days_diff = (end_date - start_date).days
    ly_end = end_date.replace(year=end_date.year - 1)
    ly_start = start_date.replace(year=start_date.year - 1)
    ly_filter = get_date_filter(ly_start, ly_end)
    
    # Gross revenue from orders placed in this period
    totals = db.query(
        func.coalesce(func.sum(SalesOrder.total_amount), 0).label('revenue'),
        func.count(SalesOrder.id).label('order_count')
    ).filter(date_filter).first()

    gross_revenue = float(totals.revenue or 0)
    total_orders = totals.order_count or 0

    # Refunds processed in this period (from refund table, by refund_date)
    total_refunded = get_refunds_total(db, start_date, end_date)
    total_revenue = gross_revenue - total_refunded

    # Last year metrics
    ly_gross = db.query(
        func.coalesce(func.sum(SalesOrder.total_amount), 0).label('revenue')
    ).filter(ly_filter).first()
    ly_refunded = get_refunds_total(db, ly_start, ly_end)
    ly_revenue = float(ly_gross.revenue or 0) - ly_refunded
    
    # Get total items sold
    total_items = db.query(
        func.coalesce(func.sum(SalesOrderItem.quantity), 0)
    ).join(SalesOrder).filter(date_filter).scalar() or 0
    
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    avg_item_value = total_revenue / total_items if total_items > 0 else 0
    yoy_change = ((total_revenue - ly_revenue) / ly_revenue * 100) if ly_revenue > 0 else 0
    
    # Metrics by location (gross from orders; Online refunds subtracted below)
    location_data = db.query(
        SalesOrder.location,
        func.sum(SalesOrder.total_amount).label('revenue'),
        func.count(SalesOrder.id).label('order_count')
    ).filter(date_filter).group_by(SalesOrder.location).order_by(
        func.sum(SalesOrder.total_amount).desc()
    ).all()

    # Last year by location
    ly_location_data = db.query(
        SalesOrder.location,
        func.sum(SalesOrder.total_amount).label('revenue')
    ).filter(ly_filter).group_by(SalesOrder.location).all()
    ly_location_map = {loc.location: float(loc.revenue or 0) for loc in ly_location_data}
    
    locations = []
    for loc in location_data:
        loc_items = db.query(
            func.coalesce(func.sum(SalesOrderItem.quantity), 0)
        ).join(SalesOrder).filter(
            and_(date_filter, SalesOrder.location == loc.location)
        ).scalar() or 0

        loc_revenue = float(loc.revenue or 0)
        # Subtract Shopify refunds from Online location
        if loc.location == 'Online':
            loc_revenue -= total_refunded
        loc_orders = loc.order_count or 0
        loc_ly_revenue = ly_location_map.get(loc.location, 0)
        if loc.location == 'Online':
            loc_ly_revenue -= ly_refunded
        loc_yoy = ((loc_revenue - loc_ly_revenue) / loc_ly_revenue * 100) if loc_ly_revenue > 0 else 0

        locations.append(LocationMetrics(
            location=loc.location or 'Unknown',
            revenue=loc_revenue,
            order_count=loc_orders,
            item_count=loc_items,
            avg_order_value=loc_revenue / loc_orders if loc_orders > 0 else 0,
            avg_item_value=loc_revenue / loc_items if loc_items > 0 else 0,
            revenue_last_year=loc_ly_revenue,
            yoy_change=round(loc_yoy, 1)
        ))
    
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_revenue": total_revenue,
        "total_refunded": total_refunded,
        "total_orders": total_orders,
        "total_items": total_items,
        "avg_order_value": avg_order_value,
        "avg_item_value": avg_item_value,
        "revenue_last_year": ly_revenue,
        "yoy_change": round(yoy_change, 1),
        "locations": [loc.dict() for loc in locations]
    }


@router.get("/staff")
async def get_staff_performance(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    target_date: date = Query(default=None),
    location: str = Query(default="ALL", description="Filter: ALL, Stores, or specific location"),
    db: Session = Depends(get_db)
):
    """Get staff performance for the date range with location filter"""
    if target_date and not start_date:
        start_date = target_date
        end_date = target_date
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    # Build filters — use staff_name (enriched from staff_mappings) rather
    # than staff_id (Sitoo externalid) since not all POS users have an
    # external ID assigned.
    filters = [
        get_date_filter(start_date, end_date),
        SalesOrder.staff_name.isnot(None),
        SalesOrder.staff_name != '',
    ]
    
    # Location filter (staff are only in Sitoo/POS)
    if location and location not in ['ALL', 'Online']:
        if location == 'Stores':
            filters.append(SalesOrder.source_system == 'sitoo')
        else:
            filters.append(SalesOrder.location == location)
    
    date_filter = and_(*filters)
    
    staff = db.query(
        SalesOrder.staff_name,
        SalesOrder.location,
        func.sum(SalesOrder.total_amount).label('revenue'),
        func.count(SalesOrder.id).label('orders'),
        func.avg(SalesOrder.total_amount).label('avg_order')
    ).filter(date_filter).group_by(
        SalesOrder.staff_name, SalesOrder.location
    ).order_by(func.sum(SalesOrder.total_amount).desc()).all()

    result = []
    for s in staff:
        items_sold = db.query(
            func.coalesce(func.sum(SalesOrderItem.quantity), 0)
        ).join(SalesOrder).filter(
            and_(get_date_filter(start_date, end_date), SalesOrder.staff_name == s.staff_name)
        ).scalar() or 0

        result.append({
            'staff_id': s.staff_name,  # use name as identifier
            'staff_name': s.staff_name,
            'location': s.location,
            'revenue': float(s.revenue or 0),
            'orders': s.orders,
            'items_sold': items_sold,
            'avg_order': float(s.avg_order or 0)
        })
    
    return result


@router.get("/products")
async def get_top_products(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    target_date: date = Query(default=None),
    location: str = Query(default="All", description="Filter: All, Stores, Online, or specific location"),
    category: str = Query(default=None, description="Filter by category"),
    vendor: str = Query(default=None, description="Filter by vendor"),
    designed_for: str = Query(default=None, description="Filter by designed_for: men, women, unisex"),
    aggregate_by: str = Query(default="parent", description="Aggregate by: 'sku' or 'parent'"),
    compare_to: str = Query(default=None, description="Compare to: 'previous_period' or 'previous_year'"),
    limit: int = Query(default=20),
    db: Session = Depends(get_db)
):
    """Get top selling products with filters and optional comparison"""
    from database.models import ParentSkuMapping
    
    if target_date and not start_date:
        start_date = target_date
        end_date = target_date
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    date_filter = get_date_filter(start_date, end_date)
    loc_filter = get_location_filter(location)
    
    # Build category filter
    cat_filter = True
    if category and category not in ['ALL', 'All', '']:
        cat_filter = func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category) == category
    
    # Build vendor filter
    vendor_filter = True
    if vendor and vendor not in ['ALL', 'All', '']:
        vendor_filter = SalesOrderItem.vendor == vendor

    # Build designed_for filter
    gender_filter = True
    if designed_for and designed_for not in ['ALL', 'All', '']:
        gender_filter = CategoryMapping.designed_for == designed_for

    # Helper function to get product data for a period
    def get_products_data(d_filter, l_filter, c_filter, v_filter, g_filter):
        if aggregate_by == "parent":
            return db.query(
                func.coalesce(ParentSkuMapping.parent_sku, SalesOrderItem.sku).label('sku'),
                func.min(SalesOrderItem.product_name).label('name'),
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category).label('category'),
                CategoryMapping.designed_for,
                func.sum(SalesOrderItem.quantity).label('quantity_sold'),
                func.sum(SalesOrderItem.line_total).label('revenue'),
                func.count(func.distinct(SalesOrderItem.sku)).label('variant_count')
            ).join(SalesOrder).outerjoin(
                ParentSkuMapping, SalesOrderItem.sku == ParentSkuMapping.sku
            ).outerjoin(
                CategoryMapping, SalesOrderItem.sku == CategoryMapping.sku
            ).filter(
                and_(d_filter, l_filter, c_filter, v_filter, g_filter)
            ).group_by(
                func.coalesce(ParentSkuMapping.parent_sku, SalesOrderItem.sku),
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category),
                CategoryMapping.designed_for
            ).order_by(
                func.sum(SalesOrderItem.line_total).desc()
            ).limit(limit).all()
        else:
            return db.query(
                SalesOrderItem.sku,
                SalesOrderItem.product_name.label('name'),
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category).label('category'),
                CategoryMapping.designed_for,
                func.sum(SalesOrderItem.quantity).label('quantity_sold'),
                func.sum(SalesOrderItem.line_total).label('revenue')
            ).join(SalesOrder).outerjoin(
                CategoryMapping, SalesOrderItem.sku == CategoryMapping.sku
            ).filter(
                and_(d_filter, l_filter, c_filter, v_filter, g_filter)
            ).group_by(
                SalesOrderItem.sku, SalesOrderItem.product_name,
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category),
                CategoryMapping.designed_for
            ).order_by(
                func.sum(SalesOrderItem.line_total).desc()
            ).limit(limit).all()
    
    # Get current period data
    products = get_products_data(date_filter, loc_filter, cat_filter, vendor_filter, gender_filter)

    # Get comparison data if requested
    compare_data = {}
    if compare_to:
        compare_start, compare_end = get_comparison_period(start_date, end_date, compare_to)
        if compare_start and compare_end:
            compare_date_filter = get_date_filter(compare_start, compare_end)
            compare_products = get_products_data(compare_date_filter, loc_filter, cat_filter, vendor_filter, gender_filter)
            compare_data = {p.sku: {'qty': p.quantity_sold, 'rev': float(p.revenue or 0)} for p in compare_products}
    
    # Build result
    result = []
    for p in products:
        item = {
            'sku': p.sku,
            'name': p.name,
            'category': p.category,
            'designed_for': p.designed_for,
            'quantity_sold': p.quantity_sold,
            'revenue': float(p.revenue or 0),
        }
        if aggregate_by == "parent":
            item['variants'] = p.variant_count
        
        # Add comparison data if available
        if compare_to and p.sku in compare_data:
            item['prev_quantity_sold'] = compare_data[p.sku]['qty']
            item['prev_revenue'] = compare_data[p.sku]['rev']
        elif compare_to:
            item['prev_quantity_sold'] = 0
            item['prev_revenue'] = 0
            
        result.append(item)
    
    return result


@router.get("/products/variants")
async def get_product_variants(
    parent_sku: str = Query(..., description="Parent SKU to get variants for"),
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    location: str = Query(default="All"),
    db: Session = Depends(get_db),
):
    """Get variant-level breakdown for a parent product."""
    from database.models import ParentSkuMapping

    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    date_filter = get_date_filter(start_date, end_date)
    loc_filter = get_location_filter(location)

    # Get all variant SKUs for this parent
    variant_skus = [r.sku for r in db.query(ParentSkuMapping.sku).filter(
        ParentSkuMapping.parent_sku == parent_sku
    ).all()]

    # If no parent mapping, treat the SKU itself as the only variant
    if not variant_skus:
        variant_skus = [parent_sku]

    rows = (
        db.query(
            SalesOrderItem.sku,
            func.min(SalesOrderItem.product_name).label('name'),
            func.coalesce(ParentSkuMapping.size_code, SalesOrderItem.sku).label('size'),
            func.sum(SalesOrderItem.quantity).label('quantity_sold'),
            func.sum(SalesOrderItem.line_total).label('revenue'),
        )
        .join(SalesOrder)
        .outerjoin(ParentSkuMapping, SalesOrderItem.sku == ParentSkuMapping.sku)
        .filter(
            and_(date_filter, loc_filter, SalesOrderItem.sku.in_(variant_skus))
        )
        .group_by(SalesOrderItem.sku, ParentSkuMapping.size_code)
        .order_by(func.sum(SalesOrderItem.line_total).desc())
        .all()
    )

    return [
        {
            'sku': r.sku,
            'size': r.size,
            'quantity_sold': r.quantity_sold,
            'revenue': float(r.revenue or 0),
        }
        for r in rows
    ]


@router.get("/categories/list")
async def get_categories_list(db: Session = Depends(get_db)):
    """Get list of all categories for filter dropdown"""
    categories = db.query(
        CategoryMapping.standard_category
    ).distinct().filter(
        CategoryMapping.standard_category.isnot(None)
    ).order_by(CategoryMapping.standard_category).all()
    
    return [cat.standard_category for cat in categories]


@router.get("/categories")
async def get_top_categories(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    target_date: date = Query(default=None),
    location: str = Query(default="All", description="Filter: All, Stores, Online, or specific location"),
    vendor: str = Query(default=None, description="Filter by vendor"),
    compare_to: str = Query(default=None, description="Compare to: 'previous_period' or 'previous_year'"),
    limit: int = Query(default=20),
    db: Session = Depends(get_db)
):
    """Get top selling categories with filters and optional comparison"""
    if target_date and not start_date:
        start_date = target_date
        end_date = target_date
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date
    
    date_filter = get_date_filter(start_date, end_date)
    loc_filter = get_location_filter(location)
    
    # Build vendor filter
    vendor_filter = True
    if vendor and vendor not in ['ALL', 'All', '']:
        vendor_filter = SalesOrderItem.vendor == vendor
    
    # Helper function to get category data for a period
    def get_category_data(d_filter, l_filter, v_filter):
        return db.query(
            func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category, 'Uncategorized').label('category'),
            func.sum(SalesOrderItem.quantity).label('quantity_sold'),
            func.sum(SalesOrderItem.line_total).label('revenue'),
            func.count(func.distinct(SalesOrder.id)).label('order_count')
        ).join(SalesOrder).outerjoin(
            CategoryMapping, SalesOrderItem.sku == CategoryMapping.sku
        ).filter(
            and_(d_filter, l_filter, v_filter)
        ).group_by(
            func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category, 'Uncategorized')
        ).order_by(
            func.sum(SalesOrderItem.line_total).desc()
        ).limit(limit).all()
    
    # Get current period data
    categories = get_category_data(date_filter, loc_filter, vendor_filter)
    
    # Get comparison data if requested
    compare_data = {}
    if compare_to:
        compare_start, compare_end = get_comparison_period(start_date, end_date, compare_to)
        if compare_start and compare_end:
            compare_date_filter = get_date_filter(compare_start, compare_end)
            compare_categories = get_category_data(compare_date_filter, loc_filter, vendor_filter)
            compare_data = {c.category: {'qty': c.quantity_sold, 'rev': float(c.revenue or 0)} for c in compare_categories}
    
    # Build result
    result = []
    for c in categories:
        item = {
            'category': c.category,
            'quantity_sold': c.quantity_sold,
            'revenue': float(c.revenue or 0),
            'order_count': c.order_count
        }
        
        # Add comparison data if available
        if compare_to and c.category in compare_data:
            item['prev_quantity_sold'] = compare_data[c.category]['qty']
            item['prev_revenue'] = compare_data[c.category]['rev']
        elif compare_to:
            item['prev_quantity_sold'] = 0
            item['prev_revenue'] = 0
            
        result.append(item)
    
    return result


@router.get("/locations/list")
async def get_location_list(db: Session = Depends(get_db)):
    """Get list of all locations for filter dropdown"""
    locations = db.query(
        SalesOrder.location
    ).distinct().filter(
        SalesOrder.location.isnot(None)
    ).order_by(SalesOrder.location).all()
    
    return [loc.location for loc in locations]


@router.get("/vendors/list")
async def get_vendors_list(db: Session = Depends(get_db)):
    """Get list of all vendors for filter dropdown"""
    vendors = db.query(
        SalesOrderItem.vendor
    ).distinct().filter(
        SalesOrderItem.vendor.isnot(None),
        SalesOrderItem.vendor != ''
    ).order_by(SalesOrderItem.vendor).all()
    
    return [v.vendor for v in vendors]


@router.get("/sync-status")
async def get_sync_status(db: Session = Depends(get_db)):
    """Get detailed sync status for all sources"""
    from database.models import SyncStatus as SyncStatusModel
    from pipelines.sales_sync import get_sync_status_summary
    
    source_counts = db.query(
        SalesOrder.source_system,
        func.count(SalesOrder.id).label('count'),
        func.min(SalesOrder.order_date).label('min_date'),
        func.max(SalesOrder.order_date).label('max_date')
    ).group_by(SalesOrder.source_system).all()
    
    total = db.query(func.count(SalesOrder.id)).scalar() or 0
    sync_statuses = get_sync_status_summary()
    
    # Get last sync time
    last_sync = None
    for source, status in sync_statuses.items():
        inc_sync = status.get('last_incremental_sync')
        if inc_sync:
            sync_time = datetime.fromisoformat(inc_sync)
            if last_sync is None or sync_time > last_sync:
                last_sync = sync_time
    
    sources = {}
    for sc in source_counts:
        sources[sc.source_system] = {
            'order_count': sc.count,
            'date_range_start': sc.min_date.isoformat() if sc.min_date else None,
            'date_range_end': sc.max_date.isoformat() if sc.max_date else None,
            'sync_status': sync_statuses.get(sc.source_system, {})
        }
    
    return {
        'total_orders': total,
        'last_sync': last_sync.isoformat() if last_sync else None,
        'sources': sources,
        'sync_statuses': sync_statuses
    }


@router.post("/sync")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Trigger an incremental sales data sync"""
    from pipelines.sales_sync import SalesSyncPipeline
    import os
    
    config = {
        'sitoo': {
            'base_url': os.environ.get('SITOO_BASE_URL'),
            'api_id': os.environ.get('SITOO_API_ID'),
            'api_key': os.environ.get('SITOO_API_KEY')
        },
        'shopify': {
            'base_url': os.environ.get('SHOPIFY_BASE_URL'),
            'api_key': os.environ.get('SHOPIFY_API_KEY')
        }
    }
    
    pipeline = SalesSyncPipeline(config)
    
    # Run sync in background
    def run_sync():
        try:
            pipeline.sync_incremental()
        except Exception as e:
            logger.error(f"Sync error: {e}")
    
    background_tasks.add_task(run_sync)
    
    return {
        "status": "started",
        "message": "Sync started in background. Refresh in a few moments to see updated data."
    }


@router.post("/sync-full")
async def trigger_full_sync(
    background_tasks: BackgroundTasks,
    max_orders: int = Query(default=None),
    db: Session = Depends(get_db)
):
    """Trigger a FULL historical sync"""
    from pipelines.sales_sync import SalesSyncPipeline
    import os
    
    config = {
        'sitoo': {
            'base_url': os.environ.get('SITOO_BASE_URL'),
            'api_id': os.environ.get('SITOO_API_ID'),
            'api_key': os.environ.get('SITOO_API_KEY')
        },
        'shopify': {
            'base_url': os.environ.get('SHOPIFY_BASE_URL'),
            'api_key': os.environ.get('SHOPIFY_API_KEY')
        }
    }
    
    pipeline = SalesSyncPipeline(config)
    
    def run_full_sync():
        try:
            pipeline.sync_full_history(max_orders=max_orders)
        except Exception as e:
            logger.error(f"Full sync error: {e}")
    
    background_tasks.add_task(run_full_sync)
    
    return {
        "status": "started",
        "message": "Full historical sync started in background. Check /sync-status for progress."
    }


# ============== CSV EXPORT ENDPOINTS ==============

def _csv_response(rows: list, headers: list, filename: str) -> StreamingResponse:
    """Build a CSV StreamingResponse from rows and headers."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/export/sales")
async def export_sales(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Export all sales order line items as CSV for the given date range."""
    if start_date is None:
        start_date = date.today().replace(day=1)
    if end_date is None:
        end_date = date.today()

    filters = [get_date_filter(start_date, end_date)]
    if location and location not in ("ALL",):
        if location == "Online":
            filters.append(SalesOrder.source_system == "shopify")
        elif location == "Stores":
            filters.append(SalesOrder.source_system == "sitoo")
        else:
            filters.append(SalesOrder.location == location)

    rows = (
        db.query(
            SalesOrder.order_date,
            SalesOrder.source_system,
            SalesOrder.location,
            SalesOrder.source_id,
            SalesOrder.staff_name,
            SalesOrderItem.sku,
            SalesOrderItem.product_name,
            SalesOrderItem.product_category,
            SalesOrderItem.vendor,
            CategoryMapping.designed_for,
            SalesOrderItem.quantity,
            SalesOrderItem.unit_price,
            SalesOrderItem.discount_amount,
            SalesOrderItem.line_total,
        )
        .join(SalesOrder)
        .outerjoin(CategoryMapping, SalesOrderItem.sku == CategoryMapping.sku)
        .filter(and_(*filters))
        .order_by(SalesOrder.order_date.desc())
        .all()
    )

    headers = [
        "order_date", "source", "location", "order_id", "staff",
        "sku", "product", "category", "vendor", "designed_for",
        "qty", "unit_price", "discount", "line_total",
    ]
    return _csv_response(rows, headers, f"sales_{start_date}_{end_date}.csv")


@router.get("/export/staff")
async def export_staff_performance(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Export staff performance summary as CSV."""
    if start_date is None:
        start_date = date.today().replace(day=1)
    if end_date is None:
        end_date = date.today()

    filters = [
        get_date_filter(start_date, end_date),
        SalesOrder.staff_name.isnot(None),
        SalesOrder.staff_name != "",
    ]
    if location and location not in ("ALL",):
        filters.append(SalesOrder.location == location)

    rows = (
        db.query(
            SalesOrder.staff_name,
            SalesOrder.location,
            func.count(SalesOrder.id).label("orders"),
            func.sum(SalesOrder.total_amount).label("revenue"),
            func.avg(SalesOrder.total_amount).label("avg_order"),
        )
        .filter(and_(*filters))
        .group_by(SalesOrder.staff_name, SalesOrder.location)
        .order_by(func.sum(SalesOrder.total_amount).desc())
        .all()
    )

    headers = ["staff_name", "location", "orders", "revenue", "avg_order"]
    return _csv_response(
        [(r.staff_name, r.location, r.orders, f"{r.revenue:.2f}", f"{r.avg_order:.2f}") for r in rows],
        headers,
        f"staff_{start_date}_{end_date}.csv",
    )


@router.get("/export/categories")
async def export_categories(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    location: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Export category breakdown as CSV."""
    if start_date is None:
        start_date = date.today().replace(day=1)
    if end_date is None:
        end_date = date.today()

    filters = [get_date_filter(start_date, end_date)]
    if location and location not in ("ALL",):
        if location == "Online":
            filters.append(SalesOrder.source_system == "shopify")
        elif location == "Stores":
            filters.append(SalesOrder.source_system == "sitoo")
        else:
            filters.append(SalesOrder.location == location)

    rows = (
        db.query(
            SalesOrderItem.product_category,
            SalesOrder.location,
            func.count(SalesOrderItem.id).label("items_sold"),
            func.sum(SalesOrderItem.quantity).label("quantity"),
            func.sum(SalesOrderItem.line_total).label("revenue"),
        )
        .join(SalesOrder)
        .filter(and_(*filters))
        .group_by(SalesOrderItem.product_category, SalesOrder.location)
        .order_by(func.sum(SalesOrderItem.line_total).desc())
        .all()
    )

    headers = ["category", "location", "items_sold", "quantity", "revenue"]
    return _csv_response(
        [(r.product_category, r.location, r.items_sold, r.quantity, f"{r.revenue:.2f}") for r in rows],
        headers,
        f"categories_{start_date}_{end_date}.csv",
    )
