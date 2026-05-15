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
    ly_totals = db.query(
        func.coalesce(func.sum(SalesOrder.total_amount), 0).label('revenue'),
        func.count(SalesOrder.id).label('order_count')
    ).filter(ly_filter).first()
    ly_refunded = get_refunds_total(db, ly_start, ly_end)
    ly_revenue = float(ly_totals.revenue or 0) - ly_refunded
    ly_orders = ly_totals.order_count or 0

    # Get total items sold (current and LY)
    total_items = db.query(
        func.coalesce(func.sum(SalesOrderItem.quantity), 0)
    ).join(SalesOrder).filter(date_filter).scalar() or 0

    ly_items = db.query(
        func.coalesce(func.sum(SalesOrderItem.quantity), 0)
    ).join(SalesOrder).filter(ly_filter).scalar() or 0

    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    ly_avg_order_value = ly_revenue / ly_orders if ly_orders > 0 else 0

    avg_items_per_order = total_items / total_orders if total_orders > 0 else 0
    ly_avg_items_per_order = ly_items / ly_orders if ly_orders > 0 else 0

    def yoy(current, last):
        return round((current - last) / last * 100, 1) if last else 0

    yoy_change = yoy(total_revenue, ly_revenue)
    orders_yoy = yoy(total_orders, ly_orders)
    items_yoy = yoy(total_items, ly_items)
    avg_order_value_yoy = yoy(avg_order_value, ly_avg_order_value)
    avg_items_per_order_yoy = yoy(avg_items_per_order, ly_avg_items_per_order)
    
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
        "avg_items_per_order": round(avg_items_per_order, 2),
        "revenue_last_year": ly_revenue,
        "yoy_change": yoy_change,
        "orders_yoy": orders_yoy,
        "items_yoy": items_yoy,
        "avg_order_value_yoy": avg_order_value_yoy,
        "avg_items_per_order_yoy": avg_items_per_order_yoy,
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
    category: str = Query(default=None, description="Filter by category group"),
    standard_category: str = Query(default=None, description="Filter by exact standard_category (subcategory drill-down)"),
    vendor: str = Query(default=None, description="Filter by vendor"),
    designed_for: str = Query(default=None, description="Filter by designed_for: men, women, unisex"),
    aggregate_by: str = Query(default="parent", description="Aggregate by: 'sku' or 'parent'"),
    compare_to: str = Query(default=None, description="Compare to: 'previous_period' or 'previous_year'"),
    sort_by: str = Query(default="revenue", description="Sort by: 'revenue' or 'quantity'"),
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
    if standard_category and standard_category not in ['ALL', 'All', '']:
        sc_filter = CategoryMapping.standard_category == standard_category
        if category and category not in ['ALL', 'All', '']:
            grp_filter = func.coalesce(CategoryMapping.category_group, CategoryMapping.standard_category, SalesOrderItem.product_category) == category
            cat_filter = and_(sc_filter, grp_filter)
        else:
            cat_filter = sc_filter
    elif category and category not in ['ALL', 'All', '']:
        cat_filter = func.coalesce(CategoryMapping.category_group, CategoryMapping.standard_category, SalesOrderItem.product_category) == category
    
    # Build vendor filter
    vendor_filter = True
    if vendor and vendor not in ['ALL', 'All', '']:
        vendor_filter = SalesOrderItem.vendor == vendor

    # Build designed_for filter
    gender_filter = True
    if designed_for and designed_for not in ['ALL', 'All', '']:
        gender_filter = CategoryMapping.designed_for == designed_for

    sort_expr = func.sum(SalesOrderItem.quantity).desc() if sort_by == "quantity" else func.sum(SalesOrderItem.line_total).desc()

    def apply_limit(q):
        return q.all() if limit == 0 else q.limit(limit).all()

    # Helper function to get product data for a period
    def get_products_data(d_filter, l_filter, c_filter, v_filter, g_filter):
        if aggregate_by == "parent":
            q_parent = db.query(
                func.coalesce(ParentSkuMapping.parent_sku, SalesOrderItem.sku).label('sku'),
                func.coalesce(
                    func.min(ParentSkuMapping.base_product_name),
                    func.min(SalesOrderItem.product_name)
                ).label('name'),
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category).label('category'),
                func.min(CategoryMapping.designed_for).label('designed_for'),
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
                func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category)
            ).order_by(sort_expr)
            return apply_limit(q_parent)
        else:
            q_sku = db.query(
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
            ).order_by(sort_expr
            )
            return apply_limit(q_sku)
    
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


@router.get("/categories/groups")
async def get_category_groups(db: Session = Depends(get_db)):
    """Get distinct category groups for filter dropdowns"""
    rows = db.query(
        func.coalesce(CategoryMapping.category_group, CategoryMapping.standard_category).label('group')
    ).distinct().filter(
        CategoryMapping.standard_category.isnot(None)
    ).order_by('group').all()
    return [r.group for r in rows if r.group]


@router.get("/categories")
async def get_top_categories(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    target_date: date = Query(default=None),
    location: str = Query(default="All", description="Filter: All, Stores, Online, or specific location"),
    vendor: str = Query(default=None, description="Filter by vendor"),
    compare_to: str = Query(default=None, description="Compare to: 'previous_period' or 'previous_year'"),
    category_group: str = Query(default=None, description="Drill into subcategories of this group"),
    limit: int = Query(default=20),
    db: Session = Depends(get_db)
):
    """Get top selling categories with filters and optional comparison.

    Without category_group: groups by category_group (rolled-up view).
    With category_group: shows standard_category breakdown within that group.
    """
    if target_date and not start_date:
        start_date = target_date
        end_date = target_date
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    date_filter = get_date_filter(start_date, end_date)
    loc_filter = get_location_filter(location)

    vendor_filter = True
    if vendor and vendor not in ['ALL', 'All', '']:
        vendor_filter = SalesOrderItem.vendor == vendor

    # group_expr: the label used for aggregation
    # In drill-down mode we filter to the chosen group and break out by standard_category
    group_col = func.coalesce(CategoryMapping.category_group, CategoryMapping.standard_category, SalesOrderItem.product_category, 'Uncategorized')
    std_col   = func.coalesce(CategoryMapping.standard_category, SalesOrderItem.product_category, 'Uncategorized')

    if category_group:
        agg_col   = std_col
        group_filter = group_col == category_group
    else:
        agg_col   = group_col
        group_filter = True

    def get_category_data(d_filter, l_filter, v_filter):
        return db.query(
            agg_col.label('category'),
            func.sum(SalesOrderItem.quantity).label('quantity_sold'),
            func.sum(SalesOrderItem.line_total).label('revenue'),
            func.count(func.distinct(SalesOrder.id)).label('order_count')
        ).join(SalesOrder).outerjoin(
            CategoryMapping, SalesOrderItem.sku == CategoryMapping.sku
        ).filter(
            and_(d_filter, l_filter, v_filter, group_filter)
        ).group_by(agg_col).order_by(
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


@router.get("/brands")
async def get_brands(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    location: str = Query(default="All"),
    compare_to: str = Query(default=None),
    db: Session = Depends(get_db),
):
    """Ranked brand/vendor list with optional period comparison."""
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    date_filter = get_date_filter(start_date, end_date)
    loc_filter = get_location_filter(location)

    def get_brand_data(d_filter, l_filter):
        return db.query(
            SalesOrderItem.vendor.label('vendor'),
            func.sum(SalesOrderItem.line_total).label('revenue'),
            func.sum(SalesOrderItem.quantity).label('quantity'),
            func.count(func.distinct(SalesOrder.id)).label('orders'),
        ).join(SalesOrder).filter(
            and_(d_filter, l_filter,
                 SalesOrderItem.vendor.isnot(None),
                 SalesOrderItem.vendor != '')
        ).group_by(SalesOrderItem.vendor).order_by(
            func.sum(SalesOrderItem.line_total).desc()
        ).all()

    brands = get_brand_data(date_filter, loc_filter)

    compare_map = {}
    if compare_to:
        compare_start, compare_end = get_comparison_period(start_date, end_date, compare_to)
        if compare_start and compare_end:
            comp_filter = get_date_filter(compare_start, compare_end)
            comp_brands = get_brand_data(comp_filter, loc_filter)
            compare_map = {
                b.vendor: {'revenue': float(b.revenue or 0), 'quantity': b.quantity, 'orders': b.orders}
                for b in comp_brands
            }

    result = []
    seen = set()
    for b in brands:
        seen.add(b.vendor)
        item = {
            'vendor': b.vendor,
            'revenue': float(b.revenue or 0),
            'quantity': b.quantity,
            'orders': b.orders,
        }
        if compare_to:
            prev = compare_map.get(b.vendor, {'revenue': 0, 'quantity': 0, 'orders': 0})
            item['prev_revenue'] = prev['revenue']
            item['prev_quantity'] = prev['quantity']
            item['prev_orders'] = prev['orders']
        result.append(item)

    # Include brands that had LY sales but none this period
    if compare_to:
        for vendor, prev in compare_map.items():
            if vendor not in seen:
                result.append({
                    'vendor': vendor,
                    'revenue': 0,
                    'quantity': 0,
                    'orders': 0,
                    'prev_revenue': prev['revenue'],
                    'prev_quantity': prev['quantity'],
                    'prev_orders': prev['orders'],
                })

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


@router.get("/stock-cancellations")
async def get_stock_cancellations(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    db: Session = Depends(get_db)
):
    """Orders cancelled due to lack of stock (Shopify cancelReason=INVENTORY), YTD by default."""
    if start_date is None:
        start_date = date(datetime.now().year, 1, 1)
    if end_date is None:
        end_date = date.today()

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    orders = (
        db.query(SalesOrder)
        .filter(
            SalesOrder.source_system == 'shopify',
            SalesOrder.cancel_reason == 'INVENTORY',
            SalesOrder.cancelled_at >= start_dt,
            SalesOrder.cancelled_at <= end_dt,
        )
        .order_by(SalesOrder.cancelled_at.desc())
        .all()
    )

    rows = [
        {
            'order_number': o.order_number,
            'cancelled_at': o.cancelled_at.isoformat() if o.cancelled_at else None,
            'total_amount': o.total_amount,
            'total_refunded': o.total_refunded,
            'location': o.location,
            'status': o.status,
        }
        for o in orders
    ]

    return {
        'period': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
        'order_count': len(rows),
        'total_lost_revenue': sum(r['total_amount'] for r in rows),
        'orders': rows,
    }


@router.get("/refunds-with-notes")
async def get_refunds_with_notes(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    db: Session = Depends(get_db)
):
    """All YTD Shopify orders with REFUNDED/PARTIALLY_REFUNDED/VOIDED status or a cancel_reason,
    returning order notes and any refund notes so staff can review."""
    if start_date is None:
        start_date = date(datetime.now().year, 1, 1)
    if end_date is None:
        end_date = date.today()

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    from sqlalchemy import or_

    orders = (
        db.query(SalesOrder)
        .filter(
            SalesOrder.source_system == 'shopify',
            SalesOrder.order_date >= start_dt,
            SalesOrder.order_date <= end_dt,
            or_(
                SalesOrder.status.in_(['REFUNDED', 'PARTIALLY_REFUNDED', 'VOIDED']),
                SalesOrder.cancel_reason.isnot(None),
            )
        )
        .order_by(SalesOrder.order_date.desc())
        .all()
    )

    rows = []
    for o in orders:
        refund_notes = [r.note for r in o.refunds if r.note]
        rows.append({
            'order_number': o.order_number,
            'order_date': o.order_date.isoformat() if o.order_date else None,
            'status': o.status,
            'cancel_reason': o.cancel_reason,
            'cancelled_at': o.cancelled_at.isoformat() if o.cancelled_at else None,
            'total_amount': o.total_amount,
            'total_refunded': o.total_refunded,
            'order_note': o.note,
            'refund_notes': refund_notes,
        })

    return {
        'period': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
        'order_count': len(rows),
        'orders': rows,
    }


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
    """Trigger an incremental sync: Sitoo and Shopify sales only."""
    from pipelines.sales_sync import SalesSyncPipeline
    import os

    sales_pipeline = SalesSyncPipeline({
        'sitoo': {
            'base_url': os.environ.get('SITOO_BASE_URL'),
            'api_id': os.environ.get('SITOO_API_ID'),
            'api_key': os.environ.get('SITOO_API_KEY')
        },
        'shopify': {
            'base_url': os.environ.get('SHOPIFY_BASE_URL'),
            'api_key': os.environ.get('SHOPIFY_API_KEY')
        }
    })

    def run_sync():
        try:
            sales_pipeline.sync_incremental()
        except Exception as e:
            logger.error(f"Sales sync error: {e}")

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": "Sync started in background. Refresh in a few moments to see updated data."
    }


@router.post("/sync-shopify")
async def trigger_shopify_sync(
    background_tasks: BackgroundTasks,
    from_date: Optional[date] = Query(default=None),
    to_date: Optional[date] = Query(default=None),
):
    """Trigger a Shopify-only sync. Defaults to YTD. Pass from_date/to_date to narrow the range."""
    import os
    from pipelines.sales_sync import SalesSyncPipeline

    if from_date is None:
        from_date = date(datetime.now().year, 1, 1)

    from_dt = datetime.combine(from_date, datetime.min.time())
    to_dt = datetime.combine(to_date, datetime.max.time()) if to_date else None

    pipeline = SalesSyncPipeline({
        'sitoo': {},
        'shopify': {
            'base_url': os.environ.get('SHOPIFY_BASE_URL'),
            'api_key': os.environ.get('SHOPIFY_API_KEY'),
        }
    })

    def run():
        from database.config import SessionLocal
        from database.models import SalesOrder
        db = SessionLocal()
        try:
            pipeline._update_sync_status(db, 'shopify', sync_in_progress=True, last_error=None)
            logger.info(f"Shopify sync: fetching orders from {from_dt} to {to_dt or 'now'}...")
            batch_size = 250
            cursor = None
            has_next = True
            saved = 0
            while has_next:
                result = pipeline.shopify.get_all_detailed_orders(
                    from_date=from_dt, to_date=to_dt, batch_size=batch_size
                )
                # get_all_detailed_orders fetches all pages at once — save in chunks
                pipeline._save_sales_orders(db, result)
                saved += len(result)
                logger.info(f"Shopify sync: saved {saved} orders so far")
                has_next = False  # get_all_detailed_orders handles pagination internally

            total = db.query(SalesOrder).filter(SalesOrder.source_system == 'shopify').count()
            max_date = (
                db.query(SalesOrder.order_date)
                .filter(SalesOrder.source_system == 'shopify')
                .order_by(SalesOrder.order_date.desc())
                .first()
            )
            pipeline._update_sync_status(db, 'shopify',
                sync_in_progress=False,
                last_full_sync=datetime.now(),
                last_order_date=max_date[0] if max_date else None,
                total_orders_synced=total,
                last_sync_orders_count=saved,
            )
            logger.info(f"Shopify sync complete: {saved} orders processed")
        except Exception as e:
            pipeline._update_sync_status(db, 'shopify', sync_in_progress=False, last_error=str(e))
            logger.error(f"Shopify sync error: {e}")
        finally:
            db.close()

    background_tasks.add_task(run)
    return {
        "status": "started",
        "message": f"Shopify sync started from {from_date} to {to_date or 'today'} in background."
    }


@router.post("/sync-full")
async def trigger_full_sync(
    background_tasks: BackgroundTasks,
    max_orders: int = Query(default=None),
    db: Session = Depends(get_db)
):
    """Trigger a FULL historical sync for all sources (Sitoo, Shopify, Cin7, SameSystem)"""
    from pipelines.sales_sync import SalesSyncPipeline
    from pipelines.stock_sync import StockSyncPipeline
    from pipelines.budget_sync import BudgetSyncPipeline
    import os

    sales_config = {
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
    stock_config = {
        'cin7': {
            'account_id': os.environ.get('CIN7_ACCOUNT_ID'),
            'api_key': os.environ.get('CIN7_API_KEY')
        }
    }
    budget_config = {
        'samesystem': {
            'email': os.environ.get('SAMESYSTEM_EMAIL'),
            'password': os.environ.get('SAMESYSTEM_PASSWORD'),
            'departments': os.environ.get('SAMESYSTEM_DEPARTMENTS', '{}')
        }
    }

    import json
    raw_depts = os.environ.get('SAMESYSTEM_DEPARTMENTS', '{}')
    try:
        budget_config['samesystem']['departments'] = json.loads(raw_depts)
    except Exception:
        budget_config['samesystem']['departments'] = {}

    sales_pipeline = SalesSyncPipeline(sales_config)
    stock_pipeline = StockSyncPipeline(stock_config)
    budget_pipeline = BudgetSyncPipeline(budget_config)

    def run_full_sync():
        try:
            logger.info("Full sync: starting Sitoo + Shopify...")
            sales_pipeline.sync_full_history(max_orders=max_orders)
        except Exception as e:
            logger.error(f"Sales sync error: {e}")
        try:
            logger.info("Full sync: starting Cin7 stock + wholesale...")
            stock_pipeline.sync_stock_levels()
            stock_pipeline.sync_wholesale_orders()
            stock_pipeline.sync_purchase_orders()
        except Exception as e:
            logger.error(f"Stock sync error: {e}")
        try:
            logger.info("Full sync: starting SameSystem budgets + worktime...")
            budget_pipeline.sync_budgets()
            budget_pipeline.sync_worktime()
        except Exception as e:
            logger.error(f"Budget sync error: {e}")
        logger.info("Full sync complete.")

    background_tasks.add_task(run_full_sync)

    return {
        "status": "started",
        "message": "Full sync started (Sitoo, Shopify, Cin7, SameSystem). Check /sync-status for progress."
    }


@router.post("/sync-stock")
async def trigger_stock_sync(background_tasks: BackgroundTasks):
    """Trigger Cin7 stock + wholesale sync"""
    from pipelines.stock_sync import StockSyncPipeline
    import os

    pipeline = StockSyncPipeline({
        'cin7': {
            'account_id': os.environ.get('CIN7_ACCOUNT_ID'),
            'api_key': os.environ.get('CIN7_API_KEY')
        }
    })

    def run():
        try:
            pipeline.sync_stock_levels()
            pipeline.sync_wholesale_orders()
            pipeline.sync_purchase_orders()
        except Exception as e:
            logger.error(f"Stock sync error: {e}")

    background_tasks.add_task(run)
    return {"status": "started", "message": "Cin7 stock sync started."}


@router.post("/sync-budget")
async def trigger_budget_sync(background_tasks: BackgroundTasks):
    """Trigger SameSystem budget + worktime sync"""
    from pipelines.budget_sync import BudgetSyncPipeline
    import os
    import json

    raw = os.environ.get('SAMESYSTEM_DEPARTMENTS', '{}')
    try:
        departments = json.loads(raw)
    except Exception:
        departments = {}

    pipeline = BudgetSyncPipeline({
        'samesystem': {
            'email': os.environ.get('SAMESYSTEM_EMAIL'),
            'password': os.environ.get('SAMESYSTEM_PASSWORD'),
            'departments': departments
        }
    })

    def run():
        try:
            pipeline.sync_budgets()
            pipeline.sync_worktime()
        except Exception as e:
            logger.error(f"Budget sync error: {e}")

    background_tasks.add_task(run)
    return {"status": "started", "message": "SameSystem budget sync started."}


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


# ============== SALES TAB ANALYTICS ==============

@router.get("/revenue/daily")
async def get_daily_revenue(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    group_by: str = Query(default="day", description="day, week, month, quarter"),
    db: Session = Depends(get_db),
):
    """Revenue grouped by day/week/month/quarter with last-year overlay."""
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    trunc = group_by if group_by in ("day", "week", "month", "quarter") else "day"

    date_filter = get_date_filter(start_date, end_date)
    ly_start = start_date.replace(year=start_date.year - 1)
    ly_end = end_date.replace(year=end_date.year - 1)
    ly_filter = get_date_filter(ly_start, ly_end)

    from database.models import SameSystemBudget

    period_expr = func.date_trunc(trunc, SalesOrder.order_date)

    current = db.query(
        period_expr.label("period"),
        func.sum(SalesOrder.total_amount).label("revenue"),
    ).filter(date_filter).group_by(period_expr).order_by(period_expr).all()

    ly = db.query(
        period_expr.label("period"),
        func.sum(SalesOrder.total_amount).label("revenue"),
    ).filter(ly_filter).group_by(period_expr).order_by(period_expr).all()

    # Budget grouped by the same truncation
    budget_period_expr = func.date_trunc(trunc, SameSystemBudget.date)
    budget_rows = db.query(
        budget_period_expr.label("period"),
        func.sum(SameSystemBudget.amount).label("budget"),
    ).filter(
        SameSystemBudget.date >= start_date,
        SameSystemBudget.date <= end_date,
        SameSystemBudget.budget_type == "sales",
        SameSystemBudget.granularity == "daily",
    ).group_by(budget_period_expr).all()

    # Budget is ex VAT; multiply by 1.25 to make it comparable to revenue (inc VAT)
    budget_map = {
        row.period.date().isoformat(): float(row.budget or 0) * 1.25
        for row in budget_rows
    }

    # Align LY by position — nth period of current ↔ nth period of LY.
    # Date-key matching breaks for week/quarter grouping because truncated
    # period boundaries don't shift by exactly one year across years.
    return [
        {
            "date": row.period.date().isoformat(),
            "revenue": float(row.revenue or 0),
            "revenue_last_year": float(ly[i].revenue or 0) if i < len(ly) else 0,
            "budget": budget_map.get(row.period.date().isoformat(), None),
        }
        for i, row in enumerate(current)
    ]


@router.get("/revenue/by-weekday")
async def get_revenue_by_weekday(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    db: Session = Depends(get_db),
):
    """Average revenue by day of week split by Online vs Stores (Monday–Sunday)."""
    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    date_filter = get_date_filter(start_date, end_date)

    # Day counts per weekday (used for averaging, regardless of channel)
    day_counts = db.query(
        func.extract("dow", SalesOrder.order_date).label("weekday"),
        func.count(func.distinct(cast(SalesOrder.order_date, Date))).label("day_count"),
    ).filter(date_filter).group_by(
        func.extract("dow", SalesOrder.order_date)
    ).all()
    day_count_map = {int(r.weekday): int(r.day_count) for r in day_counts}

    # Revenue per weekday per channel
    rows = db.query(
        func.extract("dow", SalesOrder.order_date).label("weekday"),
        SalesOrder.source_system,
        func.sum(SalesOrder.total_amount).label("total_revenue"),
    ).filter(date_filter).group_by(
        func.extract("dow", SalesOrder.order_date),
        SalesOrder.source_system,
    ).all()

    # Build lookup: {dow: {source: total}}
    revenue_map: dict = {}
    for r in rows:
        dow = int(r.weekday)
        revenue_map.setdefault(dow, {})
        revenue_map[dow][r.source_system] = float(r.total_revenue or 0)

    day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    result = []
    for dow in [1, 2, 3, 4, 5, 6, 0]:
        counts = max(day_count_map.get(dow, 1), 1)
        online = revenue_map.get(dow, {}).get("shopify", 0)
        stores = revenue_map.get(dow, {}).get("sitoo", 0)
        result.append({
            "weekday": dow,
            "name": day_names[dow],
            "online_revenue": online,
            "store_revenue": stores,
            "total_revenue": online + stores,
            "online_avg": online / counts,
            "store_avg": stores / counts,
            "avg_revenue": (online + stores) / counts,
            "day_count": counts,
        })
    return result


@router.get("/revenue/running-total")
async def get_running_total(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    db: Session = Depends(get_db),
):
    """Cumulative revenue vs cumulative budget for the selected period."""
    from database.models import SameSystemBudget

    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    date_filter = get_date_filter(start_date, end_date)

    daily = db.query(
        cast(SalesOrder.order_date, Date).label("day"),
        func.sum(SalesOrder.total_amount).label("revenue"),
    ).filter(date_filter).group_by(
        cast(SalesOrder.order_date, Date)
    ).order_by(cast(SalesOrder.order_date, Date)).all()

    budget_rows = db.query(
        SameSystemBudget.date,
        func.sum(SameSystemBudget.amount).label("budget"),
    ).filter(
        SameSystemBudget.date >= start_date,
        SameSystemBudget.date <= end_date,
        SameSystemBudget.budget_type == "sales",
        SameSystemBudget.granularity == "daily",
    ).group_by(SameSystemBudget.date).all()

    revenue_map = {row.day.isoformat(): float(row.revenue or 0) for row in daily}
    # Budget is ex VAT; multiply by 1.25 to make it comparable to revenue (inc VAT)
    budget_map = {row.date.isoformat(): float(row.budget or 0) * 1.25 for row in budget_rows}

    result = []
    cum_revenue = 0.0
    cum_budget = 0.0
    current = start_date
    while current <= end_date:
        day_str = current.isoformat()
        day_rev = revenue_map.get(day_str, 0)
        day_budget = budget_map.get(day_str, 0)
        cum_revenue += day_rev
        cum_budget += day_budget
        result.append({
            "date": day_str,
            "daily_revenue": day_rev,
            "daily_budget": day_budget,
            "cumulative_revenue": cum_revenue,
            "cumulative_budget": cum_budget,
        })
        current += timedelta(days=1)

    return result
