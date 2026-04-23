#!/usr/bin/env python3
"""
Retroactively apply category_mappings to existing sales_order_items.

Steps:
  0. Populate sold_as_vendor in category_mappings from rules + existing data
  1. Update product_category and vendor from category_mappings
  2. Direct fallback: copy vendor across items with the same SKU
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database.config import engine


def populate_sold_as_vendor():
    """Fill in missing sold_as_vendor in category_mappings.

    Order matters: Shopify vendor is authoritative (it reflects how the product
    is sold to customers, e.g. 'Livid Unisex'). designed_for is only used as a
    fallback for SKUs with no Shopify sales history and reflects the original
    design intent (men/women), which is kept separate for budgeting.
    """
    with engine.begin() as conn:

        # Vintage SKUs — hardcoded, unambiguous
        res = conn.execute(text("""
            UPDATE category_mappings
            SET sold_as_vendor = 'Vintage'
            WHERE (
                    sku LIKE 'VN-%'
                 OR sku LIKE 'VIN-%'
                 OR sku LIKE 'EXT-VN-%'
                 OR sku LIKE 'EXT-VIN-%'
                 OR sku LIKE 'EXT-NV-%'
            )
              AND (sold_as_vendor IS NULL OR sold_as_vendor = '')
        """))
        print(f"  Vintage SKU prefixes:          {res.rowcount:,} SKUs")

        # DEP-* → Depot
        res = conn.execute(text("""
            UPDATE category_mappings SET sold_as_vendor = 'Depot'
            WHERE sku LIKE 'DEP-%'
              AND (sold_as_vendor IS NULL OR sold_as_vendor = '')
        """))
        print(f"  DEP-* → Depot:                 {res.rowcount:,} SKUs")

        # Shopify vendor is authoritative for sold_as_vendor.
        # Must run BEFORE the designed_for fallback so that products labelled
        # 'Livid Unisex' in Shopify (even if designed_for='men' in Sitoo)
        # retain that label across all channels.
        res = conn.execute(text("""
            UPDATE category_mappings cm
            SET sold_as_vendor = (
                SELECT soi.vendor
                FROM sales_order_items soi
                JOIN sales_orders so ON soi.order_id = so.id
                WHERE soi.sku = cm.sku
                  AND soi.vendor IS NOT NULL
                  AND soi.vendor != ''
                  AND so.source_system = 'shopify'
                GROUP BY soi.vendor
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE (cm.sold_as_vendor IS NULL OR cm.sold_as_vendor = '')
              AND EXISTS (
                SELECT 1
                FROM sales_order_items soi
                JOIN sales_orders so ON soi.order_id = so.id
                WHERE soi.sku = cm.sku
                  AND soi.vendor IS NOT NULL
                  AND soi.vendor != ''
                  AND so.source_system = 'shopify'
              )
        """))
        print(f"  Shopify vendor (authoritative): {res.rowcount:,} SKUs")

        # Sitoo/general lookup for SKUs with no Shopify data
        res = conn.execute(text("""
            UPDATE category_mappings cm
            SET sold_as_vendor = (
                SELECT vendor
                FROM sales_order_items
                WHERE sku = cm.sku
                  AND vendor IS NOT NULL
                  AND vendor != ''
                GROUP BY vendor
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE (cm.sold_as_vendor IS NULL OR cm.sold_as_vendor = '')
              AND EXISTS (
                SELECT 1 FROM sales_order_items
                WHERE sku = cm.sku
                  AND vendor IS NOT NULL
                  AND vendor != ''
              )
        """))
        print(f"  Sitoo/general lookup:          {res.rowcount:,} SKUs")

        # Sibling lookup: infer from other SKUs sharing the same first two dash-segments
        # (e.g. LIV-KRI-JP-BK-* inherits from other LIV-KRI-* that already have a vendor)
        res = conn.execute(text("""
            UPDATE category_mappings cm
            SET sold_as_vendor = (
                SELECT cm2.sold_as_vendor
                FROM category_mappings cm2
                WHERE cm2.sku LIKE (
                    SPLIT_PART(cm.sku, '-', 1) || '-' || SPLIT_PART(cm.sku, '-', 2) || '-%'
                )
                  AND cm2.sold_as_vendor IS NOT NULL
                  AND cm2.sold_as_vendor != ''
                  AND cm2.sku != cm.sku
                GROUP BY cm2.sold_as_vendor
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE (cm.sold_as_vendor IS NULL OR cm.sold_as_vendor = '')
              AND EXISTS (
                SELECT 1 FROM category_mappings cm2
                WHERE cm2.sku LIKE (
                    SPLIT_PART(cm.sku, '-', 1) || '-' || SPLIT_PART(cm.sku, '-', 2) || '-%'
                )
                  AND cm2.sold_as_vendor IS NOT NULL
                  AND cm2.sold_as_vendor != ''
                  AND cm2.sku != cm.sku
              )
        """))
        print(f"  Sibling lookup:                {res.rowcount:,} SKUs")

        # LIV-* fallback: designed_for for SKUs with no sales history at all.
        # Note: designed_for reflects original design intent (men/women), kept
        # separate from sold_as_vendor which reflects the customer-facing label.
        res = conn.execute(text("""
            UPDATE category_mappings
            SET sold_as_vendor = CASE designed_for
                WHEN 'men'    THEN 'Livid Men'
                WHEN 'women'  THEN 'Livid Femme'
                WHEN 'unisex' THEN 'Livid Unisex'
            END
            WHERE sku LIKE 'LIV-%'
              AND designed_for IS NOT NULL
              AND designed_for != ''
              AND (sold_as_vendor IS NULL OR sold_as_vendor = '')
        """))
        print(f"  LIV-* by designed_for:         {res.rowcount:,} SKUs")

        # LIV-* remaining (isolated model names, no siblings) → Livid Men
        res = conn.execute(text("""
            UPDATE category_mappings SET sold_as_vendor = 'Livid Men'
            WHERE sku LIKE 'LIV-%'
              AND (sold_as_vendor IS NULL OR sold_as_vendor = '')
              AND standard_category NOT IN ('Services', 'Sample', 'Gift Cards', 'Uncategorized')
        """))
        print(f"  LIV-* orphan fallback → Livid Men: {res.rowcount:,} SKUs")

        # Report remaining gaps in category_mappings
        still_null = conn.execute(text("""
            SELECT COUNT(*) FROM category_mappings
            WHERE (sold_as_vendor IS NULL OR sold_as_vendor = '')
              AND standard_category NOT IN (
                  'Gift Cards', 'Services', 'Sample', 'Uncategorized'
              )
        """)).scalar()
        print(f"  Still no vendor (non-trivial categories): {still_null:,} SKUs")


def apply_mappings():
    """Apply category_mappings -> sales_order_items."""
    with engine.begin() as conn:

        # 1. Update product_category from standard_category
        res = conn.execute(text("""
            UPDATE sales_order_items soi
            SET product_category = cm.standard_category
            FROM category_mappings cm
            WHERE soi.sku = cm.sku
              AND cm.standard_category IS NOT NULL
              AND cm.standard_category != ''
              AND (soi.product_category IS DISTINCT FROM cm.standard_category)
        """))
        print(f"  Updated product_category:      {res.rowcount:,} items")

        # 2. Update vendor from sold_as_vendor
        res = conn.execute(text("""
            UPDATE sales_order_items soi
            SET vendor = cm.sold_as_vendor
            FROM category_mappings cm
            WHERE soi.sku = cm.sku
              AND cm.sold_as_vendor IS NOT NULL
              AND cm.sold_as_vendor != ''
              AND (soi.vendor IS DISTINCT FROM cm.sold_as_vendor)
        """))
        print(f"  Updated vendor from mappings:  {res.rowcount:,} items")


def fix_remaining_vendor():
    """
    For items still missing vendor, copy from other sales_order_items
    rows that share the same SKU and already have a vendor set.
    Handles cases where only some historical orders had vendor populated.
    """
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE sales_order_items soi
            SET vendor = (
                SELECT vendor
                FROM sales_order_items
                WHERE sku = soi.sku
                  AND vendor IS NOT NULL
                  AND vendor != ''
                GROUP BY vendor
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            WHERE (soi.vendor IS NULL OR soi.vendor = '')
              AND soi.sku IS NOT NULL
              AND soi.sku != ''
              AND EXISTS (
                SELECT 1 FROM sales_order_items soi2
                WHERE soi2.sku = soi.sku
                  AND soi2.vendor IS NOT NULL
                  AND soi2.vendor != ''
              )
        """))
        print(f"  Fallback copy (same SKU):      {res.rowcount:,} items")


def report_coverage():
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM sales_order_items")).scalar()
        with_cat = conn.execute(text(
            "SELECT COUNT(*) FROM sales_order_items "
            "WHERE product_category IS NOT NULL AND product_category != ''"
        )).scalar()
        with_vendor = conn.execute(text(
            "SELECT COUNT(*) FROM sales_order_items "
            "WHERE vendor IS NOT NULL AND vendor != ''"
        )).scalar()
        print(f"\n  Total items:       {total:,}")
        print(f"  With category:     {with_cat:,}  ({100 * with_cat // total}%)")
        print(f"  With vendor:       {with_vendor:,}  ({100 * with_vendor // total}%)")

        remaining = conn.execute(text("""
            SELECT product_category, COUNT(*) as n
            FROM sales_order_items
            WHERE vendor IS NULL OR vendor = ''
            GROUP BY product_category
            ORDER BY n DESC
            LIMIT 15
        """)).fetchall()
        if remaining:
            print("\n  Still missing vendor (by category):")
            for r in remaining:
                print(f"    {(r.product_category or 'NULL'):30s}  {r.n:,}")


if __name__ == "__main__":
    print("Step 0: Populating sold_as_vendor in category_mappings...")
    populate_sold_as_vendor()

    print("\nStep 1: Applying category_mappings -> sales_order_items...")
    apply_mappings()

    print("\nStep 2: Fallback — copy vendor from same-SKU items...")
    fix_remaining_vendor()

    print("\nCoverage after all steps:")
    report_coverage()
