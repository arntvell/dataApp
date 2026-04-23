"""
Update the SALE_FW25 planner CSV with fresh stock and sell-through data.

Matching strategy (in priority order):
  1. Shopify SALE_FW25 tag: product title -> variant SKUs
  2. sales_order_items: product_name -> SKUs (for items sold but not in Shopify tag)
  3. category_mappings: product_name -> SKU (stripping size suffixes for family match)

Usage:
    docker compose exec backend python scripts/update_sale_planner.py
"""

import csv
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from connectors.shopify_connector import ShopifyConnector
from database.config import SessionLocal
from sqlalchemy import text

INPUT_PATH = "/tmp/sale_planner_input.csv"
OUTPUT_PATH = "/tmp/sale_planner_updated.csv"
SALE_TAG = "SALE_FW25"
SALE_START = "2025-12-27"

PHYSICAL_STORES = [
    "Livid Oslo", "Livid Trondheim", "Livid Bergen",
    "Livid Stavanger", "Past Oslo",
]
ONLINE_LOCATION = "Online"
SENTRALLAGER_LOCATION = "Livid Sentrallager"


def fetch_shopify_products() -> dict[str, list[str]]:
    """Fetch SALE_FW25 products from Shopify. Returns {title: [skus]}."""
    connector = ShopifyConnector(settings.get_connector_configs()["shopify"])
    products = {}
    cursor = None
    page = 0

    while True:
        page += 1
        after_clause = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
            products(first: 50, query: "tag:{SALE_TAG}"{after_clause}) {{
                edges {{
                    cursor
                    node {{
                        title
                        variants(first: 100) {{
                            edges {{
                                node {{
                                    sku
                                }}
                            }}
                        }}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                }}
            }}
        }}
        """
        result = connector._make_graphql_request(query)
        if "errors" in result and result["errors"]:
            print(f"  Shopify error: {result['errors']}")
            break

        data = result.get("data", {}).get("products", {})
        edges = data.get("edges", [])

        for edge in edges:
            node = edge["node"]
            title = node["title"]
            skus = [
                v["node"]["sku"].strip()
                for v in node.get("variants", {}).get("edges", [])
                if v["node"].get("sku", "").strip()
            ]
            if skus:
                products.setdefault(title, []).extend(skus)
            cursor = edge["cursor"]

        has_next = data.get("pageInfo", {}).get("hasNextPage", False)
        print(f"  Page {page}: {len(edges)} products ({len(products)} families)")
        if not has_next or not edges:
            break

    return products


def build_family_sku_map_from_sales(families: list[str]) -> dict[str, list[str]]:
    """
    Match planner family names to SKUs via sales_order_items.product_name.
    Only considers sales since SALE_START.
    Returns {family_name: [skus]}.
    """
    db = SessionLocal()
    try:
        q = text("""
            SELECT DISTINCT soi.product_name, soi.sku
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE so.order_date >= :sale_start
              AND soi.product_name = ANY(:families)
        """)
        result = db.execute(q, {"sale_start": SALE_START, "families": families})
        family_map = {}
        for row in result:
            family_map.setdefault(row.product_name, []).append(row.sku)
        return family_map
    finally:
        db.close()


def build_family_sku_map_from_category(families: list[str]) -> dict[str, list[str]]:
    """
    Match planner family names to SKUs via category_mappings.product_name.
    Strips common size suffixes (" - 32", " - OS", " - M", etc.) to match at family level.
    Returns {family_name: [skus]}.
    """
    db = SessionLocal()
    try:
        # Get all category_mappings product_name -> sku
        q = text("""
            SELECT product_name, sku FROM category_mappings
        """)
        result = db.execute(q)

        # Build a normalized lookup: strip trailing " - <size>" pattern
        # e.g. "Boston Taupe - 36" -> "Boston Taupe"
        # e.g. "Boston Taupe Suede - 40" -> "Boston Taupe Suede"
        size_pattern = re.compile(r'\s*-\s*[\w./]+$')

        cm_families = {}  # normalized_name -> [skus]
        for row in result:
            base = size_pattern.sub('', row.product_name).strip()
            cm_families.setdefault(base, []).append(row.sku)

        # Also try the raw product_name (for one-size products like perfumes)
        result2 = db.execute(text("SELECT product_name, sku FROM category_mappings"))
        for row in result2:
            cm_families.setdefault(row.product_name.strip(), []).append(row.sku)

        # Match against planner families
        families_lower = {f.lower().strip(): f for f in families}
        cm_lower = {k.lower(): v for k, v in cm_families.items()}

        family_map = {}
        for norm, orig in families_lower.items():
            if norm in cm_lower:
                # Deduplicate SKUs
                family_map[orig] = list(set(cm_lower[norm]))

        return family_map
    finally:
        db.close()


def query_data(all_skus: list[str]) -> tuple[dict, dict]:
    """Query sales and stock for SKUs."""
    db = SessionLocal()
    try:
        sales_q = text("""
            SELECT soi.sku, so.location, SUM(soi.quantity) AS qty
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE soi.sku = ANY(:skus)
              AND so.order_date >= :sale_start
              AND so.location IS NOT NULL
            GROUP BY soi.sku, so.location
        """)
        result = db.execute(sales_q, {"skus": all_skus, "sale_start": SALE_START})
        sales = {}
        for row in result:
            sales[(row.sku, row.location)] = int(row.qty)

        all_locs = PHYSICAL_STORES + [SENTRALLAGER_LOCATION]
        stock_q = text("""
            SELECT sku, location, available
            FROM raw.cin7_stock
            WHERE sku = ANY(:skus)
              AND location = ANY(:locations)
        """)
        result = db.execute(stock_q, {"skus": all_skus, "locations": all_locs})
        stock = {}
        for row in result:
            stock[(row.sku, row.location)] = int(row.available)

        return sales, stock
    finally:
        db.close()


def aggregate_family(skus: list[str], sales: dict, stock: dict) -> dict:
    """Aggregate sales and stock for a product family."""
    total_sold = 0
    butikk_stock = 0
    sentral_stock = 0

    for sku in skus:
        for loc in PHYSICAL_STORES + [ONLINE_LOCATION]:
            total_sold += sales.get((sku, loc), 0)
        for loc in PHYSICAL_STORES:
            s = stock.get((sku, loc), 0)
            butikk_stock += max(0, s)  # clamp negatives
        s = stock.get((sku, SENTRALLAGER_LOCATION), 0)
        sentral_stock += max(0, s)

    total_stock = butikk_stock + sentral_stock

    if total_sold + total_stock > 0:
        sell_through = total_sold / (total_sold + total_stock)
    else:
        sell_through = 0

    return {
        "total_sold": total_sold,
        "butikk_stock": butikk_stock,
        "sentral_stock": sentral_stock,
        "total_stock": total_stock,
        "sell_through": sell_through,
    }


def main():
    # --- Read planner ---
    print("Reading planner CSV...")
    with open(INPUT_PATH, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    print(f"  {len(rows)} rows, {len(header)} columns\n")

    planner_families = {}
    for i, row in enumerate(rows):
        family = row[2].strip()
        if family:
            planner_families[family] = i
    family_names = list(planner_families.keys())
    print(f"  {len(family_names)} unique product families\n")

    # --- Pass 1: Shopify SALE_FW25 tag ---
    print("Pass 1: Shopify SALE_FW25 tag...")
    shopify_products = fetch_shopify_products()

    matched = {}
    shopify_norm = {t.strip().lower(): t for t in shopify_products}
    for family in family_names:
        norm = family.strip().lower()
        if norm in shopify_norm:
            matched[family] = shopify_products[shopify_norm[norm]]

    remaining = [f for f in family_names if f not in matched]
    print(f"  Matched: {len(matched)}, remaining: {len(remaining)}\n")

    # --- Pass 2: sales_order_items.product_name ---
    print("Pass 2: Sales order product names...")
    sales_map = build_family_sku_map_from_sales(remaining)
    for family, skus in sales_map.items():
        if family in planner_families and family not in matched:
            matched[family] = skus

    remaining = [f for f in family_names if f not in matched]
    print(f"  Matched: {len(sales_map)}, total matched: {len(matched)}, remaining: {len(remaining)}\n")

    # --- Pass 3: category_mappings ---
    print("Pass 3: Category mappings...")
    cm_map = build_family_sku_map_from_category(remaining)
    for family, skus in cm_map.items():
        if family in planner_families and family not in matched:
            matched[family] = skus

    remaining = [f for f in family_names if f not in matched]
    print(f"  Matched: {len(cm_map)}, total matched: {len(matched)}, remaining: {len(remaining)}\n")

    if remaining:
        print(f"  Still unmatched (first 30):")
        for f in remaining[:30]:
            print(f"    - {f}")
        if len(remaining) > 30:
            print(f"    ... and {len(remaining) - 30} more")
        print()

    # --- Shopify-only (not in planner) ---
    planner_norm = {f.strip().lower() for f in family_names}
    shopify_only = [t for t in shopify_products if t.strip().lower() not in planner_norm]
    if shopify_only:
        print(f"  In Shopify SALE_FW25 but NOT in planner ({len(shopify_only)}):")
        for t in shopify_only[:20]:
            print(f"    - {t}")
        if len(shopify_only) > 20:
            print(f"    ... and {len(shopify_only) - 20} more")
        print()

    # --- Query fresh data ---
    all_skus = list(set(sku for skus in matched.values() for sku in skus))
    print(f"Querying DB for {len(all_skus)} unique SKUs...")
    sales, stock = query_data(all_skus)
    print(f"  {len(sales)} sale records, {len(stock)} stock records\n")

    # --- Update planner rows ---
    print("Updating planner rows...")
    updated_count = 0
    for family, skus in matched.items():
        row_idx = planner_families[family]
        row = rows[row_idx]
        agg = aggregate_family(skus, sales, stock)

        while len(row) < 17:
            row.append("")

        row[3] = str(agg["total_stock"])
        row[4] = str(agg["sentral_stock"]) if agg["sentral_stock"] else ""
        row[5] = str(agg["butikk_stock"])
        row[13] = f"{agg['sell_through']:.0%}" if (agg["total_sold"] + agg["total_stock"]) > 0 else ""
        row[15] = str(agg["butikk_stock"])
        row[16] = str(agg["total_sold"])

        rows[row_idx] = row
        updated_count += 1

    print(f"  {updated_count} / {len(family_names)} rows updated\n")

    # --- Write output ---
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Written to {OUTPUT_PATH}")

    # --- Summary ---
    total_sold_all = 0
    total_stock_all = 0
    for family, skus in matched.items():
        agg = aggregate_family(skus, sales, stock)
        total_sold_all += agg["total_sold"]
        total_stock_all += agg["total_stock"]

    print(f"\nOverall ({updated_count} matched products):")
    print(f"  Total sold: {total_sold_all}")
    print(f"  Total stock: {total_stock_all}")
    if total_sold_all + total_stock_all > 0:
        print(f"  Sell-through: {total_sold_all / (total_sold_all + total_stock_all):.1%}")


if __name__ == "__main__":
    main()
