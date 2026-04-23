"""
Generate sale_fw25_variants.csv from fresh data.

1. Fetches SKUs for products tagged SALE_FW25 from Shopify GraphQL
2. Queries sales from DB (since Dec 27, 2025)
3. Queries current stock from raw.cin7_stock (available field)
4. Outputs /tmp/sale_fw25_variants.csv

Usage:
    docker compose exec backend python scripts/generate_sale_fw25_csv.py
"""

import csv
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from connectors.shopify_connector import ShopifyConnector
from database.config import SessionLocal
from sqlalchemy import text

OUTPUT_PATH = "/tmp/sale_fw25_variants.csv"
SALE_TAG = "SALE_FW25"
SALE_START = "2025-12-27"

STORES = [
    "Livid Oslo",
    "Livid Trondheim",
    "Livid Bergen",
    "Livid Stavanger",
    "Past Løkka",
]

ALL_LOCATIONS = STORES + ["Online"]


def fetch_sale_skus_from_shopify() -> dict[str, dict]:
    """
    Fetch all products tagged SALE_FW25 from Shopify.
    Returns {sku: {"product_title": ..., "variant_title": ...}} for all variants.
    """
    connector = ShopifyConnector(settings.get_connector_configs()["shopify"])

    sku_map = {}
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
                                    title
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
            print(f"  Shopify API error: {result['errors']}")
            break

        products = result.get("data", {}).get("products", {})
        edges = products.get("edges", [])

        for edge in edges:
            node = edge["node"]
            product_title = node["title"]
            for var_edge in node.get("variants", {}).get("edges", []):
                var = var_edge["node"]
                sku = var.get("sku", "").strip()
                if sku:
                    sku_map[sku] = {
                        "product_title": product_title,
                        "variant_title": var.get("title", ""),
                    }
            cursor = edge["cursor"]

        has_next = products.get("pageInfo", {}).get("hasNextPage", False)
        print(f"  Page {page}: {len(edges)} products fetched, {len(sku_map)} SKUs total")

        if not has_next or not edges:
            break

    return sku_map


def query_sales_and_stock(skus: list[str]) -> list[dict]:
    """
    Query sales (since SALE_START) and current stock for the given SKUs.
    Returns one row per SKU with sold/stock per location.
    """
    db = SessionLocal()
    try:
        # Sales per SKU per location
        sales_query = text("""
            SELECT
                soi.sku,
                so.location,
                SUM(soi.quantity) AS qty_sold
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE soi.sku = ANY(:skus)
              AND so.order_date >= :sale_start
              AND so.location IS NOT NULL
            GROUP BY soi.sku, so.location
        """)
        sales_result = db.execute(sales_query, {"skus": skus, "sale_start": SALE_START})
        sales_data = {}
        for row in sales_result:
            key = (row.sku, row.location)
            sales_data[key] = int(row.qty_sold)

        # Stock per SKU per location (available = on_hand - allocated)
        stock_query = text("""
            SELECT sku, location, available
            FROM raw.cin7_stock
            WHERE sku = ANY(:skus)
              AND location = ANY(:locations)
        """)
        stock_result = db.execute(stock_query, {"skus": skus, "locations": ALL_LOCATIONS})
        stock_data = {}
        for row in stock_result:
            key = (row.sku, row.location)
            stock_data[key] = int(row.available)

        return sales_data, stock_data
    finally:
        db.close()


def build_csv(sku_map: dict, sales_data: dict, stock_data: dict):
    """Build and write the CSV."""
    # Get product name + size from parent_sku_mappings where possible
    db = SessionLocal()
    try:
        psm_query = text("""
            SELECT sku, base_product_name, size_code
            FROM parent_sku_mappings
            WHERE sku = ANY(:skus)
        """)
        psm_result = db.execute(psm_query, {"skus": list(sku_map.keys())})
        psm_map = {}
        for row in psm_result:
            psm_map[row.sku] = {
                "product_name": row.base_product_name,
                "size_code": row.size_code,
            }
    finally:
        db.close()

    # Build rows
    rows = []
    for sku, shopify_info in sku_map.items():
        psm = psm_map.get(sku, {})
        product_name = psm.get("product_name") or shopify_info["product_title"]
        size = psm.get("size_code") or shopify_info["variant_title"]

        row = {
            "Product": product_name,
            "Size/Variant": size,
            "SKU": sku,
        }

        total_sold = 0
        total_stock = 0
        for loc in ALL_LOCATIONS:
            sold = sales_data.get((sku, loc), 0)
            stock = stock_data.get((sku, loc), 0)
            row[f"{loc} Sold"] = sold
            row[f"{loc} Stock"] = stock
            total_sold += sold
            total_stock += stock

        row["Total Sold"] = total_sold
        row["Total Stock"] = total_stock
        rows.append(row)

    # Sort by product name then size
    rows.sort(key=lambda r: (r["Product"], str(r["Size/Variant"])))

    # Write CSV
    fieldnames = ["Product", "Size/Variant", "SKU"]
    for loc in ALL_LOCATIONS:
        fieldnames.extend([f"{loc} Sold", f"{loc} Stock"])
    # Interleave: all Sold columns first per location, then stock? No — match original format
    # Original format: {loc} Sold for all locs, then {loc} Stock for all locs? Let me check...
    # Actually original is: Oslo Sold, Trondheim Sold, Bergen Sold, Stavanger Sold, Past Sold, Online Sold, Total Sold, Oslo Stock, ...
    fieldnames = (
        ["Product", "Size/Variant", "SKU"]
        + [f"{loc} Sold" for loc in ALL_LOCATIONS]
        + ["Total Sold"]
        + [f"{loc} Stock" for loc in ALL_LOCATIONS]
        + ["Total Stock"]
    )

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    return rows


def main():
    print("Step 1: Fetching SALE_FW25 SKUs from Shopify...")
    sku_map = fetch_sale_skus_from_shopify()
    print(f"  {len(sku_map)} variant SKUs found\n")

    if not sku_map:
        print("ERROR: No SKUs found. Check Shopify tag.")
        return

    skus = list(sku_map.keys())

    print("Step 2: Querying sales and stock from DB...")
    sales_data, stock_data = query_sales_and_stock(skus)
    print(f"  {len(sales_data)} sale records, {len(stock_data)} stock records\n")

    print("Step 3: Building CSV...")
    rows = build_csv(sku_map, sales_data, stock_data)
    print(f"  {len(rows)} variant rows written to {OUTPUT_PATH}\n")

    # Quick summary
    phys_sold = sum(
        sales_data.get((sku, loc), 0)
        for sku in skus
        for loc in STORES
    )
    phys_stock = sum(
        stock_data.get((sku, loc), 0)
        for sku in skus
        for loc in STORES
    )
    print(f"Summary (physical stores):")
    print(f"  Total sold: {phys_sold}")
    print(f"  Total stock: {phys_stock}")
    print(f"  Sell-through: {phys_sold / (phys_sold + phys_stock) * 100:.1f}%")


if __name__ == "__main__":
    main()
