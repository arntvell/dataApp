#!/usr/bin/env python3
"""
Set up category_group on category_mappings.

category_group rolls up related standard_categories into a single parent
for top-level reporting. Subcategory detail is preserved.

Edit CATEGORY_GROUPS below to adjust groupings, then re-run the script.
"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database.config import engine

# Map standard_category → category_group.
# Categories not listed here default to their own standard_category.
CATEGORY_GROUPS = {
    # Jersey — all jersey/top knit styles
    "Jersey":       "Jersey",
    "T-shirt":      "Jersey",
    "T-Shirt":      "Jersey",
    "Singlet":      "Jersey",
    "Sweatshirt":   "Jersey",

    # Knitwear — knitted tops and sweaters
    "Knitwear":     "Knitwear",
    "Sweater":      "Knitwear",

    # Outerwear — jackets, coats, vests
    "Jacket":       "Outerwear",
    "Coat":         "Outerwear",
    "Vest":         "Outerwear",

    # Bottoms — non-denim trousers and skirts (Jeans stays its own group)
    "Trouser":      "Bottoms",
    "Shorts":       "Bottoms",
    "Skirt":        "Bottoms",
    "Overall":      "Bottoms",
    "Suiting":      "Bottoms",
    "Dress":        "Dress",
    "Blouse":       "Blouse",
    "Top":          "Top",

    # Footwear — all shoes, boots, sandals, sneakers
    "Shoe Men":         "Footwear",
    "Shoe Women":       "Footwear",
    "Shoe Unisex":      "Footwear",
    "Sneaker Men":      "Footwear",
    "Sneaker Women":    "Footwear",
    "Sneaker Unisex":   "Footwear",
    "Sandal Men":       "Footwear",
    "Sandal Women":     "Footwear",
    "Sandal Unisex":    "Footwear",
    "Boot Men":         "Footwear",
    "Boot Women":       "Footwear",
    "Boots unisex":     "Footwear",

    # Accessories — bags, belts, hats, sunglasses, socks, etc.
    "Accessories":  "Accessories",
    "Hat":          "Accessories",
    "Gloves":       "Accessories",
    "Belt":         "Accessories",
    "Bag":          "Accessories",
    "Scarf":        "Accessories",
    "Sunglasses":   "Accessories",
    "Socks":        "Accessories",
    "Socks Unisex": "Accessories",
    "Underwear":    "Accessories",

    # Vintage — all vintage department categories
    "Vintage Blouse":       "Vintage",
    "Vintage Top":          "Vintage",
    "Vintage Kimono":       "Vintage",
    "Vintage Dress":        "Vintage",
    "Vintage Sweatshirt":   "Vintage",
    "Vintage Shirt":        "Vintage",
    "Vintage Knitwear":     "Vintage",
    "Vintage Jeans":        "Vintage",
    "Vintage Jacket":       "Vintage",
    "Vintage T-Shirt":      "Vintage",
    "Vintage Other":        "Vintage",
    "Vintage Coat":         "Vintage",
    "Vintage Scarf":        "Vintage",
    "Vintage Dress":        "Vintage",
    "Vintage Trouser":      "Vintage",
    "Vintage Skirt":        "Vintage",

    # Lifestyle — home goods, apothecary, food/drink
    "Home":         "Lifestyle",
    "Apothecary":   "Lifestyle",
    "Care":         "Lifestyle",
    "Books":        "Lifestyle",
    "Coffee":       "Lifestyle",
}


def run():
    with engine.begin() as conn:
        # Add column if missing
        conn.execute(text("""
            ALTER TABLE category_mappings
            ADD COLUMN IF NOT EXISTS category_group VARCHAR
        """))
        print("Column category_group ensured.")

        # Default: category_group = standard_category
        res = conn.execute(text("""
            UPDATE category_mappings
            SET category_group = standard_category
            WHERE standard_category IS NOT NULL
        """))
        print(f"Defaulted {res.rowcount:,} rows to standard_category.")

        # Apply overrides
        for std_cat, group in CATEGORY_GROUPS.items():
            res = conn.execute(text("""
                UPDATE category_mappings
                SET category_group = :group
                WHERE standard_category = :std_cat
            """), {"group": group, "std_cat": std_cat})
            if res.rowcount:
                print(f"  {std_cat} → {group}  ({res.rowcount:,} SKUs)")

        # Summary
        rows = conn.execute(text("""
            SELECT category_group, COUNT(*) as n
            FROM category_mappings
            WHERE category_group IS NOT NULL
            GROUP BY category_group
            ORDER BY n DESC
        """)).fetchall()
        print("\nCategory groups:")
        for r in rows:
            print(f"  {r.category_group:30s} {r.n:,} SKUs")


if __name__ == "__main__":
    run()
