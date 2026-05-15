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
from data.category_groups import CATEGORY_GROUPS


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
