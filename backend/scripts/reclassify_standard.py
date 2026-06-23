"""
Surgical reclassification of category_mappings rows stuck at standard_category='Standard'.

These are (mostly Sitoo) products that were never categorized at source and never
appeared in Shopify, so the Shopify-authoritative sync left them as 'Standard'.

Strategy (first match wins), touching ONLY 'Standard' rows — never the
17k+ Shopify-sourced mappings:
  1. SKU prefix rules           (reused from rebuild_category_mapping.SKU_PREFIX_RULES)
  2. Parent-SKU inheritance     (mode of categorized sibling variants)
  3. Product-name keyword rules (reused from rebuild_category_mapping.NAME_KEYWORD_RULES)
  4. Denim SKU size pattern     (size_type='denim' or 4-digit size tail) -> Jeans

Usage:
  DATABASE_URL=... python scripts/reclassify_standard.py [--apply]
Default is a DRY RUN (prints projected changes, writes nothing).
"""
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from database.config import DATABASE_URL
from data.category_groups import CATEGORY_GROUPS
from scripts.rebuild_category_mapping import (
    get_category_from_sku_prefix,
    get_category_from_name,
)

SKIP = {"Standard", "Unknown", "Uncategorized", "", None}


def resolve(sku, name, parent, sibling_mode, size_type):
    """Return (category, source, confidence) or (None, ...) if unresolved."""
    cat = get_category_from_sku_prefix(sku)
    if cat:
        return cat, "sku_prefix", 0.95

    if parent and sibling_mode.get(parent):
        return sibling_mode[parent], "parent_sku_inheritance", 0.9

    cat = get_category_from_name(name)
    if cat:
        return cat, "keyword_inference", 0.85

    # Denim size pattern: explicit denim size_type, or a 4-digit waist/length tail
    tail = (sku or "").rsplit("-", 1)[-1]
    if size_type == "denim" or (tail.isdigit() and len(tail) == 4):
        return "Jeans", "sku_prefix", 0.8

    return None, None, None


def main(apply: bool):
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        std = conn.execute(text(
            "SELECT sku, product_name FROM category_mappings WHERE standard_category='Standard'"
        )).fetchall()

        parent = dict(conn.execute(text(
            "SELECT sku, parent_sku FROM parent_sku_mappings"
        )).fetchall())
        size_type = dict(conn.execute(text(
            "SELECT sku, size_type FROM parent_sku_mappings"
        )).fetchall())

        # Mode of categorized sibling categories per parent
        sib_counts = defaultdict(Counter)
        rows = conn.execute(text("""
            SELECT ps.parent_sku, cm.standard_category, COUNT(*)
            FROM parent_sku_mappings ps
            JOIN category_mappings cm ON cm.sku = ps.sku
            WHERE cm.standard_category IS NOT NULL AND cm.standard_category <> 'Standard'
            GROUP BY 1, 2
        """)).fetchall()
        for par, cat, n in rows:
            sib_counts[par][cat] += n
        sibling_mode = {p: c.most_common(1)[0][0] for p, c in sib_counts.items()}

        by_method = Counter()
        new_group = Counter()
        updates = []
        unresolved = []
        for sku, name in std:
            cat, src, conf = resolve(sku, name, parent.get(sku), sibling_mode, size_type.get(sku))
            if not cat:
                unresolved.append((sku, name))
                continue
            grp = CATEGORY_GROUPS.get(cat, cat)
            by_method[src] += 1
            new_group[grp] += 1
            updates.append((sku, cat, grp, src, conf))

        print(f"\n'Standard' rows: {len(std)}")
        print(f"Resolved: {len(updates)}   Unresolved (stay 'Standard'): {len(unresolved)}")
        print("\nBy method:")
        for m, n in by_method.most_common():
            print(f"  {m:24} {n}")
        print("\nNew category_group distribution for resolved rows:")
        for g, n in new_group.most_common():
            print(f"  {g:18} {n}")
        if unresolved:
            print(f"\nLeft as 'Standard' ({len(unresolved)}):")
            for sku, name in unresolved[:30]:
                print(f"  {str(sku):26} {name}")

        if apply:
            for sku, cat, grp, src, conf in updates:
                conn.execute(text("""
                    UPDATE category_mappings
                    SET standard_category=:cat, category_group=:grp,
                        mapping_source=:src, confidence=:conf
                    WHERE sku=:sku
                """), {"cat": cat, "grp": grp, "src": src, "conf": conf, "sku": sku})
            print(f"\n✅ APPLIED {len(updates)} updates to {DATABASE_URL.split('@')[-1]}")
        else:
            print("\n(DRY RUN — nothing written. Re-run with --apply to persist.)")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
