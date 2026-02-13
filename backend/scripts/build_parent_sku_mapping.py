#!/usr/bin/env python3
"""
Build Parent SKU Mapping Table

Maps variant SKUs to parent (base) SKUs for product-level aggregation.
Extracts size information from SKU suffixes.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
from collections import defaultdict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from database.config import DATABASE_URL, Base
from database.models import ParentSkuMapping

# Create engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Create the table if it doesn't exist
Base.metadata.create_all(engine, tables=[ParentSkuMapping.__table__])


# ============== SIZE EXTRACTION PATTERNS ==============

# Order matters - more specific patterns first
SIZE_PATTERNS = [
    # Denim sizes (4 digits: waist + length)
    (r'^(.+)-(\d{4})$', 'denim'),           # LIV-KR-JPN-BLCK-3132
    (r'^(.+)-(\d{2}/\d{2})$', 'denim'),     # Some use slash format
    
    # Letter sizes (including extended)
    (r'^(.+)-(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL|XXXL)$', 'letter'),
    
    # One Size
    (r'^(.+)-(OS)$', 'one_size'),
    
    # Numeric sizes (shoe sizes, etc) - 1-2 digits, possibly with decimal
    (r'^(.+)-(\d{1,2}(?:\.\d)?)$', 'numeric'),
    
    # Special cases - two-part sizes like "31-32" or "32 / 34"
    (r'^(.+)-(\d{2}-\d{2})$', 'denim'),
]


def extract_parent_and_size(sku: str) -> tuple[str, str, str]:
    """
    Extract parent SKU and size from variant SKU.
    Returns (parent_sku, size_code, size_type)
    """
    if not sku:
        return sku, None, None
    
    # Try each pattern
    for pattern, size_type in SIZE_PATTERNS:
        match = re.match(pattern, sku, re.IGNORECASE)
        if match:
            parent_sku = match.group(1)
            size_code = match.group(2).upper()
            return parent_sku, size_code, size_type
    
    # No size pattern found - SKU is its own parent
    return sku, None, None


def extract_base_product_name(product_name: str) -> str:
    """
    Remove size information from product name.
    """
    if not product_name:
        return product_name
    
    # Patterns to remove from end of product name
    name_patterns = [
        r'\s*-?\s*\d{2}\s*/\s*\d{2}$',      # " - 31 / 32" or "31/32"
        r'\s*-?\s*\d{4}$',                   # " - 3132" or "3132"
        r',?\s*(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL)$',  # ", M" or " M"
        r'\s*-\s*(XXS|XS|S|M|L|XL|XXL|2XL|3XL|4XL)$',
        r',?\s*OS$',                         # ", OS" or " OS"
        r'\s*-\s*OS$',
        r',?\s*\d{1,2}(?:\.\d)?$',           # ", 42" (shoe size)
    ]
    
    name = product_name.strip()
    for pattern in name_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE).strip()
    
    return name


def build_mapping():
    """Build the parent SKU mapping table"""
    session = Session()
    
    try:
        # Clear existing mappings
        session.execute(text("DELETE FROM parent_sku_mappings"))
        session.commit()
        print("Cleared existing mappings")
        
        # Get all SKUs with product names
        print("\n📦 Loading all SKUs...")
        results = session.execute(text("""
            SELECT DISTINCT sku, product_name
            FROM category_mappings
            WHERE sku IS NOT NULL AND sku != ''
        """)).fetchall()
        
        print(f"   Found {len(results)} unique SKUs")
        
        # Process each SKU
        print("\n🔗 Extracting parent SKUs...")
        mappings = []
        parent_counts = defaultdict(int)
        
        for sku, product_name in results:
            parent_sku, size_code, size_type = extract_parent_and_size(sku)
            base_name = extract_base_product_name(product_name) if product_name else None
            
            mappings.append(ParentSkuMapping(
                sku=sku,
                parent_sku=parent_sku,
                size_code=size_code,
                size_type=size_type,
                product_name=product_name,
                base_product_name=base_name,
                variant_count=1  # Will update below
            ))
            
            parent_counts[parent_sku] += 1
        
        # Bulk insert
        session.bulk_save_objects(mappings)
        session.commit()
        
        # Update variant counts
        print("   Updating variant counts...")
        for parent_sku, count in parent_counts.items():
            session.execute(
                text("UPDATE parent_sku_mappings SET variant_count = :count WHERE parent_sku = :parent"),
                {"count": count, "parent": parent_sku}
            )
        session.commit()
        
        print(f"   Created {len(mappings)} mappings")
        
        # Summary statistics
        print("\n" + "="*70)
        print("PARENT SKU MAPPING SUMMARY")
        print("="*70)
        
        stats = session.execute(text("""
            SELECT 
                size_type,
                COUNT(*) as sku_count,
                COUNT(DISTINCT parent_sku) as unique_parents
            FROM parent_sku_mappings
            GROUP BY size_type
            ORDER BY sku_count DESC
        """)).fetchall()
        
        print("\nBy Size Type:")
        for size_type, count, parents in stats:
            type_name = size_type if size_type else "No size (single variant)"
            print(f"   {type_name:25} {count:6,} SKUs → {parents:,} parent products")
        
        # Count parent products with multiple variants
        multi_variant = session.execute(text("""
            SELECT COUNT(DISTINCT parent_sku)
            FROM parent_sku_mappings
            WHERE parent_sku IN (
                SELECT parent_sku 
                FROM parent_sku_mappings 
                GROUP BY parent_sku 
                HAVING COUNT(*) > 1
            )
        """)).scalar()
        
        single_variant = session.execute(text("""
            SELECT COUNT(DISTINCT parent_sku)
            FROM parent_sku_mappings
            WHERE parent_sku IN (
                SELECT parent_sku 
                FROM parent_sku_mappings 
                GROUP BY parent_sku 
                HAVING COUNT(*) = 1
            )
        """)).scalar()
        
        total_parents = session.execute(text("""
            SELECT COUNT(DISTINCT parent_sku) FROM parent_sku_mappings
        """)).scalar()
        
        print(f"\nParent Products:")
        print(f"   Total unique parent SKUs: {total_parents:,}")
        print(f"   With multiple variants:   {multi_variant:,}")
        print(f"   Single variant only:      {single_variant:,}")
        
        # Show example families
        print("\n" + "="*70)
        print("EXAMPLE PRODUCT FAMILIES (Top by variant count)")
        print("="*70)
        
        families = session.execute(text("""
            SELECT parent_sku, base_product_name, variant_count
            FROM parent_sku_mappings
            WHERE variant_count > 1
            GROUP BY parent_sku, base_product_name, variant_count
            ORDER BY variant_count DESC
            LIMIT 15
        """)).fetchall()
        
        for parent, name, count in families:
            print(f"   {parent:40} ({count} variants) - {name}")
        
        return len(mappings), total_parents
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    print("="*70)
    print("BUILDING PARENT SKU MAPPING TABLE")
    print("="*70)
    
    total_skus, total_parents = build_mapping()
    
    print("\n" + "="*70)
    print(f"✅ Complete!")
    print(f"   {total_skus:,} variant SKUs → {total_parents:,} parent products")
    print("="*70)
