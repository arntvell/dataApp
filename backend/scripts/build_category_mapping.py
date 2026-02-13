#!/usr/bin/env python3
"""
Build Category Mapping Table

This script creates a unified SKU → Category mapping by:
1. Using Shopify's productType for SKUs that exist in Shopify
2. Using keyword-based inference for Sitoo-only SKUs
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from database.config import DATABASE_URL, Base
from database.models import CategoryMapping

# Create engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Create the category_mappings table if it doesn't exist
Base.metadata.create_all(engine, tables=[CategoryMapping.__table__])


# ============== KEYWORD INFERENCE RULES ==============

# Priority order matters - first match wins
KEYWORD_RULES = [
    # Services (check first to catch LIV-SKREDDER, LIV-REPS)
    (r'skredder|tailor', 'Services'),
    (r'repara|repair', 'Services'),
    (r'gift\s*card|gavekort|gftcrd', 'Gift Cards'),
    
    # Jeans - specific patterns
    (r'\bjeans\b', 'Jeans'),
    (r'\blevis\b.*\b(blue|black|denim)\b', 'Jeans'),
    (r'\b(levis|levi\'s)\b', 'Jeans'),
    (r'japan\s*(black|blue|indigo|fade|dawn|rinse|dry)', 'Jeans'),
    (r'selvage|selvedge', 'Jeans'),
    (r'denim\s*(pant|trouser)', 'Jeans'),
    
    # Pants/Trousers (not jeans)
    (r'\b(chino|trouser|pant|slack)\b', 'Trouser'),
    (r'dickies\s*pant|carpenter\s*pant', 'Trouser'),
    (r'carhartt\s*pant', 'Trouser'),
    
    # Shirts
    (r'oxford\s*shirt', 'Shirt'),
    (r'cord\s*shirt|corduroy\s*shirt', 'Shirt'),
    (r'flannel\s*shirt', 'Shirt'),
    (r'denim\s*shirt', 'Shirt'),
    (r'button.*shirt|shirt.*button', 'Shirt'),
    (r'hawaiian\s*shirt', 'Shirt'),
    (r'rugby\s*shirt', 'Shirt'),
    (r'stripe\s*shirt', 'Shirt'),
    (r'work\s*shirt', 'Shirt'),
    (r'\bshirt\b(?!.*dress)', 'Shirt'),
    
    # Blouses (before general shirt)
    (r'\bblouse\b', 'Blouse'),
    
    # Dresses
    (r'\bdress\b', 'Dress'),
    
    # Skirts
    (r'\bskirt\b', 'Skirt'),
    
    # Outerwear - Jackets
    (r'barbour\s*jacket', 'Jacket'),
    (r'worker\s*jacket|work\s*jacket', 'Jacket'),
    (r'denim\s*jacket', 'Jacket'),
    (r'harrington', 'Jacket'),
    (r'\bjacket\b', 'Jacket'),
    
    # Outerwear - Coats
    (r'trench\s*coat', 'Coat'),
    (r'wool\s*coat', 'Coat'),
    (r'sheepskin', 'Coat'),
    (r'\bcoat\b', 'Coat'),
    
    # Knitwear
    (r'jaquard\s*knit|jacquard\s*knit', 'Knitwear'),
    (r'norwegian\s*knit', 'Knitwear'),
    (r'traditional\s*knit', 'Knitwear'),
    (r'\bknit\b|\bknitwear\b', 'Knitwear'),
    (r'\bsweater\b|\bjumper\b', 'Knitwear'),
    (r'\bcardigan\b', 'Knitwear'),
    (r'college(?!\s*shirt)', 'Knitwear'),  # College sweater, not college shirt
    
    # T-shirts and tops
    (r'\bt-shirt\b|\btee\b', 'T-Shirt'),
    (r'\bsinglet\b|\btank\b', 'Singlet'),
    (r'quarter\s*zip', 'Knitwear'),
    
    # Accessories - Scarves
    (r'silk\s*scarf', 'Scarf'),
    (r'\bscarf\b', 'Scarf'),
    
    # Accessories - Hats
    (r'\bbeanie\b', 'Beanie'),
    (r'\bcap\b', 'Cap'),
    (r'\bhat\b', 'Hat'),
    
    # Footwear
    (r'\bboot\b', 'Boots'),
    (r'\bsneaker\b', 'Sneakers'),
    (r'\bshoe\b', 'Shoes'),
    (r'\bsandal\b', 'Sandals'),
    
    # Sunglasses
    (r'sunglasses|aviator', 'Sunglasses'),
    
    # Fragrances
    (r'eau\s*de\s*parfum|parfum|fragrance|edp', 'Fragrance'),
    
    # Socks
    (r'\bsocks?\b', 'Socks'),
    
    # Care products
    (r'boot\s*cream|leather\s*care|conditioner|polish', 'Care'),
    (r'candle', 'Home'),
    
    # Bags
    (r'\bbag\b|\btote\b|\bbackpack\b', 'Bags'),
    
    # Belts
    (r'\bbelt\b', 'Belt'),
    
    # Vintage catch-all (check product patterns)
    (r'vintage|secondhand', 'Vintage'),
]


def infer_category(product_name: str, sku: str) -> tuple[str, float]:
    """
    Infer category from product name using keyword rules.
    Returns (category, confidence)
    """
    if not product_name:
        return 'Uncategorized', 0.5
    
    name_lower = product_name.lower()
    sku_lower = sku.lower() if sku else ''
    
    # Check SKU patterns first for vintage
    if sku_lower.startswith('ext-vn-') or sku_lower.startswith('ext-vin-') or sku_lower.startswith('vn-') or sku_lower.startswith('vin-'):
        # Still try to categorize by product type, but mark as vintage confidence
        for pattern, category in KEYWORD_RULES:
            if re.search(pattern, name_lower, re.IGNORECASE):
                return f"Vintage {category}" if category not in ['Services', 'Gift Cards'] else category, 0.7
        return 'Vintage Other', 0.6
    
    # Apply keyword rules
    for pattern, category in KEYWORD_RULES:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return category, 0.8
    
    return 'Uncategorized', 0.5


def build_mapping():
    """Build the category mapping table"""
    session = Session()
    
    try:
        # Clear existing mappings
        session.execute(text("DELETE FROM category_mappings"))
        session.commit()
        print("Cleared existing mappings")
        
        # Step 1: Get Shopify categories (highest priority)
        print("\n📦 Step 1: Loading Shopify categories...")
        shopify_categories = session.execute(text("""
            SELECT DISTINCT 
                soi.sku, 
                soi.product_category,
                soi.product_name
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE so.source_system = 'shopify' 
              AND soi.sku IS NOT NULL 
              AND soi.sku != ''
              AND soi.product_category IS NOT NULL
              AND soi.product_category != ''
        """)).fetchall()
        
        shopify_mapped = {}
        for sku, category, name in shopify_categories:
            if sku not in shopify_mapped:
                shopify_mapped[sku] = (category, name)
        
        print(f"   Found {len(shopify_mapped)} unique SKUs in Shopify")
        
        # Step 2: Get all Sitoo SKUs
        print("\n🏪 Step 2: Loading Sitoo SKUs...")
        sitoo_skus = session.execute(text("""
            SELECT DISTINCT 
                soi.sku, 
                soi.product_name
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE so.source_system = 'sitoo' 
              AND soi.sku IS NOT NULL 
              AND soi.sku != ''
        """)).fetchall()
        
        sitoo_only = {}
        for sku, name in sitoo_skus:
            if sku not in shopify_mapped:
                sitoo_only[sku] = name
        
        print(f"   Found {len(sitoo_skus)} unique SKUs in Sitoo")
        print(f"   {len(sitoo_only)} are Sitoo-only (not in Shopify)")
        
        # Step 3: Create mappings
        print("\n🔗 Step 3: Creating category mappings...")
        
        mappings = []
        
        # Shopify mappings (confidence = 1.0)
        for sku, (category, name) in shopify_mapped.items():
            mappings.append(CategoryMapping(
                sku=sku,
                original_category=category,
                product_name=name,
                standard_category=category,
                mapping_source='shopify',
                confidence=1.0
            ))
        
        # Sitoo-only mappings (keyword inference)
        for sku, name in sitoo_only.items():
            inferred_category, confidence = infer_category(name, sku)
            mappings.append(CategoryMapping(
                sku=sku,
                original_category='Standard',
                product_name=name,
                standard_category=inferred_category,
                mapping_source='keyword_inference',
                confidence=confidence
            ))
        
        # Bulk insert
        session.bulk_save_objects(mappings)
        session.commit()
        
        print(f"   Created {len(mappings)} category mappings")
        
        # Step 4: Summary
        print("\n" + "="*70)
        print("CATEGORY MAPPING SUMMARY")
        print("="*70)
        
        summary = session.execute(text("""
            SELECT 
                mapping_source,
                COUNT(*) as count,
                AVG(confidence) as avg_confidence
            FROM category_mappings
            GROUP BY mapping_source
        """)).fetchall()
        
        for source, count, confidence in summary:
            print(f"   {source}: {count} SKUs (avg confidence: {confidence:.2f})")
        
        print("\n" + "="*70)
        print("CATEGORY DISTRIBUTION")
        print("="*70)
        
        categories = session.execute(text("""
            SELECT 
                standard_category,
                COUNT(*) as count,
                mapping_source
            FROM category_mappings
            GROUP BY standard_category, mapping_source
            ORDER BY count DESC
            LIMIT 30
        """)).fetchall()
        
        for cat, count, source in categories:
            print(f"   {cat}: {count} ({source})")
        
        return len(mappings)
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    print("="*70)
    print("BUILDING CATEGORY MAPPING TABLE")
    print("="*70)
    
    total = build_mapping()
    
    print("\n" + "="*70)
    print(f"✅ Complete! Created {total} category mappings")
    print("="*70)
