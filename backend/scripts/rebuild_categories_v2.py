"""
Category Mapping Rebuild V2

New logic:
1. Shopify productType → Source of truth for online sales → Standardize
2. Sitoo categories → Source of truth for in-store sales → Standardize  
3. Prefix rules → Fallback for vintage and items not in respective systems
"""

import json
import re
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import DATABASE_URL
from data.category_standardization import standardize_category, CATEGORY_MAP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# === PREFIX RULES FOR FALLBACK ===
# Used when item doesn't exist in Sitoo catalog or Shopify doesn't have productType

SKU_PREFIX_RULES = {
    # Livid Jeans
    ('LIV', 'BRNS'): 'Jeans', ('LIV', 'BTH'): 'Jeans', ('LIV', 'BLY'): 'Jeans',
    ('LIV', 'JNE'): 'Jeans', ('LIV', 'KRI'): 'Jeans', ('LIV', 'ISL'): 'Jeans',
    ('LIV', 'AVA'): 'Jeans', ('LIV', 'ELL'): 'Jeans', ('LIV', 'EVRD'): 'Jeans',
    ('LIV', 'EDVRD'): 'Jeans', ('LIV', 'EDV'): 'Jeans', ('LIV', 'RLD'): 'Jeans',
    ('LIV', 'TUE'): 'Jeans', ('LIV', 'PG'): 'Jeans', ('LIV', 'MK'): 'Jeans',
    ('LIV', 'FUL'): 'Jeans', ('LIV', 'FLLR'): 'Jeans', ('LIV', 'HYS'): 'Jeans',
    ('LIV', 'K'): 'Jeans', ('LIV', 'JN'): 'Jeans', ('LIV', 'T'): 'Jeans',
    ('LIV', 'VR'): 'Jeans', ('LIV', 'WMN'): 'Jeans', ('LIV', 'ADS'): 'Jeans',
    ('LIV', 'ADDSN'): 'Jeans', ('LIV', 'EMRSN'): 'Jeans',
    
    # Livid Shirts
    ('LIV', 'KN'): 'Shirt', ('LIV', 'HF'): 'Shirt', ('LIV', 'AID'): 'Shirt',
    ('LIV', 'ANT'): 'Shirt', ('LIV', 'STN'): 'Shirt', ('LIV', 'WST'): 'Shirt',
    ('LIV', 'TK'): 'Shirt', ('LIV', 'HL'): 'Shirt', ('LIV', 'INTL'): 'Shirt',
    ('LIV', 'CMPT'): 'Shirt', ('LIV', 'SHN'): 'Shirt', ('LIV', 'SRN'): 'Shirt',
    
    # Livid T-Shirts
    ('LIV', 'NLSN'): 'T-Shirt', ('LIV', 'NLS'): 'T-Shirt',
    ('LIV', 'RCHMND'): 'T-Shirt', ('LIV', 'N'): 'T-Shirt',
    
    # Livid Knitwear
    ('LIV', 'FM'): 'Knitwear', ('LIV', 'KLMR'): 'Knitwear',
    ('LIV', 'MRCR'): 'Knitwear', ('LIV', 'LRK'): 'Knitwear',
    ('LIV', 'PLTH'): 'Knitwear', ('LIV', 'WLM'): 'Knitwear',
    ('LIV', 'ML'): 'Knitwear', ('LIV', 'OLY'): 'Knitwear',
    
    # Livid Trousers
    ('LIV', 'JP'): 'Trouser', ('LIV', 'KR'): 'Trouser',
    ('LIV', 'NRMN'): 'Trouser', ('LIV', 'RGNR'): 'Trouser',
    ('LIV', 'CSDY'): 'Trouser', ('LIV', 'CNLY'): 'Trouser',
    ('LIV', 'WCSTR'): 'Trouser', ('LIV', 'SLGSV'): 'Trouser',
    
    # Livid Jackets
    ('LIV', 'BWSR'): 'Jacket', ('LIV', 'CRMY'): 'Jacket',
    ('LIV', 'KVN'): 'Jacket', ('LIV', 'NLN'): 'Jacket',
    ('LIV', 'NSH'): 'Jacket', ('LIV', 'HM'): 'Jacket',
    ('LIV', 'RLY'): 'Jacket', ('LIV', 'JSH'): 'Jacket',
    ('LIV', 'JKOB'): 'Jacket', ('LIV', 'FLY'): 'Jacket', ('LIV', 'WLY'): 'Jacket',
    
    # Livid Dresses
    ('LIV', 'DPHN'): 'Dress', ('LIV', 'PYTN'): 'Dress',
    ('LIV', 'CRYSTL'): 'Dress', ('LIV', 'Lou'): 'Dress',
    ('LIV', 'Joelle'): 'Dress', ('LIV', 'Faye'): 'Dress',
    
    # External Footwear
    ('EXT', 'RW'): 'Boot Men', ('EXT', 'PR'): 'Boot Women',
    ('EXT', 'PB'): 'Shoe Men', ('EXT', 'DM'): 'Shoe Men',
    ('EXT', 'NRD'): 'Shoe Men', ('EXT', 'ZDA'): 'Shoe Men',
    ('EXT', 'MCHL'): 'Shoe Men', ('EXT', 'BT'): 'Shoe Men',
    ('EXT', 'CMPLB'): 'Shoe Men', ('EXT', 'CMP'): 'Shoe Men',
    ('EXT', 'KEEN'): 'Sneaker Unisex', ('EXT', 'BKST'): 'Sandal Unisex',
    ('EXT', 'BS'): 'Sandal Unisex', ('EXRT', 'ASC'): 'Sneaker Unisex',
    
    # External Accessories
    ('EXT', 'HST'): 'Gloves', ('EXT', 'WLW'): 'Sunglasses',
    ('EXT', 'STS'): 'Hat', ('EXT', 'PEN'): 'Accessories',
    ('EXT', 'NULL'): 'Accessories',
    
    # External Socks
    ('EXT', 'RT'): 'Socks', ('EXT', 'PNT'): 'Socks', ('EXT', 'ANY'): 'Socks',
    
    # External Home
    ('EXT', 'PF'): 'Home',
    
    # Vintage
    ('VN', 'ONLN'): None,  # Use keyword inference
    ('EXT', 'VN'): 'Vintage Other',
    
    # Samples
    ('LIV', 'SMPLS'): 'Sample', ('LIV', 'MSCSMPL'): 'Sample',
}

# Keyword patterns for vintage items
KEYWORD_RULES = [
    (r'\b(jean|denim|levi)', 'Vintage Jeans'),
    (r'\b(jacket|coat|parka|bomber|blazer|anorak|windbreaker)', 'Vintage Jacket'),
    (r'\b(sweat|hoodie|hoody|fleece)', 'Vintage Sweatshirt'),
    (r'\b(knit|sweater|cardigan|wool)', 'Vintage Knitwear'),
    (r'\b(shirt|flannel|oxford|rugby)', 'Vintage Shirt'),
    (r'\bt-?shirt|tee\b', 'Vintage T-Shirt'),
]


def get_category_from_prefix(sku: str) -> str | None:
    """Get category from SKU prefix rules"""
    if not sku:
        return None
    parts = sku.upper().split('-')
    if len(parts) >= 2:
        key = (parts[0], parts[1])
        if key in SKU_PREFIX_RULES:
            return SKU_PREFIX_RULES[key]
    return None


def get_category_from_keywords(product_name: str) -> str | None:
    """Get category from product name keywords (for vintage)"""
    if not product_name:
        return None
    name_lower = product_name.lower()
    for pattern, category in KEYWORD_RULES:
        if re.search(pattern, name_lower):
            return category
    return 'Vintage Other'


def rebuild_category_mappings():
    """Rebuild all category mappings with new logic"""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # === STEP 1: Load Sitoo product categories ===
        logger.info("Loading Sitoo product categories...")
        try:
            with open('/app/sitoo_product_categories.json', 'r') as f:
                sitoo_data = json.load(f)
            sitoo_categories = sitoo_data['categories']
            sitoo_products = {p['sku']: p.get('categories', []) for p in sitoo_data['products'] if p.get('sku')}
            logger.info(f"Loaded {len(sitoo_products)} Sitoo products")
        except FileNotFoundError:
            logger.warning("Sitoo categories file not found, will use fallback rules")
            sitoo_categories = {}
            sitoo_products = {}
        
        # === STEP 2: Get all unique SKUs from sales data ===
        logger.info("Fetching all SKUs from sales data...")
        result = session.execute(text("""
            SELECT DISTINCT 
                soi.sku,
                soi.product_name,
                soi.product_category as shopify_category,
                so.source_system
            FROM sales_order_items soi
            JOIN sales_orders so ON soi.order_id = so.id
            WHERE soi.sku IS NOT NULL AND soi.sku != ''
        """))
        
        all_items = result.fetchall()
        logger.info(f"Found {len(all_items)} unique SKU/source combinations")
        
        # === STEP 3: Build category mappings ===
        logger.info("Building category mappings...")
        
        # Track best category for each SKU
        sku_mappings = {}  # sku -> {category, source, confidence, product_name, original_category}
        
        for row in all_items:
            sku, product_name, shopify_category, source_system = row
            
            # Skip if already have a higher-confidence mapping
            if sku in sku_mappings and sku_mappings[sku]['confidence'] >= 1.0:
                continue
            
            category = None
            mapping_source = None
            confidence = 0.0
            
            # === PRIORITY 1: Sitoo categories (for store products) ===
            if sku in sitoo_products and sitoo_products[sku]:
                cat_id = sitoo_products[sku][0]  # First category
                raw_cat = sitoo_categories.get(str(cat_id)) or sitoo_categories.get(cat_id)
                if raw_cat:
                    category = standardize_category(raw_cat)
                    if category and category != "Uncategorized":
                        mapping_source = 'sitoo_categories'
                        confidence = 1.0
            
            # === PRIORITY 2: Shopify productType (for online products) ===
            if not category or category == "Uncategorized":
                if source_system == 'shopify' and shopify_category:
                    if shopify_category not in ['Standard', 'Uncategorized', '']:
                        category = standardize_category(shopify_category)
                        if category and category != "Uncategorized":
                            mapping_source = 'shopify_producttype'
                            confidence = 1.0
            
            # === PRIORITY 3: SKU prefix rules ===
            if not category or category == "Uncategorized":
                prefix_cat = get_category_from_prefix(sku)
                if prefix_cat:
                    category = prefix_cat
                    mapping_source = 'sku_prefix'
                    confidence = 0.9
                elif prefix_cat is None and sku.upper().startswith('VN-ONLN'):
                    # Vintage online - use keyword inference
                    category = get_category_from_keywords(product_name)
                    mapping_source = 'keyword_inference'
                    confidence = 0.8
            
            # === PRIORITY 4: Keyword inference for remaining ===
            if not category or category == "Uncategorized":
                # Try keyword inference on product name
                for pattern, cat in KEYWORD_RULES:
                    if product_name and re.search(pattern, product_name.lower()):
                        category = cat.replace('Vintage ', '')  # Non-vintage version
                        mapping_source = 'keyword_inference'
                        confidence = 0.7
                        break
            
            # === DEFAULT ===
            if not category:
                category = "Uncategorized"
                mapping_source = 'default'
                confidence = 0.0
            
            # Update if better than existing
            if sku not in sku_mappings or confidence > sku_mappings[sku]['confidence']:
                sku_mappings[sku] = {
                    'category': category,
                    'source': mapping_source,
                    'confidence': confidence,
                    'product_name': product_name,
                    'original_category': shopify_category
                }
        
        # === STEP 4: Update database ===
        logger.info("Updating database...")
        
        # Clear existing
        session.execute(text("DELETE FROM category_mappings"))
        session.commit()
        
        # Insert new mappings
        mappings = []
        for sku, data in sku_mappings.items():
            mappings.append({
                'sku': sku,
                'product_name': data['product_name'],
                'original_category': data['original_category'],
                'standard_category': data['category'],
                'mapping_source': data['source'],
                'confidence': data['confidence']
            })
        
        batch_size = 500
        for i in range(0, len(mappings), batch_size):
            batch = mappings[i:i+batch_size]
            session.execute(text("""
                INSERT INTO category_mappings 
                (sku, product_name, original_category, standard_category, mapping_source, confidence)
                VALUES (:sku, :product_name, :original_category, :standard_category, :mapping_source, :confidence)
                ON CONFLICT (sku) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    original_category = EXCLUDED.original_category,
                    standard_category = EXCLUDED.standard_category,
                    mapping_source = EXCLUDED.mapping_source,
                    confidence = EXCLUDED.confidence
            """), batch)
            session.commit()
        
        logger.info(f"Created {len(mappings)} category mappings")
        
        # === SUMMARY ===
        print("\n" + "="*60)
        print("CATEGORY MAPPING SUMMARY")
        print("="*60)
        
        source_counts = {}
        category_counts = {}
        for data in sku_mappings.values():
            source_counts[data['source']] = source_counts.get(data['source'], 0) + 1
            category_counts[data['category']] = category_counts.get(data['category'], 0) + 1
        
        print("\nBy Source:")
        for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
            print(f"  {source:25s}: {count:6d}")
        
        print("\nTop Categories:")
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"  {cat:25s}: {count:6d}")
        
        uncat = category_counts.get('Uncategorized', 0)
        total = len(sku_mappings)
        print(f"\n  Total: {total}, Uncategorized: {uncat} ({uncat/total*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    rebuild_category_mappings()
