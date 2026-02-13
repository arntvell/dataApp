"""
Comprehensive Category Mapping Builder
Handles all product types with improved rules
"""

import re
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CATEGORY RULES - Priority order (first match wins)
# ============================================================================

# SKU prefix rules: (prefix1, prefix2) -> category
SKU_PREFIX_RULES = {
    # Livid Jeans - various models
    ('LIV', 'BRNS'): 'Jeans',      # Barnes
    ('LIV', 'BTH'): 'Jeans',       # Beth
    ('LIV', 'BLY'): 'Jeans',       # Bailey
    ('LIV', 'JNE'): 'Jeans',       # Jone
    ('LIV', 'KRI'): 'Jeans',       # Keri
    ('LIV', 'ISL'): 'Jeans',       # Isle
    ('LIV', 'WMN'): 'Jeans',       # Women's jeans
    ('LIV', 'AVA'): 'Jeans',       # Ava
    ('LIV', 'ELL'): 'Jeans',       # Ella
    ('LIV', 'ADS'): 'Jeans',       # Addison
    ('LIV', 'ADDSN'): 'Jeans',     # Addison
    ('LIV', 'EMRSN'): 'Jeans',     # Emerson
    ('LIV', 'EVRD'): 'Jeans',      # Edvard
    ('LIV', 'EDVRD'): 'Jeans',     # Edvard
    ('LIV', 'EDV'): 'Jeans',       # Edvard
    ('LIV', 'RLD'): 'Jeans',       # Roald
    ('LIV', 'TUE'): 'Jeans',       # Tue
    ('LIV', 'PG'): 'Jeans',        # Page
    ('LIV', 'MK'): 'Jeans',        # Miko
    ('LIV', 'FUL'): 'Jeans',       # Fuller
    ('LIV', 'FLLR'): 'Jeans',      # Fuller (alternate)
    ('LIV', 'HYS'): 'Jeans',       # Hayes
    ('LIV', 'K'): 'Jeans',         # Kai
    ('LIV', 'JN'): 'Jeans',        # Jon
    ('LIV', 'T'): 'Jeans',         # Tuck
    ('LIV', 'VR'): 'Jeans',        # Vår
    
    # Livid Shirts
    ('LIV', 'KN'): 'Shirt',        # Ken/Knut
    ('LIV', 'HF'): 'Shirt',        # Half
    ('LIV', 'AID'): 'Shirt',       # Aidan
    ('LIV', 'ANT'): 'Shirt',       # Anton
    ('LIV', 'STN'): 'Shirt',       # Stone
    ('LIV', 'WST'): 'Shirt',       # West
    ('LIV', 'TK'): 'Shirt',        # Tiki
    ('LIV', 'HL'): 'Shirt',        # Hill
    ('LIV', 'INTL'): 'Shirt',      # Initial
    ('LIV', 'CMPT'): 'Shirt',      # Compton
    ('LIV', 'SHN'): 'Shirt',       # Shane
    ('LIV', 'SRN'): 'Shirt',       # Soren
    
    # Livid T-Shirts
    ('LIV', 'NLSN'): 'T-Shirt',    # Nelson
    ('LIV', 'NLS'): 'T-Shirt',     # Nelson
    ('LIV', 'RCHMND'): 'T-Shirt',  # Richmond (basics)
    ('LIV', 'N'): 'T-Shirt',       # N
    
    # Livid Knitwear
    ('LIV', 'FM'): 'Knitwear',     # Women's knitwear
    ('LIV', 'KLMR'): 'Knitwear',   # Kilmer
    ('LIV', 'MRCR'): 'Knitwear',   # Mercer
    ('LIV', 'LRK'): 'Knitwear',    # Lerke
    ('LIV', 'PLTH'): 'Knitwear',   # Plath
    ('LIV', 'WLM'): 'Knitwear',    # William
    ('LIV', 'ML'): 'Knitwear',     # Mila
    ('LIV', 'OLY'): 'Knitwear',    # Olivia
    
    # Livid Trousers
    ('LIV', 'JP'): 'Trouser',      # Japan trouser
    ('LIV', 'KR'): 'Trouser',      # Keri trouser (non-denim)
    ('LIV', 'NRMN'): 'Trouser',    # Norman
    ('LIV', 'RGNR'): 'Trouser',    # Ragnar
    ('LIV', 'CSDY'): 'Trouser',    # Cassidy
    ('LIV', 'CNLY'): 'Trouser',    # Connely
    ('LIV', 'WCSTR'): 'Trouser',   # Worcester
    ('LIV', 'SLGSV'): 'Trouser',   # Selvage trouser
    
    # Livid Jackets/Outerwear
    ('LIV', 'BWSR'): 'Jacket',     # Bowser
    ('LIV', 'CRMY'): 'Jacket',     # Carmy
    ('LIV', 'KVN'): 'Jacket',      # Kevin
    ('LIV', 'NLN'): 'Jacket',      # Nolan
    ('LIV', 'NSH'): 'Jacket',      # Nash
    ('LIV', 'HM'): 'Jacket',       # Hume
    ('LIV', 'RLY'): 'Jacket',      # Riley
    ('LIV', 'JSH'): 'Jacket',      # Joshua
    ('LIV', 'JKOB'): 'Jacket',     # Jakob
    ('LIV', 'FLY'): 'Jacket',      # Flynn
    ('LIV', 'WLY'): 'Jacket',      # Willy
    
    # Livid Dresses/Women's
    ('LIV', 'DPHN'): 'Dress',      # Daphne
    ('LIV', 'PYTN'): 'Dress',      # Peyton
    ('LIV', 'CRYSTL'): 'Dress',    # Crystal
    ('LIV', 'Lou'): 'Dress',       # Lou
    ('LIV', 'Joelle'): 'Dress',    # Joelle
    ('LIV', 'Faye'): 'Dress',      # Faye
    
    # External - Shoes/Boots
    ('EXT', 'RW'): 'Boot Men',     # Red Wing
    ('EXT', 'PR'): 'Boot Women',   # Paraboot women
    ('EXT', 'PB'): 'Shoe Men',     # Paraboot men
    ('EXT', 'DM'): 'Shoe Men',     # Dolomite
    ('EXT', 'NRD'): 'Shoe Men',    # Nordic
    ('EXT', 'ZDA'): 'Shoe Men',    # ZDA
    ('EXT', 'MCHL'): 'Shoe Men',   # Michael
    ('EXT', 'KEEN'): 'Shoe Unisex', # Keen
    ('EXT', 'BT'): 'Shoe Men',     # Buttero
    ('EXT', 'CMPLB'): 'Shoe Men',  # Camper Lab
    ('EXT', 'CMP'): 'Shoe Men',    # Camper
    ('EXT', 'JP'): 'Shoe Unisex',  # Japan shoe
    ('EXRT', 'ASC'): 'Shoe Unisex', # Asics (typo in data)
    
    # External - Sandals
    ('EXT', 'BKST'): 'Sandal Unisex',  # Birkenstock
    ('EXT', 'BS'): 'Sandal Unisex',    # Birkenstock
    
    # External - Accessories
    ('EXT', 'HST'): 'Accessories',  # Hestra gloves
    ('EXT', 'WLW'): 'Accessories',  # Wollow sunglasses
    ('EXT', 'ASC'): 'Shoe Unisex',  # Asics
    ('EXT', 'NULL'): 'Accessories', # Insoles
    ('EXT', 'STS'): 'Accessories',  # Stetson (hats/beanies)
    ('EXT', 'PEN'): 'Accessories',  # Pendleton
    
    # External - Socks
    ('EXT', 'RT'): 'Socks',         # Rototo
    ('EXT', 'PNT'): 'Socks',        # Pantherella
    ('EXT', 'ANY'): 'Socks',        # Anonymous Ism
    
    # External - Home
    ('EXT', 'PF'): 'Home',          # P.F. Candle
    
    # Samples/Other
    ('LIV', 'SMPLS'): 'Sample',     # Samples
    ('LIV', 'MSCSMPL'): 'Sample',   # Misc samples
    
    # Vintage
    ('VN', 'ONLN'): None,           # Needs name-based classification
    ('EXT', 'VN'): 'Vintage Other',
}

# Product name keyword rules for vintage and fallback
# Order matters - more specific patterns first
NAME_KEYWORD_RULES = [
    # Services (check first)
    (r'\b(gift\s*card|gift\s*wrap|alteration|repair|hemming|skredder)\b', 'Services'),
    
    # Jeans patterns
    (r'\b(jean|denim|selvage|selvedge)\b', 'Jeans'),
    (r'\bjapan\b.*\b(black|blue|indigo|dry|stone|comfort|vintage|new)\b', 'Jeans'),
    (r'\b(barnes|beth|bailey|jone|keri|isle|edvard|roald|miko|fuller|hayes|kai|tuck|page|ava|ella|addison|emerson|vår)\b', 'Jeans'),
    (r'\blevi\'?s?\b', 'Jeans'),
    
    # Jackets/Outerwear (before shirts to catch "shirt jacket")
    (r'\b(jacket|coat|parka|bomber|blazer|overshirt|anorak|windbreaker|shearling|leather\s+jacket)\b', 'Jacket'),
    (r'\b(uniform\s+jacket|military\s+jacket|army\s+jacket|work\s+jacket|chore\s+coat)\b', 'Jacket'),
    (r'\b(bowser|carmy|kevin|nolan|nash|hume|riley|joshua)\b', 'Jacket'),
    (r'\b(varsity|letterman|liner|vest)\b', 'Jacket'),
    
    # Sweatshirts/Hoodies (before shirts)
    (r'\b(sweat|hoodie|hoody|fleece|crewneck)\b', 'Sweatshirt'),
    (r'\b(sportswear|athletic|track\s*suit|track\s*top)\b', 'Sweatshirt'),
    (r'\bzip\b.*\b(fleece|sweat)\b', 'Sweatshirt'),
    
    # Knitwear/Sweaters
    (r'\b(knit|sweater|cardigan|pullover|jumper|wool|mohair|cashmere|merino|cable\s*knit)\b', 'Knitwear'),
    (r'\b(turtle|turtleneck|half.?turtle|crew\s*neck|v.?neck)\b.*\b(knit|wool|sweater)\b', 'Knitwear'),
    (r'\b(kilmer|mercer|lerke|plath|william|mila|olivia)\b', 'Knitwear'),
    (r'\bnorwegian\b.*\b(sweater|knit)\b', 'Knitwear'),
    
    # Shirts
    (r'\b(shirt|oxford|chambray|flannel|western|button.?down|button.?up)\b', 'Shirt'),
    (r'\b(cord\s+shirt|rugby|polo)\b', 'Shirt'),
    (r'\b(anton|aidan|stone|west|tiki|ken|knut|hill|initial|compton|shane|soren)\b', 'Shirt'),
    (r'\b(hawaiian|aloha|camp\s+collar)\b', 'Shirt'),
    
    # T-Shirts
    (r'\bt-?shirt\b', 'T-Shirt'),
    (r'\b(tee|nelson|richmond)\b', 'T-Shirt'),
    (r'\blong\s*sleeve\s*t(ee)?\b', 'T-Shirt'),
    
    # Trousers
    (r'\b(trouser|pant|chino|cord|corduroy|moleskin|slacks)\b', 'Trouser'),
    (r'\b(norman|ragnar|cassidy|connely|worcester)\b', 'Trouser'),
    (r'\b(cargo|fatigue|utility)\s*(pant|trouser)\b', 'Trouser'),
    
    # Shorts
    (r'\bshort[s]?\b', 'Shorts'),
    
    # Dresses/Women
    (r'\b(dress|skirt|blouse)\b', 'Dress'),
    (r'\b(daphne|peyton|crystal|lou|joelle|faye)\b', 'Dress'),
    
    # Boots
    (r'\b(boot|roughneck|moc\s*toe|iron\s*ranger|logger|engineer)\b', 'Boot'),
    (r'\b(avoriaz|neige|clusaz)\b', 'Boot'),
    
    # Shoes
    (r'\b(shoe|loafer|derby|brogue|monk\s*strap)\b', 'Shoe'),
    (r'\b(sneaker|trainer|runner|tennis\s*shoe)\b', 'Sneaker'),
    (r'\b(paraboot|cornaro|villandry|adriatic|bergerac|chimey|michael|buttero)\b', 'Shoe'),
    
    # Sandals
    (r'\b(sandal|arizona|boston|birkenstock)\b', 'Sandal'),
    
    # Accessories
    (r'\b(glove|mitt|mitten)\b', 'Accessories'),
    (r'\b(scarf|beanie|hat|cap|headwear)\b', 'Accessories'),
    (r'\b(belt|wallet|bag|sunglasses|glasses|eyewear)\b', 'Accessories'),
    (r'\b(hestra|wollow|idun|valeria|margaret|otra|stetson|pendleton)\b', 'Accessories'),
    (r'\b(insole|shoe\s*care|polish|brush)\b', 'Accessories'),
    
    # Socks
    (r'\b(sock|hosiery)\b', 'Socks'),
    
    # Home
    (r'\b(candle|incense|fragrance|room\s*spray|diffuser)\b', 'Home'),
    
    # Coffee
    (r'\b(kaffe|coffee)\b', 'Coffee'),
]


def get_category_from_sku_prefix(sku: str) -> str | None:
    """Get category from SKU prefix rules"""
    if not sku:
        return None
    
    parts = sku.upper().split('-')
    if len(parts) >= 2:
        key = (parts[0], parts[1])
        if key in SKU_PREFIX_RULES:
            return SKU_PREFIX_RULES[key]
    
    if len(parts) >= 1:
        # Try single prefix
        for (p1, p2), cat in SKU_PREFIX_RULES.items():
            if parts[0] == p1 and len(parts) > 1 and parts[1].startswith(p2[:2]):
                return cat
    
    return None


def get_category_from_name(product_name: str) -> str | None:
    """Get category from product name using keyword rules"""
    if not product_name:
        return None
    
    name_lower = product_name.lower()
    
    for pattern, category in NAME_KEYWORD_RULES:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return category
    
    return None


def categorize_product(sku: str, product_name: str, original_category: str) -> tuple[str, str, float]:
    """
    Determine category for a product.
    Returns: (category, source, confidence)
    """
    # Categories to skip (not useful for categorization)
    SKIP_CATEGORIES = ['Standard', 'Unknown', 'Uncategorized', None, '']
    
    # 1. Try SKU prefix rules first
    cat = get_category_from_sku_prefix(sku)
    if cat:
        return cat, 'sku_prefix', 0.95
    
    # 2. Try name-based inference (before using original category to get better coverage)
    cat = get_category_from_name(product_name)
    if cat:
        return cat, 'keyword_inference', 0.85
    
    # 3. Try original category from Shopify if valid
    if original_category and original_category not in SKIP_CATEGORIES:
        return original_category, 'shopify', 1.0
    
    # 4. Default to Uncategorized
    return 'Uncategorized', 'default', 0.0


def rebuild_category_mappings():
    """Rebuild all category mappings with improved rules"""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        logger.info("Starting category mapping rebuild...")
        
        # Get unique SKUs - pick the best product_name and category for each SKU
        # Prefer non-Standard categories and longer product names
        result = session.execute(text("""
            SELECT 
                sku,
                product_name,
                original_category
            FROM (
                SELECT 
                    soi.sku,
                    soi.product_name,
                    soi.product_category as original_category,
                    ROW_NUMBER() OVER (
                        PARTITION BY soi.sku 
                        ORDER BY 
                            CASE WHEN soi.product_category NOT IN ('Standard', '') THEN 0 ELSE 1 END,
                            LENGTH(soi.product_name) DESC
                    ) as rn
                FROM sales_order_items soi
                WHERE soi.sku IS NOT NULL AND soi.sku != ''
            ) sub
            WHERE rn = 1
        """))
        
        all_skus = result.fetchall()
        logger.info(f"Found {len(all_skus)} unique SKUs to categorize")
        
        # Clear existing mappings
        session.execute(text("DELETE FROM category_mappings"))
        session.commit()
        logger.info("Cleared existing category mappings")
        
        # Build new mappings - deduplicate by SKU
        seen_skus = set()
        mappings = []
        category_counts = {}
        
        for row in all_skus:
            sku, product_name, original_category = row
            
            if sku in seen_skus:
                continue
            seen_skus.add(sku)
            
            category, source, confidence = categorize_product(sku, product_name, original_category)
            
            mappings.append({
                'sku': sku,
                'product_name': product_name,
                'original_category': original_category,
                'standard_category': category,
                'mapping_source': source,
                'confidence': confidence
            })
            
            category_counts[category] = category_counts.get(category, 0) + 1
        
        # Insert in batches
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
        
        # Print summary
        print("\n" + "="*60)
        print("CATEGORY MAPPING SUMMARY")
        print("="*60)
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
            print(f"  {cat:25s}: {count:6d}")
        
        # Check remaining uncategorized
        uncat = category_counts.get('Uncategorized', 0)
        total = len(mappings)
        print(f"\n  Total: {total}, Uncategorized: {uncat} ({uncat/total*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    rebuild_category_mappings()
