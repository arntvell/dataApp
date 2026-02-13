"""
Category Standardization Mapping

Maps raw categories from Shopify (productType) and Sitoo (categories API)
to a standardized set of categories for unified reporting.
"""

# Standard category names (the unified output)
STANDARD_CATEGORIES = [
    # Bottoms
    "Jeans",
    "Trouser",
    "Shorts",
    "Skirt",
    
    # Tops
    "Shirt",
    "T-Shirt",
    "Knitwear",
    "Sweatshirt",
    "Jersey",
    "Singlet",
    "Top",
    "Blouse",
    
    # Outerwear
    "Jacket",
    "Coat",
    "Vest",
    
    # Dresses
    "Dress",
    "Overall",
    "Suiting",
    
    # Footwear
    "Shoe Men",
    "Shoe Women",
    "Boot Men",
    "Boot Women",
    "Sneaker Men",
    "Sneaker Women",
    "Sneaker Unisex",
    "Sandal Men",
    "Sandal Women",
    "Sandal Unisex",
    
    # Accessories
    "Socks",
    "Belt",
    "Sunglasses",
    "Hat",
    "Scarf",
    "Gloves",
    "Bag",
    "Accessories",
    "Underwear",
    
    # Home & Lifestyle
    "Home",
    "Apothecary",
    "Care",
    "Coffee",
    "Books",
    
    # Vintage
    "Vintage Jeans",
    "Vintage Shirt",
    "Vintage Jacket",
    "Vintage Knitwear",
    "Vintage Sweatshirt",
    "Vintage T-Shirt",
    "Vintage Other",
    
    # Services & Other
    "Gift Cards",
    "Services",
    "Sample",
]

# Mapping from source categories to standard categories
# Key: source category (lowercase for matching)
# Value: standard category
CATEGORY_MAP = {
    # === JEANS ===
    "jeans": "Jeans",
    "denim": "Jeans",
    
    # === TROUSERS ===
    "trouser": "Trouser",
    "trousers": "Trouser",
    "pant": "Trouser",
    "pants": "Trouser",
    "chino": "Trouser",
    "chinos": "Trouser",
    "bottoms": "Trouser",
    
    # === SHORTS ===
    "shorts": "Shorts",
    "short": "Shorts",
    
    # === SKIRT ===
    "skirt": "Skirt",
    
    # === SHIRTS ===
    "shirt": "Shirt",
    "shirts": "Shirt",
    "shortsleeved shirt": "Shirt",
    "longsleeve": "Shirt",
    "tops / shirt": "Shirt",
    
    # === T-SHIRTS ===
    "t-shirt": "T-Shirt",
    "tee": "T-Shirt",
    "tees": "T-Shirt",
    
    # === KNITWEAR ===
    "knitwear": "Knitwear",
    "sweater": "Knitwear",
    "jumper": "Knitwear",
    "cardigan": "Knitwear",
    
    # === SWEATSHIRTS ===
    "sweatshirt": "Sweatshirt",
    "hoodie": "Sweatshirt",
    "hoody": "Sweatshirt",
    "fleece": "Sweatshirt",
    
    # === JERSEY ===
    "jersey": "Jersey",
    
    # === SINGLET ===
    "singlet": "Singlet",
    "tank": "Singlet",
    
    # === TOP ===
    "top": "Top",
    "tops": "Top",
    "top/pullover": "Top",
    
    # === BLOUSE ===
    "blouse": "Blouse",
    
    # === JACKETS ===
    "jacket": "Jacket",
    "jackets": "Jacket",
    "leather jacket": "Jacket",
    "leather": "Jacket",
    
    # === COATS ===
    "coat": "Coat",
    "coats": "Coat",
    
    # === VEST ===
    "vest": "Vest",
    
    # === DRESS ===
    "dress": "Dress",
    "dresses": "Dress",
    
    # === OVERALL ===
    "overall": "Overall",
    "overalls": "Overall",
    
    # === SUITING ===
    "suiting": "Suiting",
    "suit": "Suiting",
    
    # === SHOES - MEN ===
    "shoe men": "Shoe Men",
    "shoes men": "Shoe Men",
    "shoe": "Shoe Men",  # Default shoe to men
    "shoes": "Shoe Men",
    
    # === SHOES - WOMEN ===
    "shoe women": "Shoe Women",
    "shoes women": "Shoe Women",
    "shoe woman": "Shoe Women",
    "women shoe": "Shoe Women",
    
    # === BOOTS - MEN ===
    "boot men": "Boot Men",
    "boots men": "Boot Men",
    
    # === BOOTS - WOMEN ===
    "boot women": "Boot Women",
    "boots women": "Boot Women",
    
    # === BOOTS - UNISEX ===
    "boots unisex": "Boot Men",  # Map to Boot Men for simplicity
    
    # === SNEAKERS ===
    "sneaker men": "Sneaker Men",
    "sneakers men": "Sneaker Men",
    "sneaker women": "Sneaker Women",
    "sneakers women": "Sneaker Women",
    "sneaker unisex": "Sneaker Unisex",
    "sneakers unisex": "Sneaker Unisex",
    "sneaker": "Sneaker Unisex",
    "sneakers": "Sneaker Unisex",
    "trainer": "Sneaker Unisex",
    "trainers": "Sneaker Unisex",
    
    # === SANDALS ===
    "sandal men": "Sandal Men",
    "sandals men": "Sandal Men",
    "sandal women": "Sandal Women",
    "sandals women": "Sandal Women",
    "sandal unisex": "Sandal Unisex",
    "sandals unisex": "Sandal Unisex",
    "sandal": "Sandal Unisex",
    "sandals": "Sandal Unisex",
    
    # === SOCKS ===
    "socks": "Socks",
    "socks men": "Socks",
    "socks women": "Socks",
    "socks unisex": "Socks",
    "sock": "Socks",
    
    # === BELTS ===
    "belt": "Belt",
    "belts": "Belt",
    
    # === SUNGLASSES ===
    "sunglasses": "Sunglasses",
    "eyewear": "Sunglasses",
    
    # === HATS ===
    "hat": "Hat",
    "hats": "Hat",
    "hats men": "Hat",
    "cap": "Hat",
    "caps": "Hat",
    "beanie": "Hat",
    "headwear": "Hat",
    
    # === SCARVES ===
    "scarf": "Scarf",
    "scarves": "Scarf",
    
    # === GLOVES ===
    "gloves": "Gloves",
    "gloves men": "Gloves",
    "gloves women": "Gloves",
    "gloves unisex": "Gloves",
    "glove": "Gloves",
    
    # === BAGS ===
    "bag": "Bag",
    "bags": "Bag",
    
    # === ACCESSORIES ===
    "accessories": "Accessories",
    "accessory": "Accessories",
    "laces": "Accessories",
    
    # === UNDERWEAR ===
    "underwear": "Underwear",
    
    # === HOME ===
    "home": "Home",
    
    # === APOTHECARY ===
    "apothecary": "Apothecary",
    "apothecary unisex": "Apothecary",
    "apothecary men": "Apothecary",
    "apothecary women": "Apothecary",
    
    # === CARE ===
    "care": "Care",
    "clothing care": "Care",
    
    # === COFFEE ===
    "coffee": "Coffee",
    "kaffe": "Coffee",
    
    # === BOOKS ===
    "books": "Books",
    "book": "Books",
    
    # === GIFT CARDS ===
    "gift cards": "Gift Cards",
    "gift card": "Gift Cards",
    "giftcard": "Gift Cards",
    
    # === SERVICES ===
    "services": "Services",
    "skredder": "Services",
    "alteration": "Services",
    "repair": "Services",
    
    # === SAMPLE ===
    "sample": "Sample",
    "sample pack": "Sample",
    
    # === INTERNAL/IGNORED (map to None to exclude) ===
    "market": None,
    "popup": None,
    "saved": None,
    "vintage pack": None,
    "other pack": None,
    "other": None,
    "misc": None,
    "essentials": None,
    "stork": None,
    "wrapin": None,
    "fitguide": None,
}


def standardize_category(raw_category: str) -> str:
    """
    Convert a raw category from Shopify or Sitoo to a standard category.
    
    Args:
        raw_category: The raw category string from the source system
        
    Returns:
        The standardized category name, or "Uncategorized" if no mapping exists
    """
    if not raw_category:
        return "Uncategorized"
    
    # Normalize: lowercase and strip
    normalized = raw_category.lower().strip()
    
    # Look up in mapping
    if normalized in CATEGORY_MAP:
        result = CATEGORY_MAP[normalized]
        return result if result else "Uncategorized"  # None means excluded
    
    # If not found, check if it starts with "vintage"
    if normalized.startswith("vintage"):
        return "Vintage Other"
    
    # Return as-is if it looks like a valid category (capitalized)
    if raw_category[0].isupper() and raw_category not in ["Standard", "Uncategorized", ""]:
        return raw_category
    
    return "Uncategorized"


def get_standard_categories() -> list:
    """Return the list of standard categories"""
    return STANDARD_CATEGORIES.copy()
