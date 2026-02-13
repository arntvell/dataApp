"""
Vendor/Manufacturer Standardization

Maps raw vendor names from various sources (Shopify, Sitoo) to standardized names.
This ensures consistent vendor naming across the dashboard.
"""

# Standard vendor names (the canonical form we want to use)
STANDARD_VENDORS = [
    "Abel",
    "Anonymousism",
    "Asics",
    "Avolt",
    "Baxter of California",
    "Birkenstock",
    "Bon Parfumeur",
    "Books",
    "Boy Smells",
    "Brooklyn Soap Company",
    "Buttero",
    "Camper",
    "Camperlab",
    "Chimi",
    "Clarks",
    "Depot",
    "Diemme",
    "EYM",
    "FEIT",
    "Fellow",
    "Fitguide",
    "Frama",
    "G.H. Bass",
    "Haeckels",
    "Han Kjøbenhavn",
    "Hestra",
    "Hibi",
    "Ichiko Ichie",
    "Imperfect Femme",
    "Imperfect Men",
    "Jacobsen & Svart",
    "Keen",
    "Kinfill",
    "Kinto",
    "Lesca",
    "Livid",
    "Livid Femme",
    "Livid Men",
    "Livid Saved",
    "Livid Skredder",
    "Livid Unisex",
    "Lola James Harper",
    "Mario Lorenzin",
    "Miir",
    "New Balance",
    "Norda",
    "Norsk Ullsåle",
    "Novesta",
    "P.F. Candle",
    "Pantherella",
    "Paraboot",
    "Primeboots",
    "På Stell",
    "Red Wing",
    "Reproduction of Found",
    "RoToTo",
    "Saphir",
    "Son Venin",
    "Steamery",
    "Stetson",
    "Subu",
    "Transparent",
    "Ursa Major",
    "Vintage",
    "Wollow",
    "Woods Copenhagen",
    "Wrapin",
    "Yuketen",
    "ZDA",
]

# Mapping from raw vendor names to standard names
# Keys are lowercase for case-insensitive matching
VENDOR_MAPPING = {
    # Livid variations
    "livid men": "Livid Men",
    "livid femme": "Livid Femme",
    "livid unisex": "Livid Unisex",
    "livid skredder": "Livid Skredder",
    "livid saved": "Livid Saved",
    "livid": "Livid",
    
    # Case variations
    "rototo": "RoToTo",
    "yuketen": "Yuketen",
    "g.h bass": "G.H. Bass",
    "g.h. bass": "G.H. Bass",
    "woods copenhagen": "Woods Copenhagen",
    "wrapin": "Wrapin",
    
    # Common typos or variations
    "p.f. candle": "P.F. Candle",
    "pf candle": "P.F. Candle",
    "p.f candle": "P.F. Candle",
}


def standardize_vendor(raw_vendor: str) -> str:
    """
    Standardize a vendor name to its canonical form.
    
    Args:
        raw_vendor: The raw vendor name from source system
        
    Returns:
        Standardized vendor name
    """
    if not raw_vendor:
        return None
    
    # Clean up the input
    vendor = raw_vendor.strip()
    if not vendor:
        return None
    
    # Check direct mapping (case-insensitive)
    vendor_lower = vendor.lower()
    if vendor_lower in VENDOR_MAPPING:
        return VENDOR_MAPPING[vendor_lower]
    
    # Check if it matches a standard vendor (case-insensitive)
    for standard in STANDARD_VENDORS:
        if vendor_lower == standard.lower():
            return standard
    
    # Return original with title case cleanup for unknown vendors
    # This handles new vendors not yet in our mapping
    return vendor


def get_all_standard_vendors() -> list:
    """Return list of all standard vendor names."""
    return STANDARD_VENDORS.copy()
