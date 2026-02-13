# Category Mapping Logic

## Overview

The category mapping system assigns standardized categories to products using **authoritative sources** from each sales channel, with fallback rules for edge cases.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
├─────────────────────────────┬────────────────────────────────────┤
│     SHOPIFY (Online)        │        SITOO (In-Store)            │
│     ─────────────────       │        ────────────────            │
│     productType field       │        categories API              │
│     Source of truth for     │        Source of truth for         │
│     online orders           │        store orders                │
└─────────────────────────────┴────────────────────────────────────┘
                    │                         │
                    ▼                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                 STANDARDIZATION LAYER                            │
├──────────────────────────────────────────────────────────────────┤
│   Maps source categories → Standard categories                   │
│                                                                  │
│   Shopify "Denim" ───────┐                                       │
│                          ├──→ "Jeans"                            │
│   Sitoo "Jeans" ─────────┘                                       │
│                                                                  │
│   Shopify "Jumper" ──────┐                                       │
│                          ├──→ "Knitwear"                         │
│   Sitoo "Knitwear" ──────┘                                       │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│              FALLBACK: PREFIX & KEYWORD RULES                    │
├──────────────────────────────────────────────────────────────────┤
│   For items NOT in their respective system:                      │
│   - Vintage Online (VN-ONLN-*) → keyword inference               │
│   - Vintage In-store (EXT-VN-*) → prefix rules                   │
│   - Livid products → SKU prefix rules                            │
└──────────────────────────────────────────────────────────────────┘
```

## Priority Order

| Priority | Source | Confidence | Description |
|----------|--------|------------|-------------|
| 1 | `sitoo_categories` | 1.0 | Categories from Sitoo API (authoritative for in-store) |
| 2 | `shopify_producttype` | 1.0 | productType from Shopify (authoritative for online) |
| 3 | `sku_prefix` | 0.9 | SKU pattern rules (e.g., LIV-BRNS → Jeans) |
| 4 | `keyword_inference` | 0.8 | Product name keywords (e.g., "sweater" → Knitwear) |
| 5 | `default` | 0.0 | Uncategorized |

## Current Statistics

| Source | SKUs | Revenue | % of SKUs |
|--------|------|---------|-----------|
| `sitoo_categories` | 8,845 | 166.3M NOK | 41.9% |
| `shopify_producttype` | 2,522 | 25.2M NOK | 12.0% |
| `sku_prefix` | 581 | 6.6M NOK | 2.8% |
| `keyword_inference` | 8,675 | 5.9M NOK | 41.1% |
| `default` (Uncategorized) | 464 | 5.2M NOK | **2.2%** |

**Total: 21,087 SKUs, 2.2% Uncategorized**

---

## Standard Categories

### Bottoms
- Jeans
- Trouser
- Shorts
- Skirt

### Tops
- Shirt
- T-Shirt
- Knitwear
- Sweatshirt
- Jersey
- Singlet
- Top
- Blouse

### Outerwear
- Jacket
- Coat
- Vest

### Dresses & Suits
- Dress
- Overall
- Suiting

### Footwear
- Shoe Men / Shoe Women
- Boot Men / Boot Women
- Sneaker Men / Sneaker Women / Sneaker Unisex
- Sandal Men / Sandal Women / Sandal Unisex

### Accessories
- Socks
- Belt
- Sunglasses
- Hat
- Scarf
- Gloves
- Bag
- Accessories
- Underwear

### Home & Lifestyle
- Home
- Apothecary
- Care
- Coffee
- Books

### Vintage
- Vintage Jeans
- Vintage Shirt
- Vintage Jacket
- Vintage Knitwear
- Vintage Sweatshirt
- Vintage T-Shirt
- Vintage Other

### Services & Other
- Gift Cards
- Services
- Sample

---

## Standardization Mapping

The mapping file is located at: `backend/data/category_standardization.py`

### Examples

| Source Category | Standard Category |
|-----------------|-------------------|
| "jeans", "denim" | Jeans |
| "trouser", "pants", "chino", "bottoms" | Trouser |
| "shirt", "shirts", "longsleeve" | Shirt |
| "knitwear", "sweater", "jumper", "cardigan" | Knitwear |
| "sweatshirt", "hoodie", "fleece" | Sweatshirt |
| "socks", "socks men", "socks women" | Socks |
| "hat", "hats", "cap", "beanie", "headwear" | Hat |

---

## SKU Prefix Rules

For products without Sitoo/Shopify categorization:

### Livid Jeans (LIV-*)
| Prefix | Model | Category |
|--------|-------|----------|
| LIV-BRNS | Barnes | Jeans |
| LIV-BTH | Beth | Jeans |
| LIV-BLY | Bailey | Jeans |
| LIV-FUL, LIV-FLLR | Fuller | Jeans |
| LIV-MK | Miko | Jeans |
| ... | ... | Jeans |

### Livid Shirts (LIV-*)
| Prefix | Model | Category |
|--------|-------|----------|
| LIV-KN | Ken/Knut | Shirt |
| LIV-ANT | Anton | Shirt |
| LIV-WST | West | Shirt |
| ... | ... | Shirt |

### External Footwear (EXT-*)
| Prefix | Brand | Category |
|--------|-------|----------|
| EXT-RW | Red Wing | Boot Men |
| EXT-PB | Paraboot | Shoe Men |
| EXT-BKST | Birkenstock | Sandal Unisex |
| ... | ... | ... |

---

## Keyword Inference (Vintage Items)

For vintage online items (VN-ONLN-*) without explicit categories:

| Pattern | Category |
|---------|----------|
| "jean", "denim", "levi" | Vintage Jeans |
| "jacket", "coat", "parka", "bomber" | Vintage Jacket |
| "sweat", "hoodie", "fleece" | Vintage Sweatshirt |
| "knit", "sweater", "cardigan", "wool" | Vintage Knitwear |
| "shirt", "flannel", "oxford", "rugby" | Vintage Shirt |
| "t-shirt", "tee" | Vintage T-Shirt |
| *(no match)* | Vintage Other |

---

## Files

- **Standardization Mapping**: `backend/data/category_standardization.py`
- **Rebuild Script**: `backend/scripts/rebuild_categories_v2.py`
- **Database Table**: `category_mappings`
- **Export**: `category_mappings_v3.csv`

## Rebuilding Categories

To rebuild all category mappings:

```bash
docker exec dataapp-backend-1 python scripts/rebuild_categories_v2.py
```
