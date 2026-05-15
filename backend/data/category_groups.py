"""
Shared mapping: standard_category → category_group.

Used by both the sync pipeline (to keep category_group in sync when
Shopify's productType changes) and the setup_category_groups script.

Categories not listed here default to their own standard_category.
"""

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

    # Accessories
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

    # Vintage
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
    "Vintage Trouser":      "Vintage",
    "Vintage Skirt":        "Vintage",

    # Lifestyle
    "Home":         "Lifestyle",
    "Apothecary":   "Lifestyle",
    "Care":         "Lifestyle",
    "Books":        "Lifestyle",
    "Coffee":       "Lifestyle",
}
