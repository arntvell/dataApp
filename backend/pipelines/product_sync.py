"""
Product source-of-truth (SSOT) sync pipeline.

Flow:
  1. sync_catalogs()      pull Shopify/Sitoo/Cin7 catalogs -> raw.* snapshot tables (truncate-reload)
  2. build_master()       merge raw tables by normalized SKU -> public.product_master (per-field precedence)
  3. project_to_mappings() refresh category_mappings / parent_sku_mappings FROM product_master,
                          and backfill sales_order_items.vendor / product_category

The dashboard keeps joining SalesOrderItem.sku -> category_mappings/parent_sku_mappings unchanged;
this pipeline simply replaces "derive attributes from sales lines" with "derive from the catalog SSOT".

SKU casing: sales_order_items.sku is mixed-case, and the dashboard joins on exact equality. We therefore
merge sources on a NORMALIZED key (upper/trim) but project rows using the EXACT sku casing already present
in sales_order_items / category_mappings, matched case-insensitively.
"""
import logging
import re
from typing import Dict, List, Any, Callable

from sqlalchemy import text
from database.config import SessionLocal, engine
from database.models import (
    RawShopifyProduct, RawSitooProduct, RawCin7Product, ProductMaster, SyncStatus,
)
from connectors.shopify_connector import ShopifyConnector
from connectors.sitoo_connector import SitooConnector
from connectors.cin7_connector import Cin7Connector
from data.category_groups import CATEGORY_GROUPS
from data.vendor_standardization import standardize_vendor
from data.category_standardization import standardize_category
from scripts.rebuild_category_mapping import get_category_from_sku_prefix, get_category_from_name

logger = logging.getLogger(__name__)

INVALID_CATEGORIES = {None, "", "Uncategorized", "Standard", "Fitguide", "Sample", "Service"}
_STATUS_RANK = {"ACTIVE": 0, "DRAFT": 1, "ARCHIVED": 2}


def _norm(sku):
    return sku.strip().upper() if sku and sku.strip() else None


def _gender_from_shopify(vendor: str, tags: str):
    t = f"{vendor or ''} {tags or ''}".lower()
    if "unisex" in t:
        return "unisex"
    if "femme" in t or "women" in t or "woman" in t:
        return "women"
    if "men" in t:  # 'women' already returned above, so this is safe
        return "men"
    return None


def _gender_from_cin7(category: str):
    if not category:
        return None
    c = category.lower()
    if "unisex" in c:
        return "unisex"
    if "women" in c or "femme" in c:
        return "women"
    if "men" in c:
        return "men"
    return None


def _extract_parent(sku: str):
    """Return (parent_sku, size_code, size_type) from SKU patterns (mirrors sales_sync)."""
    if not sku:
        return sku, None, None
    patterns = [
        (r'^(.+)-(\d{4})$', 'denim'),
        (r'^(.+)-(XXS|XS|S|M|L|XL|XXL|2XL|3XL)$', 'letter'),
        (r'^(.+)-(OS)$', 'one_size'),
        (r'^(.+)-(\d{1,2})$', 'numeric'),
    ]
    for pat, st in patterns:
        m = re.match(pat, sku, re.IGNORECASE)
        if m:
            return m.group(1), m.group(2).upper(), st
    return sku, None, None


def _strip_gender(vendor: str):
    if not vendor:
        return vendor
    return re.sub(r'\b(men|women|femme|unisex|mens|womens)\b', '', vendor, flags=re.IGNORECASE).strip()


class ProductSyncPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.shopify = ShopifyConnector(config.get("shopify", {}))
        self.sitoo = SitooConnector(config.get("sitoo", {}))
        self.cin7 = Cin7Connector(config.get("cin7", {}))

    # ---------- sync status ----------
    def _update_status(self, db, **kwargs):
        st = db.query(SyncStatus).filter(SyncStatus.source_system == "product_master").first()
        if not st:
            st = SyncStatus(source_system="product_master")
            db.add(st)
            db.commit()
            db.refresh(st)
        for k, v in kwargs.items():
            if hasattr(st, k):
                setattr(st, k, v)
        db.commit()

    # ---------- 1. raw catalog snapshots ----------
    def sync_catalogs(self):
        from datetime import datetime
        db = SessionLocal()
        try:
            self._update_status(db, sync_in_progress=True, last_error=None)

            shop = self.shopify.get_all_products()
            sitoo = self.sitoo.get_all_products()
            cin7 = self.cin7.get_all_products()

            with engine.begin() as conn:
                conn.execute(text("TRUNCATE raw.shopify_products, raw.sitoo_products, raw.cin7_products"))
            db.bulk_insert_mappings(RawShopifyProduct, shop)
            db.bulk_insert_mappings(RawSitooProduct, sitoo)
            db.bulk_insert_mappings(RawCin7Product, cin7)
            db.commit()
            logger.info(f"Catalog snapshot: shopify={len(shop)} sitoo={len(sitoo)} cin7={len(cin7)}")
            return {"shopify": len(shop), "sitoo": len(sitoo), "cin7": len(cin7)}
        except Exception as e:
            db.rollback()
            self._update_status(db, sync_in_progress=False, last_error=str(e))
            logger.error(f"Catalog sync failed: {e}")
            raise
        finally:
            db.close()

    # ---------- 2. build the unified master ----------
    def build_master(self):
        db = SessionLocal()
        try:
            shop = {}
            for r in db.query(RawShopifyProduct).all():
                k = _norm(r.sku)
                if not k:
                    continue
                # prefer ACTIVE > DRAFT > ARCHIVED when a SKU repeats
                cur = shop.get(k)
                if cur is None or _STATUS_RANK.get((r.status or "").upper(), 9) < _STATUS_RANK.get((cur.status or "").upper(), 9):
                    shop[k] = r
            sitoo = {_norm(r.sku): r for r in db.query(RawSitooProduct).all() if _norm(r.sku)}
            cin7 = {_norm(r.sku): r for r in db.query(RawCin7Product).all() if _norm(r.sku)}

            all_keys = set(shop) | set(sitoo) | set(cin7)
            rows = []
            for k in all_keys:
                sp, si, ci = shop.get(k), sitoo.get(k), cin7.get(k)

                # ----- category (Shopify productType -> Cin7 -> prefix -> keyword) -----
                category, csource = None, None
                if sp and sp.product_type and sp.product_type not in INVALID_CATEGORIES:
                    category, csource = sp.product_type, "shopify"
                elif ci and ci.category and ci.category not in INVALID_CATEGORIES:
                    category, csource = ci.category, "cin7"
                else:
                    pc = get_category_from_sku_prefix(k)
                    if pc:
                        category, csource = pc, "sku_prefix"
                    else:
                        name_for_kw = (sp and sp.title) or (ci and ci.name) or (si and si.title)
                        nc = get_category_from_name(name_for_kw)
                        if nc:
                            category, csource = nc, "keyword"
                if not category:
                    category, csource = "Uncategorized", "none"
                category_group = CATEGORY_GROUPS.get(category, category)

                # ----- gender -----
                designed_for = None
                if sp:
                    designed_for = _gender_from_shopify(sp.vendor, sp.tags)
                if not designed_for and ci:
                    designed_for = _gender_from_cin7(ci.category)

                # ----- vendor / brand -----
                raw_brand = (ci and ci.brand) or (si and si.manufacturer_name) or (sp and _strip_gender(sp.vendor))
                vsource = "cin7" if (ci and ci.brand) else ("sitoo" if (si and si.manufacturer_name) else ("shopify" if sp and sp.vendor else None))
                is_livid = bool((raw_brand and "livid" in raw_brand.lower())
                                or k.startswith("LIV") or k.startswith("IMP-LIV"))
                if is_livid:
                    sold_as_vendor = {"men": "Livid Men", "women": "Livid Femme",
                                      "unisex": "Livid Unisex"}.get(designed_for, "Livid Men")
                    vsource = vsource or "livid_rule"
                else:
                    sold_as_vendor = standardize_vendor(raw_brand) if raw_brand else None

                # ----- vintage is a sourcing segment, orthogonal to garment type -----
                # Shopify productType reports the garment (Shirt/Knitwear/...), which would
                # otherwise dissolve the Vintage segment the business reports on. Preserve it.
                is_vintage = (k.startswith(("VN-", "EXT-VN-", "EXT-VIN-", "VIN-")) or "-VN-" in k
                              or (category or "").lower().startswith("vintage"))
                if is_vintage:
                    # Canonicalise the garment so "Vintage Shirt/shirt/Shirts/Tee/Sweater/..."
                    # collapse to one subcategory each (consolidation is vintage-only).
                    garment = category or ""
                    if garment.lower().startswith("vintage"):
                        garment = garment[len("vintage"):].strip()
                    canon = standardize_category(garment) if garment else "Uncategorized"
                    category = f"Vintage {canon}" if canon and canon != "Uncategorized" else "Vintage Other"
                    category_group = "Vintage"
                    csource = (csource or "none") + "+vintage"
                    sold_as_vendor = "Vintage"
                    vsource = "vintage_rule"

                # ----- parent sku -----
                # Always size-strip the variant SKU so the parent groups variants and
                # never shows a size (e.g. LIV-BRNS-JPN-DWN-3834 -> LIV-BRNS-JPN-DWN).
                # Fall back to Sitoo's (also size-stripped) parent only when the SKU
                # itself carries no size pattern.
                base, size_code, size_type = _extract_parent(k)
                if base == k and si and si.parent_sku:
                    base = _extract_parent(_norm(si.parent_sku))[0]
                parent_sku = base

                rows.append({
                    "sku": k,
                    "parent_sku": parent_sku,
                    "product_name": (sp and sp.title) or (ci and ci.name) or (si and si.title),
                    "standard_category": category,
                    "category_group": category_group,
                    "designed_for": designed_for,
                    "sold_as_vendor": sold_as_vendor,
                    "cost": (ci and ci.average_cost) or (sp and sp.unit_cost) or (si and si.cost),
                    "price": (si and si.price) or (sp and sp.price),
                    "status": (sp and sp.status) or (ci and ci.status),
                    "country_of_origin": ci and ci.country_of_origin,
                    "image_url": sp.image_url if sp else None,
                    "in_shopify": sp is not None,
                    "in_sitoo": si is not None,
                    "in_cin7": ci is not None,
                    "category_source": csource,
                    "vendor_source": vsource,
                    "shopify_product_id": sp and sp.product_id,
                    "sitoo_product_id": si and si.product_id,
                    "cin7_product_id": ci and str(ci.product_id),
                    "_size_code": size_code,
                    "_size_type": size_type,
                })

            # size info isn't a master column; keep it for the projection step
            self._size_info = {r["sku"]: (r.pop("_size_code"), r.pop("_size_type")) for r in rows}

            with engine.begin() as conn:
                conn.execute(text("TRUNCATE product_master"))
            db.bulk_insert_mappings(ProductMaster, rows)
            db.commit()
            logger.info(f"product_master built: {len(rows)} SKUs")
            return len(rows)
        finally:
            db.close()

    # ---------- 3. project into the read layer ----------
    def project_to_mappings(self):
        with engine.begin() as conn:
            # master keyed by normalized sku
            master = {m.sku: m for m in conn.execute(text(
                "SELECT sku, parent_sku, product_name, standard_category, category_group, "
                "designed_for, sold_as_vendor, category_source FROM product_master")).fetchall()}

            # target SKUs the dashboard can join on (exact casing preserved)
            target = [r[0] for r in conn.execute(text(
                "SELECT DISTINCT sku FROM sales_order_items WHERE sku IS NOT NULL AND sku<>'' "
                "UNION SELECT DISTINCT sku FROM category_mappings WHERE sku IS NOT NULL "
                "UNION SELECT DISTINCT sku FROM parent_sku_mappings WHERE sku IS NOT NULL")).fetchall()]

            cat_rows, par_rows, matched = [], [], 0
            size_info = getattr(self, "_size_info", {})
            for sku in target:
                m = master.get(_norm(sku))
                if not m:
                    continue
                matched += 1
                conf = 1.0 if m.category_source in ("shopify", "cin7") else 0.7
                cat_rows.append({
                    "sku": sku, "product_name": m.product_name,
                    "standard_category": m.standard_category, "category_group": m.category_group,
                    "designed_for": m.designed_for, "sold_as_vendor": m.sold_as_vendor,
                    "confidence": conf,
                })
                size_code, size_type = size_info.get(_norm(sku), (None, None))
                par_rows.append({
                    "sku": sku, "parent_sku": m.parent_sku, "size_code": size_code,
                    "size_type": size_type, "product_name": m.product_name,
                })

            if cat_rows:
                conn.execute(text("""
                    INSERT INTO category_mappings
                      (sku, product_name, standard_category, category_group, designed_for,
                       sold_as_vendor, mapping_source, confidence)
                    VALUES (:sku, :product_name, :standard_category, :category_group, :designed_for,
                            :sold_as_vendor, 'product_master', :confidence)
                    ON CONFLICT (sku) DO UPDATE SET
                      product_name=EXCLUDED.product_name,
                      standard_category=EXCLUDED.standard_category,
                      category_group=EXCLUDED.category_group,
                      designed_for=EXCLUDED.designed_for,
                      sold_as_vendor=EXCLUDED.sold_as_vendor,
                      mapping_source='product_master',
                      confidence=EXCLUDED.confidence
                """), cat_rows)
            if par_rows:
                conn.execute(text("""
                    INSERT INTO parent_sku_mappings
                      (sku, parent_sku, size_code, size_type, product_name, base_product_name, variant_count)
                    VALUES (:sku, :parent_sku, :size_code, :size_type, :product_name, :product_name, 1)
                    ON CONFLICT (sku) DO UPDATE SET
                      parent_sku=EXCLUDED.parent_sku,
                      size_code=EXCLUDED.size_code,
                      size_type=EXCLUDED.size_type,
                      product_name=EXCLUDED.product_name,
                      base_product_name=EXCLUDED.base_product_name
                """), par_rows)

            # backfill sales line items (case-insensitive match to master)
            conn.execute(text("""
                UPDATE sales_order_items i
                SET product_category = m.standard_category,
                    vendor = COALESCE(m.sold_as_vendor, i.vendor)
                FROM product_master m
                WHERE upper(btrim(i.sku)) = m.sku
            """))

        logger.info(f"Projected {matched}/{len(target)} target SKUs into mapping tables")
        return {"target_skus": len(target), "matched": matched}

    # ---------- orchestration ----------
    def run(self):
        from datetime import datetime
        counts = self.sync_catalogs()
        n = self.build_master()
        proj = self.project_to_mappings()
        db = SessionLocal()
        try:
            self._update_status(db, sync_in_progress=False, last_full_sync=datetime.now(),
                                 total_orders_synced=n, last_sync_orders_count=proj["matched"])
        finally:
            db.close()
        return {"catalog": counts, "master_skus": n, **proj}
