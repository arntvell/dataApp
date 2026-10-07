"""
Stock level synchronisation from Loom (truncate-and-reload).

Loom replaces Cin7 Core as the source of stock. It serves the full
variant x location matrix; this pipeline drops all-zero rows, classifies each
row so business views can hide non-merchandise, and replaces raw.stock_levels
wholesale — which is what Loom's own guide recommends over delta pulls.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from database.config import SessionLocal
from database.models import StockLevel, SyncStatus, CategoryMapping
from connectors.loom_connector import LoomConnector

logger = logging.getLogger(__name__)

SOURCE = "loom_stock"

# ---- Stock classification -------------------------------------------------
# Business views care about sellable merchandise. Production components, service
# SKUs and holding bins inflate every total (Cin7's button style alone was 95k
# units). Vintage and imperfect ARE merchandise, just separate lines, so they are
# classified rather than excluded and the API decides what to show by default.

CLASS_COMPONENT   = "component"     # production parts — never sellable
CLASS_SERVICE     = "service"       # pickup / giftwrap / service SKUs
CLASS_PLACEHOLDER = "placeholder"   # holding bins, incl. stock with no real SKU
CLASS_SAMPLE      = "sample"        # showroom/press samples, not for sale
CLASS_VINTAGE     = "vintage"       # second-hand / archive line
CLASS_IMPERFECT   = "imperfect"     # seconds
CLASS_MERCH       = "merch"         # core range

# Classes hidden from business views unless explicitly opted into.
NON_BUSINESS_CLASSES = {CLASS_COMPONENT, CLASS_SERVICE, CLASS_PLACEHOLDER, CLASS_SAMPLE}
# Real merchandise, but off by default — opt-in via the API's `include` param.
OPT_IN_CLASSES = {CLASS_VINTAGE}

SERVICE_SKUS = {"LIV-PCKUP", "GFTWRP", "LIV-SVD"}


def classify(sku: str, category_group: Optional[str]) -> str:
    """Bucket a SKU. Driven by SKU shape first, catalogue category second.

    Order matters: roughly a quarter of Loom's stock is SKUs the catalogue has
    never seen (future collections Loom holds before they are produced), so a
    classifier that leaned on category_group alone would be blind to them.
    """
    s = (sku or "").strip().upper()
    cat = (category_group or "").strip().upper()

    if s in SERVICE_SKUS:
        return CLASS_SERVICE
    if s.startswith("S-") or s == "S":
        return CLASS_COMPONENT
    if s.startswith("STORAGE-"):
        return CLASS_PLACEHOLDER
    if cat in {"BUTTON", "WRAPIN", "SAVED"}:
        return CLASS_COMPONENT if cat == "BUTTON" else CLASS_SERVICE
    if cat == "SAMPLE":
        return CLASS_SAMPLE
    if s.startswith("IMP-") or s.startswith("LIV-IMP-"):
        return CLASS_IMPERFECT
    if s.startswith("VN-") or cat == "VINTAGE":
        return CLASS_VINTAGE
    return CLASS_MERCH


class LoomStockSyncPipeline:
    """Replace raw.stock_levels with a fresh Loom snapshot."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connector = LoomConnector(config.get("loom", {}))

    def _get_sync_status(self, db, source: str) -> SyncStatus:
        status = db.query(SyncStatus).filter(SyncStatus.source_system == source).first()
        if not status:
            status = SyncStatus(source_system=source)
            db.add(status)
            db.commit()
            db.refresh(status)
        return status

    def _update_sync_status(self, db, source: str, **kwargs):
        status = self._get_sync_status(db, source)
        for key, value in kwargs.items():
            if hasattr(status, key):
                setattr(status, key, value)
        db.commit()

    def sync_stock_levels(self):
        """Full snapshot pull, classify, truncate-and-reload."""
        if not self.connector.authenticate():
            logger.error("Loom authentication failed — stock not synced")
            return

        db = SessionLocal()
        try:
            self._update_sync_status(db, SOURCE, sync_in_progress=True, last_error=None)

            snapshot = self.connector.get_stock_levels()
            rows = snapshot["rows"]
            as_of = _parse_ts(snapshot.get("as_of"))
            logger.info(f"Fetched {len(rows)} stock rows from Loom (asOf={snapshot.get('as_of')})")

            if not rows:
                # Never blank the table on an empty answer — that would read as
                # "no stock anywhere" across every module.
                raise RuntimeError("Loom returned zero stock rows; keeping previous snapshot")

            cat_by_sku = {
                (sku or "").strip().upper(): grp
                for sku, grp in db.query(CategoryMapping.sku, CategoryMapping.category_group).all()
            }

            db.query(StockLevel).delete()

            kept, skipped_zero = 0, 0
            seen = set()
            payload = []
            for r in rows:
                sku = (r.get("variant_sku") or "").strip()
                location = (r.get("location_name") or "").strip() or "Unknown"
                if not sku:
                    continue

                on_hand = _num(r.get("on_hand"))
                reserved = _num(r.get("reserved"))
                available = _num(r.get("available"))

                # Loom serves the whole matrix; ~3 in 4 rows are all-zero and would
                # inflate every SKU count. Drop them, as the Cin7 feed effectively did.
                if on_hand == 0 and available == 0 and reserved == 0:
                    skipped_zero += 1
                    continue

                # (sku, location) is unique; Loom keys on (variant_id, location_id),
                # so a SKU reused across two variant ids would collide here.
                key = (sku.upper(), location)
                if key in seen:
                    continue
                seen.add(key)

                payload.append({
                    "sku": sku,
                    "location": location,
                    "location_id": r.get("location_id"),
                    "source": r.get("source"),
                    "on_hand": on_hand,
                    "allocated": reserved,
                    "available": available,
                    "colorway_sku": r.get("colorway_sku"),
                    "style_sku": r.get("style_sku"),
                    "product_name": r.get("colorway_name"),
                    "style_name": r.get("style_name"),
                    "brand": r.get("brand"),
                    "stock_class": classify(sku, cat_by_sku.get(sku.upper())),
                    "archived": bool(r.get("archived")),
                    "as_of": as_of,
                })
                kept += 1

            db.bulk_insert_mappings(StockLevel, payload)
            db.commit()

            self._update_sync_status(
                db, SOURCE,
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=kept,
            )
            logger.info(
                f"Loom stock sync complete: {kept} rows loaded, {skipped_zero} all-zero rows skipped"
            )

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, SOURCE, sync_in_progress=False, last_error=str(e))
            logger.error(f"Loom stock sync failed: {e}")
        finally:
            db.close()


def _num(v) -> float:
    """Loom sends null for figures the owning system does not report."""
    return float(v) if v is not None else 0.0


def _parse_ts(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
