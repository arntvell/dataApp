"""
Seasonal Sale Planner API.

Builds the Brand -> Gender -> Style -> Variant sale-planning hierarchy from the
product SSOT + live stock + sales history, persists curation (included) and
per-round % discounts (1-6 rounds, set per style and cascading to variants with
optional per-variant override), and exports Sitoo / Shopify price-list CSVs.
"""
import csv
import io
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, Body, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func, and_, or_, distinct, case
from sqlalchemy.orm import Session

from database.config import get_db
from database.models import (
    ProductMaster, ParentSkuMapping, Cin7Stock, RawShopifyProduct,
    SalesOrder, SalesOrderItem,
    SaleSeason, SalePlanItem, SaleVariantOverride,
)
from api.stock import PHYSICAL_LOCATIONS, _pm_sku, _since

# Shopify collection-season tags look like ss20 / FW24 / AW23 (not the SALE_* tags)
_SEASON_RE = re.compile(r"^(SS|FW|AW|HO|PRE|RESORT)\s?\d{2}$", re.IGNORECASE)


def _pick_season(tokens):
    """From a set of season tags pick the earliest (origin collection)."""
    if not tokens:
        return None
    def yr(t):
        m = re.search(r"\d{2}$", t)
        return int(m.group()) if m else 99
    return sorted(tokens, key=lambda t: (yr(t), t))[0]

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard/sale", tags=["Sale Planner"])

MAX_ROUNDS = 6
AGE_CUTOFF_YEARS = 6        # first sold this many years ago or older => "old"
CARRYOVER_YEARS = 4         # sold across this many distinct years => likely carry-over
VELOCITY_DAYS = 365         # window for the "sold" velocity column

# Noise: stored items that aren't sellable products — excluded from candidates & exports.
NOISE_BRANDS = {"EXT", "LAGER", "STORAGE", "WRAPIN", "--", "—", "", "NONE"}
NOISE_SKU_PREFIXES = ("B2B", "OLD", "IMP")
NOISE_SKU_CONTAINS = (
    "LIV-IMP", "LIV-SKRD", "LIV-MSCSMPL", "2526-1208", "LIV-PCKUP",
    "LIV-REPS", "LIV-RSDIST", "LIV-SVD", "LIV-SLGSV", "LIV-SMPLS",
)


def _is_noise(parent_sku, brand):
    if (brand or "").strip().upper() in NOISE_BRANDS:
        return True
    p = (parent_sku or "").upper()
    if p.startswith(NOISE_SKU_PREFIXES):
        return True
    return any(tok in p for tok in NOISE_SKU_CONTAINS)


# ---------- helpers ----------

def _clean_rounds(rounds) -> list:
    if not isinstance(rounds, list):
        return []
    out = []
    for r in rounds[:MAX_ROUNDS]:
        if not isinstance(r, dict):
            continue
        out.append({
            "label": str(r.get("label") or f"Round {len(out)+1}"),
            "pct": float(r["pct"]) if r.get("pct") is not None else None,
            "date": r.get("date"),
        })
    return out


def _resolve_pct(season_rounds, style_pcts, variant_pcts, i):
    """variant override -> style -> season default, for round index i."""
    def at(arr):
        return arr[i] if isinstance(arr, list) and i < len(arr) and arr[i] is not None else None
    v = at(variant_pcts)
    if v is not None:
        return v
    s = at(style_pcts)
    if s is not None:
        return s
    return season_rounds[i].get("pct") if i < len(season_rounds) else None


def _sale_price(regular, pct):
    if regular is None or pct is None:
        return None
    return round(float(regular) * (1 - float(pct) / 100.0))


def _season_or_404(db, season_id):
    s = db.query(SaleSeason).get(season_id)
    if not s:
        raise HTTPException(404, "Season not found")
    return s


# ---------- seasons ----------

@router.get("/seasons")
async def list_seasons(db: Session = Depends(get_db)):
    rows = db.query(SaleSeason).order_by(SaleSeason.created_at.desc()).all()
    return [_season_dict(s) for s in rows]


def _season_dict(s):
    return {
        "id": s.id, "name": s.name,
        "starts_on": s.starts_on.isoformat() if s.starts_on else None,
        "status": s.status, "shopify_sale_tag": s.shopify_sale_tag,
        "rounds": s.rounds or [],
    }


@router.post("/seasons")
async def create_season(payload: dict = Body(...), db: Session = Depends(get_db)):
    rounds = _clean_rounds(payload.get("rounds") or [])
    if not rounds:
        rounds = [{"label": "Round 1", "pct": 20, "date": None}]
    starts = payload.get("starts_on")
    s = SaleSeason(
        name=payload.get("name") or "Untitled sale",
        starts_on=date.fromisoformat(starts) if starts else None,
        status=payload.get("status") or "draft",
        shopify_sale_tag=payload.get("shopify_sale_tag"),
        rounds=rounds,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    return _season_dict(s)


@router.get("/seasons/{season_id}")
async def get_season(season_id: int, db: Session = Depends(get_db)):
    return _season_dict(_season_or_404(db, season_id))


@router.put("/seasons/{season_id}")
async def update_season(season_id: int, payload: dict = Body(...), db: Session = Depends(get_db)):
    s = _season_or_404(db, season_id)
    if "name" in payload:
        s.name = payload["name"]
    if "starts_on" in payload:
        s.starts_on = date.fromisoformat(payload["starts_on"]) if payload["starts_on"] else None
    if "status" in payload:
        s.status = payload["status"]
    if "shopify_sale_tag" in payload:
        s.shopify_sale_tag = payload["shopify_sale_tag"]
    if "rounds" in payload:
        rounds = _clean_rounds(payload["rounds"])
        if not (1 <= len(rounds) <= MAX_ROUNDS):
            raise HTTPException(400, f"A sale must have 1–{MAX_ROUNDS} rounds")
        s.rounds = rounds
    db.commit()
    db.refresh(s)
    return _season_dict(s)


# ---------- candidate aggregation (shared by /candidates and exports) ----------

def _aggregate_styles(db):
    """Per parent SKU: attrs, on-hand (physical), sold (velocity window), age signals."""
    stock = dict(db.query(
        ProductMaster.parent_sku, func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).join(
        ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)
    ).filter(
        Cin7Stock.location.in_(PHYSICAL_LOCATIONS), ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku).all())

    since = _since(VELOCITY_DAYS)
    sold = dict(db.query(
        ProductMaster.parent_sku, func.sum(SalesOrderItem.quantity)
    ).select_from(SalesOrderItem).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).join(ProductMaster, ProductMaster.sku == _pm_sku(SalesOrderItem.sku)).filter(
        SalesOrder.order_date >= since, ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku).all())

    # sold last 30 days per parent
    since30 = _since(30)
    sold30 = dict(db.query(
        ProductMaster.parent_sku, func.sum(SalesOrderItem.quantity)
    ).select_from(SalesOrderItem).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).join(ProductMaster, ProductMaster.sku == _pm_sku(SalesOrderItem.sku)).filter(
        SalesOrder.order_date >= since30, ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku).all())

    # sold split full-price vs discounted over the velocity window
    split = {}
    for p, disc_u, full_u in db.query(
        ProductMaster.parent_sku,
        func.sum(case((SalesOrderItem.discount_amount > 0, SalesOrderItem.quantity), else_=0)),
        func.sum(case((or_(SalesOrderItem.discount_amount == 0, SalesOrderItem.discount_amount.is_(None)),
                       SalesOrderItem.quantity), else_=0)),
    ).select_from(SalesOrderItem).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).join(ProductMaster, ProductMaster.sku == _pm_sku(SalesOrderItem.sku)).filter(
        SalesOrder.order_date >= since, ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku):
        split[p] = (int(disc_u or 0), int(full_u or 0))

    age = {}
    for p, firstsold, yrs in db.query(
        ProductMaster.parent_sku, func.min(SalesOrder.order_date),
        func.count(distinct(func.extract("year", SalesOrder.order_date)))
    ).select_from(SalesOrderItem).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id
    ).join(ProductMaster, ProductMaster.sku == _pm_sku(SalesOrderItem.sku)).filter(
        ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku):
        age[p] = (firstsold.year if firstsold else None, int(yrs or 0))

    # season tags per parent (from Shopify collection tags)
    season_tokens = {}
    for parent, tags in db.query(ProductMaster.parent_sku, RawShopifyProduct.tags).join(
        RawShopifyProduct, ProductMaster.sku == _pm_sku(RawShopifyProduct.sku)
    ).filter(ProductMaster.parent_sku.isnot(None), RawShopifyProduct.tags.isnot(None)):
        for t in (tags or "").split(","):
            t = t.strip()
            if _SEASON_RE.match(t):
                season_tokens.setdefault(parent, set()).add(t.upper().replace(" ", ""))

    styles = {}
    for parent, brand, gender, category, name, price in db.query(
        ProductMaster.parent_sku, func.min(ProductMaster.sold_as_vendor),
        func.min(ProductMaster.designed_for), func.min(ProductMaster.category_group),
        func.min(ProductMaster.product_name), func.max(ProductMaster.price)
    ).filter(ProductMaster.parent_sku.isnot(None)).group_by(ProductMaster.parent_sku):
        if _is_noise(parent, brand):
            continue
        first_yr, yrs_active = age.get(parent, (None, 0))
        cur_year = date.today().year
        styles[parent] = {
            "parent_sku": parent, "brand": brand or "—", "gender": gender or "unisex",
            "category": category or "Uncategorized", "name": name or parent,
            "price": float(price) if price else None,
            "on_hand": float(stock.get(parent, 0) or 0),
            "sold": int(sold.get(parent, 0) or 0),
            "sold_30d": int(sold30.get(parent, 0) or 0),
            "sold_disc": split.get(parent, (0, 0))[0],
            "sold_full": split.get(parent, (0, 0))[1],
            "first_sold_year": first_yr, "years_active": yrs_active,
            "season": _pick_season(season_tokens.get(parent)),
            "old_flag": bool(first_yr and first_yr <= cur_year - AGE_CUTOFF_YEARS),
            "carryover_flag": bool(yrs_active >= CARRYOVER_YEARS),
        }
    return styles


@router.get("/candidates")
async def candidates(
    season_id: int = Query(...),
    brand: str = Query(None), gender: str = Query(None), category: str = Query(None),
    collection: str = Query(None),  # product collection-season filter, e.g. SS25
    q: str = Query(None), in_stock: int = Query(1),
    carryover: str = Query("all"),  # all | yes | no  (user-assigned flag)
    db: Session = Depends(get_db),
):
    season = _season_or_404(db, season_id)
    rounds = season.rounds or []
    styles = _aggregate_styles(db)
    plan = {p.parent_sku: p for p in db.query(SalePlanItem).filter(SalePlanItem.season_id == season_id)}

    available_seasons = set()
    out = []
    for st in styles.values():
        if in_stock and st["on_hand"] <= 0:
            continue
        if st["season"]:
            available_seasons.add(st["season"])
        if brand and brand not in ("All", "") and st["brand"] != brand:
            continue
        if collection and collection not in ("All", "") and st["season"] != collection:
            continue
        if gender and gender not in ("All", "") and st["gender"] != gender:
            continue
        if category and category not in ("All", "") and st["category"] != category:
            continue
        if q and q.lower() not in (st["name"] or "").lower() and q.lower() not in (st["parent_sku"] or "").lower():
            continue

        pi = plan.get(st["parent_sku"])
        included = pi.included if pi else True          # everything included by default; user excludes
        is_carryover = bool(pi.is_carryover) if pi else False
        if carryover == "yes" and not is_carryover:
            continue
        if carryover == "no" and is_carryover:
            continue

        style_pcts = (pi.round_pcts if pi else None) or []
        resolved = [_resolve_pct(rounds, style_pcts, None, i) for i in range(len(rounds))]
        out.append({
            **st,
            "included": included,
            "is_carryover": is_carryover,
            "round_pcts": style_pcts,                       # raw style overrides (null=inherit)
            "resolved_pcts": resolved,                       # effective % per round
            "sale_prices": [_sale_price(st["price"], p) for p in resolved],
            "note": pi.note if pi else None,
        })

    out.sort(key=lambda r: (r["brand"], r["gender"], r["name"]))
    seasons_sorted = sorted(available_seasons,
                            key=lambda s: (int(re.search(r"\d{2}$", s).group()) if re.search(r"\d{2}$", s) else 99, s),
                            reverse=True)

    # header stats over the shown set: 30-day sold + sell-through split full/discounted.
    on_hand = sum(s["on_hand"] for s in out)
    sold_full = sum(s["sold_full"] for s in out)
    sold_disc = sum(s["sold_disc"] for s in out)
    sold_30d = sum(s["sold_30d"] for s in out)
    denom = sold_full + sold_disc + on_hand
    stats = {
        "sold_30d": sold_30d,
        "on_hand": round(on_hand),
        "sell_through_full": round(sold_full / denom * 100, 1) if denom else 0,
        "sell_through_disc": round(sold_disc / denom * 100, 1) if denom else 0,
    }
    return {"season": _season_dict(season), "count": len(out),
            "available_seasons": seasons_sorted, "stats": stats, "styles": out}


@router.get("/candidates/variants")
async def candidate_variants(
    season_id: int = Query(...), parent_sku: str = Query(...),
    db: Session = Depends(get_db),
):
    season = _season_or_404(db, season_id)
    rounds = season.rounds or []
    pi = db.query(SalePlanItem).filter(
        SalePlanItem.season_id == season_id, SalePlanItem.parent_sku == parent_sku).first()
    style_pcts = (pi.round_pcts if pi else None) or []

    variants = db.query(ParentSkuMapping.sku, ParentSkuMapping.size_code).filter(
        ParentSkuMapping.parent_sku == parent_sku).all()
    skus = [v.sku for v in variants] or [parent_sku]
    size_of = {v.sku: (v.size_code or v.sku) for v in variants}

    price = dict(db.query(ProductMaster.sku, ProductMaster.price).filter(
        ProductMaster.sku.in_([s.upper().strip() for s in skus])).all())
    stock = dict(db.query(_pm_sku(Cin7Stock.sku), func.sum(Cin7Stock.on_hand)).filter(
        _pm_sku(Cin7Stock.sku).in_([s.upper().strip() for s in skus]),
        Cin7Stock.location.in_(PHYSICAL_LOCATIONS)).group_by(_pm_sku(Cin7Stock.sku)).all())
    since = _since(VELOCITY_DAYS)
    sold = dict(db.query(SalesOrderItem.sku, func.sum(SalesOrderItem.quantity)).join(
        SalesOrder, SalesOrderItem.order_id == SalesOrder.id).filter(
        SalesOrderItem.sku.in_(skus), SalesOrder.order_date >= since).group_by(SalesOrderItem.sku).all())
    ov = {o.sku: o for o in db.query(SaleVariantOverride).filter(
        SaleVariantOverride.season_id == season_id, SaleVariantOverride.sku.in_(skus))}

    rows = []
    for sku in skus:
        up = sku.upper().strip()
        o = ov.get(sku)
        vpcts = (o.round_pcts if o else None) or []
        reg = float(price[up]) if price.get(up) else None
        resolved = [_resolve_pct(rounds, style_pcts, vpcts, i) for i in range(len(rounds))]
        rows.append({
            "sku": sku, "size": size_of.get(sku, sku),
            "on_hand": float(stock.get(up, 0) or 0), "sold": int(sold.get(sku, 0) or 0),
            "price": reg, "round_pcts": vpcts, "resolved_pcts": resolved,
            "sale_prices": [_sale_price(reg, p) for p in resolved],
            "excluded": bool(o.excluded) if o else False,
        })
    rows.sort(key=lambda r: str(r["size"]))
    return {"parent_sku": parent_sku, "rounds": rounds, "variants": rows}


# ---------- plan editing ----------

@router.put("/plan-item")
async def upsert_plan_item(payload: dict = Body(...), db: Session = Depends(get_db)):
    season_id = payload["season_id"]
    parent_sku = payload["parent_sku"]
    pi = db.query(SalePlanItem).filter(
        SalePlanItem.season_id == season_id, SalePlanItem.parent_sku == parent_sku).first()
    if not pi:
        pi = SalePlanItem(season_id=season_id, parent_sku=parent_sku)
        db.add(pi)
    if "included" in payload:
        pi.included = bool(payload["included"])
    if "is_carryover" in payload:
        pi.is_carryover = bool(payload["is_carryover"])
    if "round_pcts" in payload:
        pi.round_pcts = payload["round_pcts"]
    if "note" in payload:
        pi.note = payload["note"]
    db.commit()
    return {"ok": True}


@router.put("/variant-override")
async def upsert_variant_override(payload: dict = Body(...), db: Session = Depends(get_db)):
    season_id = payload["season_id"]
    sku = payload["sku"]
    o = db.query(SaleVariantOverride).filter(
        SaleVariantOverride.season_id == season_id, SaleVariantOverride.sku == sku).first()
    if not o:
        o = SaleVariantOverride(season_id=season_id, sku=sku)
        db.add(o)
    if "round_pcts" in payload:
        o.round_pcts = payload["round_pcts"]
    if "excluded" in payload:
        o.excluded = bool(payload["excluded"])
    db.commit()
    return {"ok": True}


@router.post("/bulk")
async def bulk_update(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Apply included / a single round % to many styles at once (one request).

    set: { included?: bool, round_index?: int, round_pct?: number|null }
    round_index+round_pct sets that one round on each style, preserving the others.
    """
    season = _season_or_404(db, payload["season_id"])
    n_rounds = len(season.rounds or [])
    parents = payload.get("parent_skus") or []
    setvals = payload.get("set") or {}
    n = 0
    for parent in parents:
        pi = db.query(SalePlanItem).filter(
            SalePlanItem.season_id == season.id, SalePlanItem.parent_sku == parent).first()
        if not pi:
            pi = SalePlanItem(season_id=season.id, parent_sku=parent)
            db.add(pi)
        if "included" in setvals:
            pi.included = bool(setvals["included"])
        if "is_carryover" in setvals:
            pi.is_carryover = bool(setvals["is_carryover"])
        if "round_index" in setvals:
            arr = list(pi.round_pcts or [])
            while len(arr) < n_rounds:
                arr.append(None)
            idx = int(setvals["round_index"])
            if 0 <= idx < n_rounds:
                rp = setvals.get("round_pct")
                arr[idx] = float(rp) if rp is not None else None
            pi.round_pcts = arr
        n += 1
    db.commit()
    return {"ok": True, "updated": n}


# ---------- export ----------

def _included_variant_rows(db, season, round_index):
    """All variants of included styles with the resolved sale price for one round."""
    rounds = season.rounds or []
    if not (0 <= round_index < len(rounds)):
        raise HTTPException(400, "Invalid round")
    styles = _aggregate_styles(db)
    plan = {p.parent_sku: p for p in db.query(SalePlanItem).filter(SalePlanItem.season_id == season.id)}

    included_parents = []
    for parent, st in styles.items():
        if st["on_hand"] <= 0:
            continue
        pi = plan.get(parent)
        included = pi.included if pi else True   # everything included unless explicitly excluded
        if included:
            included_parents.append(parent)

    if not included_parents:
        return []

    # variants of included styles
    vrows = db.query(
        ParentSkuMapping.sku, ParentSkuMapping.parent_sku, ParentSkuMapping.size_code,
        ProductMaster.product_name, ProductMaster.price,
    ).join(ProductMaster, ProductMaster.sku == _pm_sku(ParentSkuMapping.sku)).filter(
        ParentSkuMapping.parent_sku.in_(included_parents)).all()

    style_pcts = {parent: ((plan[parent].round_pcts if parent in plan else None) or []) for parent in included_parents}
    ov = {o.sku: o for o in db.query(SaleVariantOverride).filter(SaleVariantOverride.season_id == season.id)}

    rows = []
    for sku, parent, size, name, price in vrows:
        o = ov.get(sku)
        if o and o.excluded:
            continue
        vpcts = (o.round_pcts if o else None) or []
        pct = _resolve_pct(rounds, style_pcts.get(parent, []), vpcts, round_index)
        sale = _sale_price(price, pct)
        if sale is None:
            continue
        rows.append({
            "sku": sku, "name": name, "size": size, "regular": round(float(price)) if price else None,
            "pct": pct, "sale": sale,
        })
    return rows


def _csv_stream(header, rows, delimiter, filename):
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delimiter)
    w.writerow(header)
    w.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/export/shopify")
async def export_shopify(season_id: int = Query(...), round: int = Query(1), db: Session = Depends(get_db)):
    season = _season_or_404(db, season_id)
    rows = _included_variant_rows(db, season, round - 1)
    # Default Shopify price-update layout (Matrixify-compatible); adjust to your template.
    out = [[r["sku"], r["sale"], r["regular"]] for r in rows]
    return _csv_stream(["Variant SKU", "Variant Price", "Variant Compare At Price"], out, ",",
                       f"shopify_sale_round{round}.csv")


@router.get("/export/sitoo")
async def export_sitoo(season_id: int = Query(...), round: int = Query(1), db: Session = Depends(get_db)):
    season = _season_or_404(db, season_id)
    rows = _included_variant_rows(db, season, round - 1)
    out = [[r["sku"], r["sale"], r["regular"]] for r in rows]
    return _csv_stream(["sku", "price", "price_org"], out, ";", f"sitoo_sale_round{round}.csv")
