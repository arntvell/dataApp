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
from fastapi.responses import StreamingResponse, HTMLResponse
from sqlalchemy import func, and_, or_, distinct, case
from sqlalchemy.orm import Session

from database.config import get_db
from database.models import (
    ProductMaster, ParentSkuMapping, Cin7Stock, RawShopifyProduct,
    SalesOrder, SalesOrderItem,
    SaleSeason, SalePlanItem, SaleVariantOverride, SaleAllocation,
)
from api.stock import PHYSICAL_LOCATIONS, RETAIL_STORES, WAREHOUSE, _pm_sku, _since
from config import settings
from connectors.shopify_connector import ShopifyConnector
import threading
import time
import uuid

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

round_ = round  # builtin alias — some handlers take a `round` query param that shadows it

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
    for parent, brand, gender, category, name, price, image in db.query(
        ProductMaster.parent_sku, func.min(ProductMaster.sold_as_vendor),
        func.min(ProductMaster.designed_for), func.min(ProductMaster.category_group),
        func.min(ProductMaster.product_name), func.max(ProductMaster.price),
        func.max(ProductMaster.image_url)
    ).filter(ProductMaster.parent_sku.isnot(None)).group_by(ProductMaster.parent_sku):
        if _is_noise(parent, brand):
            continue
        first_yr, yrs_active = age.get(parent, (None, 0))
        cur_year = date.today().year
        styles[parent] = {
            "parent_sku": parent, "brand": brand or "—", "gender": gender or "unisex",
            "category": category or "Uncategorized", "name": name or parent,
            "price": float(price) if price else None,
            "image_url": image,
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


# ---------- validation & store allocation ----------

def _store_weights(db):
    """Normalized store weight = each store's units sold in the last 90 days (size/market proxy)."""
    since = _since(90)
    w = {s: 0.0 for s in RETAIL_STORES}
    for loc, q in db.query(SalesOrder.location, func.sum(SalesOrderItem.quantity)).join(
        SalesOrderItem, SalesOrderItem.order_id == SalesOrder.id
    ).filter(SalesOrder.order_date >= since, SalesOrder.location.in_(RETAIL_STORES)).group_by(SalesOrder.location):
        w[loc] = float(q or 0)
    tot = sum(w.values()) or 1.0
    return {s: w[s] / tot for s in RETAIL_STORES}


def _recommend_style(total_units, norm_w):
    """Distribute a style's total units across stores: coverage floor of 1, remainder sales-weighted."""
    stores = RETAIL_STORES
    n = len(stores)
    t = int(round(total_units or 0))
    alloc = {s: 0 for s in stores}
    if t <= 0:
        return alloc
    order = sorted(stores, key=lambda s: norm_w.get(s, 0), reverse=True)
    if t <= n:
        for s in order[:t]:
            alloc[s] = 1
        return alloc
    for s in stores:
        alloc[s] = 1                       # baseline coverage
    rem = t - n
    raw = {s: rem * norm_w.get(s, 0) for s in stores}
    for s in stores:
        alloc[s] += int(raw[s])
    leftover = rem - sum(int(raw[s]) for s in stores)
    for s in sorted(stores, key=lambda s: raw[s] - int(raw[s]), reverse=True)[:leftover]:
        alloc[s] += 1
    return alloc


@router.get("/validation")
async def validation(season_id: int = Query(...), db: Session = Depends(get_db)):
    """On-sale (with per-store/warehouse stock + recommended/saved store targets) and
    not-on-sale style lists, both sorted by brand, for final review + allocation."""
    season = _season_or_404(db, season_id)
    rounds = season.rounds or []
    styles = _aggregate_styles(db)
    plan = {p.parent_sku: p for p in db.query(SalePlanItem).filter(SalePlanItem.season_id == season_id)}

    stock_ps = {}
    for parent, loc, oh in db.query(
        ProductMaster.parent_sku, Cin7Stock.location, func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).join(ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)).filter(
        Cin7Stock.location.in_(PHYSICAL_LOCATIONS), ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku, Cin7Stock.location):
        stock_ps.setdefault(parent, {})[loc] = float(oh or 0)

    norm_w = _store_weights(db)
    saved = {}
    for a in db.query(SaleAllocation).filter(SaleAllocation.season_id == season_id):
        saved[(a.parent_sku, a.store)] = a.target_qty
    wh = WAREHOUSE[0]

    on_sale, not_on = [], []
    for parent, st in styles.items():
        if st["on_hand"] <= 0:
            continue
        loc_stock = stock_ps.get(parent, {})
        store_stock = {s: loc_stock.get(s, 0) for s in RETAIL_STORES}
        row = {
            "parent_sku": parent, "brand": st["brand"], "name": st["name"], "gender": st["gender"],
            "season": st["season"], "sold": st["sold"],
            "store_stock": store_stock, "warehouse": loc_stock.get(wh, 0), "total": st["on_hand"],
        }
        pi = plan.get(parent)
        included = pi.included if pi else True
        if included:
            style_pcts = (pi.round_pcts if pi else None) or []
            row["discount"] = _resolve_pct(rounds, style_pcts, None, 0) if rounds else None
            if any((parent, s) in saved for s in RETAIL_STORES):
                row["target"] = {s: saved.get((parent, s), 0) for s in RETAIL_STORES}
            else:
                row["target"] = _recommend_style(st["on_hand"], norm_w)
            on_sale.append(row)
        else:
            not_on.append(row)

    on_sale.sort(key=lambda r: (r["brand"], r["name"]))
    not_on.sort(key=lambda r: (r["brand"], r["name"]))
    return {"season": _season_dict(season), "stores": RETAIL_STORES, "warehouse": wh,
            "on_sale": on_sale, "not_on_sale": not_on}


@router.post("/allocation")
async def save_allocation(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Upsert store target quantities. items: [{parent_sku, store, qty}]"""
    season_id = payload["season_id"]
    for it in payload.get("items") or []:
        a = db.query(SaleAllocation).filter(
            SaleAllocation.season_id == season_id,
            SaleAllocation.parent_sku == it["parent_sku"],
            SaleAllocation.store == it["store"]).first()
        if not a:
            a = SaleAllocation(season_id=season_id, parent_sku=it["parent_sku"], store=it["store"])
            db.add(a)
        a.target_qty = int(it.get("qty") or 0)
    db.commit()
    return {"ok": True}


@router.post("/allocation/reset")
async def reset_allocation(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Discard manual store targets so the recommendation is used again."""
    db.query(SaleAllocation).filter(SaleAllocation.season_id == payload["season_id"]).delete()
    db.commit()
    return {"ok": True}


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

    # Variants of included styles. Source from product_master (has EVERY variant +
    # its price) and only borrow size_code from parent_sku_mappings — that mapping
    # table doesn't cover all variants, so joining through it silently dropped SKUs.
    vrows = db.query(
        ProductMaster.sku, ProductMaster.parent_sku, ProductMaster.product_name,
        ProductMaster.price, func.min(ParentSkuMapping.size_code),
    ).outerjoin(
        ParentSkuMapping, _pm_sku(ParentSkuMapping.sku) == ProductMaster.sku
    ).filter(ProductMaster.parent_sku.in_(included_parents)).group_by(
        ProductMaster.sku, ProductMaster.parent_sku, ProductMaster.product_name, ProductMaster.price
    ).all()

    style_pcts = {parent: ((plan[parent].round_pcts if parent in plan else None) or []) for parent in included_parents}
    ov = {(o.sku or "").upper().strip(): o
          for o in db.query(SaleVariantOverride).filter(SaleVariantOverride.season_id == season.id)}

    rows = []
    for sku, parent, name, price, size in vrows:
        o = ov.get((sku or "").upper().strip())
        if o and o.excluded:
            continue
        vpcts = (o.round_pcts if o else None) or []
        pct = _resolve_pct(rounds, style_pcts.get(parent, []), vpcts, round_index)
        if not pct or pct <= 0:
            continue  # 0% or unset for this round => the variant is NOT part of this round
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


SITOO_PRICELIST_ID = 4  # the Sitoo price list we update for the sale


@router.get("/export/sitoo")
async def export_sitoo(season_id: int = Query(...), round: int = Query(1), db: Session = Depends(get_db)):
    """Sitoo pricelist import for one round: pricelistid, productid (Sitoo's id), sku,
    moneyprice (whole-number sale price). Comma-separated. Only variants that exist in Sitoo."""
    season = _season_or_404(db, season_id)
    rows = _included_variant_rows(db, season, round - 1)
    skus_up = list({r["sku"].upper().strip() for r in rows})
    sid = {}
    if skus_up:
        for sku_up, spid in db.query(
            _pm_sku(ProductMaster.sku), ProductMaster.sitoo_product_id
        ).filter(_pm_sku(ProductMaster.sku).in_(skus_up), ProductMaster.sitoo_product_id.isnot(None)):
            sid[sku_up] = spid
    out = []
    for r in rows:
        spid = sid.get(r["sku"].upper().strip())
        if not spid or r["sale"] is None:
            continue
        out.append([SITOO_PRICELIST_ID, spid, r["sku"], int(round_(r["sale"]))])
    return _csv_stream(["pricelistid", "productid", "sku", "moneyprice"], out, ",",
                       f"sitoo_pricelist{SITOO_PRICELIST_ID}_round{round}.csv")


# ---------- store-manager list (#4) & transfer plan (#5) ----------

@router.get("/stores")
async def list_stores():
    return {"stores": RETAIL_STORES, "warehouse": WAREHOUSE[0]}


def _sale_export_rows(db, season):
    """Included, in-stock styles with per-store/warehouse stock, store target (saved or
    recommended), and resolved % per round. Shared by store list + transfer plan."""
    rounds = season.rounds or []
    styles = _aggregate_styles(db)
    plan = {p.parent_sku: p for p in db.query(SalePlanItem).filter(SalePlanItem.season_id == season.id)}
    stock_ps = {}
    for parent, loc, oh in db.query(
        ProductMaster.parent_sku, Cin7Stock.location, func.sum(Cin7Stock.on_hand)
    ).select_from(Cin7Stock).join(ProductMaster, ProductMaster.sku == _pm_sku(Cin7Stock.sku)).filter(
        Cin7Stock.location.in_(PHYSICAL_LOCATIONS), ProductMaster.parent_sku.isnot(None)
    ).group_by(ProductMaster.parent_sku, Cin7Stock.location):
        stock_ps.setdefault(parent, {})[loc] = float(oh or 0)
    norm_w = _store_weights(db)
    saved = {}
    for a in db.query(SaleAllocation).filter(SaleAllocation.season_id == season.id):
        saved[(a.parent_sku, a.store)] = a.target_qty
    wh = WAREHOUSE[0]

    rows = []
    for parent, st in styles.items():
        if st["on_hand"] <= 0:
            continue
        pi = plan.get(parent)
        if not (pi.included if pi else True):
            continue
        loc_stock = stock_ps.get(parent, {})
        store_stock = {s: round(loc_stock.get(s, 0)) for s in RETAIL_STORES}
        if any((parent, s) in saved for s in RETAIL_STORES):
            target = {s: int(saved.get((parent, s), 0)) for s in RETAIL_STORES}
        else:
            target = _recommend_style(st["on_hand"], norm_w)
        style_pcts = (pi.round_pcts if pi else None) or []
        resolved = [_resolve_pct(rounds, style_pcts, None, i) for i in range(len(rounds))]
        rows.append({
            "parent_sku": parent, "brand": st["brand"], "name": st["name"], "price": st["price"],
            "image_url": st.get("image_url"),
            "store_stock": store_stock, "warehouse": round(loc_stock.get(wh, 0)),
            "target": target, "resolved": resolved,
        })
    rows.sort(key=lambda r: (r["brand"], r["name"]))
    return rows, rounds, wh


def _compute_transfers(rows, wh):
    """Greedy per-style transfers: fill each store's shortfall from the warehouse first,
    then from stores holding more than their target."""
    moves = []
    for r in rows:
        need = {s: max(0, r["target"][s] - r["store_stock"][s]) for s in RETAIL_STORES}
        excess = {s: max(0, r["store_stock"][s] - r["target"][s]) for s in RETAIL_STORES}
        wh_avail = r["warehouse"]
        for dest in RETAIL_STORES:
            n = need[dest]
            if n <= 0:
                continue
            take = min(n, wh_avail)
            if take > 0:
                moves.append({"from": wh, "to": dest, "brand": r["brand"], "name": r["name"],
                              "parent_sku": r["parent_sku"], "qty": take})
                wh_avail -= take
                n -= take
            for src in RETAIL_STORES:
                if n <= 0:
                    break
                if src == dest or excess[src] <= 0:
                    continue
                t = min(n, excess[src])
                moves.append({"from": src, "to": dest, "brand": r["brand"], "name": r["name"],
                              "parent_sku": r["parent_sku"], "qty": t})
                excess[src] -= t
                n -= t
    return moves


@router.get("/export/transfers.csv")
async def export_transfers_csv(season_id: int = Query(...), db: Session = Depends(get_db)):
    season = _season_or_404(db, season_id)
    rows, _, wh = _sale_export_rows(db, season)
    moves = _compute_transfers(rows, wh)
    out = [[m["from"], m["to"], m["brand"], m["name"], m["parent_sku"], m["qty"]] for m in moves]
    return _csv_stream(["From", "To", "Brand", "Style", "SKU", "Qty"], out, ",",
                       f"sale_transfers_{season.id}.csv")


def _disc_color(pct):
    """Each whole discount % gets its own colour: green (low) → red (deep)."""
    if pct is None:
        return "#f3f4f6"
    h = max(0, 130 - float(pct) * 1.7)   # 0% green-ish → high% red
    return f"hsl({h:.0f} 75% 88%)"


_PRINT_CSS = """
<style>
  body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px;color:#111827}
  h1{font-size:20px;margin:0 0 2px} .sub{color:#6b7280;font-size:13px;margin-bottom:16px}
  table{border-collapse:collapse;width:100%;font-size:13px} th,td{padding:6px 10px;text-align:left;border-bottom:1px solid #e5e7eb}
  th{background:#f9fafb;font-size:11px;text-transform:uppercase;color:#6b7280}
  td.r,th.r{text-align:right} .brand{background:#eef2ff;font-weight:600}
  .new{font-size:9px;font-weight:700;padding:1px 6px;border-radius:4px;background:#dcfce7;color:#15803d;margin-left:6px}
  .thumb{width:34px;height:34px;object-fit:cover;border-radius:6px;border:1px solid #e5e7eb;vertical-align:middle;margin-right:8px}
  .sku{font-size:10px;color:#9ca3af;font-family:monospace}
  .was{text-decoration:line-through;color:#9ca3af}
  .pill{padding:1px 8px;border-radius:9999px;font-weight:600}
  .noprint{margin-bottom:16px} @media print{.noprint{display:none}}
  button{padding:6px 12px;border:1px solid #d1d5db;border-radius:8px;background:#2563eb;color:#fff;cursor:pointer}
  .rnav{display:flex;flex-wrap:wrap;gap:6px;margin:10px 0 16px}
  .rnav a{font-size:12px;padding:4px 10px;border:1px solid #d1d5db;border-radius:9999px;color:#374151;text-decoration:none}
  .rnav a.on{background:#2563eb;color:#fff;border-color:#2563eb}
</style>
"""


@router.get("/export/store-list", response_class=HTMLResponse)
async def export_store_list(season_id: int = Query(...), store: str = Query(...),
                            round_no: int = Query(1, alias="round"), db: Session = Depends(get_db)):
    season = _season_or_404(db, season_id)
    rows, rounds, wh = _sale_export_rows(db, season)
    ri = round_no - 1
    if not (0 <= ri < len(rounds)):
        raise HTTPException(400, "Invalid round")

    items = []
    for r in rows:
        have = r["store_stock"].get(store, 0)
        incoming = max(0, r["target"].get(store, 0) - have)
        if have <= 0 and incoming <= 0:
            continue
        pct = r["resolved"][ri]
        nxt = r["resolved"][ri + 1] if ri + 1 < len(rounds) else None
        items.append({
            "brand": r["brand"], "name": r["name"], "sku": r["parent_sku"], "image": r.get("image_url"),
            "have": have, "incoming": incoming, "pct": pct,
            "sale": _sale_price(r["price"], pct), "regular": round(r["price"]) if r["price"] else None,
            "new": have <= 0 and incoming > 0,
            "next": nxt if (nxt is not None and nxt != pct) else None,
        })

    body = []
    last_brand = None
    for it in items:
        if it["brand"] != last_brand:
            body.append(f'<tr class="brand"><td colspan="6">{it["brand"]}</td></tr>')
            last_brand = it["brand"]
        badge = '<span class="new">NEW</span>' if it["new"] else ""
        pill = f'<span class="pill" style="background:{_disc_color(it["pct"])}">{it["pct"]:.0f}%</span>' if it["pct"] is not None else "—"
        recv = f'+{it["incoming"]}' if it["incoming"] else ""
        img = f'<img src="{it["image"]}" class="thumb">' if it["image"] else ""
        body.append(
            f'<tr><td>{img}{it["name"]}{badge}<div class="sku">{it["sku"]}</div></td>'
            f'<td class="r">{it["have"] or ""}</td><td class="r" style="color:#16a34a">{recv}</td>'
            f'<td class="r">{pill}</td><td class="r was">{it["regular"] or ""}</td>'
            f'<td class="r"><b>{it["sale"] if it["sale"] is not None else ""}</b></td></tr>'
        )

    rlabel = (rounds[ri].get("label") or f"Round {round_no}")
    cur_def = rounds[ri].get("pct")
    new_count = sum(1 for it in items if it["new"])
    sched = ""
    if ri + 1 < len(rounds):
        nd = rounds[ri + 1].get("pct")
        nl = rounds[ri + 1].get("label") or f"Round {round_no + 1}"
        if nd is not None:
            sched = f' &nbsp;·&nbsp; then <b>{nd:.0f}% off</b> at {nl}'
    head_line = f'<b>{rlabel}: {cur_def:.0f}% off</b>' if cur_def is not None else f'<b>{rlabel}</b>'
    counts = f'{len(items)} styles' + (f' &nbsp;·&nbsp; {new_count} new to this store' if new_count else '')
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>{store} — {season.name}</title>{_PRINT_CSS}</head>
    <body><div class="noprint"><button onclick="window.print()">Print / Save as PDF</button></div>
    <h1>{store} — {season.name}</h1>
    <div class="sub">{head_line}{sched}</div>
    <div class="sub">{counts}</div>
    <table><thead><tr><th>Style</th><th class="r">Have</th><th class="r">Incoming</th>
    <th class="r">Off</th><th class="r">Was</th><th class="r">Now</th></tr></thead>
    <tbody>{''.join(body)}</tbody></table></body></html>"""
    return HTMLResponse(html)


def _round_nav(season_id, rounds, active):
    """Print-hidden links to switch between the full list and each single round."""
    def link(r, label):
        cls = "on" if r == active else ""
        return f'<a class="{cls}" href="?season_id={season_id}&round={r}">{label}</a>'
    parts = [link(0, "Full (all rounds)")]
    for i, rd in enumerate(rounds):
        parts.append(link(i + 1, rd.get("label") or f"Round {i+1}"))
    return '<div class="noprint rnav">' + " ".join(parts) + "</div>"


@router.get("/export/price-schedule", response_class=HTMLResponse)
async def export_price_schedule(season_id: int = Query(...), round: int = Query(0),
                                embed: int = Query(0), q: str = Query(None),
                                db: Session = Depends(get_db)):
    """Sale list. round=0 → master list with every round's discount/price (colour-coded).
    round=N → a single-round pick list (only that round's items, WAS/NOW/% off + stock).
    embed=1 hides the round-switcher nav (the host page provides its own).
    q filters by product name, SKU or brand."""
    season = _season_or_404(db, season_id)
    rows, rounds, wh = _sale_export_rows(db, season)
    rows = sorted(rows, key=lambda r: ((r["brand"] or "").lower(), (r["name"] or "").lower()))
    ql = (q or "").strip().lower()
    if ql:
        rows = [r for r in rows if ql in (r["name"] or "").lower()
                or ql in (r["parent_sku"] or "").lower() or ql in (r["brand"] or "").lower()]
    nav = "" if embed else _round_nav(season_id, rounds, round if 1 <= round <= len(rounds) else 0)

    # ---- Single-round pick list ----
    if 1 <= round <= len(rounds):
        i = round - 1
        rd = rounds[i]
        rlabel = rd.get("label") or f"Round {round}"
        picks = [r for r in rows if r["resolved"][i] is not None and r["resolved"][i] > 0]
        body, last_brand, total_units = [], None, 0
        for r in picks:
            if r["brand"] != last_brand:
                body.append(f'<tr class="brand"><td colspan="6">{r["brand"]}</td></tr>')
                last_brand = r["brand"]
            pct = r["resolved"][i]
            reg = round_(r["price"]) if r["price"] else None
            sale = _sale_price(r["price"], pct)
            wh_qty = r["warehouse"]
            store_qty = sum(r["store_stock"].values())
            total_units += wh_qty + store_qty
            body.append(
                f'<tr><td>{r["name"]}<div class="sku">{r["parent_sku"]}</div></td>'
                f'<td class="r was">{reg or ""}</td>'
                f'<td class="r" style="background:{_disc_color(pct)}"><b>{sale if sale is not None else ""}</b></td>'
                f'<td class="r"><b>{pct:.0f}%</b></td>'
                f'<td class="r">{store_qty}</td>'
                f'<td class="r">{wh_qty}</td></tr>'
            )
        if not body:
            body.append(f'<tr><td colspan="6" style="color:#9ca3af;padding:16px">No matches{f" for “{q}”" if ql else ""}.</td></tr>')
        html = f"""<!doctype html><html><head><meta charset="utf-8"><title>{rlabel} — {season.name}</title>{_PRINT_CSS}</head>
    <body><div class="noprint"><button onclick="window.print()">Print / Save as PDF</button></div>{nav}
    <h1>{rlabel} sale list — {season.name}</h1>
    <div class="sub">{len(picks)} styles on sale this round at <b>{(rd.get('pct') or 0):.0f}% off</b> · by brand · Stores/Lager = units to gather</div>
    <table><thead><tr><th>Style</th><th class="r">Was</th><th class="r">Now</th><th class="r">Off</th><th class="r">Stores</th><th class="r">Lager</th></tr></thead>
    <tbody>{''.join(body)}</tbody></table></body></html>"""
        return HTMLResponse(html)

    # ---- Full master list (all rounds) ----
    round_heads = "".join(f'<th class="r">{(rd.get("label") or f"R{i+1}")}</th>' for i, rd in enumerate(rounds))
    ncols = 2 + len(rounds)
    body = []
    last_brand = None
    for r in rows:
        if r["brand"] != last_brand:
            body.append(f'<tr class="brand"><td colspan="{ncols}">{r["brand"]}</td></tr>')
            last_brand = r["brand"]
        reg = round_(r["price"]) if r["price"] else None
        cells = ""
        for i in range(len(rounds)):
            pct = r["resolved"][i]
            if pct is None or pct <= 0:
                cells += '<td class="r">—</td>'
                continue
            sale = _sale_price(r["price"], pct)
            cells += (f'<td class="r" style="background:{_disc_color(pct)}">'
                      f'<b>{sale if sale is not None else ""}</b>'
                      f'<div style="font-size:10px;color:#374151">{pct:.0f}% off</div></td>')
        body.append(
            f'<tr><td>{r["name"]}<div class="sku">{r["parent_sku"]}</div></td>'
            f'<td class="r was">{reg or ""}</td>{cells}</tr>'
        )
    if not body:
        body.append(f'<tr><td colspan="{ncols}" style="color:#9ca3af;padding:16px">No matches{f" for “{q}”" if ql else ""}.</td></tr>')
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Sale list — {season.name}</title>{_PRINT_CSS}</head>
    <body><div class="noprint"><button onclick="window.print()">Print / Save as PDF</button></div>{nav}
    <h1>Sale list — {season.name}</h1>
    <div class="sub">{len(rows)} styles on sale · by brand · colour = discount depth</div>
    <table><thead><tr><th>Style</th><th class="r">Was</th>{round_heads}</tr></thead>
    <tbody>{''.join(body)}</tbody></table></body></html>"""
    return HTMLResponse(html)


@router.get("/export/transfers", response_class=HTMLResponse)
async def export_transfers_html(season_id: int = Query(...), db: Session = Depends(get_db)):
    season = _season_or_404(db, season_id)
    rows, _, wh = _sale_export_rows(db, season)
    moves = sorted(_compute_transfers(rows, wh), key=lambda m: (m["to"], m["brand"], m["name"]))
    body, last = [], None
    for m in moves:
        if m["to"] != last:
            body.append(f'<tr class="brand"><td colspan="4">→ {m["to"]}</td></tr>')
            last = m["to"]
        body.append(f'<tr><td>{m["brand"]}</td><td>{m["name"]}'
                     f'<div style="font-size:10px;color:#9ca3af;font-family:monospace">{m["parent_sku"]}</div></td>'
                     f'<td>{m["from"]}</td><td class="r"><b>{m["qty"]}</b></td></tr>')
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Transfers — {season.name}</title>{_PRINT_CSS}</head>
    <body><div class="noprint"><button onclick="window.print()">Print / Save as PDF</button></div>
    <h1>Store transfer plan — {season.name}</h1>
    <div class="sub">{len(moves)} moves · grouped by destination store</div>
    <table><thead><tr><th>Brand</th><th>Style</th><th>From</th><th class="r">Qty</th></tr></thead>
    <tbody>{''.join(body)}</tbody></table></body></html>"""
    return HTMLResponse(html)


# ============== Sale performance (actuals per round + full list) ==============
# Measures how the sale actually sold: for each round's date window, the units /
# net revenue / realized markdown / sell-through of the variants that were on sale
# in THAT round, plus a per-style full list across the whole sale.

def _round_windows(season):
    """Ordered (start_date, end_date) per round. A round's window runs from its own
    date (round 1 falls back to season.starts_on) until the next dated round's date,
    the last one until today. Undated rounds return (None, None)."""
    rounds = season.rounds or []
    today = date.today()
    raw = []
    for i, r in enumerate(rounds):
        d = r.get("date")
        try:
            d = date.fromisoformat(d) if d else (season.starts_on if i == 0 else None)
        except (TypeError, ValueError):
            d = season.starts_on if i == 0 else None
        raw.append(d)
    windows = []
    for i, s in enumerate(raw):
        if not s:
            windows.append((None, None))
            continue
        end = None
        for j in range(i + 1, len(raw)):
            if raw[j] and raw[j] > s:
                end = raw[j]
                break
        windows.append((s, end or (today + timedelta(days=1))))
    return windows


def _variant_round_matrix(db, season, styles, plan):
    """For every variant of every included, in-stock style: its parent, regular price,
    and the resolved discount % for each round. Single pass (respects variant overrides
    & exclusions). Returns (sku_meta, round_sku_sets) where sku_meta[sku_upper] =
    {parent, regular, resolved:[pct...]} and round_sku_sets[i] = set(sku_upper on sale)."""
    rounds = season.rounds or []
    included_parents = [
        p for p, st in styles.items()
        if st["on_hand"] > 0 and (plan[p].included if p in plan else True)
    ]
    sku_meta = {}
    round_sku_sets = [set() for _ in rounds]
    if not included_parents:
        return sku_meta, round_sku_sets

    style_pcts = {p: ((plan[p].round_pcts if p in plan else None) or []) for p in included_parents}
    ov = {(o.sku or "").upper().strip(): o
          for o in db.query(SaleVariantOverride).filter(SaleVariantOverride.season_id == season.id)}

    vrows = db.query(
        ProductMaster.sku, ProductMaster.parent_sku, ProductMaster.price
    ).filter(ProductMaster.parent_sku.in_(included_parents)).all()

    for sku, parent, price in vrows:
        up = (sku or "").upper().strip()
        if not up:
            continue
        o = ov.get(up)
        if o and o.excluded:
            continue
        vpcts = (o.round_pcts if o else None) or []
        resolved = [_resolve_pct(rounds, style_pcts.get(parent, []), vpcts, i) for i in range(len(rounds))]
        sku_meta[up] = {"parent": parent, "regular": float(price) if price else None, "resolved": resolved}
        for i, pct in enumerate(resolved):
            if pct and pct > 0:
                round_sku_sets[i].add(up)
    return sku_meta, round_sku_sets


def _blank_agg():
    return {"units": 0, "returns_units": 0, "net_revenue": 0.0,
            "full_value": 0.0, "recorded_discount": 0.0}


@router.get("/performance")
async def performance(season_id: int = Query(...), db: Session = Depends(get_db)):
    """Actual sales performance of a sale: per-round KPIs (over each round's date
    window, for the variants on sale that round) and a per-style full list across
    the whole sale. Revenue is net (VAT incl, returns netted via negative orders).
    Realized markdown = regular value − net revenue."""
    season = _season_or_404(db, season_id)
    rounds = season.rounds or []
    windows = _round_windows(season)
    styles = _aggregate_styles(db)
    plan = {p.parent_sku: p for p in db.query(SalePlanItem).filter(SalePlanItem.season_id == season_id)}
    sku_meta, round_sku_sets = _variant_round_matrix(db, season, styles, plan)

    # Whole-sale window: earliest dated round start → today (inclusive).
    dated_starts = [w[0] for w in windows if w[0]]
    min_start = min(dated_starts) if dated_starts else None
    global_end = date.today() + timedelta(days=1)

    # Style-level resolved pct per round (season/style, ignoring variant overrides).
    style_resolved = {}
    for parent in {m["parent"] for m in sku_meta.values()}:
        pcts = (plan[parent].round_pcts if parent in plan else None) or []
        style_resolved[parent] = [_resolve_pct(rounds, pcts, None, i) for i in range(len(rounds))]

    round_aggs = [_blank_agg() for _ in rounds]
    round_style_ids = [set() for _ in rounds]

    # Per-style totals (whole sale window) + per-round unit breakdown.
    style_tot = {}
    for up, m in sku_meta.items():
        parent = m["parent"]
        if parent not in style_tot:
            st = styles.get(parent, {})
            style_tot[parent] = {
                "parent_sku": parent, "brand": st.get("brand", "—"), "name": st.get("name", parent),
                "gender": st.get("gender", ""), "regular": st.get("price"),
                "on_hand": float(st.get("on_hand", 0) or 0),
                "resolved_pcts": style_resolved.get(parent, [None] * len(rounds)),
                "round_units": [0] * len(rounds),
                **_blank_agg(),
            }

    if min_start and sku_meta:
        start_dt = datetime.combine(min_start, datetime.min.time())
        union = list(sku_meta.keys())
        rows = db.query(
            _pm_sku(SalesOrderItem.sku), SalesOrder.order_date,
            SalesOrderItem.quantity, SalesOrderItem.unit_price,
            SalesOrderItem.discount_amount, SalesOrderItem.line_total,
        ).select_from(SalesOrderItem).join(
            SalesOrder, SalesOrderItem.order_id == SalesOrder.id
        ).filter(
            _pm_sku(SalesOrderItem.sku).in_(union),
            SalesOrder.order_date >= start_dt,
        ).all()

        for up, od, qty, unit_price, disc, line_total in rows:
            if not od:
                continue
            od = od.date() if hasattr(od, "date") else od
            if not (min_start <= od < global_end):
                continue
            m = sku_meta.get(up)
            if not m:
                continue
            qty = int(qty or 0)
            reg = m["regular"]
            lt = float(line_total or 0)
            dsc = float(disc or 0)

            def apply(agg):
                if qty >= 0:
                    agg["units"] += qty
                else:
                    agg["returns_units"] += -qty
                agg["net_revenue"] += lt
                if reg is not None:
                    agg["full_value"] += reg * qty
                agg["recorded_discount"] += dsc * qty

            stt = style_tot.get(m["parent"])
            if stt:
                apply(stt)

            # attribute to the round whose window contains the sale AND where the sku was on sale
            for i, (ws, we) in enumerate(windows):
                if ws and ws <= od < we and up in round_sku_sets[i]:
                    apply(round_aggs[i])
                    round_style_ids[i].add(m["parent"])
                    if qty >= 0:
                        stt["round_units"][i] += qty if stt else 0
                    break

    def finalize(agg):
        net_units = agg["units"] - agg["returns_units"]
        full = agg["full_value"]
        markdown = full - agg["net_revenue"]
        return {
            **agg,
            "net_units": net_units,
            "markdown": round(markdown),
            "avg_disc_pct": round(markdown / full * 100, 1) if full > 0 else 0,
            "net_revenue": round(agg["net_revenue"]),
            "full_value": round(full),
            "recorded_discount": round(agg["recorded_discount"]),
        }

    rounds_out = []
    for i, rd in enumerate(rounds):
        ws, we = windows[i]
        agg = finalize(round_aggs[i])
        rounds_out.append({
            "index": i,
            "label": rd.get("label") or f"Round {i + 1}",
            "pct": rd.get("pct"),
            "start": ws.isoformat() if ws else None,
            # inclusive last day; None once we're at/after the last boundary (round still open)
            "end": (we - timedelta(days=1)).isoformat() if (we and ws and we > ws) else None,
            "dated": bool(ws),
            "styles_on_sale": len({sku_meta[s]["parent"] for s in round_sku_sets[i]}),
            "variants_on_sale": len(round_sku_sets[i]),
            "styles_sold": len(round_style_ids[i]),
            **agg,
        })

    styles_out = []
    tot = _blank_agg()
    tot_onhand = 0.0
    for stt in style_tot.values():
        f = finalize(stt)
        f["on_hand"] = round(stt["on_hand"])
        sold = f["net_units"]
        denom = sold + stt["on_hand"]
        f["sell_through"] = round(sold / denom * 100, 1) if denom > 0 else 0
        f["regular"] = round(stt["regular"]) if stt["regular"] else None
        f["round_units"] = stt["round_units"]
        styles_out.append(f)
        for k in tot:
            tot[k] += stt[k]
        tot_onhand += stt["on_hand"]
    styles_out.sort(key=lambda r: r["net_revenue"], reverse=True)

    totals = finalize(tot)
    totals["on_hand"] = round(tot_onhand)
    denom = totals["net_units"] + tot_onhand
    totals["sell_through"] = round(totals["net_units"] / denom * 100, 1) if denom > 0 else 0
    totals["styles"] = len(styles_out)

    return {
        "season": _season_dict(season),
        "window": {"start": min_start.isoformat() if min_start else None,
                   "end": (global_end - timedelta(days=1)).isoformat()},
        "any_dated": bool(dated_starts),
        "rounds": rounds_out,
        "totals": totals,
        "styles": styles_out,
    }


# ============== Shopify tag push (newsletter pre-sale) ==============
# Adds/removes a product tag on every Shopify product that has a discount in a
# given sale round — e.g. to gate a "pre-sale" collection to newsletter subscribers.
# Runs as a background job (hundreds of products) with a polled status endpoint.

_TAG_JOBS = {}  # job_id -> {status, action, tag, total, done, ok, failed, failures, error}


def _round_shopify_products(db, season, round_index, tag=None, category_group=None):
    """Distinct Shopify products discounted in THIS round (resolved pct > 0), optionally
    narrowed to a category group. Everything is round-specific — a later round's additions
    never appear until that round. Returns (products, unmapped_count, already_tagged_count)."""
    rows = _included_variant_rows(db, season, round_index)
    skus_up = list({r["sku"].upper().strip() for r in rows})
    if not skus_up:
        return [], 0, 0

    base = [_pm_sku(ProductMaster.sku).in_(skus_up)]
    if category_group:
        base.append(ProductMaster.category_group == category_group)

    prod = {}
    for pid, title in db.query(
        ProductMaster.shopify_product_id, func.min(ProductMaster.product_name)
    ).filter(*base, ProductMaster.shopify_product_id.isnot(None)).group_by(ProductMaster.shopify_product_id):
        prod[pid] = title

    missing = db.query(func.count(func.distinct(_pm_sku(ProductMaster.sku)))).filter(
        *base, ProductMaster.shopify_product_id.is_(None)
    ).scalar() or 0

    already = 0
    if tag and prod:
        ids = list(prod.keys())
        tagged = {p for (p,) in db.query(func.distinct(RawShopifyProduct.product_id)).filter(
            RawShopifyProduct.product_id.in_(ids), RawShopifyProduct.tags.ilike(f"%{tag}%")
        )}
        already = len(tagged & set(ids))

    products = [{"product_id": pid, "title": title or pid} for pid, title in prod.items()]
    products.sort(key=lambda p: (p["title"] or "").lower())
    return products, int(missing), already


def _shopify_conn():
    cfg = settings.get_connector_configs().get("shopify", {})
    if not cfg.get("base_url") or not cfg.get("api_key"):
        return None
    return ShopifyConnector(cfg)


@router.get("/shopify-tag/preview")
async def shopify_tag_preview(season_id: int = Query(...), round: int = Query(1),
                              tag: str = Query(...), category: str = Query(None),
                              db: Session = Depends(get_db)):
    """Read-only: which Shopify products would be tagged for this round (no writes).
    Everything is round-specific; pass category=<group> to also narrow to that category."""
    season = _season_or_404(db, season_id)
    cat = (category or "").strip()
    products, missing, already = _round_shopify_products(
        db, season, round - 1, tag=tag.strip(), category_group=cat or None)
    scope = f"round {round}" + (f", {cat}" if cat else "")
    base = (settings.get_connector_configs().get("shopify", {}).get("base_url") or "").rstrip("/")
    admin_base = f"{base}/admin/products/" if base else None
    return {
        "season": season.name, "scope": scope, "round": round, "category": cat, "tag": tag.strip(),
        "total_products": len(products),
        "missing_shopify": missing,
        "already_tagged": already,
        "shopify_configured": bool(base),
        "admin_base": admin_base,
        "products": [{"title": p["title"], "product_id": p["product_id"]} for p in products],
    }


def _run_tag_job(job_id, product_ids, tag, remove):
    job = _TAG_JOBS[job_id]
    try:
        conn = _shopify_conn()
        if conn is None:
            job.update(status="error", error="Shopify is not configured on this server.")
            return

        def progress(done, ok, failed):
            job.update(done=done, ok=ok, failed=failed)

        res = conn.bulk_modify_product_tags(product_ids, [tag], remove=remove, on_progress=progress)
        if res.get("access_denied"):
            job.update(status="error",
                       error="Shopify rejected the write — the API token is missing the 'write_products' scope.",
                       done=res.get("done", 0), ok=res.get("ok", 0), failed=res.get("failed", 0))
            return
        job.update(status="done", done=res["done"], ok=res["ok"], failed=res["failed"],
                   failures=res["failures"][:50])
    except Exception as e:
        logger.exception("Tag job failed")
        job.update(status="error", error=str(e))


@router.post("/shopify-tag")
async def shopify_tag_start(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Start a background job that adds/removes a tag on this round's Shopify products."""
    season_id = payload.get("season_id")
    round_no = int(payload.get("round", 1))
    tag = (payload.get("tag") or "").strip()
    category = (payload.get("category") or "").strip()
    remove = bool(payload.get("remove", False))
    if not season_id or not tag:
        raise HTTPException(400, "season_id and tag are required")

    season = _season_or_404(db, season_id)
    conn = _shopify_conn()
    if conn is None:
        raise HTTPException(400, "Shopify is not configured on this server")

    if remove:
        # Remove from every product that actually carries the tag (complete cleanup,
        # independent of the round/category computation).
        product_ids = conn.get_product_ids_by_tag(tag)
        if not product_ids:
            raise HTTPException(400, f"No Shopify products currently carry the tag '{tag}'")
    else:
        products, _, _ = _round_shopify_products(
            db, season, round_no - 1, tag=tag, category_group=category or None)
        product_ids = [p["product_id"] for p in products]
        if not product_ids:
            raise HTTPException(400, f"No Shopify products found for round {round_no}"
                                + (f" in category '{category}'" if category else ""))

    job_id = uuid.uuid4().hex
    _TAG_JOBS[job_id] = {
        "status": "running", "action": "remove" if remove else "add", "tag": tag,
        "total": len(product_ids), "done": 0, "ok": 0, "failed": 0, "failures": [], "error": None,
    }
    threading.Thread(target=_run_tag_job, args=(job_id, product_ids, tag, remove), daemon=True).start()
    return {"job_id": job_id, "total": len(product_ids), "action": _TAG_JOBS[job_id]["action"], "tag": tag}


@router.get("/shopify-tag/status")
async def shopify_tag_status(job_id: str = Query(...)):
    job = _TAG_JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {"job_id": job_id, **job}


# ============== Shopify price push (sale prices via API) ==============
# Sets variant price = round sale price and compareAtPrice = regular (or reverts).
# Round-specific. Runs as a background job with the same status endpoint.

def _run_price_job(job_id, product_ids, sku_prices, revert):
    job = _TAG_JOBS[job_id]
    try:
        conn = _shopify_conn()
        if conn is None:
            job.update(status="error", error="Shopify is not configured on this server.")
            return
        done = ok = failed = 0
        failures = []
        for pid in product_ids:
            variants = conn.get_product_variants(pid)
            updates = []
            for v in variants:
                pr = sku_prices.get((v.get("sku") or "").upper().strip())
                if not pr:
                    continue
                if revert:
                    updates.append({"id": v["id"], "price": str(pr["regular"]), "compareAtPrice": None})
                else:
                    updates.append({"id": v["id"], "price": str(pr["sale"]),
                                    "compareAtPrice": str(pr["regular"])})
            done += 1
            if not updates:
                job.update(done=done, ok=ok, failed=failed)
                continue
            attempt = 0
            while True:
                r = conn.set_variant_prices(pid, updates)
                if r.get("access_denied"):
                    job.update(status="error",
                               error="Shopify rejected the write — token missing 'write_products' scope.",
                               done=done, ok=ok, failed=failed)
                    return
                if r.get("throttled") and attempt < 5:
                    attempt += 1
                    time.sleep(1.5 * attempt)
                    continue
                break
            if r.get("ok"):
                ok += 1
            else:
                failed += 1
                failures.append({"product_id": str(pid), "errors": r.get("errors", [])})
            job.update(done=done, ok=ok, failed=failed)
            time.sleep(0.08)
        job.update(status="done", done=done, ok=ok, failed=failed, failures=failures[:50])
    except Exception as e:
        logger.exception("Price job failed")
        job.update(status="error", error=str(e))


def _round_sku_prices(db, season, round_index):
    """sku (upper) -> {sale, regular} for the round's on-sale variants (whole numbers)."""
    out = {}
    for r in _included_variant_rows(db, season, round_index):
        if r["sale"] is None or r["regular"] is None:
            continue
        out[r["sku"].upper().strip()] = {"sale": int(round_(r["sale"])), "regular": int(round_(r["regular"]))}
    return out


@router.get("/shopify-price/preview")
async def shopify_price_preview(season_id: int = Query(...), round: int = Query(1),
                                db: Session = Depends(get_db)):
    """Read-only: which products/variants would be repriced for this round (no writes)."""
    season = _season_or_404(db, season_id)
    rows = _included_variant_rows(db, season, round - 1)
    products, missing, _ = _round_shopify_products(db, season, round - 1)
    base = (settings.get_connector_configs().get("shopify", {}).get("base_url") or "").rstrip("/")
    admin_base = f"{base}/admin/products/" if base else None
    sample = [{"sku": r["sku"], "name": r["name"], "was": r["regular"], "now": r["sale"], "pct": r["pct"]}
              for r in rows[:25]]
    return {
        "season": season.name, "round": round,
        "products_count": len(products), "variants_count": len(rows),
        "missing_shopify": missing, "shopify_configured": bool(base), "admin_base": admin_base,
        "products": [{"title": p["title"], "product_id": p["product_id"]} for p in products],
        "sample_prices": sample,
    }


@router.post("/shopify-price")
async def shopify_price_start(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Start a background job that sets (or reverts) sale prices on Shopify for a round.
    Optional single-product test: pass sku=<sku> or test_one=true to only touch one product."""
    season_id = payload.get("season_id")
    round_no = int(payload.get("round", 1))
    revert = bool(payload.get("revert", False))
    sku = (payload.get("sku") or "").strip()
    test_one = bool(payload.get("test_one", False))
    if not season_id:
        raise HTTPException(400, "season_id is required")

    season = _season_or_404(db, season_id)
    conn = _shopify_conn()
    if conn is None:
        raise HTTPException(400, "Shopify is not configured on this server")

    sku_prices = _round_sku_prices(db, season, round_no - 1)
    products, _, _ = _round_shopify_products(db, season, round_no - 1)
    title_by_id = {p["product_id"]: p["title"] for p in products}
    all_ids = [p["product_id"] for p in products]
    if not all_ids:
        raise HTTPException(400, f"No Shopify products found for round {round_no}")

    if sku:
        su = sku.upper().strip()
        if su not in sku_prices:
            raise HTTPException(400, f"SKU '{sku}' is not on sale in round {round_no}")
        row = db.query(ProductMaster.shopify_product_id).filter(
            _pm_sku(ProductMaster.sku) == su, ProductMaster.shopify_product_id.isnot(None)).first()
        if not row:
            raise HTTPException(400, f"SKU '{sku}' has no Shopify product")
        product_ids = [row[0]]
    elif test_one:
        product_ids = all_ids[:1]
    else:
        product_ids = all_ids

    base = (settings.get_connector_configs().get("shopify", {}).get("base_url") or "").rstrip("/")
    single = len(product_ids) == 1
    tested_admin_url = f"{base}/admin/products/{product_ids[0]}" if (single and base) else None

    job_id = uuid.uuid4().hex
    _TAG_JOBS[job_id] = {
        "status": "running", "action": "revert" if revert else "price", "tag": f"round {round_no}",
        "total": len(product_ids), "done": 0, "ok": 0, "failed": 0, "failures": [], "error": None,
    }
    threading.Thread(target=_run_price_job, args=(job_id, product_ids, sku_prices, revert),
                     daemon=True).start()
    return {"job_id": job_id, "total": len(product_ids), "action": _TAG_JOBS[job_id]["action"],
            "single": single, "tested_admin_url": tested_admin_url,
            "tested_title": title_by_id.get(product_ids[0]) if single else None}
