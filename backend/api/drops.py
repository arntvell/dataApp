"""
Drop plan API.

Backs two views:
  /store     — read-mostly table of what lands when, per drop
  /dashboard — admin: edit dates/styles/status, mark goods sent, upload flats

Flats are stored in Postgres rather than on disk because the container
filesystem is ephemeral — anything written locally is gone on the next deploy.
"""

import io
import logging
import re
import zipfile
from datetime import date, datetime
from typing import Optional
from xml.etree import ElementTree as ET

from fastapi import APIRouter, Depends, Query, Body, File, UploadFile, Form, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Integer

from database.config import get_db
from database.models import DropPlanItem, DropFlat

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard/drops", tags=["Drops"])

# The sheet writes dates as "26. Jun" / "w/c 14 Sep" with no year.
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_DAYMONTH_RE = re.compile(r"(\d{1,2})\s*\.?\s*([A-Za-z]{3,})")
DEFAULT_PLAN_YEAR = 2026
BLANKS = {"", "-", "—", "–", "n/a", "na", "none"}


def _blank(v):
    return v is None or str(v).strip().lower() in BLANKS


def _txt(v):
    return None if _blank(v) else str(v).strip()


def _parse_daymonth(value, year=DEFAULT_PLAN_YEAR):
    """'26. Jun' or 'w/c 14 Sep' -> date. None when the cell is a dash or unparseable."""
    if _blank(value):
        return None
    m = _DAYMONTH_RE.search(str(value))
    if not m:
        return None
    month = _MONTHS.get(m.group(2)[:3].lower())
    if not month:
        return None
    try:
        return date(year, month, int(m.group(1)))
    except ValueError:
        return None


def _parse_qty(value):
    if _blank(value):
        return None
    try:
        return int(float(str(value).replace(",", ".")))
    except ValueError:
        return None


def _parse_bool(value):
    if _blank(value):
        return None
    return str(value).strip().lower() in ("yes", "ja", "true", "1", "y")


def _item_dict(it: DropPlanItem, flat_ids=None):
    return {
        "id": it.id,
        "drop_name": it.drop_name,
        "drop_date": it.drop_date.isoformat() if it.drop_date else None,
        "drop_label": it.drop_label,
        "supplier": it.supplier,
        "style": it.style,
        "qty": it.qty,
        "warehouse_date": it.warehouse_date.isoformat() if it.warehouse_date else None,
        "warehouse_label": it.warehouse_label,
        "plan_status": it.plan_status,
        "flat_status": it.flat_status,
        "photographed": it.photographed,
        "status": it.status,
        "outstanding": it.outstanding,
        "shoot_session": it.shoot_session,
        "missing_web_images": it.missing_web_images,
        "flat_files": it.flat_files,
        "note": it.note,
        "kommentar": it.kommentar,
        "sent": bool(it.sent),
        "sent_at": it.sent_at.isoformat() if it.sent_at else None,
        "sort_order": it.sort_order,
        "flat_ids": flat_ids if flat_ids is not None else [],
    }


# ---------------------------------------------------------------- read

@router.get("")
async def list_drops(
    drop: Optional[str] = Query(None, description="Drop name, e.g. 'Drop 1'"),
    status: Optional[str] = Query(None),
    supplier: Optional[str] = Query(None),
    sent: Optional[int] = Query(None, description="1 = sent only, 0 = not sent only"),
    q: Optional[str] = Query(None, description="Search style or supplier"),
    include_cancelled: int = Query(0),
    db: Session = Depends(get_db),
):
    """Drop plan rows, newest drop first, with the ids of any uploaded flats."""
    qry = db.query(DropPlanItem)
    if drop:
        qry = qry.filter(DropPlanItem.drop_name == drop)
    if status:
        qry = qry.filter(DropPlanItem.status == status)
    if supplier:
        qry = qry.filter(DropPlanItem.supplier == supplier)
    if sent is not None:
        qry = qry.filter(DropPlanItem.sent.is_(bool(sent)))
    if not include_cancelled:
        qry = qry.filter(func.coalesce(DropPlanItem.status, "") != "Cancelled")
    if q:
        like = f"%{q.strip()}%"
        qry = qry.filter(DropPlanItem.style.ilike(like) | DropPlanItem.supplier.ilike(like))
    items = qry.order_by(DropPlanItem.drop_date.is_(None), DropPlanItem.drop_date,
                         DropPlanItem.sort_order, DropPlanItem.style).all()

    flats = {}
    if items:
        for fid, iid in db.query(DropFlat.id, DropFlat.item_id).filter(
                DropFlat.item_id.in_([i.id for i in items])):
            flats.setdefault(iid, []).append(fid)
    return {"items": [_item_dict(i, flats.get(i.id, [])) for i in items], "count": len(items)}


@router.get("/meta")
async def drops_meta(db: Session = Depends(get_db)):
    """Filter values + per-drop counts, for the filter buttons."""
    drops = []
    for name, n, sent_n, dt in db.query(
        DropPlanItem.drop_name, func.count(DropPlanItem.id),
        func.sum(cast(DropPlanItem.sent, Integer)),
        func.min(DropPlanItem.drop_date),
    ).group_by(DropPlanItem.drop_name):
        drops.append({"drop_name": name, "count": n, "sent": int(sent_n or 0),
                      "drop_date": dt.isoformat() if dt else None})
    drops.sort(key=lambda d: (d["drop_date"] is None, d["drop_date"] or "", d["drop_name"]))
    def distinct(col):
        return sorted({v for (v,) in db.query(col).distinct() if v})
    unassigned = db.query(func.count(DropFlat.id)).filter(DropFlat.item_id.is_(None)).scalar() or 0
    return {
        "drops": drops,
        "statuses": distinct(DropPlanItem.status),
        "suppliers": distinct(DropPlanItem.supplier),
        "plan_statuses": distinct(DropPlanItem.plan_status),
        "flat_statuses": distinct(DropPlanItem.flat_status),
        "total": db.query(func.count(DropPlanItem.id)).scalar() or 0,
        "unassigned_flats": unassigned,
    }


# ---------------------------------------------------------------- write

EDITABLE = {
    "drop_name": str, "drop_label": str, "supplier": str, "style": str,
    "plan_status": str, "flat_status": str, "status": str, "outstanding": str,
    "shoot_session": str, "missing_web_images": str, "flat_files": str,
    "note": str, "kommentar": str,
}


@router.put("/{item_id}")
async def update_drop_item(item_id: int, payload: dict = Body(...), db: Session = Depends(get_db)):
    """Patch one row. Only keys present in the body are touched."""
    it = db.query(DropPlanItem).filter(DropPlanItem.id == item_id).first()
    if not it:
        raise HTTPException(404, "Drop item not found")
    for key in EDITABLE:
        if key in payload:
            setattr(it, key, _txt(payload[key]))
    for key in ("drop_date", "warehouse_date"):
        if key in payload:
            raw = payload[key]
            if _blank(raw):
                setattr(it, key, None)
            else:
                try:
                    setattr(it, key, date.fromisoformat(str(raw)[:10]))
                except ValueError:
                    raise HTTPException(400, f"{key} must be YYYY-MM-DD")
    if "qty" in payload:
        it.qty = _parse_qty(payload["qty"])
    if "photographed" in payload:
        it.photographed = None if payload["photographed"] is None else bool(payload["photographed"])
    if "sent" in payload:
        new = bool(payload["sent"])
        if new != bool(it.sent):
            it.sent_at = datetime.now() if new else None
        it.sent = new
    if not it.style or not it.drop_name:
        raise HTTPException(400, "style and drop_name cannot be empty")
    db.commit()
    return {"ok": True, "item": _item_dict(it)}


@router.post("")
async def create_drop_item(payload: dict = Body(...), db: Session = Depends(get_db)):
    style = _txt(payload.get("style"))
    drop_name = _txt(payload.get("drop_name"))
    if not style or not drop_name:
        raise HTTPException(400, "style and drop_name are required")
    if db.query(DropPlanItem).filter(DropPlanItem.drop_name == drop_name,
                                     DropPlanItem.style == style).first():
        raise HTTPException(409, f"{style} already exists in {drop_name}")
    it = DropPlanItem(drop_name=drop_name, style=style, sent=False, sort_order=0)
    db.add(it)
    db.commit()
    return await update_drop_item(it.id, payload, db)


@router.post("/{item_id}/delete")
async def delete_drop_item(item_id: int, db: Session = Depends(get_db)):
    db.query(DropFlat).filter(DropFlat.item_id == item_id).update({"item_id": None})
    n = db.query(DropPlanItem).filter(DropPlanItem.id == item_id).delete()
    db.commit()
    return {"ok": True, "deleted": n}


BULK_FIELDS = ("status", "sent", "drop_date", "warehouse_date", "drop_name",
               "plan_status", "flat_status", "supplier", "shoot_session")


@router.post("/bulk-update")
async def bulk_update(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Set the same value on many rows. Body: {ids:[...] | drop:"Drop 1",
    fields:{status?, sent?, drop_date?, warehouse_date?, ...}}. Only the fields
    present are written, so one call can set a date without touching status."""
    fields = payload.get("fields") or {}
    unknown = [k for k in fields if k not in BULK_FIELDS]
    if unknown:
        raise HTTPException(400, f"cannot bulk-set {unknown}; allowed: {list(BULK_FIELDS)}")
    if not fields:
        raise HTTPException(400, "no fields given")

    qry = db.query(DropPlanItem)
    if payload.get("ids"):
        qry = qry.filter(DropPlanItem.id.in_(payload["ids"]))
    elif payload.get("drop"):
        qry = qry.filter(DropPlanItem.drop_name == payload["drop"])
    else:
        raise HTTPException(400, "pass ids or drop")

    parsed = {}
    for key, raw in fields.items():
        if key in ("drop_date", "warehouse_date"):
            if _blank(raw):
                parsed[key] = None
            else:
                try:
                    parsed[key] = date.fromisoformat(str(raw)[:10])
                except ValueError:
                    raise HTTPException(400, f"{key} must be YYYY-MM-DD")
        elif key == "sent":
            parsed[key] = bool(raw)
        else:
            parsed[key] = _txt(raw)

    items = qry.all()
    for it in items:
        for key, val in parsed.items():
            if key == "sent":
                if bool(it.sent) != val:
                    it.sent_at = datetime.now() if val else None
                it.sent = val
            else:
                setattr(it, key, val)
    db.commit()
    return {"ok": True, "updated": len(items), "fields": list(parsed)}


@router.post("/bulk-sent")
async def bulk_sent(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Mark many rows sent / not sent. Body: {ids: [...], sent: bool} or {drop, sent}."""
    sent = bool(payload.get("sent"))
    qry = db.query(DropPlanItem)
    if payload.get("ids"):
        qry = qry.filter(DropPlanItem.id.in_(payload["ids"]))
    elif payload.get("drop"):
        qry = qry.filter(DropPlanItem.drop_name == payload["drop"])
    else:
        raise HTTPException(400, "pass ids or drop")
    n = 0
    for it in qry.all():
        if bool(it.sent) != sent:
            it.sent = sent
            it.sent_at = datetime.now() if sent else None
            n += 1
    db.commit()
    return {"ok": True, "updated": n, "sent": sent}


# ---------------------------------------------------------------- flats

# Print-resolution flats routinely exceed 8MB, which was silently skipping
# real uploads. 25MB per file; the reason is reported per file either way.
MAX_FLAT_BYTES = 25 * 1024 * 1024
# Browsers are inconsistent about content_type (empty for some drags, odd values
# for .heic/.tif), so accept on extension too and only reject what is clearly not
# an image — a folder of flats usually has a stray .pdf or .psd in it.
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif", ".heic", ".heif", ".tif", ".tiff", ".bmp")


def _norm_name(name):
    """Filename/style to a comparable token: letters+digits only, lowercased."""
    return re.sub(r"[^a-z0-9]+", "", (name or "").rsplit(".", 1)[0].lower())


def _flat_lookup(db):
    """(by exact filename, by normalised filename, by normalised style) -> item id."""
    by_file, by_norm, by_style = {}, {}, {}
    for it in db.query(DropPlanItem).all():
        if it.style:
            by_style.setdefault(_norm_name(it.style), it.id)
        for f in (it.flat_files or "").split(","):
            f = f.strip()
            if not f:
                continue
            by_file.setdefault(f.lower(), it.id)
            by_norm.setdefault(_norm_name(f), it.id)
    return by_file, by_norm, by_style


@router.post("/flats")
async def upload_flats(files: list[UploadFile] = File(...), db: Session = Depends(get_db)):
    """Bulk upload. Each file is matched to a style by the 'Flat file(s)' column
    first, then by a normalised filename/style comparison. Anything that matches
    nothing is still stored, unassigned, for manual assignment."""
    by_file, by_norm, by_style = _flat_lookup(db)
    matched, unmatched, skipped = [], [], []
    for f in files:
        data = await f.read()
        if not data:
            skipped.append({"filename": f.filename, "reason": "empty file"})
            continue
        if len(data) > MAX_FLAT_BYTES:
            skipped.append({"filename": f.filename, "reason": f"larger than {MAX_FLAT_BYTES // 1048576}MB"})
            continue
        ctype = (f.content_type or "").lower()
        looks_image = (f.filename or "").lower().endswith(IMAGE_EXTS)
        if not ctype.startswith("image/") and not looks_image:
            skipped.append({"filename": f.filename,
                            "reason": f"not an image ({ctype or 'unknown type'})"})
            continue
        key = _norm_name(f.filename)
        item_id = by_file.get((f.filename or "").lower()) or by_norm.get(key) or by_style.get(key)
        row = DropFlat(item_id=item_id, filename=f.filename, content_type=ctype or "image/jpeg",
                       size_bytes=len(data), data=data)
        db.add(row)
        db.flush()
        (matched if item_id else unmatched).append({"id": row.id, "filename": f.filename,
                                                    "item_id": item_id})
    db.commit()
    return {"ok": True, "matched": len(matched), "unmatched": len(unmatched),
            "skipped": skipped, "matched_files": matched, "unmatched_files": unmatched}


@router.get("/flats")
async def list_flats(unassigned_only: int = Query(0), db: Session = Depends(get_db)):
    qry = db.query(DropFlat.id, DropFlat.item_id, DropFlat.filename, DropFlat.size_bytes)
    if unassigned_only:
        qry = qry.filter(DropFlat.item_id.is_(None))
    return {"flats": [{"id": i, "item_id": it, "filename": fn, "size_bytes": sz}
                      for i, it, fn, sz in qry.order_by(DropFlat.filename)]}


@router.get("/flats/{flat_id}/image")
async def get_flat_image(flat_id: int, db: Session = Depends(get_db)):
    row = db.query(DropFlat).filter(DropFlat.id == flat_id).first()
    if not row:
        raise HTTPException(404, "Flat not found")
    return Response(content=row.data, media_type=row.content_type or "image/jpeg",
                    headers={"Cache-Control": "public, max-age=86400"})


@router.post("/flats/{flat_id}/assign")
async def assign_flat(flat_id: int, payload: dict = Body(...), db: Session = Depends(get_db)):
    row = db.query(DropFlat).filter(DropFlat.id == flat_id).first()
    if not row:
        raise HTTPException(404, "Flat not found")
    item_id = payload.get("item_id")
    if item_id is not None and not db.query(DropPlanItem).filter(DropPlanItem.id == item_id).first():
        raise HTTPException(404, "Drop item not found")
    row.item_id = item_id
    db.commit()
    return {"ok": True, "id": row.id, "item_id": row.item_id}


@router.post("/flats/{flat_id}/delete")
async def delete_flat(flat_id: int, db: Session = Depends(get_db)):
    n = db.query(DropFlat).filter(DropFlat.id == flat_id).delete()
    db.commit()
    return {"ok": True, "deleted": n}


# ---------------------------------------------------------------- import

_XL_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
          "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
          "pr": "http://schemas.openxmlformats.org/package/2006/relationships"}


def _sheet_rows(blob: bytes, sheet_name: str):
    """Read one sheet of an .xlsx into rows of strings, using only the stdlib —
    openpyxl is not a dependency of this project and this is the only place we
    touch spreadsheets."""
    z = zipfile.ZipFile(io.BytesIO(blob))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", _XL_NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % _XL_NS["m"])))
    rels = {r.get("Id"): r.get("Target")
            for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")).findall("pr:Relationship", _XL_NS)}
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    target = None
    names = []
    for sh in wb.find("m:sheets", _XL_NS):
        names.append(sh.get("name"))
        if (sh.get("name") or "").strip().lower() == sheet_name.strip().lower():
            target = rels[sh.get("{%s}id" % _XL_NS["r"])]
    if target is None:
        raise HTTPException(400, f"Sheet {sheet_name!r} not found. Sheets: {names}")
    path = target if target.startswith("xl/") else "xl/" + target.lstrip("/")
    root = ET.fromstring(z.read(path))

    def cidx(ref):
        letters = re.match(r"([A-Z]+)", ref).group(1)
        n = 0
        for ch in letters:
            n = n * 26 + (ord(ch) - 64)
        return n - 1

    rows = []
    for row in root.iter("{%s}row" % _XL_NS["m"]):
        cells = {}
        for c in row.findall("m:c", _XL_NS):
            t, v = c.get("t"), c.find("m:v", _XL_NS)
            isn = c.find("m:is", _XL_NS)
            if isn is not None:
                val = "".join(x.text or "" for x in isn.iter("{%s}t" % _XL_NS["m"]))
            elif v is None:
                val = None
            elif t == "s":
                val = shared[int(v.text)]
            else:
                val = v.text
            cells[cidx(c.get("r"))] = val
        rows.append([cells.get(i) for i in range(max(cells) + 1)] if cells else [])
    return rows


COLUMN_MAP = {
    "drop": "drop_name", "go live": "drop_label", "supplier": "supplier", "style": "style",
    "qty": "qty", "warehouse date": "warehouse_label", "plan status": "plan_status",
    "flat status": "flat_status", "photographed": "photographed", "status": "status",
    "outstanding": "outstanding", "shoot session": "shoot_session",
    "missing web images": "missing_web_images", "flat file(s)": "flat_files",
    "note": "note", "kommentar": "kommentar",
}


@router.post("/import")
async def import_drop_plan(
    file: UploadFile = File(...),
    sheet: str = Form("3-Drop Plan"),
    year: int = Form(DEFAULT_PLAN_YEAR),
    replace: int = Form(0),
    db: Session = Depends(get_db),
):
    """Import the drop plan from the spreadsheet. Upserts on (drop_name, style), so
    re-importing an updated sheet refreshes the plan. `sent` is never touched — it
    is tracked here, not in the sheet. replace=1 deletes rows the sheet no longer has."""
    blob = await file.read()
    if not blob:
        raise HTTPException(400, "Empty file")
    try:
        rows = _sheet_rows(blob, sheet)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Could not read the workbook: {e}")

    header_at, headers = None, []
    for i, row in enumerate(rows[:25]):
        cells = [str(c).strip().lower() if c else "" for c in row]
        if "drop" in cells and "style" in cells:
            header_at, headers = i, cells
            break
    if header_at is None:
        raise HTTPException(400, "Could not find a header row containing 'Drop' and 'Style'")
    idx = {COLUMN_MAP[h]: i for i, h in enumerate(headers) if h in COLUMN_MAP}
    for required in ("drop_name", "style"):
        if required not in idx:
            raise HTTPException(400, f"Sheet is missing a {required!r} column")

    def cell(row, key):
        i = idx.get(key)
        return row[i] if i is not None and i < len(row) else None

    created = updated = skipped = 0
    seen = set()
    for order, row in enumerate(rows[header_at + 1:]):
        drop_name, style = _txt(cell(row, "drop_name")), _txt(cell(row, "style"))
        if not drop_name or not style:
            skipped += 1
            continue
        seen.add((drop_name, style))
        it = db.query(DropPlanItem).filter(DropPlanItem.drop_name == drop_name,
                                           DropPlanItem.style == style).first()
        if not it:
            it = DropPlanItem(drop_name=drop_name, style=style, sent=False)
            db.add(it)
            created += 1
        else:
            updated += 1
        it.supplier = _txt(cell(row, "supplier"))
        it.qty = _parse_qty(cell(row, "qty"))
        it.drop_label = _txt(cell(row, "drop_label"))
        it.drop_date = _parse_daymonth(cell(row, "drop_label"), year)
        it.warehouse_label = _txt(cell(row, "warehouse_label"))
        it.warehouse_date = _parse_daymonth(cell(row, "warehouse_label"), year)
        it.plan_status = _txt(cell(row, "plan_status"))
        it.flat_status = _txt(cell(row, "flat_status"))
        it.photographed = _parse_bool(cell(row, "photographed"))
        it.status = _txt(cell(row, "status"))
        it.outstanding = _txt(cell(row, "outstanding"))
        it.shoot_session = _txt(cell(row, "shoot_session"))
        it.missing_web_images = _txt(cell(row, "missing_web_images"))
        it.flat_files = _txt(cell(row, "flat_files"))
        it.note = _txt(cell(row, "note"))
        it.kommentar = _txt(cell(row, "kommentar"))
        it.sort_order = order
    db.commit()

    removed = 0
    if replace:
        for it in db.query(DropPlanItem).all():
            if (it.drop_name, it.style) not in seen:
                db.query(DropFlat).filter(DropFlat.item_id == it.id).update({"item_id": None})
                db.delete(it)
                removed += 1
        db.commit()

    # newly-imported flat_files may now match previously unassigned uploads
    by_file, by_norm, by_style = _flat_lookup(db)
    linked = 0
    for row in db.query(DropFlat).filter(DropFlat.item_id.is_(None)).all():
        key = _norm_name(row.filename)
        item_id = by_file.get((row.filename or "").lower()) or by_norm.get(key) or by_style.get(key)
        if item_id:
            row.item_id = item_id
            linked += 1
    db.commit()
    return {"ok": True, "created": created, "updated": updated, "skipped": skipped,
            "removed": removed, "flats_linked": linked,
            "total": db.query(func.count(DropPlanItem.id)).scalar() or 0}
