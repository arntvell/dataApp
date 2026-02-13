"""
Stock & wholesale synchronization pipeline for Cin7 Core data.
Syncs stock levels (truncate-and-reload), wholesale orders, and purchase orders.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import and_
from database.config import SessionLocal
from database.models import (
    Cin7Stock, Cin7Sale, Cin7SaleItem,
    Cin7Invoice, Cin7InvoiceItem,
    Cin7Purchase, Cin7PurchaseItem, SyncStatus,
)
from connectors.cin7_connector import Cin7Connector

logger = logging.getLogger(__name__)


class StockSyncPipeline:
    """Sync Cin7 Core stock, wholesale, and purchase data"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connector = Cin7Connector(config.get("cin7", {}))

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

    # ---- Stock levels (truncate-and-reload) ----

    def sync_stock_levels(self):
        """Fetch all stock and replace the entire cin7_stock table (current state snapshot)."""
        if not self.connector.authenticate():
            logger.error("Cin7 authentication failed")
            return

        db = SessionLocal()
        try:
            self._update_sync_status(db, "cin7_stock", sync_in_progress=True, last_error=None)

            stock_data = self.connector.get_product_availability()
            logger.info(f"Fetched {len(stock_data)} stock rows from Cin7")

            # Truncate existing stock
            db.query(Cin7Stock).delete()

            # Insert fresh data (Cin7 Core field names: SKU, Location, OnHand, Allocated, Available, OnOrder)
            for row in stock_data:
                sku = row.get("SKU") or row.get("Code") or row.get("sku") or ""
                location = row.get("Location") or row.get("location") or "Unknown"
                db.add(Cin7Stock(
                    sku=sku,
                    location=location,
                    on_hand=float(row.get("OnHand", 0) or row.get("onHand", 0) or 0),
                    allocated=float(row.get("Allocated", 0) or row.get("allocated", 0) or 0),
                    available=float(row.get("Available", 0) or row.get("available", 0) or 0),
                    on_order=float(row.get("OnOrder", 0) or row.get("onOrder", 0) or 0),
                ))

            db.commit()
            self._update_sync_status(
                db, "cin7_stock",
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=len(stock_data),
            )
            logger.info(f"Stock sync complete: {len(stock_data)} rows loaded")

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, "cin7_stock", sync_in_progress=False, last_error=str(e))
            logger.error(f"Stock sync failed: {e}")
        finally:
            db.close()

    # ---- Wholesale orders ----

    def sync_wholesale_orders(self, from_date: datetime = None):
        """Fetch sale list + details for new/modified orders, upsert into cin7_sales + items."""
        if not self.connector.authenticate():
            logger.error("Cin7 authentication failed")
            return

        if from_date is None:
            from_date = datetime.now() - timedelta(days=7)

        db = SessionLocal()
        try:
            self._update_sync_status(db, "cin7_sales", sync_in_progress=True, last_error=None)

            sale_list = self.connector.get_sale_list(from_date)
            logger.info(f"Fetched {len(sale_list)} wholesale orders from Cin7")

            upserted = 0
            for sale_summary in sale_list:
                sale_id = str(sale_summary.get("SaleID") or sale_summary.get("ID") or sale_summary.get("id") or "")
                if not sale_id:
                    continue

                try:
                    detail = self.connector.get_sale_detail(sale_id)
                    upserted += self._upsert_sale(db, detail)
                except Exception as e:
                    logger.warning(f"Error fetching sale detail {sale_id}: {e}")

            db.commit()
            self._update_sync_status(
                db, "cin7_sales",
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=upserted,
            )
            logger.info(f"Wholesale sync complete: {upserted} orders upserted")

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, "cin7_sales", sync_in_progress=False, last_error=str(e))
            logger.error(f"Wholesale sync failed: {e}")
        finally:
            db.close()

    # ---- Purchase orders ----

    def sync_purchase_orders(self, from_date: datetime = None):
        """Fetch purchase list + details, upsert into cin7_purchases + items."""
        if not self.connector.authenticate():
            logger.error("Cin7 authentication failed")
            return

        if from_date is None:
            from_date = datetime.now() - timedelta(days=7)

        db = SessionLocal()
        try:
            self._update_sync_status(db, "cin7_purchases", sync_in_progress=True, last_error=None)

            purchase_list = self.connector.get_purchase_list(from_date)
            logger.info(f"Fetched {len(purchase_list)} purchase orders from Cin7")

            upserted = 0
            for po_summary in purchase_list:
                po_id = str(po_summary.get("ID") or po_summary.get("id") or po_summary.get("purchaseId") or "")
                if not po_id:
                    continue

                try:
                    detail = self.connector.get_purchase_detail(po_id)
                    upserted += self._upsert_purchase(db, detail)
                except Exception as e:
                    logger.warning(f"Error fetching purchase detail {po_id}: {e}")

            db.commit()
            self._update_sync_status(
                db, "cin7_purchases",
                sync_in_progress=False,
                last_incremental_sync=datetime.now(),
                last_sync_orders_count=upserted,
            )
            logger.info(f"Purchase sync complete: {upserted} orders upserted")

        except Exception as e:
            db.rollback()
            self._update_sync_status(db, "cin7_purchases", sync_in_progress=False, last_error=str(e))
            logger.error(f"Purchase sync failed: {e}")
        finally:
            db.close()

    # ---- Private helpers ----

    def _upsert_sale(self, db, detail: dict) -> int:
        """Upsert a single sale order with items and invoices. Returns 1 on success."""
        sale_id = str(detail.get("SaleID") or detail.get("ID") or detail.get("id") or "")
        if not sale_id:
            return 0

        existing = db.query(Cin7Sale).filter(Cin7Sale.sale_id == sale_id).first()

        order_date = self._parse_datetime(detail.get("OrderDate") or detail.get("orderDate"))
        total = float(detail.get("Total", 0) or detail.get("total", 0) or 0)
        customer = detail.get("Customer") or detail.get("CustomerName") or detail.get("customer") or ""
        status = detail.get("Status") or detail.get("status") or ""
        sales_rep = detail.get("SalesRepresentative") or detail.get("salesRepresentative") or ""

        if existing:
            existing.customer_name = customer
            existing.sales_representative = sales_rep
            existing.status = status
            existing.order_date = order_date
            existing.total_amount = total
            # Replace items
            db.query(Cin7SaleItem).filter(Cin7SaleItem.sale_id == existing.id).delete()
            self._add_sale_items(db, existing.id, detail)
            # Replace invoices
            for inv in db.query(Cin7Invoice).filter(Cin7Invoice.sale_id == existing.id).all():
                db.query(Cin7InvoiceItem).filter(Cin7InvoiceItem.invoice_id == inv.id).delete()
            db.query(Cin7Invoice).filter(Cin7Invoice.sale_id == existing.id).delete()
            self._add_invoices(db, existing.id, detail)
        else:
            sale = Cin7Sale(
                sale_id=sale_id,
                customer_name=customer,
                sales_representative=sales_rep,
                status=status,
                order_date=order_date,
                total_amount=total,
            )
            db.add(sale)
            db.flush()
            self._add_sale_items(db, sale.id, detail)
            self._add_invoices(db, sale.id, detail)

        return 1

    def _add_sale_items(self, db, db_sale_id: int, detail: dict):
        """Add line items for a sale order"""
        items = detail.get("Lines") or detail.get("Line") or detail.get("lineItems") or detail.get("items") or []
        for item in items:
            db.add(Cin7SaleItem(
                sale_id=db_sale_id,
                sku=item.get("SKU") or item.get("ProductCode") or item.get("sku") or "",
                quantity=float(item.get("Quantity", 0) or item.get("qty", 0) or 0),
                unit_price=float(item.get("Price", 0) or item.get("UnitPrice", 0) or 0),
                line_total=float(item.get("Total", 0) or item.get("LineTotal", 0) or 0),
            ))

    def _add_invoices(self, db, db_sale_id: int, detail: dict):
        """Add invoices and their line items for a sale order"""
        invoices = detail.get("Invoices") or []
        for inv in invoices:
            invoice = Cin7Invoice(
                sale_id=db_sale_id,
                invoice_number=inv.get("InvoiceNumber") or "",
                status=inv.get("Status") or "",
                invoice_date=self._parse_datetime(inv.get("InvoiceDate")),
                due_date=self._parse_datetime(inv.get("InvoiceDueDate")),
                total_before_tax=float(inv.get("TotalBeforeTax", 0) or 0),
                tax=float(inv.get("Tax", 0) or 0),
                total=float(inv.get("Total", 0) or 0),
                paid=float(inv.get("Paid", 0) or 0),
            )
            db.add(invoice)
            db.flush()

            for line in inv.get("Lines", []):
                db.add(Cin7InvoiceItem(
                    invoice_id=invoice.id,
                    sku=line.get("SKU") or "",
                    product_name=line.get("Name") or "",
                    quantity=float(line.get("Quantity", 0) or 0),
                    unit_price=float(line.get("Price", 0) or 0),
                    discount=float(line.get("Discount", 0) or 0),
                    tax=float(line.get("Tax", 0) or 0),
                    line_total=float(line.get("Total", 0) or 0),
                ))

    def _upsert_purchase(self, db, detail: dict) -> int:
        """Upsert a single purchase order with items. Returns 1 on success."""
        purchase_id = str(detail.get("ID") or detail.get("id") or detail.get("purchaseId") or "")
        if not purchase_id:
            return 0

        existing = db.query(Cin7Purchase).filter(Cin7Purchase.purchase_id == purchase_id).first()

        order_date = self._parse_datetime(detail.get("OrderDate") or detail.get("orderDate"))
        total = float(detail.get("Total", 0) or detail.get("total", 0) or 0)
        supplier = detail.get("Supplier") or detail.get("SupplierName") or detail.get("supplier") or ""

        if existing:
            existing.supplier_name = supplier
            existing.order_date = order_date
            existing.total_amount = total
            db.query(Cin7PurchaseItem).filter(Cin7PurchaseItem.purchase_id == existing.id).delete()
            self._add_purchase_items(db, existing.id, detail)
        else:
            po = Cin7Purchase(
                purchase_id=purchase_id,
                supplier_name=supplier,
                order_date=order_date,
                total_amount=total,
            )
            db.add(po)
            db.flush()
            self._add_purchase_items(db, po.id, detail)

        return 1

    def _add_purchase_items(self, db, db_purchase_id: int, detail: dict):
        """Add line items for a purchase order"""
        items = detail.get("Lines") or detail.get("Line") or detail.get("lineItems") or detail.get("items") or []
        for item in items:
            db.add(Cin7PurchaseItem(
                purchase_id=db_purchase_id,
                sku=item.get("SKU") or item.get("ProductCode") or item.get("sku") or "",
                quantity=float(item.get("Quantity", 0) or item.get("qty", 0) or 0),
                unit_cost=float(item.get("Cost", 0) or item.get("UnitCost", 0) or 0),
                line_total=float(item.get("Total", 0) or item.get("LineTotal", 0) or 0),
            ))

    @staticmethod
    def _parse_datetime(value) -> datetime:
        """Parse a datetime string into a datetime object"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(value)[:26], fmt)
            except (ValueError, TypeError):
                continue
        return None
