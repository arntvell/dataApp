from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, ForeignKey, Text, Index, UniqueConstraint, Date
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .config import Base

class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, index=True, nullable=True)  # Not unique - some products may lack SKU
    name = Column(String)
    description = Column(Text)
    price = Column(Float)
    cost = Column(Float)
    inventory_quantity = Column(Integer, default=0)
    source_system = Column(String)  # 'sitoo', 'shopify'
    source_id = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Composite unique constraint on source_system + source_id (the true unique identifier)
    __table_args__ = (
        Index('ix_products_source', 'source_system', 'source_id', unique=True),
    )

class Customer(Base):
    __tablename__ = "customers"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    first_name = Column(String)
    last_name = Column(String)
    phone = Column(String)
    source_system = Column(String)
    source_id = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    order_number = Column(String, index=True)  # Not unique - can have same order number from different systems
    customer_source_id = Column(String)  # External customer ID from source system
    total_amount = Column(Float)
    status = Column(String)
    source_system = Column(String)
    source_id = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Composite unique constraint on source_system + source_id
    __table_args__ = (
        Index('ix_orders_source', 'source_system', 'source_id', unique=True),
    )

class Inventory(Base):
    __tablename__ = "inventory"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    location = Column(String)  # Store location or warehouse
    quantity = Column(Integer)
    reserved_quantity = Column(Integer, default=0)
    last_updated = Column(DateTime(timezone=True), onupdate=func.now())
    
    product = relationship("Product")


# ============== SALES DASHBOARD MODELS ==============

class SalesOrder(Base):
    """Enriched order data for sales dashboard"""
    __tablename__ = "sales_orders"
    
    id = Column(Integer, primary_key=True, index=True)
    order_number = Column(String, index=True)
    source_system = Column(String, index=True)  # 'sitoo', 'shopify'
    source_id = Column(String)
    
    # Location & Channel
    location = Column(String, index=True)  # Store name or 'Online'
    channel = Column(String)  # 'pos', 'web', 'pos-card', etc.
    
    # Staff (POS only)
    staff_id = Column(String, nullable=True)  # External ID (numeric)
    staff_userid = Column(String, nullable=True)  # Sitoo GUID for mapping
    staff_name = Column(String, nullable=True)
    
    # Amounts
    subtotal = Column(Float, default=0)
    total_discount = Column(Float, default=0)
    total_amount = Column(Float, default=0)
    total_refunded = Column(Float, default=0)
    currency = Column(String, default='NOK')
    
    # Status
    status = Column(String)  # PAID, PENDING, REFUNDED, etc.
    cancel_reason = Column(String, nullable=True)  # Shopify: CUSTOMER, FRAUD, INVENTORY, DECLINED, STAFF, OTHER
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    note = Column(Text, nullable=True)  # Order-level note from Shopify
    payment_method = Column(String, nullable=True)  # Card, Cash, etc.
    
    # Customer info
    customer_source_id = Column(String, nullable=True)
    is_new_customer = Column(Boolean, nullable=True)
    
    # Timestamps
    order_date = Column(DateTime(timezone=True), index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    items = relationship("SalesOrderItem", back_populates="order", cascade="all, delete-orphan")
    refunds = relationship("SalesRefund", back_populates="order", cascade="all, delete-orphan")

    __table_args__ = (
        Index('ix_sales_orders_source', 'source_system', 'source_id', unique=True),
        Index('ix_sales_orders_date_location', 'order_date', 'location'),
        Index('ix_sales_orders_date_source', 'order_date', 'source_system'),
        Index('ix_sales_orders_staff_name', 'staff_name'),
        Index('ix_sales_orders_date_staff', 'order_date', 'staff_name'),
    )


class SalesRefund(Base):
    """Individual refund records with their own dates (for date-accurate return tracking)"""
    __tablename__ = "sales_refunds"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("sales_orders.id"), index=True)
    source_system = Column(String, index=True)  # 'shopify'
    source_id = Column(String)  # Shopify refund GID

    amount = Column(Float, default=0)  # Refund amount
    currency = Column(String, default='NOK')
    note = Column(Text, nullable=True)  # Note added when processing the refund
    refund_date = Column(DateTime(timezone=True), index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    order = relationship("SalesOrder", back_populates="refunds")

    __table_args__ = (
        Index('ix_sales_refunds_source', 'source_system', 'source_id', unique=True),
        Index('ix_sales_refunds_date', 'refund_date', 'source_system'),
    )


class SalesOrderItem(Base):
    """Line items for sales orders"""
    __tablename__ = "sales_order_items"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("sales_orders.id"), index=True)
    
    # Product info
    sku = Column(String, index=True, nullable=True)
    product_name = Column(String)
    product_category = Column(String, nullable=True)
    vendor = Column(String, nullable=True, index=True)  # Product vendor/manufacturer
    
    # Pricing
    quantity = Column(Integer, default=1)
    unit_price = Column(Float)  # Original price per unit
    discount_amount = Column(Float, default=0)  # Discount per unit
    line_total = Column(Float)  # Final amount for this line
    
    # Source reference
    source_product_id = Column(String, nullable=True)
    
    order = relationship("SalesOrder", back_populates="items")

    __table_args__ = (
        Index('ix_sales_order_items_order_sku', 'order_id', 'sku'),
    )


class SyncStatus(Base):
    """Track sync status for each source system"""
    __tablename__ = "sync_status"
    
    id = Column(Integer, primary_key=True, index=True)
    source_system = Column(String, unique=True, index=True)  # 'sitoo', 'shopify'
    
    # Sync timestamps
    last_full_sync = Column(DateTime(timezone=True), nullable=True)
    last_incremental_sync = Column(DateTime(timezone=True), nullable=True)
    last_order_date = Column(DateTime(timezone=True), nullable=True)  # Most recent order synced
    
    # Stats
    total_orders_synced = Column(Integer, default=0)
    last_sync_orders_count = Column(Integer, default=0)
    
    # Status
    sync_in_progress = Column(Boolean, default=False)
    last_error = Column(Text, nullable=True)
    
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class CategoryMapping(Base):
    """SKU to standardized category mapping"""
    __tablename__ = "category_mappings"
    
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True)
    
    # Original values
    original_category = Column(String, nullable=True)  # Category from source system
    product_name = Column(String, nullable=True)  # For reference
    
    # Standardized category
    standard_category = Column(String, index=True)  # Unified category
    category_group = Column(String, nullable=True, index=True)  # Parent group (e.g. Jersey covers T-shirt, Singlet, Sweatshirt)
    
    # Gender / target audience based on Sitoo vendor classification
    designed_for = Column(String, nullable=True, index=True)  # 'men', 'women', 'unisex'
    # Unified vendor label for cross-channel consistency
    sold_as_vendor = Column(String, nullable=True)  # e.g. 'Livid Unisex'

    # Mapping metadata
    mapping_source = Column(String)  # 'shopify', 'keyword_inference', 'manual'
    confidence = Column(Float, default=1.0)  # 1.0 for shopify, 0.8 for keyword inference
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class ParentSkuMapping(Base):
    """Map variant SKUs to parent (base) SKUs for product-level aggregation"""
    __tablename__ = "parent_sku_mappings"
    
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True)  # Variant SKU
    parent_sku = Column(String, index=True)  # Parent/base SKU (without size)
    
    # Extracted size info
    size_code = Column(String, nullable=True)  # e.g., "3132", "M", "OS", "42"
    size_type = Column(String, nullable=True)  # "denim", "letter", "one_size", "numeric"
    
    # Product info (denormalized for convenience)
    product_name = Column(String, nullable=True)
    base_product_name = Column(String, nullable=True)  # Name without size
    
    # Variant count (updated after all variants mapped)
    variant_count = Column(Integer, default=1)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class StaffMapping(Base):
    """Map Sitoo staff IDs to names"""
    __tablename__ = "staff_mappings"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Sitoo identifiers
    staff_userid = Column(String, unique=True, index=True)  # GUID from pos-staff-userid
    staff_externalid = Column(String, index=True, nullable=True)  # Numeric ID from pos-staff-externalid
    
    # Staff info
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    full_name = Column(String, index=True)  # Computed: first + last
    email = Column(String, nullable=True)
    
    # Metadata
    source = Column(String, default='sitoo_api')  # 'sitoo_api', 'manual'
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


# ============== RAW SCHEMA MODELS (SameSystem + Cin7) ==============

class SameSystemBudget(Base):
    """SameSystem budget data (sales and salary budgets)"""
    __tablename__ = "samesystem_budgets"
    __table_args__ = (
        UniqueConstraint('store', 'date', 'budget_type', 'granularity', name='uq_ss_budget'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    store = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    budget_type = Column(String, nullable=False)  # 'sales' or 'salary'
    amount = Column(Float, default=0)
    granularity = Column(String, nullable=False)  # 'daily' or 'monthly'
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class SameSystemWorktime(Base):
    """SameSystem worktime/salary export data"""
    __tablename__ = "samesystem_worktime"
    __table_args__ = (
        UniqueConstraint('store', 'date', 'employee_id', name='uq_ss_worktime'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    store = Column(String, nullable=False, index=True)
    employee_id = Column(String, nullable=False)
    date = Column(Date, nullable=False, index=True)
    hours_worked = Column(Float, default=0)
    salary_cost = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class Cin7Stock(Base):
    """Cin7 Core stock/inventory levels (current snapshot)"""
    __tablename__ = "cin7_stock"
    __table_args__ = (
        UniqueConstraint('sku', 'location', name='uq_cin7_stock'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, nullable=False, index=True)
    location = Column(String, nullable=False, index=True)
    on_hand = Column(Float, default=0)
    allocated = Column(Float, default=0)
    available = Column(Float, default=0)
    on_order = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class Cin7Sale(Base):
    """Cin7 Core wholesale sale orders"""
    __tablename__ = "cin7_sales"
    __table_args__ = (
        UniqueConstraint('sale_id', name='uq_cin7_sale'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(String, nullable=False, index=True)
    customer_name = Column(String, nullable=True)
    sales_representative = Column(String, nullable=True, index=True)
    status = Column(String, nullable=True)
    order_date = Column(DateTime(timezone=True), nullable=True, index=True)
    total_amount = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    items = relationship("Cin7SaleItem", back_populates="sale", cascade="all, delete-orphan")
    invoices = relationship("Cin7Invoice", back_populates="sale", cascade="all, delete-orphan")


class Cin7SaleItem(Base):
    """Line items for Cin7 wholesale sales"""
    __tablename__ = "cin7_sale_items"
    __table_args__ = {'schema': 'raw'}

    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(Integer, ForeignKey("raw.cin7_sales.id"), nullable=False, index=True)
    sku = Column(String, nullable=True)
    quantity = Column(Float, default=0)
    unit_price = Column(Float, default=0)
    line_total = Column(Float, default=0)

    sale = relationship("Cin7Sale", back_populates="items")


class Cin7Invoice(Base):
    """Cin7 invoices (actual billed amounts) linked to sale orders"""
    __tablename__ = "cin7_invoices"
    __table_args__ = (
        UniqueConstraint('sale_id', 'invoice_number', name='uq_cin7_invoice'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(Integer, ForeignKey("raw.cin7_sales.id"), nullable=False, index=True)
    invoice_number = Column(String, nullable=True, index=True)
    status = Column(String, nullable=True)
    invoice_date = Column(DateTime(timezone=True), nullable=True, index=True)
    due_date = Column(DateTime(timezone=True), nullable=True)
    total_before_tax = Column(Float, default=0)
    tax = Column(Float, default=0)
    total = Column(Float, default=0)
    paid = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    sale = relationship("Cin7Sale", back_populates="invoices")
    items = relationship("Cin7InvoiceItem", back_populates="invoice", cascade="all, delete-orphan")


class Cin7InvoiceItem(Base):
    """Line items for Cin7 invoices"""
    __tablename__ = "cin7_invoice_items"
    __table_args__ = {'schema': 'raw'}

    id = Column(Integer, primary_key=True, index=True)
    invoice_id = Column(Integer, ForeignKey("raw.cin7_invoices.id"), nullable=False, index=True)
    sku = Column(String, nullable=True, index=True)
    product_name = Column(String, nullable=True)
    quantity = Column(Float, default=0)
    unit_price = Column(Float, default=0)
    discount = Column(Float, default=0)
    tax = Column(Float, default=0)
    line_total = Column(Float, default=0)

    invoice = relationship("Cin7Invoice", back_populates="items")


class Cin7Purchase(Base):
    """Cin7 Core purchase orders"""
    __tablename__ = "cin7_purchases"
    __table_args__ = (
        UniqueConstraint('purchase_id', name='uq_cin7_purchase'),
        {'schema': 'raw'}
    )

    id = Column(Integer, primary_key=True, index=True)
    purchase_id = Column(String, nullable=False, index=True)
    supplier_name = Column(String, nullable=True)
    order_date = Column(DateTime(timezone=True), nullable=True, index=True)
    total_amount = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    items = relationship("Cin7PurchaseItem", back_populates="purchase", cascade="all, delete-orphan")


class Cin7PurchaseItem(Base):
    """Line items for Cin7 purchase orders"""
    __tablename__ = "cin7_purchase_items"
    __table_args__ = {'schema': 'raw'}

    id = Column(Integer, primary_key=True, index=True)
    purchase_id = Column(Integer, ForeignKey("raw.cin7_purchases.id"), nullable=False, index=True)
    sku = Column(String, nullable=True)
    quantity = Column(Float, default=0)
    unit_cost = Column(Float, default=0)
    line_total = Column(Float, default=0)

    purchase = relationship("Cin7Purchase", back_populates="items")
