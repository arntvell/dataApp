from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from database.config import get_db
from database.models import Product, Customer, Order, Inventory
from pydantic import BaseModel

router = APIRouter()

# Pydantic models for API responses
class ProductResponse(BaseModel):
    id: int
    sku: str
    name: str
    description: Optional[str]
    price: float
    cost: float
    inventory_quantity: int
    source_system: str
    source_id: str
    
    class Config:
        from_attributes = True

class CustomerResponse(BaseModel):
    id: int
    email: str
    first_name: str
    last_name: str
    phone: Optional[str]
    source_system: str
    source_id: str
    
    class Config:
        from_attributes = True

class OrderResponse(BaseModel):
    id: int
    order_number: str
    customer_source_id: Optional[str]
    total_amount: float
    status: str
    source_system: str
    source_id: str
    
    class Config:
        from_attributes = True

class InventoryResponse(BaseModel):
    id: int
    product_id: int
    location: str
    quantity: int
    reserved_quantity: int
    
    class Config:
        from_attributes = True

@router.get("/products", response_model=List[ProductResponse])
async def get_products(
    skip: int = 0,
    limit: int = 100,
    source_system: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all products with optional filtering"""
    query = db.query(Product)
    
    if source_system:
        query = query.filter(Product.source_system == source_system)
    
    products = query.offset(skip).limit(limit).all()
    return products

@router.get("/products/{product_id}", response_model=ProductResponse)
async def get_product(product_id: int, db: Session = Depends(get_db)):
    """Get a specific product by ID"""
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

@router.get("/customers", response_model=List[CustomerResponse])
async def get_customers(
    skip: int = 0,
    limit: int = 100,
    source_system: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all customers with optional filtering"""
    query = db.query(Customer)
    
    if source_system:
        query = query.filter(Customer.source_system == source_system)
    
    customers = query.offset(skip).limit(limit).all()
    return customers

@router.get("/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer(customer_id: int, db: Session = Depends(get_db)):
    """Get a specific customer by ID"""
    customer = db.query(Customer).filter(Customer.id == customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer

@router.get("/orders", response_model=List[OrderResponse])
async def get_orders(
    skip: int = 0,
    limit: int = 100,
    source_system: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all orders with optional filtering"""
    query = db.query(Order)
    
    if source_system:
        query = query.filter(Order.source_system == source_system)
    
    orders = query.offset(skip).limit(limit).all()
    return orders

@router.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: int, db: Session = Depends(get_db)):
    """Get a specific order by ID"""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@router.get("/inventory", response_model=List[InventoryResponse])
async def get_inventory(
    skip: int = 0,
    limit: int = 100,
    location: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all inventory with optional filtering"""
    query = db.query(Inventory)
    
    if location:
        query = query.filter(Inventory.location == location)
    
    inventory = query.offset(skip).limit(limit).all()
    return inventory

@router.get("/analytics/summary")
async def get_analytics_summary(db: Session = Depends(get_db)):
    """Get summary analytics"""
    total_products = db.query(Product).count()
    total_customers = db.query(Customer).count()
    total_orders = db.query(Order).count()
    
    # Calculate total revenue
    total_revenue = db.query(func.sum(Order.total_amount)).scalar() or 0
    
    return {
        "total_products": total_products,
        "total_customers": total_customers,
        "total_orders": total_orders,
        "total_revenue": total_revenue
    }

