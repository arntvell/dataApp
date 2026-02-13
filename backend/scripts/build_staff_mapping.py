#!/usr/bin/env python3
"""
Build Staff Mapping Table

Fetches staff/user data from Sitoo API and creates a mapping table.
Then updates existing sales orders with proper staff names.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from database.config import DATABASE_URL, Base
from database.models import StaffMapping

# Create engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Create the table if it doesn't exist
Base.metadata.create_all(engine, tables=[StaffMapping.__table__])


def fetch_sitoo_users():
    """Fetch all users from Sitoo API"""
    api_id = os.environ.get('SITOO_API_ID', '91622-174')
    api_key = os.environ.get('SITOO_API_KEY')
    base_url = os.environ.get('SITOO_BASE_URL', 'https://api140.mysitoo.com/v2/accounts/91622').rstrip('/')
    
    print(f"Fetching users from Sitoo API...")
    
    all_users = []
    start = 0
    batch_size = 100
    
    while True:
        response = requests.get(
            f'{base_url}/sites/1/users.json',
            auth=(api_id, api_key),
            params={'num': batch_size, 'start': start}
        )
        
        if response.status_code != 200:
            print(f"Error fetching users: {response.status_code}")
            break
        
        data = response.json()
        users = data.get('items', [])
        all_users.extend(users)
        
        if len(users) < batch_size:
            break
        start += batch_size
    
    print(f"Fetched {len(all_users)} users from Sitoo")
    return all_users


def build_externalid_mapping_from_orders():
    """
    Build a mapping of staff_userid -> staff_externalid by sampling orders.
    This is needed because the user API doesn't provide externalid.
    """
    api_id = os.environ.get('SITOO_API_ID', '91622-174')
    api_key = os.environ.get('SITOO_API_KEY')
    base_url = os.environ.get('SITOO_BASE_URL', 'https://api140.mysitoo.com/v2/accounts/91622').rstrip('/')
    
    print(f"Scanning orders to build userid -> externalid mapping...")
    
    mapping = {}
    
    # Sample orders from many different time periods to get diverse staff
    sample_points = list(range(10000, 180000, 5000))  # Every 5000 orders
    
    for start_order in sample_points:
        try:
            response = requests.get(
                f'{base_url}/sites/1/orders.json',
                auth=(api_id, api_key),
                params={'num': 100, 'start': start_order}
            )
            
            if response.status_code == 200:
                orders = response.json().get('items', [])
                for order in orders:
                    additional = order.get('additionaldata', {})
                    userid = additional.get('pos-staff-userid', '')
                    extid = additional.get('pos-staff-externalid', '')
                    
                    # Only store if extid is numeric
                    if userid and extid and extid.isdigit() and userid not in mapping:
                        mapping[userid] = extid
        except Exception as e:
            print(f"Error fetching orders at {start_order}: {e}")
    
    print(f"Found {len(mapping)} userid -> externalid mappings from orders")
    return mapping


def build_mapping():
    """Build the staff mapping table"""
    session = Session()
    
    try:
        # Fetch users from Sitoo
        users = fetch_sitoo_users()
        
        # Build externalid mapping from orders
        extid_mapping = build_externalid_mapping_from_orders()
        
        # Clear existing mappings
        session.execute(text("DELETE FROM staff_mappings"))
        session.commit()
        print("Cleared existing staff mappings")
        
        # Create mappings
        mappings = []
        for user in users:
            userid = user.get('userid', '')
            first_name = user.get('namefirst', '').strip()
            last_name = user.get('namelast', '').strip()
            full_name = f"{first_name} {last_name}".strip()
            email = user.get('email', '')
            
            if not userid:
                continue
            
            # Get externalid if we found it in orders
            extid = extid_mapping.get(userid)
            
            mappings.append(StaffMapping(
                staff_userid=userid,
                staff_externalid=extid,
                first_name=first_name if first_name else None,
                last_name=last_name if last_name else None,
                full_name=full_name if full_name else f"Staff {extid}" if extid else "Unknown",
                email=email if email else None,
                source='sitoo_api',
                is_active=True
            ))
        
        session.bulk_save_objects(mappings)
        session.commit()
        print(f"Created {len(mappings)} staff mappings")
        
        # Show summary
        print("\n" + "="*70)
        print("STAFF MAPPING SUMMARY")
        print("="*70)
        
        stats = session.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN full_name NOT LIKE 'Staff %' AND full_name != 'Unknown' THEN 1 END) as with_names,
                COUNT(staff_externalid) as with_extid
            FROM staff_mappings
        """)).fetchone()
        
        print(f"Total staff records: {stats[0]}")
        print(f"With real names:     {stats[1]}")
        print(f"With external ID:    {stats[2]}")
        
        # Show sample
        print("\nSample staff mappings (with numeric external ID):")
        samples = session.execute(text("""
            SELECT staff_externalid, full_name, email
            FROM staff_mappings
            WHERE staff_externalid IS NOT NULL 
              AND staff_externalid ~ '^[0-9]+$'
            ORDER BY staff_externalid::int
            LIMIT 20
        """)).fetchall()
        
        for extid, name, email in samples:
            print(f"  ID {extid:>3}: {name:30} ({email or 'no email'})")
        
        return len(mappings)
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


def update_sales_orders():
    """Update existing sales orders with proper staff names"""
    session = Session()
    
    try:
        print("\n" + "="*70)
        print("UPDATING SALES ORDERS WITH STAFF NAMES")
        print("="*70)
        
        # Update orders that have staff_id matching staff_externalid (only numeric IDs)
        result = session.execute(text("""
            UPDATE sales_orders so
            SET staff_name = sm.full_name
            FROM staff_mappings sm
            WHERE so.staff_id = sm.staff_externalid
              AND so.source_system = 'sitoo'
              AND sm.staff_externalid ~ '^[0-9]+$'
              AND sm.full_name IS NOT NULL
              AND sm.full_name != 'Unknown'
              AND sm.full_name NOT LIKE 'Staff %'
        """))
        session.commit()
        
        updated = result.rowcount
        print(f"Updated {updated:,} orders with staff names")
        
        # Check results
        stats = session.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN staff_name IS NOT NULL AND staff_name NOT LIKE 'Staff %' THEN 1 END) as with_names,
                COUNT(CASE WHEN staff_name LIKE 'Staff %' THEN 1 END) as with_placeholder
            FROM sales_orders
            WHERE source_system = 'sitoo' AND staff_id IS NOT NULL
        """)).fetchone()
        
        print(f"\nResults:")
        print(f"  Orders with staff:         {stats[0]:,}")
        print(f"  Orders with real names:    {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"  Orders with placeholder:   {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
        
        # Show top staff by orders
        print("\nTop 15 staff by order count:")
        top_staff = session.execute(text("""
            SELECT staff_id, staff_name, COUNT(*) as orders, SUM(total_amount) as revenue
            FROM sales_orders
            WHERE source_system = 'sitoo' AND staff_id IS NOT NULL
            GROUP BY staff_id, staff_name
            ORDER BY orders DESC
            LIMIT 15
        """)).fetchall()
        
        for staff_id, name, orders, revenue in top_staff:
            print(f"  {staff_id:>4}: {name:25} | {orders:5,} orders | {revenue:12,.0f} NOK")
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    print("="*70)
    print("BUILDING STAFF MAPPING")
    print("="*70)
    
    total = build_mapping()
    update_sales_orders()
    
    print("\n" + "="*70)
    print(f"✅ Complete!")
    print("="*70)
