import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta

def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    # 1. Schema Definition with Full Transactional Fields
    c.execute('CREATE TABLE vendors (vendor_id INTEGER PRIMARY KEY, vendor_name TEXT, rating REAL)')
    
    # Added fields: deal_date, rights_scope, deal_type to match your high-detail screenshots
    c.execute('''CREATE TABLE deals (
        id INTEGER PRIMARY KEY, 
        vendor_id INTEGER, 
        vendor_name TEXT, 
        deal_name TEXT, 
        deal_value REAL, 
        deal_date TEXT,
        region TEXT, 
        rights_scope TEXT, 
        deal_type TEXT,
        status TEXT)''')
        
    c.execute('''CREATE TABLE work_orders (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        title_name TEXT,
        vendor_id INTEGER, 
        vendor_name TEXT,
        status TEXT, 
        region TEXT, 
        due_date TEXT,
        priority TEXT)''')
        
    c.execute('''CREATE TABLE content_planning (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        content_title TEXT,
        status TEXT, 
        region TEXT,
        localization_status TEXT,
        delivery_method TEXT)''')

    # 2. Master Data
    v_map = {1: 'PixelPerfect', 2: 'GlobalDub', 3: 'StreamOps', 4: 'VisionPost'}
    v_data = [(i, name, round(random.uniform(3.5, 5.0), 1)) for i, name in v_map.items()]
    c.executemany("INSERT INTO vendors VALUES (?,?,?)", v_data)

    titles = [
        (101, 'The Penguin', 'Warner Bros'), 
        (102, 'Dune: Prophecy', 'Legendary'), 
        (103, 'The Last of Us', 'Sony'), 
        (104, 'House of the Dragon', 'HBO')
    ]

    # 3. Seed Dense Data
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    rights_options = ["Global", "Multi-Region", "Territory Specific"]
    deal_types = ["Library Buy", "Volume Deal", "Output Deal"]
    
    for reg in regions:
        # Seed Deals with multi-column richness
        for i in range(50):
            v_id = random.randint(1, 4)
            # Generate random date in 2024-2025
            d_date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400))).strftime('%Y-%m-%d')
            c.execute("""INSERT INTO deals 
                (vendor_id, vendor_name, deal_name, deal_value, deal_date, region, rights_scope, deal_type, status) 
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (v_id, v_map[v_id], f"Package Deal {i+1}", random.uniform(500000, 5000000), 
                 d_date, reg, random.choice(rights_options), random.choice(deal_types), "Active"))
        
        # Seed Work Orders
        for i in range(30):
            t_id, t_name, _ = random.choice(titles)
            c.execute("""INSERT INTO work_orders 
                (title_id, title_name, vendor_id, vendor_name, status, region, due_date, priority) 
                VALUES (?,?,?,?,?,?,?,?)""",
                (t_id, t_name, random.randint(1,4), v_map[random.randint(1,4)], 
                 random.choice(["Completed", "Delayed", "In Progress"]), reg, "2024-12-01", "High"))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        df = pd.read_sql_query(sql.strip().rstrip(';'), conn)
        return df, None
    except Exception as e:
        return None, str(e)
