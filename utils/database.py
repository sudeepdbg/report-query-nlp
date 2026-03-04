import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta

def get_db_connection():
    # Connecting to a local file 'foundry.db' to persist data for Tableau
    conn = sqlite3.connect('foundry.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    # 1. CLEANUP: Avoid "Table Already Exists" errors by dropping old versions
    c.execute('DROP TABLE IF EXISTS vendors')
    c.execute('DROP TABLE IF EXISTS deals')
    c.execute('DROP TABLE IF EXISTS work_orders')
    c.execute('DROP TABLE IF EXISTS content_planning')

    # 2. SCHEMA DEFINITION: Enriched with all new requested fields
    c.execute('''CREATE TABLE vendors (
        vendor_id INTEGER PRIMARY KEY, 
        vendor_name TEXT, 
        rating REAL, 
        contact_email TEXT, 
        phone TEXT, 
        address TEXT, 
        active INTEGER, 
        created_date TEXT
    )''')

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
        status TEXT, 
        currency TEXT DEFAULT 'USD', 
        description TEXT, 
        contract_signed_date TEXT, 
        expiry_date TEXT, 
        renewal_terms TEXT, 
        created_at TEXT
    )''')

    c.execute('''CREATE TABLE work_orders (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        title_name TEXT, 
        vendor_id INTEGER, 
        vendor_name TEXT, 
        status TEXT, 
        region TEXT, 
        due_date TEXT, 
        priority TEXT, 
        assigned_to TEXT, 
        start_date TEXT, 
        completion_date TEXT, 
        notes TEXT, 
        estimated_hours REAL
    )''')

    c.execute('''CREATE TABLE content_planning (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        content_title TEXT, 
        status TEXT, 
        region TEXT, 
        localization_status TEXT, 
        delivery_method TEXT, 
        budget REAL, 
        target_release_date TEXT, 
        language TEXT, 
        format TEXT, 
        content_type TEXT, 
        production_company TEXT, 
        notes TEXT
    )''')

    # 3. MASTER DATA DEFINITIONS
    vendor_list = [
        (1, 'PixelPerfect', 4.8, 'ops@pixel.com', '555-0101', 'Los Angeles', 1, '2024-01-01'),
        (2, 'GlobalDub', 4.5, 'sales@globaldub.com', '555-0202', 'London', 1, '2024-01-05'),
        (3, 'StreamOps', 4.2, 'info@streamops.sg', '555-0303', 'Singapore', 1, '2024-01-10'),
        (4, 'VisionPost', 4.6, 'deals@vision.br', '555-0404', 'Sao Paulo', 1, '2024-01-15')
    ]
    c.executemany("INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?)", vendor_list)

    titles = [
        (101, 'The Penguin', 'Warner Bros'), 
        (102, 'Dune: Prophecy', 'Legendary'), 
        (103, 'The Last of Us', 'Sony'), 
        (104, 'House of the Dragon', 'HBO')
    ]

    # 4. DATA SEEDING LOGIC
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    rights_options = ["Global", "Multi-Region", "Territory Specific"]
    deal_types = ["Library Buy", "Volume Deal", "Output Deal"]
    currencies = ["USD", "EUR", "GBP"]
    work_statuses = ["Completed", "Delayed", "In Progress", "Not Started"]
    
    start_2024 = datetime(2024, 1, 1)

    for reg in regions:
        # Seed 75 Deals per region for density
        for i in range(75):
            v_id = random.randint(1, 4)
            v_name = next(v[1] for v in vendor_list if v[0] == v_id)
            d_date = (start_2024 + timedelta(days=random.randint(0, 500))).strftime('%Y-%m-%d')
            
            c.execute('''INSERT INTO deals (
                vendor_id, vendor_name, deal_name, deal_value, deal_date, 
                region, rights_scope, deal_type, status, currency, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)''', 
            (v_id, v_name, f"Package Deal {i+1}", random.uniform(500000, 5000000), 
             d_date, reg, random.choice(rights_options), random.choice(deal_types), 
             "Active", random.choice(currencies), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        # Seed 50 Work Orders per region
        for i in range(50):
            t_id, t_name, _ = random.choice(titles)
            v_id = random.randint(1, 4)
            v_name = next(v[1] for v in vendor_list if v[0] == v_id)
            
            c.execute('''INSERT INTO work_orders (
                title_id, title_name, vendor_id, vendor_name, status, 
                region, due_date, priority, estimated_hours
            ) VALUES (?,?,?,?,?,?,?,?,?)''',
            (t_id, t_name, v_id, v_name, random.choice(work_statuses), 
             reg, "2025-06-01", random.choice(["High", "Medium"]), random.uniform(20, 150)))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        # Strip semicolons to prevent injection errors in some environments
        df = pd.read_sql_query(sql.strip().rstrip(';'), conn)
        return df, None
    except Exception as e:
        return None, str(e)
