import sqlite3
import pandas as pd
import random

def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    # 1. Schema Definition
    # Added vendor_name to transactional tables to support your existing parser's simple SELECTs
    c.execute('CREATE TABLE vendors (vendor_id INTEGER PRIMARY KEY, vendor_name TEXT, rating REAL)')
    
    c.execute('''CREATE TABLE deals (
        id INTEGER PRIMARY KEY, 
        vendor_id INTEGER, 
        vendor_name TEXT, 
        deal_name TEXT, 
        deal_value REAL, 
        region TEXT, 
        rights_scope TEXT, 
        status TEXT)''')
        
    c.execute('''CREATE TABLE work_orders (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        vendor_id INTEGER, 
        vendor_name TEXT,
        status TEXT, 
        region TEXT, 
        due_date TEXT)''')
        
    c.execute('''CREATE TABLE content_planning (
        id INTEGER PRIMARY KEY, 
        title_id INTEGER, 
        content_title TEXT,
        status TEXT, 
        region TEXT,
        localization_status TEXT)''')

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

    # 3. Seed Data with full coverage for Sidebar Suggestions
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    rights_options = ["SVOD Exclusive", "AVOD", "TVOD", "Linear Rights", "All Rights"]
    loc_statuses = ["Completed", "Dubbing In-Progress", "Subbing In-Progress", "Pending Materials"]
    
    for reg in regions:
        # Seed Deals (Ensures 'Total spend per vendor' always has data)
        for i in range(30):
            v_id = random.randint(1, 4)
            v_name = v_map[v_id]
            c.execute("""INSERT INTO deals 
                (vendor_id, vendor_name, deal_name, deal_value, region, rights_scope, status) 
                VALUES (?,?,?,?,?,?,?)""",
                (v_id, v_name, f"{reg} Package {i+100}", random.uniform(500000, 5000000), 
                 reg, random.choice(rights_options), "Active"))
        
        # Seed Work Orders (Ensures 'Delayed tasks' always has data)
        for i in range(40):
            v_id = random.randint(1, 4)
            v_name = v_map[v_id]
            t_id, t_name, _ = random.choice(titles)
            status = "Delayed" if i % 5 == 0 else random.choice(["Completed", "In Progress"])
            c.execute("""INSERT INTO work_orders 
                (title_id, vendor_id, vendor_name, status, region, due_date) 
                VALUES (?,?,?,?,?,?)""",
                (t_id, v_id, v_name, status, reg, "2024-12-01"))

        # Seed Content Planning (Ensures 'Ready status' always has data)
        for t_id, t_name, studio in titles:
            status = random.choice(["Ready", "Not Ready", "Delivered", "Scheduled"])
            c.execute("""INSERT INTO content_planning 
                (title_id, content_title, status, region, localization_status) 
                VALUES (?,?,?,?,?)""",
                (t_id, t_name, status, reg, random.choice(loc_statuses)))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        # Added a small cleanup to handle semicolons or whitespace from parser
        df = pd.read_sql_query(sql.strip().rstrip(';'), conn)
        return df, None
    except Exception as e:
        return None, str(e)
