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

    # 1. Master Tables
    c.execute('CREATE TABLE vendors (vendor_id INTEGER PRIMARY KEY, vendor_name TEXT, rating REAL)')
    
    # 2. Transactional Tables (Denormalized vendor_name to prevent JOIN failures)
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

    # 3. Master Data Seeding
    v_map = {1: 'PixelPerfect', 2: 'GlobalDub', 3: 'StreamOps', 4: 'VisionPost'}
    v_data = [(i, name, round(random.uniform(3.5, 5.0), 1)) for i, name in v_map.items()]
    c.executemany("INSERT INTO vendors VALUES (?,?,?)", v_data)

    titles = [
        (101, 'The Penguin', 'Warner Bros'), 
        (102, 'Dune: Prophecy', 'Legendary'), 
        (103, 'The Last of Us', 'Sony'), 
        (104, 'House of the Dragon', 'HBO')
    ]

    # 4. Dense Data Seeding (Guarantees every region has results)
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    rights_options = ["SVOD", "AVOD", "TVOD", "Linear", "All Rights"]
    
    for reg in regions:
        # Seed 50 Deals per region
        for i in range(50):
            v_id = random.randint(1, 4)
            v_name = v_map[v_id]
            # Ensure names and regions are saved in UPPERCASE to match the parser's logic
            c.execute("""INSERT INTO deals 
                (vendor_id, vendor_name, deal_name, deal_value, region, rights_scope, status) 
                VALUES (?,?,?,?,?,?,?)""",
                (v_id, v_name, f"Package {i+100}", random.uniform(500000, 5000000), 
                 reg, random.choice(rights_options), "Active"))
        
        # Seed 40 Work Orders (Force 'Delayed' status for reliability)
        for i in range(40):
            v_id = random.randint(1, 4)
            v_name = v_map[v_id]
            t_id, t_name, _ = random.choice(titles)
            # Guarantee at least some delayed tasks for the 'Delayed' query
            status = "Delayed" if i % 4 == 0 else "Completed"
            c.execute("""INSERT INTO work_orders 
                (title_id, vendor_id, vendor_name, status, region, due_date) 
                VALUES (?,?,?,?,?,?)""",
                (t_id, v_id, v_name, status, reg, "2024-12-01"))

        # Seed Content Planning
        for t_id, t_name, _ in titles:
            c.execute("""INSERT INTO content_planning 
                (title_id, content_title, status, region, localization_status) 
                VALUES (?,?,?,?,?)""",
                (t_id, t_name, random.choice(["Ready", "Not Ready"]), reg, "In-Progress"))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        # Clean the SQL string from any potential parser artifacts
        clean_sql = sql.strip().replace('\n', ' ')
        df = pd.read_sql_query(clean_sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
