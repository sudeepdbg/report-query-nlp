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

    # 1. Master Title Registry (The Anchor)
    c.execute('''CREATE TABLE title_registry (
        title_id INTEGER PRIMARY KEY, content_title TEXT, studio_owner TEXT, genre TEXT)''')

    # 2. Content Planning (linked via title_id)
    c.execute('''CREATE TABLE content_planning (
        id INTEGER PRIMARY KEY, title_id INTEGER, network TEXT, status TEXT, 
        planned_date TEXT, region TEXT, localization_status TEXT)''')

    # 3. Work Orders (linked via title_id and vendor_id)
    c.execute('''CREATE TABLE work_orders (
        id INTEGER PRIMARY KEY, title_id INTEGER, work_order TEXT, status TEXT, 
        due_date TEXT, region TEXT, vendor_id INTEGER, language_target TEXT)''')

    # 4. Deals (linked via vendor_id)
    c.execute('''CREATE TABLE deals (
        id INTEGER PRIMARY KEY, vendor_id INTEGER, deal_name TEXT, 
        deal_value REAL, region TEXT, status TEXT)''')

    # 5. Vendor Master (Performance Metrics)
    c.execute('''CREATE TABLE vendor_master (
        vendor_id INTEGER PRIMARY KEY, vendor_name TEXT, rating REAL, category TEXT)''')

    # --- DATA POPULATION ---
    vendors = [(1, 'PixelLocal', 4.8, 'Localization'), (2, 'DubMaster', 3.2, 'Dubbing'), 
               (3, 'GlobalStream', 4.5, 'Packaging'), (4, 'VisionPost', 2.9, 'Post-Production')]
    c.executemany("INSERT INTO vendor_master VALUES (?,?,?,?)", vendors)

    titles = [(101, 'The Penguin', 'Warner Bros', 'Drama'), (102, 'Dune: Prophecy', 'Legendary', 'Sci-Fi'),
              (103, 'The Last of Us', 'Sony', 'Action'), (104, 'House of the Dragon', 'HBO', 'Fantasy')]
    c.executemany("INSERT INTO title_registry VALUES (?,?,?,?)", titles)

    # Populate Deals & Work Orders
    for i in range(1, 101):
        reg = random.choice(["NA", "APAC", "EMEA", "LATAM"])
        v_id = random.randint(1, 4)
        t_id = random.choice([101, 102, 103, 104])
        
        c.execute("INSERT INTO deals (vendor_id, deal_name, deal_value, region, status) VALUES (?,?,?,?,?)",
                  (v_id, f"Package_{i}", random.uniform(500000, 2000000), reg, "Active"))
        
        c.execute("INSERT INTO work_orders (title_id, work_order, status, due_date, region, vendor_id, language_target) VALUES (?,?,?,?,?,?,?)",
                  (t_id, f"WO-{i}", random.choice(["Delayed", "Completed"]), "2024-12-01", reg, v_id, "Spanish"))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e: return None, str(e)
