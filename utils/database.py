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
    c.execute('CREATE TABLE titles (title_id INTEGER PRIMARY KEY, title_name TEXT, studio TEXT)')

    # 2. Transactional Tables
    c.execute('''CREATE TABLE deals (
        id INTEGER PRIMARY KEY, vendor_id INTEGER, deal_name TEXT, 
        deal_value REAL, region TEXT, status TEXT)''')
    c.execute('''CREATE TABLE work_orders (
        id INTEGER PRIMARY KEY, title_id INTEGER, vendor_id INTEGER, 
        status TEXT, region TEXT, due_date TEXT)''')
    c.execute('''CREATE TABLE content_planning (
        id INTEGER PRIMARY KEY, title_id INTEGER, status TEXT, region TEXT)''')

    # SEED MASTER DATA
    v_data = [(1, 'PixelPerfect', 4.8), (2, 'GlobalDub', 3.9), (3, 'StreamOps', 4.2), (4, 'VisionPost', 3.5)]
    c.executemany("INSERT INTO vendors VALUES (?,?,?)", v_data)

    t_data = [(101, 'The Penguin', 'Warner Bros'), (102, 'Dune: Prophecy', 'Legendary'), 
              (103, 'The Last of Us', 'Sony'), (104, 'House of the Dragon', 'HBO')]
    c.executemany("INSERT INTO titles VALUES (?,?,?)", t_data)

    # SEED TRANSACTIONAL DATA (Ensuring data for ALL regions)
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    for reg in regions:
        # 25 Deals per region
        for i in range(25):
            v_id = random.randint(1, 4)
            c.execute("INSERT INTO deals (vendor_id, deal_name, deal_value, region, status) VALUES (?,?,?,?,?)",
                      (v_id, f"{reg} Deal {i}", random.uniform(500000, 5000000), reg, "Active"))
        
        # 50 Work Orders per region
        for i in range(50):
            v_id = random.randint(1, 4)
            t_id = random.choice([101, 102, 103, 104])
            status = random.choice(["Delayed", "Completed", "In Progress"])
            c.execute("INSERT INTO work_orders (title_id, vendor_id, status, region, due_date) VALUES (?,?,?,?,?)",
                      (t_id, v_id, status, reg, "2024-12-01"))

        # Content Planning
        for t_id in [101, 102, 103, 104]:
            c.execute("INSERT INTO content_planning (title_id, status, region) VALUES (?,?,?)",
                      (t_id, random.choice(["Ready", "Not Ready"]), reg))

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
