import sqlite3
import pandas as pd
import random
import threading
from datetime import datetime, timedelta

_local = threading.local()

def get_db_connection():
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect('foundry.db', check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn

def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    # 1. Create Schema (Idempotent)
    c.execute('CREATE TABLE IF NOT EXISTS vendors (vendor_id INTEGER PRIMARY KEY, vendor_name TEXT, rating REAL, contact_email TEXT, phone TEXT, address TEXT, active INTEGER, created_date TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS deals (id INTEGER PRIMARY KEY, vendor_id INTEGER, vendor_name TEXT, deal_name TEXT, deal_value REAL, deal_date TEXT, region TEXT, rights_scope TEXT, deal_type TEXT, status TEXT, currency TEXT, created_at TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS work_orders (id INTEGER PRIMARY KEY, title_id INTEGER, title_name TEXT, vendor_id INTEGER, vendor_name TEXT, status TEXT, region TEXT, due_date TEXT, priority TEXT, estimated_hours REAL)')
    c.execute('CREATE TABLE IF NOT EXISTS content_planning (id INTEGER PRIMARY KEY, title_id INTEGER, content_title TEXT, status TEXT, region TEXT, budget REAL, language TEXT, format TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)')

    # 2. Seed Guard
    c.execute("SELECT value FROM _meta WHERE key='seeded'")
    if not c.fetchone():
        vendor_list = [
            (1, 'PixelPerfect', 4.8, 'ops@pixel.com', '555-0101', 'Los Angeles', 1, '2024-01-01'),
            (2, 'GlobalDub', 4.5, 'sales@globaldub.com', '555-0202', 'London', 1, '2024-01-05'),
            (3, 'StreamOps', 4.2, 'info@streamops.sg', '555-0303', 'Singapore', 1, '2024-01-10'),
            (4, 'VisionPost', 4.6, 'deals@vision.br', '555-0404', 'Sao Paulo', 1, '2024-01-15')
        ]
        c.executemany("INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?)", vendor_list)

        regions = ["NA", "APAC", "EMEA", "LATAM"]
        titles = [(101, 'The Penguin'), (102, 'Dune: Prophecy'), (103, 'The Last of Us')]
        
        # Seed hundreds of rows
        for reg in regions:
            for i in range(75): # 300 total deals
                v_id = random.randint(1, 4)
                v_name = vendor_list[v_id-1][1]
                c.execute("INSERT INTO deals (vendor_id, vendor_name, deal_name, deal_value, deal_date, region, status) VALUES (?,?,?,?,?,?,?)",
                          (v_id, v_name, f"Package Deal {i}", random.uniform(500000, 5000000), "2024-12-01", reg, "Active"))
            
            for i in range(50): # 200 total work orders
                t_id, t_name = random.choice(titles)
                c.execute("INSERT INTO work_orders (title_id, title_name, vendor_id, vendor_name, status, region, due_date, priority) VALUES (?,?,?,?,?,?,?,?)",
                          (t_id, t_name, random.randint(1,4), "Vendor", "In Progress", reg, "2025-06-01", "High"))

        c.execute("INSERT INTO _meta VALUES ('seeded', 'true')")
        conn.commit()

def execute_sql(sql):
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(sql.strip().rstrip(';'), conn)
        return df, None
    except Exception as e:
        return None, str(e)

def get_smart_chart_config(df):
    """Automatically picks columns for the UI based on names"""
    cols = [c.lower() for c in df.columns]
    x_col = next((c for c in df.columns if any(h in c.lower() for h in ['name', 'region', 'status', 'title'])), df.columns[0])
    y_col = next((c for c in df.columns if any(h in c.lower() for h in ['value', 'budget', 'hours', 'rating'])), None)
    return x_col, y_col
