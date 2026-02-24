import sqlite3
import pandas as pd
import random
import logging

logger = logging.getLogger(__name__)

def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Create tables and insert enriched media supply chain data."""
    conn = get_db_connection()
    c = conn.cursor()

    # 1. ENHANCED CONTENT PLANNING TABLE
    # Added: rights_type, acquisition_status, localization_status, dubbing_languages
    c.execute('''
        CREATE TABLE content_planning (
            id INTEGER PRIMARY KEY,
            network TEXT,
            content_title TEXT,
            status TEXT,
            planned_date TEXT,
            region TEXT,
            rights_type TEXT,
            acquisition_status TEXT,
            localization_status TEXT,
            available_languages TEXT
        )
    ''')

    # 2. ENHANCED WORK ORDERS TABLE
    # Added: work_status (specifically for Duplo/Workflow tracking), language_target
    c.execute('''
        CREATE TABLE work_orders (
            id INTEGER PRIMARY KEY,
            work_order TEXT,
            offering TEXT,
            status TEXT,
            work_status TEXT,
            due_date TEXT,
            region TEXT,
            vendor TEXT,
            priority TEXT,
            language_target TEXT
        )
    ''')

    # 3. ENHANCED DEALS TABLE
    # Added: rights_scope, deal_type
    c.execute('''
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY,
            deal_name TEXT,
            vendor TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            status TEXT,
            rights_scope TEXT,
            deal_type TEXT
        )
    ''')

    # ---------- DATA GENERATION HELPERS ----------
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    rights_options = ["SVOD Exclusive", "TVOD", "AVOD", "Linear Rights", "All Rights"]
    acq_statuses = ["Acquired", "Pending Materials", "In Negotiation", "Under Review"]
    loc_statuses = ["Completed", "Subbing In-Progress", "Dubbing In-Progress", "Awaiting Assets"]
    
    # Language mapping based on Market
    market_langs = {
        "NA": "English, French (Quebec), Spanish",
        "EMEA": "English, French, German, Italian, Spanish, Arabic",
        "APAC": "English, Mandarin, Japanese, Hindi, Thai, Korean",
        "LATAM": "Spanish (LatAm), Portuguese (Brazil)"
    }

    # ---------- POPULATE CONTENT PLANNING (200+ rows) ----------
    networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX India"]
    content_base = ["House of the Dragon S{}", "The Penguin", "The Last of Us S{}", "Dune: Prophecy", "Succession S{}"]
    
    content_entries = []
    content_id = 1
    for base in content_base:
        for season in range(1, 4):
            title = base.format(season) if "S{}" in base else base
            for reg in regions:
                status = random.choice(["Scheduled", "Delivered", "Not Ready"])
                acq = random.choice(acq_statuses)
                rights = random.choice(rights_options)
                loc = random.choice(loc_statuses)
                
                content_entries.append((
                    content_id, random.choice(networks), title, status, 
                    f"2024-{random.randint(1,12):02d}-01", reg, 
                    rights, acq, loc, market_langs[reg]
                ))
                content_id += 1
    
    c.executemany("INSERT INTO content_planning VALUES (?,?,?,?,?,?,?,?,?,?)", content_entries)

    # ---------- POPULATE WORK ORDERS (500+ rows) ----------
    # Tracking Duplo Work Status and Localization Tasks
    offering_types = ["Localization - Sub", "Localization - Dub", "QC Review", "Mastering"]
    work_orders = []
    for i in range(1, 501):
        reg = random.choice(regions)
        work_orders.append((
            i, f"WO-2024-{i:04d}", 
            f"MAX {reg} - {random.choice(offering_types)}", 
            random.choice(["Delayed", "In Progress", "Completed"]),
            random.choice(["In Duplo Queue", "Processing", "Packaging", "Asset Upload"]),
            f"2024-{random.randint(1,12):02d}-20", reg,
            f"Vendor {random.choice('ABCDEF')}", random.choice(["A", "B", "C"]),
            random.choice(market_langs[reg].split(", "))
        ))
    c.executemany("INSERT INTO work_orders VALUES (?,?,?,?,?,?,?,?,?,?)", work_orders)

    # ---------- POPULATE DEALS (250+ rows) ----------
    deals = []
    for i in range(1, 251):
        reg = random.choice(regions)
        deals.append((
            i, f"Package Deal {i}", f"Studio {random.choice('XYZ')}",
            round(random.uniform(500000, 5000000), 2),
            f"2024-{random.randint(1,12):02d}-15", reg,
            random.choice(["Active", "Negotiating"]),
            random.choice(["Global", "Territory Specific", "Multi-Region"]),
            random.choice(["Output Deal", "Volume Deal", "Library Buy"])
        ))
    c.executemany("INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?)", deals)

    conn.commit()
    return conn

def execute_sql(sql, conn):
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
