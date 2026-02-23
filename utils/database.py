import sqlite3
import pandas as pd
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global in‑memory database connection
def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Create tables and insert enriched sample data into in‑memory DB."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Content Planning table
    c.execute('''
        CREATE TABLE content_planning (
            id INTEGER PRIMARY KEY,
            network TEXT,
            content_title TEXT,
            status TEXT,
            planned_date TEXT,
            region TEXT
        )
    ''')
    
    # Work Orders table
    c.execute('''
        CREATE TABLE work_orders (
            id INTEGER PRIMARY KEY,
            work_order TEXT,
            offering TEXT,
            status TEXT,
            due_date TEXT,
            region TEXT,
            vendor TEXT,
            priority TEXT
        )
    ''')
    
    # Deals table
    c.execute('''
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY,
            deal_name TEXT,
            vendor TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            status TEXT
        )
    ''')
    
    # --- Enriched content planning data ---
    content_titles = [
        ("MAX US", "House of the Dragon S2", "Fulfilled", "NA"),
        ("MAX US", "The Penguin", "Not Ready", "NA"),
        ("MAX US", "The Last of Us S2", "Scheduled", "NA"),
        ("MAX US", "Dune: Prophecy", "Delivered", "NA"),
        ("MAX Europe", "The White Lotus S3", "Scheduled", "EMEA"),
        ("MAX Europe", "Industry S3", "Fulfilled", "EMEA"),
        ("MAX Europe", "Euphoria S3", "Not Ready", "EMEA"),
        ("MAX Australia", "The Last Kingdom", "Delivered", "APAC"),
        ("MAX Australia", "Dune: Prophecy", "Scheduled", "APAC"),
        ("MAX Australia", "The Gilded Age", "Fulfilled", "APAC"),
        ("MAX LatAm", "El Encargado", "Delivered", "LATAM"),
        ("MAX LatAm", "Iosi, el espía arrepentido", "Scheduled", "LATAM"),
        ("MAX Asia", "Oppenheimer", "Fulfilled", "APAC"),
        ("MAX Asia", "Godzilla Minus One", "Not Ready", "APAC"),
    ]
    for i, (net, title, status, reg) in enumerate(content_titles, start=1):
        c.execute('''
            INSERT INTO content_planning (id, network, content_title, status, planned_date, region)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (i, net, title, status, f"2024-{i%12+1:02d}-{i%28+1:02d}", reg))
    
    # --- Enriched work orders ---
    vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E"]
    statuses = ["Delayed", "In Progress", "Completed", "Pending Review"]
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    priorities = ["A", "B", "C"]
    
    for i in range(1, 21):
        wo = f"WO-2024-{i:03d}"
        offering = f"MAX {random.choice(regions)} - {random.choice(['Migration', 'Encoding', 'Subtitle', 'QC Review', 'Audio'])}"
        status = random.choice(statuses)
        due = f"2024-{i%12+1:02d}-{i%28+1:02d}"
        region = random.choice(regions)
        vendor = random.choice(vendors)
        priority = random.choice(priorities)
        c.execute('''
            INSERT INTO work_orders (id, work_order, offering, status, due_date, region, vendor, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (i, wo, offering, status, due, region, vendor, priority))
    
    # --- Enriched deals ---
    deal_names = [
        "Warner Bros 2024 Package", "BBC Studios Renewal", "Sony Pictures Deal",
        "Paramount Animation", "Studio Ghibli Classics", "A24 Film Slate",
        "Discovery+ Originals", "HBO Max Acquisitions", "CNN International",
        "Cartoon Network Library", "Adult Swim Series", "TNT Sports Rights",
        "TBS Comedy Specials", "Rooster Teeth Collection", "DC Universe Animated"
    ]
    vendors_deals = ["Warner Bros", "BBC", "Sony", "Paramount", "Ghibli", "A24", "Discovery", "HBO", "CNN", "Cartoon Network", "Adult Swim", "TNT Sports", "TBS", "Rooster Teeth", "DC"]
    statuses_deal = ["Active", "Completed", "Pending"]
    
    for i, (name, vendor) in enumerate(zip(deal_names, vendors_deals), start=1):
        value = round(random.uniform(500000, 5000000), 2)
        date = f"2024-{i%12+1:02d}-{i%28+1:02d}"
        region = random.choice(regions)
        status = random.choice(statuses_deal)
        c.execute('''
            INSERT INTO deals (id, deal_name, vendor, deal_value, deal_date, region, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (i, name, vendor, value, date, region, status))
    
    conn.commit()
    logger.info("✅ In‑memory database initialized with sample data.")
    return conn

def execute_sql(sql, conn):
    """Execute SQL on the provided connection."""
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
