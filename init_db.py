import sqlite3
from datetime import datetime, timedelta
import random

conn = sqlite3.connect('vantage.db')
c = conn.cursor()

# --- Helper to generate dates ---
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

# --- Content Planning table ---
c.execute('''
    CREATE TABLE IF NOT EXISTS content_planning (
        id INTEGER PRIMARY KEY,
        network TEXT,
        content_title TEXT,
        status TEXT,
        planned_date TEXT,
        region TEXT
    )
''')

c.execute("DELETE FROM content_planning")  # clear old data

content_titles = [
    ("MAX US", "House of the Dragon S2", "Fulfilled"),
    ("MAX US", "The Penguin", "Not Ready"),
    ("MAX US", "The Last of Us S2", "Scheduled"),
    ("MAX US", "Dune: Prophecy", "Delivered"),
    ("MAX Europe", "The White Lotus S3", "Scheduled"),
    ("MAX Europe", "Industry S3", "Fulfilled"),
    ("MAX Europe", "Euphoria S3", "Not Ready"),
    ("MAX Australia", "The Last Kingdom", "Delivered"),
    ("MAX Australia", "Dune: Prophecy", "Scheduled"),
    ("MAX Australia", "The Gilded Age", "Fulfilled"),
    ("MAX LatAm", "El Encargado", "Delivered"),
    ("MAX LatAm", "Iosi, el espía arrepentido", "Scheduled"),
    ("MAX Asia", "Oppenheimer", "Fulfilled"),
    ("MAX Asia", "Godzilla Minus One", "Not Ready"),
]
sample_content = []
for i, (net, title, status) in enumerate(content_titles, start=1):
    region = net.split()[1] if " " in net else "Global"
    planned = random_date(start_date, end_date).strftime("%Y-%m-%d")
    sample_content.append((i, net, title, status, planned, region))

c.executemany("INSERT INTO content_planning VALUES (?,?,?,?,?,?)", sample_content)

# --- Work Orders table ---
c.execute('''
    CREATE TABLE IF NOT EXISTS work_orders (
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

c.execute("DELETE FROM work_orders")

vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E"]
statuses = ["Delayed", "In Progress", "Completed", "Pending Review"]
regions = ["NA", "APAC", "EMEA", "LATAM"]
priorities = ["A", "B", "C"]

sample_orders = []
for i in range(1, 21):
    wo = f"WO-2024-{i:03d}"
    offering = f"MAX {random.choice(regions)} - {random.choice(['Migration', 'Encoding', 'Subtitle', 'QC Review', 'Audio'])}"
    status = random.choice(statuses)
    due = random_date(start_date, end_date).strftime("%Y-%m-%d")
    region = random.choice(regions)
    vendor = random.choice(vendors)
    priority = random.choice(priorities)
    sample_orders.append((i, wo, offering, status, due, region, vendor, priority))

c.executemany("INSERT INTO work_orders VALUES (?,?,?,?,?,?,?,?)", sample_orders)

# --- Deals table ---
c.execute('''
    CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY,
        deal_name TEXT,
        vendor TEXT,
        deal_value REAL,
        deal_date TEXT,
        region TEXT,
        status TEXT
    )
''')

c.execute("DELETE FROM deals")

deal_names = [
    "Warner Bros 2024 Package", "BBC Studios Renewal", "Sony Pictures Deal",
    "Paramount Animation", "Studio Ghibli Classics", "A24 Film Slate",
    "Discovery+ Originals", "HBO Max Acquisitions", "CNN International",
    "Cartoon Network Library", "Adult Swim Series", "TNT Sports Rights",
    "TBS Comedy Specials", "Rooster Teeth Collection", "DC Universe Animated"
]
vendors_deals = ["Warner Bros", "BBC", "Sony", "Paramount", "Ghibli", "A24", "Discovery", "HBO", "CNN", "Cartoon Network", "Adult Swim", "TNT Sports", "TBS", "Rooster Teeth", "DC"]
statuses_deal = ["Active", "Completed", "Pending"]

sample_deals = []
for i, (name, vendor) in enumerate(zip(deal_names, vendors_deals), start=1):
    value = round(random.uniform(500000, 5000000), 2)
    date = random_date(start_date, end_date).strftime("%Y-%m-%d")
    region = random.choice(regions)
    status = random.choice(statuses_deal)
    sample_deals.append((i, name, vendor, value, date, region, status))

c.executemany("INSERT INTO deals VALUES (?,?,?,?,?,?,?)", sample_deals)

conn.commit()
conn.close()
print("✅ Database initialized with extensive sample data.")import sqlite3

conn = sqlite3.connect('vantage.db')
c = conn.cursor()

# Content Planning table
c.execute('''
    CREATE TABLE IF NOT EXISTS content_planning (
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
    CREATE TABLE IF NOT EXISTS work_orders (
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
    CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY,
        deal_name TEXT,
        vendor TEXT,
        deal_value REAL,
        deal_date TEXT,
        region TEXT,
        status TEXT
    )
''')

# Sample data (only insert if empty)
c.execute("SELECT COUNT(*) FROM content_planning")
if c.fetchone()[0] == 0:
    sample_content = [
        (1, 'MAX Australia', 'The Last Kingdom', 'Delivered', '2024-03-15', 'APAC'),
        (2, 'MAX Australia', 'Dune: Prophecy', 'Scheduled', '2024-04-01', 'APAC'),
        (3, 'MAX US', 'House of the Dragon S2', 'Fulfilled', '2024-05-20', 'NA'),
        (4, 'MAX US', 'The Penguin', 'Not Ready', '2024-06-10', 'NA'),
        (5, 'MAX Europe', 'The White Lotus S3', 'Scheduled', '2024-07-01', 'EMEA')
    ]
    c.executemany("INSERT INTO content_planning VALUES (?,?,?,?,?,?)", sample_content)

c.execute("SELECT COUNT(*) FROM work_orders")
if c.fetchone()[0] == 0:
    sample_orders = [
        (1, 'WO-2024-001', 'MAX Australia - Migration', 'Delayed', '2024-03-20', 'APAC', 'Vendor A', 'A'),
        (2, 'WO-2024-002', 'MAX US - Encoding', 'In Progress', '2024-03-25', 'NA', 'Vendor B', 'A'),
        (3, 'WO-2024-003', 'MAX Europe - Subtitle', 'Completed', '2024-02-15', 'EMEA', 'Vendor C', 'B'),
        (4, 'WO-2024-004', 'MAX US - QC Review', 'In Progress', '2024-03-30', 'NA', 'Vendor B', 'A'),
        (5, 'WO-2024-005', 'MAX Australia - Audio', 'Delayed', '2024-03-10', 'APAC', 'Vendor A', 'A')
    ]
    c.executemany("INSERT INTO work_orders VALUES (?,?,?,?,?,?,?,?)", sample_orders)

c.execute("SELECT COUNT(*) FROM deals")
if c.fetchone()[0] == 0:
    sample_deals = [
        (1, 'Warner Bros 2024 Package', 'Warner Bros', 1500000, '2024-02-01', 'NA', 'Active'),
        (2, 'BBC Studios Renewal', 'BBC', 850000, '2024-02-15', 'EMEA', 'Completed'),
        (3, 'Sony Pictures Deal', 'Sony', 2200000, '2024-03-01', 'APAC', 'Pending'),
        (4, 'Paramount Animation', 'Paramount', 1200000, '2024-02-20', 'NA', 'Active'),
        (5, 'Studio Ghibli Classics', 'Ghibli', 650000, '2024-01-30', 'APAC', 'Completed')
    ]
    c.executemany("INSERT INTO deals VALUES (?,?,?,?,?,?,?)", sample_deals)

conn.commit()
conn.close()
print("Database initialized with sample data.")
