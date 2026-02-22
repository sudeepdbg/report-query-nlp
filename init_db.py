import sqlite3

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
