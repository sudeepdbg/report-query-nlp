import sqlite3
import pandas as pd
import random
import logging

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

    # Create tables
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

    # ---------- CONTENT PLANNING (200+ rows) ----------
    networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX Africa", "MAX India", "MAX UK"]
    statuses_content = ["Scheduled", "Fulfilled", "Delivered", "Not Ready"]
    regions = ["NA", "EMEA", "APAC", "LATAM"]

    content_base = [
        "House of the Dragon S{}", "The Penguin", "The Last of Us S{}", "Dune: Prophecy",
        "The White Lotus S{}", "Industry S{}", "Euphoria S{}", "The Last Kingdom",
        "The Gilded Age", "El Encargado", "Iosi, el espía arrepentido", "Oppenheimer",
        "Godzilla Minus One", "Barbie", "Succession S{}", "Barry S{}", "Curb Your Enthusiasm S{}",
        "Westworld S{}", "His Dark Materials S{}", "Raised by Wolves", "Lovecraft Country",
        "The Nevers", "Perry Mason", "The Undoing", "Mare of Easttown", "The Flight Attendant",
        "The Great", "The Plot Against America", "The New Pope", "The Young Pope",
        "My Brilliant Friend S{}", "Gomorrah S{}", "ZeroZeroZero", "The Name of the Rose",
        "Devs", "Little Birds", "The Third Day", "The Outsider", "The Stand",
        "The Haunting of Bly Manor", "The Haunting of Hill House", "Midnight Mass",
        "Midnight Club", "The Fall of the House of Usher", "The Watcher",
        "Dahmer – Monster: The Jeffrey Dahmer Story", "Monster: The Menendez Brothers",
        "The Crown S{}", "The Queen's Gambit", "Unorthodox", "The Politician",
        "Hollywood", "Ratched", "Halston", "The Andy Warhol Diaries",
        "Pretend It's a City", "My Next Guest Needs No Introduction", "The Shop",
        "5th July", "The Vow", "Heist", "Trial 4", "The Innocence Files",
        "The Confession Tapes", "The Keepers", "Making a Murderer", "The Staircase",
        "The Jinx", "The Act", "Dirty John", "The Thing About Pam", "The Dropout",
        "WeWork: or the Making and Breaking of a $47 Billion Unicorn", "Fyre",
        "The Tinder Swindler", "Bad Vegan", "The Most Hated Man on the Internet",
        "The Social Dilemma", "The Great Hack", "The Vow", "Wild Wild Country",
        "Tiger King", "Cheer", "Last Chance U", "Drive to Survive", "Full Swing",
        "Break Point", "Quarterback", "Receiver", "Formula 1: Drive to Survive",
        "Tour de France: Unchained", "NASCAR: Full Speed", "Six Nations: Full Contact",
        "The Test", "All or Nothing", "Hard Knocks", "The Last Dance"
    ]

    content_entries = []
    content_id = 1
    for base in content_base:
        if "S{}" in base:
            for season in range(1, 6):
                title = base.format(season)
                for net in random.sample(networks, k=min(3, len(networks))):
                    status = random.choice(statuses_content)
                    region = random.choice(regions)
                    planned = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                    content_entries.append((content_id, net, title, status, planned, region))
                    content_id += 1
        else:
            for net in random.sample(networks, k=min(3, len(networks))):
                status = random.choice(statuses_content)
                region = random.choice(regions)
                planned = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                content_entries.append((content_id, net, base, status, planned, region))
                content_id += 1
        if content_id > 200:
            break
    c.executemany("INSERT INTO content_planning VALUES (?,?,?,?,?,?)", content_entries)

    # ---------- WORK ORDERS (500+ rows) ----------
    vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E", "Vendor F", "Vendor G", "Vendor H"]
    statuses_work = ["Delayed", "In Progress", "Completed", "Pending Review", "On Hold", "Cancelled"]
    priorities = ["A", "B", "C", "D"]
    offering_types = ["Migration", "Encoding", "Subtitle", "QC Review", "Audio", "Video Edit", "Metadata", "Artwork", "Trailer", "Promo"]

    work_orders = []
    for i in range(1, 501):
        wo = f"WO-2024-{i:04d}"
        offering = f"MAX {random.choice(regions)} - {random.choice(offering_types)}"
        status = random.choice(statuses_work)
        due = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        region = random.choice(regions)
        vendor = random.choice(vendors)
        priority = random.choice(priorities)
        work_orders.append((i, wo, offering, status, due, region, vendor, priority))
    c.executemany("INSERT INTO work_orders VALUES (?,?,?,?,?,?,?,?)", work_orders)

    # ---------- DEALS (250+ rows) ----------
    deal_names = [
        "Warner Bros 2024 Package", "BBC Studios Renewal", "Sony Pictures Deal",
        "Paramount Animation", "Studio Ghibli Classics", "A24 Film Slate",
        "Discovery+ Originals", "HBO Max Acquisitions", "CNN International",
        "Cartoon Network Library", "Adult Swim Series", "TNT Sports Rights",
        "TBS Comedy Specials", "Rooster Teeth Collection", "DC Universe Animated",
        "Universal Pictures Package", "Disney+ Hotstar Exclusives", "Netflix Originals",
        "Amazon Prime Video Acquisition", "Apple TV+ Series", "Hulu Co-Production",
        "Peacock Exclusive", "BBC America Co-Production", "ITV Studios Deal",
        "Channel 4 Acquisitions", "Sky Original Series", "NBCUniversal Content",
        "ViacomCBS Library", "Nickelodeon Animation", "MTV Reality Shows",
        "Comedy Central Specials", "BET Plus Originals", "Showtime Series",
        "Starz Premium Content", "Lionsgate Films", "MGM Classics",
        "Legendary Entertainment", "Annapurna Pictures", "STX Entertainment",
        "FilmNation Productions", "A24 Horror Collection", "Neon Films",
        "Bleecker Street Releases", "Sony Pictures Classics", "Focus Features",
        "Searchlight Pictures", "Fox Searchlight", "Miramax Library",
        "The Weinstein Co. (pre-2018)", "DreamWorks Animation", "Illumination Films",
        "Blue Sky Studios", "Laika Productions", "StudioCanal Collection",
        "Pathé Films", "Gaumont Distribution", "TF1 Group", "M6 Group",
        "ProSiebenSat.1 Content", "RTL Group", "Sky Deutschland Originals",
        "Mediaset Premium", "Telecinco Cinema", "Antena 3 Series", "TVE International",
        "RTVE Content", "TVP Poland", "Česká televize", "RÚV Iceland",
        "SVT Sweden", "NRK Norway", "YLE Finland", "DR Denmark",
        "Viaplay Originals", "TV2 Norway", "TV4 Sweden", "MTV Finland",
        "C More Entertainment", "HBO Nordic", "Canal+ Series", "OCS France",
        "Arte France", "ZDF Enterprises", "ARD Degeto", "ORF Austria",
        "SRG SSR", "RSI Switzerland", "RTS Switzerland", "RTR Russia"
    ]
    vendors_deals = [
        "Warner Bros", "BBC", "Sony", "Paramount", "Ghibli", "A24", "Discovery", "HBO",
        "CNN", "Cartoon Network", "Adult Swim", "TNT Sports", "TBS", "Rooster Teeth", "DC",
        "Universal", "Disney", "Netflix", "Amazon", "Apple", "Hulu", "Peacock", "BBC America",
        "ITV", "Channel 4", "Sky", "NBCUniversal", "ViacomCBS", "Nickelodeon", "MTV",
        "Comedy Central", "BET", "Showtime", "Starz", "Lionsgate", "MGM", "Legendary",
        "Annapurna", "STX", "FilmNation", "A24", "Neon", "Bleecker Street",
        "Sony Pictures Classics", "Focus Features", "Searchlight", "Fox Searchlight",
        "Miramax", "The Weinstein Co.", "DreamWorks", "Illumination", "Blue Sky",
        "Laika", "StudioCanal", "Pathé", "Gaumont", "TF1", "M6", "ProSiebenSat.1",
        "RTL", "Sky Deutschland", "Mediaset", "Telecinco", "Antena 3", "TVE", "RTVE",
        "TVP", "Česká televize", "RÚV", "SVT", "NRK", "YLE", "DR", "Viaplay",
        "TV2 Norway", "TV4 Sweden", "MTV Finland", "C More", "HBO Nordic", "Canal+",
        "OCS", "Arte", "ZDF", "ARD", "ORF", "SRG SSR", "RSI", "RTS", "RTR"
    ]
    statuses_deal = ["Active", "Completed", "Pending", "Negotiating", "Expired"]

    deals = []
    for i in range(1, 251):
        name = random.choice(deal_names)
        vendor = random.choice(vendors_deals)
        value = round(random.uniform(200000, 20000000), 2)
        date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        region = random.choice(regions)
        status = random.choice(statuses_deal)
        deals.append((i, name, vendor, value, date, region, status))
    c.executemany("INSERT INTO deals VALUES (?,?,?,?,?,?,?)", deals)

    conn.commit()
    logger.info(f"✅ Database initialized with {len(content_entries)} content items, {len(work_orders)} work orders, {len(deals)} deals.")
    return conn

def execute_sql(sql, conn):
    """Execute SQL on the provided connection."""
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
