"""
database.py — Foundry Vantage Rights Explorer
Schema mirrors the HBO/WBD Rights Explorer MVP data model.
"""
import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
from typing import Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Reference data ───────────────────────────────────────────────────────────
SERIES_LIST = [
    ("S001", "House of the Dragon",    "HBO",        "HBO/Cinemax/HBO Max"),
    ("S002", "The Last of Us",         "Sony",       "HBO/Cinemax/HBO Max"),
    ("S003", "Succession",             "HBO",        "HBO/Cinemax/HBO Max"),
    ("S004", "The White Lotus",        "HBO",        "HBO/Cinemax/HBO Max"),
    ("S005", "Euphoria",               "HBO",        "HBO/Cinemax/HBO Max"),
    ("S006", "Westworld",              "HBO",        "HBO/Cinemax/HBO Max"),
    ("S007", "Barry",                  "HBO",        "HBO/Cinemax/HBO Max"),
    ("S008", "True Detective",         "HBO",        "HBO/Cinemax/HBO Max"),
    ("S009", "The Wire",               "HBO",        "HBO/Cinemax/HBO Max"),
    ("S010", "The Sopranos",           "HBO",        "HBO/Cinemax/HBO Max"),
    ("S011", "The Penguin",            "WB TV",      "HBO/Cinemax/HBO Max"),
    ("S012", "Dune: Prophecy",         "Legendary",  "HBO/Cinemax/HBO Max"),
    ("S013", "The Bear",               "FX",         "Hulu"),
    ("S014", "Andor",                  "Disney",     "Disney+"),
    ("S015", "The Mandalorian",        "Disney",     "Disney+"),
    ("S016", "Foundation",             "Apple",      "Apple TV+"),
    ("S017", "Shrinking",              "Apple",      "Apple TV+"),
    ("S018", "Reacher",                "Amazon",     "Prime Video"),
    ("S019", "The Boys",               "Amazon",     "Prime Video"),
    ("S020", "Squid Game",             "Netflix",    "Netflix"),
]

MOVIES_LIST = [
    ("M001", "Dune: Part One",            "Legendary/WB",   "Warner Bros",    "Sci-Fi",      "Dune",         "Theatrical",  401,   "PG-13", 2021),
    ("M002", "Dune: Part Two",            "Legendary/WB",   "Warner Bros",    "Sci-Fi",      "Dune",         "Theatrical",  711,   "PG-13", 2024),
    ("M003", "Barbie",                    "WB",             "Warner Bros",    "Comedy",      "Mattel",       "Theatrical", 1441,   "PG-13", 2023),
    ("M004", "Oppenheimer",               "Universal/WB",   "Warner Bros",    "Historical", None,           "Theatrical",  952,   "R",     2023),
    ("M005", "The Batman",                "WB",             "Warner Bros",    "Action",      "DC",           "Theatrical",  770,   "PG-13", 2022),
    ("M006", "Aquaman and the Lost Kingdom", "WB",          "Warner Bros",    "Action",      "DC",           "Theatrical",  297,   "PG-13", 2023),
    ("M007", "The Flash",                 "WB",             "Warner Bros",    "Action",      "DC",           "Theatrical",  268,   "PG-13", 2023),
    ("M008", "Black Adam",                "WB",             "Warner Bros",    "Action",      "DC",           "Theatrical",  393,   "PG-13", 2022),
    ("M009", "Shazam! Fury of the Gods",  "WB",             "Warner Bros",    "Action",      "DC",           "Theatrical",  134,   "PG-13", 2023),
    ("M010", "Wonka",                     "WB",             "Warner Bros",    "Fantasy",    None,           "Theatrical",  632,   "PG",    2023),
    ("M011", "Beetlejuice Beetlejuice",   "WB",             "Warner Bros",    "Comedy",     None,           "Theatrical",  449,   "PG-13", 2024),
    ("M012", "Furiosa",                   "WB",             "Warner Bros",    "Action",      "Mad Max",      "Theatrical",  173,   "R",     2024),
    ("M013", "Meg 2: The Trench",         "WB",             "Warner Bros",    "Action",     None,           "Theatrical",  396,   "PG-13", 2023),
    ("M014", "The Color Purple",          "WB/Amblin",      "Warner Bros",    "Drama",      None,           "Theatrical",  67,    "PG-13", 2023),
    ("M015", "Elvis",                     "WB",             "Warner Bros",    "Drama",      None,           "Theatrical",  287,   "PG-13", 2022),
    ("M016", "Animal Kingdom",            "HBO",            "HBO/Cinemax/HBO Max", "Crime", None,           "HBO Original", 0,    "TV-MA", 2022),
    ("M017", "White Noise",               "Netflix/WB",     "Warner Bros",    "Drama",      None,           "Library",      0,    "R",     2022),
    ("M018", "The Witches",               "WB",             "Warner Bros",    "Fantasy",    None,           "Library",      27,   "PG",    2020),
    ("M019", "Tenet",                     "WB",             "Warner Bros",    "Thriller",   None,           "Library",     365,   "PG-13", 2020),
    ("M020", "Wonder Woman 1984",         "WB",             "Warner Bros",    "Action",      "DC",           "Library",     169,   "PG-13", 2020),
    ("M021", "Mortal Kombat",             "WB/New Line",    "Warner Bros",    "Action",     None,           "Library",     83,    "R",     2021),
    ("M022", "The Suicide Squad",         "WB",             "Warner Bros",    "Action",      "DC",           "Library",     167,   "R",     2021),
    ("M023", "Matrix Resurrections",      "WB",             "Warner Bros",    "Sci-Fi",      "Matrix",       "Library",     157,   "R",     2021),
    ("M024", "Space Jam: A New Legacy",   "WB",             "Warner Bros",    "Animation",  None,           "Library",     162,   "PG",    2021),
    ("M025", "Godzilla vs. Kong",         "WB/Legendary",   "Warner Bros",    "Action",      "MonsterVerse", "Library",     468,   "PG-13", 2021),
]

CONTENT_CATEGORIES = ["Theatrical", "HBO Original", "Library", "Direct-to-Streaming", "DTV"]
GENRES        = ["Drama", "Thriller", "Fantasy", "Sci-Fi", "Comedy", "Action",
                 "Historical", "Crime", "Documentary", "Horror", "Animation", "Reality"]
LANGUAGES     = ["English", "Spanish", "French", "German", "Italian", "Japanese",
                 "Korean", "Mandarin", "Hindi", "Portuguese", "Arabic", "Dutch"]
TERRITORIES_BY_REGION = {
    "NA":    ["USA", "Canada", "Mexico", "Puerto Rico"],
    "APAC":  ["Japan", "South Korea", "Australia", "Singapore", "India", "China", "New Zealand", "Thailand"],
    "EMEA":  ["UK", "Germany", "France", "Italy", "Spain", "UAE", "Netherlands", "Poland", "South Africa", "Sweden"],
    "LATAM": ["Brazil", "Argentina", "Colombia", "Chile", "Peru", "Venezuela", "Uruguay"],
}
ALL_TERRITORIES  = [t for ts in TERRITORIES_BY_REGION.values() for t in ts]
REGIONS          = list(TERRITORIES_BY_REGION.keys())
TERRITORY_REGION = {t: r for r, ts in TERRITORIES_BY_REGION.items() for t in ts}
RIGHTS_TYPES     = ["Exhibition", "Exhibition & Distribution"]
MEDIA_PRIMARY    = ["PayTV", "STB-VOD", "SVOD", "FAST"]
MEDIA_ANCILLARY  = ["CatchUp", "StartOver", "Simulcast", "TempDownload", "DownloadToOwn"]
BRANDS           = ["HBO", "HBO Max", "Cinemax", "Max", "WB", "TNT", "TBS", "truTV", "CNN"]
DEAL_TYPES       = ["Output Deal", "Library Buy", "First-Look Deal", "Co-Production",
                    "Licensing Agreement", "Distribution Deal", "Volume Deal"]
DNA_CATEGORIES   = ["Nudity", "Violence", "Language", "Drug Use", "Mature Themes"]
DNA_SUBCATS      = {
    "Nudity":        ["Full Nudity", "Partial Nudity", "Sexual Content"],
    "Violence":      ["Graphic Violence", "Mild Violence", "Animated Violence"],
    "Language":      ["Strong Language", "Mild Language"],
    "Drug Use":      ["Recreational", "Medical"],
    "Mature Themes": ["Suicide", "Abuse", "War", "Political"],
}
VENDORS = [
    (1, "PixelPerfect Studios",4.8, "Post-Production", "NA"),
    (2, "GlobalDub International",4.5, "Localization", "EMEA"),
    (3, "StreamOps Asia",4.2, "Content Delivery", "APAC"),
    (4, "VisionPost Brasil",4.6, "Post-Production", "LATAM"),
    (5, "CineColor Labs",4.9, "Color Grading", "NA"),
    (6, "AudioMasters DE",4.7, "Audio Post", "EMEA"),
    (7, "CaptionWorks India",4.3, "Subtitling", "APAC"),
    (8, "VFX Masters Mexico",4.4, "VFX", "LATAM"),
    (9, "ContentGuard",4.1, "Content Security", "NA"),
    (10, "MediaBridge AU",4.5, "Distribution", "APAC"),
]

def _rnd_date(start, end):
    delta = max((end - start).days, 1)
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")

def _rnd_terr(region=None, n=None):
    pool = TERRITORIES_BY_REGION.get(region, ALL_TERRITORIES)
    k = n or random.randint(1, min(4, len(pool)))
    return ",".join(random.sample(pool, min(k, len(pool))))

def _rnd_lang(n=None):
    return ",".join(random.sample(LANGUAGES, n or random.randint(1,4)))

def _rnd_brand(n=None):
    return ",".join(random.sample(BRANDS, n or random.randint(1,3)))

def _rnd_media_p():
    return ",".join(random.sample(MEDIA_PRIMARY, random.randint(1,3)))

def _rnd_media_a():
    return ",".join(random.sample(MEDIA_ANCILLARY, random.randint(0,3))) if random.random()>0.3 else ""

# ─── Schema ───────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE series (
series_id TEXT PRIMARY KEY, series_title TEXT NOT NULL,
series_source TEXT, controlling_entity TEXT,
genre TEXT, description TEXT, release_year INTEGER,
franchise TEXT, created_at TEXT
);
CREATE TABLE season (
season_id TEXT PRIMARY KEY, series_id TEXT REFERENCES series(series_id),
season_title TEXT, season_number INTEGER, season_source TEXT,
controlling_entity TEXT, episode_count INTEGER, release_year INTEGER, created_at TEXT
);
CREATE TABLE movie (
movie_id TEXT PRIMARY KEY,
movie_title TEXT NOT NULL,
movie_source TEXT,
controlling_entity TEXT,
genre TEXT, description TEXT,
franchise TEXT,
content_category TEXT,
theatrical_release_date TEXT,
box_office_gross_usd_m REAL DEFAULT 0,
age_rating TEXT,
release_year INTEGER,
created_at TEXT
);
CREATE TABLE title (
title_id TEXT PRIMARY KEY,
season_id TEXT REFERENCES season(season_id),
series_id TEXT REFERENCES series(series_id),
movie_id TEXT REFERENCES movie(movie_id),
title_source TEXT, title_identifier TEXT,
episode_number INTEGER, title_type TEXT,
title_name TEXT NOT NULL, controlling_entity TEXT,
release_year INTEGER, description TEXT, genre TEXT,
franchise TEXT, runtime_minutes INTEGER, age_rating TEXT,
content_category TEXT,
theatrical_release_date TEXT,
box_office_gross_usd_m REAL DEFAULT 0,
region TEXT, created_at TEXT
);
CREATE TABLE vendors (
vendor_id INTEGER PRIMARY KEY, vendor_name TEXT NOT NULL,
rating REAL, vendor_type TEXT, region TEXT,
certification_level TEXT, active INTEGER DEFAULT 1, created_date TEXT,
contact_email TEXT, phone TEXT, address TEXT,
payment_terms TEXT, total_spend REAL DEFAULT 0
);
CREATE TABLE deals (
deal_id         INTEGER PRIMARY KEY,
deal_name       TEXT NOT NULL,
vendor_id       INTEGER REFERENCES vendors(vendor_id),
vendor_name     TEXT,
deal_type       TEXT, 
deal_value      REAL,
deal_date       TEXT,
expiry_date     TEXT,
rights_scope    TEXT,
territory       TEXT,
region          TEXT,
status          TEXT,
payment_status  TEXT,
notes           TEXT,
created_at      TEXT
);
CREATE TABLE work_orders (
work_order_id INTEGER PRIMARY KEY,
title_id TEXT REFERENCES title(title_id), title_name TEXT,
vendor_id INTEGER REFERENCES vendors(vendor_id), vendor_name TEXT,
work_type TEXT, status TEXT, priority TEXT, region TEXT, territory TEXT,
due_date TEXT, quality_score REAL, rework_count INTEGER DEFAULT 0,
cost REAL, billing_status TEXT, created_at TEXT
);
CREATE TABLE content_deal (
deal_id TEXT PRIMARY KEY, deal_source TEXT, deal_type TEXT,
deal_name TEXT, primary_parties TEXT, secondary_parties TEXT,
region TEXT, notes TEXT, created_at TEXT
);
CREATE TABLE media_rights (
rights_id TEXT PRIMARY KEY,
deal_id TEXT REFERENCES content_deal(deal_id),
title_id TEXT REFERENCES title(title_id), title_name TEXT,
rights_type TEXT,
term_from TEXT, term_to TEXT,
estimated_term_from TEXT, estimated_term_to TEXT,
territories TEXT, region TEXT,
media_platform_primary TEXT, media_platform_ancillary TEXT,
language TEXT, brand TEXT,
exclusivity INTEGER DEFAULT 0, holdback INTEGER DEFAULT 0, holdback_days INTEGER DEFAULT 0,
notes_general TEXT, notes_restrictive TEXT, notes_end_user_rights TEXT,
status TEXT, days_remaining INTEGER, created_at TEXT
);
CREATE TABLE exhibition_restrictions (
restriction_id TEXT PRIMARY KEY,
rights_id TEXT REFERENCES media_rights(rights_id),
title_id TEXT, deal_id TEXT,
restriction_term_from TEXT, restriction_term_to TEXT,
max_plays INTEGER, max_plays_per_day INTEGER,
max_days INTEGER, max_networks INTEGER,
additional_notes TEXT, created_at TEXT
);
CREATE TABLE elemental_deal (
elemental_deal_id TEXT PRIMARY KEY, deal_source TEXT, deal_type TEXT,
deal_id TEXT, deal_name TEXT, primary_parties TEXT, secondary_parties TEXT,
region TEXT, created_at TEXT
);
CREATE TABLE elemental_rights (
elemental_rights_id TEXT PRIMARY KEY,
elemental_deal_id TEXT REFERENCES elemental_deal(elemental_deal_id),
title_id TEXT REFERENCES title(title_id), title_name TEXT,
term_from TEXT, term_to TEXT, territories TEXT, region TEXT,
media_platform_primary TEXT, media_platform_ancillary TEXT,
language TEXT, brand TEXT, status TEXT, created_at TEXT
);
CREATE TABLE do_not_air (
dna_id TEXT PRIMARY KEY,
title_id TEXT REFERENCES title(title_id), title_name TEXT,
region TEXT, territory TEXT, media_type TEXT,
reason_category TEXT, reason_subcategory TEXT,
term_from TEXT, term_to TEXT,
additional_notes TEXT, active INTEGER DEFAULT 1, created_at TEXT
);
CREATE TABLE sales_deal (
sales_deal_id TEXT PRIMARY KEY, deal_type TEXT, deal_name TEXT,
title_id TEXT REFERENCES title(title_id), title_name TEXT,
buyer TEXT, territory TEXT, region TEXT, media_platform TEXT,
term_from TEXT, term_to TEXT, deal_value REAL,
currency TEXT DEFAULT 'USD', status TEXT, created_at TEXT
);
CREATE TABLE IF NOT EXISTS alerts (
alert_id    INTEGER PRIMARY KEY AUTOINCREMENT,
alert_type  TEXT NOT NULL,
label       TEXT NOT NULL,
title_name  TEXT,
rights_id   TEXT,
region      TEXT,
platform    TEXT,
expiry_date TEXT,
days_threshold INTEGER DEFAULT 90,
persona     TEXT,
notes       TEXT,
created_at  TEXT DEFAULT (DATETIME('now')),
dismissed   INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS query_log (
log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
timestamp       TEXT DEFAULT (DATETIME('now')),
session_id      TEXT NOT NULL,
user_id         TEXT,
region          TEXT,
persona         TEXT,
question        TEXT,
generated_sql   TEXT,
intent_domain   TEXT,
cross_intent    INTEGER DEFAULT 0,
latency_ms      REAL,
success         INTEGER DEFAULT 1,
error_message   TEXT,
rows_returned   INTEGER DEFAULT 0,
chart_type      TEXT
);
CREATE TABLE IF NOT EXISTS feedback (
feedback_id     INTEGER PRIMARY KEY AUTOINCREMENT,
log_id          INTEGER REFERENCES query_log(log_id),
feedback_type   TEXT NOT NULL,
timestamp       TEXT DEFAULT (DATETIME('now')),
comment         TEXT
);
CREATE TABLE IF NOT EXISTS user_session (
session_id      TEXT PRIMARY KEY,
start_time      TEXT DEFAULT (DATETIME('now')),
last_active     TEXT DEFAULT (DATETIME('now')),
user_id         TEXT,
region          TEXT,
persona         TEXT
);
"""

INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_title_series ON title(series_id)",
    "CREATE INDEX IF NOT EXISTS idx_title_season ON title(season_id)",
    "CREATE INDEX IF NOT EXISTS idx_title_region ON title(region)",
    "CREATE INDEX IF NOT EXISTS idx_mr_deal ON media_rights(deal_id)",
    "CREATE INDEX IF NOT EXISTS idx_mr_title ON media_rights(title_id)",
    "CREATE INDEX IF NOT EXISTS idx_mr_region ON media_rights(region)",
    "CREATE INDEX IF NOT EXISTS idx_mr_status ON media_rights(status)",
    "CREATE INDEX IF NOT EXISTS idx_mr_term ON media_rights(term_to)",
    "CREATE INDEX IF NOT EXISTS idx_er_title ON elemental_rights(title_id)",
    "CREATE INDEX IF NOT EXISTS idx_dna_title ON do_not_air(title_id)",
    "CREATE INDEX IF NOT EXISTS idx_dna_region ON do_not_air(region)",
    "CREATE INDEX IF NOT EXISTS idx_deals_vendor ON deals(vendor_id)",
    "CREATE INDEX IF NOT EXISTS idx_deals_region ON deals(region)",
    "CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status)",
    "CREATE INDEX IF NOT EXISTS idx_wo_region ON work_orders(region)",
    "CREATE INDEX IF NOT EXISTS idx_sd_region ON sales_deal(region)",
    "CREATE INDEX IF NOT EXISTS idx_sd_buyer ON sales_deal(buyer)",
    "CREATE INDEX IF NOT EXISTS idx_title_movie ON title(movie_id)",
    "CREATE INDEX IF NOT EXISTS idx_movie_cat ON movie(content_category)",
    "CREATE INDEX IF NOT EXISTS idx_movie_genre ON movie(genre)",
    "CREATE INDEX IF NOT EXISTS idx_alerts_region ON alerts(region)",
    "CREATE INDEX IF NOT EXISTS idx_ql_session ON query_log(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_ql_timestamp ON query_log(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_ql_success ON query_log(success)",
    "CREATE INDEX IF NOT EXISTS idx_fb_log ON feedback(log_id)",
]

# ─── Seed helpers ─────────────────────────────────────────────────────────────
def _seed_hierarchy(cur):
    today = datetime.now()
    base  = datetime(2015,1,1)
    series_rows, season_rows, title_rows = [], [], []
    tid_ctr = [1]
    for s_id, s_title, s_src, entity in SERIES_LIST:
        genre = random.choice(GENRES)
        series_rows.append((s_id, s_title, s_src, entity, genre,
            f"Premium original series: {s_title}.",
            random.randint(2015,2024), None, today.strftime("%Y-%m-%d %H:%M:%S")))

        for sn in range(1, random.randint(2,6)):
            sea_id = f"{s_id}-SN{sn:02d}"
            n_eps  = random.randint(6,12)
            season_rows.append((sea_id, s_id, f"{s_title} Season {sn}", sn,
                s_src, entity, n_eps, random.randint(2015,2025),
                today.strftime("%Y-%m-%d %H:%M:%S")))

            for ep in range(1, n_eps+1):
                t_id   = f"T{tid_ctr[0]:05d}"
                tid_ctr[0] += 1
                region = random.choice(REGIONS)
                title_rows.append((
                    t_id, sea_id, s_id, None, s_src, f"EIDR-{t_id}",
                    ep,  "Episode", f"{s_title} S{sn:02d}E{ep:02d}",
                    entity, random.randint(2015,2025),
                    f"Episode {ep} of {s_title} Season {sn}.",
                    genre, None, random.randint(25,65),
                    random.choice(["TV-MA", "TV-14", "TV-PG", "TV-G"]),
                    "Series", None, 0,
                    region, today.strftime("%Y-%m-%d %H:%M:%S"),
                ))

        for _ in range(random.randint(1,3)):
            t_id = f"T{tid_ctr[0]:05d}"
            tid_ctr[0] += 1
            region = random.choice(REGIONS)
            title_rows.append((
                t_id, None, s_id, None, s_src, f"EIDR-{t_id}",
                None,  "Special", f"{s_title} — Special",
                entity, random.randint(2015,2025),
                f"Standalone special for {s_title}.",
                genre, None, random.randint(60,120),
                random.choice(["TV-MA", "TV-14", "TV-PG"]),
                "Series", None, 0,
                region, today.strftime("%Y-%m-%d %H:%M:%S"),
            ))

    cur.executemany("INSERT INTO series VALUES (?,?,?,?,?,?,?,?,?)", series_rows)
    cur.executemany("INSERT INTO season VALUES (?,?,?,?,?,?,?,?,?)", season_rows)
    cur.executemany("""INSERT INTO title (title_id,season_id,series_id,movie_id,title_source,
        title_identifier,episode_number,title_type,title_name,controlling_entity,
        release_year,description,genre,franchise,runtime_minutes,age_rating,
        content_category,theatrical_release_date,box_office_gross_usd_m,region,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?)""", title_rows)

    logger.info(f"Hierarchy: {len(series_rows)} series, {len(season_rows)} seasons, {len(title_rows)} titles")
    return [r[0] for r in title_rows]

VENDOR_EMAILS = {
    1: "operations@pixelperfect.com", 2: "sales@globaldub.com",
    3: "info@streamops.sg",           4: "deals@visionpost.br",
    5: "studio@cinecolor.com",        6: "studio@audiomasters.de",
    7: "support@captionworks.in",     8: "info@vfxmasters.mx",
    9: "security@contentguard.com",  10: "connect@mediabridge.au",
}
VENDOR_PHONES = {
    1: "+1-415-555-0101", 2: "+44-20-5550-0202", 3: "+65-6555-0303",
    4: "+55-11-5550-0404",5: "+1-310-555-0505",  6: "+49-30-5550-0606",
    7: "+91-80-5550-0707", 8: "+52-55-5550-0808",9: "+1-212-555-0909",
    10: "+61-2-5550-1010",
}

def _seed_vendors(cur):
    today = datetime.now()
    rows = []
    for v in VENDORS:
        rows.append((
            v[0], v[1], v[2], v[3], v[4],
            random.choice(["Gold", "Silver", "Platinum", "Certified"]),
            1, today.strftime("%Y-%m-%d"),
            VENDOR_EMAILS.get(v[0], f"contact@vendor{v[0]}.com"),
            VENDOR_PHONES.get(v[0], "+1-000-000-0000"),
            f"{random.randint(100,999)} {random.choice(['Main St','Park Ave','Tech Blvd'])}, {v[4]}",
            random.choice(["Net 30", "Net 60", "Net 45", "Immediate"]),
            round(random.uniform(50000, 2000000), 2),
        ))
    cur.executemany("""INSERT INTO vendors (vendor_id,vendor_name,rating,vendor_type,region,
        certification_level,active,created_date,contact_email,phone,address,
        payment_terms,total_spend) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

def _seed_deals(cur):
    today  = datetime.now()
    start  = datetime(2021, 1, 1)
    far    = datetime(2027, 12, 31)
    deal_types  = ["Output Deal", "Library Buy", "First-Look Deal", "Co-Production",
                   "Licensing Agreement", "Distribution Deal", "Volume Deal", "Format Deal"]
    rights_scope= ["Linear + SVOD", "SVOD Only", "PayTV Only", "All Rights", "Digital Only",
                   "Linear Only", "SVOD + EST", "Home Entertainment"]
    statuses    = ["Active", "Active", "Active", "Expired", "Pending", "Under Negotiation", "Terminated"]
    pay_statuses= ["Paid", "Pending", "Invoiced", "Overdue", "Partially Paid"]
    rows = []
    for i in range(1, 501):
        vendor   = random.choice(VENDORS)
        region   = random.choice(REGIONS)
        d_date   = _rnd_date(start, today)
        exp_date = _rnd_date(today - timedelta(days=365), far)
        status   = "Expired" if exp_date < today.strftime("%Y-%m-%d") else random.choice(statuses)
        rows.append((
            i,
            f"{random.choice(['Premium','Global','Strategic','Master','Regional'])} "
            f"{random.choice(['Output','Content','Distribution','License'])} Deal {i:03d}",
            vendor[0], vendor[1],
            random.choice(deal_types),
            round(random.uniform(50000, 10000000), 2),
            d_date, exp_date,
            random.choice(rights_scope),
            _rnd_terr(region, 2),
            region, status,
            random.choice(pay_statuses),
            "Standard deal terms and conditions apply.",
            today.strftime("%Y-%m-%d %H:%M:%S"),
        ))
    cur.executemany("""INSERT INTO deals (deal_id,deal_name,vendor_id,vendor_name,deal_type,
        deal_value,deal_date,expiry_date,rights_scope,territory,region,status,
        payment_status,notes,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    logger.info(f"Deals: {len(rows)} original deals seeded")

def _seed_work_orders(cur, title_ids):
    today = datetime.now()
    rows  = []
    cur.execute("SELECT title_id, title_name FROM title")
    title_name_map = {row[0]: row[1] for row in cur.fetchall()}
    for _ in range(800):
        t_id   = random.choice(title_ids)
        vendor = random.choice(VENDORS)
        region = random.choice(REGIONS)
        rows.append((t_id, title_name_map.get(t_id, t_id), vendor[0], vendor[1],
            random.choice(["Encoding", "QC", "Localization", "VFX", "Audio", "Mastering", "Subtitling"]),
            random.choice(["Not Started", "In Progress", "Review", "Completed", "Delayed", "On Hold"]),
            random.choice(["Critical", "High", "Medium", "Low"]),
            region, random.choice(TERRITORIES_BY_REGION[region]),
            _rnd_date(today, today+timedelta(days=180)),
            round(random.uniform(70,100),1), random.randint(0,5),
            round(random.uniform(5000,50000),2),
            random.choice(["Paid", "Pending", "Invoiced", "Overdue"]),
            today.strftime("%Y-%m-%d %H:%M:%S"),
        ))
    cur.executemany("""INSERT INTO work_orders (title_id,title_name,vendor_id,vendor_name,
        work_type,status,priority,region,territory,due_date,quality_score,rework_count,
        cost,billing_status,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

def _seed_content_deals(cur, title_ids):
    today = datetime.now()
    start = datetime(2020,1,1)
    far   = datetime(2028,12,31)
    d_rows, mr_rows, er_rows = [], [], []
    dctr = [1]
    buyers = ["Netflix","Amazon","Disney+","Apple TV+","Peacock","Paramount+","Hulu"]
    cur.execute("SELECT title_id, title_name FROM title")
    title_name_map = {row[0]: row[1] for row in cur.fetchall()}

    for t_id in title_ids:
        if random.random() > 0.65:
            continue
        real_title_name = title_name_map.get(t_id, t_id)
        for _ in range(random.randint(1,3)):
            d_id   = f"CD{dctr[0]:05d}"
            dctr[0] += 1
            src    = random.choice(["TRL", "C2", "FRL"])
            region = random.choice(REGIONS)
            d_rows.append((d_id, src, random.choice(DEAL_TYPES),
                f"Deal {d_id}",
                random.choice(["Warner Bros Discovery", "HBO", "Cinemax"]),
                random.choice(buyers) if random.random() >0.5 else None,
                region, "Standard output deal terms apply.",
                today.strftime("%Y-%m-%d %H:%M:%S")))

            for _ in range(random.randint(1,3)):
                r_id  = f"MR{len(mr_rows)+1:06d}"
                tf    = _rnd_date(start, datetime(2023,6,1))
                roll  = random.random()
                if   roll < 0.15: tt = _rnd_date(today, today+timedelta(days=89));  status="Active"
                elif roll < 0.35: tt = _rnd_date(datetime(2022,1,1), today-timedelta(days=1)); status="Expired"
                elif roll < 0.42: tt = _rnd_date(today+timedelta(days=92), far);  status="Pending"
                else:             tt = _rnd_date(today+timedelta(days=90), far);  status="Active"
                days_rem = (datetime.strptime(tt, "%Y-%m-%d") - today).days

                mr_rows.append((r_id, d_id, t_id, real_title_name,
                    random.choice(RIGHTS_TYPES), tf, tt, None, None,
                    _rnd_terr(region), region, _rnd_media_p(), _rnd_media_a(),
                    _rnd_lang(), _rnd_brand(),
                    random.choice([1,1,0]), random.choice([0,0,1]),
                    random.randint(0,180) if random.random() >0.5 else 0,
                    "Standard terms per contract.",
                    "No sublicensing without written consent." if random.random() >0.5 else None,
                    "End user streaming only." if random.random() >0.5 else None,
                    status, days_rem, today.strftime("%Y-%m-%d %H:%M:%S")))

                if random.random() < 0.40:
                    e_id = f"ER{len(er_rows)+1:06d}"
                    er_rows.append((e_id, r_id, t_id, d_id, tf, tt,
                        random.randint(2,20) if random.random() >0.5 else None,
                        random.randint(1,5)  if random.random() >0.5 else None,
                        random.randint(7,90) if random.random() >0.5 else None,
                        random.randint(1,10) if random.random() >0.5 else None,
                        "Exhibition restricted per contract schedule.",
                        today.strftime("%Y-%m-%d %H:%M:%S")))

    cur.executemany("INSERT INTO content_deal VALUES (?,?,?,?,?,?,?,?,?)", d_rows)
    cur.executemany("""INSERT INTO media_rights (rights_id,deal_id,title_id,title_name,
        rights_type,term_from,term_to,estimated_term_from,estimated_term_to,
        territories,region,media_platform_primary,media_platform_ancillary,
        language,brand,exclusivity,holdback,holdback_days,
        notes_general,notes_restrictive,notes_end_user_rights,
        status,days_remaining,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", mr_rows)
    cur.executemany("""INSERT INTO exhibition_restrictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", er_rows)
    logger.info(f"Content deals: {len(d_rows)} deals, {len(mr_rows)} media rights, {len(er_rows)} exhibition restrictions")

def _seed_elemental(cur, title_ids):
    today = datetime.now()
    far   = datetime(2028,12,31)
    ed_rows, er_rows = [], []
    ctr = [1]
    cur.execute("SELECT title_id, title_name FROM title")
    title_name_map = {row[0]: row[1] for row in cur.fetchall()}

    for t_id in random.sample(title_ids, min(len(title_ids)//3, 500)):
        ed_id  = f"ED{ctr[0]:05d}"; ctr[0] += 1
        region = random.choice(REGIONS)
        ed_rows.append((ed_id, random.choice(["C2", "FRL"]), random.choice(DEAL_TYPES),
            ed_id, f"Elemental Deal {ed_id}", "HBO Max", "", region,
            today.strftime("%Y-%m-%d %H:%M:%S")))
        tf = _rnd_date(datetime(2020,1,1), datetime(2023,1,1))
        tt = _rnd_date(datetime(2024,1,1), far)
        status = "Active" if datetime.strptime(tt, "%Y-%m-%d") > today else "Expired"
        er_id  = f"ELRG{len(er_rows)+1:06d}"
        er_rows.append((er_id, ed_id, t_id, title_name_map.get(t_id, t_id), tf, tt,
            _rnd_terr(region), region, _rnd_media_p(), _rnd_media_a(),
            _rnd_lang(2), _rnd_brand(2), status,
            today.strftime("%Y-%m-%d %H:%M:%S")))
    cur.executemany("INSERT INTO elemental_deal VALUES (?,?,?,?,?,?,?,?,?)", ed_rows)
    cur.executemany("""INSERT INTO elemental_rights (elemental_rights_id,elemental_deal_id,
        title_id,title_name,term_from,term_to,territories,region,
        media_platform_primary,media_platform_ancillary,language,brand,status,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", er_rows)
    logger.info(f"Elemental: {len(ed_rows)} deals, {len(er_rows)} rights")

def _seed_dna(cur, title_ids):
    today = datetime.now()
    rows  = []
    cur.execute("SELECT title_id, title_name FROM title")
    title_name_map = {row[0]: row[1] for row in cur.fetchall()}

    for t_id in random.sample(title_ids, min(len(title_ids)//5, 300)):
        cat  = random.choice(DNA_CATEGORIES)
        sub   = random.choice(DNA_SUBCATS[cat])
        region = random.choice(REGIONS)
        rows.append((f"DNA{len(rows)+1:05d}", t_id, title_name_map.get(t_id, t_id),
            region, _rnd_terr(region,2), _rnd_media_p(),
            cat, sub,
            today.strftime("%Y-%m-%d"),
            (today+timedelta(days=random.randint(30,730))).strftime("%Y-%m-%d"),
            f"Do-not-air: {sub} restriction.", 1,
            today.strftime("%Y-%m-%d %H:%M:%S")))
    cur.executemany("""INSERT INTO do_not_air (dna_id,title_id,title_name,region,territory,
        media_type,reason_category,reason_subcategory,term_from,term_to,
        additional_notes,active,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    logger.info(f"DNA: {len(rows)} records")

def _seed_sales(cur, title_ids):
    today  = datetime.now()
    buyers = ["Netflix", "Amazon", "Disney+", "Apple TV+", "Peacock",
              "Paramount+", "Hulu", "BritBox", "Stan", "Hotstar"]
    cur.execute("SELECT title_id, title_name FROM title")
    title_name_map = {row[0]: row[1] for row in cur.fetchall()}
    rows = []
    for t_id in random.sample(title_ids, min(len(title_ids)//2, 700)):
        region = random.choice(REGIONS)
        tf = _rnd_date(datetime(2021,1,1), today)
        tt = _rnd_date(today, datetime(2027,12,31))
        status = "Active" if datetime.strptime(tt, "%Y-%m-%d") > today else "Expired"
        real_name = title_name_map.get(t_id, t_id)
        rows.append((f"SD{len(rows)+1:05d}",
            random.choice(["Affiliate Sales", "3rd Party Sales"]),
            f"Sales Deal — {real_name}",
            t_id, real_name, random.choice(buyers),
            _rnd_terr(region,2), region, _rnd_media_p(),
            tf, tt, round(random.uniform(50000,5000000),2),
            random.choice(["USD", "EUR", "GBP"]), status,
            today.strftime("%Y-%m-%d %H:%M:%S")))
    cur.executemany("""INSERT INTO sales_deal (sales_deal_id,deal_type,deal_name,
        title_id,title_name,buyer,territory,region,media_platform,
        term_from,term_to,deal_value,currency,status,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    logger.info(f"Sales deals: {len(rows)}")

def _seed_movies(cur):
    """Seed the movie table and corresponding title records for WBD film slate."""
    today = datetime.now()
    movie_rows, title_rows = [], []
    tid_ctr = [5000]
    for (m_id, m_title, m_src, entity, genre, franchise,
         category, bo_m, rating, year) in MOVIES_LIST:
        release_date = f"{year}-{random.randint(1,12):02d}-{random.choice([7,14,21,28]):02d}"
        movie_rows.append((
            m_id, m_title, m_src, entity, genre,
            f"Warner Bros / HBO Max feature film: {m_title}.",
            franchise, category, release_date,
            float(bo_m), rating, year,
            today.strftime("%Y-%m-%d %H:%M:%S"),
        ))
        versions = ["Theatrical Cut"]
        if random.random() > 0.5: versions.append("Director's Cut")
        if category == "Theatrical" and random.random() > 0.6: versions.append("IMAX Version")
        if random.random() > 0.7: versions.append("4K Remaster")

        for version in versions:
            t_id   = f"T{tid_ctr[0]:05d}"; tid_ctr[0] += 1
            region = random.choice(REGIONS)
            runtime = random.randint(95, 175)
            title_rows.append((
                t_id, None, None, m_id, m_src, f"EIDR-{t_id}",
                None, "Movie",
                f"{m_title} ({version})" if version != "Theatrical Cut" else m_title,
                entity, year,
                f"{m_title} — {version}. {category} release.",
                genre, franchise, runtime, rating,
                category, release_date, float(bo_m),
                region, today.strftime("%Y-%m-%d %H:%M:%S"),
            ))

    cur.executemany("""INSERT INTO movie (movie_id,movie_title,movie_source,
        controlling_entity,genre,description,franchise,content_category,
        theatrical_release_date,box_office_gross_usd_m,age_rating,
        release_year,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", movie_rows)
    cur.executemany("""INSERT INTO title (title_id,season_id,series_id,movie_id,title_source,
        title_identifier,episode_number,title_type,title_name,controlling_entity,
        release_year,description,genre,franchise,runtime_minutes,age_rating,
        content_category,theatrical_release_date,box_office_gross_usd_m,region,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?)""", title_rows)
    logger.info(f"Movies: {len(movie_rows)} films, {len(title_rows)} title records seeded")
    return [r[0] for r in title_rows]

# ─── Public API ───────────────────────────────────────────────────────────────
def init_database(db_path="foundry.db"):
    db   = DatabaseManager(db_path)
    conn = db.get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("BEGIN TRANSACTION")
        for tbl in ["exhibition_restrictions", "media_rights", "content_deal",
                    "elemental_rights", "elemental_deal", "do_not_air",
                    "sales_deal", "work_orders", "deals", "vendors", "title", "season", "series", "movie"]:
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        
        for stmt in SCHEMA_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)

        title_ids       = _seed_hierarchy(cur)
        movie_title_ids = _seed_movies(cur)
        all_title_ids   = title_ids + movie_title_ids
        _seed_vendors(cur)
        _seed_deals(cur)
        _seed_work_orders(cur, all_title_ids)
        _seed_content_deals(cur, all_title_ids)
        _seed_elemental(cur, all_title_ids)
        _seed_dna(cur, all_title_ids)
        _seed_sales(cur, all_title_ids)

        cur.execute("COMMIT")
        for idx in INDEXES_SQL:
            cur.execute(idx)
        cur.execute("ANALYZE")
        logger.info("Database initialised — Rights Explorer MVP schema ready")
    except Exception as e:
        cur.execute("ROLLBACK")
        logger.error(f"DB init failed: {e}")
        raise
    return conn

def _seed_safe(cur, func, table_name, *args):
    """Wrapper to catch exactly which table fails during seeding."""
    try:
        func(cur, *args)
    except Exception as e:
        logger.error(f"Failed to seed {table_name}: {e}")
        raise

def save_alert(conn, alert_type, label, title_name=None, rights_id=None,
               region="NA", platform=None, expiry_date=None, days_threshold=90,
               persona="Business Affairs", notes=None):
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO alerts (alert_type,label,title_name,rights_id,region,
        platform,expiry_date,days_threshold,persona,notes)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (alert_type, label, title_name, rights_id, region,
              platform, expiry_date, days_threshold, persona, notes))
        conn.commit()
        return cur.lastrowid, None
    except Exception as e:
        return None, str(e)

def dismiss_alert(conn, alert_id):
    try:
        conn.cursor().execute("UPDATE alerts SET dismissed=1 WHERE alert_id=?", (alert_id,))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)

def get_alerts(conn, region=None, include_dismissed=False):
    try:
        where = "WHERE dismissed=0" if not include_dismissed else "WHERE 1=1"
        if region:
            where += f" AND (region='{region}' OR region IS NULL)"
        df = pd.read_sql_query(f"""
        SELECT alert_id, alert_type, label, title_name, platform,
        expiry_date, days_threshold, region, persona, notes,
        created_at, dismissed
        FROM alerts {where}
        ORDER BY created_at DESC
        """, conn)
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

_ALLOWED_STMTS = ("select", "with", "explain")

def _is_readonly(sql: str) -> bool:
    first = sql.strip().lstrip("(").lower().split()[0] if sql.strip() else ""
    return first in _ALLOWED_STMTS

def execute_sql(sql: str, conn) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    if not sql or not sql.strip():
        return None, "Empty query"
    if not _is_readonly(sql):
        first_word = sql.strip().split()[0].upper()
        return None, f"Only SELECT queries are permitted. Got: {first_word}"
    try:
        df = pd.read_sql_query(sql.strip().rstrip(";"), conn)
        return df, None
    except Exception as e:
        return None, str(e)

def get_table_stats(conn) -> dict:
    stats = {}
    for t in ["series", "season", "title", "movie", "content_deal", "media_rights",
              "elemental_rights", "do_not_air", "sales_deal", "deals", "work_orders", "alerts"]:
        try:
            df = pd.read_sql_query(f"SELECT COUNT(*) AS c FROM {t}", conn)
            stats[t] = int(df.iloc[0]["c"])
        except:
            stats[t] = 0
    return stats

def log_query(conn, session_id: str, user_id: Optional[str], region: str,
              persona: str, question: str, generated_sql: str, intent_domain: str,
              cross_intent: bool, latency_ms: float, success: bool,
              error_message: str, rows_returned: int, chart_type: str) -> Optional[int]:
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO query_log
        (session_id, user_id, region, persona, question, generated_sql,
        intent_domain, cross_intent, latency_ms, success,
        error_message, rows_returned, chart_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (session_id, user_id, region, persona,
         question, generated_sql, intent_domain,
         1 if cross_intent else 0,
         latency_ms,
         1 if success else 0,
         error_message or "",
         rows_returned,
         chart_type or "",
        ))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        logger.error(f"log_query failed: {e}")
        return None

def log_feedback(conn, log_id: int, feedback_type: str, comment: Optional[str] = None) -> bool:
    try:
        conn.cursor().execute(
            "INSERT INTO feedback (log_id, feedback_type, comment) VALUES (?,?,?)",
            (log_id, feedback_type, comment),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"log_feedback failed: {e}")
        return False

def update_session(conn, session_id: str, user_id: Optional[str] = None,
                   region: Optional[str] = None, persona: Optional[str] = None) -> bool:
    try:
        conn.cursor().execute("""
        INSERT INTO user_session (session_id, user_id, region, persona)
        VALUES (?,?,?,?)
        ON CONFLICT(session_id) DO UPDATE SET
        last_active = DATETIME('now'),
        user_id  = COALESCE(excluded.user_id,  user_session.user_id),
        region   = COALESCE(excluded.region,   user_session.region),
        persona  = COALESCE(excluded.persona,  user_session.persona)
        """, (session_id, user_id, region, persona))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"update_session failed: {e}")
        return False

class DatabaseManager:
    def __init__(self, db_path="foundry.db"):
        self.db_path = db_path
        self._conn   = None

    def get_connection(self):
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path, check_same_thread=False,
                timeout=30, isolation_level=None)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn
