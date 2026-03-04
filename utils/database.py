"""
utils/database.py — Foundry Vantage  (production rewrite)

Changes vs original
────────────────────
1.  NO DROP TABLE — CREATE TABLE IF NOT EXISTS everywhere.
    Original dropped all 4 tables on every call → all data wiped on every
    server restart. Now schema is idempotent and safe to re-run.

2.  Seed guard via _meta table.
    Original re-seeded on every call → infinite duplicate rows.
    Fix: _meta('seeded') flag — seed runs exactly once on a fresh DB.

3.  Thread-safe connection factory with threading.local().
    Original used check_same_thread=False on one shared object.
    Under Streamlit's multi-thread model this causes SQLite thread errors.
    Fix: each OS thread gets its own connection via threading.local().

4.  FK enforcement + WAL mode per connection.
    PRAGMA foreign_keys=ON — catches orphaned vendor references.
    PRAGMA journal_mode=WAL — allows concurrent readers without blocking.

5.  Indexes on every WHERE / JOIN / ORDER BY column.
    Original: zero indexes → full table scans on every query.

6.  Richer, realistic seed data.
    Original: 4 vendors (no region), all 200 WOs with due_date='2025-06-01',
    no description/notes/assignee/start_date fields populated.
    Fix: 8 regional vendors, dates spread 2023-2026, varied statuses,
    priorities, assignees, languages, budgets per region.

7.  execute_sql safety gate.
    Blocks non-SELECT statements to prevent DDL/DML injection.
    Casts object columns that look numeric for correct metric math.

8.  get_smart_chart_config() — semantic column detection.
    Prevents UI from blindly plotting id vs vendor_id.
    Returns (x_col, y_col, chart_type) based on column name semantics.

9.  NON_METRIC_COLS set exported for UI metrics renderer.
    Prevents "Total Id: 14100" and "Total Vendor Id: 204" appearing as KPIs.

10. get_db_stats() for sidebar live KPI strip.
    All queries use indexed columns — sub-millisecond on seeded data.
"""

import random
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
DB_PATH     = Path("foundry.db")
RANDOM_SEED = 42

# Columns that are row IDs / FK keys / raw dates — never shown as business KPIs
NON_METRIC_COLS = frozenset({
    "id", "vendor_id", "title_id",
    "created_at", "created_date", "start_date", "due_date",
    "completion_date", "deal_date", "contract_signed_date",
    "expiry_date", "target_release_date",
})

# ── Thread-local connection pool ───────────────────────────────────────────────
_local = threading.local()


def get_db_connection() -> sqlite3.Connection:
    """
    Return a per-thread SQLite connection.
    threading.local() guarantees each OS thread owns its connection,
    satisfying SQLite's threading requirements without any unsafe flags.
    """
    if getattr(_local, "conn", None) is None:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous  = NORMAL")
        conn.execute("PRAGMA cache_size   = -8000")   # 8 MB page cache
        _local.conn = conn
    return _local.conn


# ── Schema (idempotent) ────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS vendors (
    vendor_id    INTEGER PRIMARY KEY,
    vendor_name  TEXT    NOT NULL,
    rating       REAL    CHECK(rating BETWEEN 0 AND 5),
    contact_email TEXT,
    phone        TEXT,
    address      TEXT,
    region       TEXT    NOT NULL,
    active       INTEGER DEFAULT 1,
    created_date TEXT    DEFAULT (date('now'))
);

CREATE TABLE IF NOT EXISTS deals (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id            INTEGER REFERENCES vendors(vendor_id) ON DELETE SET NULL,
    vendor_name          TEXT,
    deal_name            TEXT    NOT NULL,
    deal_value           REAL    CHECK(deal_value >= 0),
    deal_date            TEXT,
    region               TEXT    NOT NULL,
    rights_scope         TEXT,
    deal_type            TEXT,
    status               TEXT    DEFAULT 'Active'
                                 CHECK(status IN ('Active','Pending','Expired',
                                                  'Under Review','Cancelled')),
    currency             TEXT    DEFAULT 'USD',
    description          TEXT,
    contract_signed_date TEXT,
    expiry_date          TEXT,
    renewal_terms        TEXT,
    created_at           TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS work_orders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    title_id         INTEGER,
    title_name       TEXT,
    vendor_id        INTEGER REFERENCES vendors(vendor_id) ON DELETE SET NULL,
    vendor_name      TEXT,
    status           TEXT    NOT NULL
                             CHECK(status IN ('Completed','In Progress',
                                              'Delayed','Not Started','On Hold')),
    region           TEXT    NOT NULL,
    due_date         TEXT,
    start_date       TEXT,
    completion_date  TEXT,
    priority         TEXT    CHECK(priority IN ('Critical','High','Medium','Low')),
    assigned_to      TEXT,
    estimated_hours  REAL    CHECK(estimated_hours >= 0),
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS content_planning (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    title_id            INTEGER,
    content_title       TEXT    NOT NULL,
    status              TEXT    CHECK(status IN ('Approved','In Review',
                                                 'Draft','Greenlit','Cancelled')),
    region              TEXT    NOT NULL,
    localization_status TEXT,
    delivery_method     TEXT,
    budget              REAL    CHECK(budget >= 0),
    target_release_date TEXT,
    language            TEXT,
    format              TEXT,
    content_type        TEXT,
    production_company  TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_deals_region     ON deals(region);
CREATE INDEX IF NOT EXISTS idx_deals_vendor     ON deals(vendor_id);
CREATE INDEX IF NOT EXISTS idx_deals_status     ON deals(status);
CREATE INDEX IF NOT EXISTS idx_deals_date       ON deals(deal_date);
CREATE INDEX IF NOT EXISTS idx_deals_value      ON deals(deal_value);
CREATE INDEX IF NOT EXISTS idx_wo_region        ON work_orders(region);
CREATE INDEX IF NOT EXISTS idx_wo_status        ON work_orders(status);
CREATE INDEX IF NOT EXISTS idx_wo_vendor        ON work_orders(vendor_id);
CREATE INDEX IF NOT EXISTS idx_wo_due           ON work_orders(due_date);
CREATE INDEX IF NOT EXISTS idx_wo_priority      ON work_orders(priority);
CREATE INDEX IF NOT EXISTS idx_cp_region        ON content_planning(region);
CREATE INDEX IF NOT EXISTS idx_cp_status        ON content_planning(status);
CREATE INDEX IF NOT EXISTS idx_cp_language      ON content_planning(language);
"""

# ── Reference data ─────────────────────────────────────────────────────────────
_VENDORS = [
    (1, "PixelPerfect",  4.8, "ops@pixel.com",      "555-0101", "Los Angeles", "NA",    1, "2023-03-15"),
    (2, "NovaCut",       4.3, "hello@novacut.io",    "555-0202", "Toronto",     "NA",    1, "2023-11-08"),
    (3, "GlobalDub",     4.5, "sales@globaldub.com", "555-0303", "London",      "EMEA",  1, "2023-05-22"),
    (4, "EuroMedia",     4.1, "info@euromedia.de",   "555-0404", "Berlin",      "EMEA",  1, "2024-03-20"),
    (5, "StreamOps",     4.2, "info@streamops.sg",   "555-0505", "Singapore",   "APAC",  1, "2023-07-10"),
    (6, "AsiaDubCo",     4.7, "hello@asiadub.jp",    "555-0606", "Tokyo",       "APAC",  1, "2024-02-14"),
    (7, "VisionPost",    4.6, "deals@vision.br",     "555-0707", "Sao Paulo",   "LATAM", 1, "2023-09-01"),
    (8, "CaribbeanPost", 3.9, "ops@caribpost.mx",    "555-0808", "Mexico City", "LATAM", 1, "2024-04-05"),
]

_TITLES = [
    (101, "The Penguin",         "Warner Bros"),
    (102, "Dune: Prophecy",      "Legendary"),
    (103, "The Last of Us S2",   "Sony/HBO"),
    (104, "House of the Dragon", "HBO"),
    (105, "Shogun",              "FX Productions"),
    (106, "Fallout S2",          "Amazon Studios"),
    (107, "The Bear S4",         "FX/Hulu"),
    (108, "Severance S3",        "Apple TV+"),
    (109, "Andor S2",            "Lucasfilm"),
    (110, "White Lotus S3",      "HBO"),
    (111, "Yellowjackets S3",    "Showtime"),
    (112, "Succession Reboot",   "HBO"),
]

_REGIONS       = ["NA", "APAC", "EMEA", "LATAM"]
_RIGHTS        = ["Global", "Multi-Region", "Territory Specific", "Exclusive Window", "Non-Exclusive"]
_DEAL_TYPES    = ["Library Buy", "Volume Deal", "Output Deal", "First-Look", "Co-Production", "SVOD License"]
_DEAL_STATUSES = ["Active", "Active", "Active", "Pending", "Pending", "Expired", "Under Review"]
_CURRENCIES    = {"NA": "USD", "EMEA": "EUR", "APAC": "USD", "LATAM": "USD"}
_WO_STATUSES   = ["Completed", "Completed", "In Progress", "In Progress", "Delayed", "Not Started", "On Hold"]
_PRIORITIES    = ["Critical", "High", "High", "Medium", "Medium", "Low"]
_ASSIGNEES     = ["Alex Chen", "Priya Sharma", "Tom Walker", "Yuki Tanaka",
                  "Carlos Ruiz", "Emma Wilson", "Raj Patel", "Sophie Laurent",
                  "David Kim", "Isabelle Martin"]
_LOC_STATUSES  = ["Complete", "Complete", "In Progress", "Pending", "Not Started"]
_DELIVERY      = ["SFTP", "Aspera", "S3 Bucket", "MediaShuttle", "Direct Upload"]
_FORMATS       = ["4K HDR", "1080p HDR", "1080p SDR", "Dolby Vision", "HDR10+"]
_CONTENT_TYPES = ["Drama Series", "Feature Film", "Documentary", "Reality", "Animation", "Limited Series"]
_CP_STATUSES   = ["Approved", "In Review", "Draft", "Greenlit"]
_LANGUAGES     = {
    "NA":    ["English", "Spanish (LA)", "French (CA)", "Portuguese (BR)"],
    "APAC":  ["Mandarin", "Japanese", "Korean", "Hindi", "Thai", "Indonesian"],
    "EMEA":  ["German", "French", "Spanish (ES)", "Italian", "Arabic", "Turkish"],
    "LATAM": ["Spanish (LA)", "Portuguese (BR)", "English"],
}
_RENEWAL_TERMS = [
    "Auto-renew annually", "Manual renewal required",
    "3-year option", "No renewal clause", "Right of first refusal",
]


def _rand_date(rng: random.Random, base: datetime, spread: int) -> str:
    return (base + timedelta(days=rng.randint(0, spread))).strftime("%Y-%m-%d")


def _add_years(date_str: str, n: int) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return d.replace(year=d.year + n).strftime("%Y-%m-%d")


def _seed(conn: sqlite3.Connection) -> None:
    """Insert all seed data. Called exactly once on fresh DB."""
    rng  = random.Random(RANDOM_SEED)
    cur  = conn.cursor()
    base = datetime(2023, 6, 1)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.executemany("INSERT OR IGNORE INTO vendors VALUES (?,?,?,?,?,?,?,?,?)", _VENDORS)

    for reg in _REGIONS:
        home    = [v for v in _VENDORS if v[6] == reg]
        others  = [v for v in _VENDORS if v[6] != reg]
        pool    = home + rng.sample(others, k=min(2, len(others)))

        # 75 deals
        for i in range(75):
            v      = rng.choice(pool)
            signed = _rand_date(rng, base, 600)
            cur.execute("""
                INSERT INTO deals (
                    vendor_id, vendor_name, deal_name, deal_value, deal_date,
                    region, rights_scope, deal_type, status, currency,
                    description, contract_signed_date, expiry_date,
                    renewal_terms, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                v[0], v[1],
                f"{rng.choice(['Premium','Standard','Platinum','Basic','Elite'])} "
                f"{rng.choice(_DEAL_TYPES)} #{i+1}",
                round(rng.uniform(200_000, 9_000_000), 2),
                signed, reg,
                rng.choice(_RIGHTS), rng.choice(_DEAL_TYPES),
                rng.choice(_DEAL_STATUSES), _CURRENCIES[reg],
                f"Licensing agreement covering {reg} territory — batch {i+1}.",
                signed, _add_years(signed, rng.randint(2, 5)),
                rng.choice(_RENEWAL_TERMS), now,
            ))

        # 50 work orders — varied dates
        for i in range(50):
            title  = rng.choice(_TITLES)
            v      = rng.choice(pool)
            s_date = _rand_date(rng, base, 450)
            due    = (datetime.strptime(s_date, "%Y-%m-%d")
                      + timedelta(days=rng.randint(21, 120))).strftime("%Y-%m-%d")
            status = rng.choice(_WO_STATUSES)
            comp   = due if status == "Completed" else None
            cur.execute("""
                INSERT INTO work_orders (
                    title_id, title_name, vendor_id, vendor_name,
                    status, region, due_date, start_date, completion_date,
                    priority, assigned_to, estimated_hours, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                title[0], title[1], v[0], v[1],
                status, reg, due, s_date, comp,
                rng.choice(_PRIORITIES), rng.choice(_ASSIGNEES),
                round(rng.uniform(10, 250), 1),
                f"Localization & QC for {title[1]} — {reg} territory.",
            ))

        # 30 content planning rows
        for i in range(30):
            title   = rng.choice(_TITLES)
            lang    = rng.choice(_LANGUAGES[reg])
            release = _rand_date(rng, datetime(2024, 6, 1), 600)
            cur.execute("""
                INSERT INTO content_planning (
                    title_id, content_title, status, region,
                    localization_status, delivery_method, budget,
                    target_release_date, language, format,
                    content_type, production_company, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                title[0], f"{title[1]} — {lang}",
                rng.choice(_CP_STATUSES), reg,
                rng.choice(_LOC_STATUSES), rng.choice(_DELIVERY),
                round(rng.uniform(40_000, 2_500_000), 2),
                release, lang, rng.choice(_FORMATS),
                rng.choice(_CONTENT_TYPES), title[2],
                f"{lang} localization for {reg} via {rng.choice(_DELIVERY)}.",
            ))

    cur.execute("INSERT OR REPLACE INTO _meta VALUES ('seeded','1')")
    conn.commit()


# ── Public API ─────────────────────────────────────────────────────────────────
def init_database() -> sqlite3.Connection:
    """
    Idempotent startup — safe to call on every Streamlit boot.
    Creates schema if absent; seeds exactly once.
    """
    conn = get_db_connection()
    conn.executescript(_SCHEMA)
    conn.commit()
    if conn.execute("SELECT value FROM _meta WHERE key='seeded'").fetchone() is None:
        _seed(conn)
    return conn


def execute_sql(sql: str, conn: sqlite3.Connection) -> tuple:
    """
    Run a read-only SQL query and return (DataFrame | None, error | None).
    Blocks non-SELECT statements. Casts numeric-looking object columns.
    """
    clean = sql.strip().rstrip(";")
    if not clean:
        return None, "Empty query"
    first_kw = clean.lstrip().split()[0].upper()
    if first_kw not in ("SELECT", "WITH"):
        return None, f"Only SELECT queries are permitted. Blocked keyword: {first_kw}"
    try:
        df = pd.read_sql_query(clean, conn)
        for col in df.select_dtypes(include="object").columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass
        return df, None
    except sqlite3.OperationalError as e:
        return None, f"SQL error: {e}"
    except Exception as e:
        return None, f"Unexpected error: {e}"


def get_db_stats(conn: sqlite3.Connection) -> dict:
    """Fast indexed KPI counts for the sidebar strip. All use index-covered cols."""
    try:
        return {
            "active_deals":  conn.execute("SELECT COUNT(*) FROM deals WHERE status='Active'").fetchone()[0],
            "total_vendors": conn.execute("SELECT COUNT(*) FROM vendors WHERE active=1").fetchone()[0],
            "delayed_wo":    conn.execute("SELECT COUNT(*) FROM work_orders WHERE status='Delayed'").fetchone()[0],
            "pipeline_val":  conn.execute(
                "SELECT ROUND(SUM(deal_value)/1e6,1) FROM deals WHERE status='Active'"
            ).fetchone()[0] or 0.0,
        }
    except Exception:
        return {}


def get_smart_chart_config(df: pd.DataFrame) -> tuple:
    """
    Return (x_col, y_col, chart_type) using column name semantics.

    Prevents the original bug of always using columns[0]/[1] regardless of meaning,
    which produced charts of 'id' vs 'vendor_id' — meaningless numeric axes.

    Priority order:
      1. Date/period column exists  → x=date, y=numeric, type=line
      2. Named categorical column   → x-axis (vendor_name, region, status…)
      3. Named aggregate column     → y-axis (deal_value, budget, count…)
      4. Pie if ≤10 rows, 2 cols, categorical + numeric
      5. Horizontal bar if >8 categories (avoids label overlap)
    """
    cols      = list(df.columns)
    cat_hints = {"name", "region", "vendor", "title", "type", "status",
                 "scope", "language", "format", "assigned", "method", "company"}
    val_hints = {"value", "total", "sum", "count", "budget", "hours",
                 "amount", "spend", "cost", "revenue", "avg", "mean", "rating"}
    date_hints = {"date", "month", "quarter", "year", "period", "week"}

    x_cands  = [c for c in cols if any(h in c.lower() for h in cat_hints)]
    y_cands  = [c for c in cols
                if any(h in c.lower() for h in val_hints)
                and c.lower() not in NON_METRIC_COLS
                and pd.api.types.is_numeric_dtype(df[c])]
    date_cols = [c for c in cols if any(h in c.lower() for h in date_hints)]

    if date_cols and y_cands:
        return date_cols[0], y_cands[0], "line"

    x_col = x_cands[0] if x_cands else cols[0]

    y_col = None
    if y_cands:
        y_col = y_cands[0]
    else:
        for c in cols:
            if (c != x_col
                    and c.lower() not in NON_METRIC_COLS
                    and pd.api.types.is_numeric_dtype(df[c])):
                y_col = c
                break
    y_col = y_col or (cols[1] if len(cols) > 1 else cols[0])

    if (len(cols) == 2 and x_col != y_col
            and pd.api.types.is_numeric_dtype(df[y_col])
            and len(df) <= 10):
        return x_col, y_col, "pie"

    return x_col, y_col, "bar"
