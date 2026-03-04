import sqlite3
import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Production-grade database manager with connection pooling and error handling"""
    
    def __init__(self, db_path: str = 'foundry.db'):
        self.db_path = db_path
        self._conn = None
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection with proper configuration"""
        if self._conn is None:
            try:
                self._conn = sqlite3.connect(
                    self.db_path, 
                    check_same_thread=False,
                    timeout=30,
                    isolation_level=None  # Autocommit mode
                )
                self._conn.row_factory = sqlite3.Row
                self._conn.execute("PRAGMA foreign_keys = ON")
                self._conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
                self._conn.execute("PRAGMA synchronous = NORMAL")  # Faster writes
                logger.info(f"Database connection established: {self.db_path}")
            except sqlite3.Error as e:
                logger.error(f"Database connection failed: {e}")
                raise
        return self._conn
    
    def close(self):
        """Close database connection"""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed")

class DataGenerator:
    """Generates realistic media supply chain data"""
    
    # Vendor data with realistic details
    VENDORS = [
        (1, 'PixelPerfect Studios', 4.8, 'operations@pixelperfect.com', '+1-310-555-0123', 
         '100 Universal City Plaza, Los Angeles, CA 91608', 1, '2024-01-01', 'Post-Production', 'USA'),
        (2, 'GlobalDub International', 4.5, 'sales@globaldub.com', '+44-20-7946-0123',
         '123 Media Village, London, W6 8JB, UK', 1, '2024-01-05', 'Localization', 'EMEA'),
        (3, 'StreamOps Asia', 4.2, 'info@streamops.sg', '+65-6789-0123',
         '1 Fusionopolis View, Singapore 138577', 1, '2024-01-10', 'Content Delivery', 'APAC'),
        (4, 'VisionPost Brasil', 4.6, 'deals@visionpost.br', '+55-11-98765-4321',
         'Av. Paulista, 1000 - Bela Vista, São Paulo - SP, 01310-100', 1, '2024-01-15', 'Post-Production', 'LATAM'),
        (5, 'CineColor Labs', 4.9, 'studio@cinecolor.com', '+1-416-555-0789',
         '225 Richmond St W, Toronto, ON M5V 1W2, Canada', 1, '2024-01-20', 'Color Grading', 'NA'),
        (6, 'AudioMasters DE', 4.7, 'studio@audiomasters.de', '+49-30-1234-5678',
         'Friedrichstraße 123, 10117 Berlin, Germany', 1, '2024-01-25', 'Audio Post', 'EMEA'),
        (7, 'CaptionWorks India', 4.3, 'support@captionworks.in', '+91-80-6789-0123',
         'Embassy Golf Links, Bengaluru, Karnataka 560071', 1, '2024-02-01', 'Subtitling', 'APAC'),
        (8, 'VFX Masters Mexico', 4.4, 'info@vfxmasters.mx', '+52-55-5678-1234',
         'Av. Presidente Masaryk 111, Polanco, 11560 CDMX', 1, '2024-02-05', 'VFX', 'LATAM'),
        (9, 'ContentGuard', 4.1, 'security@contentguard.com', '+1-206-555-0456',
         '2021 7th Ave, Seattle, WA 98121', 1, '2024-02-10', 'Content Security', 'NA'),
        (10, 'MediaBridge AU', 4.5, 'connect@mediabridge.au', '+61-2-9876-5432',
         '35 Harris St, Pyrmont NSW 2009, Australia', 1, '2024-02-15', 'Distribution', 'APAC')
    ]
    
    # Content titles with metadata
    CONTENT_TITLES = [
        (101, 'The Penguin', 'Warner Bros', 'Series', 'Drama', 2024, 8.5, 5000000),
        (102, 'Dune: Prophecy', 'Legendary', 'Series', 'Sci-Fi', 2024, 9.2, 15000000),
        (103, 'The Last of Us', 'Sony', 'Series', 'Drama', 2023, 9.1, 12000000),
        (104, 'House of the Dragon', 'HBO', 'Series', 'Fantasy', 2024, 8.9, 20000000),
        (105, 'The Witcher', 'Netflix', 'Series', 'Fantasy', 2023, 8.3, 10000000),
        (106, 'Foundation', 'Apple TV+', 'Series', 'Sci-Fi', 2024, 7.8, 8000000),
        (107, 'The Boys', 'Amazon', 'Series', 'Action', 2024, 8.7, 11000000),
        (108, 'Squid Game', 'Netflix', 'Series', 'Thriller', 2024, 8.8, 9000000),
        (109, 'The Crown', 'Netflix', 'Series', 'Drama', 2023, 8.6, 13000000),
        (110, 'Stranger Things', 'Netflix', 'Series', 'Sci-Fi', 2025, 9.0, 25000000),
        (111, 'The Mandalorian', 'Disney+', 'Series', 'Sci-Fi', 2024, 8.9, 15000000),
        (112, 'Loki', 'Disney+', 'Series', 'Fantasy', 2023, 8.5, 14000000),
        (113, 'Andor', 'Disney+', 'Series', 'Sci-Fi', 2024, 9.2, 16000000),
        (114, 'The Bear', 'FX', 'Series', 'Drama', 2024, 8.8, 6000000),
        (115, 'Succession', 'HBO', 'Series', 'Drama', 2023, 9.3, 11000000),
        (116, 'The White Lotus', 'HBO', 'Series', 'Drama', 2024, 8.4, 7000000),
        (117, 'Euphoria', 'HBO', 'Series', 'Drama', 2024, 8.6, 8500000),
        (118, 'The Last Kingdom', 'Netflix', 'Series', 'Historical', 2023, 8.3, 7500000),
        (119, 'Vikings: Valhalla', 'Netflix', 'Series', 'Historical', 2024, 7.9, 8000000),
        (120, 'The Sandman', 'Netflix', 'Series', 'Fantasy', 2024, 8.1, 12000000)
    ]
    
    REGIONS = ["NA", "APAC", "EMEA", "LATAM"]
    REGION_COUNTRIES = {
        "NA": ["USA", "Canada", "Mexico"],
        "APAC": ["Japan", "South Korea", "Australia", "Singapore", "India", "China"],
        "EMEA": ["UK", "Germany", "France", "Italy", "Spain", "UAE", "South Africa"],
        "LATAM": ["Brazil", "Argentina", "Colombia", "Chile", "Peru"]
    }
    
    LANGUAGES = ["English", "Spanish", "French", "German", "Italian", "Japanese", 
                 "Korean", "Mandarin", "Hindi", "Portuguese", "Arabic", "Russian"]
    
    DEAL_TYPES = ["Volume Deal", "Output Deal", "Library Buy", "First-Look Deal", 
                  "Co-Production", "Licensing Agreement", "Distribution Deal"]
    
    RIGHTS_SCOPES = ["Global", "Multi-Region", "Territory Specific", "Exclusive", 
                     "Non-Exclusive", "Pay TV", "Free TV", "SVOD", "AVOD", "TVOD"]
    
    WORK_STATUSES = ["Not Started", "In Progress", "Review", "Completed", "Delayed", "On Hold"]
    PRIORITIES = ["Critical", "High", "Medium", "Low"]
    
    LOCALIZATION_TYPES = ["Subtitles", "Dubbing", "Voice-over", "Audio Description", 
                         "Closed Captions", "Sign Language", "Localized Marketing"]
    
    DELIVERY_METHODS = ["FTP", "ASPERA", "Satellite", "Physical Media", "Cloud", "CDN", "Hard Drive"]
    
    CONTENT_FORMATS = ["4K HDR", "4K SDR", "HD 1080p", "HD 720p", "SD", "DCP", "IMF"]
    
    PRODUCTION_COMPANIES = ["Warner Bros", "Legendary", "Sony Pictures", "HBO", "Netflix",
                           "Amazon Studios", "Disney+", "Apple TV+", "Paramount", "Universal"]

def init_database():
    """Initialize database with comprehensive schema and seeded data"""
    db_manager = DatabaseManager()
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    
    try:
        # Begin transaction
        cursor.execute("BEGIN TRANSACTION")
        
        # Drop existing tables (CASCADE to handle dependencies)
        cursor.execute("DROP TABLE IF EXISTS content_planning")
        cursor.execute("DROP TABLE IF EXISTS work_orders")
        cursor.execute("DROP TABLE IF EXISTS deals")
        cursor.execute("DROP TABLE IF EXISTS vendors")
        
        # Create vendors table with enhanced schema
        cursor.execute("""
        CREATE TABLE vendors (
            vendor_id INTEGER PRIMARY KEY,
            vendor_name TEXT NOT NULL,
            rating REAL CHECK (rating >= 0 AND rating <= 5),
            contact_email TEXT,
            phone TEXT,
            address TEXT,
            active INTEGER DEFAULT 1,
            created_date TEXT,
            vendor_type TEXT,
            region TEXT,
            payment_terms TEXT,
            tax_id TEXT,
            certification_level TEXT,
            avg_response_time REAL,
            completed_projects INTEGER DEFAULT 0,
            total_spend REAL DEFAULT 0,
            last_audit_date TEXT,
            insurance_verified INTEGER DEFAULT 0,
            security_compliance TEXT
        )
        """)
        
        # Create deals table with comprehensive fields
        cursor.execute("""
        CREATE TABLE deals (
            deal_id INTEGER PRIMARY KEY,
            vendor_id INTEGER,
            vendor_name TEXT,
            deal_name TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            country TEXT,
            rights_scope TEXT,
            deal_type TEXT,
            status TEXT,
            currency TEXT DEFAULT 'USD',
            description TEXT,
            contract_signed_date TEXT,
            effective_date TEXT,
            expiry_date TEXT,
            renewal_terms TEXT,
            payment_schedule TEXT,
            revenue_share REAL,
            minimum_guarantee REAL,
            territories_covered INTEGER,
            languages_included INTEGER,
            exclusivity_type TEXT,
            approval_status TEXT,
            approved_by TEXT,
            approved_date TEXT,
            created_at TEXT,
            updated_at TEXT,
            created_by TEXT,
            deal_owner TEXT,
            risk_level TEXT,
            notes TEXT,
            FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
        )
        """)
        
        # Create work_orders table with operational details
        cursor.execute("""
        CREATE TABLE work_orders (
            work_order_id INTEGER PRIMARY KEY,
            title_id INTEGER,
            title_name TEXT,
            vendor_id INTEGER,
            vendor_name TEXT,
            status TEXT,
            region TEXT,
            country TEXT,
            due_date TEXT,
            priority TEXT,
            assigned_to TEXT,
            start_date TEXT,
            completion_date TEXT,
            actual_completion_date TEXT,
            estimated_hours REAL,
            actual_hours REAL,
            work_type TEXT,
            quality_score REAL,
            rework_count INTEGER DEFAULT 0,
            asset_count INTEGER,
            specifications TEXT,
            delivery_method TEXT,
            approved_by TEXT,
            approved_date TEXT,
            cost REAL,
            billing_status TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            created_by TEXT,
            FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
        )
        """)
        
        # Create content_planning table with comprehensive metadata
        cursor.execute("""
        CREATE TABLE content_planning (
            planning_id INTEGER PRIMARY KEY,
            title_id INTEGER,
            content_title TEXT,
            status TEXT,
            region TEXT,
            country TEXT,
            localization_status TEXT,
            delivery_method TEXT,
            budget REAL,
            actual_cost REAL,
            target_release_date TEXT,
            actual_release_date TEXT,
            language TEXT,
            format TEXT,
            content_type TEXT,
            production_company TEXT,
            director TEXT,
            cast_info TEXT,
            genre TEXT,
            season_number INTEGER,
            episode_count INTEGER,
            runtime_minutes INTEGER,
            age_rating TEXT,
            keywords TEXT,
            marketing_budget REAL,
            expected_audience INTEGER,
            critical_score REAL,
            audience_score REAL,
            awards_count INTEGER,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            created_by TEXT
        )
        """)
        
        # Insert vendors with enhanced data
        vendors_data = []
        for vendor in DataGenerator.VENDORS:
            vendor_data = list(vendor) + [
                random.choice(["Net 30", "Net 45", "Net 60", "Prepaid"]),
                f"TAX-{random.randint(10000, 99999)}",
                random.choice(["Gold", "Silver", "Platinum", "Certified"]),
                round(random.uniform(2, 48), 1),  # avg_response_time in hours
                random.randint(50, 500),  # completed_projects
                round(random.uniform(500000, 50000000), 2),  # total_spend
                (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
                random.choice([0, 1]),
                random.choice(["ISO 27001", "SOC2", "TPN", "None"])
            ]
            vendors_data.append(vendor_data)
        
        cursor.executemany("""
        INSERT INTO vendors (
            vendor_id, vendor_name, rating, contact_email, phone, address, 
            active, created_date, vendor_type, region, payment_terms, tax_id,
            certification_level, avg_response_time, completed_projects, total_spend,
            last_audit_date, insurance_verified, security_compliance
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, vendors_data)
        
        # Generate massive dataset
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2026, 12, 31)
        
        deals_data = []
        work_orders_data = []
        content_planning_data = []
        
        # Seed 1000+ deals across regions
        for region in DataGenerator.REGIONS:
            countries = DataGenerator.REGION_COUNTRIES[region]
            
            for _ in range(250):  # 250 deals per region = 1000+ total
                vendor = random.choice(DataGenerator.VENDORS)
                title = random.choice(DataGenerator.CONTENT_TITLES)
                country = random.choice(countries)
                
                deal_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
                effective_date = deal_date + timedelta(days=random.randint(0, 30))
                expiry_date = effective_date + timedelta(days=random.randint(365, 1095))  # 1-3 years
                
                deal_value = round(random.uniform(100000, 50000000), 2)
                mg = round(deal_value * random.uniform(0.5, 0.8), 2) if random.random() > 0.3 else 0
                
                deals_data.append((
                    vendor[0], vendor[1],
                    f"{title[1]} - {region} Deal",
                    deal_value,
                    deal_date.strftime('%Y-%m-%d'),
                    region,
                    country,
                    random.choice(DataGenerator.RIGHTS_SCOPES),
                    random.choice(DataGenerator.DEAL_TYPES),
                    random.choice(["Active", "Pending", "Expired", "Negotiation"]),
                    random.choice(["USD", "EUR", "GBP", "JPY", "BRL"]),
                    f"Deal for {title[1]} in {country}",
                    (deal_date - timedelta(days=random.randint(7, 60))).strftime('%Y-%m-%d'),
                    effective_date.strftime('%Y-%m-%d'),
                    expiry_date.strftime('%Y-%m-%d'),
                    f"Auto-renewal with {random.randint(30, 90)} days notice",
                    random.choice(["Quarterly", "Bi-Annual", "Annual", "Milestone-based"]),
                    round(random.uniform(0, 0.25), 3),
                    mg,
                    random.randint(1, 50),  # territories
                    random.randint(1, 20),  # languages
                    random.choice(["Exclusive", "Non-Exclusive", "Window Exclusive"]),
                    random.choice(["Approved", "Pending", "Under Review"]),
                    f"User-{random.randint(1000, 9999)}",
                    (deal_date - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    f"user{random.randint(1, 20)}@foundry.com",
                    f"owner{random.randint(1, 10)}@foundry.com",
                    random.choice(["Low", "Medium", "High"]),
                    f"Additional notes for deal in {region}"
                ))
        
        # Insert deals in batches
        cursor.executemany("""
        INSERT INTO deals (
            vendor_id, vendor_name, deal_name, deal_value, deal_date,
            region, country, rights_scope, deal_type, status, currency,
            description, contract_signed_date, effective_date, expiry_date,
            renewal_terms, payment_schedule, revenue_share, minimum_guarantee,
            territories_covered, languages_included, exclusivity_type,
            approval_status, approved_by, approved_date, created_at, updated_at,
            created_by, deal_owner, risk_level, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, deals_data)
        
        # Generate 750 work orders
        for region in DataGenerator.REGIONS:
            countries = DataGenerator.REGION_COUNTRIES[region]
            
            for _ in range(188):  # ~750 total
                vendor = random.choice(DataGenerator.VENDORS)
                title = random.choice(DataGenerator.CONTENT_TITLES)
                country = random.choice(countries)
                
                start = start_date + timedelta(days=random.randint(0, 800))
                due = start + timedelta(days=random.randint(7, 90))
                completed = due - timedelta(days=random.randint(-10, 20)) if random.random() > 0.3 else None
                
                est_hours = random.uniform(20, 500)
                actual_hours = est_hours * random.uniform(0.8, 1.5)
                
                work_orders_data.append((
                    title[0], title[1],
                    vendor[0], vendor[1],
                    random.choice(DataGenerator.WORK_STATUSES),
                    region,
                    country,
                    due.strftime('%Y-%m-%d'),
                    random.choice(DataGenerator.PRIORITIES),
                    f"user{random.randint(1, 50)}@foundry.com",
                    start.strftime('%Y-%m-%d'),
                    completed.strftime('%Y-%m-%d') if completed else None,
                    completed.strftime('%Y-%m-%d') if completed and random.random() > 0.2 else None,
                    est_hours,
                    actual_hours,
                    random.choice(["Encoding", "QC", "Localization", "VFX", "Audio", "Mastering"]),
                    round(random.uniform(70, 100), 1),
                    random.randint(0, 5),
                    random.randint(1, 100),
                    f"4K HDR, Dolby Atmos, {random.randint(1, 10)} languages",
                    random.choice(DataGenerator.DELIVERY_METHODS),
                    f"user{random.randint(1, 20)}@foundry.com" if random.random() > 0.3 else None,
                    (due + timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d') if random.random() > 0.3 else None,
                    round(random.uniform(5000, 50000), 2),
                    random.choice(["Paid", "Pending", "Invoiced", "Overdue"]),
                    f"Work order for {title[1]}",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    f"creator{random.randint(1, 10)}@foundry.com"
                ))
        
        cursor.executemany("""
        INSERT INTO work_orders (
            title_id, title_name, vendor_id, vendor_name, status,
            region, country, due_date, priority, assigned_to,
            start_date, completion_date, actual_completion_date,
            estimated_hours, actual_hours, work_type, quality_score,
            rework_count, asset_count, specifications, delivery_method,
            approved_by, approved_date, cost, billing_status,
            notes, created_at, updated_at, created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, work_orders_data)
        
        # Generate 500 content planning items
        for region in DataGenerator.REGIONS:
            countries = DataGenerator.REGION_COUNTRIES[region]
            
            for _ in range(125):  # 500 total
                title = random.choice(DataGenerator.CONTENT_TITLES)
                country = random.choice(countries)
                
                target_date = start_date + timedelta(days=random.randint(300, 1000))
                actual_date = target_date + timedelta(days=random.randint(-30, 60)) if random.random() > 0.3 else None
                
                content_planning_data.append((
                    title[0], title[1],
                    random.choice(["Planning", "In Production", "Completed", "Delayed", "On Hold"]),
                    region,
                    country,
                    random.choice(["Not Started", "In Progress", "Completed", "Partial"]),
                    random.choice(DataGenerator.DELIVERY_METHODS),
                    round(random.uniform(50000, 2000000), 2),
                    round(random.uniform(45000, 2200000), 2) if random.random() > 0.3 else None,
                    target_date.strftime('%Y-%m-%d'),
                    actual_date.strftime('%Y-%m-%d') if actual_date else None,
                    random.choice(DataGenerator.LANGUAGES),
                    random.choice(DataGenerator.CONTENT_FORMATS),
                    title[2],  # content_type from title
                    title[1],  # production company
                    f"Director {random.randint(1, 50)}",
                    f"Cast Member {random.randint(1, 20)}, Cast Member {random.randint(21, 40)}",
                    title[3],  # genre from title
                    random.randint(1, 5),
                    random.randint(6, 24),
                    random.randint(30, 120),
                    random.choice(["G", "PG", "PG-13", "R", "TV-MA", "TV-14"]),
                    f"keyword{random.randint(1, 50)}, keyword{random.randint(51, 100)}",
                    round(random.uniform(100000, 5000000), 2),
                    random.randint(100000, 10000000),
                    round(random.uniform(5.0, 9.5), 1),
                    round(random.uniform(5.0, 9.5), 1),
                    random.randint(0, 15),
                    f"Content planning for {title[1]} in {region}",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    f"planner{random.randint(1, 8)}@foundry.com"
                ))
        
        cursor.executemany("""
        INSERT INTO content_planning (
            title_id, content_title, status, region, country,
            localization_status, delivery_method, budget, actual_cost,
            target_release_date, actual_release_date, language, format,
            content_type, production_company, director, cast_info, genre,
            season_number, episode_count, runtime_minutes, age_rating,
            keywords, marketing_budget, expected_audience, critical_score,
            audience_score, awards_count, notes, created_at, updated_at, created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, content_planning_data)
        
        # Commit transaction
        cursor.execute("COMMIT")
        
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_region ON deals(region)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_vendor ON deals(vendor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deals_date ON deals(deal_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_orders_region ON work_orders(region)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_orders_status ON work_orders(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_planning_region ON content_planning(region)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_planning_status ON content_planning(status)")
        
        # Analyze tables for query optimization
        cursor.execute("ANALYZE")
        
        logger.info("Database initialized successfully with 2000+ records")
        
    except sqlite3.Error as e:
        cursor.execute("ROLLBACK")
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        # Don't close connection here as it's cached
        pass
    
    return conn

def execute_sql(sql: str, conn: sqlite3.Connection) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Execute SQL query safely with error handling"""
    try:
        # Remove any trailing semicolons and strip whitespace
        sql = sql.strip().rstrip(';')
        
        # Log query for debugging
        logger.debug(f"Executing SQL: {sql}")
        
        # Execute query
        df = pd.read_sql_query(sql, conn)
        
        # Log result info
        logger.info(f"Query returned {len(df)} rows, {len(df.columns)} columns")
        
        return df, None
    except Exception as e:
        error_msg = f"SQL execution error: {str(e)}"
        logger.error(error_msg)
        return None, error_msg

def get_table_stats(conn: sqlite3.Connection) -> dict:
    """Get statistics about database tables"""
    stats = {}
    tables = ['vendors', 'deals', 'work_orders', 'content_planning']
    
    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn)
            stats[table] = df.iloc[0]['count']
        except:
            stats[table] = 0
    
    return stats
