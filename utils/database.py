def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    # ----- 1. Create tables with all columns (old + new) -----
    c.execute('''
        CREATE TABLE vendors (
            vendor_id INTEGER PRIMARY KEY,
            vendor_name TEXT,
            rating REAL,
            contact_email TEXT,
            phone TEXT,
            address TEXT,
            active INTEGER,
            created_date TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY,
            vendor_id INTEGER,
            vendor_name TEXT,
            deal_name TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            rights_scope TEXT,
            deal_type TEXT,
            status TEXT,
            currency TEXT DEFAULT 'USD',
            description TEXT,
            contract_signed_date TEXT,
            expiry_date TEXT,
            renewal_terms TEXT,
            created_at TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE work_orders (
            id INTEGER PRIMARY KEY,
            title_id INTEGER,
            title_name TEXT,
            vendor_id INTEGER,
            vendor_name TEXT,
            status TEXT,
            region TEXT,
            due_date TEXT,
            priority TEXT,
            assigned_to TEXT,
            start_date TEXT,
            completion_date TEXT,
            notes TEXT,
            estimated_hours REAL
        )
    ''')

    c.execute('''
        CREATE TABLE content_planning (
            id INTEGER PRIMARY KEY,
            title_id INTEGER,
            content_title TEXT,
            status TEXT,
            region TEXT,
            localization_status TEXT,
            delivery_method TEXT,
            budget REAL,
            target_release_date TEXT,
            language TEXT,
            format TEXT,
            content_type TEXT,
            production_company TEXT,
            notes TEXT
        )
    ''')

    # ----- 2. Insert master data (vendors, titles) -----
    # (Vendor list as shown above)
    c.executemany('''
        INSERT INTO vendors VALUES (?,?,?,?,?,?,?,?)
    ''', vendors)

    # Titles are not stored in a separate table, but used as a reference list.

    # ----- 3. Seed dense data -----
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    rights_options = ["Global", "Multi-Region", "Territory Specific"]
    deal_types = ["Library Buy", "Volume Deal", "Output Deal"]
    currencies = ["USD", "EUR", "GBP"]
    statuses = ["Active", "Pending", "Expired"]
    work_statuses = ["Completed", "Delayed", "In Progress", "Not Started"]
    priorities = ["High", "Medium", "Low"]
    languages = ["English", "Spanish", "French", "German", "Japanese"]
    formats = ["4K", "HD", "SD"]
    content_types = ["Series", "Movie", "Documentary", "Special"]
    delivery_methods = ["Satellite", "File Transfer", "Physical Media"]
    loc_statuses = ["Completed", "In Progress", "Pending", "Not Required"]

    # Helper to generate random date between two bounds
    def random_date(start, end):
        return start + timedelta(days=random.randint(0, (end - start).days))

    start_2024 = datetime(2024, 1, 1)
    end_2025 = datetime(2025, 12, 31)
    start_2023 = datetime(2023, 1, 1)

    # Deals
    for reg in regions:
        for i in range(75):
            v_id = random.randint(1, 8)
            vendor_row = next(v for v in vendors if v[0] == v_id)
            vendor_name = vendor_row[1]
            deal_date = random_date(start_2024, end_2025).strftime('%Y-%m-%d')
            signed_date = random_date(start_2023, datetime(2024,6,30)).strftime('%Y-%m-%d')
            expiry = (datetime.strptime(deal_date, '%Y-%m-%d') + timedelta(days=random.randint(180, 730))).strftime('%Y-%m-%d')
            c.execute('''
                INSERT INTO deals (
                    vendor_id, vendor_name, deal_name, deal_value, deal_date,
                    region, rights_scope, deal_type, status, currency,
                    description, contract_signed_date, expiry_date, renewal_terms, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                v_id, vendor_name, f"Package Deal {i+1}", random.uniform(500000, 5000000),
                deal_date, reg, random.choice(rights_options), random.choice(deal_types),
                random.choice(statuses), random.choice(currencies),
                f"Description for deal {i+1}", signed_date, expiry,
                f"Renewal terms {random.choice(['1yr', '2yr', 'negotiable'])}",
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))

    # Work Orders
    for reg in regions:
        for i in range(50):
            t_id, t_name, _ = random.choice(titles)
            v_id = random.randint(1, 8)
            vendor_row = next(v for v in vendors if v[0] == v_id)
            vendor_name = vendor_row[1]
            due = random_date(start_2024, end_2025).strftime('%Y-%m-%d')
            start = random_date(start_2024, datetime.strptime(due, '%Y-%m-%d')).strftime('%Y-%m-%d') if random.random()>0.3 else None
            comp = random_date(datetime.strptime(start, '%Y-%m-%d'), datetime.strptime(due, '%Y-%m-%d')).strftime('%Y-%m-%d') if start and random.random()>0.5 else None
            c.execute('''
                INSERT INTO work_orders (
                    title_id, title_name, vendor_id, vendor_name, status,
                    region, due_date, priority, assigned_to, start_date,
                    completion_date, notes, estimated_hours
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                t_id, t_name, v_id, vendor_name, random.choice(work_statuses),
                reg, due, random.choice(priorities),
                f"User{random.randint(1,10)}", start, comp,
                f"Notes for order {i+1}", random.uniform(10,200)
            ))

    # Content Planning (100 rows)
    for i in range(100):
        t_id, t_name, prod_co = random.choice(titles)
        reg = random.choice(regions)
        target = random_date(start_2024, end_2025).strftime('%Y-%m-%d')
        c.execute('''
            INSERT INTO content_planning (
                title_id, content_title, status, region, localization_status,
                delivery_method, budget, target_release_date, language,
                format, content_type, production_company, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            t_id, t_name, random.choice(["Planning", "In Production", "Completed"]),
            reg, random.choice(loc_statuses), random.choice(delivery_methods),
            random.uniform(50000, 5000000), target,
            random.choice(languages), random.choice(formats),
            random.choice(content_types), prod_co,
            f"Planning notes for title {t_name}"
        ))

    conn.commit()
    return conn
