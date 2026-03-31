"""
query_parser.py — Foundry Vantage NL→SQL engine (v2.0)
Converts plain-English questions into SQLite queries against the Rights Explorer schema.

SUPPORTED TABLES (14):
- title, movie, series, season
- media_rights, content_deal
- do_not_air
- sales_deal
- work_orders, vendors
- deals
- elemental_rights, elemental_deal
- exhibition_restrictions

CROSS-TABLE JOIN PATTERNS (15+):
- deals + rights
- vendors + work_orders
- movies + DNA + sales
- titles + rights + DNA + sales
- work_orders + rights expiry
- elemental + deals
- And more...
"""
import re
from typing import Tuple, Optional, List
from datetime import datetime, timedelta

# ── Keyword sets (FIXED: NO trailing spaces) ─────────────────────────────────
DNA_KW       = {"do not air", "do-not-air", "dna", "restrict", "banned", "blocked", "not allowed"}
ELEMENTAL_KW = {"elemental", "element", "promo", "trailer", "edit", "featurette", "asset", "raw"}
SALES_KW     = {"sales deal", "rights out", "rights-out", "sold", "affiliate", "3rd party", "buyer"}
EXPIRY_KW    = {"expir", "renew", "laps", "upcoming", "due", "alert", "soon", "days left", "days remaining"}
WORK_KW      = {"work order", "quality", "task", "workload"}
DEAL_KW      = {"deal", "deals", "contract", "contracts", "agreement"}
RIGHTS_KW    = {"rights", "license", "licensed", "window", "windows", "hold", "holds", "have rights", "rights to"}
TITLE_KW     = {"title", "titles", "show", "shows", "series", "season", "episode", "episodes", "catalog", "what do we have"}
VENDOR_KW    = {"vendor", "vendors", "supplier", "partner"}

# Movie keywords
MOVIE_TITLES = {
    "dune", "barbie", "batman", "oppenheimer", "aquaman", "wonka", "beetlejuice", "furiosa",
    "shazam", "tenet", "godzilla", "matrix", "mortal kombat", "suicide squad", "wonder woman",
    "black adam", "space jam", "meg", "elvis", "the flash", "the batman", "the penguin",
    "white noise", "the witches", "color purple", "animal kingdom",
}
MOVIE_VOCAB = {
    "movie", "movies", "film", "films", "feature", "theatrical", "cinema",
    "box office", "box-office", "film slate", "movie slate",
    "dc film", "dc movie", "warner film", "wbd film", "wb film",
    "franchise film", "library film", "library title",
    "direct-to-streaming", "dtv", "hbo original film", "hbo film",
}
MOVIE_KW = MOVIE_TITLES | MOVIE_VOCAB

# FIXED: No trailing spaces
REGION_CANONICAL = {"NA", "APAC", "EMEA", "LATAM"}
ALL_MEDIA = ["PayTV", "STB-VOD", "SVOD", "FAST", "CatchUp", "StartOver",
             "Simulcast", "TempDownload", "DownloadToOwn"]

ONTOLOGY = {
    "streaming": "SVOD", "subscription": "SVOD", "cable": "PayTV",
    "ad-supported": "FAST", "ad supported": "FAST", "free tv": "FAST",
    "catch-up": "CatchUp", "catch up": "CatchUp", "download": "TempDownload",
    "uk": "EMEA", "europe": "EMEA", "asia": "APAC",
    "latin america": "LATAM", "south america": "LATAM", "north america": "NA",
    "united states": "NA", "usa": "NA",
}

# ── Helper functions ─────────────────────────────────────────────────────────

def _apply_ontology(q):
    for phrase in sorted(ONTOLOGY.keys(), key=len, reverse=True):
        if phrase in q:
            q = q.replace(phrase, ONTOLOGY[phrase])
    return q

def _extract_regions(q):
    """Extract ALL regions mentioned in query (supports multiple regions)"""
    found_regions = []
    q_upper = q.upper()
    for r in REGION_CANONICAL:
        if r in q_upper:
            found_regions.append(r)
    return found_regions if found_regions else None

def _extract_date_range(q):
    """
    Extract date filters from natural language
    Returns: sql_filter string or "1=1" if no date filter
    """
    # Pattern: "last X days"
    match = re.search(r'last\s+(\d+)\s+days?', q)
    if match:
        days = int(match.group(1))
        return f"deal_date >= DATE('now', '-{days} days')"
    
    # Pattern: "last X weeks"
    match = re.search(r'last\s+(\d+)\s+weeks?', q)
    if match:
        weeks = int(match.group(1))
        days = weeks * 7
        return f"deal_date >= DATE('now', '-{days} days')"
    
    # Pattern: "last X months"
    match = re.search(r'last\s+(\d+)\s+months?', q)
    if match:
        months = int(match.group(1))
        days = months * 30
        return f"deal_date >= DATE('now', '-{days} days')"
    
    # Pattern: "in YYYY"
    match = re.search(r'(?:in|year)\s*(\d{4})', q)
    if match:
        year = int(match.group(1))
        return f"deal_date BETWEEN '{year}-01-01' AND '{year}-12-31'"
    
    # Pattern: "between DATE and DATE"
    match = re.search(r'between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})', q)
    if match:
        start, end = match.group(1), match.group(2)
        return f"deal_date BETWEEN '{start}' AND '{end}'"
    
    return "1=1"

def _extract_platforms(q):
    q2 = _apply_ontology(q.lower())
    return [p for p in ALL_MEDIA if p.lower() in q2.lower() or p in q2]

def _extract_title_hints(q):
    quoted = re.findall(r'"([^"]+)"', q)
    if quoted:
        return quoted[0]
    known = [
        # TV Series
        "House of the Dragon", "The Last of Us", "Succession", "The White Lotus",
        "Euphoria", "Westworld", "Barry", "True Detective", "The Wire", "The Sopranos",
        "The Penguin", "Dune: Prophecy", "The Bear", "Andor", "The Mandalorian",
        "Foundation", "Shrinking", "Reacher", "The Boys", "Squid Game",
        # Movies
        "Dune: Part One", "Dune: Part Two", "Barbie", "Oppenheimer",
        "The Batman", "Aquaman and the Lost Kingdom", "The Flash", "Black Adam",
        "Shazam! Fury of the Gods", "Wonka", "Beetlejuice Beetlejuice", "Furiosa",
        "Meg 2: The Trench", "The Color Purple", "Elvis", "Animal Kingdom",
        "White Noise", "The Witches", "Tenet", "Wonder Woman 1984", "Mortal Kombat",
        "The Suicide Squad", "Matrix Resurrections", "Space Jam: A New Legacy",
        "Godzilla vs. Kong",
    ]
    for s in sorted(known, key=len, reverse=True):
        if s.lower() in q.lower():
            return s
    return None

def _region_where(regions, col="region"):
    """Handle single or multiple regions properly"""
    if not regions:
        return "1=1"
    if len(regions) == 1:
        return f"UPPER({col}) = '{regions[0]}'"
    joined = "','".join(regions)
    return f"UPPER({col}) IN ('{joined}')"

def _platform_like(platforms, col):
    if not platforms:
        return "1=1"
    return "(" + " OR ".join(f"{col} LIKE '%{p}%'" for p in platforms) + ")"

def _title_like(hint, col="title_name"):
    # Basic sanitization to prevent SQL injection
    safe_hint = hint.replace("'", "''").replace(";", "")[:100]
    return f"LOWER({col}) LIKE '%{safe_hint.lower()}%'"

def _is_movie_query(q):
    return any(kw in q for kw in MOVIE_KW)

def _movie_cat_filter(q, prefix="m"):
    if "theatrical" in q and "library" not in q:
        return f"AND {prefix}.content_category = 'Theatrical'"
    if "library" in q:
        return f"AND {prefix}.content_category = 'Library'"
    if "hbo original" in q or "hbo film" in q:
        return f"AND {prefix}.content_category = 'HBO Original'"
    if "direct-to-streaming" in q or "dtv" in q:
        return f"AND {prefix}.content_category = 'Direct-to-Streaming'"
    return ""

# ── Cross-table join patterns (ENHANCED) ──────────────────────────────────────

def _detect_cross_intent(q):
    """
    Detect when user wants data from 2+ distinct tables.
    Returns join pattern name or None.
    
    SUPPORTED PATTERNS:
    - deals_rights: deals + media_rights
    - vendors_workorders: vendors + work_orders
    - movies_dna_sales: movies + DNA + sales_deal
    - title_health: title + rights + DNA + sales
    - expiry_sales: expiring rights + sales deals
    - workorder_rights: work orders + rights expiry
    - elemental_deals: elemental rights + deals
    - movies_dna: movies + DNA flags
    - movies_sales: movies + sales / buyer
    - title_sales: specific title + sales deal lookup
    """
    has_movie   = any(kw in q for kw in MOVIE_KW)
    has_rights  = any(kw in q for kw in RIGHTS_KW) or any(kw in q for kw in EXPIRY_KW)
    has_dna     = any(kw in q for kw in DNA_KW) or "flag" in q or "flagged" in q
    has_sales   = any(kw in q for kw in SALES_KW) or "netflix" in q or "amazon" in q or "buyer" in q
    has_work    = any(kw in q for kw in WORK_KW) or "work order" in q
    has_expiry  = any(kw in q for kw in EXPIRY_KW)
    has_title   = any(kw in q for kw in TITLE_KW) or any(kw in q for kw in MOVIE_KW)
    has_deals   = any(kw in q for kw in DEAL_KW)
    has_vendor  = any(kw in q for kw in VENDOR_KW)
    has_elemental = any(kw in q for kw in ELEMENTAL_KW)
    
    # Rank by specificity — most specific first
    
    # NEW: Deals + Rights combination
    if has_deals and has_rights:
        return "deals_rights"
    
    # NEW: Vendors + Work Orders combination
    if has_vendor and has_work:
        return "vendors_workorders"
    
    # NEW: Elemental + Deals combination
    if has_elemental and has_deals:
        return "elemental_deals"
    
    # Existing patterns
    if has_movie and has_dna and has_sales:
        return "movies_dna_sales"
    if has_movie and has_dna:
        return "movie_dna"
    if has_movie and has_sales:
        return "movie_sales"
    if (has_movie or has_title) and has_rights and has_dna:
        return "title_health"
    if has_expiry and has_sales:
        return "expiry_sales"
    if has_work and (has_rights or has_title):
        return "workorder_rights"
    if has_title and has_sales:
        return "title_sales"
    
    return None

# ── Main parser ──────────────────────────────────────────────────────────────

class QueryParser:
    @classmethod
    def generate_sql(cls, question, selected_region):
        q = question.lower().strip()
        
        # ── Region resolution (SUPPORTS MULTIPLE REGIONS) ───────────────────
        text_regions = _extract_regions(q)
        if text_regions:
            # If regions are explicitly mentioned in query, use them
            regions = text_regions
        else:
            regions = [selected_region]
        
        # ── Date range extraction ───────────────────────────────────────────
        date_filter = _extract_date_range(q)
        
        region_ctx = " vs ".join(regions) if len(regions) > 1 else regions[0]
        rw = _region_where(regions)
        rw_mr = _region_where(regions, "mr.region")
        rw_t = _region_where(regions, "t.region")
        rw_deals = _region_where(regions, "region")
        rw_vendors = _region_where(regions, "v.region")
        rw_wo = _region_where(regions, "wo.region")
        rw_sales = _region_where(regions, "sd.region")
        
        platforms = _extract_platforms(q)
        plat_mr = _platform_like(platforms, "mr.media_platform_primary") if platforms else "1=1"
        title_hint = _extract_title_hints(question)
        
        # ── 0. CROSS-TABLE JOIN QUERIES (highest priority) ─────────────────
        cross = _detect_cross_intent(q)
        
        # ── NEW: Deals + Rights Join ───────────────────────────────────────
        if cross == "deals_rights":
            """
            Query: "LATAM and EMEA deals and rights in last 60 days"
            Returns: Combined view of deals with their associated rights
            """
            # Build WHERE clause with date filter
            where_parts = [rw_deals]
            if date_filter != "1=1":
                where_parts.append(date_filter)
            where_clause = " AND ".join(where_parts)
            
            sql = f"""
                SELECT 
                    d.deal_id,
                    d.deal_name,
                    d.vendor_name,
                    d.deal_type,
                    d.deal_value,
                    d.deal_date,
                    d.expiry_date,
                    d.status AS deal_status,
                    d.territory AS deal_territory,
                    mr.title_name,
                    mr.rights_type,
                    mr.media_platform_primary,
                    mr.term_to AS rights_expiry,
                    mr.status AS rights_status,
                    mr.territories AS rights_territories
                FROM deals d
                LEFT JOIN media_rights mr ON d.territory LIKE '%' || mr.territories || '%'
                WHERE {where_clause}
                ORDER BY d.deal_value DESC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── NEW: Vendors + Work Orders Join ────────────────────────────────
        if cross == "vendors_workorders":
            """
            Query: "vendors and work orders performance"
            Returns: Vendor details with their work order metrics
            """
            sql = f"""
                SELECT 
                    v.vendor_id,
                    v.vendor_name,
                    v.vendor_type,
                    v.rating,
                    v.total_spend,
                    COUNT(wo.work_order_id) AS total_orders,
                    SUM(CASE WHEN wo.status = 'Completed' THEN 1 ELSE 0 END) AS completed_orders,
                    SUM(CASE WHEN wo.status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_orders,
                    AVG(wo.quality_score) AS avg_quality,
                    SUM(wo.cost) AS total_cost,
                    SUM(CASE WHEN wo.billing_status = 'Overdue' THEN 1 ELSE 0 END) AS overdue_count
                FROM vendors v
                LEFT JOIN work_orders wo ON v.vendor_id = wo.vendor_id
                WHERE {rw_vendors}
                GROUP BY v.vendor_id
                ORDER BY total_orders DESC
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── NEW: Elemental + Deals Join ────────────────────────────────────
        if cross == "elemental_deals":
            """
            Query: "elemental rights and deals"
            Returns: Elemental rights with their associated deal information
            """
            sql = f"""
                SELECT 
                    er.elemental_rights_id,
                    er.title_name,
                    er.media_platform_primary,
                    er.territories,
                    er.language,
                    er.term_from,
                    er.term_to,
                    er.status AS rights_status,
                    ed.deal_name,
                    ed.deal_source,
                    ed.deal_type,
                    ed.deal_value,
                    ed.status AS deal_status
                FROM elemental_rights er
                JOIN elemental_deal ed ON er.elemental_deal_id = ed.elemental_deal_id
                WHERE {rw}
                ORDER BY er.term_to ASC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Movies + DNA + Sales (3-table join) ────────────────────────────
        if cross == "movies_dna_sales":
            cat_f = _movie_cat_filter(q, "m")
            sql = f"""
                SELECT
                    m.movie_title,
                    m.content_category,
                    m.genre,
                    m.box_office_gross_usd_m AS box_office_usd_m,
                    m.franchise,
                    COUNT(DISTINCT mr.rights_id) AS active_rights,
                    COUNT(DISTINCT dna.dna_id) AS dna_flags,
                    GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                    GROUP_CONCAT(DISTINCT dna.territory) AS restricted_territories,
                    COUNT(DISTINCT sd.sales_deal_id) AS sales_deals,
                    GROUP_CONCAT(DISTINCT sd.buyer) AS buyers,
                    SUM(sd.deal_value) AS total_sales_value,
                    CASE WHEN COUNT(dna.dna_id) > 0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status
                FROM movie m
                LEFT JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND UPPER(mr.region) = '{regions[0]}' AND mr.status = 'Active'
                LEFT JOIN do_not_air dna ON dna.title_id = t.title_id AND dna.active = 1
                LEFT JOIN sales_deal sd ON sd.title_id = t.title_id
                  AND UPPER(sd.region) = '{regions[0]}'
                WHERE 1=1 {cat_f}
                GROUP BY m.movie_id
                ORDER BY dna_flags DESC, total_sales_value DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Title Health (4-table join) ────────────────────────────────────
        if cross == "title_health":
            title_f = f"AND {_title_like(title_hint,'t.title_name')}" if title_hint else ""
            cat_f = _movie_cat_filter(q, "t")
            sql = f"""
                SELECT
                    t.title_name,
                    t.title_type,
                    t.content_category,
                    t.genre,
                    COUNT(DISTINCT mr.rights_id) AS total_rights,
                    SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                    SUM(CASE WHEN mr.status='Active'
                        AND mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d,
                    MAX(CASE WHEN dna.active=1 THEN '🚫 YES' ELSE '✅ Clean' END) AS dna_flag,
                    COUNT(DISTINCT dna.dna_id) AS dna_count,
                    GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                    COUNT(DISTINCT sd.sales_deal_id) AS sales_deals,
                    GROUP_CONCAT(DISTINCT sd.buyer) AS buyers
                FROM title t
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND UPPER(mr.region) = '{regions[0]}'
                LEFT JOIN do_not_air dna ON t.title_id = dna.title_id AND dna.active = 1
                LEFT JOIN sales_deal sd ON t.title_id = sd.title_id
                  AND UPPER(sd.region) = '{regions[0]}'
                WHERE {rw_t} {title_f} {cat_f}
                GROUP BY t.title_id
                ORDER BY dna_count DESC, expiring_90d DESC, active_rights DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Expiry + Sales ─────────────────────────────────────────────────
        if cross == "expiry_sales":
            days = 90
            for d in re.findall(r'\b(\d+)\s*day', q):
                days = int(d)
                break
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT
                    mr.title_name,
                    mr.media_platform_primary AS rights_platform,
                    mr.territories,
                    mr.term_to AS rights_expiry,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                    mr.exclusivity,
                    mr.rights_type,
                    sd.buyer AS sold_to,
                    sd.deal_value AS sales_value,
                    sd.media_platform AS sales_platform,
                    sd.term_to AS sales_expiry,
                    sd.status AS sales_status,
                    CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale' ELSE '— No Sale' END AS renewal_flag
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                LEFT JOIN sales_deal sd ON mr.title_id = sd.title_id
                  AND UPPER(sd.region) = '{regions[0]}'
                WHERE UPPER(mr.region) = '{regions[0]}'
                  AND mr.status = 'Active'
                  AND mr.term_to <= DATE('now', '+{days} days')
                  AND mr.term_to >= DATE('now')
                  {plat_f}
                ORDER BY days_remaining ASC, sd.deal_value DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Work Orders + Rights ───────────────────────────────────────────
        if cross == "workorder_rights":
            sql = f"""
                SELECT
                    wo.work_order_id,
                    wo.title_name,
                    wo.work_type,
                    wo.status AS wo_status,
                    wo.priority,
                    wo.due_date,
                    wo.quality_score,
                    wo.vendor_name,
                    COUNT(DISTINCT mr.rights_id) AS active_rights,
                    MIN(mr.term_to) AS earliest_rights_expiry,
                    CAST(JULIANDAY(MIN(mr.term_to))-JULIANDAY('now') AS INTEGER) AS days_to_rights_expiry,
                    SUM(CASE WHEN mr.term_to <= DATE('now','+90 days')
                        AND mr.status='Active' THEN 1 ELSE 0 END) AS rights_expiring_90d
                FROM work_orders wo
                LEFT JOIN title t ON wo.title_id = t.title_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND UPPER(mr.region) = '{regions[0]}' AND mr.status = 'Active'
                WHERE {rw_wo}
                GROUP BY wo.work_order_id
                ORDER BY rights_expiring_90d DESC, wo.due_date ASC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Movie + DNA ────────────────────────────────────────────────────
        if cross == "movie_dna":
            cat_f = _movie_cat_filter(q, "m")
            sql = f"""
                SELECT
                    m.movie_title,
                    m.content_category,
                    m.genre,
                    m.box_office_gross_usd_m AS box_office_usd_m,
                    m.franchise,
                    COUNT(DISTINCT mr.rights_id) AS active_rights,
                    COUNT(DISTINCT dna.dna_id) AS dna_flags,
                    GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                    GROUP_CONCAT(DISTINCT dna.reason_subcategory) AS dna_subcategories,
                    GROUP_CONCAT(DISTINCT dna.territory) AS restricted_territories,
                    CASE WHEN COUNT(dna.dna_id) > 0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status
                FROM movie m
                LEFT JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND UPPER(mr.region) = '{regions[0]}' AND mr.status = 'Active'
                LEFT JOIN do_not_air dna ON dna.title_id = t.title_id AND dna.active = 1
                WHERE 1=1 {cat_f}
                GROUP BY m.movie_id
                ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Movie + Sales ──────────────────────────────────────────────────
        if cross == "movie_sales":
            cat_f = _movie_cat_filter(q, "m")
            sql = f"""
                SELECT
                    m.movie_title,
                    m.content_category,
                    m.genre,
                    m.box_office_gross_usd_m AS box_office_usd_m,
                    sd.buyer,
                    sd.deal_type,
                    sd.media_platform AS sold_platform,
                    sd.territory AS sold_territory,
                    sd.deal_value,
                    sd.currency,
                    sd.term_from AS sale_from,
                    sd.term_to AS sale_to,
                    sd.status AS sale_status,
                    COUNT(DISTINCT mr.rights_id) AS rights_in_count
                FROM movie m
                JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN sales_deal sd ON sd.title_id = t.title_id
                  AND UPPER(sd.region) = '{regions[0]}'
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND UPPER(mr.region) = '{regions[0]}' AND mr.status = 'Active'
                WHERE 1=1 {cat_f}
                GROUP BY m.movie_id, sd.sales_deal_id
                ORDER BY sd.deal_value DESC NULLS LAST, m.box_office_gross_usd_m DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── Title + Sales ──────────────────────────────────────────────────
        if cross == "title_sales":
            title_f = f"AND {_title_like(title_hint,'t.title_name')}" if title_hint else ""
            sql = f"""
                SELECT
                    t.title_name,
                    t.title_type,
                    t.content_category,
                    mr.media_platform_primary AS rights_platform,
                    mr.term_to AS rights_expiry,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS rights_days_left,
                    mr.status AS rights_status,
                    sd.buyer,
                    sd.media_platform AS sold_platform,
                    sd.deal_value,
                    sd.currency,
                    sd.term_to AS sale_expiry,
                    sd.status AS sale_status
                FROM title t
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND UPPER(mr.region) = '{regions[0]}'
                LEFT JOIN sales_deal sd ON t.title_id = sd.title_id
                  AND UPPER(sd.region) = '{regions[0]}'
                WHERE {rw_t} {title_f}
                ORDER BY mr.term_to ASC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 1. Do-Not-Air ──────────────────────────────────────────────────
        if any(kw in q for kw in DNA_KW):
            title_filter = f" AND {_title_like(title_hint,'dna.title_name')}" if title_hint else ""
            sql = f"""
                SELECT dna.dna_id, dna.title_name,
                       dna.reason_category, dna.reason_subcategory,
                       dna.territory, dna.media_type,
                       dna.term_from, dna.term_to, dna.additional_notes
                FROM do_not_air dna
                JOIN title t ON dna.title_id = t.title_id
                WHERE {_region_where(regions,'dna.region')} AND dna.active=1 {title_filter}
                ORDER BY dna.reason_category, dna.title_name
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 2. MOVIES / FILMS ──────────────────────────────────────────────
        if _is_movie_query(q):
            cat_f_m = _movie_cat_filter(q, "m")
            cat_f_t = _movie_cat_filter(q, "t")
            
            if "franchise" in q:
                sql = f"""
                    SELECT COALESCE(m.franchise,'Standalone') AS franchise_name,
                           COUNT(DISTINCT m.movie_id) AS films,
                           SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                           COUNT(DISTINCT mr.rights_id) AS rights_count
                    FROM movie m
                    LEFT JOIN title t ON t.movie_id = m.movie_id
                    LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                      AND UPPER(mr.region) = '{regions[0]}'
                    GROUP BY franchise_name
                    ORDER BY total_box_office_usd_m DESC
                """
                return sql.strip(), None, 'bar', region_ctx
            
            if any(kw in q for kw in ["box office", "revenue", "gross", "value", "earnings"]):
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre, m.franchise,
                           m.box_office_gross_usd_m AS box_office_usd_m,
                           m.age_rating, m.release_year,
                           COUNT(DISTINCT mr.rights_id) AS active_rights
                    FROM movie m
                    LEFT JOIN title t ON t.movie_id = m.movie_id
                    LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                      AND UPPER(mr.region) = '{regions[0]}' AND mr.status = 'Active'
                    WHERE 1=1 {cat_f_m}
                    GROUP BY m.movie_id
                    ORDER BY m.box_office_gross_usd_m DESC
                """
                return sql.strip(), None, 'bar', region_ctx
            
            if any(kw in q for kw in ["rights", "license", "window", "platform", "svod", "paytv"]):
                title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
                plat_f = f"AND {plat_mr}" if platforms else ""
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre,
                           m.box_office_gross_usd_m AS box_office_usd_m,
                           mr.rights_type, mr.media_platform_primary,
                           mr.territories, mr.language, mr.exclusivity,
                           mr.term_from, mr.term_to,
                           CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                           mr.status
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE UPPER(mr.region) = '{regions[0]}'
                      AND mr.status = 'Active'
                      {cat_f_t} {title_f} {plat_f}
                    ORDER BY m.box_office_gross_usd_m DESC, mr.term_to ASC
                    LIMIT 200
                """
                return sql.strip(), None, 'table', region_ctx
            
            if any(kw in q for kw in EXPIRY_KW):
                days = 90
                for d in re.findall(r'\b(\d+)\s*day', q):
                    days = int(d)
                    break
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre,
                           m.box_office_gross_usd_m AS box_office_usd_m,
                           mr.media_platform_primary, mr.territories,
                           mr.term_to,
                           CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                           mr.exclusivity, mr.status
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE UPPER(mr.region) = '{regions[0]}'
                      AND mr.status = 'Active'
                      AND mr.term_to <= DATE('now','+{days} days')
                      AND mr.term_to >= DATE('now')
                      {cat_f_t}
                    ORDER BY mr.term_to ASC
                    LIMIT 100
                """
                return sql.strip(), None, 'table', region_ctx
            
            if any(kw in q for kw in ["breakdown", "by genre", "by category", "count", "how many", "genre"]):
                group_col = "m.genre" if "genre" in q else "m.content_category"
                sql = f"""
                    SELECT {group_col} AS category,
                           COUNT(DISTINCT m.movie_id) AS films,
                           SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                           COUNT(DISTINCT mr.rights_id) AS rights_count
                    FROM movie m
                    LEFT JOIN title t ON t.movie_id = m.movie_id
                    LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                      AND UPPER(mr.region) = '{regions[0]}'
                    WHERE 1=1 {cat_f_m}
                    GROUP BY {group_col}
                    ORDER BY films DESC
                """
                return sql.strip(), None, 'bar', region_ctx
            
            if title_hint:
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre,
                           m.franchise, m.box_office_gross_usd_m AS box_office_usd_m,
                           m.age_rating, m.release_year,
                           mr.rights_type, mr.media_platform_primary,
                           mr.territories, mr.exclusivity,
                           mr.term_from, mr.term_to, mr.status
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE UPPER(mr.region) = '{regions[0]}'
                      AND LOWER(m.movie_title) LIKE '%{title_hint.lower().split(":")[0].strip()}%'
                    ORDER BY mr.term_to ASC
                    LIMIT 100
                """
                return sql.strip(), None, 'table', region_ctx
            
            sql = f"""
                SELECT m.movie_id, m.movie_title, m.content_category, m.genre,
                       m.franchise, m.box_office_gross_usd_m AS box_office_usd_m,
                       m.age_rating, m.release_year,
                       COUNT(DISTINCT mr.rights_id) AS total_rights,
                       SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                       SUM(CASE WHEN mr.status='Active'
                          AND mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d
                FROM movie m
                LEFT JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND UPPER(mr.region) = '{regions[0]}'
                WHERE 1=1 {cat_f_m}
                GROUP BY m.movie_id
                ORDER BY m.box_office_gross_usd_m DESC
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 3. Elemental rights ────────────────────────────────────────────
        if any(kw in q for kw in ELEMENTAL_KW) and any(kw in q for kw in ["right", "deal"]):
            title_filter = f" AND {_title_like(title_hint,'er.title_name')}" if title_hint else ""
            sql = f"""
                SELECT er.elemental_rights_id, er.title_name,
                       ed.deal_name, ed.deal_source, ed.deal_type,
                       er.territories, er.media_platform_primary, er.language,
                       er.term_from, er.term_to, er.status
                FROM elemental_rights er
                JOIN elemental_deal ed ON er.elemental_deal_id = ed.elemental_deal_id
                WHERE {_region_where(regions,'er.region')} {title_filter}
                ORDER BY er.status DESC, er.title_name
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 4. Sales deals / Rights-Out ────────────────────────────────────
        if any(kw in q for kw in SALES_KW):
            if any(kw in q for kw in ["breakdown", "by buyer", "platform", "by platform"]):
                sql = f"""
                    SELECT buyer, COUNT(*) AS deals,
                           SUM(deal_value) AS total_value,
                           COUNT(DISTINCT title_id) AS titles
                    FROM sales_deal
                    WHERE {rw_sales} AND status='Active'
                    GROUP BY buyer
                    ORDER BY total_value DESC
                    LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT sd.sales_deal_id, sd.deal_type, sd.title_name,
                       sd.buyer, sd.territory, sd.media_platform,
                       sd.term_from, sd.term_to,
                       sd.deal_value, sd.currency, sd.status
                FROM sales_deal sd
                WHERE {rw_sales}
                ORDER BY sd.deal_value DESC
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 5. Rights expiry alerts ────────────────────────────────────────
        if any(kw in q for kw in EXPIRY_KW):
            days = 90
            for d in re.findall(r'\b(\d+)\s*day', q):
                days = int(d)
                break
            plat_f = f"AND {plat_mr}" if platforms else ""
            title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
            sql = f"""
                SELECT mr.rights_id, mr.title_name,
                       cd.deal_name, cd.deal_source,
                       mr.rights_type, mr.media_platform_primary,
                       mr.territories, mr.term_to,
                       CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                WHERE {rw_mr} AND mr.status='Active'
                  AND mr.term_to <= DATE('now','+{days} days')
                  AND mr.term_to >= DATE('now')
                  {plat_f} {title_f}
                ORDER BY mr.term_to ASC
                LIMIT 200
            """
            return sql.strip(), None, 'bar', region_ctx
        
        # ── 6. Specific title — full rights detail ─────────────────────────
        if title_hint and any(kw in q for kw in list(RIGHTS_KW) + ["deal"]):
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT mr.rights_id, mr.title_name,
                       cd.deal_source, cd.deal_type, cd.deal_name,
                       mr.rights_type, mr.media_platform_primary, mr.media_platform_ancillary,
                       mr.territories, mr.language, mr.brand,
                       mr.term_from, mr.term_to,
                       CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                WHERE {rw_mr}
                  AND {_title_like(title_hint,'mr.title_name')}
                  {plat_f}
                ORDER BY mr.term_to ASC
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 7. Rights by count / platform ──────────────────────────────────
        if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["count", "how many", "total"]):
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT mr.rights_type, mr.status,
                       COUNT(DISTINCT mr.title_id) AS title_count,
                       COUNT(mr.rights_id) AS rights_count,
                       SUM(mr.exclusivity) AS exclusive_count
                FROM media_rights mr
                WHERE {rw_mr} {plat_f}
                GROUP BY mr.rights_type, mr.status
                ORDER BY rights_count DESC
            """
            return sql.strip(), None, 'bar', region_ctx
        
        # ── 8. Rights by season hierarchy ──────────────────────────────────
        if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["season", "hierarchy", "by season"]):
            sql = f"""
                SELECT s.series_title, se.season_number,
                       COUNT(DISTINCT t.title_id) AS episodes,
                       COUNT(DISTINCT mr.rights_id) AS rights_count,
                       SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN mr.term_to <= DATE('now','+90 days')
                           AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d
                FROM series s
                JOIN season se ON s.series_id = se.series_id
                JOIN title t ON se.season_id = t.season_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND UPPER(mr.region) = '{regions[0]}'
                GROUP BY s.series_title, se.season_number
                ORDER BY s.series_title, se.season_number
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 9. Exhibition restrictions ─────────────────────────────────────
        if "exhibition" in q and "restrict" in q:
            sql = f"""
                SELECT mr.title_name, er.max_plays, er.max_plays_per_day,
                       er.max_days, er.max_networks,
                       mr.media_platform_primary, mr.territories, mr.status
                FROM exhibition_restrictions er
                JOIN media_rights mr ON er.rights_id = mr.rights_id
                WHERE {rw_mr}
                ORDER BY mr.title_name
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 10. Rights breakdown / analytics ───────────────────────────────
        if any(kw in q for kw in ["breakdown", "distribution", "mix", "analytics", "by territory", "by platform", "by deal"]):
            if "territory" in q or "territories" in q:
                sql = f"""
                    SELECT mr.territories, COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive,
                           COUNT(DISTINCT mr.title_id) AS titles
                    FROM media_rights mr
                    WHERE {rw_mr} AND mr.status='Active'
                    GROUP BY mr.territories
                    ORDER BY rights_count DESC
                    LIMIT 20
                """
            elif any(kw in q for kw in ["deal source", "trl", "c2", "frl"]):
                sql = f"""
                    SELECT cd.deal_source, COUNT(DISTINCT mr.rights_id) AS rights,
                           COUNT(DISTINCT mr.title_id) AS titles,
                           SUM(mr.exclusivity) AS exclusive
                    FROM media_rights mr
                    JOIN content_deal cd ON mr.deal_id = cd.deal_id
                    WHERE {rw_mr}
                    GROUP BY cd.deal_source
                    ORDER BY rights DESC
                """
            else:
                sql = f"""
                    SELECT mr.media_platform_primary AS platform, mr.rights_type,
                           COUNT(*) AS rights_count,
                           COUNT(DISTINCT mr.title_id) AS titles,
                           SUM(mr.exclusivity) AS exclusive
                    FROM media_rights mr
                    WHERE {rw_mr} AND mr.status='Active'
                    GROUP BY platform, mr.rights_type
                    ORDER BY rights_count DESC
                    LIMIT 20
                """
            return sql.strip(), None, 'bar', region_ctx
        
        # ── 11. Deals (vendor contracts) ───────────────────────────────────
        if any(kw in q for kw in DEAL_KW):
            stat_f = "AND status='Active'" if "active" in q else \
                     "AND status='Expired'" if "expired" in q else \
                     "AND (status='Pending' OR status='Under Negotiation')" if "pending" in q else ""
            
            # Combine all WHERE conditions (region + status + date)
            where_parts = [rw_deals]
            if stat_f:
                where_parts.append(stat_f.strip())
            if date_filter != "1=1":
                where_parts.append(date_filter)
            where_clause = " AND ".join(where_parts)
            
            if any(kw in q for kw in ["vendor", "by vendor", "breakdown", "by type", "value"]):
                group_col = "vendor_name" if "vendor" in q else "deal_type"
                sql = f"""
                    SELECT {group_col},
                           COUNT(*) AS deal_count,
                           SUM(deal_value) AS total_value,
                           AVG(deal_value) AS avg_value,
                           SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
                           SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY {group_col}
                    ORDER BY total_value DESC
                    LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx
            
            sql = f"""
                SELECT deal_id, deal_name, vendor_name, deal_type,
                       deal_value, rights_scope, territory,
                       deal_date, expiry_date, status, payment_status,
                       CAST(JULIANDAY(expiry_date)-JULIANDAY('now') AS INTEGER) AS days_to_expiry
                FROM deals
                WHERE {where_clause}
                ORDER BY deal_value DESC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 12. Work orders / operational ──────────────────────────────────
        if any(kw in q for kw in WORK_KW):
            if "quality" in q or "vendor" in q:
                sql = f"""
                    SELECT vendor_name, COUNT(*) AS orders,
                           AVG(quality_score) AS avg_quality,
                           SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delays,
                           SUM(cost) AS total_cost
                    FROM work_orders
                    WHERE {rw_wo}
                    GROUP BY vendor_name
                    ORDER BY orders DESC
                    LIMIT 10
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT status, COUNT(*) AS count
                FROM work_orders
                WHERE {rw_wo}
                GROUP BY status
                ORDER BY count DESC
            """
            return sql.strip(), None, 'pie', region_ctx
        
        # ── 13. Title catalog ──────────────────────────────────────────────
        if any(kw in q for kw in TITLE_KW):
            title_f = f"WHERE {rw_t}" + (
                f" AND {_title_like(title_hint,'t.title_name')}" if title_hint else "")
            if any(kw in q for kw in ["count", "how many", "total"]):
                sql = f"""
                    SELECT t.genre, COUNT(*) AS title_count
                    FROM title t {title_f}
                    GROUP BY t.genre
                    ORDER BY title_count DESC
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT t.title_id, t.title_name, t.title_type,
                       t.content_category, t.genre,
                       t.release_year, t.controlling_entity,
                       t.age_rating, t.runtime_minutes, t.region,
                       s.series_title, se.season_number, m.movie_title
                FROM title t
                LEFT JOIN season se ON t.season_id = se.season_id
                LEFT JOIN series s ON t.series_id = s.series_id
                LEFT JOIN movie m ON t.movie_id = m.movie_id
                {title_f}
                ORDER BY t.title_type, s.series_title, se.season_number, t.episode_number
                LIMIT 300
            """
            return sql.strip(), None, 'table', region_ctx
        
        # ── 14. Generic fallback ───────────────────────────────────────────
        stat_f = "AND mr.status='Active'" if "active" in q else \
                 "AND mr.status='Expired'" if "expired" in q else ""
        plat_f = f"AND {plat_mr}" if platforms else ""
        sql = f"""
            SELECT mr.title_name, mr.rights_type,
                   mr.media_platform_primary, mr.territories,
                   mr.term_from, mr.term_to, mr.status,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
            FROM media_rights mr
            WHERE {rw_mr} {stat_f} {plat_f}
            ORDER BY mr.term_to DESC
            LIMIT 100
        """
        return sql.strip(), None, 'table', region_ctx

def parse_query(question, selected_region="NA"):
    """
    Public entry point.
    Returns: (sql, error, chart_type, region_context)
    """
    try:
        return QueryParser.generate_sql(question, selected_region)
    except Exception as e:
        return None, str(e), 'table', selected_region
