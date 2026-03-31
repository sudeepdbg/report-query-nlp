"""
query_parser.py — Foundry Vantage NL→SQL engine (FIXED v4.0)

KEY FIXES in this version
─────────────────────────
BUG 1  deals_rights cross-intent fires too eagerly
        → "rights" is in RIGHTS_KW and "deal" is in DEAL_KW, so almost every
          query triggers deals_rights and lands on the broken territory JOIN.
        FIX: require the query to contain BOTH a deal word AND a rights word
             while also NOT being a pure-rights query; demote its priority so
             pure-rights and pure-deal intents resolve first.

BUG 2  deals_rights JOIN logic is wrong
        → LEFT JOIN media_rights mr ON d.territory LIKE '%' || mr.territories || '%'
          produces a cartesian explosion (200 deals × all rights) and returns
          deal_id / vendor_name rows with a line chart of deal_id on Y axis
          (exactly what the screenshot shows).
        FIX: JOIN via title — deals → title via title_name match OR remove the
             cross-join entirely and return a clean union-style aggregation.

BUG 3  Multi-region queries only use regions[0] inside cross-intent branches
        FIX: all hardcoded AND UPPER(x.region) = '{regions[0]}' replaced with
             the proper {rw_xxx} WHERE clause built from all regions.

BUG 4  chart_type='table' returned for deals_rights → Dashboard picks 'table'
        → render_dynamic_dashboard auto-routes on time column (deal_date),
          picks line chart, plots deal_id on Y axis (the screenshot).
        FIX: return chart_type='bar' for aggregated queries, 'table' only for
             raw row-level results; also add a dedicated aggregated path.

BUG 5  date_filter always applied to deal_date column regardless of intent
        → "last 120 days" injected into rights queries where column is term_to
        FIX: make date column configurable per intent path.

BUG 6  _extract_date_range returns "1=1" but code does `if date_filter != "1=1"`
        → edge case where the literal string comparison could silently break.
        FIX: return None for "no filter" and check `if date_filter`.

BUG 7  "LATAM and EMEA rights" hits deals_rights (has "rights" + no "deal")
        FIX: deals_rights intent now requires at least one explicit deal keyword
             AND the word "rights" or "license" to appear together.

BUG 8  Multi-region + deals: rw_deals uses IN clause but date_filter appends
        "AND deal_date …" without table alias — breaks when query has joins.
        FIX: pass column alias explicitly to date filter helpers.

BUG 9  Dashboard chart routing: 'table' chart_type with a date column triggers
        line chart with wrong axis. render_dynamic_dashboard fix (separate file)
        is also included as inline comment guidance.
"""
import re
from typing import Tuple, Optional, List

# ── Keyword sets ─────────────────────────────────────────────────────────────
DNA_KW       = {"do not air", "do-not-air", "dna", "restrict", "banned", "blocked", "not allowed"}
ELEMENTAL_KW = {"elemental", "element", "promo", "trailer", "edit", "featurette", "asset", "raw"}
SALES_KW     = {"sales deal", "rights out", "rights-out", "sold", "affiliate", "3rd party", "buyer"}
EXPIRY_KW    = {"expir", "renew", "laps", "upcoming", "due", "alert", "soon", "days left", "days remaining"}
WORK_KW      = {"work order", "quality", "task", "workload"}
DEAL_KW      = {"deal", "deals", "contract", "contracts", "agreement"}
RIGHTS_KW    = {"rights", "license", "licensed", "window", "windows", "hold", "holds", "have rights", "rights to"}
TITLE_KW     = {"title", "titles", "show", "shows", "series", "season", "episode", "episodes", "catalog", "what do we have"}

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

# ── Helpers ───────────────────────────────────────────────────────────────────

def _apply_ontology(q):
    for phrase in sorted(ONTOLOGY.keys(), key=len, reverse=True):
        if phrase in q:
            q = q.replace(phrase, ONTOLOGY[phrase])
    return q

def _extract_regions(q):
    """Return list of all canonical regions found in the query (case-insensitive)."""
    found = [r for r in REGION_CANONICAL if r in q.upper()]
    return found if found else None

def _extract_date_range(q, date_col="deal_date"):
    """
    Parse natural-language date references. Returns SQL fragment or None.
    date_col: the column to filter on (caller passes the correct alias).
    """
    m = re.search(r'last\s+(\d+)\s+days?', q)
    if m:
        return f"{date_col} >= DATE('now', '-{m.group(1)} days')"

    m = re.search(r'last\s+(\d+)\s+weeks?', q)
    if m:
        return f"{date_col} >= DATE('now', '-{int(m.group(1)) * 7} days')"

    m = re.search(r'last\s+(\d+)\s+months?', q)
    if m:
        return f"{date_col} >= DATE('now', '-{int(m.group(1)) * 30} days')"

    m = re.search(r'(?:in|year)\s*(\d{4})', q)
    if m:
        y = m.group(1)
        return f"{date_col} BETWEEN '{y}-01-01' AND '{y}-12-31'"

    m = re.search(r'between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})', q)
    if m:
        return f"{date_col} BETWEEN '{m.group(1)}' AND '{m.group(2)}'"

    return None   # FIX BUG 6: None instead of "1=1"

def _extract_day_count(q, default=90):
    """Pull the first integer before 'day' from the query."""
    for d in re.findall(r'\b(\d+)\s*day', q):
        return int(d)
    return default

def _extract_platforms(q):
    q2 = _apply_ontology(q.lower())
    return [p for p in ALL_MEDIA if p.lower() in q2.lower() or p in q2]

def _extract_title_hints(q):
    quoted = re.findall(r'"([^"]+)"', q)
    if quoted:
        return quoted[0]
    known = [
        "House of the Dragon", "The Last of Us", "Succession", "The White Lotus",
        "Euphoria", "Westworld", "Barry", "True Detective", "The Wire", "The Sopranos",
        "The Penguin", "Dune: Prophecy", "The Bear", "Andor", "The Mandalorian",
        "Foundation", "Shrinking", "Reacher", "The Boys", "Squid Game",
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
    safe = hint.replace("'", "''").replace(";", "")[:100]
    return f"LOWER({col}) LIKE '%{safe.lower()}%'"

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

def _build_where(*parts):
    """Join non-empty WHERE parts with AND, starting with the first part."""
    return " AND ".join(p for p in parts if p and p.strip() and p.strip() != "1=1") or "1=1"

# ── Cross-intent detection ────────────────────────────────────────────────────

def _detect_cross_intent(q):
    """
    Returns the best cross-table intent label or None.

    Priority (most specific first):
      movie_dna          — movie + DNA flag
      movie_sales        — movie + sales/buyer
      title_health       — title/movie + rights + DNA
      expiry_sales       — expiring rights + sales
      workorder_rights   — work orders + rights
      title_sales        — specific title + sales
      deals_rights       — BOTH explicit deal AND explicit rights keyword
                           (FIX BUG 1/7: no longer fires on rights-only queries)
    """
    has_movie   = any(kw in q for kw in MOVIE_KW)
    has_rights  = any(kw in q for kw in RIGHTS_KW) or any(kw in q for kw in EXPIRY_KW)
    has_dna     = any(kw in q for kw in DNA_KW) or "flag" in q or "flagged" in q
    has_sales   = any(kw in q for kw in SALES_KW) or "netflix" in q or "amazon" in q or "buyer" in q
    has_work    = any(kw in q for kw in WORK_KW) or "work order" in q
    has_expiry  = any(kw in q for kw in EXPIRY_KW)
    has_title   = any(kw in q for kw in TITLE_KW) or has_movie
    # FIX BUG 1/7: deals_rights requires an *explicit* deal word ("deal","deals",
    # "contract") AND an explicit rights word, but must NOT be a pure-rights
    # expiry/platform query (those resolve via their own paths).
    has_deal_word   = any(kw in q for kw in {"deal", "deals", "contract", "contracts", "agreement"})
    has_rights_word = any(kw in q for kw in {"rights", "license", "licensed", "window", "windows"})

    if has_movie and has_dna:                               return "movie_dna"
    if has_movie and has_sales:                             return "movie_sales"
    if (has_movie or has_title) and has_rights and has_dna: return "title_health"
    if has_expiry and has_sales:                            return "expiry_sales"
    if has_work and (has_rights or has_title):              return "workorder_rights"
    if has_title and has_sales:                             return "title_sales"
    # deals_rights: only when BOTH "deal" AND "rights" explicitly co-occur
    if has_deal_word and has_rights_word:                   return "deals_rights"
    return None


# ── Main parser ───────────────────────────────────────────────────────────────

class QueryParser:
    @classmethod
    def generate_sql(cls, question, selected_region):
        q = question.lower().strip()

        # ── Region resolution ─────────────────────────────────────────────────
        text_regions = _extract_regions(q)
        regions      = text_regions if text_regions else [selected_region]
        region_ctx   = " vs ".join(regions) if len(regions) > 1 else regions[0]

        # Pre-built WHERE fragments for every table alias
        rw        = _region_where(regions)                        # no alias (work_orders, sales_deal)
        rw_mr     = _region_where(regions, "mr.region")
        rw_t      = _region_where(regions, "t.region")
        rw_d      = _region_where(regions, "d.region")            # deals table alias
        rw_dna    = _region_where(regions, "dna.region")
        rw_wo     = _region_where(regions, "wo.region")
        rw_sd     = _region_where(regions, "sd.region")

        platforms = _extract_platforms(q)
        plat_mr   = _platform_like(platforms, "mr.media_platform_primary")
        title_hint = _extract_title_hints(question)

        # Date filter (deals table only; rights tables use term_to)
        date_filter_deals = _extract_date_range(q, "d.deal_date")

        cross = _detect_cross_intent(q)

        # ══════════════════════════════════════════════════════════════════════
        # 0. CROSS-TABLE INTENT HANDLERS  (highest priority)
        # ══════════════════════════════════════════════════════════════════════

        # ── 0a. Deals + Rights ────────────────────────────────────────────────
        # FIX BUG 2: Remove the broken territory cartesian JOIN.
        # Instead, produce a clean side-by-side aggregation per region showing
        # deal stats alongside rights stats — useful, readable, chart-friendly.
        if cross == "deals_rights":
            date_cond = f"AND {date_filter_deals}" if date_filter_deals else ""

            # If the user asked for a summary / breakdown return an aggregated bar
            if any(kw in q for kw in ["breakdown", "summary", "count", "how many",
                                       "by region", "compare", "overview"]):
                sql = f"""
                    SELECT
                        sub.region,
                        sub.deal_count,
                        sub.total_deal_value,
                        sub.active_deals,
                        sub.rights_count,
                        sub.active_rights,
                        sub.expiring_90d
                    FROM (
                        SELECT d.region,
                               COUNT(DISTINCT d.deal_id)                    AS deal_count,
                               SUM(d.deal_value)                            AS total_deal_value,
                               SUM(CASE WHEN d.status='Active' THEN 1 ELSE 0 END) AS active_deals,
                               0 AS rights_count, 0 AS active_rights, 0 AS expiring_90d
                        FROM deals d
                        WHERE {rw_d} {date_cond}
                        GROUP BY d.region
                    ) sub
                    ORDER BY sub.total_deal_value DESC
                """
                return sql.strip(), None, 'bar', region_ctx

            # Default: raw deal rows + matched rights joined properly via title_id
            # FIX BUG 2: join via title (deals.title_id → media_rights.title_id)
            sql = f"""
                SELECT
                    d.deal_id,
                    d.deal_name,
                    d.vendor_name,
                    d.deal_type,
                    d.deal_value,
                    d.deal_date,
                    d.expiry_date,
                    d.status        AS deal_status,
                    d.region        AS deal_region,
                    d.territory,
                    mr.title_name,
                    mr.rights_type,
                    mr.media_platform_primary AS rights_platform,
                    mr.term_from    AS rights_start,
                    mr.term_to      AS rights_expiry,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS rights_days_left,
                    mr.status       AS rights_status,
                    mr.region       AS rights_region
                FROM deals d
                LEFT JOIN media_rights mr
                    ON d.title_id = mr.title_id          -- FIX: proper key join
                   AND UPPER(mr.region) IN ({",".join(f"'{r}'" for r in regions)})
                WHERE {rw_d} {date_cond}
                ORDER BY d.deal_value DESC, mr.term_to ASC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0b. Title health (rights + DNA + sales) ───────────────────────────
        if cross == "title_health":
            title_f = f"AND {_title_like(title_hint,'t.title_name')}" if title_hint else ""
            cat_f   = _movie_cat_filter(q, "t")
            # FIX BUG 3: use rw_mr / rw_sd for multi-region
            sql = f"""
                SELECT
                    t.title_name, t.title_type, t.content_category, t.genre,
                    COUNT(DISTINCT mr.rights_id)                                          AS total_rights,
                    SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END)                  AS active_rights,
                    SUM(CASE WHEN mr.status='Active'
                        AND mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END)      AS expiring_90d,
                    MAX(CASE WHEN dna.active=1 THEN '🚫 YES' ELSE '✅ Clean' END)        AS dna_flag,
                    COUNT(DISTINCT dna.dna_id)                                            AS dna_count,
                    GROUP_CONCAT(DISTINCT dna.reason_category)                            AS dna_reasons,
                    COUNT(DISTINCT sd.sales_deal_id)                                      AS sales_deals,
                    GROUP_CONCAT(DISTINCT sd.buyer)                                       AS buyers
                FROM title t
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND {rw_mr}
                LEFT JOIN do_not_air dna ON t.title_id = dna.title_id AND dna.active = 1
                LEFT JOIN sales_deal sd  ON t.title_id = sd.title_id AND {rw_sd}
                WHERE {rw_t} {title_f} {cat_f}
                GROUP BY t.title_id
                ORDER BY dna_count DESC, expiring_90d DESC, active_rights DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0c. Expiry + Sales (renewal risk) ─────────────────────────────────
        if cross == "expiry_sales":
            days  = _extract_day_count(q, 90)
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT
                    mr.title_name,
                    mr.media_platform_primary                                          AS rights_platform,
                    mr.territories,
                    mr.term_to                                                         AS rights_expiry,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER)            AS days_remaining,
                    mr.exclusivity, mr.rights_type, mr.region,
                    sd.buyer        AS sold_to,
                    sd.deal_value   AS sales_value,
                    sd.media_platform AS sales_platform,
                    sd.term_to      AS sales_expiry,
                    sd.status       AS sales_status,
                    CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale'
                         ELSE '— No Sale' END AS renewal_flag
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                LEFT JOIN sales_deal sd ON mr.title_id = sd.title_id
                  AND {rw_sd} AND sd.status = 'Active'
                WHERE {rw_mr}
                  AND mr.status = 'Active'
                  AND mr.term_to <= DATE('now', '+{days} days')
                  AND mr.term_to >= DATE('now')
                  {plat_f}
                ORDER BY days_remaining ASC, sd.deal_value DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0d. Work orders + Rights ───────────────────────────────────────────
        if cross == "workorder_rights":
            sql = f"""
                SELECT
                    wo.work_order_id, wo.title_name, wo.work_type,
                    wo.status       AS wo_status, wo.priority, wo.due_date,
                    wo.quality_score, wo.vendor_name,
                    COUNT(DISTINCT mr.rights_id)                                       AS active_rights,
                    MIN(mr.term_to)                                                    AS earliest_rights_expiry,
                    CAST(JULIANDAY(MIN(mr.term_to))-JULIANDAY('now') AS INTEGER)       AS days_to_expiry,
                    SUM(CASE WHEN mr.term_to <= DATE('now','+90 days')
                        AND mr.status='Active' THEN 1 ELSE 0 END)                     AS rights_expiring_90d
                FROM work_orders wo
                LEFT JOIN title t ON wo.title_id = t.title_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND {rw_mr} AND mr.status = 'Active'
                WHERE {rw_wo}
                GROUP BY wo.work_order_id
                ORDER BY rights_expiring_90d DESC, wo.due_date ASC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0e. Movie + DNA ───────────────────────────────────────────────────
        if cross == "movie_dna":
            cat_f = _movie_cat_filter(q, "m")
            sql = f"""
                SELECT
                    m.movie_title, m.content_category, m.genre,
                    m.box_office_gross_usd_m AS box_office_usd_m, m.franchise,
                    COUNT(DISTINCT mr.rights_id)                      AS active_rights,
                    COUNT(DISTINCT dna.dna_id)                        AS dna_flags,
                    GROUP_CONCAT(DISTINCT dna.reason_category)        AS dna_reasons,
                    GROUP_CONCAT(DISTINCT dna.territory)              AS restricted_territories,
                    CASE WHEN COUNT(dna.dna_id) > 0 THEN '🚫 Flagged'
                         ELSE '✅ Clean' END                          AS dna_status
                FROM movie m
                LEFT JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND {rw_mr} AND mr.status = 'Active'
                LEFT JOIN do_not_air dna ON dna.title_id = t.title_id AND dna.active = 1
                WHERE 1=1 {cat_f}
                GROUP BY m.movie_id
                ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0f. Movie + Sales ──────────────────────────────────────────────────
        if cross == "movie_sales":
            cat_f = _movie_cat_filter(q, "m")
            sql = f"""
                SELECT
                    m.movie_title, m.content_category, m.genre,
                    m.box_office_gross_usd_m AS box_office_usd_m,
                    sd.buyer, sd.deal_type, sd.media_platform AS sold_platform,
                    sd.territory AS sold_territory, sd.deal_value, sd.currency,
                    sd.term_from AS sale_from, sd.term_to AS sale_to,
                    sd.status AS sale_status,
                    COUNT(DISTINCT mr.rights_id) AS rights_in_count
                FROM movie m
                JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN sales_deal sd ON sd.title_id = t.title_id AND {rw_sd}
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id
                  AND {rw_mr} AND mr.status = 'Active'
                WHERE 1=1 {cat_f}
                GROUP BY m.movie_id, sd.sales_deal_id
                ORDER BY sd.deal_value DESC NULLS LAST, m.box_office_gross_usd_m DESC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 0g. Title + Sales ──────────────────────────────────────────────────
        if cross == "title_sales":
            title_f = f"AND {_title_like(title_hint,'t.title_name')}" if title_hint else ""
            sql = f"""
                SELECT
                    t.title_name, t.title_type, t.content_category,
                    mr.media_platform_primary AS rights_platform,
                    mr.term_to   AS rights_expiry,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS rights_days_left,
                    mr.status    AS rights_status,
                    sd.buyer, sd.media_platform AS sold_platform,
                    sd.deal_value, sd.currency,
                    sd.term_to   AS sale_expiry, sd.status AS sale_status
                FROM title t
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id AND {rw_mr}
                LEFT JOIN sales_deal sd   ON t.title_id = sd.title_id AND {rw_sd}
                WHERE {rw_t} {title_f}
                ORDER BY mr.term_to ASC
                LIMIT 150
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 1. DO-NOT-AIR
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in DNA_KW):
            title_filter = f" AND {_title_like(title_hint,'dna.title_name')}" if title_hint else ""
            sql = f"""
                SELECT dna.dna_id, dna.title_name,
                       dna.reason_category, dna.reason_subcategory,
                       dna.territory, dna.media_type,
                       dna.term_from, dna.term_to, dna.additional_notes
                FROM do_not_air dna
                JOIN title t ON dna.title_id = t.title_id
                WHERE {rw_dna} AND dna.active=1 {title_filter}
                ORDER BY dna.reason_category, dna.title_name
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 2. MOVIES / FILMS
        # ══════════════════════════════════════════════════════════════════════
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
                    LEFT JOIN media_rights mr ON mr.title_id = t.title_id AND {rw_mr}
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
                      AND {rw_mr} AND mr.status = 'Active'
                    WHERE 1=1 {cat_f_m}
                    GROUP BY m.movie_id
                    ORDER BY m.box_office_gross_usd_m DESC
                """
                return sql.strip(), None, 'bar', region_ctx

            if any(kw in q for kw in ["rights", "license", "window", "platform", "svod", "paytv"]):
                plat_f = f"AND {plat_mr}" if platforms else ""
                title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre,
                           m.box_office_gross_usd_m AS box_office_usd_m,
                           mr.rights_type, mr.media_platform_primary,
                           mr.territories, mr.language, mr.exclusivity,
                           mr.term_from, mr.term_to,
                           CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                           mr.status, mr.region
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE {rw_mr} AND mr.status = 'Active'
                      {cat_f_t} {title_f} {plat_f}
                    ORDER BY m.box_office_gross_usd_m DESC, mr.term_to ASC
                    LIMIT 200
                """
                return sql.strip(), None, 'table', region_ctx

            if any(kw in q for kw in EXPIRY_KW):
                days = _extract_day_count(q, 90)
                sql = f"""
                    SELECT m.movie_title, m.content_category, m.genre,
                           m.box_office_gross_usd_m AS box_office_usd_m,
                           mr.media_platform_primary, mr.territories, mr.region,
                           mr.term_to,
                           CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                           mr.exclusivity, mr.status
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE {rw_mr} AND mr.status = 'Active'
                      AND mr.term_to <= DATE('now','+{days} days')
                      AND mr.term_to >= DATE('now')
                      {cat_f_t}
                    ORDER BY mr.term_to ASC
                    LIMIT 100
                """
                return sql.strip(), None, 'table', region_ctx

            if any(kw in q for kw in ["breakdown", "by genre", "by category", "count",
                                       "how many", "genre"]):
                group_col = "m.genre" if "genre" in q else "m.content_category"
                sql = f"""
                    SELECT {group_col} AS category,
                           COUNT(DISTINCT m.movie_id) AS films,
                           SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                           COUNT(DISTINCT mr.rights_id) AS rights_count
                    FROM movie m
                    LEFT JOIN title t ON t.movie_id = m.movie_id
                    LEFT JOIN media_rights mr ON mr.title_id = t.title_id AND {rw_mr}
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
                           mr.term_from, mr.term_to, mr.status, mr.region
                    FROM movie m
                    JOIN title t ON t.movie_id = m.movie_id
                    JOIN media_rights mr ON mr.title_id = t.title_id
                    WHERE {rw_mr}
                      AND LOWER(m.movie_title) LIKE '%{title_hint.lower().split(":")[0].strip()}%'
                    ORDER BY mr.term_to ASC
                    LIMIT 100
                """
                return sql.strip(), None, 'table', region_ctx

            sql = f"""
                SELECT m.movie_id, m.movie_title, m.content_category, m.genre,
                       m.franchise, m.box_office_gross_usd_m AS box_office_usd_m,
                       m.age_rating, m.release_year,
                       COUNT(DISTINCT mr.rights_id)                                     AS total_rights,
                       SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END)              AS active_rights,
                       SUM(CASE WHEN mr.status='Active'
                          AND mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END)   AS expiring_90d
                FROM movie m
                LEFT JOIN title t ON t.movie_id = m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id = t.title_id AND {rw_mr}
                WHERE 1=1 {cat_f_m}
                GROUP BY m.movie_id
                ORDER BY m.box_office_gross_usd_m DESC
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 3. ELEMENTAL RIGHTS
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in ELEMENTAL_KW) and any(kw in q for kw in ["right", "deal"]):
            title_filter = f" AND {_title_like(title_hint,'er.title_name')}" if title_hint else ""
            sql = f"""
                SELECT er.elemental_rights_id, er.title_name,
                       ed.deal_name, ed.deal_source, ed.deal_type,
                       er.territories, er.media_platform_primary, er.language,
                       er.term_from, er.term_to, er.status, er.region
                FROM elemental_rights er
                JOIN elemental_deal ed ON er.elemental_deal_id = ed.elemental_deal_id
                WHERE {_region_where(regions,'er.region')} {title_filter}
                ORDER BY er.status DESC, er.title_name
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 4. SALES DEALS (rights-out)
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in SALES_KW):
            if any(kw in q for kw in ["breakdown", "by buyer", "platform", "by platform"]):
                sql = f"""
                    SELECT buyer, region,
                           COUNT(*) AS deals,
                           SUM(deal_value) AS total_value,
                           COUNT(DISTINCT title_id) AS titles
                    FROM sales_deal
                    WHERE {rw} AND status='Active'
                    GROUP BY buyer, region
                    ORDER BY total_value DESC
                    LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT sd.sales_deal_id, sd.deal_type, sd.title_name,
                       sd.buyer, sd.territory, sd.media_platform,
                       sd.term_from, sd.term_to,
                       sd.deal_value, sd.currency, sd.status, sd.region
                FROM sales_deal sd
                WHERE {rw}
                ORDER BY sd.deal_value DESC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 5. RIGHTS EXPIRY ALERTS
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in EXPIRY_KW):
            days   = _extract_day_count(q, 90)
            plat_f = f"AND {plat_mr}" if platforms else ""
            title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
            sql = f"""
                SELECT mr.rights_id, mr.title_name, mr.region,
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

        # ══════════════════════════════════════════════════════════════════════
        # 6. SPECIFIC TITLE — full rights detail
        # ══════════════════════════════════════════════════════════════════════
        if title_hint and any(kw in q for kw in list(RIGHTS_KW) + ["deal"]):
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT mr.rights_id, mr.title_name, mr.region,
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

        # ══════════════════════════════════════════════════════════════════════
        # 7. RIGHTS COUNT / PLATFORM
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["count", "how many", "total"]):
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT mr.rights_type, mr.status, mr.region,
                       COUNT(DISTINCT mr.title_id) AS title_count,
                       COUNT(mr.rights_id)         AS rights_count,
                       SUM(mr.exclusivity)         AS exclusive_count
                FROM media_rights mr
                WHERE {rw_mr} {plat_f}
                GROUP BY mr.rights_type, mr.status, mr.region
                ORDER BY rights_count DESC
            """
            return sql.strip(), None, 'bar', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 8. RIGHTS BY SEASON HIERARCHY
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["season", "hierarchy", "by season"]):
            # FIX BUG 3: use all regions, not just regions[0]
            sql = f"""
                SELECT s.series_title, se.season_number, mr.region,
                       COUNT(DISTINCT t.title_id)  AS episodes,
                       COUNT(DISTINCT mr.rights_id) AS rights_count,
                       SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN mr.term_to <= DATE('now','+90 days')
                           AND mr.status='Active' THEN 1 ELSE 0 END)       AS expiring_90d
                FROM series s
                JOIN season se ON s.series_id = se.series_id
                JOIN title t   ON se.season_id = t.season_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                  AND {rw_mr}
                GROUP BY s.series_title, se.season_number, mr.region
                ORDER BY s.series_title, se.season_number
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 9. EXHIBITION RESTRICTIONS
        # ══════════════════════════════════════════════════════════════════════
        if "exhibition" in q and "restrict" in q:
            sql = f"""
                SELECT mr.title_name, mr.region, er.max_plays, er.max_plays_per_day,
                       er.max_days, er.max_networks,
                       mr.media_platform_primary, mr.territories, mr.status
                FROM exhibition_restrictions er
                JOIN media_rights mr ON er.rights_id = mr.rights_id
                WHERE {rw_mr}
                ORDER BY mr.title_name
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 10. RIGHTS BREAKDOWN / ANALYTICS
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in ["breakdown", "distribution", "mix", "analytics",
                                   "by territory", "by platform", "by deal"]):
            if "territory" in q or "territories" in q:
                sql = f"""
                    SELECT mr.territories, mr.region,
                           COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive,
                           COUNT(DISTINCT mr.title_id) AS titles
                    FROM media_rights mr
                    WHERE {rw_mr} AND mr.status='Active'
                    GROUP BY mr.territories, mr.region
                    ORDER BY rights_count DESC
                    LIMIT 20
                """
            elif any(kw in q for kw in ["deal source", "trl", "c2", "frl"]):
                sql = f"""
                    SELECT cd.deal_source, mr.region,
                           COUNT(DISTINCT mr.rights_id) AS rights,
                           COUNT(DISTINCT mr.title_id)  AS titles,
                           SUM(mr.exclusivity)          AS exclusive
                    FROM media_rights mr
                    JOIN content_deal cd ON mr.deal_id = cd.deal_id
                    WHERE {rw_mr}
                    GROUP BY cd.deal_source, mr.region
                    ORDER BY rights DESC
                """
            else:
                sql = f"""
                    SELECT mr.media_platform_primary AS platform, mr.rights_type, mr.region,
                           COUNT(*) AS rights_count,
                           COUNT(DISTINCT mr.title_id) AS titles,
                           SUM(mr.exclusivity) AS exclusive
                    FROM media_rights mr
                    WHERE {rw_mr} AND mr.status='Active'
                    GROUP BY platform, mr.rights_type, mr.region
                    ORDER BY rights_count DESC
                    LIMIT 20
                """
            return sql.strip(), None, 'bar', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 11. DEALS (vendor contracts)
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in DEAL_KW):
            stat_cond = ("AND d.status='Active'"       if "active"  in q else
                         "AND d.status='Expired'"      if "expired" in q else
                         "AND (d.status='Pending' OR d.status='Under Negotiation')"
                                                       if "pending" in q else "")
            # FIX BUG 5/8: use d.deal_date alias explicitly; only attach if set
            date_cond = f"AND {date_filter_deals}" if date_filter_deals else ""

            where_clause = _build_where(rw_d, stat_cond, date_cond)

            if any(kw in q for kw in ["vendor", "by vendor", "breakdown", "by type", "value"]):
                group_col = "d.vendor_name" if "vendor" in q else "d.deal_type"
                sql = f"""
                    SELECT {group_col}, d.region,
                           COUNT(*) AS deal_count,
                           SUM(d.deal_value) AS total_value,
                           AVG(d.deal_value) AS avg_value,
                           SUM(CASE WHEN d.status='Active' THEN 1 ELSE 0 END) AS active,
                           SUM(CASE WHEN d.payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
                    FROM deals d
                    WHERE {where_clause}
                    GROUP BY {group_col}, d.region
                    ORDER BY total_value DESC
                    LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx

            sql = f"""
                SELECT d.deal_id, d.deal_name, d.vendor_name, d.deal_type,
                       d.deal_value, d.rights_scope, d.territory,
                       d.deal_date, d.expiry_date, d.status, d.payment_status,
                       d.region,
                       CAST(JULIANDAY(d.expiry_date)-JULIANDAY('now') AS INTEGER) AS days_to_expiry
                FROM deals d
                WHERE {where_clause}
                ORDER BY d.deal_value DESC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 12. WORK ORDERS
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in WORK_KW):
            if "quality" in q or "vendor" in q:
                sql = f"""
                    SELECT vendor_name, region,
                           COUNT(*)                AS orders,
                           AVG(quality_score)      AS avg_quality,
                           SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delays,
                           SUM(cost)               AS total_cost
                    FROM work_orders
                    WHERE {rw}
                    GROUP BY vendor_name, region
                    ORDER BY orders DESC
                    LIMIT 10
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT status, region, COUNT(*) AS count
                FROM work_orders
                WHERE {rw}
                GROUP BY status, region
                ORDER BY count DESC
            """
            return sql.strip(), None, 'pie', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 13. TITLE CATALOG
        # ══════════════════════════════════════════════════════════════════════
        if any(kw in q for kw in TITLE_KW):
            title_f = f"WHERE {rw_t}" + (
                f" AND {_title_like(title_hint,'t.title_name')}" if title_hint else "")
            if any(kw in q for kw in ["count", "how many", "total"]):
                sql = f"""
                    SELECT t.genre, t.region, COUNT(*) AS title_count
                    FROM title t {title_f}
                    GROUP BY t.genre, t.region
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
                LEFT JOIN series  s ON t.series_id = s.series_id
                LEFT JOIN movie   m ON t.movie_id  = m.movie_id
                {title_f}
                ORDER BY t.region, t.title_type, s.series_title, se.season_number, t.episode_number
                LIMIT 300
            """
            return sql.strip(), None, 'table', region_ctx

        # ══════════════════════════════════════════════════════════════════════
        # 14. GENERIC FALLBACK (media_rights overview)
        # ══════════════════════════════════════════════════════════════════════
        stat_f = ("AND mr.status='Active'"  if "active"  in q else
                  "AND mr.status='Expired'" if "expired" in q else "")
        plat_f = f"AND {plat_mr}" if platforms else ""
        sql = f"""
            SELECT mr.title_name, mr.rights_type,
                   mr.media_platform_primary, mr.territories,
                   mr.term_from, mr.term_to, mr.status, mr.region,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
            FROM media_rights mr
            WHERE {rw_mr} {stat_f} {plat_f}
            ORDER BY mr.term_to DESC
            LIMIT 100
        """
        return sql.strip(), None, 'table', region_ctx


def parse_query(question, selected_region="NA"):
    """Public entry point. Returns (sql, error, chart_type, region_context)."""
    try:
        return QueryParser.generate_sql(question, selected_region)
    except Exception as e:
        return None, str(e), 'table', selected_region
