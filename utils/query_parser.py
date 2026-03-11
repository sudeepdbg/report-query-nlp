"""
query_parser.py — Rights Explorer NL→SQL
Covers all five query intents from the MVP spec:
  1. what titles do we have (title catalog)
  2. what titles do we have rights to (titles with active media_rights)
  3. what rights do we hold for specific titles/deals (rights detail)
  4. rights expiry / renewal alerts
  5. do-not-air / elemental / sales deal queries
  + operational: vendors, work orders

Ontology: maps synonymous user terms to canonical platform/media enums.
"""
import re
from typing import Tuple, Optional, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Ontology / glossary ──────────────────────────────────────────────────────
ONTOLOGY: Dict[str, str] = {
    # Platform synonyms → canonical
    "streaming":        "SVOD",
    "subscription":     "SVOD",
    "svod":             "SVOD",
    "fast":             "FAST",
    "free stream":      "FAST",
    "ad-supported":     "FAST",
    "avod":             "FAST",
    "pay tv":           "PayTV",
    "paytv":            "PayTV",
    "cable":            "PayTV",
    "premium tv":       "PayTV",
    "stb":              "STB-VOD",
    "stb-vod":          "STB-VOD",
    "vod":              "STB-VOD",
    "catchup":          "CatchUp",
    "catch-up":         "CatchUp",
    "catch up":         "CatchUp",
    "startover":        "StartOver",
    "start-over":       "StartOver",
    "simulcast":        "Simulcast",
    "download":         "TempDownload",
    "tempdownload":     "TempDownload",
    "temp download":    "TempDownload",
    "download to own":  "DownloadToOwn",
    "dto":              "DownloadToOwn",
    "tvod":             "DownloadToOwn",
    # Rights type synonyms
    "exhibition":       "Exhibition",
    "distribution":     "Exhibition & Distribution",
    # Region synonyms
    "us":               "NA",
    "usa":              "NA",
    "north america":    "NA",
    "canada":           "NA",
    "uk":               "EMEA",
    "europe":           "EMEA",
    "middle east":      "EMEA",
    "africa":           "EMEA",
    "apac":             "APAC",
    "asia":             "APAC",
    "pacific":          "APAC",
    "australia":        "APAC",
    "japan":            "APAC",
    "india":            "APAC",
    "latam":            "LATAM",
    "latin america":    "LATAM",
    "brazil":           "LATAM",
}

REGION_CANONICAL = {"NA","APAC","EMEA","LATAM"}
MEDIA_PRIMARY    = {"PayTV","STB-VOD","SVOD","FAST"}
MEDIA_ANCILLARY  = {"CatchUp","StartOver","Simulcast","TempDownload","DownloadToOwn"}
ALL_MEDIA        = MEDIA_PRIMARY | MEDIA_ANCILLARY

EXPIRY_KW = {"expir","renew","laps","soon","upcoming","due","alert","warning","90 day","30 day","60 day"}
DNA_KW    = {"do not air","do-not-air","dna","restricted","blocked","not air","cannot air"}
ELEMENTAL_KW = {"elemental","element","promo","edit","component"}
SALES_KW     = {"sales deal","rights out","rights-out","sold","affiliate","3rd party","buyer"}
TITLE_KW     = {"title","titles","show","shows","series","season","episode","episodes","catalog","library","what do we have"}
RIGHTS_KW    = {"rights","license","licensed","window","windows","hold","holds","have rights","rights to"}
DEAL_KW      = {"deal","deals","contract","contracts","agreement"}
WORK_KW      = {"work order","vendor","quality","operational","task","status","workload"}

def _apply_ontology(q: str) -> str:
    """Replace synonyms with canonical terms (longest match first)."""
    q_out = q
    for phrase in sorted(ONTOLOGY.keys(), key=len, reverse=True):
        if phrase in q_out:
            q_out = q_out.replace(phrase, ONTOLOGY[phrase])
    return q_out

def _extract_regions(q: str) -> List[str]:
    found = []
    for r in REGION_CANONICAL:
        if r in q.upper():
            found.append(r)
    return found

def _extract_platforms(q: str) -> List[str]:
    q_mapped = _apply_ontology(q.lower())
    return [p for p in ALL_MEDIA if p.lower() in q_mapped.lower() or p in q_mapped]

def _extract_title_hints(q: str) -> Optional[str]:
    """Try to find a quoted title or known series name."""
    quoted = re.findall(r'"([^"]+)"', q)
    if quoted:
        return quoted[0]
    known_series = [
        "House of the Dragon","The Last of Us","Succession","The White Lotus",
        "Euphoria","Westworld","Barry","True Detective","The Wire","The Sopranos",
        "The Penguin","Dune: Prophecy","The Bear","Andor","The Mandalorian",
        "Foundation","Shrinking","Reacher","The Boys","Squid Game",
    ]
    for s in sorted(known_series, key=len, reverse=True):
        if s.lower() in q.lower():
            return s
    return None

def _region_where(regions, col="region"):
    if not regions:
        return "1=1"
    if len(regions) == 1:
        return f"UPPER({col}) = '{regions[0]}'"
    rl = "','".join(regions)
    return f"UPPER({col}) IN ('{rl}')"

def _platform_like(platforms: List[str], col: str) -> str:
    if not platforms:
        return "1=1"
    parts = [f"{col} LIKE '%{p}%'" for p in platforms]
    return "(" + " OR ".join(parts) + ")"

def _title_like(hint: str, col: str = "title_name") -> str:
    return f"LOWER({col}) LIKE '%{hint.lower()}%'"


class QueryParser:
    @classmethod
    def generate_sql(cls, question: str, selected_region: str
                     ) -> Tuple[Optional[str], Optional[str], str, str]:
        q        = question.lower().strip()

        # Sidebar region is the primary context.
        # Query text may mention a different region explicitly — honour it only
        # if it differs from the sidebar AND the query clearly refers to that
        # region as the target (e.g. "in APAC", "for EMEA"). Otherwise sidebar wins.
        text_regions = _extract_regions(q)
        if text_regions and text_regions != [selected_region]:
            # Only override if user explicitly wrote "in X" or "for X" or "X region"
            explicit = any(f"in {r.lower()}" in q or f"for {r.lower()}" in q
                           or f"{r.lower()} region" in q
                           for r in text_regions)
            regions = text_regions if explicit else [selected_region]
        else:
            regions = [selected_region]

        region_ctx = " vs ".join(regions)
        rw       = _region_where(regions)
        rw_mr    = _region_where(regions, "mr.region")
        rw_t     = _region_where(regions, "t.region")
        platforms = _extract_platforms(q)
        plat_mr  = _platform_like(platforms, "mr.media_platform_primary") if platforms else "1=1"
        title_hint = _extract_title_hints(question)

        # ── 1. Do-Not-Air queries ─────────────────────────────────────────────
        if any(kw in q for kw in DNA_KW):
            title_filter = f" AND {_title_like(title_hint,'dna.title_name')}" if title_hint else ""
            sql = f"""
                SELECT dna.dna_id, dna.title_name, t.series_id,
                       dna.reason_category, dna.reason_subcategory,
                       dna.territory, dna.media_type,
                       dna.term_from, dna.term_to, dna.additional_notes
                FROM do_not_air dna
                JOIN title t ON dna.title_id = t.title_id
                WHERE {_region_where(regions,'dna.region')} AND dna.active=1
                  {title_filter}
                ORDER BY dna.reason_category, dna.title_name
                LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 2. Elemental rights ────────────────────────────────────────────────
        if any(kw in q for kw in ELEMENTAL_KW) and any(kw in q for kw in ["right","deal"]):
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

        # ── 3. Sales deal / rights-out ─────────────────────────────────────────
        if any(kw in q for kw in SALES_KW):
            if any(kw in q for kw in ["breakdown","by buyer","platform","by platform"]):
                sql = f"""
                    SELECT buyer, COUNT(*) AS deals, SUM(deal_value) AS total_value,
                           COUNT(DISTINCT title_id) AS titles
                    FROM sales_deal WHERE {rw} AND status='Active'
                    GROUP BY buyer ORDER BY total_value DESC LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT sd.sales_deal_id, sd.deal_type, sd.title_name, sd.buyer,
                       sd.territory, sd.media_platform, sd.term_from, sd.term_to,
                       sd.deal_value, sd.currency, sd.status
                FROM sales_deal sd WHERE {rw}
                ORDER BY sd.deal_value DESC LIMIT 100
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 4. Rights expiry alerts ────────────────────────────────────────────
        if any(kw in q for kw in EXPIRY_KW):
            days  = 90
            for d in re.findall(r'\b(\d+)\s*day', q):
                days = int(d); break
            plat_f = f"AND {plat_mr}" if platforms else ""
            title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
            sql = f"""
                SELECT mr.rights_id, mr.title_name, t.series_id,
                       cd.deal_name, cd.deal_source,
                       mr.rights_type, mr.media_platform_primary,
                       mr.territories, mr.language, mr.exclusivity,
                       mr.term_to AS expiry_date,
                       CAST(JULIANDAY(mr.term_to) - JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.holdback, mr.holdback_days,
                       mr.notes_restrictive, mr.status
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                JOIN title t ON mr.title_id = t.title_id
                WHERE {rw_mr} AND mr.status='Active'
                  AND mr.term_to <= DATE('now','+{days} days')
                  AND mr.term_to >= DATE('now')
                  {plat_f} {title_f}
                ORDER BY mr.term_to ASC
                LIMIT 200
            """
            return sql.strip(), None, 'bar', region_ctx

        # ── 5. "What rights do we hold for specific title / deal" ─────────────
        if title_hint and any(kw in q for kw in RIGHTS_KW | DEAL_KW):
            plat_f = f"AND {plat_mr}" if platforms else ""
            sql = f"""
                SELECT mr.rights_id,
                       t.title_name, t.series_id, t.episode_number,
                       cd.deal_name, cd.deal_source, cd.deal_type,
                       mr.rights_type,
                       mr.media_platform_primary, mr.media_platform_ancillary,
                       mr.territories, mr.language, mr.brand,
                       mr.exclusivity, mr.holdback, mr.holdback_days,
                       mr.term_from, mr.term_to,
                       CAST(JULIANDAY(mr.term_to) - JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.notes_general, mr.notes_restrictive, mr.status
                FROM media_rights mr
                JOIN content_deal cd ON mr.deal_id = cd.deal_id
                JOIN title t ON mr.title_id = t.title_id
                WHERE {_title_like(title_hint,'t.title_name')} {plat_f}
                ORDER BY mr.term_from ASC
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 6. "What titles do we have rights to" ─────────────────────────────
        if (any(kw in q for kw in RIGHTS_KW) or any(kw in q for kw in TITLE_KW)):
            plat_f   = f"AND {plat_mr}" if platforms else ""
            excl_f   = "AND mr.exclusivity=1" if "exclusiv" in q else ""
            hold_f   = "AND mr.holdback=1"    if "holdback" in q else ""
            stat_f   = "AND mr.status='Active'" if any(x in q for x in ["active","current","live"]) else \
                       "AND mr.status='Expired'" if "expired" in q else ""

            if any(kw in q for kw in ["count","how many","total","number"]):
                sql = f"""
                    SELECT mr.media_platform_primary AS platform,
                           mr.rights_type,
                           COUNT(DISTINCT mr.title_id) AS title_count,
                           COUNT(*) AS rights_count
                    FROM media_rights mr WHERE {rw_mr} {plat_f} {excl_f} {stat_f}
                    GROUP BY mr.media_platform_primary, mr.rights_type
                    ORDER BY title_count DESC
                """
                return sql.strip(), None, 'bar', region_ctx

            if any(kw in q for kw in ["by season","season","hierarchy","series"]):
                sql = f"""
                    SELECT s.series_title,
                           se.season_number,
                           COUNT(DISTINCT t.title_id) AS episodes_with_rights,
                           GROUP_CONCAT(DISTINCT mr.media_platform_primary) AS platforms,
                           MIN(mr.term_from) AS earliest_from,
                           MAX(mr.term_to)   AS latest_to,
                           SUM(mr.exclusivity) AS exclusive_count,
                           mr.status
                    FROM media_rights mr
                    JOIN title t   ON mr.title_id   = t.title_id
                    JOIN season se ON t.season_id   = se.season_id
                    JOIN series s  ON se.series_id  = s.series_id
                    WHERE {rw_mr} {plat_f} {excl_f} {stat_f}
                    GROUP BY s.series_title, se.season_number, mr.status
                    ORDER BY s.series_title, se.season_number
                    LIMIT 200
                """
                return sql.strip(), None, 'table', region_ctx

            # Default: list of titles with rights
            sql = f"""
                SELECT DISTINCT
                       t.title_name, t.title_type, t.genre, t.release_year,
                       t.controlling_entity,
                       mr.rights_type,
                       mr.media_platform_primary, mr.media_platform_ancillary,
                       mr.territories, mr.language, mr.brand,
                       mr.exclusivity, mr.term_from, mr.term_to,
                       CAST(JULIANDAY(mr.term_to) - JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.status
                FROM media_rights mr
                JOIN title t ON mr.title_id = t.title_id
                WHERE {rw_mr} {plat_f} {excl_f} {hold_f} {stat_f}
                ORDER BY t.title_name, mr.term_from
                LIMIT 300
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 7. Exhibition restrictions ─────────────────────────────────────────
        if any(kw in q for kw in ["restriction","restriction","max play","play limit","stacking","catchup"]):
            title_f = f"AND {_title_like(title_hint,'mr.title_name')}" if title_hint else ""
            sql = f"""
                SELECT mr.title_name, cd.deal_name, cd.deal_source,
                       er.max_plays, er.max_plays_per_day, er.max_days, er.max_networks,
                       er.restriction_term_from, er.restriction_term_to,
                       er.additional_notes
                FROM exhibition_restrictions er
                JOIN media_rights mr ON er.rights_id = mr.rights_id
                JOIN content_deal cd ON er.deal_id   = cd.deal_id
                WHERE {_region_where(regions,'mr.region')} {title_f}
                ORDER BY mr.title_name
                LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 8. Rights breakdown analytics ─────────────────────────────────────
        if any(kw in q for kw in ["breakdown","distribution","mix","by platform","by territory",
                                   "overview","landscape","summary"]):
            if any(kw in q for kw in ["territory","country","countries","geo"]):
                sql = f"""
                    SELECT mr.territories,
                           COUNT(DISTINCT mr.title_id) AS titles,
                           COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive,
                           SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active,
                           SUM(CASE WHEN mr.status='Expired' THEN 1 ELSE 0 END) AS expired
                    FROM media_rights mr WHERE {rw_mr}
                    GROUP BY mr.territories ORDER BY rights_count DESC LIMIT 30
                """
            elif any(kw in q for kw in ["platform","window","media"]):
                sql = f"""
                    SELECT mr.media_platform_primary AS platform,
                           mr.status,
                           COUNT(DISTINCT mr.title_id) AS title_count,
                           COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive,
                           SUM(mr.holdback) AS holdback_count
                    FROM media_rights mr WHERE {rw_mr}
                    GROUP BY mr.media_platform_primary, mr.status
                    ORDER BY title_count DESC
                """
            elif any(kw in q for kw in ["deal","source","trl","c2","frl"]):
                sql = f"""
                    SELECT cd.deal_source,
                           COUNT(DISTINCT mr.title_id) AS title_count,
                           COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive,
                           cd.deal_type
                    FROM media_rights mr
                    JOIN content_deal cd ON mr.deal_id = cd.deal_id
                    WHERE {rw_mr}
                    GROUP BY cd.deal_source, cd.deal_type
                    ORDER BY rights_count DESC
                """
            else:
                sql = f"""
                    SELECT mr.rights_type,
                           mr.status,
                           COUNT(DISTINCT mr.title_id) AS title_count,
                           COUNT(*) AS rights_count,
                           SUM(mr.exclusivity) AS exclusive_count,
                           SUM(mr.holdback) AS holdback_count
                    FROM media_rights mr WHERE {rw_mr}
                    GROUP BY mr.rights_type, mr.status
                    ORDER BY title_count DESC
                """
            return sql.strip(), None, 'bar', region_ctx

        # ── 9. Deals table queries ────────────────────────────────────────────
        if any(kw in q for kw in DEAL_KW) and not any(kw in q for kw in RIGHTS_KW):
            stat_f  = "AND status='Active'" if "active" in q else \
                      "AND status='Expired'" if "expired" in q else \
                      "AND (status='Pending' OR status='Under Negotiation')" if "pending" in q else ""
            if any(kw in q for kw in ["vendor","by vendor","breakdown","by type","value","source"]):
                group_col = "vendor_name" if "vendor" in q else "deal_type"
                sql = f"""
                    SELECT {group_col}, COUNT(*) AS deal_count,
                           SUM(deal_value) AS total_value,
                           AVG(deal_value) AS avg_value,
                           SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
                           SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
                    FROM deals WHERE UPPER(region)='{regions[0]}' {stat_f}
                    GROUP BY {group_col} ORDER BY total_value DESC LIMIT 15
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT deal_id, deal_name, vendor_name, deal_type,
                       deal_value, rights_scope, territory,
                       deal_date, expiry_date, status, payment_status,
                       CAST(JULIANDAY(expiry_date)-JULIANDAY('now') AS INTEGER) AS days_to_expiry
                FROM deals WHERE UPPER(region)='{regions[0]}' {stat_f}
                ORDER BY deal_value DESC LIMIT 200
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 10. Work orders / operational ─────────────────────────────────────
        if any(kw in q for kw in WORK_KW):
            if "quality" in q or "vendor" in q:
                sql = f"""
                    SELECT vendor_name, COUNT(*) AS orders,
                           AVG(quality_score) AS avg_quality,
                           SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delays,
                           SUM(cost) AS total_cost
                    FROM work_orders WHERE {rw}
                    GROUP BY vendor_name ORDER BY orders DESC LIMIT 10
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT status, COUNT(*) AS count
                FROM work_orders WHERE {rw}
                GROUP BY status ORDER BY count DESC
            """
            return sql.strip(), None, 'pie', region_ctx

        # ── 11. Title catalog (what titles do we have) ─────────────────────────
        if any(kw in q for kw in TITLE_KW):
            title_f = f"WHERE {_region_where(regions,'t.region')}" + (
                f" AND {_title_like(title_hint,'t.title_name')}" if title_hint else "")
            if any(kw in q for kw in ["count","how many","total"]):
                sql = f"""
                    SELECT t.genre, COUNT(*) AS title_count
                    FROM title t {title_f}
                    GROUP BY t.genre ORDER BY title_count DESC
                """
                return sql.strip(), None, 'bar', region_ctx
            sql = f"""
                SELECT t.title_id, t.title_name, t.title_type, t.genre,
                       t.release_year, t.controlling_entity,
                       t.age_rating, t.runtime_minutes, t.region,
                       s.series_title, se.season_number
                FROM title t
                LEFT JOIN season se ON t.season_id  = se.season_id
                LEFT JOIN series s  ON t.series_id  = s.series_id
                {title_f}
                ORDER BY s.series_title, se.season_number, t.episode_number
                LIMIT 300
            """
            return sql.strip(), None, 'table', region_ctx

        # ── 12. Generic fallback — most recent active rights ───────────────────
        stat_f = "AND mr.status='Active'" if "active" in q else \
                 "AND mr.status='Expired'" if "expired" in q else ""
        sql = f"""
            SELECT mr.title_name, mr.rights_type,
                   mr.media_platform_primary, mr.territories,
                   mr.term_from, mr.term_to, mr.status,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
            FROM media_rights mr
            WHERE {rw_mr} {stat_f}
            ORDER BY mr.term_to DESC LIMIT 100
        """
        return sql.strip(), None, 'table', region_ctx


def parse_query(question: str, region: str) -> Tuple[Optional[str], Optional[str], str, str]:
    return QueryParser.generate_sql(question, region)
