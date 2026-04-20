"""
query_pipeline.py — Foundry Vantage NL Query Pipeline  v5.2
FIXES v5.2:
  1. deals_rights / DEAL_KW: removed bad JOIN deals→media_rights on d.title_id
     (`deals` table has NO title_id). All "deals+rights" queries now go via content_deal.
  2. Ollama: prompt shortened drastically (llama3.2:1b chokes on long prompts).
     Timeout raised 15s→30s. LLM_LAST_STATUS dict exposes success/error to app.py.
  3. parse_with_llm: validates each LLM field before accepting it (prevents garbage override).
  4. match_method never overwritten after successful LLM call (v5.1 fix kept).
"""
from __future__ import annotations
import re, json, logging
import requests
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ========== LLM Config ==========
OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2:1b"
USE_LLM      = True
# app.py reads this dict to show a live LLM status indicator in the sidebar
LLM_LAST_STATUS: dict = {"success": None, "error": ""}

# ========== Vocabulary ==========
DNA_KW       = {"do not air","do-not-air","dna","restrict","banned","blocked","not allowed"}
ELEMENTAL_KW = {"elemental","element","promo","trailer","edit","featurette","asset","raw"}
SALES_KW     = {"sales deal","rights out","rights-out","sold","affiliate","3rd party","buyer"}
EXPIRY_KW    = {"expir","renew","laps","upcoming","due","alert","soon","days left","days remaining"}
WORK_KW      = {"work order","quality","task","workload"}
DEAL_KW      = {"deal","deals","contract","contracts","agreement"}
RIGHTS_KW    = {"rights","license","licensed","window","windows","hold","holds","have rights","rights to"}
TITLE_KW     = {"title","titles","show","shows","series","season","episode","episodes","catalog","what do we have"}

MOVIE_TITLES = {
    "dune","barbie","batman","oppenheimer","aquaman","wonka","beetlejuice","furiosa",
    "shazam","tenet","godzilla","matrix","mortal kombat","suicide squad","wonder woman",
    "black adam","space jam","meg","elvis","the flash","the batman","the penguin",
    "white noise","the witches","color purple","animal kingdom",
}
MOVIE_VOCAB = {
    "movie","movies","film","films","feature","theatrical","cinema",
    "box office","box-office","film slate","movie slate",
    "dc film","dc movie","warner film","wbd film","wb film",
    "franchise film","library film","library title",
    "direct-to-streaming","dtv","hbo original film","hbo film",
}
MOVIE_KW = MOVIE_TITLES | MOVIE_VOCAB

REGION_CANONICAL = {"NA","APAC","EMEA","LATAM"}
ALL_MEDIA = ["PayTV","STB-VOD","SVOD","FAST","CatchUp","StartOver","Simulcast","TempDownload","DownloadToOwn"]

ONTOLOGY = {
    "streaming":"SVOD","subscription":"SVOD","cable":"PayTV",
    "ad-supported":"FAST","ad supported":"FAST","free tv":"FAST",
    "catch-up":"CatchUp","catch up":"CatchUp","download":"TempDownload",
    "uk":"EMEA","europe":"EMEA","asia":"APAC",
    "latin america":"LATAM","south america":"LATAM","north america":"NA",
    "united states":"NA","usa":"NA",
}

KNOWN_TITLES = [
    "House of the Dragon","The Last of Us","Succession","The White Lotus",
    "Euphoria","Westworld","Barry","True Detective","The Wire","The Sopranos",
    "The Penguin","Dune: Prophecy","The Bear","Andor","The Mandalorian",
    "Foundation","Shrinking","Reacher","The Boys","Squid Game",
    "Dune: Part One","Dune: Part Two","Barbie","Oppenheimer",
    "The Batman","Aquaman and the Lost Kingdom","The Flash","Black Adam",
    "Shazam! Fury of the Gods","Wonka","Beetlejuice Beetlejuice","Furiosa",
    "Meg 2: The Trench","The Color Purple","Elvis","Animal Kingdom",
    "White Noise","The Witches","Tenet","Wonder Woman 1984","Mortal Kombat",
    "The Suicide Squad","Matrix Resurrections","Space Jam: A New Legacy","Godzilla vs. Kong",
]

# ========== Dataclasses ==========
@dataclass
class DateFilter:
    kind: str; value: object; label: str; sql_fragment: str

@dataclass
class QueryIntent:
    raw_question: str; normalised: str
    regions: list[str]; platforms: list[str]
    title_hint: Optional[str]; date_filter: Optional[DateFilter]
    expiry_days: Optional[int]; status_filter: Optional[str]
    movie_category: Optional[str]; domain: str; cross_intent: Optional[str]
    has_movie: bool=False; has_rights: bool=False; has_dna: bool=False
    has_sales: bool=False; has_work: bool=False; has_expiry: bool=False
    has_title: bool=False; has_deal_word: bool=False; has_rights_word: bool=False
    chips: list[dict]=field(default_factory=list)
    match_method: str="rule"

# ========== Helpers ==========
def _apply_ontology(q):
    for p in sorted(ONTOLOGY, key=len, reverse=True):
        if p in q: q=q.replace(p,ONTOLOGY[p])
    return q

def _extract_regions(q):  return [r for r in REGION_CANONICAL if r in q.upper()]
def _extract_platforms(q):
    q2=_apply_ontology(q.lower()); return [p for p in ALL_MEDIA if p.lower() in q2 or p in q2]

def _extract_title_hint(question):
    quoted=re.findall(r'"([^"]+)"',question)
    if quoted: return quoted[0]
    for s in sorted(KNOWN_TITLES,key=len,reverse=True):
        if s.lower() in question.lower(): return s
    return None

def _extract_date_filter(q, date_col="d.deal_date"):
    m=re.search(r'last\s+(\d+)\s+days?',q)
    if m: n=int(m.group(1)); return DateFilter("last_days",n,f"Last {n} Days",f"{date_col} >= DATE('now','-{n} days')")
    m=re.search(r'last\s+(\d+)\s+weeks?',q)
    if m: n=int(m.group(1)); return DateFilter("last_weeks",n,f"Last {n} Weeks",f"{date_col} >= DATE('now','-{n*7} days')")
    m=re.search(r'last\s+(\d+)\s+months?',q)
    if m: n=int(m.group(1)); return DateFilter("last_months",n,f"Last {n} Months",f"{date_col} >= DATE('now','-{n*30} days')")
    m=re.search(r'(?:in|year)\s*(\d{4})',q)
    if m: y=int(m.group(1)); return DateFilter("year",y,f"Year {y}",f"{date_col} BETWEEN '{y}-01-01' AND '{y}-12-31'")
    m=re.search(r'between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})',q)
    if m: s,e=m.group(1),m.group(2); return DateFilter("between",(s,e),f"{s} → {e}",f"{date_col} BETWEEN '{s}' AND '{e}'")
    return None

def _extract_expiry_days(q):
    for d in re.findall(r'\b(\d+)\s*day',q): return int(d)
    return None

def _extract_status(q):
    if "active" in q: return "Active"
    if "expired" in q: return "Expired"
    if "pending" in q: return "Pending"
    return None

def _extract_movie_category(q):
    if "theatrical" in q and "library" not in q: return "Theatrical"
    if "library" in q: return "Library"
    if "hbo original" in q or "hbo film" in q: return "HBO Original"
    if "direct-to-streaming" in q or "dtv" in q: return "Direct-to-Streaming"
    return None

def _detect_cross_intent(i):
    if i.has_movie and i.has_dna: return "movie_dna"
    if i.has_movie and i.has_sales: return "movie_sales"
    if (i.has_movie or i.has_title) and i.has_rights and i.has_dna: return "title_health"
    if i.has_expiry and i.has_sales: return "expiry_sales"
    if i.has_work and (i.has_rights or i.has_title): return "workorder_rights"
    if i.has_title and i.has_sales: return "title_sales"
    if i.has_deal_word and i.has_rights_word: return "deals_rights"
    return None

def _detect_domain(q, intent):
    if any(kw in q for kw in DNA_KW): return "do_not_air"
    if intent.has_movie: return "movies"
    if any(kw in q for kw in ELEMENTAL_KW): return "elemental"
    if any(kw in q for kw in SALES_KW): return "sales"
    if any(kw in q for kw in EXPIRY_KW): return "expiry"
    if any(kw in q for kw in DEAL_KW): return "deals"
    if any(kw in q for kw in WORK_KW): return "work_orders"
    if any(kw in q for kw in TITLE_KW): return "titles"
    if any(kw in q for kw in RIGHTS_KW): return "rights"
    return "rights"

def _build_chips(intent):
    chips=[]
    dl={"rights":"Rights","deals":"Deals","movies":"Movies","sales":"Sales Deals",
        "do_not_air":"Do-Not-Air","expiry":"Expiry Alert","work_orders":"Work Orders",
        "titles":"Title Catalog","elemental":"Elemental Rights"}
    chips.append({"id":"domain","kind":"domain","label":"Domain",
        "value":dl.get(intent.domain,intent.domain.replace("_"," ").title()),"editable":False,"removable":False})
    if intent.cross_intent:
        cl={"deals_rights":"Content Deals + Rights","title_health":"Title Health",
            "expiry_sales":"Expiry + Renewal","workorder_rights":"Work Orders + Rights",
            "movie_dna":"Movie DNA","movie_sales":"Movie Sales","title_sales":"Title Sales"}
        chips.append({"id":"cross_intent","kind":"cross_intent","label":"Join",
            "value":cl.get(intent.cross_intent,intent.cross_intent),"editable":False,"removable":False})
    for r in intent.regions:
        chips.append({"id":f"region_{r}","kind":"region","label":"Region","value":r,"editable":False,"removable":True})
    for p in intent.platforms:
        chips.append({"id":f"platform_{p}","kind":"platform","label":"Platform","value":p,"editable":False,"removable":True})
    if intent.title_hint:
        chips.append({"id":"title","kind":"title","label":"Title","value":intent.title_hint,"editable":True,"removable":True})
    if intent.date_filter:
        chips.append({"id":"date","kind":"date","label":"Date","value":intent.date_filter.label,"editable":True,"removable":True})
    if intent.expiry_days:
        chips.append({"id":"expiry_days","kind":"expiry_days","label":"Expiry Window","value":f"{intent.expiry_days} days","editable":True,"removable":True})
    if intent.status_filter:
        chips.append({"id":"status","kind":"status","label":"Status","value":intent.status_filter,"editable":True,"removable":True})
    if intent.movie_category:
        chips.append({"id":"movie_category","kind":"movie_category","label":"Category","value":intent.movie_category,"editable":True,"removable":True})
    return chips

def preprocess(question, selected_region):
    q=question.lower().strip(); q_norm=_apply_ontology(q)
    regions=_extract_regions(q_norm) or [selected_region]
    platforms=_extract_platforms(q_norm)
    has_movie=any(kw in q for kw in MOVIE_KW)
    has_rights=any(kw in q for kw in RIGHTS_KW) or any(kw in q for kw in EXPIRY_KW)
    has_dna=any(kw in q for kw in DNA_KW) or "flag" in q
    has_sales=any(kw in q for kw in SALES_KW) or any(x in q for x in ["netflix","amazon","buyer"])
    has_work=any(kw in q for kw in WORK_KW) or "work order" in q
    has_expiry=any(kw in q for kw in EXPIRY_KW)
    has_title=any(kw in q for kw in TITLE_KW) or has_movie
    has_deal_word=any(kw in q for kw in {"deal","deals","contract","contracts","agreement"})
    has_rights_word=any(kw in q for kw in {"rights","license","licensed","window","windows"})
    intent=QueryIntent(
        raw_question=question,normalised=q_norm,regions=regions,platforms=platforms,
        title_hint=_extract_title_hint(question),
        date_filter=_extract_date_filter(q,"d.deal_date"),
        expiry_days=_extract_expiry_days(q) if has_expiry else None,
        status_filter=_extract_status(q),
        movie_category=_extract_movie_category(q) if has_movie else None,
        domain=" ",cross_intent=None,
        has_movie=has_movie,has_rights=has_rights,has_dna=has_dna,
        has_sales=has_sales,has_work=has_work,has_expiry=has_expiry,
        has_title=has_title,has_deal_word=has_deal_word,has_rights_word=has_rights_word,
        match_method="rule",
    )
    intent.domain=_detect_domain(q,intent)
    intent.cross_intent=_detect_cross_intent(intent)
    intent.chips=_build_chips(intent)
    return intent


# ========== LLM Stage ==========
def call_ollama(prompt: str) -> Optional[str]:
    """Call Ollama. Updates LLM_LAST_STATUS for sidebar debug display."""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=30,          # raised from 15s — 1B model can be slow on first call
        )
        if resp.status_code == 200:
            text = resp.json().get("response","").strip()
            LLM_LAST_STATUS.update({"success": True, "error": ""})
            return text
        LLM_LAST_STATUS.update({"success": False, "error": f"HTTP {resp.status_code}"})
        return None
    except requests.exceptions.ConnectionError:
        LLM_LAST_STATUS.update({"success": False, "error": "Connection refused — is Ollama running on port 11434?"})
        return None
    except requests.exceptions.Timeout:
        LLM_LAST_STATUS.update({"success": False, "error": "Timeout >30s"})
        return None
    except Exception as e:
        LLM_LAST_STATUS.update({"success": False, "error": str(e)})
        return None


_VALID_DOMAINS     = {"rights","deals","movies","sales","do_not_air","expiry","work_orders","titles","elemental"}
_VALID_CROSS       = {"deals_rights","title_health","expiry_sales","workorder_rights","movie_dna","movie_sales","title_sales"}
_VALID_STATUSES    = {"Active","Expired","Pending"}
_VALID_CATEGORIES  = {"Theatrical","Library","HBO Original","Direct-to-Streaming"}


def parse_with_llm(question: str, selected_region: str) -> Optional[QueryIntent]:
    """
    FIX v5.2: Compact prompt so llama3.2:1b can actually handle it.
    Validates every LLM field before using it — garbage fields fall back to preprocess() values.
    """
    prompt = (
        'Extract media rights query intent. Reply ONLY with compact JSON, no prose.\n'
        f'Question: "{question}"  Default region: "{selected_region}"\n'
        'JSON (use null for unknown):\n'
        '{"domain":"rights|deals|movies|sales|do_not_air|expiry|work_orders|titles|elemental",'
        '"cross_intent":"deals_rights|title_health|expiry_sales|workorder_rights|movie_dna|movie_sales|title_sales|null",'
        '"regions":["NA","EMEA","APAC","LATAM"],'
        '"platforms":[],'
        '"title_hint":null,'
        '"expiry_days":null,'
        '"status_filter":"Active|Expired|Pending|null",'
        '"movie_category":"Theatrical|Library|HBO Original|Direct-to-Streaming|null"}\n'
        'JSON:'
    )

    resp = call_ollama(prompt)
    if not resp:
        return None

    # Extract JSON block — model may add prose or ```json fences
    jm = re.search(r'```json\s*(\{.*?\})\s*```', resp, re.DOTALL) \
      or re.search(r'(\{[^{}]+\})', resp, re.DOTALL)
    if not jm:
        LLM_LAST_STATUS.update({"success": False, "error": f"No JSON in response: {resp[:100]}"})
        logger.warning(f"Ollama no JSON: {resp[:200]}")
        return None

    try:
        data = json.loads(jm.group(1))
    except json.JSONDecodeError as e:
        LLM_LAST_STATUS.update({"success": False, "error": f"JSON parse error: {e}"})
        logger.warning(f"Ollama JSON error: {e} raw={resp[:200]}")
        return None

    # Build from rule base, override only valid LLM fields
    base = preprocess(question, selected_region)

    if data.get("domain") in _VALID_DOMAINS:
        base.domain = data["domain"]
    ci = data.get("cross_intent")
    if ci in _VALID_CROSS:
        base.cross_intent = ci
    regions = [r for r in (data.get("regions") or []) if r in REGION_CANONICAL]
    if regions:
        base.regions = regions
    platforms = data.get("platforms")
    if isinstance(platforms, list) and platforms:
        base.platforms = platforms
    if data.get("title_hint"):
        base.title_hint = str(data["title_hint"])
    if isinstance(data.get("expiry_days"), int):
        base.expiry_days = data["expiry_days"]
    if data.get("status_filter") in _VALID_STATUSES:
        base.status_filter = data["status_filter"]
    if data.get("movie_category") in _VALID_CATEGORIES:
        base.movie_category = data["movie_category"]

    base.match_method = "llm"
    base.chips = _build_chips(base)
    logger.info(f"LLM parsed OK: domain={base.domain} regions={base.regions} cross={base.cross_intent}")
    return base


# ========== Stage 2: SQL Generation ==========
def _rw(regions, col="region"):
    if not regions: return "1=1"
    if len(regions)==1: return f"UPPER({col})='{regions[0]}'"
    return f"UPPER({col}) IN ('{"','".join(regions)}')"

def _plat(platforms, col):
    if not platforms: return "1=1"
    return "("+(" OR ".join(f"{col} LIKE '%{p}%'" for p in platforms))+")"

def _title_like(hint, col="title_name"):
    safe=hint.replace("'","''").replace(";","")[:100]
    return f"LOWER({col}) LIKE '%{safe.lower()}%'"

def _movie_cat_sql(cat, prefix="m"):
    return f"AND {prefix}.content_category='{cat}'" if cat else ""

def _date_sql(df): return f"AND {df.sql_fragment}" if df else ""

def _status_sql(status, col="d.status"):
    if not status: return ""
    if status=="Pending": return f"AND ({col}='Pending' OR {col}='Under Negotiation')"
    return f"AND {col}='{status}'"

def _build_where(*parts):
    c=[p.strip() for p in parts if p and p.strip() not in ("","1=1","AND 1=1")]
    return " AND ".join(c) if c else "1=1"


def generate(intent: QueryIntent) -> tuple[str, Optional[str], str]:
    q=intent.normalised; r=intent.regions; p=intent.platforms; th=intent.title_hint
    rw=_rw(r); rw_mr=_rw(r,"mr.region"); rw_t=_rw(r,"t.region")
    rw_cd=_rw(r,"cd.region")   # content_deal
    rw_sd=_rw(r,"sd.region"); rw_dna=_rw(r,"dna.region"); rw_wo=_rw(r,"wo.region")
    plat_f=f"AND {_plat(p,'mr.media_platform_primary')}" if p else ""
    date_sql=_date_sql(intent.date_filter)
    cat_m=_movie_cat_sql(intent.movie_category,"m"); cat_t=_movie_cat_sql(intent.movie_category,"t")
    days=intent.expiry_days or 90; ci=intent.cross_intent

    # ── deals_rights ─────────────────────────────────────────────────────────
    # FIX v5.2: ALWAYS routes through content_deal (cd), NOT the `deals` vendor table.
    # `deals` has no title_id column — joining it to media_rights crashes.
    if ci=="deals_rights":
        if any(kw in q for kw in ["breakdown","summary","count","how many","by region","compare","overview"]):
            return f"""
                SELECT cd.region,
                       COUNT(DISTINCT cd.deal_id)  AS deal_count,
                       COUNT(DISTINCT mr.rights_id) AS rights_count,
                       COUNT(DISTINCT mr.title_id)  AS titles_covered,
                       SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights
                FROM content_deal cd
                LEFT JOIN media_rights mr ON cd.deal_id=mr.deal_id AND {rw_mr}
                WHERE {rw_cd} {date_sql}
                GROUP BY cd.region ORDER BY deal_count DESC
            """.strip(), None, 'bar'
        return f"""
            SELECT cd.deal_id, cd.deal_name, cd.deal_source, cd.deal_type,
                   cd.region AS deal_region, mr.title_name, mr.rights_type,
                   mr.media_platform_primary, mr.territories, mr.language, mr.exclusivity,
                   mr.term_from, mr.term_to, mr.status AS rights_status,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
            FROM content_deal cd
            LEFT JOIN media_rights mr ON cd.deal_id=mr.deal_id AND {rw_mr}
            WHERE {rw_cd} {date_sql} {plat_f}
            ORDER BY cd.deal_name, mr.term_to ASC LIMIT 300
        """.strip(), None, 'table'

    if ci=="title_health":
        tf=f"AND {_title_like(th,'t.title_name')}" if th else ""
        return f"""
            SELECT t.title_name, t.title_type, t.content_category, t.genre,
                   COUNT(DISTINCT mr.rights_id) AS total_rights,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.status='Active' AND mr.term_to<=DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d,
                   MAX(CASE WHEN dna.active=1 THEN '🚫 YES' ELSE '✅ Clean' END) AS dna_flag,
                   COUNT(DISTINCT dna.dna_id) AS dna_count,
                   GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                   COUNT(DISTINCT sd.sales_deal_id) AS sales_deals,
                   GROUP_CONCAT(DISTINCT sd.buyer) AS buyers
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            LEFT JOIN do_not_air dna ON t.title_id=dna.title_id AND dna.active=1
            LEFT JOIN sales_deal sd ON t.title_id=sd.title_id AND {rw_sd}
            WHERE {rw_t} {tf} {cat_t} GROUP BY t.title_id
            ORDER BY dna_count DESC, expiring_90d DESC, active_rights DESC LIMIT 150
        """.strip(), None, 'table'

    if ci=="expiry_sales":
        return f"""
            SELECT mr.title_name, mr.media_platform_primary AS rights_platform,
                   mr.territories, mr.term_to AS rights_expiry,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.exclusivity, mr.rights_type, mr.region,
                   sd.buyer AS sold_to, sd.deal_value AS sales_value,
                   sd.media_platform AS sales_platform, sd.term_to AS sales_expiry,
                   sd.status AS sales_status,
                   CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale' ELSE '— No Sale' END AS renewal_flag
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            LEFT JOIN sales_deal sd ON mr.title_id=sd.title_id AND {rw_sd} AND sd.status='Active'
            WHERE {rw_mr} AND mr.status='Active'
              AND mr.term_to<=DATE('now','+{days} days') AND mr.term_to>=DATE('now') {plat_f}
            ORDER BY days_remaining ASC, sd.deal_value DESC LIMIT 150
        """.strip(), None, 'table'

    if ci=="workorder_rights":
        return f"""
            SELECT wo.work_order_id, wo.title_name, wo.work_type, wo.status AS wo_status,
                   wo.priority, wo.due_date, wo.quality_score, wo.vendor_name,
                   COUNT(DISTINCT mr.rights_id) AS active_rights,
                   MIN(mr.term_to) AS earliest_expiry,
                   CAST(JULIANDAY(MIN(mr.term_to))-JULIANDAY('now') AS INTEGER) AS days_to_expiry,
                   SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS rights_expiring_90d
            FROM work_orders wo LEFT JOIN title t ON wo.title_id=t.title_id
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr} AND mr.status='Active'
            WHERE {rw_wo} GROUP BY wo.work_order_id
            ORDER BY rights_expiring_90d DESC, wo.due_date ASC LIMIT 150
        """.strip(), None, 'table'

    if ci=="movie_dna":
        return f"""
            SELECT m.movie_title, m.content_category, m.genre,
                   m.box_office_gross_usd_m AS box_office_usd_m, m.franchise,
                   COUNT(DISTINCT mr.rights_id) AS active_rights,
                   COUNT(DISTINCT dna.dna_id) AS dna_flags,
                   GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                   GROUP_CONCAT(DISTINCT dna.territory) AS restricted_territories,
                   CASE WHEN COUNT(dna.dna_id)>0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status
            FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
            LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr} AND mr.status='Active'
            LEFT JOIN do_not_air dna ON dna.title_id=t.title_id AND dna.active=1
            WHERE 1=1 {cat_m} GROUP BY m.movie_id ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC
        """.strip(), None, 'table'

    if ci=="movie_sales":
        return f"""
            SELECT m.movie_title, m.content_category, m.genre,
                   m.box_office_gross_usd_m AS box_office_usd_m,
                   sd.buyer, sd.deal_type, sd.media_platform AS sold_platform,
                   sd.territory AS sold_territory, sd.deal_value, sd.currency,
                   sd.term_from AS sale_from, sd.term_to AS sale_to, sd.status AS sale_status,
                   COUNT(DISTINCT mr.rights_id) AS rights_in_count
            FROM movie m JOIN title t ON t.movie_id=m.movie_id
            LEFT JOIN sales_deal sd ON sd.title_id=t.title_id AND {rw_sd}
            LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr} AND mr.status='Active'
            WHERE 1=1 {cat_m} GROUP BY m.movie_id, sd.sales_deal_id
            ORDER BY sd.deal_value DESC NULLS LAST, m.box_office_gross_usd_m DESC LIMIT 150
        """.strip(), None, 'table'

    if ci=="title_sales":
        tf=f"AND {_title_like(th,'t.title_name')}" if th else ""
        return f"""
            SELECT t.title_name, t.title_type, t.content_category,
                   mr.media_platform_primary AS rights_platform,
                   mr.term_to AS rights_expiry,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS rights_days_left,
                   mr.status AS rights_status, sd.buyer, sd.media_platform AS sold_platform,
                   sd.deal_value, sd.currency, sd.term_to AS sale_expiry, sd.status AS sale_status
            FROM title t LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            LEFT JOIN sales_deal sd ON t.title_id=sd.title_id AND {rw_sd}
            WHERE {rw_t} {tf} ORDER BY mr.term_to ASC LIMIT 150
        """.strip(), None, 'table'

    # ── Single-domain ─────────────────────────────────────────────────────────
    if any(kw in q for kw in DNA_KW):
        tf=f" AND {_title_like(th,'dna.title_name')}" if th else ""
        return f"""
            SELECT dna.dna_id, dna.title_name, dna.reason_category, dna.reason_subcategory,
                   dna.territory, dna.media_type, dna.term_from, dna.term_to, dna.additional_notes
            FROM do_not_air dna JOIN title t ON dna.title_id=t.title_id
            WHERE {rw_dna} AND dna.active=1 {tf}
            ORDER BY dna.reason_category, dna.title_name LIMIT 200
        """.strip(), None, 'table'

    if intent.has_movie:
        if "franchise" in q:
            return f"""
                SELECT COALESCE(m.franchise,'Standalone') AS franchise_name,
                       COUNT(DISTINCT m.movie_id) AS films,
                       SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
                GROUP BY franchise_name ORDER BY total_box_office_usd_m DESC
            """.strip(), None, 'bar'
        if any(kw in q for kw in ["box office","revenue","gross","value","earnings"]):
            return f"""
                SELECT m.movie_title, m.content_category, m.genre, m.franchise,
                       m.box_office_gross_usd_m AS box_office_usd_m,
                       m.age_rating, m.release_year, COUNT(DISTINCT mr.rights_id) AS active_rights
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr} AND mr.status='Active'
                WHERE 1=1 {cat_m} GROUP BY m.movie_id ORDER BY m.box_office_gross_usd_m DESC
            """.strip(), None, 'bar'
        if any(kw in q for kw in ["rights","license","window","platform","svod","paytv"]):
            tf=f"AND {_title_like(th,'mr.title_name')}" if th else ""
            return f"""
                SELECT m.movie_title, m.content_category, m.genre,
                       m.box_office_gross_usd_m AS box_office_usd_m,
                       mr.rights_type, mr.media_platform_primary, mr.territories,
                       mr.language, mr.exclusivity, mr.term_from, mr.term_to,
                       CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.status, mr.region
                FROM movie m JOIN title t ON t.movie_id=m.movie_id
                JOIN media_rights mr ON mr.title_id=t.title_id
                WHERE {rw_mr} AND mr.status='Active' {cat_t} {tf} {plat_f}
                ORDER BY m.box_office_gross_usd_m DESC, mr.term_to ASC LIMIT 200
            """.strip(), None, 'table'
        if intent.has_expiry:
            return f"""
                SELECT m.movie_title, m.content_category, m.genre,
                       m.box_office_gross_usd_m AS box_office_usd_m,
                       mr.media_platform_primary, mr.territories, mr.region, mr.term_to,
                       CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.exclusivity, mr.status
                FROM movie m JOIN title t ON t.movie_id=m.movie_id
                JOIN media_rights mr ON mr.title_id=t.title_id
                WHERE {rw_mr} AND mr.status='Active'
                  AND mr.term_to<=DATE('now','+{days} days') AND mr.term_to>=DATE('now') {cat_t}
                ORDER BY mr.term_to ASC LIMIT 100
            """.strip(), None, 'table'
        if any(kw in q for kw in ["breakdown","by genre","by category","count","how many","genre"]):
            gc="m.genre" if "genre" in q else "m.content_category"
            return f"""
                SELECT {gc} AS category, COUNT(DISTINCT m.movie_id) AS films,
                       SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
                WHERE 1=1 {cat_m} GROUP BY {gc} ORDER BY films DESC
            """.strip(), None, 'bar'
        return f"""
            SELECT m.movie_id, m.movie_title, m.content_category, m.genre, m.franchise,
                   m.box_office_gross_usd_m AS box_office_usd_m, m.age_rating, m.release_year,
                   COUNT(DISTINCT mr.rights_id) AS total_rights,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.status='Active' AND mr.term_to<=DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d
            FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
            LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
            WHERE 1=1 {cat_m} GROUP BY m.movie_id ORDER BY m.box_office_gross_usd_m DESC
        """.strip(), None, 'table'

    if any(kw in q for kw in ELEMENTAL_KW) and any(kw in q for kw in ["right","deal"]):
        tf=f" AND {_title_like(th,'er.title_name')}" if th else ""
        return f"""
            SELECT er.elemental_rights_id, er.title_name, ed.deal_name, ed.deal_source,
                   ed.deal_type, er.territories, er.media_platform_primary, er.language,
                   er.term_from, er.term_to, er.status, er.region
            FROM elemental_rights er JOIN elemental_deal ed ON er.elemental_deal_id=ed.elemental_deal_id
            WHERE {_rw(r,'er.region')} {tf} ORDER BY er.status DESC, er.title_name LIMIT 100
        """.strip(), None, 'table'

    if any(kw in q for kw in SALES_KW):
        if any(kw in q for kw in ["breakdown","by buyer","platform","by platform"]):
            return f"""
                SELECT buyer, region, COUNT(*) AS deals, SUM(deal_value) AS total_value,
                       COUNT(DISTINCT title_id) AS titles
                FROM sales_deal WHERE {rw} AND status='Active'
                GROUP BY buyer, region ORDER BY total_value DESC LIMIT 15
            """.strip(), None, 'bar'
        return f"""
            SELECT sd.sales_deal_id, sd.deal_type, sd.title_name, sd.buyer,
                   sd.territory, sd.media_platform, sd.term_from, sd.term_to,
                   sd.deal_value, sd.currency, sd.status, sd.region
            FROM sales_deal sd WHERE {rw} ORDER BY sd.deal_value DESC LIMIT 200
        """.strip(), None, 'table'

    if intent.has_expiry:
        tf=f"AND {_title_like(th,'mr.title_name')}" if th else ""
        return f"""
            SELECT mr.rights_id, mr.title_name, mr.region, cd.deal_name, cd.deal_source,
                   mr.rights_type, mr.media_platform_primary, mr.territories, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            WHERE {rw_mr} AND mr.status='Active'
              AND mr.term_to<=DATE('now','+{days} days') AND mr.term_to>=DATE('now') {plat_f} {tf}
            ORDER BY mr.term_to ASC LIMIT 200
        """.strip(), None, 'bar'

    if th and any(kw in q for kw in list(RIGHTS_KW)+["deal"]):
        return f"""
            SELECT mr.rights_id, mr.title_name, mr.region, cd.deal_source, cd.deal_type,
                   cd.deal_name, mr.rights_type, mr.media_platform_primary,
                   mr.media_platform_ancillary, mr.territories, mr.language, mr.brand,
                   mr.term_from, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            WHERE {rw_mr} AND {_title_like(th,'mr.title_name')} {plat_f}
            ORDER BY mr.term_to ASC LIMIT 100
        """.strip(), None, 'table'

    if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["count","how many","total"]):
        return f"""
            SELECT mr.rights_type, mr.status, mr.region,
                   COUNT(DISTINCT mr.title_id) AS title_count,
                   COUNT(mr.rights_id) AS rights_count, SUM(mr.exclusivity) AS exclusive_count
            FROM media_rights mr WHERE {rw_mr} {plat_f}
            GROUP BY mr.rights_type, mr.status, mr.region ORDER BY rights_count DESC
        """.strip(), None, 'bar'

    if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["season","hierarchy","by season"]):
        return f"""
            SELECT s.series_title, se.season_number, mr.region,
                   COUNT(DISTINCT t.title_id) AS episodes,
                   COUNT(DISTINCT mr.rights_id) AS rights_count,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d
            FROM series s JOIN season se ON s.series_id=se.series_id
            JOIN title t ON se.season_id=t.season_id
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            GROUP BY s.series_title, se.season_number, mr.region
            ORDER BY s.series_title, se.season_number LIMIT 100
        """.strip(), None, 'table'

    if "exhibition" in q and "restrict" in q:
        return f"""
            SELECT mr.title_name, mr.region, er.max_plays, er.max_plays_per_day,
                   er.max_days, er.max_networks, mr.media_platform_primary, mr.territories, mr.status
            FROM exhibition_restrictions er JOIN media_rights mr ON er.rights_id=mr.rights_id
            WHERE {rw_mr} ORDER BY mr.title_name LIMIT 100
        """.strip(), None, 'table'

    if any(kw in q for kw in ["breakdown","distribution","mix","analytics","by territory","by platform","by deal"]):
        if "territory" in q or "territories" in q:
            sql=f"SELECT mr.territories, mr.region, COUNT(*) AS rights_count, SUM(mr.exclusivity) AS exclusive, COUNT(DISTINCT mr.title_id) AS titles FROM media_rights mr WHERE {rw_mr} AND mr.status='Active' GROUP BY mr.territories, mr.region ORDER BY rights_count DESC LIMIT 20"
        elif any(kw in q for kw in ["deal source","trl","c2","frl"]):
            sql=f"SELECT cd.deal_source, mr.region, COUNT(DISTINCT mr.rights_id) AS rights, COUNT(DISTINCT mr.title_id) AS titles, SUM(mr.exclusivity) AS exclusive FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id WHERE {rw_mr} GROUP BY cd.deal_source, mr.region ORDER BY rights DESC"
        else:
            sql=f"SELECT mr.media_platform_primary AS platform, mr.rights_type, mr.region, COUNT(*) AS rights_count, COUNT(DISTINCT mr.title_id) AS titles, SUM(mr.exclusivity) AS exclusive FROM media_rights mr WHERE {rw_mr} AND mr.status='Active' GROUP BY platform, mr.rights_type, mr.region ORDER BY rights_count DESC LIMIT 20"
        return sql.strip(), None, 'bar'

    # ── Vendor deals — NO title_id, never join to media_rights ───────────────
    # FIX v5.2: standalone deals query — pure `deals` table, no media_rights join
    if any(kw in q for kw in DEAL_KW):
        rw_d=_rw(r,"d.region")
        stat_cond=_status_sql(intent.status_filter,"d.status")
        where=_build_where(rw_d, stat_cond, _date_sql(intent.date_filter))
        if any(kw in q for kw in ["vendor","by vendor","breakdown","by type","value"]):
            gc="d.vendor_name" if "vendor" in q else "d.deal_type"
            return f"""
                SELECT {gc}, d.region, COUNT(*) AS deal_count,
                       SUM(d.deal_value) AS total_value, AVG(d.deal_value) AS avg_value,
                       SUM(CASE WHEN d.status='Active' THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN d.payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
                FROM deals d WHERE {where}
                GROUP BY {gc}, d.region ORDER BY total_value DESC LIMIT 15
            """.strip(), None, 'bar'
        return f"""
            SELECT d.deal_id, d.deal_name, d.vendor_name, d.deal_type, d.deal_value,
                   d.rights_scope, d.territory, d.deal_date, d.expiry_date,
                   d.status, d.payment_status, d.region,
                   CAST(JULIANDAY(d.expiry_date)-JULIANDAY('now') AS INTEGER) AS days_to_expiry
            FROM deals d WHERE {where} ORDER BY d.deal_value DESC LIMIT 200
        """.strip(), None, 'table'

    if any(kw in q for kw in WORK_KW):
        if "quality" in q or "vendor" in q:
            return f"""
                SELECT vendor_name, region, COUNT(*) AS orders, AVG(quality_score) AS avg_quality,
                       SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delays, SUM(cost) AS total_cost
                FROM work_orders WHERE {rw} GROUP BY vendor_name, region ORDER BY orders DESC LIMIT 10
            """.strip(), None, 'bar'
        return f"""
            SELECT status, region, COUNT(*) AS count FROM work_orders
            WHERE {rw} GROUP BY status, region ORDER BY count DESC
        """.strip(), None, 'pie'

    if intent.has_title:
        title_f=f"WHERE {rw_t}"+(f" AND {_title_like(th,'t.title_name')}" if th else "")
        if any(kw in q for kw in ["count","how many","total"]):
            return f"SELECT t.genre, t.region, COUNT(*) AS title_count FROM title t {title_f} GROUP BY t.genre, t.region ORDER BY title_count DESC".strip(), None, 'bar'
        return f"""
            SELECT t.title_id, t.title_name, t.title_type, t.content_category, t.genre,
                   t.release_year, t.controlling_entity, t.age_rating, t.runtime_minutes, t.region,
                   s.series_title, se.season_number, m.movie_title
            FROM title t LEFT JOIN season se ON t.season_id=se.season_id
            LEFT JOIN series s ON t.series_id=s.series_id
            LEFT JOIN movie m ON t.movie_id=m.movie_id
            {title_f} ORDER BY t.region, t.title_type, s.series_title, se.season_number LIMIT 300
        """.strip(), None, 'table'

    stat_f="AND mr.status='Active'" if "active" in q else ("AND mr.status='Expired'" if "expired" in q else "")
    return f"""
        SELECT mr.title_name, mr.rights_type, mr.media_platform_primary,
               mr.territories, mr.term_from, mr.term_to, mr.status, mr.region,
               CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
        FROM media_rights mr WHERE {rw_mr} {stat_f} {plat_f} ORDER BY mr.term_to DESC LIMIT 100
    """.strip(), None, 'table'


# ========== Stage 3: Validation ==========
DANGEROUS = re.compile(r'(;\s*drop|;\s*delete|;\s*update|;\s*insert|xp_|exec\s*\(|union\s+select)', re.IGNORECASE)

def validate(sql: str, intent: QueryIntent) -> tuple[str, Optional[str]]:
    if not sql or not sql.strip(): return sql, "Empty SQL generated"
    if DANGEROUS.search(sql): return sql, "Dangerous SQL pattern blocked"
    for r in intent.regions:
        if r not in REGION_CANONICAL: return sql, f"Invalid region: {r}"
    return sql, None


# ========== Public API ==========
def parse_query(question: str, selected_region: str = "NA") -> tuple[str, Optional[str], str, str, QueryIntent]:
    try:
        intent = None
        if USE_LLM:
            intent = parse_with_llm(question, selected_region)
        if intent is None:
            intent = preprocess(question, selected_region)
        region_ctx = " vs ".join(intent.regions) if len(intent.regions)>1 else intent.regions[0]
        sql, gen_err, chart_type = generate(intent)
        if gen_err: return sql, gen_err, chart_type, region_ctx, intent
        sql, val_err = validate(sql, intent)
        return sql, val_err, chart_type, region_ctx, intent
    except Exception as e:
        logger.error(f"parse_query: {e}", exc_info=True)
        dummy = QueryIntent(
            raw_question=question, normalised=question.lower(),
            regions=[selected_region], platforms=[], title_hint=None,
            date_filter=None, expiry_days=None, status_filter=None,
            movie_category=None, domain="rights", cross_intent=None, match_method="error",
        )
        return None, str(e), 'table', selected_region, dummy
