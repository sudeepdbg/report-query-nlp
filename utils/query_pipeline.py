"""
query_pipeline.py — Foundry Vantage NL Query Pipeline  (Hybrid v2.0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Stage 1 — PRE-PROCESS  : Hybrid intent extraction
             ┌─ PRIMARY  : Ollama LLM (llama3 / mistral) via local HTTP
             └─ FALLBACK : Rule-based keyword matching (original logic)

Stage 2 — GENERATE     : Deterministic SQL from QueryIntent (rule-based, always)
Stage 3 — VALIDATE     : Schema + injection guard (unchanged)

`parse_query()` now returns a 6-tuple:
  (sql, error, chart_type, region_ctx, intent, match_method)
  match_method → "llm" | "rule"
"""
from __future__ import annotations

import re
import json
import sqlite3
import logging
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ─── OLLAMA CONFIG ───────────────────────────────────────────────────────────
OLLAMA_URL     = "http://localhost:11434/api/generate"
OLLAMA_MODEL   = "llama3"          # change to "mistral" or "llama3.2" as preferred
OLLAMA_TIMEOUT = 8                 # seconds — fast fail so UI never hangs

# ─── VOCABULARY / ONTOLOGY ───────────────────────────────────────────────────
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

KNOWN_TITLES = [
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

# ─── STAGE 1: QueryIntent Dataclass ──────────────────────────────────────────
@dataclass
class DateFilter:
    kind: str
    value: object
    label: str
    sql_fragment: str

@dataclass
class QueryIntent:
    raw_question:   str
    normalised:     str
    regions:        list[str]
    platforms:      list[str]
    title_hint:     Optional[str]
    date_filter:    Optional[DateFilter]
    expiry_days:    Optional[int]
    status_filter:  Optional[str]
    movie_category: Optional[str]
    domain:         str
    cross_intent:   Optional[str]
    has_movie:      bool = False
    has_rights:     bool = False
    has_dna:        bool = False
    has_sales:      bool = False
    has_work:       bool = False
    has_expiry:     bool = False
    has_title:      bool = False
    has_deal_word:  bool = False
    has_rights_word:bool = False
    chips:          list[dict] = field(default_factory=list)
    # ── NEW: tracks how this intent was resolved ──────────────────────────────
    match_method:   str = "rule"   # "llm" | "rule"


# ─── STAGE 1 HELPERS (rule-based) ────────────────────────────────────────────
def _apply_ontology(q: str) -> str:
    for phrase in sorted(ONTOLOGY.keys(), key=len, reverse=True):
        if phrase in q:
            q = q.replace(phrase, ONTOLOGY[phrase])
    return q

def _extract_regions(q: str) -> list[str]:
    q_upper = q.upper()
    return [r for r in REGION_CANONICAL if r in q_upper]

def _extract_platforms(q: str) -> list[str]:
    q2 = _apply_ontology(q.lower())
    return [p for p in ALL_MEDIA if p.lower() in q2 or p in q2]

def _extract_title_hint(question: str) -> Optional[str]:
    quoted = re.findall(r'"([^"]+)"', question)
    if quoted:
        return quoted[0]
    for s in sorted(KNOWN_TITLES, key=len, reverse=True):
        if s.lower() in question.lower():
            return s
    return None

def _extract_date_filter(q: str, date_col: str = "d.deal_date") -> Optional[DateFilter]:
    m = re.search(r'last\s+(\d+)\s+days?', q)
    if m:
        n = int(m.group(1))
        return DateFilter("last_days", n, f"Last {n} Days", f"{date_col} >= DATE('now', '-{n} days')")
    m = re.search(r'last\s+(\d+)\s+weeks?', q)
    if m:
        n = int(m.group(1)); days = n * 7
        return DateFilter("last_weeks", n, f"Last {n} Weeks", f"{date_col} >= DATE('now', '-{days} days')")
    m = re.search(r'last\s+(\d+)\s+months?', q)
    if m:
        n = int(m.group(1)); days = n * 30
        return DateFilter("last_months", n, f"Last {n} Months", f"{date_col} >= DATE('now', '-{days} days')")
    m = re.search(r'(?:in|year)\s*(\d{4})', q)
    if m:
        y = int(m.group(1))
        return DateFilter("year", y, f"Year {y}", f"{date_col} BETWEEN '{y}-01-01' AND '{y}-12-31'")
    m = re.search(r'between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})', q)
    if m:
        s, e = m.group(1), m.group(2)
        return DateFilter("between", (s, e), f"{s} → {e}", f"{date_col} BETWEEN '{s}' AND '{e}'")
    return None

def _extract_expiry_days(q: str) -> Optional[int]:
    for d in re.findall(r'\b(\d+)\s*day', q):
        return int(d)
    return None

def _extract_status(q: str) -> Optional[str]:
    if "active" in q:   return "Active"
    if "expired" in q:  return "Expired"
    if "pending" in q:  return "Pending"
    return None

def _extract_movie_category(q: str) -> Optional[str]:
    if "theatrical" in q and "library" not in q: return "Theatrical"
    if "library" in q:                            return "Library"
    if "hbo original" in q or "hbo film" in q:   return "HBO Original"
    if "direct-to-streaming" in q or "dtv" in q: return "Direct-to-Streaming"
    return None

def _detect_cross_intent(intent: "QueryIntent") -> Optional[str]:
    i = intent
    if i.has_movie and i.has_dna:                          return "movie_dna"
    if i.has_movie and i.has_sales:                        return "movie_sales"
    if (i.has_movie or i.has_title) and i.has_rights and i.has_dna:
        return "title_health"
    if i.has_expiry and i.has_sales:                       return "expiry_sales"
    if i.has_work and (i.has_rights or i.has_title):       return "workorder_rights"
    if i.has_title and i.has_sales:                        return "title_sales"
    if i.has_deal_word and i.has_rights_word:              return "deals_rights"
    return None

def _detect_domain(q: str, intent: "QueryIntent") -> str:
    if any(kw in q for kw in DNA_KW):       return "do_not_air"
    if intent.has_movie:                     return "movies"
    if any(kw in q for kw in ELEMENTAL_KW): return "elemental"
    if any(kw in q for kw in SALES_KW):     return "sales"
    if any(kw in q for kw in EXPIRY_KW):    return "expiry"
    if any(kw in q for kw in DEAL_KW):      return "deals"
    if any(kw in q for kw in WORK_KW):      return "work_orders"
    if any(kw in q for kw in TITLE_KW):     return "titles"
    if any(kw in q for kw in RIGHTS_KW):    return "rights"
    return "rights"

def _build_chips(intent: "QueryIntent") -> list[dict]:
    chips = []
    domain_labels = {
        "rights": "Rights", "deals": "Deals", "movies": "Movies",
        "sales": "Sales Deals", "do_not_air": "Do-Not-Air",
        "expiry": "Expiry Alert", "work_orders": "Work Orders",
        "titles": "Title Catalog", "elemental": "Elemental Rights",
    }
    chips.append({
        "id": "domain", "kind": "domain", "label": "Domain",
        "value": domain_labels.get(intent.domain, intent.domain.replace("_", " ").title()),
        "editable": False, "removable": False,
    })
    if intent.cross_intent:
        cross_labels = {
            "deals_rights": "Deals + Rights", "title_health": "Title Health",
            "expiry_sales": "Expiry + Renewal", "workorder_rights": "Work Orders + Rights",
            "movie_dna": "Movie DNA", "movie_sales": "Movie Sales", "title_sales": "Title Sales",
        }
        chips.append({
            "id": "cross_intent", "kind": "cross_intent", "label": "Join",
            "value": cross_labels.get(intent.cross_intent, intent.cross_intent),
            "editable": False, "removable": False,
        })
    for r in intent.regions:
        chips.append({"id": f"region_{r}", "kind": "region", "label": "Region", "value": r, "editable": False, "removable": True})
    for p in intent.platforms:
        chips.append({"id": f"platform_{p}", "kind": "platform", "label": "Platform", "value": p, "editable": False, "removable": True})
    if intent.title_hint:
        chips.append({"id": "title", "kind": "title", "label": "Title", "value": intent.title_hint, "editable": True, "removable": True})
    if intent.date_filter:
        chips.append({"id": "date", "kind": "date", "label": "Date", "value": intent.date_filter.label, "editable": True, "removable": True})
    if intent.expiry_days:
        chips.append({"id": "expiry_days", "kind": "expiry_days", "label": "Expiry Window", "value": f"{intent.expiry_days} days", "editable": True, "removable": True})
    if intent.status_filter:
        chips.append({"id": "status", "kind": "status", "label": "Status", "value": intent.status_filter, "editable": True, "removable": True})
    if intent.movie_category:
        chips.append({"id": "movie_category", "kind": "movie_category", "label": "Category", "value": intent.movie_category, "editable": True, "removable": True})
    # ── NEW: intent-method chip ───────────────────────────────────────────────
    chips.append({
        "id": "match_method", "kind": "match_method", "label": "Intent",
        "value": "🤖 LLM" if intent.match_method == "llm" else "📐 Rule",
        "editable": False, "removable": False,
    })
    return chips


# ─── STAGE 1A: RULE-BASED PREPROCESSOR (original logic, unchanged) ───────────
def _rule_preprocess(question: str, selected_region: str) -> QueryIntent:
    """Pure keyword/regex intent extraction — always works, no network."""
    q      = question.lower().strip()
    q_norm = _apply_ontology(q)
    regions   = _extract_regions(q_norm) or [selected_region]
    platforms = _extract_platforms(q_norm)

    has_movie     = any(kw in q for kw in MOVIE_KW)
    has_rights    = any(kw in q for kw in RIGHTS_KW) or any(kw in q for kw in EXPIRY_KW)
    has_dna       = any(kw in q for kw in DNA_KW) or "flag" in q
    has_sales     = any(kw in q for kw in SALES_KW) or any(x in q for x in ["netflix", "amazon", "buyer"])
    has_work      = any(kw in q for kw in WORK_KW) or "work order" in q
    has_expiry    = any(kw in q for kw in EXPIRY_KW)
    has_title     = any(kw in q for kw in TITLE_KW) or has_movie
    has_deal_word  = any(kw in q for kw in {"deal", "deals", "contract", "contracts", "agreement"})
    has_rights_word= any(kw in q for kw in {"rights", "license", "licensed", "window", "windows"})

    intent = QueryIntent(
        raw_question    = question,
        normalised      = q_norm,
        regions         = regions,
        platforms       = platforms,
        title_hint      = _extract_title_hint(question),
        date_filter     = _extract_date_filter(q, "d.deal_date"),
        expiry_days     = _extract_expiry_days(q) if has_expiry else None,
        status_filter   = _extract_status(q),
        movie_category  = _extract_movie_category(q) if has_movie else None,
        domain          = " ",
        cross_intent    = None,
        has_movie       = has_movie,
        has_rights      = has_rights,
        has_dna         = has_dna,
        has_sales       = has_sales,
        has_work        = has_work,
        has_expiry      = has_expiry,
        has_title       = has_title,
        has_deal_word   = has_deal_word,
        has_rights_word = has_rights_word,
        match_method    = "rule",
    )
    intent.domain       = _detect_domain(q, intent)
    intent.cross_intent = _detect_cross_intent(intent)
    intent.chips        = _build_chips(intent)
    return intent


# ─── STAGE 1B: LLM INTENT EXTRACTOR (Ollama) ─────────────────────────────────
_LLM_SYSTEM = """You are an intent-extraction engine for a media-rights data platform.
Given a natural-language question about content rights, you MUST respond with ONLY a valid JSON
object — no preamble, no explanation, no markdown fences.

JSON schema (all fields required):
{
  "domain": "<one of: rights|deals|movies|sales|do_not_air|expiry|work_orders|titles|elemental>",
  "regions": ["<zero or more of: NA|APAC|EMEA|LATAM>"],
  "platforms": ["<zero or more of: PayTV|STB-VOD|SVOD|FAST|CatchUp|StartOver|Simulcast|TempDownload|DownloadToOwn>"],
  "title_hint": "<exact title string or null>",
  "expiry_days": <integer or null>,
  "status_filter": "<Active|Expired|Pending or null>",
  "movie_category": "<Theatrical|Library|HBO Original|Direct-to-Streaming or null>",
  "has_movie": <true|false>,
  "has_rights": <true|false>,
  "has_dna": <true|false>,
  "has_sales": <true|false>,
  "has_work": <true|false>,
  "has_expiry": <true|false>,
  "has_title": <true|false>,
  "has_deal_word": <true|false>,
  "has_rights_word": <true|false>,
  "cross_intent": "<deals_rights|title_health|expiry_sales|workorder_rights|movie_dna|movie_sales|title_sales or null>"
}

Domain definitions:
- rights      → questions about media rights windows, licenses, exclusivity
- deals       → content deals, contracts, deal values, vendor deals
- movies      → film slate, box office, franchise, theatrical catalog
- sales       → rights-out / sold rights, buyers, affiliate deals
- do_not_air  → DNA flags, restrictions, banned content
- expiry      → rights or deal expiry, renewal alerts, days remaining
- work_orders → work orders, quality scores, vendor tasks
- titles      → title/show/series/episode catalog exploration
- elemental   → promos, trailers, featurettes, raw assets"""

def _ollama_extract(question: str, selected_region: str) -> Optional[dict]:
    """
    Call Ollama local API. Returns parsed JSON dict on success, None on any failure.
    Non-streaming request with strict timeout so UI never hangs.
    """
    payload = json.dumps({
        "model":  OLLAMA_MODEL,
        "prompt": f"{_LLM_SYSTEM}\n\nQuestion: {question}",
        "stream": False,
        "options": {
            "temperature": 0,          # deterministic
            "num_predict": 400,        # cap tokens — we only need a small JSON
        },
    }).encode("utf-8")

    req = urllib.request.Request(
        OLLAMA_URL,
        data    = payload,
        headers = {"Content-Type": "application/json"},
        method  = "POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            text = data.get("response", "").strip()
            # Strip any accidental markdown fences
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            parsed = json.loads(text)
            return parsed
    except urllib.error.URLError as e:
        logger.debug("Ollama unavailable: %s", e)
    except json.JSONDecodeError as e:
        logger.warning("Ollama returned invalid JSON: %s", e)
    except Exception as e:
        logger.warning("Ollama call failed: %s", e)
    return None


def _llm_preprocess(question: str, selected_region: str) -> Optional[QueryIntent]:
    """
    Try LLM intent extraction. Returns QueryIntent on success, None on failure.
    The rule-based helpers still handle date parsing (LLM doesn't do regex).
    """
    llm_data = _ollama_extract(question, selected_region)
    if llm_data is None:
        return None

    # Validate required fields exist
    required = ["domain", "regions", "platforms", "has_movie", "has_rights",
                "has_dna", "has_sales", "has_work", "has_expiry", "has_title",
                "has_deal_word", "has_rights_word"]
    for k in required:
        if k not in llm_data:
            logger.warning("Ollama JSON missing key: %s", k)
            return None

    q     = question.lower().strip()
    q_norm = _apply_ontology(q)

    # Regions: merge LLM output + canonical check
    regions = [r for r in (llm_data.get("regions") or []) if r in REGION_CANONICAL]
    if not regions:
        regions = _extract_regions(q_norm) or [selected_region]

    # Platforms: use LLM, fallback to rule extraction for any it missed
    platforms = [p for p in (llm_data.get("platforms") or []) if p in ALL_MEDIA]
    if not platforms:
        platforms = _extract_platforms(q_norm)

    # Date filter: always rule-based (regex is more reliable than LLM for dates)
    date_filter = _extract_date_filter(q, "d.deal_date")

    # Title hint: LLM first, fallback to regex/known-titles
    title_hint = llm_data.get("title_hint") or _extract_title_hint(question)

    intent = QueryIntent(
        raw_question    = question,
        normalised      = q_norm,
        regions         = regions,
        platforms       = platforms,
        title_hint      = title_hint,
        date_filter     = date_filter,
        expiry_days     = llm_data.get("expiry_days") or (_extract_expiry_days(q) if llm_data.get("has_expiry") else None),
        status_filter   = llm_data.get("status_filter"),
        movie_category  = llm_data.get("movie_category"),
        domain          = llm_data.get("domain", "rights"),
        cross_intent    = llm_data.get("cross_intent"),
        has_movie       = bool(llm_data.get("has_movie",  False)),
        has_rights      = bool(llm_data.get("has_rights", False)),
        has_dna         = bool(llm_data.get("has_dna",    False)),
        has_sales       = bool(llm_data.get("has_sales",  False)),
        has_work        = bool(llm_data.get("has_work",   False)),
        has_expiry      = bool(llm_data.get("has_expiry", False)),
        has_title       = bool(llm_data.get("has_title",  False)),
        has_deal_word   = bool(llm_data.get("has_deal_word",   False)),
        has_rights_word = bool(llm_data.get("has_rights_word", False)),
        match_method    = "llm",
    )

    # If LLM didn't set cross_intent, recompute it with our logic
    if not intent.cross_intent:
        intent.cross_intent = _detect_cross_intent(intent)

    intent.chips = _build_chips(intent)
    return intent


# ─── STAGE 1: HYBRID PREPROCESSOR (public entry point) ───────────────────────
def preprocess(question: str, selected_region: str) -> QueryIntent:
    """
    Hybrid Stage 1:
      1. Try Ollama LLM (fast, timeout-guarded)
      2. On any failure → transparently fall back to rule-based
    match_method field on the returned QueryIntent tells you which path won.
    """
    # Attempt LLM path
    intent = _llm_preprocess(question, selected_region)
    if intent is not None:
        logger.info("Intent resolved via LLM (Ollama/%s)", OLLAMA_MODEL)
        return intent

    # Fall through to rule-based
    logger.info("Intent resolved via rule-based fallback")
    return _rule_preprocess(question, selected_region)


# ─── STAGE 2: GENERATE ───────────────────────────────────────────────────────
def _rw(regions: list[str], col: str = "region") -> str:
    if not regions: return "1=1"
    if len(regions) == 1: return f"UPPER({col}) = '{regions[0]}'"
    joined = "','".join(regions)
    return f"UPPER({col}) IN ('{joined}')"

def _plat(platforms: list[str], col: str) -> str:
    if not platforms: return "1=1"
    return "(" + " OR ".join(f"{col} LIKE '%{p}%'" for p in platforms) + ")"

def _title_like(hint: str, col: str = "title_name") -> str:
    safe = hint.replace("'","''").replace(";","")[:100]
    return f"LOWER({col}) LIKE '%{safe.lower()}%'"

def _movie_cat_sql(cat: Optional[str], prefix: str = "m") -> str:
    if not cat: return ""
    return f"AND {prefix}.content_category = '{cat}'"

def _date_sql(df: Optional[DateFilter]) -> str:
    return f"AND {df.sql_fragment}" if df else ""

def _status_sql(status: Optional[str], col: str = "d.status") -> str:
    if not status: return ""
    if status == "Pending":
        return f"AND ({col}='Pending' OR {col}='In Progress')"
    return f"AND {col}='{status}'"

def _build_where(*parts) -> str:
    non_empty = [p for p in parts if p and p.strip() and p.strip() not in ("1=1", "AND 1=1")]
    if not non_empty: return "1=1"
    cleaned = []
    for p in non_empty:
        p = p.strip()
        if p.upper().startswith("AND "):
            p = p[4:].strip()
        cleaned.append(p)
    return " AND ".join(cleaned)

def generate(intent: QueryIntent) -> tuple[str, Optional[str], str]:
    """Stage 2: deterministic SQL generation from QueryIntent (always rule-based)."""
    q    = intent.normalised
    r    = intent.regions
    th   = intent.title_hint
    days = intent.expiry_days or 90

    rw     = _rw(r)
    rw_mr  = _rw(r, "mr.region")
    rw_t   = _rw(r, "t.region")
    rw_d   = _rw(r, "d.region")
    rw_dna = _rw(r, "dna.region")
    rw_sd  = _rw(r, "sd.region")

    plat_f = (f"AND {_plat(intent.platforms,'mr.media_platform_primary')}"
              if intent.platforms else "")

    cat_t = _movie_cat_sql(intent.movie_category, "t")
    cat_m = _movie_cat_sql(intent.movie_category, "m")

    # ── Cross-intent JOINs ───────────────────────────────────────────────────
    if intent.cross_intent == "title_health":
        tf = f"AND {_title_like(th,'t.title_name')}" if th else ""
        sql = f"""
            SELECT t.title_id, t.title_name, t.title_type, t.content_category,
                   COUNT(DISTINCT mr.rights_id) AS active_rights,
                   SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d,
                   COUNT(DISTINCT dna.dna_id) AS dna_flags,
                   GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                   COUNT(DISTINCT sd.sales_deal_id) AS sales_deals,
                   GROUP_CONCAT(DISTINCT sd.buyer) AS buyers
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND mr.status='Active' AND {rw_mr}
            LEFT JOIN do_not_air dna ON t.title_id=dna.title_id AND dna.active=1 AND {rw_dna}
            LEFT JOIN sales_deal sd ON t.title_id=sd.title_id AND {rw_sd}
            WHERE {rw_t} {tf}
            GROUP BY t.title_id ORDER BY dna_flags DESC, expiring_90d DESC LIMIT 100
        """
        return sql.strip(), None, 'table'

    if intent.cross_intent == "expiry_sales":
        tf = f"AND {_title_like(th,'mr.title_name')}" if th else ""
        sql = f"""
            SELECT mr.title_name, mr.media_platform_primary AS rights_platform,
                   mr.term_to AS rights_expiry,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_left,
                   mr.exclusivity, sd.buyer, sd.deal_value, sd.currency,
                   sd.status AS sale_status,
                   CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale' ELSE '— No Sale' END AS flag
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id=cd.deal_id
            LEFT JOIN sales_deal sd ON mr.title_id=sd.title_id AND sd.status='Active' AND {rw_sd}
            WHERE {rw_mr} AND mr.status='Active'
              AND mr.term_to<=DATE('now','+{days} days') AND mr.term_to>=DATE('now')
              {plat_f} {tf}
            ORDER BY days_left ASC LIMIT 150
        """
        return sql.strip(), None, 'table'

    if intent.cross_intent == "workorder_rights":
        tf = f"AND {_title_like(th,'mr.title_name')}" if th else ""
        sql = f"""
            SELECT wo.work_order_id, wo.title_name, wo.task_type, wo.status AS wo_status,
                   wo.priority, wo.due_date, wo.vendor_name, wo.quality_score,
                   mr.rights_type, mr.media_platform_primary, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS rights_days_left,
                   mr.status AS rights_status
            FROM work_orders wo
            LEFT JOIN title t ON LOWER(wo.title_name)=LOWER(t.title_name) AND {rw_t}
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            WHERE {_rw(r,'wo.region')} {tf}
            ORDER BY wo.priority DESC, wo.due_date ASC LIMIT 100
        """
        return sql.strip(), None, 'table'

    if intent.cross_intent in ("movie_dna", "movie_sales"):
        base_join = f"""
            FROM movie m
            LEFT JOIN title t ON t.movie_id=m.movie_id
            LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
        """
        tf = f"AND {_title_like(th,'m.movie_title')}" if th else ""
        if intent.cross_intent == "movie_dna":
            sql = f"""
                SELECT m.movie_title, m.content_category, m.genre, m.box_office_gross_usd_m AS box_office_usd_m,
                       COUNT(DISTINCT mr.rights_id) AS active_rights,
                       COUNT(DISTINCT dna.dna_id) AS dna_flags,
                       GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons,
                       CASE WHEN COUNT(dna.dna_id)>0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status
                {base_join}
                LEFT JOIN do_not_air dna ON dna.title_id=t.title_id AND dna.active=1 AND {rw_dna}
                WHERE 1=1 {cat_m} {tf}
                GROUP BY m.movie_id ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC
            """
        else:
            sql = f"""
                SELECT m.movie_title, m.content_category, m.genre, m.box_office_gross_usd_m AS box_office_usd_m,
                       sd.buyer, sd.deal_value, sd.currency, sd.media_platform AS sold_platform,
                       sd.territory AS sold_territory, sd.term_to AS sale_expiry, sd.status AS sale_status
                {base_join}
                LEFT JOIN sales_deal sd ON sd.title_id=t.title_id AND {rw_sd}
                WHERE sd.sales_deal_id IS NOT NULL {cat_m} {tf}
                ORDER BY m.box_office_gross_usd_m DESC, sd.deal_value DESC LIMIT 150
            """
        return sql.strip(), None, 'table'

    if intent.cross_intent == "deals_rights":
        stat_cond = _status_sql(intent.status_filter, "d.status")
        where = _build_where(rw_d, stat_cond, _date_sql(intent.date_filter))
        sql = f"""
            SELECT d.deal_id, d.deal_name, d.vendor_name, d.deal_type, d.deal_value, d.status,
                   COUNT(DISTINCT mr.rights_id) AS linked_rights,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d,
                   MIN(mr.term_to) AS earliest_expiry
            FROM deals d
            LEFT JOIN content_deal cd ON d.deal_name=cd.deal_name
            LEFT JOIN media_rights mr ON cd.deal_id=mr.deal_id AND {rw_mr}
            WHERE {where}
            GROUP BY d.deal_id ORDER BY d.deal_value DESC LIMIT 100
        """
        return sql.strip(), None, 'table'

    if intent.cross_intent == "title_sales":
        tf = f"AND {_title_like(th,'t.title_name')}" if th else ""
        sql = f"""
            SELECT t.title_name, t.title_type, t.content_category, t.genre,
                   mr.rights_type, mr.media_platform_primary,
                   mr.term_to AS rights_expiry, mr.status AS rights_status,
                   sd.buyer, sd.media_platform AS sold_platform,
                   sd.deal_value, sd.currency, sd.term_to AS sale_expiry, sd.status AS sale_status
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            LEFT JOIN sales_deal sd ON t.title_id=sd.title_id AND {rw_sd}
            WHERE {rw_t} {tf}
            ORDER BY mr.term_to ASC LIMIT 150
        """
        return sql.strip(), None, 'table'

    # ── Single-domain queries ─────────────────────────────────────────────────
    if any(kw in q for kw in DNA_KW):
        tf = f" AND {_title_like(th,'dna.title_name')}" if th else ""
        sql = f"""
            SELECT dna.dna_id, dna.title_name, dna.reason_category, dna.reason_subcategory,
                   dna.territory, dna.media_type, dna.term_from, dna.term_to, dna.additional_notes
            FROM do_not_air dna JOIN title t ON dna.title_id=t.title_id
            WHERE {rw_dna} AND dna.active=1 {tf}
            ORDER BY dna.reason_category, dna.title_name LIMIT 200
        """
        return sql.strip(), None, 'table'

    if intent.has_movie:
        if "franchise" in q:
            sql = f"""
                SELECT COALESCE(m.franchise,'Standalone') AS franchise_name,
                       COUNT(DISTINCT m.movie_id) AS films,
                       SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
                GROUP BY franchise_name ORDER BY total_box_office_usd_m DESC
            """
            return sql.strip(), None, 'bar'
        if any(kw in q for kw in ["box office", "revenue", "gross", "value", "earnings"]):
            sql = f"""
                SELECT m.movie_title, m.content_category, m.genre, m.franchise,
                       m.box_office_gross_usd_m AS box_office_usd_m,
                       m.age_rating, m.release_year,
                       COUNT(DISTINCT mr.rights_id) AS active_rights
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr} AND mr.status='Active'
                WHERE 1=1 {cat_m}
                GROUP BY m.movie_id ORDER BY m.box_office_gross_usd_m DESC
            """
            return sql.strip(), None, 'bar'
        if any(kw in q for kw in ["rights", "license", "window", "platform", "svod", "paytv"]):
            tf = f"AND {_title_like(th,'mr.title_name')}" if th else ""
            sql = f"""
                SELECT m.movie_title, m.content_category, m.genre,
                       m.box_office_gross_usd_m AS box_office_usd_m,
                       mr.rights_type, mr.media_platform_primary, mr.territories,
                       mr.language, mr.exclusivity,
                       mr.term_from, mr.term_to,
                       CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                       mr.status, mr.region
                FROM movie m JOIN title t ON t.movie_id=m.movie_id
                JOIN media_rights mr ON mr.title_id=t.title_id
                WHERE {rw_mr} AND mr.status='Active' {cat_t} {tf} {plat_f}
                ORDER BY m.box_office_gross_usd_m DESC, mr.term_to ASC LIMIT 200
            """
            return sql.strip(), None, 'table'
        if intent.has_expiry:
            sql = f"""
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
            """
            return sql.strip(), None, 'table'
        if any(kw in q for kw in ["breakdown", "by genre", "by category", "count", "how many", "genre"]):
            group_col = "m.genre" if "genre" in q else "m.content_category"
            sql = f"""
                SELECT {group_col} AS category,
                       COUNT(DISTINCT m.movie_id) AS films,
                       SUM(m.box_office_gross_usd_m) AS total_box_office_usd_m,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
                LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
                WHERE 1=1 {cat_m}
                GROUP BY {group_col} ORDER BY films DESC
            """
            return sql.strip(), None, 'bar'
        sql = f"""
            SELECT m.movie_id, m.movie_title, m.content_category, m.genre, m.franchise,
                   m.box_office_gross_usd_m AS box_office_usd_m, m.age_rating, m.release_year,
                   COUNT(DISTINCT mr.rights_id) AS total_rights,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.status='Active' AND mr.term_to<=DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d
            FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id
            LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND {rw_mr}
            WHERE 1=1 {cat_m}
            GROUP BY m.movie_id ORDER BY m.box_office_gross_usd_m DESC
        """
        return sql.strip(), None, 'table'

    if any(kw in q for kw in ELEMENTAL_KW) and any(kw in q for kw in ["right", "deal"]):
        tf = f" AND {_title_like(th,'er.title_name')}" if th else ""
        sql = f"""
            SELECT er.elemental_rights_id, er.title_name, ed.deal_name, ed.deal_source, ed.deal_type,
                   er.territories, er.media_platform_primary, er.language,
                   er.term_from, er.term_to, er.status, er.region
            FROM elemental_rights er JOIN elemental_deal ed ON er.elemental_deal_id=ed.elemental_deal_id
            WHERE {_rw(r,'er.region')} {tf}
            ORDER BY er.status DESC, er.title_name LIMIT 100
        """
        return sql.strip(), None, 'table'

    if any(kw in q for kw in SALES_KW):
        if any(kw in q for kw in ["breakdown", "by buyer", "platform", "by platform"]):
            sql = f"""
                SELECT buyer, region, COUNT(*) AS deals,
                       SUM(deal_value) AS total_value, COUNT(DISTINCT title_id) AS titles
                FROM sales_deal WHERE {rw} AND status='Active'
                GROUP BY buyer, region ORDER BY total_value DESC LIMIT 15
            """
            return sql.strip(), None, 'bar'
        sql = f"""
            SELECT sd.sales_deal_id, sd.deal_type, sd.title_name, sd.buyer, sd.territory,
                   sd.media_platform, sd.term_from, sd.term_to, sd.deal_value,
                   sd.currency, sd.status, sd.region
            FROM sales_deal sd WHERE {rw}
            ORDER BY sd.deal_value DESC LIMIT 200
        """
        return sql.strip(), None, 'table'

    if intent.has_expiry:
        tf = f"AND {_title_like(th,'mr.title_name')}" if th else ""
        sql = f"""
            SELECT mr.rights_id, mr.title_name, mr.region, cd.deal_name, cd.deal_source,
                   mr.rights_type, mr.media_platform_primary, mr.territories, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            WHERE {rw_mr} AND mr.status='Active'
              AND mr.term_to<=DATE('now','+{days} days') AND mr.term_to>=DATE('now')
              {plat_f} {tf}
            ORDER BY mr.term_to ASC LIMIT 200
        """
        return sql.strip(), None, 'bar'

    if th and any(kw in q for kw in list(RIGHTS_KW) + ["deal"]):
        sql = f"""
            SELECT mr.rights_id, mr.title_name, mr.region, cd.deal_source, cd.deal_type, cd.deal_name,
                   mr.rights_type, mr.media_platform_primary, mr.media_platform_ancillary,
                   mr.territories, mr.language, mr.brand, mr.term_from, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.exclusivity, mr.holdback, mr.holdback_days, mr.status
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            WHERE {rw_mr} AND {_title_like(th,'mr.title_name')} {plat_f}
            ORDER BY mr.term_to ASC LIMIT 100
        """
        return sql.strip(), None, 'table'

    if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["count", "how many", "total"]):
        sql = f"""
            SELECT mr.rights_type, mr.status, mr.region,
                   COUNT(DISTINCT mr.title_id) AS title_count,
                   COUNT(mr.rights_id) AS rights_count,
                   SUM(mr.exclusivity) AS exclusive_count
            FROM media_rights mr WHERE {rw_mr} {plat_f}
            GROUP BY mr.rights_type, mr.status, mr.region
            ORDER BY rights_count DESC
        """
        return sql.strip(), None, 'bar'

    if any(kw in q for kw in RIGHTS_KW) and any(kw in q for kw in ["season", "hierarchy", "by season"]):
        sql = f"""
            SELECT s.series_title, se.season_number, mr.region,
                   COUNT(DISTINCT t.title_id) AS episodes,
                   COUNT(DISTINCT mr.rights_id) AS rights_count,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d
            FROM series s
            JOIN season se ON s.series_id=se.series_id
            JOIN title t ON se.season_id=t.season_id
            LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND {rw_mr}
            GROUP BY s.series_title, se.season_number, mr.region
            ORDER BY s.series_title, se.season_number LIMIT 100
        """
        return sql.strip(), None, 'table'

    if "exhibition" in q and "restrict" in q:
        sql = f"""
            SELECT mr.title_name, mr.region, er.max_plays, er.max_plays_per_day,
                   er.max_days, er.max_networks,
                   mr.media_platform_primary, mr.territories, mr.status
            FROM exhibition_restrictions er
            JOIN media_rights mr ON er.rights_id=mr.rights_id
            WHERE {rw_mr}
            ORDER BY mr.title_name LIMIT 100
        """
        return sql.strip(), None, 'table'

    if any(kw in q for kw in ["breakdown", "distribution", "mix", "analytics", "by territory", "by platform", "by deal"]):
        if "territory" in q or "territories" in q:
            sql = f"""
                SELECT mr.territories, mr.region,
                       COUNT(*) AS rights_count, SUM(mr.exclusivity) AS exclusive,
                       COUNT(DISTINCT mr.title_id) AS titles
                FROM media_rights mr WHERE {rw_mr} AND mr.status='Active'
                GROUP BY mr.territories, mr.region ORDER BY rights_count DESC LIMIT 20
            """
        elif any(kw in q for kw in ["deal source", "trl", "c2", "frl"]):
            sql = f"""
                SELECT cd.deal_source, mr.region,
                       COUNT(DISTINCT mr.rights_id) AS rights,
                       COUNT(DISTINCT mr.title_id) AS titles,
                       SUM(mr.exclusivity) AS exclusive
                FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
                WHERE {rw_mr}
                GROUP BY cd.deal_source, mr.region ORDER BY rights DESC
            """
        else:
            sql = f"""
                SELECT mr.media_platform_primary AS platform, mr.rights_type, mr.region,
                       COUNT(*) AS rights_count,
                       COUNT(DISTINCT mr.title_id) AS titles,
                       SUM(mr.exclusivity) AS exclusive
                FROM media_rights mr WHERE {rw_mr} AND mr.status='Active'
                GROUP BY platform, mr.rights_type, mr.region
                ORDER BY rights_count DESC LIMIT 20
            """
        return sql.strip(), None, 'bar'

    if any(kw in q for kw in DEAL_KW):
        stat_cond = _status_sql(intent.status_filter, "d.status")
        where = _build_where(rw_d, stat_cond, _date_sql(intent.date_filter))
        if any(kw in q for kw in ["vendor", "by vendor", "breakdown", "by type", "value"]):
            group_col = "d.vendor_name" if "vendor" in q else "d.deal_type"
            sql = f"""
                SELECT {group_col}, d.region, COUNT(*) AS deal_count,
                       SUM(d.deal_value) AS total_value, AVG(d.deal_value) AS avg_value,
                       SUM(CASE WHEN d.status='Active' THEN 1 ELSE 0 END) AS active,
                       SUM(CASE WHEN d.payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
                FROM deals d WHERE {where}
                GROUP BY {group_col}, d.region ORDER BY total_value DESC LIMIT 15
            """
            return sql.strip(), None, 'bar'
        sql = f"""
            SELECT d.deal_id, d.deal_name, d.vendor_name, d.deal_type, d.deal_value,
                   d.rights_scope, d.territory, d.deal_date, d.expiry_date,
                   d.status, d.payment_status, d.region,
                   CAST(JULIANDAY(d.expiry_date)-JULIANDAY('now') AS INTEGER) AS days_to_expiry
            FROM deals d WHERE {where}
            ORDER BY d.deal_value DESC LIMIT 200
        """
        return sql.strip(), None, 'table'

    if any(kw in q for kw in WORK_KW):
        if "quality" in q or "vendor" in q:
            sql = f"""
                SELECT vendor_name, region, COUNT(*) AS orders,
                       AVG(quality_score) AS avg_quality,
                       SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delays,
                       SUM(cost) AS total_cost
                FROM work_orders WHERE {rw}
                GROUP BY vendor_name, region ORDER BY orders DESC LIMIT 10
            """
            return sql.strip(), None, 'bar'
        sql = f"""
            SELECT status, region, COUNT(*) AS count FROM work_orders WHERE {rw}
            GROUP BY status, region ORDER BY count DESC
        """
        return sql.strip(), None, 'pie'

    if intent.has_title:
        title_f = f"WHERE {rw_t} " + (f" AND {_title_like(th,'t.title_name')}" if th else "")
        if any(kw in q for kw in ["count", "how many", "total"]):
            sql = f"""
                SELECT t.genre, t.region, COUNT(*) AS title_count
                FROM title t {title_f}
                GROUP BY t.genre, t.region ORDER BY title_count DESC
            """
            return sql.strip(), None, 'bar'
        sql = f"""
            SELECT t.title_id, t.title_name, t.title_type, t.content_category, t.genre,
                   t.release_year, t.controlling_entity, t.age_rating, t.runtime_minutes, t.region,
                   s.series_title, se.season_number, m.movie_title
            FROM title t
            LEFT JOIN season se ON t.season_id=se.season_id
            LEFT JOIN series s ON t.series_id=s.series_id
            LEFT JOIN movie m ON t.movie_id=m.movie_id
            {title_f}
            ORDER BY t.region, t.title_type, s.series_title, se.season_number LIMIT 300
        """
        return sql.strip(), None, 'table'

    stat_f = ("AND mr.status='Active'" if "active" in q
              else "AND mr.status='Expired'" if "expired" in q else "")
    sql = f"""
        SELECT mr.title_name, mr.rights_type, mr.media_platform_primary,
               mr.territories, mr.term_from, mr.term_to, mr.status, mr.region,
               CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining
        FROM media_rights mr
        WHERE {rw_mr} {stat_f} {plat_f}
        ORDER BY mr.term_to DESC LIMIT 100
    """
    return sql.strip(), None, 'table'


# ─── STAGE 3: VALIDATE ───────────────────────────────────────────────────────
_ALLOWED_COLS = {
    "deal_id", "deal_name", "vendor_name", "deal_type", "deal_value", "deal_date",
    "expiry_date", "deal_status", "deal_region", "rights_id", "title_name",
    "rights_type", "rights_platform", "rights_start", "rights_expiry",
    "rights_days_left", "rights_status", "region", "territory", "territories",
    "media_platform_primary", "media_platform_ancillary", "term_from", "term_to",
    "status", "active_rights", "total_rights", "expiring_90d", "days_remaining",
    "days_left", "days_to_expiry", "exclusivity", "holdback", "holdback_days",
    "buyer", "currency", "sale_from", "sale_to", "sale_status",
    "dna_flag", "dna_count", "dna_reasons", "dna_status", "sales_deals", "buyers",
    "franchise", "content_category", "genre", "box_office_usd_m", "age_rating",
    "release_year", "title_type", "series_title", "season_number", "episodes",
    "rights_count", "active", "work_order_id", "wo_status", "priority", "due_date",
    "quality_score", "earliest_expiry", "rights_expiring_90d", "movie_title",
    "sold_platform", "sold_territory", "sale_expiry", "total_deal_value",
    "active_deals", "linked_rights", "deal_count",
}
DANGEROUS = re.compile(
    r'(;\sdrop|;\sdelete|;\supdate|;\sinsert|xp|exec\s*\(|union\s+select)',
    re.IGNORECASE
)

def validate(sql: str, intent: QueryIntent) -> tuple[str, Optional[str]]:
    if not sql or not sql.strip():
        return sql, "Empty SQL generated"
    if DANGEROUS.search(sql):
        return sql, "Potentially dangerous SQL pattern detected — query blocked"
    for r in intent.regions:
        if r not in REGION_CANONICAL:
            return sql, f"Invalid region: {r}"
    return sql, None


# ─── PUBLIC API ───────────────────────────────────────────────────────────────
def parse_query(
    question: str,
    selected_region: str = "NA",
) -> tuple[str, Optional[str], str, str, QueryIntent, str]:
    """
    Run the full 3-stage hybrid pipeline.

    Returns
    -------
    sql          : str
    error        : Optional[str]
    chart_type   : str  — 'table' | 'bar' | 'pie'
    region_ctx   : str
    intent       : QueryIntent
    match_method : str  — 'llm' | 'rule'
    """
    try:
        intent = preprocess(question, selected_region)
        region_ctx = " vs ".join(intent.regions) if len(intent.regions) > 1 else intent.regions[0]
        sql, gen_err, chart_type = generate(intent)
        if gen_err:
            return sql, gen_err, chart_type, region_ctx, intent, intent.match_method
        sql, val_err = validate(sql, intent)
        return sql, val_err, chart_type, region_ctx, intent, intent.match_method
    except Exception as e:
        import traceback
        logger.error("parse_query error: %s\n%s", e, traceback.format_exc())
        dummy = QueryIntent(
            raw_question=question, normalised=question.lower(),
            regions=[selected_region], platforms=[], title_hint=None,
            date_filter=None, expiry_days=None, status_filter=None,
            movie_category=None, domain="rights", cross_intent=None,
            match_method="rule",
        )
        return None, str(e), 'table', selected_region, dummy, "rule"


# ─── OLLAMA HEALTH CHECK (convenience) ───────────────────────────────────────
def ollama_is_available() -> bool:
    """Quick ping — use in UI to show Ollama status badge in sidebar."""
    try:
        req = urllib.request.Request(
            "http://localhost:11434/api/tags",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False
