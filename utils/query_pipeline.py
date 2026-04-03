"""
query_chips_ui.py — Enhancement 4: SQL Logic Chips UI
═══════════════════════════════════════════════════════
Renders parsed QueryIntent as interactive editable chips/facets.
Users can remove a chip or edit a value → query reruns automatically.

Usage in page_chat() / page_custom_dashboard():

    from query_chips_ui import render_chips, apply_chip_edits

    # After parse_query returns intent:
    modified_intent = render_chips(intent, key_prefix="chat")
    if modified_intent:          # user changed something
        intent = modified_intent
        sql, _, chart_type = generate(intent)   # Stage 2 only
        # … re-execute + re-render
"""

from __future__ import annotations
import copy
import streamlit as st
from typing import Optional

from utils.query_parser import (
    QueryIntent, DateFilter,
    preprocess, generate, validate,
    REGION_CANONICAL, ALL_MEDIA,
)

# ── CSS injected once ──────────────────────────────────────────────────────────
_CHIPS_CSS = """
<style>
/* ── Query Chips ─────────────────────────────────────────── */
.chip-row{display:flex;flex-wrap:wrap;gap:6px;align-items:center;
  padding:10px 14px;background:#f8faff;border:1px solid #ddd6fe;
  border-radius:10px;margin-bottom:4px}
.chip-label{font-size:.6rem;font-weight:700;color:#7c3aed;
  text-transform:uppercase;letter-spacing:.08em;margin-right:3px}
.chip{display:inline-flex;align-items:center;gap:5px;
  background:#ede9fe;border:1px solid #c4b5fd;border-radius:20px;
  padding:3px 10px;font-size:.78rem;font-weight:600;color:#4c1d95;
  white-space:nowrap}
.chip-domain{background:#0f172a;border-color:#0f172a;color:#e2e8f0}
.chip-cross {background:#1e3a5f;border-color:#1e3a5f;color:#bfdbfe}
.chip-region{background:#dbeafe;border-color:#93c5fd;color:#1e40af}
.chip-platform{background:#dcfce7;border-color:#86efac;color:#166534}
.chip-date  {background:#fef3c7;border-color:#fcd34d;color:#92400e}
.chip-expiry{background:#fee2e2;border-color:#fca5a5;color:#991b1b}
.chip-status{background:#f1f5f9;border-color:#cbd5e1;color:#475569}
.chip-title {background:#fdf4ff;border-color:#e879f9;color:#86198f}
.chip-category{background:#fff7ed;border-color:#fdba74;color:#9a3412}
.chip-hint{font-size:.72rem;color:#64748b;font-style:italic;
  padding:3px 6px}
</style>
"""

# Chip kind → CSS class
_KIND_CLASS = {
    "domain": "chip chip-domain",
    "cross_intent": "chip chip-cross",
    "region": "chip chip-region",
    "platform": "chip chip-platform",
    "date": "chip chip-date",
    "expiry_days": "chip chip-expiry",
    "status": "chip chip-status",
    "title": "chip chip-title",
    "movie_category": "chip chip-category",
}


def _inject_css():
    st.markdown(_CHIPS_CSS, unsafe_allow_html=True)


# ── Read/write intent via session state ────────────────────────────────────────

def _ss_key(prefix: str) -> str:
    return f"_chips_{prefix}"


def _load_intent(prefix: str) -> Optional[QueryIntent]:
    return st.session_state.get(_ss_key(prefix))


def _save_intent(prefix: str, intent: QueryIntent):
    st.session_state[_ss_key(prefix)] = intent


# ── Core renderer ──────────────────────────────────────────────────────────────

def render_chips(
    intent: QueryIntent,
    key_prefix: str = "chips",
    on_change_rerun: bool = True,
) -> Optional[QueryIntent]:
    """
    Render the QueryIntent as an interactive chip row.

    Returns a NEW QueryIntent if the user made a change, else None.
    If on_change_rerun=True (default), calls st.rerun() automatically
    after any mutation so the caller's query reruns immediately.

    Layout
    ──────
    [Domain] [Join?] [Region×] [Platform×] [Title ✎] [Date ✎] [Expiry ✎] [Status ✎]
    Non-removable chips (domain, cross_intent) are displayed as plain pills.
    Removable chips show an × button next to the value.
    Editable chips show a ✎ pencil that opens an inline input widget.
    """
    _inject_css()
    _save_intent(key_prefix, intent)

    changed_intent: Optional[QueryIntent] = None

    # ── Static chip HTML (non-interactive summary) ─────────────────────────
    static_html = '<div class="chip-row">'
    static_html += '<span class="chip-hint">🔍 Query parsed as:</span>'
    for chip in intent.chips:
        if not chip["removable"] and not chip["editable"]:
            cls = _KIND_CLASS.get(chip["kind"], "chip")
            static_html += (f'<span class="{cls}">'
                            f'<span class="chip-label">{chip["label"]}</span>'
                            f'{chip["value"]}</span>')
    static_html += '</div>'
    st.markdown(static_html, unsafe_allow_html=True)

    # ── Interactive chips (removable / editable) ────────────────────────────
    interactive = [c for c in intent.chips if c["removable"] or c["editable"]]
    if not interactive:
        return None

    cols = st.columns(len(interactive) + 1)   # +1 for "Reset all" button

    for idx, chip in enumerate(interactive):
        col = cols[idx]
        cls = _KIND_CLASS.get(chip["kind"], "chip")
        kind = chip["kind"]

        with col:
            # ── REGION chip: remove button ────────────────────────────────
            if kind == "region":
                st.markdown(
                    f'<div style="font-size:.62rem;font-weight:700;color:#1e40af;'
                    f'text-transform:uppercase;letter-spacing:.06em">{chip["label"]}</div>',
                    unsafe_allow_html=True)
                if len(intent.regions) > 1:
                    # Can only remove if more than 1 region remains
                    if st.button(f"✕ {chip['value']}",
                                 key=f"{key_prefix}_rm_region_{chip['value']}",
                                 help=f"Remove {chip['value']} filter"):
                        new_intent = copy.deepcopy(intent)
                        new_intent.regions = [r for r in new_intent.regions
                                              if r != chip["value"]]
                        new_intent.chips   = _rebuild_chips(new_intent)
                        changed_intent     = new_intent
                else:
                    st.markdown(
                        f'<span class="chip chip-region">{chip["value"]}</span>',
                        unsafe_allow_html=True)

            # ── PLATFORM chip: remove button ──────────────────────────────
            elif kind == "platform":
                st.markdown(
                    f'<div style="font-size:.62rem;font-weight:700;color:#166534;'
                    f'text-transform:uppercase;letter-spacing:.06em">{chip["label"]}</div>',
                    unsafe_allow_html=True)
                if st.button(f"✕ {chip['value']}",
                             key=f"{key_prefix}_rm_plat_{chip['value']}",
                             help=f"Remove {chip['value']} filter"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.platforms = [p for p in new_intent.platforms
                                            if p != chip["value"]]
                    new_intent.chips     = _rebuild_chips(new_intent)
                    changed_intent       = new_intent

            # ── TITLE chip: editable text + remove ───────────────────────
            elif kind == "title":
                st.markdown(
                    '<div style="font-size:.62rem;font-weight:700;color:#86198f;'
                    'text-transform:uppercase;letter-spacing:.06em">Title Filter</div>',
                    unsafe_allow_html=True)
                new_title = st.text_input(
                    "Title", value=chip["value"],
                    key=f"{key_prefix}_edit_title",
                    label_visibility="collapsed",
                    placeholder="Title name…")
                c1, c2 = st.columns(2)
                if c1.button("✓", key=f"{key_prefix}_apply_title",
                             help="Apply title filter"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.title_hint = new_title.strip() or None
                    new_intent.chips      = _rebuild_chips(new_intent)
                    changed_intent        = new_intent
                if c2.button("✕", key=f"{key_prefix}_rm_title",
                             help="Remove title filter"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.title_hint = None
                    new_intent.chips      = _rebuild_chips(new_intent)
                    changed_intent        = new_intent

            # ── DATE chip: editable ───────────────────────────────────────
            elif kind == "date":
                st.markdown(
                    '<div style="font-size:.62rem;font-weight:700;color:#92400e;'
                    'text-transform:uppercase;letter-spacing:.06em">Date Range</div>',
                    unsafe_allow_html=True)
                # Offer common presets
                preset = st.selectbox(
                    "Date preset",
                    ["Last 30 Days","Last 60 Days","Last 90 Days","Last 120 Days",
                     "Last 6 Months","Last 12 Months","Year 2025","Year 2024",
                     chip["value"]],
                    index=8,   # default = current value
                    key=f"{key_prefix}_date_preset",
                    label_visibility="collapsed",
                )
                c1, c2 = st.columns(2)
                if c1.button("✓", key=f"{key_prefix}_apply_date",
                             help="Apply date preset"):
                    new_df = _parse_date_preset(preset)
                    new_intent = copy.deepcopy(intent)
                    new_intent.date_filter = new_df
                    new_intent.chips       = _rebuild_chips(new_intent)
                    changed_intent         = new_intent
                if c2.button("✕", key=f"{key_prefix}_rm_date",
                             help="Remove date filter"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.date_filter = None
                    new_intent.chips       = _rebuild_chips(new_intent)
                    changed_intent         = new_intent

            # ── EXPIRY DAYS chip: editable slider ─────────────────────────
            elif kind == "expiry_days":
                st.markdown(
                    '<div style="font-size:.62rem;font-weight:700;color:#991b1b;'
                    'text-transform:uppercase;letter-spacing:.06em">Expiry Window</div>',
                    unsafe_allow_html=True)
                new_days = st.select_slider(
                    "Days", options=[7,14,30,45,60,90,120,180,365],
                    value=min(intent.expiry_days or 90, 365),
                    key=f"{key_prefix}_expiry_days",
                    label_visibility="collapsed",
                )
                c1, c2 = st.columns(2)
                if c1.button("✓", key=f"{key_prefix}_apply_expiry",
                             help="Apply expiry window"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.expiry_days = new_days
                    new_intent.chips       = _rebuild_chips(new_intent)
                    changed_intent         = new_intent
                if c2.button("✕", key=f"{key_prefix}_rm_expiry",
                             help="Remove expiry filter"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.expiry_days = None
                    new_intent.chips       = _rebuild_chips(new_intent)
                    changed_intent         = new_intent

            # ── STATUS chip: toggle ───────────────────────────────────────
            elif kind == "status":
                st.markdown(
                    '<div style="font-size:.62rem;font-weight:700;color:#475569;'
                    'text-transform:uppercase;letter-spacing:.06em">Status</div>',
                    unsafe_allow_html=True)
                statuses = ["Active","Expired","Pending","(all)"]
                current  = intent.status_filter or "(all)"
                new_stat = st.selectbox(
                    "Status", statuses,
                    index=statuses.index(current) if current in statuses else 3,
                    key=f"{key_prefix}_status_sel",
                    label_visibility="collapsed",
                )
                if st.button("✓ Apply", key=f"{key_prefix}_apply_status"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.status_filter = None if new_stat == "(all)" else new_stat
                    new_intent.chips         = _rebuild_chips(new_intent)
                    changed_intent           = new_intent

            # ── MOVIE CATEGORY chip: toggle ───────────────────────────────
            elif kind == "movie_category":
                st.markdown(
                    '<div style="font-size:.62rem;font-weight:700;color:#9a3412;'
                    'text-transform:uppercase;letter-spacing:.06em">Category</div>',
                    unsafe_allow_html=True)
                cats = ["Theatrical","Library","HBO Original",
                        "Direct-to-Streaming","(all)"]
                cur  = intent.movie_category or "(all)"
                new_cat = st.selectbox(
                    "Category", cats,
                    index=cats.index(cur) if cur in cats else 4,
                    key=f"{key_prefix}_cat_sel",
                    label_visibility="collapsed",
                )
                if st.button("✓ Apply", key=f"{key_prefix}_apply_cat"):
                    new_intent = copy.deepcopy(intent)
                    new_intent.movie_category = None if new_cat == "(all)" else new_cat
                    new_intent.chips          = _rebuild_chips(new_intent)
                    changed_intent            = new_intent

    # ── Reset all button (last column) ────────────────────────────────────
    with cols[-1]:
        st.markdown('<div style="margin-top:18px"></div>', unsafe_allow_html=True)
        if st.button("↺ Reset filters",
                     key=f"{key_prefix}_reset_all",
                     help="Rerun original query"):
            # Re-parse original question with no chip overrides
            new_intent = preprocess(intent.raw_question,
                                    intent.regions[0] if intent.regions else "NA")
            changed_intent = new_intent

    # ── Apply change ───────────────────────────────────────────────────────
    if changed_intent is not None:
        _save_intent(key_prefix, changed_intent)
        if on_change_rerun:
            st.rerun()
        return changed_intent

    return None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _rebuild_chips(intent: QueryIntent) -> list[dict]:
    """Re-import chip builder to avoid circular — inline minimal version."""
    from utils.query_pipeline import _build_chips
    return _build_chips(intent)


def _parse_date_preset(label: str) -> Optional[DateFilter]:
    """Convert a preset label string back into a DateFilter."""
    import re
    m = re.match(r'Last (\d+) Days', label)
    if m:
        n = int(m.group(1))
        return DateFilter("last_days", n, f"Last {n} Days",
                          f"d.deal_date >= DATE('now', '-{n} days')")
    m = re.match(r'Last (\d+) Months', label)
    if m:
        n = int(m.group(1)); days = n * 30
        return DateFilter("last_months", n, f"Last {n} Months",
                          f"d.deal_date >= DATE('now', '-{days} days')")
    m = re.match(r'Year (\d{4})', label)
    if m:
        y = m.group(1)
        return DateFilter("year", int(y), f"Year {y}",
                          f"d.deal_date BETWEEN '{y}-01-01' AND '{y}-12-31'")
    return None


# ── Convenience wrapper used by page_chat & page_custom_dashboard ─────────────

def chips_query_block(
    question: str,
    selected_region: str,
    key_prefix: str = "chips",
    show_sql: bool = True,
) -> tuple[str, Optional[str], str, str, QueryIntent]:
    """
    All-in-one helper:
      1. Parses the question through the full pipeline.
      2. Renders the chips UI (handles edits/reruns internally).
      3. Returns the final (sql, error, chart_type, region_ctx, intent).

    Drop this into page_chat() or page_custom_dashboard() instead of
    the raw parse_query() call.

    Example
    ───────
    sql, err, chart_type, region_ctx, intent = chips_query_block(
        question, reg, key_prefix="chat_live", show_sql=show_sql
    )
    """
    # Check if a chip-edited intent is in session state
    stored_intent: Optional[QueryIntent] = st.session_state.get(f"_chips_{key_prefix}")

    if stored_intent and stored_intent.raw_question == question:
        intent = stored_intent
    else:
        # Fresh parse
        sql, err, chart_type, region_ctx, intent = parse_query(question, selected_region)
        if err:
            return sql, err, chart_type, region_ctx, intent

    # Render chips (may mutate session state + rerun)
    render_chips(intent, key_prefix=key_prefix, on_change_rerun=True)

    # Re-generate SQL from (possibly chip-edited) intent
    sql, gen_err, chart_type = generate(intent)
    sql, val_err = validate(sql, intent)
    err = gen_err or val_err

    region_ctx = " vs ".join(intent.regions) if len(intent.regions) > 1 else intent.regions[0]

    def parse_query(question: str, selected_region: str = "NA") -> QueryIntent:
    """Stage-1: parse raw question into a QueryIntent."""
    from utils.query_parser import QueryParser, preprocess

    clean = preprocess(question)
    result = QueryParser.generate_sql(clean, selected_region)

    return QueryIntent(
        raw=question,
        regions=[selected_region],
        platforms=result.get("platforms", []),
        title_hints=result.get("title_hints", []),
        date_filter=None,
        is_movie=result.get("is_movie", False),
        cross_intent=result.get("cross_intent"),
        sql=result.get("sql", ""),
        chart_type=result.get("chart_type", "table"),
        summary=result.get("summary", ""),
    )

    if show_sql and sql:
        import html
        st.markdown(
            f'<div class="sql-box">{html.escape(sql)}</div>',
            unsafe_allow_html=True)

    return sql, err, chart_type, region_ctx, intent
