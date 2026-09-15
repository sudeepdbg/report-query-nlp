# utils/semantic_layer.py
"""
Distribution Semantic Layer — POC MVP
Single source of truth for metrics & dimensions.
Consumed by: Dashboard Builder UI + Vantage NL pipeline.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any

# ── Metric Contracts ────────────────────────────────────────────────────────
@dataclass
class Metric:
    key: str
    name: str
    business_question: str
    definition: str
    formula: str
    grain: str
    required_dimensions: List[str]
    sql_template: str          # parameterized; UI/Vantage fill filters
    owner: str
    steward: str
    status: str = "Certified"  # Draft | Validated | Certified

@dataclass
class Dimension:
    key: str
    name: str
    description: str
    column: str
    values_example: List[str]

# ── The 5 Core Metrics ──────────────────────────────────────────────────────
METRICS: Dict[str, Metric] = {
    "active_offerings": Metric(
        key="active_offerings",
        name="Active Offerings",
        business_question="How many offerings are active in the selected period?",
        definition="Distinct offerings whose availability window overlaps the reporting period.",
        formula="COUNT DISTINCT offering_id WHERE start_date <= period_end AND (end_date IS NULL OR end_date >= period_start)",
        grain="One offering per content × partner × market × availability window",
        required_dimensions=["date", "partner", "market", "region", "title"],
        sql_template="""
            SELECT {group_by}
                 , COUNT(DISTINCT offering_id) AS active_offerings
            FROM dist_offerings o
            JOIN dim_partner p ON o.partner_id = p.partner_id
            JOIN dim_market  m ON o.market_id  = m.market_id
            WHERE o.start_date <= :period_end
              AND (o.end_date IS NULL OR o.end_date >= :period_start)
              {where_filters}
            GROUP BY {group_by}
        """,
        owner="Distribution Product",
        steward="DW / Analytics Engineering",
    ),
    "distributions": Metric(
        key="distributions",
        name="Distributions",
        business_question="How many offerings were initially delivered?",
        definition="Distinct offerings with an initial delivered date inside the reporting period.",
        formula="COUNT DISTINCT offering_id WHERE initial_delivered_date BETWEEN period_start AND period_end",
        grain="One distribution event per offering",
        required_dimensions=["date", "partner", "market", "region"],
        sql_template="""
            SELECT {group_by}
                 , COUNT(DISTINCT offering_id) AS distributions
            FROM dist_deliveries d
            JOIN dim_partner p ON d.partner_id = p.partner_id
            JOIN dim_market  m ON d.market_id  = m.market_id
            WHERE d.initial_delivered_date BETWEEN :period_start AND :period_end
              {where_filters}
            GROUP BY {group_by}
        """,
        owner="Distribution Product",
        steward="DW / Analytics Engineering",
    ),
    "otd_pct": Metric(
        key="otd_pct",
        name="OTD %",
        business_question="What % of eligible deliveries were on time?",
        definition="Share of eligible completed deliveries delivered on/before the target date.",
        formula="on_time_deliveries / eligible_completed_deliveries × 100",
        grain="Per delivery",
        required_dimensions=["date", "partner", "market", "region"],
        sql_template="""
            SELECT {group_by}
                 , 100.0 * SUM(CASE WHEN completed_date <= target_date THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*),0) AS otd_pct
            FROM dist_deliveries d
            JOIN dim_partner p ON d.partner_id = p.partner_id
            JOIN dim_market  m ON d.market_id  = m.market_id
            WHERE d.delivery_status = 'completed'
              AND d.initial_delivered_date BETWEEN :period_start AND :period_end
              {where_filters}
            GROUP BY {group_by}
        """,
        owner="Distribution Product",
        steward="DW / Analytics Engineering",
    ),
    "distribution_footprint": Metric(
        key="distribution_footprint",
        name="Distribution Footprint",
        business_question="Where is content available?",
        definition="Distinct partner × market combinations with at least one active offering.",
        formula="COUNT DISTINCT (partner_id, market_id) for active offerings",
        grain="Partner × Market",
        required_dimensions=["region"],
        sql_template="""
            SELECT {group_by}
                 , COUNT(DISTINCT (o.partner_id, o.market_id)) AS distribution_footprint
            FROM dist_offerings o
            JOIN dim_market m ON o.market_id = m.market_id
            WHERE o.start_date <= :period_end
              AND (o.end_date IS NULL OR o.end_date >= :period_start)
              {where_filters}
            GROUP BY {group_by}
        """,
        owner="Distribution Product",
        steward="DW / Analytics Engineering",
    ),
    "delivered_asset_volume": Metric(
        key="delivered_asset_volume",
        name="Delivered Asset Volume",
        business_question="How many assets/components were delivered?",
        definition="Distinct assets successfully delivered during the period.",
        formula="COUNT DISTINCT asset_id WHERE delivery_status = 'successful' AND delivered_date IN period",
        grain="Per asset",
        required_dimensions=["date", "partner", "market", "region"],
        sql_template="""
            SELECT {group_by}
                 , COUNT(DISTINCT asset_id) AS delivered_asset_volume
            FROM dist_assets a
            JOIN dim_partner p ON a.partner_id = p.partner_id
            WHERE a.delivery_status = 'successful'
              AND a.delivered_date BETWEEN :period_start AND :period_end
              {where_filters}
            GROUP BY {group_by}
        """,
        owner="Distribution Product",
        steward="DW / Analytics Engineering",
    ),
}

# ── The 6 Core Dimensions ───────────────────────────────────────────────────
DIMENSIONS: Dict[str, Dimension] = {
    "date":    Dimension("date",    "Date",    "Reporting period / trend grouping", "date_key",      ["2026-Q1","2026-Q2"]),
    "partner": Dimension("partner", "Partner", "Destination receiving the offering", "partner_name",  ["HBO Max","Roku","Amazon"]),
    "market":  Dimension("market",  "Market",  "Country / commercial market",        "market_name",   ["US","Mexico","UK"]),
    "region":  Dimension("region",  "Region",  "Governed grouping of markets",       "region_name",   ["NA","LATAM","EMEA","APAC"]),
    "title":   Dimension("title",   "Title",   "Content title at agreed grain",      "title_name",    ["House of the Dragon","Succession"]),
    "offering":Dimension("offering","Offering", "Content × partner × market instance","offering_id",   ["O-12345","O-67890"]),
}

# ── Public API consumed by UI + Vantage ─────────────────────────────────────
def get_metric(key: str) -> Metric:
    return METRICS[key]

def list_metrics() -> List[Metric]:
    return list(METRICS.values())

def compatible_dimensions(metric_key: str) -> List[Dimension]:
    m = METRICS[metric_key]
    return [DIMENSIONS[k] for k in m.required_dimensions if k in DIMENSIONS]

def build_query(metric_key: str, group_by: List[str], filters: Dict[str, Any]) -> str:
    """Single entry point — used by BOTH the Dashboard UI and Vantage."""
    m = METRICS[metric_key]
    gb_sql = ", ".join(f"p.partner_name" if g=="partner" else
                       f"m.market_name"  if g=="market"  else
                       f"m.region_name"  if g=="region"  else g
                       for g in group_by) or "'ALL'"
    where_parts = []
    if filters.get("region"):
        where_parts.append(f"AND m.region_name = '{filters['region']}'")
    if filters.get("partner"):
        where_parts.append(f"AND p.partner_name = '{filters['partner']}'")
    sql = m.sql_template.format(group_by=gb_sql, where_filters=" ".join(where_parts))
    return " ".join(sql.split())
