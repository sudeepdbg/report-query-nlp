import re
from typing import Tuple, Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryParser:
    """Advanced query parser with semantic understanding and context awareness"""
    
    # Intent patterns with weights
    INTENT_PATTERNS = {
        'vendor_analysis': {
            'keywords': ['vendor', 'vendors', 'studio', 'studios', 'supplier', 'partner'],
            'weight': 1.0
        },
        'financial_analysis': {
            'keywords': ['spend', 'cost', 'budget', 'revenue', 'value', 'deal', 'deals', 
                        'price', 'pricing', 'profit', 'margin', 'roi'],
            'weight': 1.0
        },
        'operational_analysis': {
            'keywords': ['work', 'order', 'orders', 'task', 'tasks', 'status', 'progress',
                        'delay', 'delayed', 'overdue', 'queue', 'backlog', 'workload'],
            'weight': 1.0
        },
        'content_analysis': {
            'keywords': ['content', 'title', 'titles', 'show', 'shows', 'series', 'movie',
                        'film', 'episode', 'season', 'inventory', 'catalog', 'library'],
            'weight': 1.0
        },
        'rights_analysis': {
            'keywords': ['rights', 'license', 'licensing', 'territory', 'territories',
                        'svod', 'avod', 'tvod', 'exclusive', 'window', 'distribution'],
            'weight': 1.0
        },
        'quality_analysis': {
            'keywords': ['quality', 'rating', 'score', 'review', 'performance', 'kpi',
                        'metric', 'analytics', 'insight'],
            'weight': 1.0
        },
        'forecast_analysis': {
            'keywords': ['forecast', 'predict', 'projection', 'trend', 'future',
                        'estimate', 'outlook', 'pipeline'],
            'weight': 1.0
        },
        'comparison_analysis': {
            'keywords': ['compare', 'comparison', 'vs', 'versus', 'difference', 'both'],
            'weight': 1.5  # Higher weight for comparison queries
        }
    }
    
    # Aggregation patterns
    AGGREGATION_PATTERNS = {
        'top': {'sql': 'ORDER BY {} DESC LIMIT {}', 'type': 'ranking'},
        'bottom': {'sql': 'ORDER BY {} ASC LIMIT {}', 'type': 'ranking'},
        'average': {'sql': 'AVG({})', 'type': 'aggregate'},
        'avg': {'sql': 'AVG({})', 'type': 'aggregate'},
        'total': {'sql': 'SUM({})', 'type': 'aggregate'},
        'sum': {'sql': 'SUM({})', 'type': 'aggregate'},
        'count': {'sql': 'COUNT(*)', 'type': 'aggregate'},
        'maximum': {'sql': 'MAX({})', 'type': 'aggregate'},
        'max': {'sql': 'MAX({})', 'type': 'aggregate'},
        'minimum': {'sql': 'MIN({})', 'type': 'aggregate'},
        'min': {'sql': 'MIN({})', 'type': 'aggregate'},
        'distribution': {'sql': 'GROUP BY', 'type': 'group'},
        'breakdown': {'sql': 'GROUP BY', 'type': 'group'},
        'by': {'sql': 'GROUP BY', 'type': 'group'}
    }
    
    @classmethod
    def extract_regions_from_query(cls, question: str) -> list:
        """Extract specific regions mentioned in the query"""
        q_lower = question.lower()
        regions = []
        region_patterns = {
            'NA': r'\bna\b|\bnorth america\b|\busa\b|\bcanada\b|\bmexico\b',
            'APAC': r'\bapac\b|\basia\b|\bpacific\b|\bjapan\b|\bkorea\b|\bindia\b|\baustralia\b|\bsingapore\b',
            'EMEA': r'\bemea\b|\beurope\b|\bmiddle east\b|\bafrica\b|\buk\b|\bgermany\b|\bfrance\b',
            'LATAM': r'\blatam\b|\blatin america\b|\bsouth america\b|\bbrazil\b|\bargentina\b|\bchile\b'
        }
        
        for region, pattern in region_patterns.items():
            if re.search(pattern, q_lower):
                regions.append(region)
        
        return regions
    
    @classmethod
    def detect_intent(cls, question: str) -> Tuple[str, float]:
        """Detect the primary intent of the query with confidence score"""
        q_lower = question.lower()
        max_score = 0
        primary_intent = 'general_analysis'
        
        for intent, config in cls.INTENT_PATTERNS.items():
            score = 0
            for keyword in config['keywords']:
                if keyword in q_lower:
                    score += config['weight']
                    # Bonus for exact matches
                    if re.search(rf'\b{keyword}\b', q_lower):
                        score += 0.5
            
            if score > max_score:
                max_score = score
                primary_intent = intent
        
        return primary_intent, min(max_score, 1.0)
    
    @classmethod
    def generate_sql(cls, question: str, selected_region: str) -> Tuple[Optional[str], Optional[str], str]:
        """Generate SQL query based on natural language input"""
        
        # Detect intent
        intent, confidence = cls.detect_intent(question)
        logger.info(f"Detected intent: {intent} with confidence: {confidence}")
        
        # Extract regions mentioned in the query
        mentioned_regions = cls.extract_regions_from_query(question)
        
        # Determine which regions to query
        if mentioned_regions:
            # If query mentions specific regions, use those
            regions_to_query = mentioned_regions
            logger.info(f"Query mentions regions: {mentioned_regions}")
        else:
            # Otherwise use the selected region
            regions_to_query = [selected_region]
            logger.info(f"No regions mentioned, using selected region: {selected_region}")
        
        # Build WHERE clause for multiple regions
        if len(regions_to_query) == 1:
            where_clause = f"UPPER(region) = '{regions_to_query[0]}'"
            region_context = regions_to_query[0]
        else:
            region_list = "', '".join(regions_to_query)
            where_clause = f"UPPER(region) IN ('{region_list}')"
            region_context = " vs ".join(regions_to_query)
        
        # Generate SQL based on intent
        if intent == 'vendor_analysis':
            sql = f"""
                SELECT 
                    vendor_name,
                    COUNT(*) as deal_count,
                    SUM(deal_value) as total_value,
                    AVG(deal_value) as avg_value
                FROM deals
                WHERE {where_clause}
                GROUP BY vendor_name
                ORDER BY total_value DESC
                LIMIT 10
            """
            chart_type = 'bar'
            
        elif intent == 'financial_analysis':
            sql = f"""
                SELECT 
                    vendor_name,
                    SUM(deal_value) as total_value
                FROM deals
                WHERE {where_clause}
                GROUP BY vendor_name
                ORDER BY total_value DESC
                LIMIT 10
            """
            chart_type = 'bar'
            
        elif intent == 'operational_analysis':
            if 'status' in question.lower() or 'breakdown' in question.lower():
                sql = f"""
                    SELECT 
                        status,
                        COUNT(*) as count
                    FROM work_orders
                    WHERE {where_clause}
                    GROUP BY status
                    ORDER BY count DESC
                """
                chart_type = 'pie'
            else:
                sql = f"""
                    SELECT 
                        vendor_name,
                        COUNT(*) as order_count,
                        AVG(quality_score) as avg_quality
                    FROM work_orders
                    WHERE {where_clause}
                    GROUP BY vendor_name
                    ORDER BY order_count DESC
                    LIMIT 10
                """
                chart_type = 'bar'
                
        elif intent == 'content_analysis':
            sql = f"""
                SELECT 
                    genre,
                    COUNT(*) as title_count
                FROM content_planning
                WHERE {where_clause}
                GROUP BY genre
                ORDER BY title_count DESC
            """
            chart_type = 'bar'
                
        elif intent == 'rights_analysis':
            sql = f"""
                SELECT 
                    rights_scope,
                    COUNT(*) as deal_count,
                    SUM(deal_value) as total_value
                FROM deals
                WHERE {where_clause}
                GROUP BY rights_scope
                ORDER BY total_value DESC
            """
            chart_type = 'pie'
            
        elif intent == 'quality_analysis':
            sql = f"""
                SELECT 
                    vendor_name,
                    AVG(quality_score) as avg_quality_score,
                    COUNT(*) as total_work_orders
                FROM work_orders
                WHERE {where_clause}
                GROUP BY vendor_name
                HAVING COUNT(*) > 0
                ORDER BY avg_quality_score DESC
                LIMIT 10
            """
            chart_type = 'bar'
            
        elif intent == 'comparison_analysis':
            # Handle comparison queries specially
            if len(regions_to_query) >= 2:
                # Multi-region comparison
                sql = f"""
                    SELECT 
                        region,
                        COUNT(*) as deal_count,
                        SUM(deal_value) as total_value,
                        AVG(deal_value) as avg_value
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY region
                    ORDER BY region
                """
                chart_type = 'bar'
            else:
                # Default to vendor comparison
                sql = f"""
                    SELECT 
                        vendor_name,
                        SUM(deal_value) as total_value,
                        COUNT(*) as deal_count
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY vendor_name
                    ORDER BY total_value DESC
                    LIMIT 10
                """
                chart_type = 'bar'
            
        else:  # general_analysis or market overview
            if 'overview' in question.lower() or 'market' in question.lower():
                sql = f"""
                    SELECT 
                        region,
                        COUNT(*) as deal_count,
                        SUM(deal_value) as total_value
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY region
                    ORDER BY region
                """
            else:
                sql = f"""
                    SELECT 
                        deal_name,
                        vendor_name,
                        deal_value,
                        region,
                        status
                    FROM deals
                    WHERE {where_clause}
                    ORDER BY deal_value DESC
                    LIMIT 20
                """
            chart_type = 'bar'
        
        # Log the generated SQL
        logger.info(f"Generated SQL: {sql}")
        
        return sql.strip(), None, chart_type, region_context


# Maintain backward compatibility
def parse_query(question: str, region: str) -> Tuple[Optional[str], Optional[str], str, str]:
    """Wrapper function for backward compatibility"""
    return QueryParser.generate_sql(question, region)
