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
    
    # Time period mappings
    TIME_PERIODS = {
        'today': "date('now')",
        'yesterday': "date('now', '-1 day')",
        'this week': "strftime('%W', date('now')) = strftime('%W', date('now'))",
        'last week': "date(deal_date) >= date('now', '-7 days')",
        'this month': "strftime('%m', deal_date) = strftime('%m', 'now')",
        'last month': "date(deal_date) >= date('now', '-30 days')",
        'this quarter': "julianday(deal_date) >= julianday(date('now', 'start of month', '-3 months'))",
        'last quarter': "date(deal_date) >= date('now', '-90 days')",
        'this year': "strftime('%Y', deal_date) = strftime('%Y', 'now')",
        'last year': "date(deal_date) >= date('now', '-365 days')",
        'ytd': "strftime('%Y', deal_date) = strftime('%Y', 'now')",
        'year to date': "strftime('%Y', deal_date) = strftime('%Y', 'now')"
    }
    
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
    def extract_aggregations(cls, question: str) -> Dict[str, Any]:
        """Extract aggregation requirements from the query"""
        q_lower = question.lower()
        aggregations = {
            'type': None,
            'field': None,
            'limit': 10,
            'group_by': None
        }
        
        # Check for aggregation patterns
        for pattern, config in cls.AGGREGATION_PATTERNS.items():
            if pattern in q_lower:
                aggregations['type'] = pattern
                
                # Try to extract field for aggregation
                field_match = re.search(rf'{pattern}\s+(\w+)', q_lower)
                if field_match:
                    aggregations['field'] = field_match.group(1)
                
                # Extract limit for top/bottom queries
                if pattern in ['top', 'bottom']:
                    limit_match = re.search(rf'{pattern}\s+(\d+)', q_lower)
                    if limit_match:
                        aggregations['limit'] = int(limit_match.group(1))
                
                break
        
        # Check for group by
        group_patterns = ['by', 'per', 'for each']
        for pattern in group_patterns:
            if pattern in q_lower:
                words = q_lower.split()
                idx = words.index(pattern) if pattern in words else -1
                if idx != -1 and idx + 1 < len(words):
                    aggregations['group_by'] = words[idx + 1]
        
        return aggregations
    
    @classmethod
    def extract_time_period(cls, question: str) -> Optional[str]:
        """Extract time period constraints from the query"""
        q_lower = question.lower()
        
        for period, condition in cls.TIME_PERIODS.items():
            if period in q_lower:
                return condition
        
        return None
    
    @classmethod
    def extract_fields(cls, question: str) -> Dict[str, str]:
        """Extract specific fields mentioned in the query"""
        q_lower = question.lower()
        fields = {}
        
        # Common field mappings
        field_mappings = {
            'vendor': 'vendor_name',
            'vendors': 'vendor_name',
            'studio': 'vendor_name',
            'studios': 'vendor_name',
            'deal': 'deal_name',
            'deals': 'deal_name',
            'value': 'deal_value',
            'amount': 'deal_value',
            'price': 'deal_value',
            'cost': 'deal_value',
            'budget': 'budget',
            'revenue': 'deal_value',
            'status': 'status',
            'region': 'region',
            'country': 'country',
            'rights': 'rights_scope',
            'type': 'deal_type',
            'date': 'deal_date',
            'time': 'deal_date',
            'rating': 'rating',
            'score': 'quality_score',
            'language': 'language',
            'format': 'format',
            'genre': 'genre'
        }
        
        for term, field in field_mappings.items():
            if term in q_lower:
                fields[term] = field
        
        return fields
    
    @classmethod
    def generate_sql(cls, question: str, region: str) -> Tuple[Optional[str], Optional[str], str]:
        """Generate SQL query based on natural language input"""
        
        # Detect intent
        intent, confidence = cls.detect_intent(question)
        logger.info(f"Detected intent: {intent} with confidence: {confidence}")
        
        # Extract components
        aggregations = cls.extract_aggregations(question)
        time_period = cls.extract_time_period(question)
        fields = cls.extract_fields(question)
        
        # Base WHERE clause
        where_clause = f"UPPER(region) = '{region.upper()}'"
        if time_period:
            where_clause += f" AND {time_period}"
        
        # Generate SQL based on intent
        if intent == 'vendor_analysis':
            if aggregations['type'] in ['top', 'bottom']:
                field = aggregations['field'] or 'deal_value'
                order = 'DESC' if aggregations['type'] == 'top' else 'ASC'
                sql = f"""
                    SELECT 
                        v.vendor_name,
                        COUNT(d.deal_id) as deal_count,
                        SUM(d.deal_value) as total_value,
                        AVG(d.deal_value) as avg_value,
                        v.rating,
                        v.certification_level
                    FROM vendors v
                    LEFT JOIN deals d ON v.vendor_id = d.vendor_id
                    WHERE {where_clause}
                    GROUP BY v.vendor_id
                    ORDER BY total_value {order}
                    LIMIT {aggregations['limit']}
                """
            else:
                sql = f"""
                    SELECT 
                        v.vendor_name,
                        v.rating,
                        v.vendor_type,
                        v.certification_level,
                        COUNT(d.deal_id) as active_deals,
                        SUM(d.deal_value) as total_deal_value
                    FROM vendors v
                    LEFT JOIN deals d ON v.vendor_id = d.vendor_id
                    WHERE {where_clause}
                    GROUP BY v.vendor_id
                    ORDER BY total_deal_value DESC
                """
            chart_type = 'bar'
            
        elif intent == 'financial_analysis':
            if aggregations['type'] in ['total', 'sum', 'average', 'avg']:
                agg_func = 'SUM' if aggregations['type'] in ['total', 'sum'] else 'AVG'
                field = aggregations['field'] or 'deal_value'
                sql = f"""
                    SELECT 
                        COALESCE(vendor_name, 'Total') as vendor,
                        {agg_func}({field}) as value
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY vendor_name
                    ORDER BY value DESC
                """
            elif aggregations['type'] == 'breakdown' or aggregations['group_by']:
                group_field = aggregations['group_by'] or 'deal_type'
                sql = f"""
                    SELECT 
                        {group_field} as category,
                        COUNT(*) as count,
                        SUM(deal_value) as total_value
                    FROM deals
                    WHERE {where_clause}
                    GROUP BY {group_field}
                    ORDER BY total_value DESC
                """
            else:
                sql = f"""
                    SELECT 
                        deal_name,
                        vendor_name,
                        deal_value,
                        currency,
                        status,
                        deal_date,
                        rights_scope
                    FROM deals
                    WHERE {where_clause}
                    ORDER BY deal_value DESC
                    LIMIT 50
                """
            chart_type = 'bar' if 'GROUP BY' in sql else 'line'
            
        elif intent == 'operational_analysis':
            if 'status' in question.lower() or 'breakdown' in question.lower():
                sql = f"""
                    SELECT 
                        status,
                        COUNT(*) as count,
                        AVG(quality_score) as avg_quality,
                        SUM(actual_hours) as total_hours
                    FROM work_orders
                    WHERE {where_clause}
                    GROUP BY status
                    ORDER BY count DESC
                """
                chart_type = 'pie'
            elif 'delay' in question.lower() or 'overdue' in question.lower():
                sql = f"""
                    SELECT 
                        title_name,
                        vendor_name,
                        due_date,
                        priority,
                        julianday('now') - julianday(due_date) as days_overdue,
                        status
                    FROM work_orders
                    WHERE {where_clause} 
                        AND status != 'Completed' 
                        AND date(due_date) < date('now')
                    ORDER BY days_overdue DESC
                """
                chart_type = 'bar'
            else:
                sql = f"""
                    SELECT 
                        work_type,
                        COUNT(*) as task_count,
                        AVG(estimated_hours) as avg_estimated_hours,
                        AVG(actual_hours) as avg_actual_hours,
                        AVG(quality_score) as avg_quality
                    FROM work_orders
                    WHERE {where_clause}
                    GROUP BY work_type
                    ORDER BY task_count DESC
                """
                chart_type = 'bar'
                
        elif intent == 'content_analysis':
            if 'genre' in question.lower():
                sql = f"""
                    SELECT 
                        genre,
                        COUNT(*) as title_count,
                        AVG(critical_score) as avg_critical_score,
                        AVG(audience_score) as avg_audience_score
                    FROM content_planning
                    WHERE {where_clause}
                    GROUP BY genre
                    ORDER BY title_count DESC
                """
                chart_type = 'bar'
            elif 'release' in question.lower():
                sql = f"""
                    SELECT 
                        strftime('%Y-%m', target_release_date) as release_month,
                        COUNT(*) as releases,
                        SUM(budget) as total_budget
                    FROM content_planning
                    WHERE {where_clause}
                    GROUP BY release_month
                    ORDER BY release_month
                """
                chart_type = 'line'
            else:
                sql = f"""
                    SELECT 
                        content_title,
                        status,
                        language,
                        format,
                        target_release_date,
                        budget
                    FROM content_planning
                    WHERE {where_clause}
                    ORDER BY target_release_date
                    LIMIT 50
                """
                chart_type = 'bar'
                
        elif intent == 'rights_analysis':
            sql = f"""
                SELECT 
                    rights_scope,
                    COUNT(*) as deal_count,
                    SUM(deal_value) as total_value,
                    AVG(territories_covered) as avg_territories
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
                    AVG(rating) as avg_rating,
                    AVG(quality_score) as avg_quality_score,
                    COUNT(*) as total_work_orders
                FROM work_orders w
                JOIN vendors v ON w.vendor_id = v.vendor_id
                WHERE {where_clause}
                GROUP BY w.vendor_id
                HAVING total_work_orders > 5
                ORDER BY avg_quality_score DESC
            """
            chart_type = 'bar'
            
        else:  # general_analysis
            # Provide a comprehensive overview
            sql = f"""
                SELECT 
                    'Deals' as category,
                    COUNT(*) as count,
                    SUM(deal_value) as total_value
                FROM deals
                WHERE {where_clause}
                UNION ALL
                SELECT 
                    'Work Orders',
                    COUNT(*),
                    SUM(cost)
                FROM work_orders
                WHERE {where_clause}
                UNION ALL
                SELECT 
                    'Content',
                    COUNT(*),
                    SUM(budget)
                FROM content_planning
                WHERE {where_clause}
            """
            chart_type = 'bar'
        
        # Log the generated SQL
        logger.info(f"Generated SQL: {sql}")
        
        return sql.strip(), None, chart_type


# Maintain backward compatibility
def parse_query(question: str, region: str) -> Tuple[Optional[str], Optional[str], str]:
    """Wrapper function for backward compatibility"""
    return QueryParser.generate_sql(question, region)
