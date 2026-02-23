import re

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using rules with synonym support.
    Returns (sql, error_message) – error_message is None if successful.
    """
    q = question.lower().strip()
    
    # Helper to add region filter
    def add_region_filter(sql, table):
        if region and region != 'GLOBAL':
            if 'WHERE' in sql:
                sql += f" AND {table}.region = '{region}'"
            else:
                sql += f" WHERE {table}.region = '{region}'"
        return sql
    
    # Team hint
    team_table_map = {
        'leadership': None,
        'product': 'work_orders',
        'content planning': 'content_planning',
        'deals': 'deals'
    }
    primary_table = team_table_map.get(team, None)
    
    # --- Count queries ---
    if re.search(r'how many|count|total number of', q):
        # Delayed / at risk
        if re.search(r'delayed|at risk|late|behind schedule', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        # MAX Australia
        elif re.search(r'max australia|australia|max au', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        # MAX US
        elif re.search(r'max us|united states|usa', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        # Active deals
        elif re.search(r'active deals|active', q) and 'deal' in q:
            table = 'deals'
            condition = "status = 'Active'"
        # Pending approval
        elif re.search(r'pending approval|pending deals|pending', q):
            table = 'deals'
            condition = "status = 'Pending'"
        # In progress
        elif re.search(r'in progress|ongoing|current', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        # Not ready
        elif re.search(r'not ready|unfinished|pending', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                return None, "I couldn't understand what to count."
        
        sql = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
        return sql, None
    
    # --- List queries ---
    if re.search(r'show|list|get|what|find|display', q):
        # Top vendors
        if re.search(r'vendor a|top vendors|vendor performance', q):
            sql = "SELECT vendor, COUNT(*) as count FROM work_orders"
            if region and region != 'GLOBAL':
                sql += f" WHERE region = '{region}'"
            sql += " GROUP BY vendor ORDER BY count DESC"
            return sql, None
        
        # Delayed work orders
        if re.search(r'delayed|at risk|late|behind schedule', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        # MAX Australia content
        elif re.search(r'max australia|australia|max au', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        # MAX US content
        elif re.search(r'max us|united states|usa', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        # Active deals
        elif re.search(r'active deals|active', q) and 'deal' in q:
            table = 'deals'
            condition = "status = 'Active'"
        # Pending approval deals
        elif re.search(r'pending approval|pending deals|pending', q):
            table = 'deals'
            condition = "status = 'Pending'"
        # In progress work orders
        elif re.search(r'in progress|ongoing|current', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        # Not ready content
        elif re.search(r'not ready|unfinished|pending', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                suggestions = []
                if dashboard == 'executive' or not primary_table:
                    suggestions.append('Show me all content for MAX Australia')
                    suggestions.append('How many content items are Not Ready?')
                if dashboard == 'workorders' or not primary_table:
                    suggestions.append('Show me all delayed work orders')
                    suggestions.append('List work orders in progress')
                if dashboard == 'deals' or not primary_table:
                    suggestions.append('Show me all active deals')
                    suggestions.append('List top vendors by work orders')
                suggestion_text = " You could try: " + ", ".join(f'"{s}"' for s in suggestions[:4])
                return None, f"I couldn't understand that query. Please try rephrasing.{suggestion_text}"
        
        sql = f"SELECT * FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
        return sql, None
    
    # --- No match ---
    suggestions = [
        "Show me all delayed work orders",
        "How many content items for MAX Australia?",
        "List active deals",
        "Show top vendors by work orders"
    ]
    suggestion_text = " You could try: " + ", ".join(f'"{s}"' for s in suggestions)
    return None, f"I couldn't understand that query. Please try rephrasing.{suggestion_text}"
