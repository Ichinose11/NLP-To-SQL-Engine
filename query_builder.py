import logging
from typing import Dict, Any, Tuple, List

logger = logging.getLogger(__name__)

# Expanded Schema Mapping for Medium-Level Queries
SCHEMA_MAPPING = {
    # --- Geographic (City) ---
    "nagpur": "city",
    "mumbai": "city",
    "pune": "city",
    "bangalore": "city",
    "hyderabad": "city",
    "delhi": "city",
    "chennai": "city",
    "noida": "city",
    "city": "city",
    "cities": "city",

    # --- Departments ---
    "engineering": "department",
    "sales": "department",
    "marketing": "department",
    "hr": "department",
    "finance": "department",
    "it": "department",
    "department": "department",
    "team": "department",

    # --- Roles & Specializations ---
    "developers": "role",
    "developer": "role",
    "managers": "role",
    "manager": "role",
    "analyst": "role",
    "scientists": "role",
    "engineer": "role",
    "devops": "role",
    "testers": "role",
    "role": "role",
    "roles": "role",

    # --- Seniority & Levels ---
    "senior": "level",
    "junior": "level",
    "lead": "level",
    "intern": "level",
    "fresher": "level",
    "director": "level",
    "level": "level",

    # --- Employment Status & Work Mode ---
    "remote": "work_mode",
    "hybrid": "work_mode",
    "onsite": "work_mode",
    "active": "status",
    "inactive": "status",
    "resigned": "status",
    "fired": "status",

    # --- Quantitative / Numeric Synonyms ---
    "salary": "salary",
    "pay": "salary",
    "compensation": "salary",
    "bonus": "bonus",
    "experience": "experience",
    "years": "experience",
    "tenure": "experience",
    "rating": "performance_score",
    "score": "performance_score",
    "performance": "performance_score",

    # --- Target Entities / Wildcards ---
    "employees": "*",
    "people": "*",
    "staff": "*",
    "workers": "*"
}

NUMERIC_COLUMNS = {"salary", "experience", "bonus", "performance_score"} 

def generate_sql(parsed_data: Dict[str, Any]) -> Tuple[str, List[Any]]:
    logger.info("Generating fully optimized safe SQL structure.")
    
    keywords = parsed_data.get('keywords', [])
    numerics = parsed_data.get('numerics', [])
    aggregations = parsed_data.get('aggregations', [])
    group_by = parsed_data.get('group_by', [])
    order_by = parsed_data.get('order_by', [])
    limit = parsed_data.get('limit', None)
    
    conditions = []
    having_conditions = []
    params = []
    having_params = []
    
    # 1. Deduplicate base keyword tracking
    unique_kws = []
    for kw in keywords:
        if kw not in unique_kws:
            unique_kws.append(kw)
    keywords = unique_kws

    # 2. Map Categorical / String Filters with Negation Isolation and Cross-Column ORs
    col_clusters = {}
    for kw in keywords:
        word = kw['word']
        negated = kw['negated']
        logic = kw.get('logic', 'AND')
        
        if word in SCHEMA_MAPPING:
            column = SCHEMA_MAPPING[word]
            if column not in NUMERIC_COLUMNS and column != "*": 
                if column not in col_clusters:
                    col_clusters[column] = {"include": [], "exclude": [], "has_or": False}
                
                if logic == "OR":
                    col_clusters[column]["has_or"] = True
                    
                if negated:
                    if word.lower() not in col_clusters[column]["exclude"]:
                        col_clusters[column]["exclude"].append(word.lower())
                else:
                    if word.lower() not in col_clusters[column]["include"]:
                        col_clusters[column]["include"].append(word.lower())

    sql_conds_formatted = []
    for column, rules in col_clusters.items():
        includes = rules["include"]
        excludes = rules["exclude"]
        cluster_logic = "OR" if rules["has_or"] and sql_conds_formatted else "AND"
        
        pieces = []
        if includes:
            if len(includes) == 1:
                pieces.append(f"lower({column}) = ?")
                params.append(includes[0])
            else:
                placeholders = ", ".join(["?"] * len(includes))
                pieces.append(f"lower({column}) IN ({placeholders})")
                params.extend(includes)
                
        if excludes:
            if len(excludes) == 1:
                pieces.append(f"lower({column}) != ?")
                params.append(excludes[0])
            else:
                placeholders = ", ".join(["?"] * len(excludes))
                pieces.append(f"lower({column}) NOT IN ({placeholders})")
                params.extend(excludes)
                
        if pieces:
            sql_conds_formatted.append({"expr": " AND ".join(pieces), "link": cluster_logic})

    for sc in sql_conds_formatted:
        if not conditions:
            conditions.append(sc["expr"])
        else:
            # Inject native OR logic bindings cross column
            conditions.append(f"{sc['link']} {sc['expr']}")

    # 3. Dynamic Aggregation Target Switching ("total salary" = SUM, "total employees" = COUNT)
    valid_group_cols = []
    for term in group_by:
        if term in SCHEMA_MAPPING:
            col = SCHEMA_MAPPING[term]
            if col not in valid_group_cols:
                valid_group_cols.append(col)

    select_items = []
    for agg in aggregations:
        func = agg['func']
        target = agg['target']
        
        mapped_col = SCHEMA_MAPPING.get(target) if target != "*" else "*"
        if not mapped_col and target != "*":
            continue
            
        if func == "TOTAL_OR_COUNT":
            func = "SUM" if mapped_col in NUMERIC_COLUMNS else "COUNT"
            
        item_str = f"{func}(*)" if mapped_col == "*" else f"{func}({mapped_col})"
        
        if item_str not in select_items:
            select_items.append(item_str)
            
    for col in valid_group_cols:
        if col not in select_items:
            select_items.insert(0, col)
            
    select_clause = ", ".join(select_items) if select_items else "*"

    # 4. Smart Target Binding for Numbers
    target_num_col = None
    for kw in reversed(keywords): # Look internally for numeric targets closest to numbers mentally
        w = kw['word']
        if w in SCHEMA_MAPPING and SCHEMA_MAPPING[w] in NUMERIC_COLUMNS:
            target_num_col = SCHEMA_MAPPING[w]
            break
            
    if not target_num_col and numerics:
        pass # Disabling dangerous 'salary' fallback per schema rules

    is_agg_target = False
    agg_func = None
    if target_num_col:
        for agg in aggregations:
            if agg['target'] in SCHEMA_MAPPING and SCHEMA_MAPPING[agg['target']] == target_num_col:
                is_agg_target = True
                f = agg['func']
                if f == "TOTAL_OR_COUNT": f = "SUM"
                agg_func = f
                break

    for num_cond in numerics:
        op = num_cond.get('operator')
        negated = num_cond.get('negated', False)
        val = num_cond.get('value')
        explicit_col = num_cond.get('target_col')
        
        active_target = explicit_col if explicit_col else target_num_col
        
        if not active_target:
            continue # Safely skip unassigned numerics without falling back
        
        left = f"{agg_func}({active_target})" if (valid_group_cols and is_agg_target and active_target == target_num_col) else active_target
        c_list = having_conditions if (valid_group_cols and is_agg_target and active_target == target_num_col) else conditions
        p_list = having_params if (valid_group_cols and is_agg_target and active_target == target_num_col) else params

        if op == "BETWEEN":
            logic = "NOT BETWEEN" if negated else "BETWEEN"
            link = "AND" if c_list else ""
            c_list.append(f"{link} {left} {logic} ? AND ?".strip())
            if logic == "OR":
                pass
            p_list.extend(val)
        else:
            if negated:
                invert_map = {'>': '<=', '<': '>=', '=': '!=', '>=': '<', '<=': '>'}
                op = invert_map.get(op, '!=')
                
            link = "AND" if c_list else ""
            c_list.append(f"{link} {left} {op} ?".strip())
            p_list.append(val)

    # 5. Pipeline Assembly
    base_query = f"SELECT {select_clause} FROM employees"

    if conditions:
        base_query += " WHERE " + " ".join(conditions) # already contains AND/OR links
        
    if valid_group_cols:
        base_query += " GROUP BY " + ", ".join(valid_group_cols)
        
    if having_conditions:
        base_query += " HAVING " + " AND ".join(having_conditions)
        params.extend(having_params)
        
    # 6. Sorting
    valid_order_cols = []
    for ord_item in order_by:
        col_name = SCHEMA_MAPPING.get(ord_item['column'])
        if col_name:
            direction = ord_item['direction']
            query_col = col_name
            for agg in aggregations:
                if agg['target'] in SCHEMA_MAPPING and SCHEMA_MAPPING[agg['target']] == col_name:
                    f = agg['func'] if agg['func'] != "TOTAL_OR_COUNT" else ("SUM" if col_name in NUMERIC_COLUMNS else "COUNT")
                    query_col = f"{f}({col_name})"
                    break
                    
            item = f"{query_col} {direction}"
            if item not in valid_order_cols:
                valid_order_cols.append(item)
                
    if order_by and not valid_order_cols and target_num_col:
        valid_order_cols.append(f"{target_num_col} {order_by[0]['direction']}")
        
    if not valid_order_cols and target_num_col:
        for kw in keywords:
            if kw.get('direction') is not None:
                valid_order_cols.append(f"{target_num_col} {kw['direction']}")
                break

    if valid_order_cols:
        base_query += " ORDER BY " + ", ".join(valid_order_cols)

    if limit:
        base_query += f" LIMIT {limit}"

    # 7. Fail Fast Defense Check
    if base_query == "SELECT * FROM employees" and not limit and not numerics and not conditions:
        raise ValueError("This text could not map to the database schema accurately. No recognizable relationships found.")

    base_query += ";"
    
    return base_query, params