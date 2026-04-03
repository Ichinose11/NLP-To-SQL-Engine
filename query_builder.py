def generate_sql(entities):
    """
    Maps extracted NLP entities to database columns and builds a SQL query.
    """
    column_mapping = {
        "nagpur": "city",
        "mumbai": "city",
        "pune": "city",
        "developers": "role",
        "developer": "role",
        "senior": "level",
        "junior": "level",
        "manager": "role"
    }

    conditions = []

    for entity in entities:
        if entity in column_mapping:
            column = column_mapping[entity]
            conditions.append(f"{column} = '{entity.capitalize()}'")

    base_query = "SELECT * FROM employees"

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    return base_query + ";"