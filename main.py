from parser import parse_query
from query_builder import generate_sql

def main():
    print("--- NLP to SQL Engine Initialized ---\n")
    
    # We will test queries that match our current employee database rules
    queries = [
        "Show me all the senior developers in Nagpur.",
        "Get me the junior developers.",
        "Find a manager in Pune.",
        "Find me cheap flights to Mumbai." # This one will only match 'Mumbai' based on our current rules!
    ]
    
    for q in queries:
        print(f"User Query: '{q}'")
        
        # Step 1: Extract entities using NLTK
        extracted_entities = parse_query(q)
        print(f"Extracted: {extracted_entities}")
        
        # Step 2: Build the SQL query
        sql_query = generate_sql(extracted_entities)
        print(f"Generated SQL: {sql_query}\n")
        print("-" * 40)

if __name__ == "__main__":
    main()