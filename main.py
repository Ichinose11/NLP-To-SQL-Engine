from parser import parse_query

def main():
    print("--- NLP to SQL Engine Initialized ---")
    
    # Sample queries to test our extraction
    queries = [
        "Show me all the senior developers in Nagpur.",
        "Get the sales numbers for the new laptops.",
        "Find me cheap flights to Mumbai."
    ]
    
    for q in queries:
        print(f"\nUser Query: '{q}'")
        extracted_entities = parse_query(q)
        print(f"Extracted Parameters: {extracted_entities}")

if __name__ == "__main__":
    main()