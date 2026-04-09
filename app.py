import streamlit as st
import logging
from parser import parse_query
from query_builder import generate_sql

# Standardize logging configuration for the production web app
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(page_title="NLP to SQL Engine", page_icon="🔍", layout="centered")

# Header Section
st.title("NLP-to-SQL Engine")
st.markdown("""
This engine securely translates natural language into strictly **parameterized SQL queries** using traditional NLP.
It relies on NLTK for POS-tagging, stopword filtering, and math state machines.
""")

st.divider()

user_input = st.text_input(
    "Ask a question about the employee database:", 
    placeholder="e.g., Find senior developers in Pune with a salary over 80,000"
)

if st.button("Generate SQL", type="primary"):
    if user_input.strip():
        with st.spinner("Analyzing text and securing query..."):
            try:
                # 1. Execute Text Parsing
                logger.info(f"Processing Request: {user_input}")
                parsed_data = parse_query(user_input)
                
                # 2. Build Safe Parameterized SQL
                sql_query, sql_params = generate_sql(parsed_data)
                
                # Render Safe Output Formats
                st.success("Query Securley Translated!")
                
                # Display base query
                st.subheader("Base Secure SQL Query String")
                st.code(sql_query, language="sql")
                
                # Display parameterized isolated variables
                st.subheader("Isolated Statement Parameters")
                st.info(f"Arguments: {sql_params}")
                
                # Expandable diagnostics for visibility
                with st.expander("See how the NLP Engine parsed this"):
                    st.write("**Processed Entities & Numerics:**")
                    st.json(parsed_data)
                    
            except ValueError as ve:
                logger.warning(f"Value Error processed safely: {ve}")
                st.warning(f"Could not translate your query securely: {ve}")
            except Exception as e:
                logger.error(f"Unexpected crash preventing SQL generation: {e}", exc_info=True)
                st.error("A system error occurred while generating the safe query.")
    else:
        st.warning("Please enter a query about the database to start.")