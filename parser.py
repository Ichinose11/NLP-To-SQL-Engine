import logging
import nltk
from typing import Dict, List, Any
import re
from nltk.corpus import stopwords
from query_builder import SCHEMA_MAPPING, NUMERIC_COLUMNS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True) 
nltk.download('averaged_perceptron_tagger_eng', quiet=True)
nltk.download('stopwords', quiet=True)

STOP_WORDS = set(stopwords.words('english'))

def parse_query(user_input: str) -> Dict[str, Any]:
    logger.info("Starting robust parsed stress testing")
    
    if not user_input or not isinstance(user_input, str):
        return {}

    text = user_input.lower()
    
    try:
        tokens = nltk.word_tokenize(text)
        tagged_tokens = nltk.pos_tag(tokens)
    except Exception as e:
        logger.error(f"NLTK pos_tagging failed: {e}")
        raise ValueError("Failed to parse the input sentence.")

    keywords = []
    numeric_conditions = []
    aggregations = []
    group_by = []
    order_by = []
    limit = None
    
    operator_map = {
        'over': '>', 'greater': '>', 'more': '>', 'above': '>',
        'under': '<', 'less': '<', 'below': '<',
        'exactly': '=', 'equal': '=',
        'most': '<=', 'least': '>=',
        'top': 'DESC', 'highest': 'DESC', 'maximum': 'DESC', 'max': 'DESC', 'paid': 'DESC',
        'bottom': 'ASC', 'lowest': 'ASC', 'minimum': 'ASC', 'min': 'ASC'
    }

    aggregation_map = {
        'average': 'AVG', 'avg': 'AVG', 'mean': 'AVG',
        'total': 'TOTAL_OR_COUNT', 'count': 'COUNT', 'number': 'COUNT', 'amount': 'SUM',
        'sum': 'SUM',
        'max': 'MAX', 'maximum': 'MAX', 'highest': 'MAX',
        'min': 'MIN', 'minimum': 'MIN', 'lowest': 'MIN'
    }
    
    current_operator = '=' 
    is_negated = False
    parse_state = "FILTER"
    order_direction = None 
    is_between = False
    between_vals = []
    logic_operator = "AND"
    
    word_to_num = {'one':1, 'two':2, 'three':3, 'four':4, 'five':5, 'six':6, 'seven':7, 'eight':8, 'nine':9, 'ten':10}
    
    for i, (word, tag) in enumerate(tagged_tokens):
        clean_word = re.sub(r'[^\w\s]', '', word)
        
        if not clean_word and word not in ('>', '<', '=', '>=', '<='):
            continue
            
        # 1. Logical OR Mapping
        if clean_word == "or":
            logic_operator = "OR"
            continue

        # 2. Negations
        if clean_word in ('not', 'excluding', 'except', 'without'):
            is_negated = True
            continue

        # 3. Ranges
        if clean_word == "between":
            is_between = True
            continue

        # 4. Limits
        if clean_word == "top" or clean_word == "limit":
            if i + 1 < len(tagged_tokens):
                next_word = tagged_tokens[i+1][0]
                if next_word.isnumeric():
                    limit = int(next_word)
                elif next_word in word_to_num:
                    limit = word_to_num[next_word]
        
        # 5. Resolving Orders & Aggregates explicitly
        if clean_word in operator_map:
            val = operator_map[clean_word]
            if val in ('ASC', 'DESC'):
                order_direction = val
            else:
                current_operator = val
            
            # words like 'highest' trigger both DESC order and MAX aggregator optionally
            if clean_word not in aggregation_map:
                continue

        elif word in ('>', '<', '=', '>=', '<='):
            current_operator = word
            continue
            
        # 5. Strict Structural Validations for Advanced Logic
        if clean_word in ("group", "grouped"):
            for j in range(i+1, min(i+4, len(tagged_tokens))):
                if tagged_tokens[j][0] == "by":
                    parse_state = "GROUP"
                    break
            continue
        elif clean_word in ("order", "sort", "rank"):
            if i + 1 < len(tagged_tokens) and tagged_tokens[i+1][0] == "by":
                parse_state = "ORDER"
            continue
        elif clean_word in ("where", "filter", "having", "only"):
            parse_state = "FILTER"
            continue

        # 7. Safe Numeric Operations
        is_num = False
        try:
            numeric_val = float(word.replace(',', '').replace('k', '000').replace('m', '000000'))
            is_num = True
        except ValueError:
            pass

        if is_num:
            target_col = None
            for j in [i-1, i-2, i+1, i+2]:
                if 0 <= j < len(tagged_tokens):
                    adj_word = re.sub(r'[^\w\s]', '', tagged_tokens[j][0])
                    if adj_word in SCHEMA_MAPPING and SCHEMA_MAPPING[adj_word] in NUMERIC_COLUMNS:
                        target_col = SCHEMA_MAPPING[adj_word]
                        break

            if limit is not None and numeric_val == limit:
                pass # Already parsed as limit 
            else:
                if is_between:
                    between_vals.append(numeric_val)
                    if len(between_vals) == 2:
                        numeric_conditions.append({
                            "operator": "BETWEEN",
                            "value": between_vals,
                            "negated": is_negated,
                            "target_col": target_col
                        })
                        between_vals = []
                        is_between = False
                        is_negated = False
                else:
                    numeric_conditions.append({
                        "operator": current_operator,
                        "value": numeric_val,
                        "negated": is_negated,
                        "target_col": target_col
                    })
                    current_operator = '=' 
                    is_negated = False
            continue

        if clean_word == "and" and is_between:
            continue
            
        # 8. Semantic Aggregation Assignments
        if clean_word in aggregation_map:
            agg_func = aggregation_map[clean_word]
            
            target = "*"
            # Prefer schema fields first
            for j in range(i+1, min(i+4, len(tagged_tokens))):
                nw = re.sub(r'[^\w\s]', '', tagged_tokens[j][0])
                if nw in SCHEMA_MAPPING: # Bind safely
                    target = nw
                    break
                    
            if target == "*":
                # Fallback to general nouns if tracking 'total developers' etc
                for j in range(i+1, len(tagged_tokens)):
                    nw = re.sub(r'[^\w\s]', '', tagged_tokens[j][0])
                    if nw and nw not in STOP_WORDS and tagged_tokens[j][1].startswith('N'):
                        target = nw
                        break
            aggregations.append({"func": agg_func, "target": target})
            continue

        # 9. POS Verification & Keyword Injection
        if tag in ('NN', 'NNS', 'NNP', 'JJ', 'VBG') or clean_word in ('developers', 'managers', 'senior', 'junior'): 
            if clean_word not in STOP_WORDS:
                payload = {"word": clean_word, "negated": is_negated, "direction": order_direction, "logic": logic_operator}
                if parse_state == "GROUP":
                    group_by.append(clean_word)
                elif parse_state == "ORDER":
                    order_by.append({"column": clean_word, "direction": order_direction})
                else:
                    keywords.append(payload)
                is_negated = False
                order_direction = None
                logic_operator = "AND"
                
    return {
        "keywords": keywords,
        "numerics": numeric_conditions,
        "aggregations": aggregations,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit
    }