import nltk

# Download required NLTK datasets (will skip if already downloaded)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)

def parse_query(user_input):
    """
    Takes a natural language string and extracts key entities 
    using Part-of-Speech (POS) tagging.
    """
    # 1. Tokenize: Break the sentence into words
    tokens = nltk.word_tokenize(user_input)
    
    # 2. POS Tagging: Identify nouns, verbs, adjectives, etc.
    tagged_tokens = nltk.pos_tag(tokens)
    
    # 3. Filter: We generally only care about Nouns (NN, NNS, NNP) and Adjectives (JJ) for database queries
    search_parameters = []
    for word, tag in tagged_tokens:
        if tag in ('NN', 'NNS', 'NNP', 'JJ'):
            # Convert to lowercase for easier database matching later
            search_parameters.append(word.lower())
            
    return search_parameters