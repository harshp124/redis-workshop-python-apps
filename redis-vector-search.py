import redis
import os
import numpy as np
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()  # Loads variables from .env

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, others
        os.system('clear')

def press_enter_to_continue():
    input("\nPress Enter to continue...\n")

# Initialize the embedding model once globally
model = SentenceTransformer('all-MiniLM-L6-v2')

def embed_text(text):
    """
    Encodes the input text into a normalized float32 vector 
    and returns its raw bytes for Redis storage.
    """
    embedding = model.encode(text, convert_to_numpy=True, normalize_embeddings=True)
    return embedding.astype(np.float32).tobytes()

def main():
    clear_screen()
    print("Redis Vector Search Lab (Python, Console App)")

    press_enter_to_continue()
    
    clear_screen()
    print("Step 1: Connect to Redis")
    
    
    client = redis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        username=os.getenv('REDIS_USERNAME'),
        password=os.getenv('REDIS_PASSWORD'),
        decode_responses=True)

    print(f"Connected to Redis")
    press_enter_to_continue()
    
    clear_screen()
    print("Step 2: Create vector search index with RediSearch module")

    try:
        client.execute_command("FT.DROPINDEX", "idx:vector_search")
    except redis.exceptions.ResponseError:
        pass  # ignore if index doesn't exist
    
    dim = 384  # Embedding dimension of `all-MiniLM-L6-v2`
    
    schema = [
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA",
        "id", "TEXT",
        "content", "TEXT",
        "embedding", "VECTOR", "FLAT", "6",
        "TYPE", "FLOAT32",
        "DIM", str(dim),
        "DISTANCE_METRIC", "COSINE"
    ]

    client.execute_command("FT.CREATE", "idx:vector_search", *schema)
    print(f"Index 'idx:vector_search' created with vector dimension {dim}.")
    press_enter_to_continue()
    
    clear_screen()
    print("Step 3: Insert sample data with vector embeddings into Redis")

    # More diverse, interesting sample texts across categories
    sample_texts = {
        "vs1": "smartphone with OLED display",
        "vs2": "wireless noise-cancelling headphones",
        "vs3": "non-stick frying pan",
        "vs4": "waterproof hiking jacket",
        "vs5": "carbon fiber road bike",
        "vs6": "organic dark roast coffee",
        "vs7": "lightweight carry-on suitcase",
        "vs8": "bestselling fantasy novel",
        "vs9": "natural lavender essential oil",
        "vs10": "LED desk lamp",
        "vs11": "men's running shoes",
        "vs12": "camping tent for 4 people",
        "vs13": "artisan sourdough bread",
        "vs14": "travel neck pillow",
        "vs15": "facial cleansing brush"
    }

    print(f"Embedding and inserting {len(sample_texts)} documents into Redis...")
    
    for doc_id, text in sample_texts.items():
        vector = embed_text(text)
        key = f"doc:{doc_id}"
        client.hset(key, mapping={
            "id": doc_id,
            "content": text,
            "embedding": vector
        })
    
    print("Sample data inserted successfully.")
    press_enter_to_continue()
    
    clear_screen()
    print("Step 4: Perform a vector similarity search")
    
    query = input("Enter a query string to search for similar items (e.g., 'wireless headphones'): ").strip()
    if not query:
        query = "wireless headphones"
    query_vec = embed_text(query)
    
    query_command = [
        "FT.SEARCH", "idx:vector_search",
        f"*=>[KNN 5 @embedding $vec_param AS vector_score]",
        "PARAMS", "2", "vec_param", query_vec,
        "SORTBY", "vector_score",
        "RETURN", "2", "content", "vector_score",
        "DIALECT", "2"
    ]

    print(f"\nSearching for top 5 similar items to '{query}' ...")
    results = client.execute_command(*query_command)

    total = results[0]
    if total == 0:
        print("No results found.")
    else:
        print(f"Found {total} results.")
        for i in range(total):
            key = results[1 + i * 2]
            fields = results[2 + i * 2]
            content = None
            score = None
            for j in range(0, len(fields), 2):
                if fields[j] == "content":
                    content = fields[j + 1]
                elif fields[j] == "vector_score":
                    score = float(fields[j + 1])
            print(f"Result {i+1}: content='{content}', similarity score={score:.5f}")

if __name__ == "__main__":
    main()
