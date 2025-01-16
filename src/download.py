from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import os
from math import ceil

# Adjust paths for imports
current_file = os.path.abspath(__file__)
lux_overlaps_root = os.path.dirname(os.path.dirname(current_file))
parent_dir = os.path.dirname(lux_overlaps_root)
sys.path.insert(0, parent_dir)

from dotenv import load_dotenv
from pipeline.config import Config

# Load environment variables and initialize configuration
load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.instantiate_all()

def fetch_chunk(chunk_range, query_word, recordcache, table_name):
    """Fetch records for a specific chunk."""
    start_id, end_id = chunk_range
    search_pattern = f"%{query_word}%"

    sql_query = f"""
        SELECT 
            identified_by->>'content' AS name
        FROM {table_name},
             LATERAL jsonb_array_elements(data::jsonb->'identified_by') AS identified_by,
             LATERAL jsonb_array_elements(identified_by->'classified_as') AS classified_as
        WHERE data::jsonb->>'type' = 'Person'
          AND classified_as->>'id' = 'http://vocab.getty.edu/aat/300404670'
          AND identified_by->>'content' ILIKE %s
          AND identifier BETWEEN %s AND %s;
    """

    results = []
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query, (search_pattern, start_id, end_id))
        results = [row['name'] for row in cur.fetchall()]
    
    return results

def fetch_data(query_word, recordcache, cache, chunk_size=50000):
    """Fetch data in parallel using threading."""
    table_name = f"{cache}_record_cache"

    # Determine ID range for chunking
    with recordcache._cursor(internal=False) as cur:
        cur.execute(f"SELECT MIN(identifier), MAX(identifier) FROM {table_name}")
        min_id, max_id = cur.fetchone()

    if not min_id or not max_id:
        print("No records found.")
        return []

    # Generate chunk ranges
    chunks = [(i, min(i + chunk_size - 1, max_id)) for i in range(int(min_id), int(max_id) + 1, chunk_size)]
    
    # Process chunks with threading
    results = []
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_chunk, chunk, query_word, recordcache, table_name): chunk
            for chunk in chunks
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Chunks"):
            results.extend(future.result())
    
    return results

def main():
    if len(sys.argv) < 4 or sys.argv[2] != "--cache":
        print("Usage: python script.py <query_word> --cache <cache_name>")
        sys.exit(1)

    # Extract query word and cache
    query_word = sys.argv[1]
    cache = sys.argv[3]

    # Determine if cache is internal or external
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    print(f"Fetching records for '{query_word}' in cache '{cache}'...")
    results = fetch_data(query_word, recordcache, cache)

    print("Query completed. Results:")
    for result in tqdm(results, desc="Results"):
        print(result)

if __name__ == "__main__":
    main()

