from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import os

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

def fetch_chunk(offset, limit, query_word, recordcache, table_name):    
    """Fetch records for a specific chunk."""
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
        LIMIT %s OFFSET %s;
    """

    results = []
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query, (search_pattern, limit, offset))
        results = [row['name'] for row in cur.fetchall()]
    
    return results

def fetch_data(query_word, recordcache, cache, chunk_size=100000):
    """Fetch data in parallel using threading."""
    table_name = f"{cache}_record_cache"

    # Determine ID range for chunking
    with recordcache._cursor(internal=False) as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = cur.fetchone()[0]

    if total_records == 0:
        print("No records found.")
        return []

    # Generate chunk ranges
    chunks = [(i, chunk_size) for i in range(0, total_records, chunk_size)]
    
    # Process chunks with threading
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_chunk, offset, limit, query_word, recordcache, table_name): (offset, limit)
            for offset, limit in chunks
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

