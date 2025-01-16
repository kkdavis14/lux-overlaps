from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import RealDictCursor
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

def fetch_id_range(recordcache, cache):
    """Fetch the ID range from the database."""
    table_name = f"{cache}_record_cache"
    qry = f"SELECT MIN(id), MAX(id) FROM {table_name}"
    with recordcache._cursor(internal=False) as cur:
        cur.execute(qry)
        return cur.fetchone()


def process_chunk(chunk_range, query, recordcache):
    """Process a chunk of records."""
    start_id, end_id = chunk_range
    results = []
    search_pattern = f"%{query}%"
    table_name = f"{cache}_record_cache"
    
    sql_query = """
        SELECT 
            jsonb_array_elements(data->'identified_by')->>'content' AS name
        FROM {table_name},
             jsonb_array_elements(data->'identified_by') AS identifier
        WHERE jsonb_array_elements(identifier->'classified_as')->>'id' = 'http://vocab.getty.edu/aat/300404670'
          AND data->>'type' = 'Person'
          AND id BETWEEN %s AND %s
          AND jsonb_array_elements(data->'identified_by')->>'content' ILIKE %s;

    """
    
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query, (start_id, end_id, search_pattern))
        results.extend(row['name'] for row in cur.fetchall())
    
    return results


def parallel_processing(query, cache, cfgs, chunk_size=100000):
    """Parallel processing with PostgreSQL partitioning."""
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']
    
    # Get ID range for partitioning
    min_id, max_id = fetch_id_range(recordcache, cache)
    if min_id is None or max_id is None:
        print("No records found.")
        return []

    # Create chunks
    chunks = [(i, min(i + chunk_size - 1, max_id)) for i in range(min_id, max_id + 1, chunk_size)]

    # Use ProcessPoolExecutor for parallel processing
    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_chunk, chunk, query, recordcache, cache): chunk for chunk in chunks}

        with tqdm(total=len(futures), desc="Processing Chunks") as pbar:
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    print(f"Error processing chunk {futures[future]}: {e}")
                pbar.update(1)
    
    return results


def main():
    if len(sys.argv) < 4 or sys.argv[2] != "--cache":
        print("Usage: python script.py <query> --cache <cache_name>")
        sys.exit(1)

    # Extract query and cache
    query = sys.argv[1]
    cache = sys.argv[3]

    results = parallel_processing(query, cache, cfgs)
    print("Processing completed. Results:")
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
