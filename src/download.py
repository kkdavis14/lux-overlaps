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

def process_records(query, recordcache, cache):
    """Process all records in a single query."""
    search_pattern = f"%{query}%"
    table_name = f"{cache}_record_cache"

    sql_query = f"""
        SELECT data FROM ils_record_cache LIMIT 5;    
    """


    results = []
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query, (search_pattern,))
        results = [row['name'] for row in cur.fetchall()]
    
    return results

def main():
    if len(sys.argv) < 4 or sys.argv[2] != "--cache":
        print("Usage: python script.py <query> --cache <cache_name>")
        sys.exit(1)

    # Extract query and cache
    query = sys.argv[1]
    cache = sys.argv[3]

    # Determine if cache is internal or external
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    print(f"Processing records in cache '{cache}' for query '{query}'...")
    results = process_records(query, recordcache, cache)

    print("Processing completed. Results:")
    for result in tqdm(results, desc="Results"):
        print(result)

if __name__ == "__main__":
    main()
