import sys
import os
from tqdm import tqdm

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

def fetch_data(recordcache):
    """Execute a simple SQL query and return the results."""
    sql_query = """
        SELECT data FROM ils_record_cache LIMIT 5;
    """

    results = []
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query)
        results = [row['data'] for row in cur.fetchall()]
    
    return results

def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--cache":
        print("Usage: python script.py --cache <cache_name>")
        sys.exit(1)

    # Extract cache
    cache = sys.argv[2]

    # Determine if cache is internal or external
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    print(f"Fetching data from cache '{cache}'...")
    results = fetch_data(recordcache)

    print("Query completed. Results:")
    for result in tqdm(results, desc="Results"):
        print(result)

if __name__ == "__main__":
    main()
