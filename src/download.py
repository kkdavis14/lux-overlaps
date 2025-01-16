import sys
import os
import json
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

def fetch_data(query_word, recordcache, cache):
    """
    Query SQL to find records with type "Person" and classified_as AAT 300404670,
    matching the query word in identified_by->content.
    """
    search_pattern = f"%{query_word}%"  # Prepare search pattern for ILIKE
    table_name = f"{cache}_record_cache"

    sql_query = f"""
        SELECT 
            identified_by::jsonb->>'content' AS name
        FROM {table_name},
             LATERAL jsonb_array_elements(data::jsonb->'identified_by') AS identified_by,
             LATERAL jsonb_array_elements(identified_by::jsonb->'classified_as') AS classified_as
        WHERE data::jsonb->>'type' = 'Person'
          AND classified_as::jsonb->>'id' = 'http://vocab.getty.edu/aat/300404670'
          AND identified_by::jsonb->>'content' ILIKE %s;
    """

    results = []
    with recordcache._cursor(internal=False) as cur:
        cur.execute(sql_query, (search_pattern,))
        results = [row['name'] for row in cur.fetchall()]
    
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
