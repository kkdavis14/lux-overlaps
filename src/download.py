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


def create_materialized_view(recordcache, cache):
    """Create or replace a materialized view."""
    table_name = f"{cache}_record_cache"
    sql_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS person_records AS
        SELECT 
            jsonb_array_elements(data->'identified_by') AS identified_by,
            jsonb_array_elements(data->'identified_by'->'classified_as') AS classified_as
        FROM {table_name}
        WHERE data->>'type' = 'Person'
        AND jsonb_array_elements(data->'identified_by'->'classified_as')->>'id' = 'http://vocab.getty.edu/aat/300404670';

    """
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query)
            print("Materialized view created successfully.")
    except Exception as e:
        print(f"Error creating materialized view: {e}")


def refresh_materialized_view(recordcache):
    """Refresh the materialized view."""
    sql_query = "REFRESH MATERIALIZED VIEW person_records;"
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query)
            print("Materialized view refreshed successfully.")
    except Exception as e:
        print(f"Error refreshing materialized view: {e}")

def fetch_data(query_word, recordcache):
    """Fetch all records sequentially."""
    search_pattern = f"%{query_word}%"
    sql_query = """
        SELECT 
            identified_by->>'content' AS name
        FROM person_records
        WHERE identified_by->>'content' ILIKE %s;
    """
    results = []
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query, (search_pattern,))
            results = [row['name'] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error fetching data: {e}")
    
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

    # Create or refresh materialized view
    print("Setting up materialized view...")
    create_materialized_view(recordcache, cache)
    refresh_materialized_view(recordcache)

    print(f"Fetching records for '{query_word}' in materialized view...")
    results = fetch_data(query_word, recordcache)

    print("Query completed. Results:")
    for result in tqdm(results, desc="Results"):
        print(result)

if __name__ == "__main__":
    main()

