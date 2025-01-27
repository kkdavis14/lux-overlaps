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


def materialized_view_exists(recordcache):
    """Check if the materialized view exists in the public schema."""
    sql_query = """
        SELECT matviewname 
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'person_records';
    """
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query)
            views = cur.fetchall()
            
            # Print available views for debugging
            print("Available materialized views in public schema:", [row['matviewname'] for row in views])
            
            return len(views) > 0
    except Exception as e:
        print(f"Error checking materialized view existence: {e}")
        return False


def create_materialized_view(recordcache, cache):
    """Create a materialized view if it doesn't already exist."""
    if materialized_view_exists(recordcache):
        print("Materialized view already exists. Skipping creation.")
        return

    table_name = f"{cache}_record_cache"
    sql_query = f"""
        CREATE MATERIALIZED VIEW public.person_records AS
        SELECT 
            identified_by
        FROM (
            SELECT jsonb_array_elements(data->'identified_by') AS identified_by
            FROM {table_name}
            WHERE data->>'type' = 'Person'
        ) subquery
        WHERE jsonb_path_exists(identified_by, '$.classified_as[*] ? (@.id == "http://vocab.getty.edu/aat/300404670")');
    """
    try:
        with recordcache._cursor(internal=False) as cur:
            print("Executing materialized view creation query...")
            cur.execute(sql_query)
            recordcache._connection.commit()  # Ensure persistence
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
        print("Usage: python script.py <query_word> --cache <cache_name> [--refresh]")
        sys.exit(1)

    # Extract query word and cache
    query_word = sys.argv[1]
    cache = sys.argv[3]
    refresh_flag = "--refresh" in sys.argv

    # Determine if cache is internal or external
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    # Create materialized view if it doesn't exist
    print("Setting up materialized view...")
    create_materialized_view(recordcache, cache)

    if refresh_flag:
        refresh_materialized_view(recordcache)

    print(f"Fetching records for '{query_word}' in materialized view...")
    results = fetch_data(query_word, recordcache)

    print("Query completed. Results:")
    for result in tqdm(results, desc="Results"):
        print(result)

if __name__ == "__main__":
    main()

