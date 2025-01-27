from tqdm import tqdm
import time
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

def materialized_view_exists(recordcache, view_name):
    """Check if the materialized view exists in the public schema."""
    sql_query = f"""
        SELECT matviewname 
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = '{view_name}';
    """
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query)
            views = cur.fetchall()
            return len(views) > 0
    except Exception as e:
        print(f"Error checking materialized view existence: {e}")
        return False

def create_combined_materialized_view(recordcache, caches):
    """Create a consolidated materialized view of all People across caches."""
    if materialized_view_exists(recordcache, "person_records_all"):
        print("Combined materialized view already exists. Skipping creation.")
        return

    # Generate UNION ALL query for multiple caches with WHERE condition inside
    union_queries = []
    for cache in caches:
        table_name = f"{cache}_record_cache"
        union_queries.append(f"""
            SELECT 
                jsonb_array_elements(data->'identified_by') AS identified_by,
                '{cache}' AS source_cache
            FROM {table_name}
            WHERE data->>'type' = 'Person'
            AND jsonb_path_exists(data->'identified_by', '$.classified_as[*] ? (@.id == "http://vocab.getty.edu/aat/300404670")')
        """)

    combined_query = " UNION ALL ".join(union_queries)

    sql_query = f"""
        CREATE MATERIALIZED VIEW public.person_records_all AS
        {combined_query};
    """

    try:
        with recordcache._cursor(internal=False) as cur:
            print("Executing creation of combined materialized view 'person_records_all'...")
            cur.execute(sql_query)
            recordcache._connection.commit()  # Ensure persistence
            print("Combined materialized view 'person_records_all' created successfully.")
    except Exception as e:
        print(f"Error creating combined materialized view: {e}")


def refresh_materialized_view(recordcache):
    """Refresh the combined materialized view."""
    sql_query = "REFRESH MATERIALIZED VIEW person_records_all;"
    try:
        with recordcache._cursor(internal=False) as cur:
            cur.execute(sql_query)
            print("Combined materialized view refreshed successfully.")
    except Exception as e:
        print(f"Error refreshing combined materialized view: {e}")

def fetch_combined_data(query_word, recordcache):
    """Fetch all records sequentially from the combined materialized view."""
    search_pattern = f"%{query_word}%"
    sql_query = """
        SELECT 
            identified_by->>'content' AS name,
            source_cache
        FROM person_records_all
        WHERE identified_by->>'content' ILIKE %s;
    """
    results = []
    try:
        with recordcache._cursor(internal=False) as cur:
            print(f"Executing fetch query from 'person_records_all' for '{query_word}'...")
            start_time = time.time()
            
            cur.execute(sql_query, (search_pattern,))
            results = [(row['name'], row['source_cache']) for row in cur.fetchall()]
            
            end_time = time.time()
            print(f"Query retrieved {len(results)} results in {end_time - start_time:.2f} seconds.")
    except Exception as e:
        print(f"Error fetching data from 'person_records_all': {e}")
    
    return results

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <query_word> [--refresh]")
        sys.exit(1)

    # Extract query word and check if refresh is requested
    query_word = sys.argv[1]
    refresh_flag = "--refresh" in sys.argv

    # Use a default record cache connection
    recordcache = cfgs.internal["ils"]["recordcache"]

    # Define the list of all caches to include in the combined view
    all_caches = ["ils", "ycba", "yuag", "ypm", "pmc", "ipch"]  

    # Create the combined materialized view
    print("Setting up combined materialized view...")
    create_combined_materialized_view(recordcache, all_caches)

    if refresh_flag:
        refresh_materialized_view(recordcache)

    print(f"Fetching records for '{query_word}' in combined materialized view...")
    results = fetch_combined_data(query_word, recordcache)

    print("Query completed. Results:")
    for result, source in tqdm(results, desc="Results"):
        print(f"{result} (from {source})")

if __name__ == "__main__":
    main()
