from tqdm import tqdm
import time
import psycopg2
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

db_config = {
    "host": cfgs.caches["host"],
    "port": cfgs.caches["port"],
    "user": cfgs.caches["user"],
    "password": cfgs.caches["password"],
    "dbname": cfgs.caches["dbname"],
}

def materialized_view_exists(view_name):
    """Check if the materialized view exists in the public schema."""
    sql_query = f"""
        SELECT matviewname 
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = '{view_name}';
    """
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                result = cur.fetchone()
                return result is not None  # Returns True if the view exists, False otherwise
    except Exception as e:
        print(f"Error checking materialized view existence: {e}")
        return False

def create_combined_materialized_view(caches, refresh=False):
    """Create or refresh a consolidated materialized view of all People across caches."""
    if materialized_view_exists("person_records_all"):
        if refresh:
            print("Refresh flag found. Recreating the combined materialized view...")
        else:
            print("Combined materialized view already exists. Skipping creation.")
            return

    # Generate UNION ALL query for multiple caches with filtering inside
    union_queries = []
    for cache in caches:
        table_name = f"{cache}_record_cache"
        union_queries.append(f"""
            SELECT 
                identified->>'content' AS name,
                '{table_name}' AS source_cache
            FROM {table_name},
                 jsonb_array_elements(data->'identified_by') AS identified
            WHERE data->>'type' = 'Person'
              AND EXISTS (
                SELECT 1 
                FROM jsonb_array_elements(identified->'classified_as') AS classified
                WHERE classified->>'id' = 'http://vocab.getty.edu/aat/300404670'
            );
        """)

    combined_query = " UNION ALL ".join(union_queries)

    sql_query = f"""
        DROP MATERIALIZED VIEW IF EXISTS public.person_records_all;
        CREATE MATERIALIZED VIEW public.person_records_all AS
        {combined_query};
    """

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                print("Executing creation of combined materialized view 'person_records_all'...")
                cur.execute(sql_query)
                conn.commit()  # Ensure persistence
                print("Combined materialized view 'person_records_all' created successfully.")
    except Exception as e:
        print(f"Error creating combined materialized view: {e}")


def fetch_combined_data(query_word):
    """Fetch all records sequentially from the combined materialized view."""
    search_pattern = f"%{query_word}%"
    sql_query = """
        SELECT 
            name
        FROM person_records_all
        WHERE name ILIKE %s;
    """
    results = []

    try:
        # Use the global db_config variable
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                print(f"Executing fetch query from 'person_records_all' for '{query_word}'...")
                start_time = time.time()
                
                # Execute the query with the search pattern
                cur.execute(sql_query, (search_pattern,))
                results = [row[0] for row in cur.fetchall()]
                
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

    # Define the list of all caches to include in the combined view
    all_caches = ["ils", "ycba", "yuag", "ypm", "pmc", "ipch"]  

    # Create the combined materialized view
    print("Checking on combined materialized view...")
    create_combined_materialized_view(all_caches, refresh=refresh_flag)

    print(f"Fetching records for '{query_word}' in combined materialized view...")
    results = fetch_combined_data(query_word,)

    print("Query completed. Results:")
    for name in tqdm(results, desc="Results"):
        print(f"{name}")

if __name__ == "__main__":
    main()
