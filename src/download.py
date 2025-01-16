from tdqm import tqdm
from multiprocessing import Pool, cpu_count
import sys
import os

current_file = os.path.abspath(__file__) 
lux_overlaps_root = os.path.dirname(os.path.dirname(current_file))
parent_dir = os.path.dirname(lux_overlaps_root)
sys.path.insert(0, parent_dir)

from dotenv import load_dotenv
from pipeline.config import Config


load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.instantiate_all()

def process_record(record):
    """Process a single record."""
    data = record['data']
    # Extract the primary name
    for identifier in data.get("identified_by", []):
        for cxn in identifier.get("classified_as", []):
            if cxn.get("id") == "http://vocab.getty.edu/aat/300404670":
                return identifier.get("content", "")
    return None

def parallel_processing(query, cache, cfgs):
    """
    Process records in parallel using multiprocessing.
    """
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    # Retrieve all "Person" records
    records = list(recordcache.iter_records_type("Person"))

    # Determine the number of CPU cores to use
    num_workers = cpu_count()

    # Use multiprocessing with a progress bar
    with Pool(num_workers) as pool:
        # Wrap pool.imap with tqdm for progress tracking
        processed_results = list(tqdm(pool.imap(process_record, records), total=len(records), desc="Processing Records"))

    filtered_results = [name for name in processed_results if name and query.lower() in name.lower()]

    return filtered_results

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
