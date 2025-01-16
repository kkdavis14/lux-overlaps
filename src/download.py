from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import sys
import os
from more_itertools import chunked

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

def process_chunk(chunk, query):
    """Process a chunk of records and filter by query."""
    results = []
    for record in chunk:
        result = process_record(record)
        if result and query.lower() in result.lower():
            results.append(result)
    return results

def parallel_processing(query, cache, cfgs, chunk_size=10000):
    """
    Process records in parallel using multiprocessing.
    """
    internal = cache in cfgs.internal
    recordcache = (cfgs.internal if internal else cfgs.external)[cache]['recordcache']

    # Retrieve all "Person" records
    record_generator = recordcache.iter_records_type("Person")
    chunks = chunked(record_generator, chunk_size)

    # Determine the number of CPU cores to use
    num_workers = cpu_count()

    all_results = []
    with Pool(num_workers) as pool:
        for chunk_results in tqdm(pool.imap(lambda chunk: process_chunk(chunk, query), chunks),
                                  desc="Processing Chunks"):
            all_results.extend(chunk_results)

    return all_results

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
