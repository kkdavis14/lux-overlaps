import os
import sys
from src.download import extract_luxy_entries
from src.visualize import create_tree, find_overlaps, write_tree, tree_to_string
from src.clean import check_parentheses, extract_parentheticals, remove_parentheticals, move_lastname, extract_name_parts, standardize_abbreviations, remove_dates
from luxy import PeopleGroups
import pandas as pd
from itertools import combinations
from tqdm import tqdm
import csv

uri_priority = [
        "https://linked-art.library.yale.edu/",
        "https://images.peabody.yale.edu/"
        "https://media.art.yale.edu/",
        "https://ycba-lux.s3.amazonaws.com/",
        "https://data.paul-mellon-center.ac.uk/",
        "http://id.loc.gov/",
        "http://vocab.getty.edu/",
        "http://www.wikidata.org/",
        "http://data.bnf.fr/",
        "https://d-nb.info/",
        "http://viaf.org",
        "https://orcid.org/",
        "https://ror.org/"
    ]

def get_priority_index(uri):
    """
    Returns the priority index of a URI based on the uri_priority list.
    Lower index = higher priority.
    
    Args:
        uri (str): The URI to check
        
    Returns:
        int: The priority index (position in uri_priority list), or len(uri_priority) if not found
    """
    for i, prefix in enumerate(uri_priority):
        if uri.startswith(prefix):
            return i
    return len(uri_priority)  # Return lowest priority if URI doesn't match any prefix

def extract_uri_from_line(line):
    """
    Extracts URI from a line containing 'equivalent:' text.
    
    Args:
        line (str): Line containing URI
        
    Returns:
        str: Extracted URI or None if not found
    """
    if 'equivalent:' in line:
        return line.split('equivalent:')[1].strip().strip('()')
    return None

def clean_group_name(name):
    """
    Removes URI from group name if present.
    
    Args:
        name (str): Group name potentially containing URI
        
    Returns:
        str: Cleaned group name
    """
    if '(equivalent:' in name:
        return name.split('(equivalent:')[0].strip()
    return name

def tree_to_csv(tree_str, output_file):
    """
    Converts tree structure to CSV mapping highest priority URIs to related URIs.
    
    Args:
        tree_str (str): String containing the tree structure
        output_file (str): Output CSV file path
    """
    current_group = None
    group_uris = {}
    
    # Parse the tree structure
    for line in tree_str.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        if line.startswith('──'):
            # New main group - clean the name first
            current_group = line.replace('──', '').strip()
            current_group = clean_group_name(current_group)
            group_uris[current_group] = []
            # If the group name had a URI, add it to the URIs list
            uri = extract_uri_from_line(line)
            if uri:
                group_uris[current_group].append(uri)
        elif line.startswith('└──'):
            # URI entry
            uri = extract_uri_from_line(line)
            if uri and current_group:
                group_uris[current_group].append(uri)
    
    # Write CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Primary URI', 'Related URI', 'Group'])
        
        for group, uris in group_uris.items():
            if not uris:
                continue
                
            # Sort URIs by priority
            sorted_uris = sorted(uris, key=get_priority_index)
            primary_uri = sorted_uris[0]
            
            # Write a row for each related URI
            for related_uri in sorted_uris[1:]:
                writer.writerow([primary_uri, related_uri, group])

def process_query(query, output='output.txt'):
    """
    Processes a query and creates tree and CSV output from the results.

    Args:
        query (str): The query to search for.
        output (str): The output file name.

    Returns:
        None
    """
    pg = PeopleGroups().filter(name=query, recordType="person").get()
    print(f"Examining the following data: {pg.view_url}")

    # Download entries from the given URL
    entries = extract_luxy_entries(pg)

    # Process entries
    entries = standardize_abbreviations(entries)
    entries = remove_dates(entries)
    entries = check_parentheses(entries)
    entries = extract_parentheticals(entries)
    entries = remove_parentheticals(entries)
    entries = move_lastname(entries)
    entries = extract_name_parts(entries)

    # Create tree and find overlaps
    tree = create_tree(entries)
    overlap_str = find_overlaps(tree)
    
    # Write overlap data
    overlap_output = f'{output.replace(".txt", "")}_overlap.txt'
    csv_output = f'{output.replace(".txt", "")}_mapping.csv'
    write_tree(overlap_str, overlap_output)
    tree_to_csv(overlap_str, csv_output)
    print(f"Simplified overlap structure saved to {overlap_output}")
    print(f"URI mappings saved to {csv_output}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python separate.py <query> [output]")
        sys.exit(1)

    query = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else 'output.txt'
    process_query(query, output)
