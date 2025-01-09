import os
import sys
from src.download import extract_luxy_entries
from src.visualize import create_tree, find_overlaps, write_tree, branch_to_string
from src.clean import check_parentheses, extract_parentheticals, remove_parentheticals, move_lastname, extract_name_parts, standardize_abbreviations, remove_dates
from luxy import PeopleGroups

def process_query(query, output='output.txt'):
    """
    Processes a query and creates a tree structure from the results.

    Args:
        query (str): The query to search for.
        output (str): The output file name.

    Returns:
        None
    """
    pg = PeopleGroups().filter(name=query, recordType="person").get()

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

    # Create tree
    tree = create_tree(entries)

    # Check if output file exists       
    if os.path.exists(output):
        print(f"Warning: Overwriting existing file {output}")

    output_str = branch_to_string(tree)
    write_tree(output_str, output)
    print(f"Tree saved to {output}")

    overlap_output = f'{output.replace(".txt", "")}_overlap.txt'
    overlap_str = find_overlaps(tree)
    write_tree(overlap_str, overlap_output)
    print(f"Simplified overlap structure saved to {overlap_output}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python separate.py <query> [output]")
        sys.exit(1)

    query = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else 'output.txt'
    process_query(query, output)
