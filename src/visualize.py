from anytree import Node
import itertools
from anytree.render import RenderTree

def create_tree(entries, consider_dates=True):
    """
    Creates a tree structure from a list of entries.

    Args:
        entries (list): A list of dictionaries containing the extracted data.
        consider_dates (bool): Whether to consider dates in the tree structure.

    Returns:
        Node: The root node of the tree.
    """
    entries = [entry for entry in entries if entry['type'] == 'person' and entry["manual_review"] == False]
    root = Node("Names")
    
    # Group entries by last name
    last_name_groups = itertools.groupby(sorted(entries, key=lambda x: x['last_name'] or ''), key=lambda x: x['last_name'] or '')
    
    for last_name, group in last_name_groups:
        last_name_node = Node(last_name, parent=root)
        
        # Convert group iterator to list for multiple uses
        group_list = list(group)
        
        # Group by expanded name if it exists, otherwise by other names
        name_groups = itertools.groupby(
            sorted(group_list, key=lambda x: (x['last_name'] or '', x['first_name'] or '', x['middle_name'] or '', x.get('parentheticals', [''])[0] if x.get('parentheticals') else '')),
            key=lambda x: (x['last_name'] or '', x['first_name'] or '', x['middle_name'] or '', x.get('parentheticals', [''])[0] if x.get('parentheticals') else '')
        )
        
        for name, subgroup in name_groups:
            if name:  # Only create a node if there's a name to use
                if name[3]: 
                    name_node = Node(' '.join(f"{name[1]} {name[2]} {name[0]} ({name[3]})".split()), parent=last_name_node)
                else:
                    name_node = Node(' '.join(f"{name[1]} {name[2]} {name[0]}".split()), parent=last_name_node)
                
                # Convert subgroup iterator to list for multiple uses
                subgroup_list = list(subgroup)

                # for entry in subgroup_list:
                #     if entry.get('equivalent'):
                #         Node(f"Equivalent URI: {entry['equivalent']}", parent=name_node)

                
                if consider_dates:
                    # Further group by dates
                    date_groups = itertools.groupby(
                        sorted(subgroup_list, key=lambda x: x.get('dates', '') or ''),
                        key=lambda x: x.get('dates', '') or ''
                    )
                    
                    for date, date_subgroup in date_groups:
                        if date:  # Only create a node if there's a date to use
                            Node(f"{date}", parent=name_node)
                        else:
                            # If no date, add entries directly under the name node
                            for entry in date_subgroup:
                                Node(entry['name'], parent=name_node)
                else:
                    # If not considering dates, add all entries under the name node
                    for entry in subgroup_list:
                        Node(entry['name'], parent=name_node)
            else:
                # If no expanded or other names, add entries directly under the last name node
                for entry in subgroup:
                    Node(entry['name'], parent=last_name_node)
    
    return root

def write_tree(tree, output):
    """
    Writes the tree structure to a file.
    """
    with open(output, 'w') as f:
        f.write(tree)
        
def tree_to_string(tree):
    """
    Converts a tree to a string.
    """
    output_branches = []
    # Print the tree
    for pre, _, node in RenderTree(tree):
        output_branches.append(f"{pre}{node.name}")

    output_str = "\n".join(output_branches)
    return output_str

def find_overlaps(tree):
    """
    Finds overlaps in the tree structure.

    Args:
        tree (Node): The root node of the tree.

    Returns:
        str: A string representation of the overlaps.
    """
    overlap_branches = []

    # Iterate through the tree to find instances where the final child has 2 or more nodes
    for pre, _, node in RenderTree(tree):
        if node.is_leaf and node.parent and len(node.parent.children) > 1:
            if node == node.parent.children[-1]:  # Check if it's the last child
                parent = node.parent
                overlap_branches.append(f"── {parent.name}")
                for child in parent.children:
                    overlap_branches.append(f"   └── {child.name}")

    overlap_str = "\n".join(overlap_branches)

    return overlap_str