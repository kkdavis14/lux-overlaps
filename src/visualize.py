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
        # For last name node, don't show equivalent
        last_name_node = Node(last_name, parent=root, display_name=last_name)
        
        group_list = list(group)
        
        name_groups = itertools.groupby(
            sorted(group_list, key=lambda x: (x['last_name'] or '', x['first_name'] or '', x['middle_name'] or '', x.get('parentheticals', [''])[0] if x.get('parentheticals') else '')),
            key=lambda x: (x['last_name'] or '', x['first_name'] or '', x['middle_name'] or '', x.get('parentheticals', [''])[0] if x.get('parentheticals') else '')
        )
        
        for name, subgroup in name_groups:
            subgroup_list = list(subgroup)
            if name:
                if name[3]:
                    base_name = ' '.join(f"{name[1]} {name[2]} {name[0]} ({name[3]})".split())
                else:
                    base_name = ' '.join(f"{name[1]} {name[2]} {name[0]}".split())
                
                # Only add equivalent if it exists for this exact name
                equivalent = next((entry['equivalent'] for entry in subgroup_list 
                                if entry.get('equivalent') and entry['name'] == base_name), None)
                display_name = f"{base_name} (equivalent: {equivalent})" if equivalent else base_name
                name_node = Node(base_name, parent=last_name_node, display_name=display_name)
                
                # Add individual name variations, only showing equivalent if it exists
                for entry in subgroup_list:
                    entry_display = f"{entry['name']} (equivalent: {entry['equivalent']})" if entry.get('equivalent') else entry['name']
                    Node(entry['name'], parent=name_node, display_name=entry_display)

    return root

def write_tree(tree, output):
    """
    Writes the tree structure to a file.
    """
    with open(output, 'w') as f:
        f.write(tree)
        
def tree_to_string(tree):
    """
    Converts a tree to a string using display names for visualization.
    """
    output_branches = []
    for pre, _, node in RenderTree(tree):
        # Use display_name if it exists, otherwise use name
        display_text = getattr(node, 'display_name', node.name)
        output_branches.append(f"{pre}{display_text}")

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
                # Use display_name for parent
                parent_display = getattr(parent, 'display_name', parent.name)
                overlap_branches.append(f"── {parent_display}")
                for child in parent.children:
                    # Use display_name for children
                    child_display = getattr(child, 'display_name', child.name)
                    overlap_branches.append(f"   └── {child_display}")

    overlap_str = "\n".join(overlap_branches)

    return overlap_str