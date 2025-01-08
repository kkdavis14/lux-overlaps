# Yale Lux Search Overlap Identifier

This project is designed to take in a Yale Lux Search URL and identify potentially overlapping records. The main script, `separate.py`, downloads entries from the query, processes them to clean and standardize the data, and then creates a hierarchical tree structure to visualize the relationships between the entries.

## Features

- Downloads entries from a given Yale Lux Search query using [LuxY](https://github.com/project-lux/luxy).
- Cleans and standardizes the data, including handling parentheticals, abbreviations, and name parts.
- Creates a hierarchical tree structure to visualize the relationships between the entries.
- Outputs the tree structure to a specified file.

## Requirements

- Python 3.6+
- tqdm
- anytree
- nameparser
- luxy
## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/wjbmattingly/yale-lux-overlap
    cd yale-lux-overlap
    ```

2. Install the required Python packages:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

To use the script, run the following command:

```sh
python separate.py <url> [output]
```

- `<url>`: The Yale Lux Search URL to process.
- `[output]`: (Optional) The output file to save the tree structure. Defaults to `output.txt`.

### Example

```sh
python separate.py "tolkien" output.txt
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
