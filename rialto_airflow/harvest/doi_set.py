import csv
import pickle


def create_doi_set(dimensions: str, openalex: str, sul_pub_csv: str) -> list:
    """Get DOIs from each source and dedupe."""
    dimensions_dois = dois_from_pickle(dimensions)
    openalex_dois = dois_from_pickle(openalex)
    sul_pub_dois = get_sul_pub_dois(sul_pub_csv)
    unique_dois = list(set(dimensions_dois + openalex_dois + sul_pub_dois))

    return unique_dois


def dois_from_pickle(pickle_file: str) -> list:
    """Load a pickled dictionary of DOIs and ORCIDs from file."""
    with open(pickle_file, "rb") as handle:
        data = pickle.load(handle)

    dois = list(data.keys())
    return dois


def get_sul_pub_dois(sul_pub_csv: str) -> list:
    """Extract DOIs from sul_pub CSV and remove empty values."""
    with open(sul_pub_csv, "r") as file:
        reader = csv.DictReader(file)
        doi_column = [row["doi"] for row in reader if row["doi"]]

    return doi_column
