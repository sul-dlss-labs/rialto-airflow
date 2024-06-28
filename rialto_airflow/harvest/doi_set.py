import csv
import logging
import pickle


import polars as pl


def create_doi_set(dimensions: str, openalex: str, sul_pub_jsonl: str) -> list:
    """Get DOIs from each source and dedupe."""
    dimensions_dois = dois_from_pickle(dimensions)
    openalex_dois = dois_from_pickle(openalex)
    sul_pub_dois = get_sul_pub_dois(sul_pub_jsonl)
    unique_dois = list(set(dimensions_dois + openalex_dois + sul_pub_dois))
    logging.info(f"found {len(unique_dois)}")

    return unique_dois


def dois_from_pickle(pickle_file: str) -> list:
    """Load a pickled dictionary of DOIs and ORCIDs from file."""
    with open(pickle_file, "rb") as handle:
        data = pickle.load(handle)

    dois = list(data.keys())
    return dois


def get_sul_pub_dois(sul_pub_jsonl: str) -> list:
    """Extract DOIs from sul_pub CSV and remove empty values."""
    df = pl.read_ndjson(sul_pub_jsonl)
    return df["doi"].to_list()
