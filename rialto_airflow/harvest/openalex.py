import csv
import logging
import os
import pickle
import time
from urllib.parse import quote

from more_itertools import batched
from pyalex import Authors, Works, config

from rialto_airflow.utils import invert_dict

config.email = os.environ.get("AIRFLOW_VAR_OPENALEX_EMAIL")
config.max_retries = 5
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


def doi_orcids_pickle(authors_csv, pickle_file, limit=None):
    """
    Pass in the Authors CSV and generate a DOI -> ORCID mapping as a pickle file.
    """
    with open(authors_csv, "r") as csv_input:
        orcid_dois = {}
        count = 0
        for row in csv.DictReader(csv_input):
            orcid = row["orcidid"].replace("https://orcid.org/", "")
            if orcid:
                count += 1
                orcid_dois[orcid] = list(dois_from_orcid(orcid))
                if limit is not None and count > limit:
                    break

    with open(pickle_file, "wb") as handle:
        pickle.dump(invert_dict(orcid_dois), handle, protocol=pickle.HIGHEST_PROTOCOL)


def dois_from_orcid(orcid: str, limit=None):
    """
    Pass in the ORCID ID and get back an iterator of DOIs for publications authored by that person.
    """

    # TODO: I think we can maybe have this function take a list of orcids and
    # batch process them since we can filter by multiple orcids in one request?

    # TODO: get a key so we don't have to sleep!
    time.sleep(1)

    logging.info(f"looking up dois for orcid {orcid}")

    # get the first (and hopefully only) openalex id for the orcid
    authors = Authors().filter(orcid=orcid).get()
    if len(authors) == 0:
        return []
    elif len(authors) > 1:
        logging.warn(f"found more than one openalex author id for {orcid}")
    author_id = authors[0]["id"]

    # get all the works for the openalex author id
    work_count = 0
    for page in Works().filter(author={"id": author_id}).paginate(per_page=200):
        for pub in page:
            if pub.get("doi"):
                work_count += 1
                if limit is not None and work_count > limit:
                    return
                yield pub.get("doi").replace("https://doi.org/", "")


def publications_csv(dois: list, csv_file: str) -> None:
    """
    Get publication records for a list of DOIs and create a CSV file.
    """
    with open(csv_file, "w") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        for pub in publications_from_dois(dois):
            writer.writerow(pub)


def publications_from_dois(dois: list, batch_size=75):
    """
    Look up works by DOI in batches that fit within OpenAlex request size limits
    """
    for doi_batch in batched(dois, batch_size):
        # TODO: do we need this to stay within 100,000 requests / day API quota?
        time.sleep(1)

        doi_list = quote("|".join([doi for doi in doi_batch]))
        for page in Works().filter(doi=doi_list).paginate(per_page=200):
            for pub in page:
                yield normalize_publication(pub)


def normalize_publication(pub) -> dict:
    """
    Ensure missing keys get empty values written to CSV.
    """
    for field in FIELDS:
        if field not in pub:
            pub[field] = None

    return pub


FIELDS = [
    "abstract_inverted_index",
    "authorships",
    "apc_list",
    "apc_paid",
    "best_oa_location",
    "biblio",
    "cited_by_api_url",
    "cited_by_count",
    "cited_by_percentile_year",
    "concepts",
    "corresponding_author_ids",
    "corresponding_institution_ids",
    "countries_distinct_count",
    "counts_by_year",
    "created_date",
    "datasets",
    "display_name",
    "doi",
    "fulltext_origin",
    "fwci",
    "grants",
    "has_fulltext",
    "id",
    "ids",
    "indexed_in",
    "institutions_distinct_count",
    "is_authors_truncated",
    "is_paratext",
    "is_retracted",
    "keywords",
    "language",
    "license",
    "locations",
    "locations_count",
    "mesh",
    "ngrams_url",
    "open_access",
    "primary_location",
    "primary_topic",
    "publication_date",
    "publication_year",
    "referenced_works",
    "referenced_works_count",
    "related_works",
    "sustainable_development_goals",
    "topics",
    "title",
    "type",
    "type_crossref",
    "updated_date",
    "versions",
]
