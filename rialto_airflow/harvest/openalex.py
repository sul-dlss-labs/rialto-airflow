import csv
import logging
import pickle
import time

from airflow.models import Variable
from pyalex import config, Works
import requests
from ssl import SSLEOFError
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_random
from more_itertools import batched

from rialto_airflow.utils import invert_dict

config.email = Variable.get("openalex_email")
config.max_retries = 0
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


@retry(
    wait=wait_random(1, 5),
    stop=stop_after_delay(60),
    retry=retry_if_exception_type(SSLEOFError),
)
def dois_from_orcid(orcid: str):
    """
    Pass in the ORCID ID and get back an iterator of DOIs for publications authored by that person.
    """

    # TODO: get a key so we don't have to sleep!
    time.sleep(1)

    logging.info(f"looking up dois for orcid {orcid}")

    orcid = f"https://orcid.org/{orcid}"
    author_resp = requests.get(
        f"https://api.openalex.org/authors/{orcid}", allow_redirects=True
    )
    if author_resp.status_code == 200:
        author_id = author_resp.json()["id"].replace("https://openalex.org/", "")
        for pub in works_from_author_id(author_id):
            # not all publications have DOIs
            doi = pub.get("doi")
            if doi:
                yield doi


def works_from_author_id(author_id, limit=None):
    """
    Pass in the OpenAlex Author ID and get back an iterator of works.
    """
    url = "https://api.openalex.org/works"
    params = {"filter": f"author.id:{author_id}", "per_page": 200}

    count = 0
    page = 0
    has_more = True
    while has_more:
        page += 1
        params["page"] = page

        logging.info(f"fetching works for {author_id} page={page}")
        resp = requests.get(url, params)

        if resp.status_code == 200:
            # TODO: get a key so we don't have to sleep!
            time.sleep(1)
            results = resp.json().get("results")
            if len(results) == 0:
                has_more = False
            else:
                for result in results:
                    count += 1
                    if limit is not None and count > limit:
                        has_more = False
                    else:
                        yield result
        else:
            logging.error(
                f"encountered HTTP {resp.status_code} response from {url} {params}: {resp.text}"
            )
            has_more = False


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
        doi_list = "|".join([doi for doi in doi_batch])

        result = Works().filter(doi=doi_list).get()
        time.sleep(1)
        for pub in result:
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
