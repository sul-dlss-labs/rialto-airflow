import csv
import logging
import pickle
import time

import requests
from requests.exceptions import SSLError
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_random

from rialto_airflow.utils import invert_dict


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
    retry=retry_if_exception_type(SSLError),
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
