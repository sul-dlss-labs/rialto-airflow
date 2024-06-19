import os
import dimcli
import dotenv
import logging
import pandas as pd
import pickle
import time
import re
import requests

dotenv.load_dotenv()

dimcli.login(
    os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER"),
    os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS"),
    "https://app.dimensions.ai",
)

dsl = dimcli.Dsl(verbose=False)


def dimensions_dois_from_orcid(orcid):
    orcid = re.sub(r"^https://orcid.org/", "", orcid)
    q = """
        search publications where researchers.orcid_id = "{}"
        return publications [doi]
        limit 1000
        """.format(orcid)

    # The Dimensions API can flake out sometimes, so try to catch & retry.
    try_count = 0
    while try_count < 20:
        try_count += 1
        try:
            result = dsl.query(q)
            break
        except requests.exceptions.HTTPError as e:
            logging.error("Dimensions API call %s resulted in error: %s", try_count, e)
            time.sleep(try_count * 10)

    if len(result["publications"]) == 1000:
        logging.warning("Truncated results for ORCID %s", orcid)
    for pub in result["publications"]:
        if pub.get("doi"):
            yield "https://doi.org/" + pub["doi"]


def invert_dict(dict):
    # Inverting the dictionary so that DOI is the common key for all tasks.
    # This adds some complexity here but reduces complexity in downstream tasks.
    original_values = []
    for v in dict.values():
        original_values.extend(v)
    original_values = list(set(original_values))

    inverted_dict = {}
    for i in original_values:
        inverted_dict[i] = [k for k, v in dict.items() if i in v]

    return inverted_dict


def dimensions_doi_orcids_dict(org_data_file, pickle_file, limit=None):
    df = pd.read_csv(org_data_file)
    orcids = df[df["orcidid"].notna()]["orcidid"]
    orcid_dois = {}

    for orcid_url in orcids[:limit]:
        orcid = orcid_url.replace("https://orcid.org/", "")
        dois = list(dimensions_dois_from_orcid(orcid))
        orcid_dois.update({orcid: dois})

    with open(pickle_file, "wb") as handle:
        pickle.dump(invert_dict(orcid_dois), handle, protocol=pickle.HIGHEST_PROTOCOL)
