import os
import dimcli
import dotenv
import logging
import pandas as pd
import pickle
import time
import re
import requests

from rialto_airflow.utils import invert_dict

dotenv.load_dotenv()

dimcli.login(
    os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER"),
    os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS"),
    "https://app.dimensions.ai",
)

dsl = dimcli.Dsl(verbose=False)


def dois_from_orcid(orcid):
    logging.info(f"looking up dois for orcid {orcid}")
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
        doi = pub.get("doi")
        if doi:
            yield pub["doi"]


def doi_orcids_pickle(org_data_file, pickle_file, limit=None):
    df = pd.read_csv(org_data_file)
    orcids = df[df["orcidid"].notna()]["orcidid"]
    orcid_dois = {}

    for orcid_url in orcids[:limit]:
        orcid = orcid_url.replace("https://orcid.org/", "")
        dois = list(dois_from_orcid(orcid))
        orcid_dois.update({orcid: dois})

    with open(pickle_file, "wb") as handle:
        pickle.dump(invert_dict(orcid_dois), handle, protocol=pickle.HIGHEST_PROTOCOL)
