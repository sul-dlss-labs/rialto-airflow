import csv
import json
import logging

import requests


def publications_jsonl(jsonl_file, host, key, since=None, limit=None):
    with open(jsonl_file, "w") as output:
        for record in harvest(host, key, since, limit):
            json.dump(record, output, ensure_ascii=False)
            output.write("\n")


def harvest(host, key, since, limit):
    url = f"https://{host}/publications.json"

    http_headers = {"CAPKEY": key}

    params = {"per": 1000}
    if since:
        params["changedSince"] = since.strftime("%Y-%m-%d")

    page = 0
    record_count = 0
    more = True

    while more:
        page += 1
        params["page"] = page

        logging.info(f"fetching sul_pub results {url} {params}")
        resp = requests.get(url, params=params, headers=http_headers)
        resp.raise_for_status()

        records = resp.json()["records"]
        if len(records) == 0:
            more = False

        for record in records:
            record_count += 1
            if limit is not None and record_count > limit:
                logging.info(f"stopping with limit={limit}")
                more = False
                break

            record["doi"] = extract_doi(record)

            yield record


def extract_doi(record):
    for id in record.get("identifier"):
        if id.get("type") == "doi" and "id" in id:
            return id["id"].replace("https://doi.org/", "")
    return None
