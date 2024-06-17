import csv
import logging

import requests


sul_pub_fields = [
    "authorship",
    "title",
    "abstract",
    "author",
    "year",
    "type",
    "mesh_headings",
    "publisher",
    "journal",
    "provenance",
    "doi",
    "issn",
    "sulpubid",
    "sw_id",
    "pmid",
    "identifier",
    "last_updated",
    "pages",
    "date",
    "country",
    "booktitle",
    "edition",
    "series",
    "chapter",
    "editor",
]


def sul_pub_csv(csv_file, host, key, since=None, limit=None):
    with open(csv_file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sul_pub_fields)
        writer.writeheader()
        for row in harvest(host, key, since, limit):
            writer.writerow(row)


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

            yield {key: record[key] for key in record if key in sul_pub_fields}
