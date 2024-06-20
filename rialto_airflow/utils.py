import csv
import datetime
from pathlib import Path


def create_snapshot_dir(data_dir):
    snapshots_dir = Path(data_dir) / "snapshots"

    if not snapshots_dir.is_dir():
        snapshots_dir.mkdir()

    now = datetime.datetime.now()
    snapshot_dir = snapshots_dir / now.strftime("%Y%m%d%H%M%S")
    snapshot_dir.mkdir()

    return str(snapshot_dir)


def rialto_authors_file(data_dir):
    """Get the path to the rialto-orgs authors.csv"""
    authors_file = Path(data_dir) / "authors.csv"

    return authors_file


def rialto_authors_orcids(rialto_authors_file):
    """Extract the orcidid column from the authors.csv file"""
    orcids = []
    with open(rialto_authors_file, "r") as file:
        reader = csv.reader(file)
        header = next(reader)
        orcidid = header.index("orcidid")
        for row in reader:
            if row[orcidid]:
                orcids.append(row[orcidid])
    return orcids
