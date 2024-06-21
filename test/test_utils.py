import csv
from pathlib import Path

import pytest

from rialto_airflow import utils


@pytest.fixture
def authors_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "orcidid"])
        writer.writerow(["author1", "https://orcid.org/0000-0000-0000-0001"])
        writer.writerow(["author2", ""])
        writer.writerow(["author3", "https://orcid.org/0000-0000-0000-0002"])
    return fixture_file


def test_create_snapshot_dir(tmpdir):
    snap_dir = Path(utils.create_snapshot_dir(tmpdir))
    assert snap_dir.is_dir()


def test_rialto_authors_orcids(tmp_path, authors_csv):
    orcids = utils.rialto_authors_orcids(authors_csv)
    assert len(orcids) == 2
    assert "https://orcid.org/0000-0000-0000-0001" in orcids


def test_rialto_authors_file():
    csv_file = utils.rialto_authors_file("test/data")
    assert Path(csv_file).is_file()

    with pytest.raises(Exception):
        utils.rialto_authors_file("/no/authors/file/here")


def test_invert_dict():
    dict = {
        "person_id1": ["pub_id1", "pub_id2", "pub_id3"],
        "person_id2": ["pub_id2", "pub_id4", "pub_id5"],
        "person_id3": ["pub_id5", "pub_id6", "pub_id7"],
    }

    inverted_dict = utils.invert_dict(dict)
    assert len(inverted_dict.items()) == 7
    assert sorted(inverted_dict.keys()) == [
        "pub_id1",
        "pub_id2",
        "pub_id3",
        "pub_id4",
        "pub_id5",
        "pub_id6",
        "pub_id7",
    ]
    assert inverted_dict["pub_id2"] == ["person_id1", "person_id2"]
