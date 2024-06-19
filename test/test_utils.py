import csv
from pathlib import Path
import pytest
from rialto_airflow.utils import create_snapshot_dir, rialto_authors_orcids


@pytest.fixture
def authors_csv(tmp_path):
    # Create a fixture authors CSV file
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "orcidid"])
        writer.writerow(["author1", "https://orcid.org/0000-0000-0000-0001"])
        writer.writerow(["author2", "https://orcid.org/0000-0000-0000-0002"])
    return fixture_file


def test_create_snapshot_dir(tmpdir):
    snap_dir = Path(create_snapshot_dir(tmpdir))
    assert snap_dir.is_dir()


def test_rialto_authors_orcids(tmp_path, authors_csv):
    orcids = rialto_authors_orcids(authors_csv)
    assert len(orcids) == 2
    assert "https://orcid.org/0000-0000-0000-0001" in orcids
