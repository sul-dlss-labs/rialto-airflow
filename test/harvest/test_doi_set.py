import csv
import pickle

import pytest

from rialto_airflow.harvest.doi_set import create_doi_set


@pytest.fixture
def dimensions_pickle(tmp_path):
    data = {
        "10.0000/1234": ["https://orcid.org/0000-0000-0000-0001"],
        "10.0000/cccc": ["https://orcid.org/0000-0000-0000-0002"],
    }
    pickle_file = tmp_path / "dimensions.pickle"
    with open(pickle_file, "wb") as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return pickle_file


@pytest.fixture
def openalex_pickle(tmp_path):
    data = {
        "10.0000/cccc": ["https://orcid.org/0000-0000-0000-0001"],
        "10.0000/zzzz": ["https://orcid.org/0000-0000-0000-0002"],
    }
    pickle_file = tmp_path / "openalex.pickle"
    with open(pickle_file, "wb") as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return pickle_file


@pytest.fixture
def sul_pub_csv(tmp_path):
    fixture_file = tmp_path / "sul_pub.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "title", "doi"])
        writer.writerow(["author1", "A Publication", "10.0000/aaaa"])
        writer.writerow(["author2", "A Research Article", "10.0000/1234"])
    return fixture_file


def test_doi_set(dimensions_pickle, openalex_pickle, sul_pub_csv):
    dois = create_doi_set(dimensions_pickle, openalex_pickle, sul_pub_csv)
    assert len(dois) == 4
    assert set(dois) == set(
        ["10.0000/1234", "10.0000/aaaa", "10.0000/cccc", "10.0000/zzzz"]
    )
