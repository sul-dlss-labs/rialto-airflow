import csv
import pickle

import pytest

from rialto_airflow.harvest.doi_sunet import create_doi_sunet_pickle, combine_maps

#
# create fixture data for dimensions, openalex, sulpub and authors
# such that there are four publications that are authored by two authors
#
# 10.0000/1234:
#   - sunet1 (via dimensions)
#   - sunet2 (via sul_pub)
#
# 10.0000/aaaa:
#   - sunet1 (via dimensions)
#
# 10.0000/cccc:
#   - sunet1 (via openalex)
#   - sunet2 (via dimensions)
#
# 10.0000/zzzz:
#   - sunet2 (via openalex)
#


@pytest.fixture
def dimensions_pickle(tmp_path):
    data = {
        "10.0000/1234": ["0000-0000-0000-0001"],
        "10.0000/cccc": ["0000-0000-0000-0002"],
    }
    pickle_file = tmp_path / "dimensions.pickle"
    with open(pickle_file, "wb") as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return pickle_file


@pytest.fixture
def openalex_pickle(tmp_path):
    data = {
        "10.0000/cccc": ["0000-0000-0000-0001"],
        "10.0000/zzzz": ["0000-0000-0000-0002"],
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
        writer.writerow(["authorship", "title", "doi"])
        writer.writerow([authorship("cap-01"), "A Publication", "10.0000/aaaa"])
        writer.writerow([authorship("cap-02"), "A Research Article", "10.0000/1234"])
    return fixture_file


@pytest.fixture
def authors_csv(tmp_path):
    fixture_file = tmp_path / "authors.csv"
    with open(fixture_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sunetid", "orcidid", "cap_profile_id"])
        writer.writerow(["sunet1", "https://orcid.org/0000-0000-0000-0001", "cap-01"])
        writer.writerow(["sunet2", "https://orcid.org/0000-0000-0000-0002", "cap-02"])
    return fixture_file


def authorship(cap_id):
    return str([{"cap_profile_id": cap_id, "status": "approved"}])


def test_doi_sunet(
    dimensions_pickle, openalex_pickle, sul_pub_csv, authors_csv, tmp_path
):
    output_file = tmp_path / "doi_sunet.pickle"

    create_doi_sunet_pickle(
        dimensions_pickle, openalex_pickle, sul_pub_csv, authors_csv, output_file
    )

    doi_sunet = pickle.load(open(output_file, "rb"))

    assert len(doi_sunet) == 4
    assert set(doi_sunet["10.0000/1234"]) == set(["sunet1", "sunet2"])
    assert doi_sunet["10.0000/aaaa"] == ["sunet1"]
    assert set(doi_sunet["10.0000/cccc"]) == set(["sunet1", "sunet2"])
    assert doi_sunet["10.0000/zzzz"] == ["sunet2"]


def test_combine_maps():
    m1 = {"a": [1], "b": [2]}
    m2 = {"a": [1, 2], "b": [3]}
    m3 = {"c": [4]}

    m4 = combine_maps(m1, m2, m3)
    assert len(m4) == 3
    assert set(m4["a"]) == set([1, 2])
    assert set(m4["b"]) == set([2, 3])
    assert set(m4["c"]) == set([4])
