import os
import dotenv
import pickle
import pytest

from rialto_airflow.harvest.dimensions import dimensions_doi_orcids_dict, invert_dict

dotenv.load_dotenv()

dimensions_user = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER")
dimensions_password = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS")

no_auth = not (dimensions_user and dimensions_password)


@pytest.mark.skipif(no_auth, reason="no dimensions key")
def test_dimensions_doi_orcids_dict(tmpdir):
    pickle_file = tmpdir / "dimensions.pickle"
    dimensions_doi_orcids_dict("test/data/authors.csv", pickle_file, limit=5)
    assert pickle_file.isfile()

    with open(pickle_file, "rb") as handle:
        doi_orcids = pickle.load(handle)

    assert len(doi_orcids) > 0
    assert doi_orcids["https://doi.org/10.1109/lra.2018.2890209"] == [
        "0000-0002-0770-2940"
    ]


def test_invert_dict():
    dict = {
        "person_id1": ["pub_id1", "pub_id2", "pub_id3"],
        "person_id2": ["pub_id2", "pub_id4", "pub_id5"],
        "person_id3": ["pub_id5", "pub_id6", "pub_id7"],
    }

    inverted_dict = invert_dict(dict)
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
