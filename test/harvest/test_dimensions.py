import os
import dotenv
import pickle
import pytest

from rialto_airflow.harvest.dimensions import doi_orcids_pickle


dotenv.load_dotenv()

dimensions_user = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER")
dimensions_password = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS")

no_auth = not (dimensions_user and dimensions_password)


@pytest.mark.skipif(no_auth, reason="no dimensions key")
def test_doi_orcids_dict(tmpdir):
    pickle_file = tmpdir / "dimensions.pickle"
    doi_orcids_pickle("test/data/authors.csv", pickle_file, limit=5)
    assert pickle_file.isfile()

    with open(pickle_file, "rb") as handle:
        doi_orcids = pickle.load(handle)

    assert len(doi_orcids) > 0
    assert doi_orcids["10.1109/lra.2018.2890209"] == ["0000-0002-0770-2940"]
