import os
import pickle

import dotenv
import pandas

from rialto_airflow.harvest import dimensions

dotenv.load_dotenv()

dimensions_user = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_USER")
dimensions_password = os.environ.get("AIRFLOW_VAR_DIMENSIONS_API_PASS")


def test_doi_orcids_dict(tmpdir):
    pickle_file = tmpdir / "dimensions.pickle"
    dimensions.doi_orcids_pickle("test/data/authors.csv", pickle_file, limit=5)
    assert pickle_file.isfile()

    with open(pickle_file, "rb") as handle:
        doi_orcids = pickle.load(handle)

    assert len(doi_orcids) > 0
    assert doi_orcids["10.1109/lra.2018.2890209"] == ["0000-0002-0770-2940"]
    assert "https://doi.org/" not in list(doi_orcids.keys())[0], "doi is an ID"


def test_publications_from_dois():
    # use batch_size=1 to test paging for two DOIs
    pubs = list(
        dimensions.publications_from_dois(
            ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], batch_size=1
        )
    )
    assert len(pubs) == 2
    assert len(pubs[0].keys()) == 74, "first publication has 74 columns"
    assert len(pubs[1].keys()) == 74, "second publication has 74 columns"


def test_publication_fields():
    fields = dimensions.publication_fields()
    assert len(fields) == 74
    assert "title" in fields


def test_publications_csv(tmpdir):
    pubs_csv = tmpdir / "dimensions-pubs.csv"
    dimensions.publications_csv(
        ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], pubs_csv
    )

    df = pandas.read_csv(pubs_csv)

    assert len(df) == 2

    # the order of the results isn't guaranteed but make sure things are coming back

    assert set(df.title.tolist()) == set(
        ["On the Dangers of Stochastic Parrots", "Attention Is All You Need"]
    )

    assert set(df.doi.tolist()) == set(
        ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"]
    )
