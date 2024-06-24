import pickle
import re

import pandas

from rialto_airflow.harvest import openalex


def test_dois_from_orcid():
    dois = list(openalex.dois_from_orcid("0000-0002-1298-3089"))
    assert len(dois) >= 54


def test_dois_from_orcid_paging():
    # the dimensions api returns 200 publications at a time, so ensure that paging is working
    # for Akihisa Inoue who has a lot of publications (> 4,000)
    dois = list(openalex.dois_from_orcid("0000-0001-6009-8669", limit=600))
    assert len(dois) == 600, "paging is limiting to 600 works"
    assert len(set(dois)) == 600, "the dois are unique"


def test_doi_orcids_pickle(tmp_path):
    # authors_csv, pickle_file):
    pickle_file = tmp_path / "openalex-doi-orcid.pickle"
    openalex.doi_orcids_pickle("test/data/authors.csv", pickle_file)
    assert pickle_file.is_file(), "created the pickle file"

    mapping = pickle.load(pickle_file.open("rb"))
    assert isinstance(mapping, dict)
    assert len(mapping) > 0

    doi = list(mapping.keys())[0]
    assert "/" in doi

    orcids = mapping[doi]
    assert isinstance(orcids, list)
    assert len(orcids) > 0
    assert re.match(r"^\d+-\d+-\d+-\d+$", orcids[0])


def test_publications_from_dois():
    pubs = list(
        openalex.publications_from_dois(
            ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"]
        )
    )
    assert len(pubs) == 2
    assert set(openalex.FIELDS) == set(pubs[0].keys()), "All fields accounted for."
    assert len(pubs[0].keys()) == 51, "first publication has 51 columns"
    assert len(pubs[1].keys()) == 51, "second publication has 51 columns"


def test_publications_csv(tmp_path):
    pubs_csv = tmp_path / "openalex-pubs.csv"
    openalex.publications_csv(
        ["10.48550/arxiv.1706.03762", "10.1145/3442188.3445922"], pubs_csv
    )

    df = pandas.read_csv(pubs_csv)

    assert len(df) == 2

    # the order of the results isn't guaranteed but make sure things are coming back

    assert set(df.title.tolist()) == set(
        ["On the Dangers of Stochastic Parrots", "Attention Is All You Need"]
    )

    assert set(df.doi.tolist()) == set(
        [
            "https://doi.org/10.48550/arxiv.1706.03762",
            "https://doi.org/10.1145/3442188.3445922",
        ]
    )
