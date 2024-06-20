import pickle
import re

from rialto_airflow.harvest import openalex


def test_dois_from_orcid():
    dois = list(openalex.dois_from_orcid("0000-0002-1298-3089"))
    assert len(dois) >= 54


def test_works_from_author_id():
    # the dimensions api returns 200 publications at a time, so ensure that paging is working
    # for Akihisa Inoue who has a lot of publications (> 4,000)
    works = list(openalex.works_from_author_id("a5008412118", limit=600))
    assert len(works) == 600, "paging is limiting to 600 works"
    assert len(set([work["id"] for work in works])) == 600, "the works are unique"


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
