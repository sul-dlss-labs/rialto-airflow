import pickle
import re

import pandas
import pyalex
import pytest

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
    assert "https://doi.org/" not in doi, "doi is an ID"
    assert "/" in doi

    orcids = mapping[doi]
    assert isinstance(orcids, list)
    assert len(orcids) > 0
    assert re.match(r"^\d+-\d+-\d+-\d+$", orcids[0])


def test_publications_from_dois():
    # get 231 dois that we know are in openalex
    dois = pandas.read_csv("test/data/openalex-dois.csv").doi.to_list()
    assert len(dois) == 231

    # look up the publication metadata for them
    pubs = list(openalex.publications_from_dois(dois))
    assert len(pubs) == 231, "should paginate (page size=200)"
    assert len(pubs) == len(set([pub["doi"] for pub in pubs])), "DOIs are unique"
    assert set(openalex.FIELDS) == set(pubs[0].keys()), "All fields accounted for."
    assert len(pubs[0].keys()) == 51, "first publication has 51 columns"
    assert len(pubs[1].keys()) == 51, "second publication has 51 columns"


def test_publications_from_invalid_dois(caplog):
    # Error may change if OpenAlex API or pyalex changes
    invalid_dois = ["doi-with-comma,a", "10.1145/3442188.3445922"]
    assert len(list(openalex.publications_from_dois(invalid_dois))) == 1
    assert (
        "OpenAlex QueryError for doi-with-comma,a: Invalid query parameter"
        in caplog.text
    ), "logs error message"


def test_publications_from_invalid_with_comma(caplog):
    # OpenAlex will interpret a DOI string with a comma as two DOIs but
    # Does not return a result for the first half even if valid. Will return an empty list
    invalid_doi = ["10.1002/cncr.33546,-(wileyonlinelibrary.com)"]
    assert len(list(openalex.publications_from_dois(invalid_doi))) == 0
    assert (
        "OpenAlex QueryError for 10.1002/cncr.33546,-(wileyonlinelibrary.com): Invalid query parameter"
        in caplog.text
    ), "logs error message"


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


def test_pyalex_urlencoding():
    # this might start working if https://github.com/J535D165/pyalex/issues/41 is fixed
    with pytest.raises(pyalex.api.QueryError):
        pyalex.Works().filter(doi="10.1207/s15327809jls0703&4_2").count() == 1

    assert (
        pyalex.Works().filter(doi="10.1207/s15327809jls0703%264_2").count() == 1
    ), "url encoding the & works with OpenAlex API"

    assert (
        len(
            list(
                openalex.publications_from_dois(
                    ["10.1207/s15327809jls0703&4_2", "10.1145/3442188.3445922"]
                )
            )
        )
        == 2
    ), "we handle url URL encoding DOIs until pyalex does"
