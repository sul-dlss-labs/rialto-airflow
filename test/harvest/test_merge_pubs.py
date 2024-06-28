import json

import polars as pl
import pytest

from rialto_airflow.harvest import merge_pubs
from rialto_airflow.utils import write_jsonl


@pytest.fixture
def dimensions_pubs_jsonl(tmp_path):
    fixture_file = tmp_path / "dimensions-pubs.jsonl"
    write_jsonl(
        fixture_file,
        [
            {
                "bogus": "a",
                "volume": "1",
                "authors": [],
                "document_type": "ARTICLE",
                "doi": "10.0000/aaaa",
                "funders": [],
                "funding_section": [],
                "open_access": "True",
                "publisher": "publisher",
                "research_orgs": [],
                "researchers": [],
                "title": "A Publication",
                "type": "article",
                "year": "2024",
            },
            {
                "bogus": "b",
                "volume": "2",
                "authors": [],
                "document_type": "ARTICLE",
                "doi": "10.0000/1234",
                "funders": [],
                "funding_section": [],
                "open_access": "True",
                "publisher": "publisher",
                "research_orgs": [],
                "researchers": [],
                "title": "A Research Article",
                "type": "article",
                "year": "2024",
            },
        ],
    )

    return fixture_file


@pytest.fixture
def openalex_pubs_jsonl(tmp_path):
    fixture_file = tmp_path / "openalex-pubs.jsonl"
    write_jsonl(
        fixture_file,
        [
            {
                "bogus": "blah",
                "apc_paid": 10,
                "authorships": [],
                "grants": [],
                "publication_year": "2024",
                "title": "A Publication",
                "type": "article",
                "doi": "https://doi.org/10.0000/cccc",
            },
            {
                "bogus": "blah",
                "apc_paid": 0,
                "authorships": [],
                "grants": [],
                "publication_year": "2024",
                "title": "A Research Article",
                "type": "article",
                "doi": "https://doi.org/10.0000/1234",
            },
        ],
    )

    return fixture_file


@pytest.fixture
def sul_pubs_jsonl(tmp_path):
    fixture_file = tmp_path / "sulpub.jsonl"
    write_jsonl(
        fixture_file,
        [
            {
                "authorship": [],
                "title": "A Publication",
                "year": "2024",
                "doi": "10.0000/cccc",
            },
            {"authorship": [], "title": "A Research Article", "year": "2024"},
            {
                "authorship": [],
                "title": "A Published Research Article",
                "year": "2024",
                "doi": "https://doi.org/10.0000/dddd",
            },
        ],
    )

    return fixture_file


def test_dimensions_pubs_df(dimensions_pubs_jsonl):
    lazy_df = merge_pubs.dimensions_pubs_df(dimensions_pubs_jsonl)
    assert type(lazy_df) == pl.lazyframe.frame.LazyFrame
    df = lazy_df.collect()
    assert df.shape[0] == 2
    assert "bogus" not in df.columns, "Unneeded columns have been dropped"
    assert df["dim_doi"].to_list() == ["10.0000/aaaa", "10.0000/1234"]


def test_openalex_pubs_df(openalex_pubs_jsonl):
    lazy_df = merge_pubs.openalex_pubs_df(openalex_pubs_jsonl)
    assert type(lazy_df) == pl.lazyframe.frame.LazyFrame
    df = lazy_df.collect()
    assert df.shape[0] == 2
    assert "bogus" not in df.columns, "Unneeded columns have been dropped"
    assert df["openalex_doi"].to_list() == ["10.0000/cccc", "10.0000/1234"]


def test_sulpub_df(sul_pubs_jsonl):
    lazy_df = merge_pubs.sulpub_df(sul_pubs_jsonl)
    assert type(lazy_df) == pl.lazyframe.frame.LazyFrame
    df = lazy_df.collect()
    assert df.shape[0] == 2, "Row without a doi has been dropped"
    assert df.columns == [
        "sul_pub_authorship",
        "sul_pub_title",
        "sul_pub_year",
        "sul_pub_doi",
    ]
    assert df["sul_pub_doi"].to_list() == ["10.0000/cccc", "10.0000/dddd"]


def test_merge(tmp_path, sul_pubs_jsonl, openalex_pubs_jsonl, dimensions_pubs_jsonl):
    output = tmp_path / "merged_pubs.parquet"
    merge_pubs.merge(sul_pubs_jsonl, openalex_pubs_jsonl, dimensions_pubs_jsonl, output)
    assert output.is_file(), "output file has been created"
    df = pl.read_parquet(output)
    assert df.shape[0] == 4
    assert df.shape[1] == 25
    assert set(df["doi"].to_list()) == set(
        ["10.0000/aaaa", "10.0000/1234", "10.0000/cccc", "10.0000/dddd"]
    )
