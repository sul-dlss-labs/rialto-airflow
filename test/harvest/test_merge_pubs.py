import csv

import polars as pl
import pytest

from rialto_airflow.harvest import merge_pubs


@pytest.fixture
def dimensions_pubs_csv(tmp_path):
    fixture_file = tmp_path / "dimensions-pubs.csv"
    with open(fixture_file, "w") as csvfile:
        writer = csv.writer(csvfile)
        header = [
            "bogus",
            "volume",
            "authors",
            "document_type",
            "doi",
            "funders",
            "funding_section",
            "linkout",
            "open_access",
            "publisher",
            "research_orgs",
            "researchers",
            "title",
            "type",
            "year",
        ]
        writer.writerow(header)
        writer.writerow(
            [
                "a",
                "1",
                "[]",
                "ARTICLE",
                "10.0000/aAaA",
                "[]",
                "[]",
                "https://example.com/my-awesome-paper",
                "True",
                "publisher",
                "[]",
                "[]",
                "A Publication",
                "article",
                "2024",
            ]
        )
        writer.writerow(
            [
                "b",
                "2",
                "[]",
                "ARTICLE",
                "10.0000/1234",
                "[]",
                "[]",
                "https://example.com/yet-another-awesome-paper",
                "True",
                "publisher",
                "[]",
                "[]",
                "A Research Article",
                "article",
                "2024",
            ]
        )
    return fixture_file


@pytest.fixture
def openalex_pubs_csv(tmp_path):
    fixture_file = tmp_path / "openalex-pubs.csv"
    with open(fixture_file, "w") as csvfile:
        writer = csv.writer(csvfile)
        header = [
            "bogus",
            "apc_paid",
            "authorships",
            "grants",
            "publication_year",
            "title",
            "type",
            "doi",
            "open_access",
        ]
        writer.writerow(header)
        writer.writerow(
            [
                "blah",
                10,
                "[]",
                "[]",
                "2024",
                "A Publication",
                "article",
                "https://doi.org/10.0000/cccc",
                "green"
            ]
        )
        writer.writerow(
            [
                "blah",
                0,
                "[]",
                "[]",
                "2024",
                "A Research Article",
                "article",
                "https://doi.org/10.0000/1234",
                "bronze"
            ]
        )
    return fixture_file


@pytest.fixture
def sul_pubs_csv(tmp_path):
    fixture_file = tmp_path / "sulpub.csv"
    with open(fixture_file, "w") as csvfile:
        writer = csv.writer(csvfile)
        header = ["authorship", "title", "year", "doi"]
        writer.writerow(header)
        writer.writerow(["[]", "A Publication", 2024, "10.0000/cccc"])
        writer.writerow(
            [
                "[]",
                "A Research Article",
                2024,
            ]
        )
        writer.writerow(
            [
                "[]",
                "A Published Research Article",
                2024,
                "doi: 10.0000/dDdD",
            ]
        )
        writer.writerow(
            [
                "[]",
                "A Published Research Article",
                "n/ a",
                "doi: 10.0000/eeee",
            ]
        )
    return fixture_file


def test_dimensions_pubs_df(dimensions_pubs_csv):
    lazy_df = merge_pubs.dimensions_pubs_df(dimensions_pubs_csv)
    assert isinstance(lazy_df, pl.lazyframe.frame.LazyFrame)
    df = lazy_df.collect()
    assert df.shape[0] == 2
    assert "bogus" not in df.columns, "Unneeded columns have been dropped"
    assert df["dim_doi"].to_list() == ["10.0000/aaaa", "10.0000/1234"]


def test_openalex_pubs_df(openalex_pubs_csv):
    lazy_df = merge_pubs.openalex_pubs_df(openalex_pubs_csv)
    assert isinstance(lazy_df, pl.lazyframe.frame.LazyFrame)
    df = lazy_df.collect()
    assert df.shape[0] == 2
    assert "bogus" not in df.columns, "Unneeded columns have been dropped"
    assert "openalex_open_access" in df.columns
    assert df["openalex_doi"].to_list() == ["10.0000/cccc", "10.0000/1234"]


def test_sulpub_df(sul_pubs_csv):
    lazy_df = merge_pubs.sulpub_df(sul_pubs_csv)
    assert isinstance(lazy_df, pl.lazyframe.frame.LazyFrame)
    df = lazy_df.collect()
    assert df.shape[0] == 3, "Row without a doi has been dropped"
    assert df.columns == [
        "sul_pub_authorship",
        "sul_pub_title",
        "sul_pub_year",
        "sul_pub_doi",
    ]
    assert df["sul_pub_doi"].to_list() == [
        "10.0000/cccc",
        "10.0000/dddd",
        "10.0000/eeee",
    ]


def test_merge(tmp_path, sul_pubs_csv, openalex_pubs_csv, dimensions_pubs_csv):
    output = tmp_path / "merged_pubs.parquet"
    merge_pubs.merge(sul_pubs_csv, openalex_pubs_csv, dimensions_pubs_csv, output)
    assert output.is_file(), "output file has been created"
    df = pl.read_parquet(output)
    assert df.shape[0] == 5
    assert df.shape[1] == 21
    assert set(df["doi"].to_list()) == set(
        ["10.0000/aaaa", "10.0000/1234", "10.0000/cccc", "10.0000/dddd", "10.0000/eeee"]
    )
