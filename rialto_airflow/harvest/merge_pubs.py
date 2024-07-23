import polars as pl

from rialto_airflow.utils import normalize_doi


def merge(sul_pub, openalex_pubs, dimensions_pubs, output):
    """
    Merge publication data from all sources, joining on DOI
    """
    dimensions_df = dimensions_pubs_df(dimensions_pubs)
    openalex_df = openalex_pubs_df(openalex_pubs)
    sul_pub_df = sulpub_df(sul_pub)

    # Join dataframes on their respective doi columns
    dim_openalex_df = dimensions_df.join(
        openalex_df, left_on="dim_doi", right_on="openalex_doi", how="full"
    )
    # make a DOI column with values from both sources for joining next df
    dim_openalex_df = dim_openalex_df.with_columns(
        pl.all(),
        pl.when(pl.col("dim_doi").is_null())
        .then(pl.col("openalex_doi"))
        .otherwise(pl.col("dim_doi"))
        .alias("dim_openalex_doi"),
    )

    # join sul_pub
    merged_df = dim_openalex_df.join(
        sul_pub_df, left_on="dim_openalex_doi", right_on="sul_pub_doi", how="full"
    )
    # create a doi column with values from all sources
    merged_df = merged_df.with_columns(
        pl.all(),
        pl.when(pl.col("dim_openalex_doi").is_null())
        .then(pl.col("sul_pub_doi"))
        .otherwise(pl.col("dim_openalex_doi"))
        .alias("doi"),
    )

    # Write output to Parquet
    merged_df.collect().write_parquet(output)


def dimensions_pubs_df(dimensions_pubs):
    """
    # Create a LazyFrame of dimension pubs to avoid loading all data into memory
    """
    # Polars is inferring volume is an integer, but it should be a string e.g. "97-B"
    df = pl.scan_csv(
        dimensions_pubs,
        schema_overrides={"volume": pl.String, "pmid": pl.String, "year": pl.String},
        low_memory=True,
    )
    df = df.select(
        pl.col("doi").map_elements(normalize_doi, return_dtype=pl.String),
        pl.col(
            "document_type",
            "funders",
            "funding_section",
            "open_access",
            "publisher",
            "research_orgs",
            "researchers",
            "title",
            "type",
            "year",
        ),
    )
    df = df.rename(lambda column_name: "dim_" + column_name)
    return df


def openalex_pubs_df(openalex_pubs):
    """
    Create an openalex pubs LazyFrame and rename columns
    """
    df = pl.scan_csv(
        openalex_pubs, schema_overrides={"publication_year": pl.String}, low_memory=True
    )
    df = df.select(
        pl.col("doi").map_elements(normalize_doi, return_dtype=pl.String),
        pl.col("apc_paid", "grants", "publication_year", "title", "type"),
    )
    df = df.rename(lambda column_name: "openalex_" + column_name)
    return df


def sulpub_df(sul_pub):
    """
    Create a sulpub LazyFrame and rename columns
    """
    df = pl.scan_csv(
        sul_pub,
        schema_overrides={"year": pl.String, "pmid": pl.String},
        low_memory=True,
    )
    df = df.drop_nulls("doi")
    df = df.with_columns(
        pl.col("doi").map_elements(normalize_doi, return_dtype=pl.String)
    )
    df = df.rename(lambda column_name: "sul_pub_" + column_name)
    return df
