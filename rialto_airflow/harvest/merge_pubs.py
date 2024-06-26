import polars as pl


def merge(sul_pub, openalex_pubs, dimensions_pubs, output):
    """
    Merge publication data from all sources, joining on DOI
    """
    # Using lazy API to avoid loading all data into memory
    # Polars is inferring volume is an integer, but it should be a string e.g. "97-B"
    dimensions_df = pl.scan_csv(dimensions_pubs, schema_overrides={"volume": pl.String})
    dimensions_df = dimensions_df.select(
        pl.col(
            "authors",
            "document_type",
            "doi",
            "funders",
            "funding_section",
            "open_access",
            "publisher",
            "research_orgs",
            "researchers",
            "title",
            "type",
            "year",
        )
    )
    dimensions_df = dimensions_df.rename(lambda column_name: "dim_" + column_name)

    # Make an openalex df and rename columns
    openalex_df = pl.scan_csv(openalex_pubs)
    openalex_df = openalex_df.select(
        pl.col("doi").str.replace("https://doi.org/", ""),
        pl.col(
            "apc_paid", "authorships", "grants", "publication_year", "title", "type"
        ),
    )
    openalex_df = openalex_df.rename(lambda column_name: "openalex_" + column_name)

    # Make a sulpub df and rename columns
    sul_pub_df = pl.scan_csv(sul_pub)
    sul_pub_df = sul_pub_df.drop_nulls("doi")
    sul_pub_df = sul_pub_df.rename(lambda column_name: "sul_pub_" + column_name)

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
