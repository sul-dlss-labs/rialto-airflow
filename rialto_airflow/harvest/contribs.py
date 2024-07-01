import pickle
import polars as pl


def create_contribs(pubs_parquet, doi_sunet_pickle, authors, contribs_path):
    pubs = pl.read_parquet(pubs_parquet)
    authors = pl.read_csv(authors)

    doi_sunet = pickle.load(open(doi_sunet_pickle, "rb"))
    doi_sunet = pl.DataFrame({"doi": doi_sunet.keys(), "sunetid": doi_sunet.values()})

    pubs = pubs.join(doi_sunet, on="doi")

    contribs = pubs.explode("sunetid")

    contribs = contribs.join(authors, on="sunetid")

    contribs.write_parquet(contribs_path)

    return contribs_path
