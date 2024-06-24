import datetime
from pathlib import Path

from airflow.models import Variable
from airflow.decorators import dag, task

from rialto_airflow.utils import create_snapshot_dir, rialto_authors_file
from rialto_airflow.harvest import dimensions, openalex
from rialto_airflow.harvest.sul_pub import sul_pub_csv
from rialto_airflow.harvest.doi_set import create_doi_set


data_dir = Variable.get("data_dir")
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")

# to artificially limit the API activity in development
dev_limit = Variable.get("dev_limit")
if dev_limit is not None:
    dev_limit = int(dev_limit)


@dag(
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def harvest():
    @task()
    def setup():
        """
        Setup the data directory.
        """
        snapshot_dir = create_snapshot_dir(data_dir)
        return snapshot_dir

    @task()
    def find_authors_csv():
        """
        Find and return the path to the rialto-orgs authors.csv snapshot.
        """
        return rialto_authors_file(data_dir)

    @task()
    def dimensions_harvest_dois(authors_csv, snapshot_dir):
        """
        Fetch the data by ORCID from Dimensions.
        """
        pickle_file = Path(snapshot_dir) / "dimensions-doi-orcid.pickle"
        dimensions.doi_orcids_pickle(authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def openalex_harvest_dois(authors_csv, snapshot_dir):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        pickle_file = Path(snapshot_dir) / "openalex-doi-orcid.pickle"
        openalex.doi_orcids_pickle(authors_csv, pickle_file, limit=dev_limit)
        return str(pickle_file)

    @task()
    def sul_pub_harvest(snapshot_dir):
        """
        Harvest data from SUL-Pub.
        """
        csv_file = Path(snapshot_dir) / "sulpub.csv"
        sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=dev_limit)

        return str(csv_file)

    @task()
    def doi_set(dimensions, openalex, sul_pub):
        """
        Extract a unique list of DOIs from the dimensions doi-orcid dict,
        the openalex doi-orcid dict, and the SUL-Pub publications.
        """
        return create_doi_set(dimensions, openalex, sul_pub)

    @task()
    def dimensions_harvest_pubs(dois, snapshot_dir):
        """
        Harvest publication metadata from Dimensions using the dois from doi_set.
        """
        csv_file = Path(snapshot_dir) / "dimensions-pubs.csv"
        dimensions.publications_csv(dois, csv_file)
        return str(csv_file)

    @task()
    def openalex_harvest_pubs(dois):
        """
        Harvest publication metadata from OpenAlex using the dois from doi_set.
        """
        return True

    @task()
    def merge_publications(sul_pub, openalex, dimensions):
        """
        Merge the OpenAlex, Dimensions and sul_pub data.
        """
        return True

    @task()
    def join_authors(pubs, authors_csv):
        """
        Add the Stanford organizational data to the publications.
        """
        return True

    @task()
    def pubs_to_contribs(pubs):
        """
        Get contributions from publications.
        """
        return True

    @task()
    def publish(dataset):
        """
        Publish aggregate data to JupyterHub environment.
        """
        return True

    snapshot_dir = setup()

    authors_csv = find_authors_csv()

    sul_pub = sul_pub_harvest(snapshot_dir)

    dimensions_dois = dimensions_harvest_dois(authors_csv, snapshot_dir)

    openalex_dois = openalex_harvest_dois(authors_csv, snapshot_dir)

    dois = doi_set(dimensions_dois, openalex_dois, sul_pub)

    dimensions_pubs = dimensions_harvest_pubs(dois, snapshot_dir)

    openalex_pubs = openalex_harvest_pubs(dois)

    pubs = merge_publications(sul_pub, dimensions_pubs, openalex_pubs)

    pubs_authors = join_authors(pubs, authors_csv)

    contribs = pubs_to_contribs(pubs_authors)

    publish(contribs)


harvest()
