import datetime
import pathlib

from airflow.models import Variable
from airflow.decorators import dag, task

from rialto_airflow.utils import create_snapshot_dir
from rialto_airflow.harvest.sul_pub import sul_pub_csv

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
    def dimensions_harvest_orcid(orcids):
        """
        Fetch the data by ORCID from Dimensions.
        """
        return True

    @task()
    def openalex_harvest_orcid(orcids):
        """
        Fetch the data by ORCID from OpenAlex.
        """
        return True

    @task()
    def sul_pub_harvest(snapshot_dir):
        """
        Harvest data from SUL-Pub.
        """
        csv_file = pathlib.Path(snapshot_dir) / "sulpub.csv"
        sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=dev_limit)

        return str(csv_file)

    @task()
    def doi_set(dimensions, openalex, sul_pub):
        """
        Extract a unique list of DOIs from the dimensions doi-orcid dict,
        the openalex doi-orcid dict, and the SUL-Pub publications.
        """
        return True

    @task()
    def dimensions_harvest_doi(dois):
        """
        Harvest publication metadata from Dimensions using the dois from doi_set.
        """
        return True

    @task()
    def openalex_harvest_doi(dois):
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
    def join_org_data(pubs, org_data):
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
    sul_pub = sul_pub_harvest(snapshot_dir)
    dimensions_orcid = dimensions_harvest_orcid(snapshot_dir)
    openalex_orcid = openalex_harvest_orcid(snapshot_dir)
    dois = doi_set(sul_pub, dimensions_orcid, openalex_orcid)
    dimensions_doi = dimensions_harvest_doi(dois)
    openalex_doi = openalex_harvest_doi(dois)
    pubs = merge_publications(sul_pub, dimensions_doi, openalex_doi)
    contribs = pubs_to_contribs(pubs)
    publish(contribs)


harvest()
