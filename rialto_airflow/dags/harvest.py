import datetime
import pathlib

from airflow.models import Variable
from airflow.decorators import dag, task

from rialto_airflow.utils import create_snapshot_dir
from rialto_airflow.harvest.sul_pub import sul_pub_csv
from rialto_airflow.harvest.dimensions import dimensions_doi_orcids_dict

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
    def dimensions_harvest_dois(orcids):
        """
        Fetch the dois by ORCID from Dimensions.
        """
        return True

    @task()
    def openalex_harvest_dois(orcids):
        """
        Fetch the dois by ORCID from OpenAlex.
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
    def dimensions_harvest_pubs(orcids):
        """
        Fetch the DOIs from Dimensions by querying the ORCIDs.
        """
        author_file = pathlib.Path(snapshot_dir) / "authors.csv"
        pickle_file = pathlib.Path(snapshot_dir) / "dimensions_pubs_orcid_dict.pickle"

        doi_orcid_dict = dimensions_pubs_orcids_dict(pickle_file, pickle_file, limit=None)
        return doi_orcid_dict

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
    dimensions_dois = dimensions_harvest_dois(snapshot_dir)
    openalex_dois = openalex_harvest_dois(snapshot_dir)
    dois = doi_set(sul_pub, dimensions_dois, openalex_dois)
    dimensions_pubs = dimensions_harvest_pubs(dois)
    openalex_pubs = openalex_harvest_pubs(dois)
    pubs = merge_publications(sul_pub, dimensions_pubs, openalex_pubs)
    contribs = pubs_to_contribs(pubs)
    publish(contribs)


harvest()
