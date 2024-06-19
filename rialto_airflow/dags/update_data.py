import datetime
import pathlib

from airflow.models import Variable
from airflow.decorators import dag, task

from rialto_airflow.utils import last_harvest, create_snapshot_dir
from rialto_airflow.harvest.sul_pub import sul_pub_csv

data_dir = Variable.get("data_dir")
sul_pub_host = Variable.get("sul_pub_host")
sul_pub_key = Variable.get("sul_pub_key")


@dag(
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
)
def update_data():
    @task(multiple_outputs=True)
    def setup():
        """
        Setup the data directory to write to and determine the last harvest.
        """
        return {
            "last_harvest": last_harvest(data_dir),
            "snapshot_dir": create_snapshot_dir(data_dir),
        }

    @task()
    def fetch_sul_pub(last_harvest, snapshot_dir):
        """
        Harvest data from sul_pub using the last harvest date.
        """
        csv_file = pathlib.Path(snapshot_dir) / "sulpub.csv"
        sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, since=last_harvest)

        return str(csv_file)

    @task()
    def extract_doi(sulpub):
        """
        Extract a unique list of DOIs from the new publications data.
        """
        return True

    @task()
    def fetch_openalex(dois):
        """
        Fetch the data by DOI from OpenAlex.
        """
        return True

    @task()
    def fetch_dimensions(dois):
        """
        Fetch the data by DOI from Dimensions.
        """
        return True

    @task()
    def merge_publications(sul_pub, openalex, dimensions):
        """
        Merge the OpenAlex, Dimensions and sul_pub data.
        """
        return True

    @task()
    def merge_contributors(pubs):
        """
        Merge in contributor and departmental data from rialto-orgs.
        """
        return True

    @task
    def create_dataset(pubs, contribs):
        """
        Aggregate the incremental snapshot data into a single dataset.
        """
        return True

    @task()
    def publish(dataset):
        """
        Publish aggregate data to JupyterHub environment.
        """
        return True

    config = setup()
    sul_pub = fetch_sul_pub(config["last_harvest"], config["snapshot_dir"])
    dois = extract_doi(sul_pub)
    openalex = fetch_openalex(dois)
    dimensions = fetch_dimensions(dois)
    pubs = merge_publications(sul_pub, openalex, dimensions)
    contribs = merge_contributors(pubs)
    dataset = create_dataset(pubs, contribs)
    publish(dataset)


update_data()
