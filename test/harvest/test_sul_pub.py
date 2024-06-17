import os
import datetime

import dotenv
import pandas
import pytest

from rialto_airflow.harvest.sul_pub import sul_pub_csv

dotenv.load_dotenv()

sul_pub_host = os.environ.get("AIRFLOW_VAR_SUL_PUB_HOST")
sul_pub_key = os.environ.get("AIRFLOW_VAR_SUL_PUB_KEY")

no_auth = not (sul_pub_host and sul_pub_key)


@pytest.mark.skipif(no_auth, reason="no sul_pub key")
def test_sul_pub_csv(tmpdir):
    csv_file = tmpdir / "sul_pub.csv"
    sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, limit=2000)
    assert csv_file.isfile()

    df = pandas.read_csv(csv_file)
    assert len(df) == 2000
    assert "title" in df.columns


@pytest.mark.skip(reason="sul_pub changeSince broken")
@pytest.mark.skipif(no_auth, reason="no sul_pub key")
def test_sul_pub_csv_since(tmpdir):
    csv_file = tmpdir / "sul_pub.csv"
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    sul_pub_csv(csv_file, sul_pub_host, sul_pub_key, since=since, limit=100)

    df = pandas.read_csv(csv_file, parse_dates=["last_updated"])
    assert len(df[df["last_updated"] < since]) == 0
