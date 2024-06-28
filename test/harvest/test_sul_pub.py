import os

import dotenv
import pandas
import pytest

from rialto_airflow.harvest import sul_pub

dotenv.load_dotenv()

sul_pub_host = os.environ.get("AIRFLOW_VAR_SUL_PUB_HOST")
sul_pub_key = os.environ.get("AIRFLOW_VAR_SUL_PUB_KEY")

no_auth = not (sul_pub_host and sul_pub_key)


@pytest.mark.skipif(no_auth, reason="no sul_pub key")
def test_publications_jsonl(tmpdir):
    jsonl_file = tmpdir / "sul_pub.jsonl"
    sul_pub.publications_jsonl(jsonl_file, sul_pub_host, sul_pub_key, limit=2000)
    assert jsonl_file.isfile()

    df = pandas.read_json(jsonl_file, orient="records", lines=True)
    assert len(df) == 2000
    assert "title" in df.columns

    # there should be some dois in here
    dois = df.doi[df.doi.notna()]
    assert len(dois) > 1, "there should be at least a few DOIs?"
    assert not dois.iloc[0].startswith("http://"), "DOI IDs not URLs"
