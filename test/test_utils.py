from pathlib import Path

from rialto_airflow.utils import create_snapshot_dir


def test_create_snapshot_dir(tmpdir):
    snap_dir = Path(create_snapshot_dir(tmpdir))
    assert snap_dir.is_dir()
