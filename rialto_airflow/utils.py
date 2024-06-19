import datetime

from pathlib import Path


def create_snapshot_dir(data_dir):
    snapshots_dir = Path(data_dir) / "snapshots"

    if not snapshots_dir.is_dir():
        snapshots_dir.mkdir()

    now = datetime.datetime.now()
    snapshot_dir = snapshots_dir / now.strftime("%Y%m%d%H%M%S")
    snapshot_dir.mkdir()

    return str(snapshot_dir)
