import os
import re
import time

from rialto_airflow.utils import last_harvest, create_snapshot_dir

def test_no_last_harvest(tmpdir):
    assert last_harvest(tmpdir) is None

def test_create_snapshot_dir(tmpdir):
    snap_dir = create_snapshot_dir(tmpdir)
    assert os.path.isdir(snap_dir)
    assert re.match(r'^\d{14}$', os.path.basename(snap_dir))

def test_last_harvest(tmpdir):
    create_snapshot_dir(tmpdir)
    last_harvest_1 = last_harvest(tmpdir)
    assert last_harvest_1

    time.sleep(3)

    create_snapshot_dir(tmpdir)
    last_harvest_2 = last_harvest(tmpdir)
    assert last_harvest_2

    assert last_harvest_2 > last_harvest_1

