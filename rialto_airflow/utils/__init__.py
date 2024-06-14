import os
import datetime

def last_harvest():
    # TODO: look in the data_dir to determine the last harvest
    return datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

def create_snapshot_dir(data_dir):
    now = datetime.datetime.now()
    snapshot_dir = os.path.join(data_dir, now.strftime('%Y%m%d%H%M%S'))
    os.mkdir(snapshot_dir)

    return snapshot_dir
