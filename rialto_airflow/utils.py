import os
import datetime

snap_dir_format = '%Y%m%d%H%M%S'


def last_harvest(data_dir):
    """
    Determine the last time a harvest was run using the most recent snapshot
    directory in the supplied data directory.
    """
    snapshot_dirs = sorted(os.listdir(data_dir))

    if '.keep' in snapshot_dirs:
        snapshot_dirs.remove('.keep')

    if len(snapshot_dirs) == 0:
        return None

    snapshot_time = datetime.datetime.strptime(snapshot_dirs[-1], snap_dir_format)
    snapshot_time = snapshot_time.replace(tzinfo=datetime.timezone.utc)
    return snapshot_time


def create_snapshot_dir(data_dir):
    """
    Create a snapshot directory in the given data directory.
    """
    now = datetime.datetime.now()
    snapshot_dir = os.path.join(data_dir, now.strftime(snap_dir_format))
    os.mkdir(snapshot_dir)

    return snapshot_dir
