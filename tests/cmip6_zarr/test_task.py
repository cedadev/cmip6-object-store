import os
import glob

from cmip6_object_store.cmip6_zarr.task import TaskManager
from cmip6_object_store.config import CONFIG


def test_TaskManager():
    tm = TaskManager("cmip6")

    assert tm._run_mode == "lotus"

    data_dir = CONFIG["workflow"]["data_dir"]
    run_version = CONFIG["workflow"]["run_version"]
    version_dir = os.path.join(data_dir, run_version)
    num_batch = len(glob.glob(f"{version_dir}/batch_*.txt"))

    assert tm._batches == list(range(1, num_batch + 1))
