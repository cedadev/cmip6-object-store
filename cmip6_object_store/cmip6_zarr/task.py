import os

from .. import logging
from ..config import CONFIG
from .batch import BatchManager
from .lotus import Lotus
from .utils import create_dir
from .zarr_writer import ZarrWriter

LOGGER = logging.getLogger(__file__)


class ConversionTask(object):
    def __init__(self, batch_number, project, run_mode="lotus"):
        self._batch_number = batch_number
        self._project = project

        if run_mode == "local":
            self.run = self._run_local
        else:
            self.run = self._run_lotus

    #    @profile(precision=1)
    def _run_local(self):
        batch = self._batch_number
        LOGGER.info(f"Running conversion locally: {batch}")

        batch_manager = BatchManager(self._project)
        dataset_ids = batch_manager.get_batch(batch)

        zarr_writer = ZarrWriter(batch, self._project)

        for dataset_id in dataset_ids:
            zarr_writer.convert(dataset_id)

    def _run_lotus(self):
        LOGGER.info(f"Submitting conversion to Lotus: " f"{self._batch_number}")
        cmd = (
            f"./cmip6_object_store/cmip6_zarr/cli.py "
            f"run -b {self._batch_number} -r local"
        )

        duration = CONFIG["workflow"]["max_duration"]
        lotus_log_dir = os.path.join(
            CONFIG["log"]["log_base_dir"], self._project, "lotus"
        )
        create_dir(lotus_log_dir)

        stdout = f"{lotus_log_dir}/{self._batch_number}.out"
        stderr = f"{lotus_log_dir}/{self._batch_number}.err"

        partition = CONFIG["workflow"]["job_queue"]

        lotus = Lotus()
        lotus.run(
            cmd,
            stdout=stdout,
            stderr=stderr,
            partition=partition,
            duration=duration,
        )


class TaskManager(object):
    def __init__(self, project, batches=None, run_mode="lotus", ignore_complete=True):
        self._project = project
        self._batches = batches
        self._run_mode = run_mode
        self._ignore_complete = ignore_complete
        self._batch_manager = BatchManager(project)
        #        self._load_datasets()

        self._setup()

    def _setup(self):
        if not self._batches:
            self._batches = range(1, len(self._batch_manager.get_batch_files()) + 1)

    def _filter_datasets(self):
        base_dir = CONFIG["log"]["log_base_dir"]
        log_file = os.path.join(base_dir, f"{self._project}.log")

        if os.path.isfile(log_file):
            with open(log_file) as reader:
                successes = reader.read().strip().split()
        else:
            successes = []

        self._datasets = sorted(list(set(self._datasets) - set(successes)))

    def get_batch(self):
        batch_size = CONFIG["workflow"]["batch_size"]

        batch = self._datasets[:batch_size]
        self._datasets = self._datasets[batch_size:]

        return batch

    def run_tasks(self):
        for batch in self._batches:
            task = ConversionTask(batch, project=self._project, run_mode=self._run_mode)
            task.run()
