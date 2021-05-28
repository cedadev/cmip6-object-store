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
    def __init__(
        self,
        project,
        batches=None,
        datasets=None,
        run_mode="lotus",
        ignore_complete=True,
    ):

        self._project = project
        self._batches = batches
        self._datasets = datasets
        self._run_mode = run_mode

        self._ignore_complete = ignore_complete
        self._batch_manager = BatchManager(project)
        self._setup()

    def _setup(self):
        
        allowed_batch_numbers = [
            self._batch_manager.batch_file_to_batch_number(batch_file_path)
            for batch_file_path in self._batch_manager.get_batch_files()
        ]

        # Overwrite batch
        if self._datasets:
            if self._batches:
                LOGGER.warning("Overwriting batches based on dataset selection!")

            self._filter_batches_by_dataset()

        if not self._batches:
            self._batches = range(1, len(allowed_batch_numbers) + 1)

        # Now make sure that there are no batches out of the range of
        # the available batches
        batches = [num for num in self._batches if num in allowed_batch_numbers]

        # Log issue if some have been removed
        if batches != self._batches:
            LOGGER.warn(f"Removed some batches that are not in range.")
            self._batches = batches

    def _filter_batches_by_dataset(self):
        """Works out which batches relate to those in self._datasets.
        Overwrites the value of self._batches accordingly.
        """
        batches = []
        datasets = set(self._datasets)

        for batch_file_path in self._batch_manager.get_batch_files():
            datasets_in_batch = set(open(batch_file_path).read().strip().split())

            if datasets & datasets_in_batch:
                batch_number = self._batch_manager.batch_file_to_batch_number(
                    batch_file_path
                )
                batches.append(batch_number)

        self._batches = sorted(batches)

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
        if not len(self._batches):
            LOGGER.warn("Nothing to run!")
            return

        for batch in self._batches:
            task = ConversionTask(batch, project=self._project, run_mode=self._run_mode)
            task.run()
