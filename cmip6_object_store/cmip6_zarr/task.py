import os

from .. import logging
from ..config import CONFIG
from .batch import BatchManager
from .lotus import Lotus
from .utils import create_dir
from .zarr_writer import ZarrWriter

LOGGER = logging.getLogger(__file__)


class ConversionTask(object):
    def __init__(self, batch_number, project):
        self._batch_number = batch_number
        self._project = project

    #    @profile(precision=1)
    def run(self):
        batch = self._batch_number
        LOGGER.info(f"Running conversion locally: {batch}")

        batch_manager = BatchManager(self._project)
        dataset_ids = batch_manager.get_batch(batch)
        zarr_writer = ZarrWriter(batch, self._project)

        for dataset_id in dataset_ids:
            zarr_writer.convert(dataset_id)

        LOGGER.info(f"{len(dataset_ids)} datasets processed in batch {batch}")



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

    def run_tasks(self):
        if not len(self._batches):
            LOGGER.warn("Nothing to run!")
            return

        if self._run_mode == 'local':
            for batch in self._batches:
                task = ConversionTask(batch, project=self._project)
                task.run()
            LOGGER.info(f"{len(self._batches)} batches completed")

        elif self._run_mode == 'lotus':
            self._run_tasks_lotus(self._batches)

        else:
            raise ValueError(f"unsupported run mode {self._run_mode}")
            
            
    def _run_tasks_lotus(self, batches):

        #batch_spec = ",".join(batches)
        batch_spec = self._get_short_batch_spec(batches)

        LOGGER.debug(f"Raw batch list: " f"{batches}")
        LOGGER.info(f"Submitting conversion to Lotus: " f"{batch_spec}")
        cmd = (
            f"./cmip6_object_store/cmip6_zarr/cli.py "
            f"run --slurm-array-member -r local "
            f"-p {self._project}"
        )

        duration = CONFIG["workflow"]["max_duration"]
        memory = CONFIG["workflow"]["memory"]
        job_limit = CONFIG["workflow"]["job_limit"]
        lotus_log_dir = os.path.join(
            CONFIG["log"]["log_base_dir"], self._project, "lotus"
        )
        create_dir(lotus_log_dir)

        stdout = f"{lotus_log_dir}/%A_%a.out"
        stderr = f"{lotus_log_dir}/%A_%a.err"

        partition = CONFIG["workflow"]["job_queue"]
        array = f"{batch_spec}%{job_limit}"

        lotus = Lotus()
        lotus.run(
            cmd,
            stdout=stdout,
            stderr=stderr,
            partition=partition,
            duration=duration,
            memory=memory,
            array=array
        )


    def _get_short_batch_spec(self, batches):
        """
        turn a list of batch numbers into a comma separated string such as 4-6,8,10
        """        
        limits = ([batches[0]] +
                  [i for pair in zip(batches[:-1], batches[1:]) for i in pair if pair[1] != pair[0] + 1] +
                  [batches[-1]])
        it = iter(limits)

        rangespecs = []
        for lower in it:
            upper = next(it)
            rangespecs.append(f"{lower}-{upper}" if upper != lower else f"{lower}")
        return ",".join(rangespecs)
