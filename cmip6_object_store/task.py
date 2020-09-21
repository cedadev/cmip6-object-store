import os

import pandas as pd

from .config import CONFIG
from .batch import BatchManager
from .lotus import Lotus
from cmip6_object_store import logging

LOGGER = logging.getLogger(__file__)


class ConversionTask(object):

    def __init__(self, batch_number, run_mode='lotus'):
        self._batch_number = batch_number
        if run_mode == 'local':
            self.run = self._run_local
        else:
            self.run = self._run_lotus

    def _run_local(self):
        LOGGER.info(f'Running conversion locally: { self._batch_number}')

    def _run_lotus(self):
        LOGGER.info(f'Submitting conversion to Lotus: { self._batch_number}')



class TaskManager(object):

    def __init__(self, project, batches=None, run_mode='lotus', ignore_complete=True):
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

    def OLD_load_datasets(self):

        datasets_file = CONFIG['datasets']['datasets_file']

        df = pd.read_csv(datasets_file, skipinitialspace=True)

        self._total_size = df['size_mb'].sum()
        self._file_count = df['num_files'].sum()
        self._datasets = list(df['dataset_id'])
        
        if not self._ignore_complete:
            self._filter_datasets()

    def _filter_datasets(self):
        log_file = os.path.join(CONFIG['log']['log_base_dir'], f'{self._project}.log')

        if os.path.isfile(log_file):
            with open(log_file) as reader:
                successes = reader.read().strip().split()
        else:
            successes = []

        self._datasets = sorted(list(set(self._datasets) - set(successes)))

    def get_batch(self):
        batch_size = CONFIG['workflow']['batch_size']

        batch = self._datasets[:batch_size]
        self._datasets = self._datasets[batch_size:]

        return batch

    def run_tasks(self):
        for batch in self._batches:
            task = ConversionTask(batch, run_mode=self._run_mode)
            task.run()





