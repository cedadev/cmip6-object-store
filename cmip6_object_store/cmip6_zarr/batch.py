import os, stat
import subprocess

import pandas as pd

from .. import logging
from ..config import CONFIG
from .utils import create_dir

LOGGER = logging.getLogger(__file__)


class BatchManager(object):

    def __init__(self, project):
        self._project = project
        data_dir = CONFIG['workflow']['data_dir']
        run_version = CONFIG['workflow']['run_version']

        self._version_dir = os.path.join(data_dir, run_version)
        create_dir(self._version_dir)

    def get_batch_files(self):
        batch_files = []

        for batch_file in sorted(os.listdir(self._version_dir)):

            batch_file_path = os.path.join(self._version_dir, batch_file)
            batch_files.append(batch_file_path)

        return batch_files

    def get_batches(self):
        for batch_file_path in self.get_batch_files():
            yield open(batch_file_path).read().strip().split()
    
    def get_batch(self, batch_number):
        batch_file_path = self.get_batch_files()[batch_number - 1]
        return open(batch_file_path).read().strip().split()

    def _write_batch(self, batch_number, batch):
        batch_file = os.path.join(self._version_dir, f'batch_{batch_number:04d}.txt')

        with open(batch_file, 'w') as writer:
            writer.write('\n'.join(batch))

        LOGGER.debug(f'Wrote batch file: {batch_file}')

    def create_batches(self):
        # Read in all datasets
        datasets_file = CONFIG['datasets']['datasets_file']
        df = pd.read_csv(datasets_file, skipinitialspace=True)

        total_volume = df['size_mb'].sum()
        max_volume = CONFIG['workflow']['max_volume']

        if total_volume > max_volume:
            raise Exception(f'Total volume exceeds limit for this project: {total_volume} > {max_volume} !')


        self._datasets = list(df['dataset_id'])
        batch_volume_limit = CONFIG['workflow']['batch_volume_limit']

        # Loop through grouping them into batches of approx batch_size (in CONFIG)
        # - write each batch to text file in versioned data directory
        current_size, current_batch, batch_count = 0, [], 1

        for _, row in df.iterrows():
            current_batch.append(row.dataset_id)
            current_size += row.size_mb

            if current_size >= batch_volume_limit:
                self._write_batch(batch_count, current_batch)
                # Reset variables
                current_size, current_batch = 0, []
                batch_count += 1

        # Write last one if not written
        if current_batch:
            self._write_batch(batch_count, current_batch)
            
        LOGGER.info(f'Wrote {batch_count} batch files.')