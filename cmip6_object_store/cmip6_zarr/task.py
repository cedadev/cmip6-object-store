import os
import math
import pandas as pd

from memory_profiler import profile
import xarray as xr

from ..config import CONFIG
from .batch import BatchManager
from .lotus import Lotus
from .caringo_store import CaringoStore
from .utils import get_credentials, get_uuid

from cmip6_object_store import logging
LOGGER = logging.getLogger(__file__)


class ConversionTask(object):

    def __init__(self, batch_number, project, run_mode='lotus'):
        self._batch_number = batch_number
        self._project = project

        if run_mode == 'local':
            self.run = self._run_local
        else:
            self.run = self._run_lotus

    def _get_bucket_name(self):
        return f'cmip6-test-{self._batch_number}'

    def _id_to_directory(self, dataset_id):
        archive_dir = CONFIG[f'project:{self._project}']['archive_dir']
        return os.path.join(archive_dir, dataset_id.replace('.', '/'))

    @profile(precision=1)
    def _run_local(self):
        LOGGER.info(f'Running conversion locally: {self._batch_number}')

        batch_manager = BatchManager(self._project)
        dataset_ids = batch_manager.get_batch(self._batch_number)

        var_index = CONFIG[f'project:{self._project}']['var_index']
        chunk_size_bytes = CONFIG['workflow']['chunk_size'] * (2**20)
        bucket = self._get_bucket_name()

        dataset_ids = ['CMIP6.HighResMIP.CMCC.CMCC-CM2-VHR4.control-1950.r1i1p1f1.Amon.va.gn.v20200917']

        for dataset_id in dataset_ids:

            LOGGER.info(f'Processing: {dataset_id}')
            var_id = dataset_id.split('.')[var_index]

            #TODO: Move this to "ZarrWriter class"
            store = CaringoStore(get_credentials())
            store.create_bucket(bucket)

            zpath = f'{bucket}/{dataset_id}.zarr'
            zpath = f'{bucket}/{get_uuid()}.zarr'
            store_map = store.get_store_map(zpath)

            dr = self._id_to_directory(dataset_id)

            LOGGER.info(f'Reading data from: {dr}')

            DO_WE_NEED_engine='netcdf4'
            ds = xr.open_mfdataset(f'{dr}/*.nc', use_cftime=True, combine='by_coords')

            LOGGER.info(f'Writing to: {zpath}')

            if hasattr(ds, 'time'):

                # Chunk by time
                LOGGER.info(f'Shape of variable "{var_id}": {ds[var_id].shape}')
                n_bytes = ds[var_id].nbytes

# N bytes: 81,494,802,432 (80GB)
# Chunk size in bytes: 104,857,600 (100MB)
# Required number of chunks

                LOGGER.info(f'Number of bytes in array: {n_bytes}')
                chunk_length = math.ceil(len(ds.time) / math.ceil(n_bytes / chunk_size_bytes))
                chunk_rule = {'time': chunk_length}

                LOGGER.info(f'Chunking into chunks of {chunk_length} time steps')
                chunked_ds = ds.chunk(chunk_rule)
                chunked_ds[var_id].unify_chunks()

                LOGGER.info(f'Chunks: {chunked_ds.chunks}')
                chunked_ds.to_zarr(store=store_map, mode='w', consolidated=True)
            else:
                ds.to_zarr(store=store_map, mode='w', consolidated=True)

            ds.close()

            LOGGER.info('Setting read permissions')
            store.set_permissions(zpath)

            LOGGER.info(f'Completed write for: {zpath}')

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
            task = ConversionTask(batch, project=self._project, run_mode=self._run_mode)
            task.run()





