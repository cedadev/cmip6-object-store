import os
import math

import pandas as pd
import dask
import xarray as xr

from ..config import CONFIG
from .catalogue import MappingCatalogue
from .caringo_store import CaringoStore
from .utils import get_credentials, get_uuid

from .. import logging
LOGGER = logging.getLogger(__file__)

class OLDZarrWriter(object):
    def _log_result(self, status, msg):
        pass
    def _log_error(self, msg):
        return self._log_result('error', msg)
    def _log_success(self, msg):
        return self._log_result('success', msg)


class ZarrWriter(object):

    def __init__(self, batch, project):
        self._batch = batch
        self._project = project
        self._map_cat = MappingCatalogue(CONFIG[f'project:{project}']['mapping_catalogue'])

    def _id_to_directory(self, dataset_id):
        archive_dir = CONFIG[f'project:{self._project}']['archive_dir']
        return os.path.join(archive_dir, dataset_id.replace('.', '/'))

    def convert(self, dataset_id):
        LOGGER.info(f'Processing: {dataset_id}')

        store = CaringoStore(get_credentials())
        bucket = f'cmip6-test-{self._batch}'
        store.create_bucket(bucket)

        uid = get_uuid()
        zpath = f'{bucket}/{dataset_id}.zarr'
 #       zpath = f'{bucket}/{uid}.zarr'
        store_map = store.get_store_map(zpath)

        dr = self._id_to_directory(dataset_id)
        LOGGER.info(f'Reading data from: {dr}')

        file_pattern = f'{dr}/*.nc'
        ds = xr.open_mfdataset(file_pattern, use_cftime=True, combine='by_coords')
        LOGGER.info(f'Writing to: {zpath}')

        if hasattr(ds, 'time'):
            self._write_zarr_in_chunks(dataset_id, ds, store_map)
        else:
            self._write_zarr_in_one(dataset_id, ds, store_map)

        ds.close()

        LOGGER.info('Setting read permissions')
        store.set_permissions(zpath)
        LOGGER.info(f'Completed write for: {zpath}')
        self._map_cat.add(uid, dataset_id)

    def _write_zarr_in_chunks(self, dataset_id, ds, store_map):
        var_index = CONFIG[f'project:{self._project}']['var_index']
        chunk_size_bytes = CONFIG['workflow']['chunk_size'] * (2**20)

        LOGGER.info(f'Processing: {dataset_id}')
        var_id = dataset_id.split('.')[var_index]   
        # Chunk by time
        LOGGER.info(f'Shape of variable "{var_id}": {ds[var_id].shape}')
        n_bytes = ds[var_id].nbytes

        LOGGER.info(f'Number of bytes in array: {n_bytes}')
        chunk_length = math.ceil(len(ds.time) / math.ceil(n_bytes / chunk_size_bytes))
        chunk_rule = {'time': chunk_length}

        LOGGER.info(f'Chunking into chunks of {chunk_length} time steps')
        chunked_ds = ds.chunk(chunk_rule)
        chunked_ds[var_id].unify_chunks()

        LOGGER.info(f'Chunks: {chunked_ds.chunks}')

        with dask.config.set(scheduler="synchronous"):
            delayed_obj = chunked_ds.to_zarr(store=store_map, mode='w', consolidated=True, compute=False)
            delayed_obj.compute()

        #chunked_ds.to_zarr(store=store_map, mode='w', consolidated=True)

    def _write_zarr_in_one(self, dataset_id, ds, store_map):
        with dask.config.set(scheduler="synchronous"):
            delayed_obj = ds.to_zarr(store=store_map, mode='w', consolidated=True, compute=False)
            delayed_obj.compute()
            
        #ds.to_zarr(store=store_map, mode='w', consolidated=True)
