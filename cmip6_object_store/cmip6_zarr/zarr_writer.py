import math
import os
import traceback

import dask
import xarray as xr

from .. import logging
from ..config import CONFIG, get_from_proj_or_workflow
from .caringo_store import CaringoStore
from .utils import get_credentials, get_var_id, get_zarr_path
from .results_store import get_results_store

LOGGER = logging.getLogger(__file__)


class ZarrWriter(object):
    def __init__(self, batch, project):
        self._batch = batch
        self._project = project

        self._config = CONFIG[f"project:{project}"]
        self._results_store = get_results_store(self._project)
        
    def _id_to_directory(self, dataset_id):
        archive_dir = self._config["archive_dir"]
        return os.path.join(archive_dir, dataset_id.replace(".", "/"))

    def convert(self, dataset_id):

        if self._results_store.ran_successfully(dataset_id):
            LOGGER.info(f"Already converted to Zarr: {dataset_id}")
            return

        # Clear out error state if previously recorded
        self._results_store.delete_result(dataset_id)

        LOGGER.info(f"Converting to Zarr: {dataset_id}")

        try:
            store = CaringoStore(get_credentials())
            bucket, zarr_file = get_zarr_path(dataset_id, self._project)
            zpath = f"{bucket}/{zarr_file}"
            LOGGER.info(f"Zarr path: {zpath}")

            store.create_bucket(bucket)
            store_map = store.get_store_map(zpath)
        except Exception:
            msg = f"Failed to create bucket for: {dataset_id}"
            return self._wrap_exception(dataset_id, msg)

        # Load the data and ready it for processing
        try:
            ds = self._get_ds(dataset_id)
        except Exception:
            msg = f"Failed to get Xarray dataset: {dataset_id}"
            return self._wrap_exception(dataset_id, msg)

        # Write to zarr
        try:
            if hasattr(ds, "time"):
                ds_to_write = self._get_chunked_ds(dataset_id, ds, store_map)
            else:
                ds_to_write = ds

            LOGGER.info(f"Writing to: {zpath}")
            self._write_zarr(ds_to_write, store_map)
        except Exception:
            msg = f"Failed to write to Zarr: {dataset_id}"
            return self._wrap_exception(dataset_id, msg)

        try:
            ds.close()
            do_perms = get_from_proj_or_workflow("set_permissions", self._project)
            if do_perms:
                LOGGER.info("Setting read permissions")
                store.set_permissions(zpath)
            else:
                LOGGER.info("Skipping setting permissions")

            LOGGER.info(f"Completed write for: {zpath}")
            self._finalise(dataset_id, zpath)
        except Exception:
            msg = f"Finalisation failed for: {dataset_id}"
            return self._wrap_exception(dataset_id, msg)

    def _get_ds(self, dataset_id):

        dr = self._id_to_directory(dataset_id)
        LOGGER.info(f"Reading data from: {dr}")

        file_pattern = f"{dr}/*.nc"
        ds = xr.open_mfdataset(file_pattern, use_cftime=True, combine="by_coords")
        return ds

    def _get_chunked_ds(self, dataset_id, ds, store_map):
        LOGGER.info(f"Processing: {dataset_id}")
        var_id = get_var_id(dataset_id, project=self._project)

        # Chunk by time
        chunk_size_bytes = get_from_proj_or_workflow("chunk_size", self._project) * (2 ** 20)
        LOGGER.info(f'Shape of variable "{var_id}": {ds[var_id].shape}')
        n_bytes = ds[var_id].nbytes

        LOGGER.info(f"Number of bytes in array: {n_bytes}")
        chunk_length = math.ceil(len(ds.time) / math.ceil(n_bytes / chunk_size_bytes))
        chunk_rule = {"time": chunk_length}

        LOGGER.info(f"Chunking into chunks of {chunk_length} time steps")
        chunked_ds = ds.chunk(chunk_rule)
        chunked_ds[var_id].unify_chunks()

        LOGGER.info(f"Chunks: {chunked_ds.chunks}")
        return chunked_ds

    def _write_zarr(self, ds, store_map):
        with dask.config.set(scheduler="synchronous"):
            delayed_obj = ds.to_zarr(
                store=store_map, mode="w", consolidated=True, compute=False
            )
            delayed_obj.compute()

    def _finalise(self, dataset_id, zpath):
        self._results_store.insert_success(dataset_id)
        LOGGER.info(f"Wrote result for: {dataset_id}")

    def _wrap_exception(self, dataset_id, msg):
        tb = traceback.format_exc()
        error = f"{msg}:\n{tb}"
        self._results_store.insert_failure(dataset_id, error)
        LOGGER.error(f"FAILED TO COMPLETE FOR: {dataset_id}\n{error}")
