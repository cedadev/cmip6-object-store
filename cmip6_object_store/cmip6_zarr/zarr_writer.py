import math
import os
import traceback

import dask
import xarray as xr

from .. import logging
from ..config import CONFIG
from .caringo_store import CaringoStore
from .utils import get_credentials, get_pickle_store, get_var_id

LOGGER = logging.getLogger(__file__)


class ZarrWriter(object):
    def __init__(self, batch, project):
        self._batch = batch
        self._project = project

        self._config = CONFIG[f"project:{project}"]
        self._zarr_pickle = get_pickle_store("zarr", self._project)
        self._error_pickle = get_pickle_store("error", self._project)

    def _get_from_proj_or_workflow(self, key):
        if key in self._config:
            return self._config[key]
        return CONFIG["workflow"][key]

    def _id_to_directory(self, dataset_id):
        archive_dir = self._config["archive_dir"]
        return os.path.join(archive_dir, dataset_id.replace(".", "/"))

    def _get_zarr_path(self, dataset_id):
        split_at = self._get_from_proj_or_workflow("split_level")

        parts = dataset_id.split(".")
        prefix = self._config.get("bucket_prefix", "")
        return (prefix + ".".join(parts[:split_at]), ".".join(parts[split_at:]) + ".zarr")

    def convert(self, dataset_id):
        # Clear out error state if previously recorded
        self._error_pickle.clear(dataset_id)

        if self._zarr_pickle.contains(dataset_id):
            LOGGER.info(f"Already converted to Zarr: {dataset_id}")
            return

        LOGGER.info(f"Converting to Zarr: {dataset_id}")

        try:
            store = CaringoStore(get_credentials())
            bucket, zarr_file = self._get_zarr_path(dataset_id)
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
            do_perms = self._get_from_proj_or_workflow("set_permissions")
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
        chunk_size_bytes = self._get_from_proj_or_workflow("chunk_size") * (2 ** 20)
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
        self._zarr_pickle.add(dataset_id, zpath)
        LOGGER.info(f"Wrote pickle entries for: {dataset_id}")

    def _wrap_exception(self, dataset_id, msg):
        tb = traceback.format_exc()
        error = f"{msg}:\n{tb}"
        self._error_pickle.add(dataset_id, error)
        LOGGER.error(f"FAILED TO COMPLETE FOR: {dataset_id}\n{error}")
