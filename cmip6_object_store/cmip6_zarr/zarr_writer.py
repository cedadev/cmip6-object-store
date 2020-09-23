from ..config import CONFIG


class ZarrWriter(object):

    def __init__(self, creds, dataset_id):
        self._ds = dataset_id
        self._creds = creds

    def _configure(self):
        self._bucket_id, self._zarr_path =  split by '/'  CONFIG['datasets']['split_level']  replace '.'
        self._bucket = CaringoBucket(self._creds, self._bucket_id)

    def _get_zarr_path(self):
        return f'{self._bucket_id/'

    def _get_archive_path(self):
        pass

    def _check_is_latest(self):
        pass

    def _log_result(self, status, msg)
        pass

    def _log_error(self, msg):
        return self._log_result('error', msg)

    def _log_success(self, msg):
        return self._log_result('success', msg)
        
    def _open_dataset(self, fpath, preprocessors=None):
        ds = xr.open_mfdataset(file_paths, preprocess=preprocessors, use_cftime=True, combine="by_coords")

    def _get_fixes(self):
        return look up fixes somewhere for preprocessing files

    def _get_dataset(self, fpath):
        preprocessors = self._get_fixes()
        return self._open_dataset(fpath, preprocessors=preprocessors)

    def _write(self, ds):
        zarr_path = self._get_zarr_path()
        credentials stuff
        ds.to_zarr(...,  ...)

    def process(self):
        try:
            archive_path = self._get_archive_path()
            ds = self._open_dataset(archive_path)
            self._write(ds)
            self._log_success('SUCCESS')
            ds.close()
            del ds
        except Exception as exc:
            self._log_success(str(exc))

