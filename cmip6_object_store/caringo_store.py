import os
import json

import s3fs

from utils import get_credentials


class CaringoStore(object):
    
    def __init__(self, creds):
        self._creds = creds
        self._fs = s3fs.S3FileSystem(anon=False, secret=self._creds['secret'], 
                                key=self._creds['token'],
                                client_kwargs={'endpoint_url': creds['endpoint_url']})

    def create_bucket(self, bucket_id):
        if not self._fs.exists(bucket_id):
            self._fs.mkdir(bucket_id)

    def exists(self, bucket_id):
        return self._fs.exists(bucket_id)

    def delete(self, bucket_id):
        try:
            self._fs.delete(bucket_id, recursive=True)
        except Exception as exc:
            raise Exception(f'Cannot delete bucket: {bucket_id}')

    def get_store_map(self, data_path):
        return s3fs.S3Map(root=data_path, s3=self._fs)

    def set_permissions(self, data_path, permission='public-read'):
        for dr, _, files in self._fs.walk(data_path):
            for item in files:
                _path = os.path.join(dr, item)

                if self._fs.isfile(_path):
                    self._fs.chmod(_path, permission)


def test_CaringoBucket():

    store = CaringoStore(get_credentials())
    bucket = 'a-bucket-test'

    store.create_bucket(bucket)

    zpath = f'{bucket}/test.zarr'
    store_map = store.get_store_map(zpath)

    import xarray as xr
    dr = "/home/users/astephen/cmip6-xarray-zarr/tests/mini-esgf-data/test_data/badc/cmip6/data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r1i1p1f1/SImon/siconc/gn/latest/"
    ds = xr.open_mfdataset(f'{dr}/*.nc', engine='netcdf4', combine='by_coords')

    ds.to_zarr(store=store_map, mode='w', consolidated=True)
    ds.close()

    store.set_permissions(zpath)


test_CaringoBucket()

