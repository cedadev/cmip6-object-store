#!/usr/bin/env python

import xarray as xr
import os
import s3fs
import json



creds_file = os.path.expanduser('~/.credentials/caringo-credentials.json')

with open(creds_file) as f:
    creds = json.load(f)

dr = "/home/users/astephen/cmip6-xarray-zarr/tests/mini-esgf-data/test_data/badc/cmip6/data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r1i1p1f1/SImon/siconc/gn/latest/"
print('Read in using xarray')

ds_id = '.'.join(dr.strip('/').split('/')[8:])
ds_id = '123'

if 1:
 ds = xr.open_mfdataset(f'{dr}/*.nc', engine='netcdf4', combine='by_coords')
 a1 = ds.siconc.values[120][3][1]
 print(f'Off disk: value = {a1}')

bucket = 'cmip6-zarr-test'
bucket = 'ag-test-3'
zarr_file = f'{ds_id}.zarr'


zarr_path = f'{bucket}/{zarr_file}'

jasmin_s3 = s3fs.S3FileSystem(anon=False, secret=creds['secret'], key=creds['token'],
                              client_kwargs={'endpoint_url': creds['endpoint_url']})

if 0:
  for i in range(1, 2):
    print(i)
    try:
        jasmin_s3.delete(f'ag-test-{i}', recursive=True)
    except Exception as exc:
        pass

import numpy

print(f'Making bucket: {bucket}')

if not jasmin_s3.exists(bucket):
    jasmin_s3.mkdir(bucket)


print('HERE: if I go into Caringo dashboard I can set as READ ONLY for all' \
    ' and then I can open it via wget or Firefox, and wget, !!!...')
print('http://ceda-archive-o.s3.jc.rl.ac.uk:81/ag-test-3/123.zarr/bounds_nav_lat/.zarray')

#jasmin_s3.chmod(bucket, 'public-read')
print('Creating S3Map object...')
s3_store = s3fs.S3Map(root=zarr_path, s3=jasmin_s3)

print(f'Writing: {zarr_path}')
ds.to_zarr(store=s3_store, mode='w', consolidated=True)
ds.close()
print(f'Wrote to: {zarr_path}')

print('Reading back to check content matches original netcdf')
ds2 = xr.open_zarr(store=s3_store, consolidated=True)
a2 = ds2.siconc.values[120][3][1]
print(f'Off Caringo: value = {a2}')

for dr, _2, files in jasmin_s3.walk(zarr_path):
    print(dr, _2, files)
    print(f'TEST: {files}')
    for item in files:

        _path = os.path.join(dr, item)
        print(_path, end=': ')
        if jasmin_s3.isfile(_path):
            jasmin_s3.chmod(_path, 'public-read')
            print('worked')

        #try:
        #    jasmin_s3.chmod(_path, 'public-read')
        #    print('worked')
        #except Exception as exc:
        #    print('failed (is it a directory)')

