#!/usr/bin/env python

from urllib.parse import urlparse

import s3fs
import xarray as xr

zarr_url = (
    "http://ceda-archive-o.s3.jc.rl.ac.uk/CMIP6.CMIP.BCC.BCC-CSM2-MR/"
    "1pctCO2.r1i1p1f1.Amon.ps.gn.v20181015.zarr"
)
url_comps = urlparse(zarr_url)

endpoint = f"{url_comps.scheme}://{url_comps.netloc}"
zarr_path = url_comps.path

jasmin_s3 = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint})

s3_store = s3fs.S3Map(root=zarr_path, s3=jasmin_s3)
ds = xr.open_zarr(store=s3_store, consolidated=True)
print(ds)
