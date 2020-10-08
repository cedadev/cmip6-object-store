import xarray as xr

from cmip6_object_store.cmip6_zarr.caringo_store import CaringoStore
from cmip6_object_store.cmip6_zarr.utils import get_credentials


def test_CaringoStore():

    store = CaringoStore(get_credentials())
    bucket = "a-bucket-test"

    store.create_bucket(bucket)

    zpath = f"{bucket}/test.zarr"
    store_map = store.get_store_map(zpath)

    dr = (
        "/home/users/astephen/cmip6-xarray-zarr/tests/mini-esgf-data/"
        "test_data/badc/cmip6/data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/"
        "historical/r1i1p1f1/SImon/siconc/gn/latest/"
    )
    ds = xr.open_mfdataset(f"{dr}/*.nc", engine="netcdf4", combine="by_coords")

    ds.to_zarr(store=store_map, mode="w", consolidated=True)
    ds.close()

    store.set_permissions(zpath)
