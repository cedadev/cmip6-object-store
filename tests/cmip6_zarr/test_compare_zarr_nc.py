"""
Tests to compare that Zarr files (in Caringo) have the same content
as the NetCDF files they came from.
"""

import glob
import random
import warnings

import xarray as xr

from cmip6_object_store.cmip6_zarr.utils import (
    get_archive_path,
    get_catalogue,
    get_var_id,
    read_zarr,
)


def test_compare_zarrs_with_ncs():
    """
        Plan:


     read zarr catalogue (pickle)
     randomly shuffle the pack
     decide how many to test (N=5)
    For each dataset:

     map ID to archive dir
     select one NC file
     if bigger than threshold then continue (THRESHOLD=250MB)
     read dataset 1 (from NC file)
     read dataset 2 (from Zarr)
     compare datasets by value, variables, coordinates,
       attributes and variable attributes
     log result
     if processed N files, then exit
    """
    n_to_test = 5
    tested = []

    cat = get_catalogue("zarr", project="cmip6").read()
    dataset_ids = list(cat.keys())

    while len(tested) < n_to_test:
        dataset_id = random.choice(dataset_ids)
        if dataset_id in tested:
            continue

        result = _compare_dataset(dataset_id)
        if not result:
            continue

        tested.append(dataset_id)


def _get_nc_file(dataset_id):
    archive_dir = get_archive_path(dataset_id)

    nc_files = glob.glob(f"{archive_dir}/*.nc")
    if not nc_files:
        return None

    return nc_files[0]


def _compare_dataset(dataset_id):
    nc_file = _get_nc_file(dataset_id)
    if not nc_file:
        return False

    print(f"\n\n==========================\nWorking on: {dataset_id}")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        nc_subset = xr.open_dataset(nc_file)

    zarr_ds = read_zarr(dataset_id)
    zarr_subset = zarr_ds.sel(time=slice(nc_subset.time[0], nc_subset.time[-1]))

    result = nc_subset.identical(zarr_subset)

    print(f"Testing: {dataset_id}")
    print(f"\tResult: {result}")

    for prop in ("data_vars", "coords"):
        a, b = getattr(nc_subset, prop), getattr(zarr_subset, prop)
        print(f'\nComparing "{prop}": {a} \n------------\n {b}')

    a, b = nc_subset.time.values, zarr_subset.time.values
    assert list(a) == list(b)
    print("Times are identical")

    var_id = get_var_id(dataset_id, project="cmip6")
    a_var, b_var = nc_subset[var_id], zarr_subset[var_id]

    a_min, a_max = float(a_var.min()), float(a_var.max())
    b_min, b_max = float(b_var.min()), float(b_var.max())

    assert a_min == b_min
    print("Minima are identical")

    assert a_max == b_max
    print("Maxima are identical")

    for attr in ("units", "long_name"):
        print(f"{attr}: {getattr(a_var, attr)} VS {getattr(b_var, attr)}")

    # compare datasets by value, variables, coordinates, attributes and
    # variable attributes
    return result


test_compare_zarrs_with_ncs()
