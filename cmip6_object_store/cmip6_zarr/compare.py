"""
Code to compare that Zarr files (in Caringo) have the same content
as the NetCDF files they came from.
"""

import glob
import random
import traceback
import warnings

import xarray as xr

from cmip6_object_store.cmip6_zarr.results_store import get_results_store, get_verification_store

from cmip6_object_store.cmip6_zarr.utils import (
    get_archive_path,
    get_var_id,
    read_zarr,
)


def compare_zarrs_with_ncs(project, n_to_test=5, dataset_id=None):
    """
    Randomly selects some datasets and checks that the contents
    of a NetCDF files in the archive matches that in a Zarr file
    in the Caringo object store.

    This logs its outputs for use elsewhere.

    If a dataset ID is passed in, this will check the specified single
    dataset ID instead of a random sample (and n_to_test is ignored)
    """
    tested = []

    successes, failures = 0, 0

    verification_store = get_verification_store(project)

    if dataset_id == None:
        print(f"\nVerifying up to {n_to_test} datasets for: {project}...")
        results_store = get_results_store(project)
        dataset_ids = results_store.get_successful_runs()
        force = False
    else:
        print(f"\nComparing single dataset {dataset_id} for: {project}...")
        n_to_test = 1
        dataset_ids = [dataset_id]
        force = True
    
    while len(tested) < n_to_test:
        dataset_id = random.choice(dataset_ids)
        if not force:
            if dataset_id in tested or verification_store.ran_successfully(dataset_id):
                continue

        print(f"==========================\nVerifying: {dataset_id}")
        try:
            _compare_dataset(dataset_id, project)
            verification_store.insert_success(dataset_id)
            successes += 1
            print(f"Comparison succeeded for: {dataset_id}")
        except Exception as exc:
            verification_store.insert_failure(dataset_id, f'failed: {exc}')
            failures += 1
            tb = traceback.format_exc()
            print(f"FAILED comparison for {dataset_id}: traceback was\n\n: {tb}")

        tested.append(dataset_id)

    total = successes + failures
    return (successes, total)


def _get_nc_file(dataset_id, project):
    archive_dir = get_archive_path(dataset_id, project)

    nc_files = glob.glob(f"{archive_dir}/*.nc")
    if not nc_files:
        return None

    return nc_files[0]


def _compare_dataset(dataset_id, project):
    nc_file = _get_nc_file(dataset_id, project)
    if not nc_file:
        return False

    print(f"\nWorking on: {dataset_id}")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        nc_subset = xr.open_dataset(nc_file)

    zarr_ds = read_zarr(dataset_id, project)
    zarr_subset = zarr_ds.sel(time=slice(nc_subset.time[0], nc_subset.time[-1]))

    result = nc_subset.identical(zarr_subset)

    print(f"Testing: {dataset_id}")
    print(f"\tResult: {result}")

    for prop in ("data_vars", "coords"):
        a, b = [
            sorted(list(_.keys()))
            for _ in (getattr(nc_subset, prop), getattr(zarr_subset, prop))
        ]
        print(f'\nComparing "{prop}": {a} \n------------\n {b}')
        assert a == b

    a, b = nc_subset.time.values, zarr_subset.time.values
    assert list(a) == list(b)
    print("Times are identical")

    var_id = get_var_id(dataset_id, project=project)
    a_var, b_var = nc_subset[var_id], zarr_subset[var_id]

    a_min, a_max = float(a_var.min()), float(a_var.max())
    b_min, b_max = float(b_var.min()), float(b_var.max())

    assert a_min == b_min
    print("Minima are identical")

    assert a_max == b_max
    print("Maxima are identical")

    for attr in ("units", "long_name"):
        a, b = getattr(a_var, attr), getattr(b_var, attr)
        print(f"{attr}: {a} VS {b}")
        assert a == b

    return result
