import os
import intake
import xarray as xr
import fsspec
import numpy as np

from cmip6_object_store.cmip6_zarr.task import TaskManager
from cmip6_object_store.config import CONFIG


def _write_tmp_catalog(json_catalog, csv_catalog, tmp_json_catalog):
    """
    write a version of the JSON catalog in which the CSV path is replaced with 
    that of the local file
    """
    with open(json_catalog) as fin, open(tmp_json_catalog, "w") as fout:
        for line in fin:
            if '"catalog_file"' in line:
                fout.write(f'  "catalog_file": "{csv_catalog}",\n')
            else:
                fout.write(line)


def _get_collection(json_catalog, csv_catalog, tmp_json_catalog="/tmp/catalog.json"):
    _write_tmp_catalog(json_catalog, csv_catalog, tmp_json_catalog)
    return intake.open_esm_datastore(tmp_json_catalog)
    
def _get_example_dataset(collection):

    params = {'mip_era': 'CMIP6',
              'activity_id': 'DCPP',
              'institution_id': 'IPSL',
              'source_id': 'IPSL-CM6A-LR',
              'experiment_id': 'dcppC-ipv-NexTrop-neg',
              'member_id': 'r1i1p1f1',
              'table_id': 'Amon',
              'variable_id': 'rsdt',
              'grid_label': 'gr',
              'version': 'v20190110'}

    filtered = collection.search(**params)
    assert len(filtered.df) == 1    
    return filtered.df.iloc[0]


def _calc_toa_rad_annual_means(toa):
    coslat = np.cos(toa.lat * np.pi / 180)
    normalisation = float(((toa[0]*0+1) * coslat).sum())
    mean_toa_rad = (toa*coslat).sum(axis=2).sum(axis=1) / normalisation
    values = list(mean_toa_rad.to_masked_array())

    ntime = len(toa.time)
    assert ntime % 12 == 0
    
    return [float(mean_toa_rad[start : start+12].mean())
            for start in range(0, ntime, 12)]
    

def test_Intake():
    """
    Do a search, open dataset in xarray, check some values
    """
    project = "cmip6"
    iconf = CONFIG["intake"]
    json_catalog = iconf["json_catalog"].format(project=project)
    csv_catalog = iconf["csv_catalog"].format(project=project)
    
    col = _get_collection(json_catalog, csv_catalog)

    #assert len(col.df) > 100000

    dataset = _get_example_dataset(col)
    zarr_path = dataset['zarr_path']    
    fsmap = fsspec.get_mapper(zarr_path)
    data = xr.open_zarr(fsmap, consolidated=True, use_cftime=True)

    variable_id = dataset["variable_id"]
    assert variable_id == "rsdt"
    var = data[variable_id]
    assert var.shape == (120, 143, 144)
    assert var.shape == (len(var.time), len(var.lat), len(var.lon))
    assert -65 < float(var.lat[20]) < -64
    assert float(var.lon[20]) == 50
    
    toa_rad_annual_means = _calc_toa_rad_annual_means(var)
        
    assert len(toa_rad_annual_means) == 10
    for value in toa_rad_annual_means:
        assert 341 < value < 342
