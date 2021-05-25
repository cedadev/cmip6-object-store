import os
import s3fs
import xarray as xr
import json
import time
import sys
from memory_profiler import profile

os.environ.update({
    'OMP_NUM_THREADS': '1',
    'MKL_NUM_THREADS': '1',
    'OPENBLAS_NUM_THREADS': '1',
    'VECLIB_MAXIMUM_THREADS': '1',
    'NUMEXPR_NUM_THREADS': '1'})

opts = {
    'bucket': '00alantest',
    'endpoint_url': 'http://cmip6-zarr-o.s3.jc.rl.ac.uk/',
    'creds_file': f'{os.environ["HOME"]}/.credentials/caringo-credentials.json',

    # list of 2-tuples: (dataset-id, chunk length)  None means don't chunk
    'datasets': [('CMIP6.CMIP.MIROC.MIROC6.amip.r6i1p1f1.Amon.tasmin.gn.v20181214', None),
                 ('CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp126.r3i1p1f1.day.zg.gr.v20190410', 2)]
}

def get_input_files(ds_id):
    dirname = os.path.join('/badc/cmip6/data',
                           ds_id.replace('.', '/'))
    return [os.path.join(dirname, file)
            for file in os.listdir(dirname)
            if file.endswith('.nc')]

def make_bucket(fs, bucket, max_tries=3, sleep_time=3):
    tries = 0
    while not fs.exists(bucket):
        try:
            print("try to make bucket")
            fs.mkdir(bucket)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            tries += 1
            print(f"making bucket failed ({tries} tries): {exc}")
            if tries == max_tries:
                print("giving up")
                raise
            time.sleep(sleep_time)
    print(f"bucket {bucket} now exists")

    
def get_store_map(zpath, fs):
    return s3fs.S3Map(root=zpath, s3=fs)


def show_first_data_value(ds):
    "show first data value of any 3d fields"
    for name, var in ds.items():
        if len(var.shape) == 3:
            print(f"{name} {float(var[0,0,0].values)}")


def remove_path(zpath, fs):
    if fs.exists(zpath):
        print(f"removing existing {zpath}")
        fs.rm(zpath, True)
        tries = 0
        while fs.exists(zpath):
            tries += 1
            if tries == 5:
                raise Exception(f"could not remove {zpath}")
            time.sleep(1)

            
# simplified version assuming CMIP6
def get_var_id(ds_id):
    return ds_id.split('.')[-3]


# simplified version that takes chunk_length as input
def get_chunked_ds(ds_id, ds, chunk_length):
    print(f'chunking with length={chunk_length}')
    var_id = get_var_id(ds_id)
    chunked_ds = ds.chunk({"time": chunk_length})
    chunked_ds[var_id].unify_chunks()
    return chunked_ds

            
@profile(precision=1)
def main(opts):
    creds = json.load(open(opts['creds_file']))
    endpoint_url = opts["endpoint_url"]

    fs = s3fs.S3FileSystem(anon=False,
                           secret=creds['secret'],
                           key=creds['token'],
                           client_kwargs={'endpoint_url': endpoint_url},
                           config_kwargs={'max_pool_connections': 50})

    make_bucket(fs, opts["bucket"])

    for ds_id, chunk_length in opts["datasets"]:

        print(f"DATASET: {ds_id}")
        zarr_file = ds_id
        zpath = f'{opts["bucket"]}/{zarr_file}'
        zarr_url = f'{endpoint_url}{zpath}'

        store_map = get_store_map(zpath, fs)
        input_files = get_input_files(ds_id)

        print("opening xarray dataset")
        ds = xr.open_mfdataset(input_files, use_cftime=True, combine='by_coords')
        show_first_data_value(ds)

        remove_path(zpath, fs)

        ds_to_write = ds if chunk_length == None else get_chunked_ds(ds_id, ds, chunk_length)
        
        print("writing data")
        ds_to_write.to_zarr(store=store_map, mode='w', consolidated=True)

        if fs.exists(zpath):
            print(f"wrote: {zarr_url}")
        else:
            raise Exception(f"could not write {zarr_url}")
        print()
        
main(opts)

