import time
from random import randint

from memory_profiler import profile
from netCDF4 import Dataset
from numpy import ma as ma

fpath = (
    "http://ceda-archive-o.s3.jc.rl.ac.uk/ag-http-range-get-test/"
    "mrros_rcp85_land-gcm_global_60km_11_day_20691201-20791130.nc#mode=bytes"
)


def timer(func):
    def timeit(*args, **kwargs):
        start = time.time()
        print(f"[INFO] Running: {func.__name__}")
        result = func(*args, **kwargs)
        duration = time.time() - start
        print(f"Duration: {duration:.1f} seconds\n\n")
        return result

    return timeit


def _open(fpath=fpath):
    return Dataset(fpath).variables["mrros"]


@timer
@profile
def test_1_read_metadata():
    v = _open()
    print(v.units)


def get_min_max(data):
    fixed_data = ma.masked_equal(data, 9.96921e36)
    return fixed_data.min(), fixed_data.max()


@timer
@profile
def test_2_small_slice():
    v = _open()
    print(f"[INFO] Shape pre-subset: {v.shape}")

    start = randint(0, 99)
    end = start + 20

    data = v[0, start:end, start:end, start:end]
    print(f"[INFO] Size: {data.size}")
    print(f"[INFO] Shape post-subset: {data.shape}")

    mn, mx = get_min_max(data)
    print(f"[INFO] min and max: {mn} --> {mx}")


@timer
@profile
def test_3_medium_slice():
    v = _open()
    print(f"[INFO] Shape pre-subset: {v.shape}")

    start = randint(0, 20)
    end = start + 200

    data = v[0, start:end, start:end, start:end]
    print(f"[INFO] Size: {data.size}")
    print(f"[INFO] Shape post-subset: {data.shape}")

    mn, mx = get_min_max(data)
    print(f"[INFO] min and max: {mn} --> {mx}")


@timer
@profile
def test_4_large_slice():
    v = _open()
    print(f"[INFO] Shape pre-subset: {v.shape}")

    start = randint(0, 20)
    end = start + 200

    data = v[0, 0:2000, start:end, start:end]
    print(f"[INFO] Size: {data.size}")
    print(f"[INFO] Shape post-subset: {data.shape}")

    mn, mx = get_min_max(data)
    print(f"[INFO] min and max: {mn} --> {mx}")


def main():
    for test in sorted([_ for _ in globals() if _.startswith("test_")]):
        func = globals()[test]
        func()


if __name__ == "__main__":

    main()
