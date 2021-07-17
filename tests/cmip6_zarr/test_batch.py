from cmip6_object_store.cmip6_zarr.batch import BatchManager


def test_BatchManager():
    bm = BatchManager("cmip6")

    batch = bm.get_batch(1)

    assert batch[0].startswith("CMIP6.")
