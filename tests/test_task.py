from cmip6_object_store.task import *


def test_TaskManager():
    tm = TaskManager('cmip6')
    batch = tm.get_batch()

    dsid = 'CMIP6.DCPP.IPSL.IPSL-CM6A-LR.dcppC-ipv-NexTrop-pos.r1i1p1f1.Amon.tauu.gr.v20190110'
    assert(batch[0] == dsid)

    assert(tm._total_size < 40000000 and tm._total_size > 30000000)
    assert(tm._file_count > 1000000)