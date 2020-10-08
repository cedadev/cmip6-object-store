import os
import time

from cmip6_object_store.cmip6_zarr.utils import FileLock

LOCK_FILE = "./test.lock"
DATA_FILE = "lock-test.dat"


def _clear_files():
    for f in LOCK_FILE, DATA_FILE:
        if os.path.isfile(f):
            os.remove(f)


def setup_module():
    _clear_files()


def teardown_module():
    _clear_files()


def test_filelock_simple():
    lock = FileLock(LOCK_FILE)

    lock.acquire()
    try:
        time.sleep(1)
        assert os.path.isfile(LOCK_FILE)
        assert lock.state == "LOCKED"
        open(DATA_FILE, "a").write("1")
    finally:
        lock.release()

    time.sleep(1)
    assert not os.path.isfile(LOCK_FILE)


def test_filelock_already_locked():
    lock1 = FileLock(LOCK_FILE)
    lock2 = FileLock(LOCK_FILE)

    lock1.acquire()

    try:
        lock2.acquire()
    except Exception as exc:
        assert str(exc) == f"Could not obtain file lock on {LOCK_FILE}"
