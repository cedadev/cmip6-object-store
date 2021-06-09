import os
import pickle
import time

from .. import logging
from .file_lock import FileLock

LOGGER = logging.getLogger(__file__)


class PickleStore:
    def __init__(self, content_file, lock_timeout=20):
        self._content_file = content_file
        self._lock = FileLock(f"{content_file}.lock")
        self._lock_timeout = lock_timeout

    def _read(self):
        self._lock.acquire(max_time=self._lock_timeout)

        if os.path.isfile(self._content_file):
            content = pickle.load(open(self._content_file, "rb")) or {}
        else:
            content = {}

        self._lock.release()
        return content

    def read(self):
        return self._read()

    def _write(self, content):
        self._lock.acquire(max_time=self._lock_timeout)

        with open(self._content_file, "wb") as f:
            pickle.dump(content, f)

        self._lock.release()

    def add(self, key, value):
        content = self._read()
        content[key] = value

        time.sleep(2)
        self._write(content)

    def clear(self, key):
        content = self._read()

        if key in content:
            LOGGER.info(f"Clearing from pickle store: {key}")
            del content[key]

        time.sleep(2)
        self._write(content)

    def contains(self, key):
        content = self._read()
        return key in content
