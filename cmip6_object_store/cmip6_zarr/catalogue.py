import os
import pickle
import time

from .. import logging
from .utils import FileLock

LOGGER = logging.getLogger(__file__)


class _CatalogueBase(object):
    def __init__(self, catalogue_file):
        self._cat_file = catalogue_file
        self._lock = FileLock(f"{catalogue_file}.lock")


class PickleCatalogue(_CatalogueBase):
    def _read(self):
        self._lock.acquire()

        if os.path.isfile(self._cat_file):
            content = pickle.load(open(self._cat_file, "rb")) or {}
        else:
            content = {}

        self._lock.release()
        return content

    def read(self):
        return self._read()

    def _write(self, content):
        self._lock.acquire()

        with open(self._cat_file, "wb") as f:
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
            LOGGER.info(f"Clearing from catalogue: {key}")
            del content[key]

        time.sleep(2)
        self._write(content)

    def contains(self, key):
        content = self._read()
        return key in content


class TextCatalogue(_CatalogueBase):
    def add(self, value):
        self._lock.acquire()

        if os.path.isfile(self._cat_file):
            content = open(self._cat_file).readlines()
        else:
            content = []

        content.append(f"{value}\n")

        with open(self._cat_file, "w") as writer:
            writer.writelines(content)

        self._lock.release()
