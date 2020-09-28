import pickle
import os

from .utils import FileLock


class _CatalogueBase(object):

    def __init__(self, catalogue_file):
        self._cat_file = catalogue_file
        self._lock = FileLock(f'{catalogue_file}.lock')


class PickleCatalogue(_CatalogueBase):

    def _read(self):
        self._lock.acquire()

        if os.path.isfile(self._cat_file):
            content = pickle.load(open(self._cat_file, 'rb')) or {}
        else:
            content = {}

        self._lock.release()
        return content
        
    def _write(self, content):
        self._lock.acquire()

        with open(self._cat_file, 'wb') as f:
            pickle.dump(content, f)

        self._lock.release()

    def add(self, key, value):
        self._lock.acquire()

        content = self._read()           
        content[key] = value

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
            
        content.append(f'{value}\n')

        with open(self._cat_file, 'w') as writer:
            writer.writelines(content)

        self._lock.release()
