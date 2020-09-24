import os
from .utils import FileLock


class MappingCatalogue(object):

    def __init__(self, catalogue_file):
        self._cat_file = catalogue_file
        self._lock = FileLock(f'{catalogue_file}.lock')

    def add(self, uid, dataset_id):
        self._lock.acquire()

        if os.path.isfile(self._cat_file):
            content = open(self._cat_file).readlines()
        else:
            content = []
            
        content.append(f'{dataset_id}:{uid}\n')

        with open(self._cat_file, 'w') as writer:
            writer.writelines(content)

        self._lock.release()




