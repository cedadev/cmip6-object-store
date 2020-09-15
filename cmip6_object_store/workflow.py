
from .config import CONFIG


class TaskManager(object):

    def __init__(self, project, ignore_complete=True):
        self._project = project
        self._ignore_complete = ignore_complete
        self._load_datasets()

    def _load_datasets(self):
        with open(CONFIG['datasets_file']) as reader:
            self._datasets = reader.read().strip().split()

        if not _ignore_complete():
            self._filter_datasets()

    def _filter_datasets(self):
        with open(CONFIG['log_file']) as reader:
            successes = reader.read().strip().split()

        self._datasets = sorted(list(set(self._datasets) - set(successes)))

    def get_batch(self):
        batch_size = CONFIG['batch_size']

        batch = self._datasets[:batch_size]
        self._datasets = self._datasets[batch_size:]

        return batch

