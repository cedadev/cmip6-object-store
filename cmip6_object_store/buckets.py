

class CaringoBucket(object):
    
    def __init__(self, creds, bucket_id):
        self._creds = creds
        self._bucket_id = bucket_id

        self._setup()

    def _setup(self):
        if not self._exists():
            self._create()

        if not self.is_writeable():
            self.set_write_permissions()

    def _create(self):
        caringo.create(self._creds, self._bucket_id)

    def exists(self):
        return caringo.exists(self._bucket_id):

    def is_writeable(self):
        return caringo.writeable(self._bucket_id)

    def set_write_permissions(self):
        caringo.set_permissions('allow', self._creds, self._bucket_id)


