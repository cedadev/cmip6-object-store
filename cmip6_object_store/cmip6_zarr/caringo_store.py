import os

import s3fs
from retry import retry

from ..config import CONFIG

retry_count = CONFIG["workflow"]["retries"]


class CaringoStore(object):
    def __init__(self, creds):
        self._creds = creds
        endpoint_url = CONFIG["store"]["endpoint_url"]

        self._fs = s3fs.S3FileSystem(
            anon=False,
            secret=self._creds["secret"],
            key=self._creds["token"],
            client_kwargs={"endpoint_url": endpoint_url},
            config_kwargs={"max_pool_connections": 50},
        )

    @retry(tries=retry_count, delay=3)
    def create_bucket(self, bucket_id):
        if not self._fs.exists(bucket_id):
            self._fs.mkdir(bucket_id)

    @retry(tries=retry_count, delay=3)
    def exists(self, bucket_id):
        return self._fs.exists(bucket_id)

    @retry(tries=retry_count, delay=3)
    def delete(self, bucket_id):
        try:
            self._fs.delete(bucket_id, recursive=True)
        except Exception:
            raise Exception(f"Cannot delete bucket: {bucket_id}")

    def list(self):
        return self._fs.ls(".")

    @retry(tries=retry_count, delay=3)
    def get_store_map(self, data_path):
        return s3fs.S3Map(root=data_path, s3=self._fs)

    @retry(tries=retry_count, delay=3)
    def set_permissions(self, data_path, permission="public-read"):
        for dr, _, files in self._fs.walk(data_path):
            for item in files:
                _path = os.path.join(dr, item)

                if self._fs.isfile(_path):
                    self._fs.chmod(_path, permission)
