from jumpscale import j
from io import BytesIO
import os
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou
from urllib.parse import urlparse

logger = j.logger.get()


class Perf:
    def __init__(self, parent):
        self._parent = parent
        self._client = None

    @property
    def client(self):
        if self._client is None:
            s3 = self._parent.s3.service
            if not s3:
                return
            url = s3.schedule_action('url').wait(die=True).result
            u = urlparse(url)

            self._client = Minio(u.netloc,
                                 access_key=s3.data['data']['minioLogin'],
                                 secret_key=s3.data['data']['minioPassword'],
                                 secure=False)
        return self._client

    def simple_write_read(self):
        try:
            try:
                logger.info("create bucket")
                self.client.make_bucket('simple-write-read')
            except BucketAlreadyExists:
                pass
            except BucketAlreadyOwnedByYou:
                pass

            buf = BytesIO()
            input = os.urandom(1024*1024*2)
            buf.write(input)
            buf.seek(0)

            logger.info("upload 2MiB file")
            self.client.put_object('test', 'blob', buf, len(input))
            logger.info("download same file")
            obj = self.client.get_object('test', 'blob')
            logger.info("compare upload and download")
            assert input == obj.read()
            logger.info("comparison valid")
        finally:
            self.client.remove_bucket('simple-write-read')
