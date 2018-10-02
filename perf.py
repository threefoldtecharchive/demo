from jumpscale import j
from io import BytesIO
import os
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou
from urllib.parse import urlparse

import subprocess

from contextlib import contextmanager

logger = j.logger.get()


class Perf:
    def __init__(self, parent):
        self._parent = parent
        self._client = None

    @contextmanager
    def _temp_bucket(self):
        bucket_name = j.data.idgenerator.generateXCharID(16)
        try:
            try:
                logger.info("create bucket")
                self.client.make_bucket(bucket_name)
            except BucketAlreadyExists:
                pass
            except BucketAlreadyOwnedByYou:
                pass

            yield bucket_name
        finally:
            self.client.remove_bucket(bucket_name)

    @property
    def client(self):
        if self._client is None:
            s3 = self._parent.service
            if not s3:
                return
            url = s3.schedule_action('url').wait(die=True).result
            u = urlparse(url['public'])

            self._client = Minio(u.netloc,
                                 access_key=s3.data['data']['minioLogin'],
                                 secret_key=s3.data['data']['minioPassword'],
                                 secure=False)
        return self._client

    def simple_write_read(self):
        with self._temp_bucket() as bucket_name:
            buf = BytesIO()
            input = os.urandom(1024*1024*2)
            buf.write(input)
            buf.seek(0)

            logger.info("upload 2MiB file")
            self.client.put_object(bucket_name, 'blob', buf, len(input))
            logger.info("download same file")
            obj = self.client.get_object(bucket_name, 'blob')
            logger.info("compare upload and download")
            assert input == obj.read()
            logger.info("comparison valid")

    def mc(self):
        files = self.generate_files()
        with self._temp_bucket() as bucket_name:
            execute_mc('s3_demo_0', bucket_name, files)

    def generate_files(self):
        GiB = 1024**3
        files = []
        # for size in [1, 4, 8]:
        for size in [0.002]:
            name = '%dgb.dat' % size
            generate_file(name, size*GiB)
            files.append(name)
        return files


def generate_file(name, size):
    try:
        with open(name)as f:
            f_size = f.seek(0, os.SEEK_END)
            if f_size == size:
                return
    except FileNotFoundError:
        pass

    size /= 1024**2  # because we write per 1MiB blocks
    args = ['dd', 'if=/dev/urandom', 'of=%s' % name, 'count=%d' % size, 'bs=1M', 'status=progress']
    print(' '.join(args))
    subprocess.run(args)


def execute_mc(minio_name, bucket_name, files):
    dest = "%s/%s" % (minio_name, bucket_name)
    args = ['mc', 'cp', *files, dest]
    print(args)
    proc = subprocess.run(args, encoding='utf-8')
