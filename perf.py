import os
import subprocess
import time
from contextlib import contextmanager
from io import BytesIO
from urllib.parse import urlparse

from gevent import pool
from jumpscale import j
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou

logger = j.logger.get()

MiB = 1024**2
GiB = 1024**3


class Perf:
    def __init__(self, parent):
        self._parent = parent
        self._client_type = 'public'
        self._client = None

    def set_network(self, network):
        if network not in ['public', 'storage']:
            raise ValueError("network must be 'public' or 'storage', not %s" % network)
        self._client_type = network
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
                raise RuntimeError("s3 services not found")

            url = s3.schedule_action('url').wait(die=True).result
            u = urlparse(url[self._client_type])

            self._client = Minio(u.netloc,
                                 access_key=s3.data['data']['minioLogin'],
                                 secret_key=s3.data['data']['minioPassword'],
                                 secure=False)
        return self._client

    def simple_write_read(self, size=None):
        if not size:
            size = 2*MiB

        with self._temp_bucket() as bucket_name:
            buf = BytesIO()
            input = os.urandom(1024*1024*2)
            buf.write(input)
            buf.seek(0)

            logger.info("upload %dMiB file" % size/MiB)
            self.client.put_object(bucket_name, 'blob', buf, len(input))
            logger.info("download same file")
            obj = self.client.get_object(bucket_name, 'blob')
            logger.info("compare upload and download")
            assert input == obj.read()
            logger.info("comparison valid")

    def write_file(self, bucket_name, file_name):
        with open(file_name, 'rb') as f:
            f_stat = os.stat(file_name)
            size = f_stat.st_size
            logger.info("upload %dMiB file" % (size / MiB))

            start = time.time()
            self.client.put_object(bucket_name, file_name, f, size)
            duration = time.time()-start
            s = speed(size, duration)
            logger.info("%dMiB file uploaded in %.2f sec (speed %.2fMiB/s)" % ((size / MiB), duration, s))
        return duration, s, size

    def write_files(self, bucket_name, file_names):
        def func(file_name):
            duration, speed, size = self.write_file(bucket_name, file_name)
            return (file_name, duration, speed, size)

        speed_sum = 0
        size_sum = 0
        group = pool.Group()
        for _, _, speed, size in group.imap_unordered(func, file_names):
            speed_sum += speed
            size_sum += size
        group.join()
        logger.info("uploaded %sMiB, total speed: %.2fMiB/s", size_sum/MiB, speed_sum)

    def mc(self):
        files = self.generate_files()

        s3 = self._parent
        guid = s3.service.guid
        url = s3.schedule_action('url').wait(die=True).result
        u = urlparse(url['public'])

        configure_minio_host(
            name=guid,
            endpoint=u.netloc,
            login=s3.data['data']['minioLogin'],
            password=s3.data['data']['minioPassword'])

        with self._temp_bucket() as bucket_name:
            execute_mc(guid, bucket_name, files)

    def generate_files(self, *sizes):
        """
        generate random files of a certain size

        e.g.: self.generate_files(1,2,3,4)
        will generate 4 files of 1,2,3 and 4 GiB

        :return: list of files name
        :rtype: list
        """
        files = []
        for size in sizes:
            name = '%.2fgb.dat' % size
            generate_file(name, size*GiB)
            files.append(name)
        return files


def speed(size, duration):
    return (size/MiB) / duration


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


def configure_minio_host(name, endpoint, login, password):
    args = ['mc', 'config', 'host', 'rm', name]
    subprocess.run(args)
    args = ['mc', 'config', 'host', 'add', name, endpoint, login, password]
    print(' '.join(args))
    subprocess.run(args)


def execute_mc(name, bucket, files):
    dest = "%s/%s" % (name, bucket)
    args = ['mc', 'cp', *files, dest]
    print(args)
    proc = subprocess.run(args, encoding='utf-8')
