#!/usr/bin/env python3
from gevent import monkey, spawn, wait
monkey.patch_all()

import click
from gevent.pool import Group
from jumpscale import j
from s3 import S3Manager


class Demo:

    def __init__(self, config):
        self.config = config
        j.clients.zrobot.get('demo', data={'url': config['robot']['url']})
        dm_robot = j.clients.zrobot.robots['demo']
        self.s3 = {}
        for service in dm_robot.services.find(template_name='s3'):
            self.s3[service.name] = S3Manager(self, service.name)

    def deploy_n(self, n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin'):
        jobs = []
        start = len(self.s3)
        for i in range(start, start+n):
            name = 's3_demo_%d' % i
            self.s3[name] = S3Manager(self, name)
            jobs.append(
                spawn(self.s3[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password)))
            wait(jobs)

    def urls(self):
        return {name: url for name, url in self._do_on_all(lambda s3: (s3.name, s3.url))}

    def minio_config(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.minio_config))}

    def states(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.service.state))}

    def _do_on_all(self, func):
        group = Group()
        return group.imap_unordered(func, self.s3.values())


def read_config(path):
    config = j.data.serializer.yaml.load(path)
    return config


@click.command()
@click.option('--config', help='path to config file', default='demo.yaml')
def main(config):
    demo = Demo(read_config('demo.yaml'))
    from IPython import embed
    embed()


# self.client.bash('test -b /dev/{0} && dd if=/dev/zero bs=1M count=500 of=/dev/{0}'.format(diskpath)).get()
if __name__ == '__main__':
    main()
