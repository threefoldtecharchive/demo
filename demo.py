#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()

import click
from gevent.pool import Pool
from jumpscale import j
from s3 import S3Manager

from reset import EnvironmentReset
from gateway import GatewayManager


class Demo:

    def __init__(self, config):
        self.config = config
        self.robot_name = config['robot']['name']
        j.clients.zrobot.get(self.robot_name, data={'url': self.config['robot']['url']})
        self.robot = j.clients.zrobot.robots(self.robot_name)
        self.reset = EnvironmentReset(self)
        self.gateway = GatewayManager(self)
        self._s3 = None

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = {}
            for service in self.robot.services.find(template_name='s3'):
                self.s3[service.name] = S3Manager(self, service.name)
        return self._s3

    def deploy_n(self, n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin'):
        start = len(self.s3)
        tasks = []
        for i in range(start, start+n):
            name = 's3_demo_%d' % i
            self.s3[name] = S3Manager(self, name)
            tasks.append(self.s3[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password))
        return tasks

    def deploy_redundant_n(self, n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin'):
        start = len(self.s3)
        tasks = []
        for i in range(start, start+n):
            name = 's3_demo_%d' % i
            self.s3[name] = S3Manager(self, name)
            tasks.append(self.s3[name].deploy_redundant(farm, size=size, data=data, parity=parity, login=login, password=password))
        return tasks

    def urls(self):
        tasks = []
        for name, s3 in self.s3.items():
            task = s3.service.schedule_action("url")
            tasks.append(task)

        while True:
            if not tasks:
                return

            for task in tasks:
                if task.state == 'ok':
                    tasks.remove(task)
                    yield task.result

    def minio_config(self):
        out = {}
        for name, config in self._do_on_all(lambda s3: (s3.name, s3.minio_config)):
            out[name] = j.data.serializer.yaml.loads(config)
        return out

    def states(self):
        return {name: config for name, config in self._do_on_all(lambda s3: (s3.name, s3.service.state))}

    def spreading(self):
        import collections
        configs = self.minio_config()
        output = {}
        tlogs = []
        for name, configs in configs.items():
            data_shards = sorted(configs['datastor']['shards'])
            output[name] = {
                'data_shards': data_shards,
                'duplicated_shards': [item for item, count in collections.Counter(data_shards).items() if count > 1],
                'tlog_shard': configs['minio']['tlog']['address'],
            }
            tlogs.append(configs['minio']['tlog']['address'])
        output['tlogs'] = sorted(tlogs)
        return output

    def _do_on_all(self, func):
        pool = Pool(size=100)
        return pool.imap_unordered(func, self.s3.values())


def read_config(path):
    config = j.data.serializer.yaml.load(path)
    return config


@click.command()
@click.option('--config', help='path to config file', default='demo.yaml')
def main(config):
    demo = Demo(read_config('demo.yaml'))
    from IPython import embed
    embed()


if __name__ == '__main__':
    main()
