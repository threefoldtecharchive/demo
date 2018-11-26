#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()

import click
from gevent.pool import Pool
from jumpscale import j
from s3 import S3Manager
from s3_redundant import S3RedundantManager
from reset import EnvironmentReset


class Demo:

    def __init__(self, config):
        self.config = config
        self.reset = EnvironmentReset(self)
        j.clients.zrobot.get('demo', data={'url': config['robot']['url'], 'god_token_': ''})
        #j.clients.zrobot.get('demo', data={'url': config['robot']['url']})
        self.dm_robot = j.clients.zrobot.robots['demo']
        self.s3 = {}
        for service in self.dm_robot.services.find(template_name='s3'):
            self.s3[service.name] = S3Manager(self, service.name)

    def deploy_n(self, n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin'):
        start = len(self.s3)
        tasks = []
        for i in range(start, start+n):
            name = 's3_demo_%d' % i
            self.s3[name] = S3Manager(self, name)
            tasks.append(self.s3[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password))
        return tasks

    def deploy_s3_redundant(self, name, farm, size=1000, data=1, parity=1, login='admin', password='adminadmin', wait=False):
        self.s3_redundant[name] = S3RedundantManager(self, name)
        return self.s3_redundant[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password,
                                              wait=wait)

    def urls(self):
        return {name: url for name, url in self._do_on_all(lambda s3: (s3.name, s3.url))}

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
