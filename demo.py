#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()

import click

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
        for i in range(n):
            name = 's3_demo_%d' % i
            self.s3[name] = S3Manager(self, name)
            self.s3[name].deploy(farm, size=size, data=data, parity=parity, login=login, password=password)


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
