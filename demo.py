#!/usr/bin/env python3

import click
from jumpscale import j
from monitoring import Monitoring
from s3 import S3Manager
from failures import FailureGenenator
from perf import Perf


class Demo:

    def __init__(self, config):
        self._config = config
        self._node_robot = None

        node, robot = create_clients(config)
        self.node = node
        self.robot = robot

        self.s3 = S3Manager(robot, config['zerotier']['id'], config['zerotier']['token'])
        self.monitoring = Monitoring(node)
        self.failures = FailureGenenator(self)
        self.perf = Perf(self)

    @property
    def node_robot(self):
        if self._node_robot is None:
            j.clients.zrobot.get('node_demo', data={'url': "http://%s:6600" % self._config['node']['ip']})
            self._node_robot = j.clients.zrobot.robots['node_demo']
            try:
                self._node_robot._try_god_token()
            except:
                pass
        return self._node_robot


def create_clients(config):
    node = j.clients.zos.get('demo', data={'host': config['node']['ip']})
    robot = j.clients.zrobot.get('demo', data={'url': config['robot']['url']})
    robot = j.clients.zrobot.robots['demo']
    return node, robot


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
