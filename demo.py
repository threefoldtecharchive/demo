#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()

import click
from failures import FailureGenenator
from gevent.pool import Group
from jumpscale import j
from monitoring import Monitoring
from perf import Perf
from s3 import S3Manager
from reset import EnvironmentReset


class Demo:

    def __init__(self, config):
        self.config = config

        self.s3 = S3Manager(self)
        self.monitoring = Monitoring(self)
        self.failures = FailureGenenator(self)
        self.perf = Perf(self)
        self.reset = EnvironmentReset(self)

    def execute_all_nodes(self, func, nodes=None):
        """
        execute func on all the nodes

        if nodes is None, func is execute on all the nodes that play a role with the minio
        deployement, if nodes is not None, it needs to be an iterable containing a node object

        :param func: function to execute, func needs to accept one argument, a node object
        :type func: function
        :param nodes: list of node on whic to execute func, defaults to None
        :param nodes: iterable, optional
        """

        if nodes is None:
            nodes = set([self.s3.vm_node, self.s3.vm_host])
            nodes.update(self.s3.zerodb_nodes)

        g = Group()
        g.map(func, nodes)
        g.join()


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
