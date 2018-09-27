import time

import requests
from requests.exceptions import ConnectTimeout, ConnectionError

from jumpscale import j
from zerorobot.template.state import StateCheckError
from urllib.parse import urlparse
logger = j.logger.get()


class EnvironmentReset:

    def __init__(self, parent):
        self._parent = parent

    def armagedon(self, organization):
        """
        Be carefull !!
        This will wipe all the disk of all the nodes from a farm
        """
        def do(node):
            try:
                node.client.ping()
            except:
                logger.info("can't reach %s skipping", node.addr)
                pass

            for disk in node.disks.list():
                logger.info("wipe disk %s on node %s", disk.name, node.name)
                node.client.bash('test -b /dev/{0} && dd if=/dev/zero bs=1M count=500 of=/dev/{0}'.format(disk.name)).get()

        self._parent.execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def restart_robots(self, organization):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.error("can't reach %s skipping", node.addr)
                pass


            try:
                node.containers.get('zrobot').stop()
                logger.info("restart robot on node %s", node.addr)
            except:
                logger.error("can't restart robot on node %s", node.addr)

        self._parent.execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def list_disks(self, organization):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.error("can't reach %s skipping", node.addr)
                return

            logger.info("Disks on node %s", node.addr)
            for disk in node.disks.list():
                logger.info("\tdisk %s", disk.name)
                for part in disk.partitions:
                    logger.info("\t\tpartition %s mounted on %s", part.name, part.mountpoint)

        self._parent.execute_all_nodes(do, nodes=list_farm_nodes(organization))


def list_farm_nodes(farm_organization):
    capacity = j.clients.threefold_directory.get(interactive=False)
    nodes, resp = capacity.api.ListCapacity(query_params={'farmer': farm_organization})
    resp.raise_for_status()
    for n in nodes:
        u = urlparse(n.robot_address)
        yield j.clients.zos.get(n.node_id, data={'host': u.hostname})
