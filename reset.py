import time
import random

import requests
from requests.exceptions import ConnectTimeout, ConnectionError

from jumpscale import j
from zerorobot.template.state import StateCheckError
from urllib.parse import urlparse
logger = j.logger.get()

from gevent.pool import Pool


class EnvironmentReset:

    def __init__(self, parent):
        self._parent = parent

    def reboot(self, organization):
        """
        Be carefull !!
        This will wipe all the disk of all the nodes from a farm
        """
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            logger.info("reboot node %s", node.name)
            node.reboot()

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def armagedon(self, organization):
        """
        Be carefull !!
        This will wipe all the disk of all the nodes from a farm
        """
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            for disk in node.disks.list():
                logger.info("wipe disk %s on node %s", disk.name, node.name)
                node.client.bash('test -b /dev/{0} && dd if=/dev/zero bs=1M count=500 of=/dev/{0}'.format(disk.name)).get()

            logger.info("reboot node %s", node.name)
            node.reboot()

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def install_network(self, organization):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            logger.info("install network on node %s" % node.addr)
            try:
                robot = j.clients.zrobot.get_by_id(node.name)
                network = robot.services.find_or_create('network', 'backplane', data={
                    'cidr': '172.16.0.0/18',
                    'vlan': 1,
                    'bonded': True,
                    'driver': ''
                })
                t = network.schedule_action('configure')
                t.wait(die=True)
            except:
                logger.error("fail to install network on %s" % node.addr)

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def install_linux_bind(self):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            logger.info("install linux bond on node %s" % node.addr)
            bond_name = 'backplane'
            node.client.system('ip link add %s type bond' % bond_name)
            node.client.system('ip link set enp2s0f0 master %s' % bond_name)
            node.client.system('ip link set enp2s0f1 master %s' % bond_name)
            node.client.system('ip link set %s up' % bond_name)

    def restart_bond_interfaces(self, organization, interface):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            logger.info("restart interface %s on node %s" % (interface, node.addr))
            node.client.ip.link.down(interface)
            time.sleep(2)
            node.client.ip.link.up(interface)

        for node in list_farm_nodes(organization):
            do(node)
            time.sleep(1)

        execute_all_nodes(do, nodes=list_farm_nodes(organization))
        # execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def hack_bond(self, organization):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            time.sleep(random.random())
            logger.info("hack node %s" % node.addr)
            script = """
            ovs-vsctl del-port backplane bond0
            modprobe bonding
            ip l del bond0
            ip l add bond0 type bond
            ip l set bond0 down
            echo balance-rr > /sys/class/net/bond0/bonding/mode
            ip l set bond0 up
            ifenslave bond0 enp2s0f1
            ifenslave bond0 enp2s0f0
            ip link set bond0 mtu 9000
            ovs-vsctl add-port backplane bond0"""
            node.client.bash(script).get()

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def restart_robots(self, organization):
        def do(node):
            try:
                node.client.ping()
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

            logger.info("restart robot on node %s", node.addr)
            node.containers.get('zrobot').stop()

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def list_disks(self, organization):
        def do(node):
            try:
                node.client.ping()

                logger.info("Disks on node %s", node.addr)
                for disk in node.disks.list():
                    logger.info("\tdisk %s", disk.name)
                    for part in disk.partitions:
                        logger.info("\t\tpartition %s mounted on %s", part.name, part.mountpoint)
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

        execute_all_nodes(do, nodes=list_farm_nodes(organization))

    def ping(self, organization):
        versions = set()

        def do(node):
            try:
                result = node.client.ping()
                version = result.split(":")[2].strip()
                versions.add(version)
                logger.info("node %s online", node.name)
            except:
                logger.warning("can't reach %s skipping", node.addr)
                return

        execute_all_nodes(do, nodes=list_farm_nodes(organization))
        return versions

    def remove_all_godtoken(self):
        for path in j.sal.fs.listFilesInDir(j.sal.fs.joinPaths(j.tools.configmanager.path, 'j.clients.zrobot')):
            data = j.data.serializer.toml.load(path)
            if data.get('god_token_'):
                print('remove god token from %s' % path)
                data['god_token_'] = ''
                j.data.serializer.toml.dump(path, data)


def execute_all_nodes(func, nodes):
    """
    execute func on all the nodes
    """
    g = Pool(size=50)
    g.map(func, nodes)
    g.join()


def list_farm_nodes(farm_organization):
    capacity = j.clients.threefold_directory.get(interactive=False)
    nodes, resp = capacity.api.ListCapacity(query_params={'farmer': farm_organization})
    resp.raise_for_status()
    for n in nodes:
        u = urlparse(n.robot_address)
        yield j.clients.zos.get(n.node_id, data={'host': u.hostname})
