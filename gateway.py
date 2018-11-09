from jumpscale import j
from zerorobot.service_collection import ServiceNotFoundError

from gevent.pool import Pool

from monitoring import Monitoring
from perf import Perf
from failures import FailureGenenator
from urllib.parse import urlparse

logger = j.logger.get('s3demo')


class GatewayManager:

    def __init__(self, parent):
        self._parent = parent
        self.name = 'tlre'
        self.dm_robot = parent.robot

        self._zt_id = self._parent.config['zerotier']['id']
        self._zt_token = self._parent.config['zerotier']['token']

    def deploy(self, node_id, farmer_org, etcd_nr=3):

        logger.info("install zerotier client")
        zt = self.dm_robot.services.find_or_create('zerotier_client', 'zt', data={'token': self._zt_token})

        # node = j.clients.zos.get('ac1f6b457b6c')
        public_robot = j.clients.zrobot.get_by_id(node_id)
        ip = public_robot.services.find_or_create('node_ip', 'public', data={'cidr': '172.20.63.1/18', 'interface': 'backplane'})  # tlre
        # ip = public_robot.services.find_or_create('node_ip', 'public', data={'cidr': '172.16.63.1/18', 'interface': 'backplane'}) #bancadati
        ip.schedule_action('install').wait(die=True)

        logger.info("install web gateway")
        gateway = self.dm_robot.services.find_or_create('web_gateway', service_name=None, data={
            'nics': [{'name': 'zerotier', 'type': 'zerotier', 'ztClient': 'zt', 'id': self._zt_id}],
            'farmerIyoOrg': farmer_org,
            'nrEtcds': etcd_nr,
            'publicNode': node_id,
            'publicIps': ['178.23.173.254']  # tlre
        })
        tasks = []
        tasks.append(gateway.schedule_action('install'))
        tasks.append(gateway.schedule_action('start'))
        return tasks
