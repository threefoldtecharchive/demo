from jumpscale import j
from zerorobot.service_collection import ServiceNotFoundError

logger = j.logger.get('demo1')


class S3Manager:

    def __init__(self, dm_robot, zt_id, zt_token):
        self.dm_robot = dm_robot
        self._zt_id = zt_id
        self._zt_token = zt_token
        self._node_robot = None
        self._vm_node = None
        self._vm_robot = None
        try:
            self.service = self.dm_robot.services.get(name='s3-demo')
        except ServiceNotFoundError:
            self.service = None

    @property
    def vm_node(self):
        if self._vm_node is None:
            if not self.service:
                return

            dm_vm = self.dm_robot.services.get(name=self.service.guid)
            ip = dm_vm.schedule_action('info').wait().result['zerotier']['ip']
            self._vm_node = j.clients.zos.get('demo_vm_node', data={'host': ip})
        return self._vm_node

    @property
    def vm_robot(self):
        if self._vm_robot is None:
            if not self.service:
                return

            dm_vm = self.dm_robot.services.get(name=self.service.guid)
            ip = dm_vm.schedule_action('info').wait().result['zerotier']['ip']
            j.clients.zrobot.get('demo_vm_robot', data={'url': "http://%s:6600" % ip})
            self._vm_robot = j.clients.zrobot.robots['demo_vm_robot']
        return self._vm_robot

    @property
    def minio_container(self):
        for cont in self.vm_node.containers.list():
            if cont.name.startswith('minio_'):
                return cont

    def create_s3(self, farm, size=20, data=4, parity=2, login='admin', password='adminadmin'):
        logger.info("install zerotier client")
        zt = self.dm_robot.services.find_or_create('zerotier_client', 'zt', data={'token': self._zt_token})

        logger.info("install s3 service")
        s3_data = {
            'mgmtNic': {'id': self._zt_id, 'ztClient': 'zt'},
            'farmerIyoOrg': farm,
            'dataShards': data,
            'parityShards': parity,
            'storageType': 'hdd',
            'storageSize': size,
            'minioLogin': login,
            'minioPassword': password}
        s3 = self.dm_robot.services.find_or_create('s3', 's3-demo', data=s3_data)
        t = s3.schedule_action('install').wait(die=True)

        self.service = s3

        return s3
