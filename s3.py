from jumpscale import j
from zerorobot.service_collection import ServiceNotFoundError

logger = j.logger.get('s3demo')


class S3Manager:

    def __init__(self, parent, s3_name):
        self._parent = parent
        self._s3_name = s3_name
        j.clients.zrobot.get('demo', data={'url': self._parent.config['robot']['url']})
        self.dm_robot = j.clients.zrobot.robots['demo']

        self._zt_id = self._parent.config['zerotier']['id']
        self._zt_token = self._parent.config['zerotier']['token']

        self._vm_node = None
        self._vm_robot = None
        try:
            self._service = self.dm_robot.services.get(name=s3_name)
        except ServiceNotFoundError:
            self._service = None

    @property
    def service(self):
        if self._service is None:
            raise RuntimeError("s3 service doesn't exist yet, call deploy to create it")
        return self._service

    @property
    def service_vm(self):
        return self.dm_robot.services.get(name=self.service.guid)

    @property
    def vm_node(self):
        """
        zos client on the zos VM that host the minio container
        """
        if self._vm_node is None:
            dm_vm = self.dm_robot.services.get(name=self.service.guid)
            ip = dm_vm.schedule_action('info').wait(die=True).result['zerotier']['ip']
            self._vm_node = j.clients.zos.get('demo_vm_node', data={'host': ip})
        return self._vm_node

    @property
    def vm_robot(self):
        """
        zrobot client on the zos VM that host the minio container
        """
        if self._vm_robot is None:
            j.clients.zrobot.get('demo_vm_robot', data={'url': "http://%s:6600" % self.vm_node.public_addr})
            self._vm_robot = j.clients.zrobot.robots['demo_vm_robot']
        return self._vm_robot

    @property
    def zerodb_nodes(self):
        for zerodb in self.service.data['data']['namespaces']:
            yield j.clients.zos.get(zerodb['node'])

    @property
    def minio_container(self):
        """
        container running minio.
        This containers run on vm_node
        """
        return self.vm_node.containers.get("minio_%s" % self.service.guid)

    @property
    def vm_host(self):
        """
        zos machine that host the vm_node
        """

        vm = self.dm_robot.services.get(template_name='dm_vm', name=self.service.guid)
        return j.clients.zos.get(vm.data['data']['nodeId'])

    def deploy(self, farm, size=20000, data=4, parity=2, shard_size=2000, login='admin', password='adminadmin'):
        """
        deploy an s3 environment

        :return: return the install task of the s3 service created
        :rtype: Task
        """

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
            'shardSize': shard_size,
            'minioLogin': login,
            'minioPassword': password}
        self._service = self.dm_robot.services.find_or_create('s3', self._s3_name, data=s3_data)
        return self._service.schedule_action('install')

    @property
    def url(self):
        """
        return the urls of the s3 once it's deployed
        """
        return self.service.schedule_action('url').wait(die=True).result
