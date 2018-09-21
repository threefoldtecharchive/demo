import time

import requests
from requests.exceptions import ConnectTimeout, ConnectionError

from jumpscale import j
from zerorobot.template.state import StateCheckError

logger = j.logger.get()


class FailureGenenator:

    def __init__(self, parent):
        self._parent = parent

    def zdb_start_all(self):
        """
        start all the zerodb services used by minio
        """

        def do(node):
            j.clients.zrobot.get(node.name, data={'url': 'http://%s:6600' % node.public_addr})
            robot = j.clients.zrobot.robots[node.name]
            for zdb in robot.services.find(template_name='zerodb'):
                logger.info('start zerodb %s on node %s', zdb.name, node.name)
                zdb.schedule_action('start')

        self._parent.execute_all_nodes(do, nodes=self._parent.s3.zerodb_nodes)

    def zdb_stop_all(self, destroy_data=False):
        """
        start all the zerodb services used by minio
        """

        def do(node):
            j.clients.zrobot.get(node.name, data={'url': 'http://%s:6600' % node.public_addr})
            robot = j.clients.zrobot.robots[node.name]
            for zdb in robot.services.find(template_name='zerodb'):
                logger.info('stop zerodb %s on node %s', zdb.name, node.name)
                zdb.schedule_action('stop').wait(die=True)
                if destroy_data:
                    logger.info("delete zerodb data from %s", node.name)
                    node.client.bash('rm -r /mnt/zdbs/*/*').get()

        self._parent.execute_all_nodes(do, nodes=self._parent.s3.zerodb_nodes)

    def minio_process_down(self):
        """
        turn off the minio process, then count how much times it takes to restart
        """
        s3 = self._parent.s3
        url = s3.url['public']
        cont = s3.minio_container

        logger.info('killing minio process')
        cont.client.job.kill('minio.%s' % s3.service.guid)

        logger.info("wait for minio to restart")
        start = time.time()
        while True:
            try:
                resp = requests.get(url, timeout=0.2)
                end = time.time()
                break
            except ConnectionError:
                continue
            except ConnectionError:
                continue

        duration = end-start
        logger.info("minio took %s sec to restart" % duration)
        return duration

    def zdb_down(self, count=1):
        """
        ensure that count zdb are turned off
        """
        robot = self._parent.node_robot
        s3 = self._parent.s3.service
        if not s3:
            return

        n = 0
        for namespace in s3.data['data']['namespaces']:
            if n >= count:
                break
            ns = robot.services.get(name=namespace['name'])
            zdb = robot.services.get(name=ns.data['data']['zerodb'])

            try:
                zdb.state.check('status', 'running', 'ok')
                logger.info('stop %s' % zdb)
                zdb.schedule_action('stop').wait(die=True)
            except StateCheckError:
                pass
            n += 1
