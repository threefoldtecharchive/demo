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
        robot = self._parent.node_robot
        for zdb in robot.services.find(template_name='zerodb'):
            logger.info('start %s' % zdb)
            zdb.schedule_action('start')

    def minio_process_down(self):
        url = self._parent.s3.url
        cont = self._parent.s3.minio_container
        for job in cont.client.job.list():
            if job['cmd']['id'].startswith('minio.'):
                logger.info('killing minio process')
                cont.client.job.kill(job['cmd']['id'])
                break
        else:
            return
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
