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
