from jumpscale import j


class Monitoring:

    def __init__(self, node):
        self.node = node

    def start_rtinfo(self, host, port=9930):
        if "%s:%s" % (host, port) not in self.node.client.rtinfo.list():
            self.node.client.rtinfo.start(host, port, ['sd'])

    def stop_rtinfo(self, host, port=9930):
        if "%s:%s" % (host, port) in self.node.client.rtinfo.list():
            self.node.client.rtinfo.stop(host, port)
