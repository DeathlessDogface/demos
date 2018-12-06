#! /usr/bin/env python
import ConfigParser
import sys

import pika

HELP = '''
-t=<notify type> #string e.g. poll/event/alert
-m=<event message> #json string
e.g. '{"key":"data.damage.silence",
       "id":"f270117d-1cb2-4226-a1f2-ba7d536c39b5.osd.2",
       "msg":"other info"}'
-f=<config file path> #string e.g. "/etc/storagemgmt/sdsagent.conf"
'''
cfg_file = "./rabbitmq.conf"

QUEUE_MAP = {'poll': 'pool_notify_queue',
             'event': 'event_notify_queue',
             'alert': 'alert_notify_queue'}


class ParamException(Exception):
    def __init__(self):
        super(ParamException, self).__init__("params error!\n{}".format(HELP))


class NotifyTool(object):
    def __init__(self, notify_type, cfg=cfg_file):
        self.queue_name = QUEUE_MAP.get(notify_type)
        self.CONF = ConfigParser.SafeConfigParser()
        self.CONF.read(cfg)
        username = self.CONF.get('oslo_messaging_rabbit', 'rabbit_userid')
        pwd = self.CONF.get('oslo_messaging_rabbit', 'rabbit_password')
        addr = self.CONF.get('oslo_messaging_rabbit', 'rabbit_hosts')
        port = self.CONF.get('oslo_messaging_rabbit', 'rabbit_port')

        user_pwd = pika.PlainCredentials(username, pwd)
        self.parameters = pika.ConnectionParameters(addr, port, credentials=user_pwd, heartbeat_interval=0)
        self.connection = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def __enter__(self):
        s_conn = pika.BlockingConnection(self.parameters)
        self.connection = s_conn
        channel = s_conn.channel()
        sender = lambda msg: channel.basic_publish(exchange='',
                                                   routing_key=self.queue_name,
                                                   body=msg,
                                                   properties=pika.BasicProperties(delivery_mode=2))
        return sender


def main(*args, **kwargs):
    notify_type = kwargs.get('t', 'alert')
    msg = kwargs.get('m')
    with NotifyTool(notify_type) as sender:
        sender(msg)


if __name__ == "__main__":
    args = sys.argv[1:]
    _args = [x.strip('-') for x in args if '=' not in x]
    kwargs = dict([(x.split('=')[0].strip('-'), x.split('=')[1]) for x in args if x.startswith('-') and "=" in x])
    if not kwargs.get('m', False):
        raise ParamException()
    if kwargs.get('f', False):
        cfg_file = kwargs.get('f')
    main(*_args, **kwargs)
