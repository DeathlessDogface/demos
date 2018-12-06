#!/usr/bin/env python
import ConfigParser
import sys
import time

import pika

HELP = '''
-f=<config file path> #string e.g. "/etc/storagemgmt/storagemgmt.conf"
-t=<notify type> #string e.g. poll/event/alert
'''
cfg_file = "./rabbitmq.conf"

QUEUE_MAP = {'poll': 'pool_notify_queue',
             'event': 'event_notify_queue',
             'alert': 'alert_notify_queue'}


class ParamException(Exception):
    def __init__(self):
        super(ParamException, self).__init__("params error!\n{}".format(HELP))


def _handler(ch, method, properties, body):
    print(" [Consumer] Received %r" % body)
    time.sleep(1)
    print(" [Consumer] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


class NotifyListener(object):
    def __init__(self, notify_type, cfg=cfg_file):
        self.queue_name = QUEUE_MAP.get(notify_type)
        self.CONF = ConfigParser.SafeConfigParser()
        self.CONF.read(cfg)

    def listen(self, handler=_handler):
        username = self.CONF.get('oslo_messaging_rabbit', 'rabbit_userid')
        pwd = self.CONF.get('oslo_messaging_rabbit', 'rabbit_password')
        addr = self.CONF.get('oslo_messaging_rabbit', 'rabbit_hosts')
        port = self.CONF.get('oslo_messaging_rabbit', 'rabbit_port')
        user_pwd = pika.PlainCredentials(username, pwd)
        s_conn = pika.BlockingConnection(pika.ConnectionParameters(addr, port, credentials=user_pwd))
        channel = s_conn.channel()
        try:
            channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception:
            print "the queue:{} is exist".format(self.queue_name)
        try:
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(handler, queue=self.queue_name)
            channel.start_consuming()
        finally:
            s_conn.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    _args = [x.strip('-') for x in args if '=' not in x]
    kwargs = dict([(x.split('=')[0].strip('-'), x.split('=')[1]) for x in args if x.startswith('-') and "=" in x])
    if kwargs.get('f', False):
        cfg_file = kwargs.get('f')
    notify_type = kwargs.get('t', 'alert')
    NotifyListener(notify_type).listen()
