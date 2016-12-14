# -*- coding: utf-8 -*-

"""
# Run server:
>>> import thriftpy
>>> from pika.connection import URLParameters
>>> from thrifypy_amqp import make_server
>>> pingpong = thriftpy.load("pingpong.thrift")
>>>
>>> class Dispatcher(object):
>>>     def ping(self):
>>>         return "pong"
>>> channel = <create amqp channel>
>>> server = make_server(pingpong.PingService, Dispatcher(), channel)
>>> server.serve()

# Run client:
>>> import thriftpy
>>> from thrifypy_amqp import make_client
>>> from pika import BlockingConnection, URLParameters
>>> amqp_connection = BlockingConnection(URLParameters('amqp://localhost'))
>>> channel = amqp_connection.channel()
>>> result = channel.queue_declare(exclusive=True)
>>> callback_queue = result.method.queue
>>> pingpong = thriftpy.load("pingpong.thrift")
>>> client = make_client(pingpong.PingService, channel, routing_key, callback_queue)
>>> client.ping()
"""

from thriftpy.server import TSimpleServer
from thriftpy.thrift import TProcessor, TClient
from thriftpy.protocol.binary import TBinaryProtocolFactory
from thriftpy.trasport.buffered import (
    TBufferedTransportFactory
)

import pika

import logging

LOGGER = logging.getLogger(__name__)


class TAMQPServer(TSimpleServer):
    """A simple AMQP-based Thrift server"""
    def __init__(self,
                 processor,
                 amqp_url,
                 exchange,
                 routing_key,
                 queue,
                 iprot_factory):
        trans = TAMQPTransport(amqp_url, exchange, routing_key, queue)
        TSimpleServer.__init__(self, processor, trans=trans,
                               itrans_factory=None, iprot_factory=iprot_factory,
                               otrans_factory=None, oprot_factory=None)


class TAMQPTransport(object):
    """AMQP implementation of TTransport base."""
    def __init__(self, amqp_url, exchange, routing_key, queue=None):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self.routing_key = routing_key
        self.queue = queue
        self.channel = None

    def open(self):
        self.parameters = pika.URLParameters(self.amqp_url)
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

        LOGGER.info('Declaring exchange %s', self.exchange)
        self.channel.exchange_declare(exchange=self.exchange)
        LOGGER.info('Exchange declared')
        LOGGER.info('Declaring queue %s', self.queue)
        self.channel.queue_declare(queue=self.queue)
        LOGGER.info('Queue declared')
        LOGGER.info('Binding %s to %s with %s',
                    self.exchange, self.queue, self.routing_key)
        self.channel.queue_bind(self.queue, self.exchange, self.routing_key)
        LOGGER.info('Queue bound')

    def close(self):
        self.channel.close()
        self.connection.close()

    def is_open(self):
        self.channel.is_open()

    def read(self, sz):
        return self.channel.basic_get(self.queue, no_ack=True)

    def write(self, buf):
        self.channel.basic_publish(self.exchange, self.routing_key, buf)

    def flush(self):
        pass

    # server
    def listen(self):
        pass

    def accept(self):
        return self


def make_client(service, amqp_url, exchange, routing_key,
                result_key, result_queue,
                proto_factory=TBinaryProtocolFactory(),
                trans_factory=TBufferedTransportFactory()):
    amqp_client = TAMQPClient(amqp_url, exchange, routing_key,
                              result_key, result_queue)
    transport = trans_factory.get_transport(amqp_client)
    iprot = proto_factory.get_protocol(transport)
    transport.open()
    return TClient(service, iprot)


def make_server(service, handler, amqp_url, exchange, routing_key,
                incoming_queue,
                proto_factory=TBinaryProtocolFactory()):
    processor = TProcessor(service, handler)
    server = TAMQPServer(processor,
                         handler, amqp_url, exchange, routing_key,
                         incoming_queue, iprot_factory=proto_factory)
    return server
