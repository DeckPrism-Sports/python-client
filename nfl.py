#!/usr/bin/env python3

import datetime
import functools
import json
import logging
from pprint import pprint
import sys
import time

import pika  # noqa
import requests  # noqa


_DURABLE = False


logformat = ('%(asctime)s %(levelname)s %(module)s:L%(lineno)s'
             ' [%(threadName)s] - %(message)s')

logging.basicConfig(filename='log.txt',
                    level=logging.INFO,
                    format=logformat)

LOG = logging.getLogger(__name__)


def today():
    return datetime.datetime.now().strftime('%Y%m%d')


_API_ARGS = {'userName': 'username',
             'userPass': 'password',
             'apiKey': 'apikey',
             'host': 'man1-phx2.deckprismsports.com',
             'vHost': 'nfl_main',
             'exchange': 'nfl_upstream',
             'sport': 'nfl',
             'date': today()}


_API_URI = "https://{APIHOST}/api/lines/{SPORT}/{DATE}?apikey={APIKEY}".format(
    APIHOST=_API_ARGS.get('host'),
    SPORT=_API_ARGS.get('sport'),
    DATE=_API_ARGS.get('date'),
    APIKEY=_API_ARGS.get('apiKey')
)


_BROKER_URI = "amqps://{USER}:{PASS}@{AMQPHOST}/{VHOST}?heartbeat=15".format(
    USER=_API_ARGS.get('userName'), PASS=_API_ARGS.get('userPass'),
    AMQPHOST=_API_ARGS.get('host'), VHOST=_API_ARGS.get('vHost', 'nfl_main'))


def api_data_handler(data, *args, **kwargs):
    LOG.info("Data arrived from API request")
    if not data:
        LOG.info("Got an empty response")
    try:
        # ...do stuff with the data...
        print("API data: ")
        print(_API_URI)
        pprint(json.dumps(data.json()))
    except json.decoder.JSONDecodeError:
        LOG.info("Got a malformed response")


def get_api_data(uri=_API_URI):
    r = requests.get(uri,
                     hooks={'response': api_data_handler},
                     params={'apikey': _API_ARGS.get('apiKey')})
    LOG.info("Request response status code: {}".format(r.status_code))
    return r.ok


get_api_data()  # fetches data from the api and calls api_data_handler(data)


def amqp_data_handler(data):
    LOG.info("Data arrived from AMQP subscription")
    print("AMQP data: ")
    pprint(data)


class AMQPConsumer:
    def __init__(self, conf=_API_ARGS):
        self._conf = conf

        self._amqp_url = _BROKER_URI
        self._exchange_name = self._conf.get('exchange')
        self._exchange_type = 'fanout'
        self._queue_name = ''
        self._routing_key = ''

        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 20

    def connect(self):
        LOG.info('Connecting to {}'.format(self._amqp_url))
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._amqp_url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOG.info('Connection is closing or already closed')
        else:
            LOG.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOG.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOG.error('Connection open failed: {}'.format(err))
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOG.warning('Connection closed, reconnect necessary: {}'.format(
                reason))
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        LOG.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOG.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange_name)

    def add_on_channel_close_callback(self):
        LOG.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        LOG.warning('Channel {} was closed: {}'.format(channel, reason))
        self.close_connection()

    def setup_exchange(self, exchange_name):
        LOG.info('Declaring exchange: {}'.format(exchange_name))
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            passive=True,
            exchange_type=self._exchange_type,
            durable=_DURABLE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOG.info('Exchange declared: {}'.format(userdata))
        self.setup_queue(self._queue_name)

    def setup_queue(self, queue_name):
        LOG.info('Declaring queue {}'.format(queue_name))
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    durable=_DURABLE,
                                    callback=cb,
                                    auto_delete=True)

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOG.info('Binding {} to {} with {}'.format(self._exchange_name,
                                                   queue_name, self._routing_key))
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self._exchange_name,
            routing_key=self._routing_key,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        LOG.info('Queue bound: {}'.format(userdata))
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOG.info('QOS set to: {}'.format(self._prefetch_count))
        self.start_consuming()

    def start_consuming(self):
        LOG.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue_name, self.on_message, auto_ack=True)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        LOG.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOG.info('Consumer was cancelled remotely, shutting down: {}'.format(
            method_frame))
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOG.info('Received message # {} length {}'.format(
            basic_deliver.delivery_tag, len(body.decode())))
        amqp_data_handler(body)

    def acknowledge_message(self, delivery_tag):
        LOG.info('Acknowledging message {}'.format(delivery_tag))
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            LOG.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        LOG.info('RabbitMQ acknowledged the cancellation of the consumer: '
                 '{}'.format(userdata))
        self.close_channel()

    def close_channel(self):
        LOG.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOG.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOG.info('Stopped')


class ReconnectingPoolConsumer:
    def __init__(self, conf=_API_ARGS):
        self._conf = conf

        self._reconnect_delay = 0
        self._consumer = AMQPConsumer(self._conf)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOG.info('Reconnecting after {} seconds'.format(reconnect_delay))
            time.sleep(reconnect_delay)
            self._consumer = AMQPConsumer(self._conf)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1.5
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


a = ReconnectingPoolConsumer()
try:
    a.run()
except KeyboardInterrupt:
    sys.exit(0)
