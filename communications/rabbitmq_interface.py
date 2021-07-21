from hashlib import md5
import os
import pika

from logger.logger import Logger
from config.envvars import RABBITMQ_HOST_KEY, get_config_param

logger = Logger()
RABBITMQ_HOST = get_config_param(RABBITMQ_HOST_KEY, logger)
STRING_ENCODING = 'utf-8'
SENTINEL_MESSAGE = "SENTINEL"
SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR = " - "
STRING_LINE_SEPARATOR = '\n'
STRING_COLUMN_SEPARATOR = ', '
LAST_HASH_DIR_PATH = "/last_hash/"

class RabbitMQConnection:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        self.channel = self.connection.channel()  

    def close(self):
        self.connection.close()


class RabbitMQInterface:
    def __init__(self, rabbit_MQ_connection, name):
        self.channel = rabbit_MQ_connection.channel
        self.name = name

    def send_sentinel(self, routing_key=''):
        self.send_string(SENTINEL_MESSAGE, routing_key)

    def _get_string_from_list_of_columns(self, list_of_columns):
        return STRING_LINE_SEPARATOR.join(
            [
                STRING_COLUMN_SEPARATOR.join(columns) for columns in list_of_columns
            ]
        )

    def send_list_as_rows(self, list, routing_key=''):
        if len(list) > 0:
            self.send_string(STRING_LINE_SEPARATOR.join(list), routing_key)

    def send_list_of_columns(self, list_of_columns, routing_key=''):
        if len(list_of_columns) > 0:
            list_string = self._get_string_from_list_of_columns(list_of_columns)
            self.send_string(list_string, routing_key)


class ExchangeInterface(RabbitMQInterface):
    def __init__(self, rabbit_MQ_connection, name, ex_type):
        RabbitMQInterface.__init__(self, rabbit_MQ_connection, name)
        self.channel.exchange_declare(
            exchange=self.name,
            exchange_type=ex_type)

    @classmethod
    def newFanout(cls, rabbit_MQ_connection, name):
        return cls(rabbit_MQ_connection, name, "fanout")

    @classmethod
    def newDirect(cls, rabbit_MQ_connection, name):
        return cls(rabbit_MQ_connection, name, "direct")

    def send_string(self, message, routing_key=''):
        self.channel.basic_publish(
            exchange=self.name,
            routing_key=routing_key,
            body=message.encode(STRING_ENCODING)
        )


class QueueInterface(RabbitMQInterface):
    STOP_CONSUMING = None
    NO_STOP_CONSUMING = 1

    def __init__(self, rabbit_MQ_connection, name, private=False):
        RabbitMQInterface.__init__(self, rabbit_MQ_connection, name)
        self.private = private
        if not private:
            os.makedirs(os.path.dirname(LAST_HASH_DIR_PATH), exist_ok=True)
            last_hash_file_path = LAST_HASH_DIR_PATH + name + ".txt"
            try:
                self.last_hash_file = open(last_hash_file_path, "r+")
                self.last_hash = self.last_hash_file.readline()
                logger.debug(f"{self.name} - Initial last hash: {self.last_hash}")
            except FileNotFoundError:
                logger.debug(f"{self.name} - Last hash file not found, creating. No initial last hash")
                self.last_hash_file = open(last_hash_file_path, "w+")
                self.last_hash = ''
            self.channel.queue_declare(queue=self.name)

    @classmethod
    def newPrivate(cls, rabbit_MQ_connection):
        result = rabbit_MQ_connection.channel.queue_declare(queue='')
        private_queue_name = result.method.queue
        return cls(rabbit_MQ_connection, private_queue_name, private=True)

    def bind(self, exchange, routing_key=None):
        self.channel.queue_bind(
            exchange=exchange.name,
            queue=self.name,
            routing_key=routing_key
        )

    def send_string(self, message, _=None):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.name,
            body=message.encode(STRING_ENCODING)
        )

    def consume(self, on_message_callback, on_sentinel_callback=None):
        self.channel.basic_consume(
            queue=self.name,
            on_message_callback=self._get_on_message_callback_function(
                on_message_callback,
                on_sentinel_callback
            ),
            auto_ack=False
        )
        self.channel.start_consuming()

    def _get_on_message_callback_function(self, on_message_callback, on_sentinel_callback):
        def internal_on_message_callback(channel, method, properties, body):
            actual_hash = md5(body).hexdigest()
            chunk_string = body.decode(STRING_ENCODING)
            if self._is_different_to_last_hash(actual_hash):
                splited_chunk_string = chunk_string.split(
                    SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR)
                if chunk_string == SENTINEL_MESSAGE or splited_chunk_string[-1] == SENTINEL_MESSAGE:
                    stop = QueueInterface.STOP_CONSUMING
                    if on_sentinel_callback is not None:
                        stop = on_sentinel_callback(splited_chunk_string[0])
                    if stop is QueueInterface.STOP_CONSUMING:
                        logger.info("Sentinel message received, stoping receiving")
                        channel.stop_consuming()
                else:
                    on_message_callback(self, chunk_string, method.routing_key)
                self._store_actual_hash(actual_hash)
            else:
                logger.debug(
                    f"{self.name} - Duplicated message: {actual_hash} {chunk_string}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        return internal_on_message_callback

    def _is_different_to_last_hash(self, actual_hash):
        if not self.private:
            return actual_hash != self.last_hash
        else:
            return True

    def _store_actual_hash(self, actual_hash):
        if not self.private:
            self.last_hash = actual_hash
            self.last_hash_file.seek(0)
            self.last_hash_file.write(self.last_hash)

    def stop_consuming(self):
        self.channel.stop_consuming()


def get_on_sentinel_send_sentinel_callback_function(output):
    def on_sentinel_callback(_):
        logger.info(
            "Sending sentinel to next stage to notify that all data has been sended")
        output.send_sentinel()
    return on_sentinel_callback

def split_columns_into_list(columns_string):
    return columns_string.split(STRING_COLUMN_SEPARATOR)

def split_rows_into_list(rows_string):
    return rows_string.split(STRING_LINE_SEPARATOR)
