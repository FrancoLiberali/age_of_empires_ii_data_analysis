import pika
from communications.constants import STRING_COLUMN_SEPARATOR, MATCHES_IDS_SEPARATOR, STRING_LINE_SEPARATOR

RABBITMQ_HOST = 'rabbitmq'  # TODO envvar
STRING_ENCODING = 'utf-8'
SENTINEL_MESSAGE = "SENTINEL"

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

    def send_list_of_columns(self, list_of_columns, routing_key=''):
        if len(list_of_columns) > 0:
            list_string = self._get_string_from_list_of_columns(list_of_columns)
            self.send_string(list_string, routing_key)

    def send_matches_ids(self, matches_ids, routing_key=''):
        if len(matches_ids) > 0:
            matches_ids_string = MATCHES_IDS_SEPARATOR.join(matches_ids)
            self.send_string(matches_ids_string, routing_key)


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

    def __init__(self, rabbit_MQ_connection, name, declare=True):
        RabbitMQInterface.__init__(self, rabbit_MQ_connection, name)
        if declare:
            self.channel.queue_declare(queue=self.name)

    @classmethod
    def newPrivate(cls, rabbit_MQ_connection):
        result = rabbit_MQ_connection.channel.queue_declare(queue='')
        private_queue_name = result.method.queue
        return cls(rabbit_MQ_connection, private_queue_name, False)

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
            chunk_string = body.decode(STRING_ENCODING)
            if chunk_string == SENTINEL_MESSAGE:
                stop = QueueInterface.STOP_CONSUMING
                if on_sentinel_callback is not None:
                    stop = on_sentinel_callback()
                if stop is QueueInterface.STOP_CONSUMING:
                    print("Sentinel message received, stoping receiving")
                    channel.stop_consuming()
            else:
                on_message_callback(self, chunk_string, method.routing_key)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        return internal_on_message_callback

    def stop_consuming(self):
        self.channel.stop_consuming()


def get_on_sentinel_send_sentinel_callback_function(output):
    def on_sentinel_callback():
        print("Sending sentinel to next stage to notify that all data has been sended")
        output.send_sentinel()
    return on_sentinel_callback
