import pika

from communications.constants import FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE
from common.partition_function import get_posibles_keys

REDUCERS_AMOUNT = 3

def group_by_match(channel, method, properties, body):
    print(
        f"Routing Key: {method.routing_key} - Message: {body.decode(STRING_ENCODING)}")

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    for key in get_posibles_keys():
        channel.queue_bind(
            exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
            queue=private_queue_name,
            routing_key=key)

    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=group_by_match,
        auto_ack=True # TODO sacar esto
    )
    print('Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
