#!/usr/bin/env python3
import pika
from datetime import datetime, time

from communications.constants import STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_matches_ids, send_sentinel_to_queue

TOKEN_INDEX = 0
AVERAGE_RATING_INDEX = 1
SERVER_INDEX = 2
DURATION_INDEX = 3

MINIMUM_AVERAGE_RATING = 2000 # TODO envvar
MINIMUM_DURATION = time(hour=2)  # TODO envvar
REQUIRED_SERVERS = ['koreacentral', 'southeastasia', 'eastus']  # TODO envvar
DURATION_FORMAT = '%H:%M:%S' # TODO envvar

def is_average_rating_enough(average_rating_string):
    # TODO catchear exceptions
    return (average_rating_string != '' and int(average_rating_string) > MINIMUM_AVERAGE_RATING)

def is_duration_enough(duration_string):
    try:
        return (datetime.strptime(duration_string, DURATION_FORMAT).time() > MINIMUM_DURATION)
    except ValueError as e:
        if duration_string[-3:] == "day" or duration_string[-4:] == "days":
            return True
        else:
            # TODO log error
            return False

def is_matched(columns):
    return (is_average_rating_enough(columns[AVERAGE_RATING_INDEX]) and \
        columns[SERVER_INDEX] in REQUIRED_SERVERS and \
        is_duration_enough(columns[DURATION_INDEX]))


def filter_by_duration_average_rating_and_server(channel, method, properties, body):
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping receiving matches")
        channel.stop_consuming()
        print("Sending sentinel to client to notify that all matches ids has been sended")
        send_sentinel_to_queue(channel, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)
    else:
        matches_ids = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                matches_ids.append(columns[TOKEN_INDEX])
        if (len(matches_ids) > 0):
            send_matches_ids(channel, LONG_MATCHES_TO_CLIENT_QUEUE_NAME, matches_ids)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()

    channel.exchange_declare(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout')
    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        queue=private_queue_name
    )

    channel.queue_declare(queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=filter_by_duration_average_rating_and_server,
    )

    print('Starting to receive matches to filter')
    channel.start_consuming()
    connection.close()

if __name__ == '__main__':
    main()
