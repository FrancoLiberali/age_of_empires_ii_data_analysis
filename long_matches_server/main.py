#!/usr/bin/env python3
import pika
from datetime import datetime, time

from communications.constants import STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_IDS_SEPARATOR, \
    CLIENT_TO_LONG_MATCHES_QUEUE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE

TOKEN_INDEX = 0
AVERAGE_RATING_INDEX = 1
SERVER_INDEX = 2
DURATION_INDEX = 3

MINIMUM_AVERAGE_RATING = 2000 # TODO envvar
MINIMUM_DURATION = time(hour=2)  # TODO envvar
REQUIRED_SERVERS = ['koreacentral', 'southeastasia', 'eastus']  # TODO envvar
DURATION_FORMAT = '%H:%M:%S'

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


def respond(response_channel, matches_ids):
    response_string = MATCHES_IDS_SEPARATOR.join(matches_ids)
    response_channel.basic_publish(
        exchange='',
        routing_key=LONG_MATCHES_TO_CLIENT_QUEUE_NAME,
        body=response_string.encode(STRING_ENCODING)
    )

# TODO usar tambien el client esto
def send_sentinel(channel, queue):
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=SENTINEL_MESSAGE.encode(STRING_ENCODING)
    )

def filter_by_duration_average_rating_and_server(channel, method, properties, body):
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping consuming")
        channel.stop_consuming()
        send_sentinel(channel, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)
    else:
        matches_ids = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                matches_ids.append(columns[TOKEN_INDEX])
        if (len(matches_ids) > 0):
            respond(channel, matches_ids)

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_LONG_MATCHES_QUEUE_NAME)
    channel.queue_declare(queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    channel.basic_consume(
        queue=CLIENT_TO_LONG_MATCHES_QUEUE_NAME,
        on_message_callback=filter_by_duration_average_rating_and_server,
        auto_ack=True # TODO sacar esto
    )

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()

if __name__ == '__main__':
    main()
