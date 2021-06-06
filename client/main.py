#!/usr/bin/env python3
import pika
import csv

from common.constants import STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_IDS_SEPARATOR, \
    CLIENT_TO_LONG_MATCHES_QUEUE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE

MATCHES_CSV_FILE = '/matches.csv'
CHUCKSIZE_IN_LINES = 100

TOKEN_INDEX = 0 # TODO envvar
AVERAGE_RATING_INDEX = 5  # TODO envvar
SERVER_INDEX = 9  # TODO envvar
DURATION_INDEX = 10  # TODO envvar

def send_chunk(channel, chunk):
    if len(chunk) > 0:
        chunk_string = STRING_LINE_SEPARATOR.join(chunk)
        channel.basic_publish(
            exchange='',
            routing_key=CLIENT_TO_LONG_MATCHES_QUEUE_NAME,
            body=chunk_string.encode(STRING_ENCODING)
        )

def get_line_string(line_list):
    return STRING_COLUMN_SEPARATOR.join(
        [
            line_list[TOKEN_INDEX],
            line_list[AVERAGE_RATING_INDEX],
            line_list[SERVER_INDEX],
            line_list[DURATION_INDEX],
        ]
    )
    
def send_sentinel(channel):
    # TODO pasar a appeden string to queue o algo asi
    channel.basic_publish(
        exchange='',
        routing_key=CLIENT_TO_LONG_MATCHES_QUEUE_NAME,
        body=SENTINEL_MESSAGE.encode(STRING_ENCODING)
    )


def get_print_matches_ids_function(connection, matches_ids):
        # function currying in python
    def print_matches_ids(channel, method, properties, body):
        received_string = body.decode(STRING_ENCODING)
        if received_string == SENTINEL_MESSAGE:
            print("Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son:")
            print(', '.join(matches_ids))
            channel.stop_consuming()
            connection.close()
        else:
            for match_id in received_string.split(MATCHES_IDS_SEPARATOR):
                matches_ids.append(match_id)
    return print_matches_ids

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_LONG_MATCHES_QUEUE_NAME)
    channel.queue_declare(queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    chunk = []
    with open(MATCHES_CSV_FILE) as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i == 0:
                # file header
                continue
            if (i % CHUCKSIZE_IN_LINES == 0 and i > 0):
                send_chunk(channel, chunk)
                del chunk[:] # delete from memory
            chunk.append(get_line_string(line))
        send_chunk(channel, chunk)
        send_sentinel(channel)

    matches_ids = []
    channel.basic_consume(
        queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME,
        on_message_callback=get_print_matches_ids_function(
            connection, matches_ids),
        auto_ack=True
    )
    channel.start_consuming()

if __name__ == '__main__':
    main()




