#!/usr/bin/env python3
import csv
import pika
import threading

from communications.constants import CLIENT_TO_WEAKER_WINNER_QUEUE_NAME, STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_IDS_SEPARATOR, \
    CLIENT_TO_LONG_MATCHES_QUEUE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME

MATCHES_CSV_FILE = '/matches.csv'
MATCH_PLAYERS_CSV_FILE = '/match_players.csv'
CHUCKSIZE_IN_LINES = 100

TOKEN_INDEX = 0 # TODO envvar
AVERAGE_RATING_INDEX = 5  # TODO envvar
SERVER_INDEX = 9  # TODO envvar
DURATION_INDEX = 10  # TODO envvar

def send_chunk(channel, queue_name, chunk):
    if len(chunk) > 0:
        chunk_string = STRING_LINE_SEPARATOR.join(chunk)
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=chunk_string.encode(STRING_ENCODING)
        )

def get_line_string_for_long_matches(line_list):
    return STRING_COLUMN_SEPARATOR.join(
        [
            line_list[TOKEN_INDEX],
            line_list[AVERAGE_RATING_INDEX],
            line_list[SERVER_INDEX],
            line_list[DURATION_INDEX],
        ]
    )

MATCH_INDEX = 1 # TODO envvar
RATING_INDEX = 2  # TODO envvar
WINNER_INDEX = 6  # TODO envvar

def get_line_string_for_weaker_winner(line_list):
    return STRING_COLUMN_SEPARATOR.join(
        [
            line_list[MATCH_INDEX],
            line_list[RATING_INDEX],
            line_list[WINNER_INDEX],
        ]
    )

def send_sentinel(channel, queue_name):
    # TODO pasar a appeden string to queue o algo asi
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=SENTINEL_MESSAGE.encode(STRING_ENCODING)
    )
    
def get_print_matches_ids_function(matches_ids, message):
    # function currying in python
    def print_matches_ids(channel, method, properties, body):
        received_string = body.decode(STRING_ENCODING)
        if received_string == SENTINEL_MESSAGE:
            print(f"{message}{', '.join(matches_ids)}")
            channel.stop_consuming()
        else:
            for match_id in received_string.split(MATCHES_IDS_SEPARATOR):
                matches_ids.append(match_id)
    return print_matches_ids


def get_matches_ids(channel, queue_name, message):
    matches_ids = []
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=get_print_matches_ids_function(
            matches_ids,
            message
        ),
        auto_ack=True
    )
    channel.start_consuming()


def send_file_in_chunks(channel, queue_name, file_path, get_line_string_function):
    chunk = []
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i == 0:
                # file header
                continue
            if (i % CHUCKSIZE_IN_LINES == 0 and i > 0):
                send_chunk(channel, queue_name, chunk)
                del chunk[:]  # delete from memory
            chunk.append(get_line_string_function(line))
        send_chunk(channel, queue_name, chunk)
        send_sentinel(channel, queue_name)


def request_long_matches():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_LONG_MATCHES_QUEUE_NAME)
    channel.queue_declare(queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    send_file_in_chunks(channel,
                        CLIENT_TO_LONG_MATCHES_QUEUE_NAME,
                        MATCHES_CSV_FILE,
                        get_line_string_for_long_matches)

    get_matches_ids(channel, LONG_MATCHES_TO_CLIENT_QUEUE_NAME,
                    "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: ")
    connection.close()


def request_weaker_winner():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME)
    channel.queue_declare(queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    send_file_in_chunks(channel,
                        CLIENT_TO_WEAKER_WINNER_QUEUE_NAME,
                        MATCH_PLAYERS_CSV_FILE,
                        get_line_string_for_weaker_winner)

    get_matches_ids(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME,
                    "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: ")
    connection.close()

def main():
    long_matches_client = threading.Thread(
        target=request_long_matches)
    long_matches_client.start()
    
    weaker_winner_client = threading.Thread(
        target=request_weaker_winner)
    weaker_winner_client.start()

    long_matches_client.join()
    weaker_winner_client.join()

if __name__ == '__main__':
    main()




