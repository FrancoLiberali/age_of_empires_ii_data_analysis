#!/usr/bin/env python3
from communications.rabbitmq_interface import send_sentinel_to_exchange, send_string_to_exchange
import csv
import pika
import threading

from communications.constants import MATCHES_FANOUT_EXCHANGE_NAME, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_IDS_SEPARATOR, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME

MATCHES_CSV_FILE = '/matches.csv'
MATCH_PLAYERS_CSV_FILE = '/match_players.csv'
CHUCKSIZE_IN_LINES = 100

ENTRY_MATCH_TOKEN_INDEX = 0 # TODO envvar
ENTRY_MATCH_AVERAGE_RATING_INDEX = 5  # TODO envvar
ENTRY_MATCH_SERVER_INDEX = 9  # TODO envvar
ENTRY_MATCH_DURATION_INDEX = 10  # TODO envvar
ENTRY_MATCH_LADDER_INDEX = 3  # TODO envvar
ENTRY_MATCH_MAP_INDEX = 6  # TODO envvar
ENTRY_MATCH_MIRROR_INDEX = 2  # TODO envvar

ENTRY_PLAYER_MATCH_INDEX = 1  # TODO envvar
ENTRY_PLAYER_RATING_INDEX = 2  # TODO envvar
ENTRY_PLAYER_WINNER_INDEX = 6  # TODO envvar
ENTRY_PLAYER_CIV_INDEX = 4 # TODO envvar

def get_print_matches_ids_function(matches_ids, message):
    # function currying in python
    def print_matches_ids(channel, method, properties, body):
        received_string = body.decode(STRING_ENCODING)
        if received_string == SENTINEL_MESSAGE:
            print(message)
            print('\n'.join(matches_ids))
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


def send_chunk(channel, exchange_name, chunk):
    if len(chunk) > 0:
        chunk_string = STRING_LINE_SEPARATOR.join(chunk)
        send_string_to_exchange(channel, exchange_name, chunk_string)


def get_line_string_for_matches(line_list):
    return STRING_COLUMN_SEPARATOR.join(
        [
            line_list[ENTRY_MATCH_TOKEN_INDEX],
            line_list[ENTRY_MATCH_AVERAGE_RATING_INDEX],
            line_list[ENTRY_MATCH_SERVER_INDEX],
            line_list[ENTRY_MATCH_DURATION_INDEX],
            line_list[ENTRY_MATCH_LADDER_INDEX],
            line_list[ENTRY_MATCH_MAP_INDEX],
            line_list[ENTRY_MATCH_MIRROR_INDEX],
        ]
    )


def get_line_string_for_players(line_list):
    return STRING_COLUMN_SEPARATOR.join(
        [
            line_list[ENTRY_PLAYER_MATCH_INDEX],
            line_list[ENTRY_PLAYER_RATING_INDEX],
            line_list[ENTRY_PLAYER_WINNER_INDEX],
            line_list[ENTRY_PLAYER_CIV_INDEX],
        ]
    )


def send_file_in_chunks(channel, exchange_name, file_path, get_line_string_function):
    chunk = []
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i == 0:
                # file header
                continue
            if (i % CHUCKSIZE_IN_LINES == 0 and i > 0):
                send_chunk(channel, exchange_name, chunk)
                del chunk[:]  # delete from memory
            chunk.append(get_line_string_function(line))
        send_chunk(channel, exchange_name, chunk)
        send_sentinel_to_exchange(channel, exchange_name)


def send_matches():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout')

    print(f"Starting to send matches to server")
    send_file_in_chunks(channel,
                        MATCHES_FANOUT_EXCHANGE_NAME,
                        MATCHES_CSV_FILE,
                        get_line_string_for_matches)
    print(f"Finished sending matches to server")
    connection.close()


def receive_long_matches_ids():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    print(f"Starting to receive ids of long matches replied")
    get_matches_ids(channel, LONG_MATCHES_TO_CLIENT_QUEUE_NAME,
                    "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: ")
    connection.close()


def send_players():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(
        exchange=PLAYERS_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout')

    print(f"Starting to send players to server")
    send_file_in_chunks(channel,
                        PLAYERS_FANOUT_EXCHANGE_NAME,
                        MATCH_PLAYERS_CSV_FILE,
                        get_line_string_for_players)
    print(f"Finished sending matches to server")
    connection.close()


def receive_weaker_winner_matches_ids():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    print(f"Starting to receive ids of matches with weaker winner replied")
    get_matches_ids(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME,
                    "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: ")
    connection.close()

def main():
    send_matches_th = threading.Thread(
        target=send_matches)
    send_matches_th.start()
    
    send_players_th = threading.Thread(
        target=send_players)
    send_players_th.start()

    send_matches_th.join()
    send_players_th.join()

    # receive_long_matches_ids_th = threading.Thread(
        # target=receive_long_matches_ids)
    # receive_long_matches_ids_th.start()

    # receive_weaker_winner_matches_ids_th = threading.Thread(
        # target=receive_weaker_winner_matches_ids)
    # receive_weaker_winner_matches_ids_th.start()
    # 
    # receive_long_matches_ids_th.join()
    # receive_weaker_winner_matches_ids_th.join()

if __name__ == '__main__':
    main()




