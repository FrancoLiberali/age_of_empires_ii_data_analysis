#!/usr/bin/env python3
import csv
import threading

from config.envvars import CHUCKSIZE_IN_LINES_KEY, ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY, ENTRY_MATCH_DURATION_INDEX_KEY, ENTRY_MATCH_LADDER_INDEX_KEY, ENTRY_MATCH_MAP_INDEX_KEY, ENTRY_MATCH_MIRROR_INDEX_KEY, ENTRY_MATCH_SERVER_INDEX_KEY, ENTRY_MATCH_TOKEN_INDEX_KEY, ENTRY_PLAYER_CIV_INDEX_KEY, ENTRY_PLAYER_MATCH_INDEX_KEY, ENTRY_PLAYER_RATING_INDEX_KEY, ENTRY_PLAYER_WINNER_INDEX_KEY, get_config_param, get_config_params
from communications.constants import MATCHES_FANOUT_EXCHANGE_NAME, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, \
    WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import QueueInterface, ExchangeInterface, RabbitMQConnection, split_columns_into_list
from logger.logger import Logger

MATCHES_CSV_FILE = '/matches.csv'
MATCH_PLAYERS_CSV_FILE = '/match_players.csv'

logger = Logger(True)
CHUCKSIZE_IN_LINES = get_config_param(CHUCKSIZE_IN_LINES_KEY, logger)

ENTRY_MATCHES_INDEXES = get_config_params([
        ENTRY_MATCH_TOKEN_INDEX_KEY,
        ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY,
        ENTRY_MATCH_SERVER_INDEX_KEY,
        ENTRY_MATCH_DURATION_INDEX_KEY,
        ENTRY_MATCH_LADDER_INDEX_KEY,
        ENTRY_MATCH_MAP_INDEX_KEY,
        ENTRY_MATCH_MIRROR_INDEX_KEY,
    ], logger)

ENTRY_PLAYERS_INDEXES = get_config_params([
        ENTRY_PLAYER_MATCH_INDEX_KEY,
        ENTRY_PLAYER_RATING_INDEX_KEY,
        ENTRY_PLAYER_WINNER_INDEX_KEY,
        ENTRY_PLAYER_CIV_INDEX_KEY,
    ], logger)


def get_receive_matches_ids_function(matches_ids):
    # function currying in python
    def receive_matches_ids(queue, received_string, _):
        for match_id in split_columns_into_list(received_string):
            matches_ids.append(match_id)
    return receive_matches_ids

def get_print_matches_ids_function(matches_ids, message):
    # function currying in python
    def print_matches_ids(_):
        logger.info(message)
        logger.info('\n'.join(matches_ids))
    return print_matches_ids


def get_matches_ids(queue, message):
    matches_ids = []
    queue.consume(
        get_receive_matches_ids_function(
            matches_ids
        ),
        on_sentinel_callback=get_print_matches_ids_function(
            matches_ids,
            message
        ),
    )


def get_line_string_for_matches(line_list):
    return [
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_TOKEN_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_SERVER_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_DURATION_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_LADDER_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_MAP_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_MIRROR_INDEX_KEY]],
    ]


def get_line_string_for_players(line_list):
    return [
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_MATCH_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_RATING_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_WINNER_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_CIV_INDEX_KEY]],
    ]


def send_file_in_chunks(exchange, file_path, get_columns_function):
    chunk = []
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i == 0:
                # file header
                continue
            if (i % CHUCKSIZE_IN_LINES == 0 and i > 0):
                exchange.send_list_of_columns(chunk)
                del chunk[:]  # delete from memory
            chunk.append(get_columns_function(line))
        exchange.send_list_of_columns(chunk)
        exchange.send_sentinel()


def send_matches():
    connection = RabbitMQConnection()
    exchange = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)

    logger.info("Starting to send matches to server")
    send_file_in_chunks(exchange,
                        MATCHES_CSV_FILE,
                        get_line_string_for_matches)
    logger.info("Finished sending matches to server")
    connection.close()


def receive_long_matches_ids():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive ids of long matches replied")
    get_matches_ids(
        queue,
        "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: "
    )
    connection.close()


def send_players():
    connection = RabbitMQConnection()
    exchange = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)

    logger.info("Starting to send players to server")
    send_file_in_chunks(exchange,
                        MATCH_PLAYERS_CSV_FILE,
                        get_line_string_for_players)
    logger.info("Finished sending players to server")
    connection.close()


def receive_weaker_winner_matches_ids():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive ids of matches with weaker winner replied")
    get_matches_ids(
        queue,
        "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: "
    )
    connection.close()


def receive_winner_rate_of_all_civs(queue, received_string, _):
    logger.info("Porcentaje de victorias por civilización en partidas 1v1(ladder == RM_1v1) con civilizaciones diferentes en mapa arena son:")
    logger.info(received_string)
    queue.stop_consuming()


def receive_winner_rate_by_civ():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive to winner rate by civ replied")
    queue.consume(receive_winner_rate_of_all_civs)
    connection.close()


def receive_top_5_civs_used(queue, received_string, _):
    logger.info("Top 5 civilizaciones más usadas por pro players(rating > 2000) en team games (ladder == RM_TEAM) en mapa islands son: ")
    logger.info(received_string)
    queue.stop_consuming()

def receive_top_5_used_civs():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive to top 5 civs used replied")
    queue.consume(receive_top_5_civs_used)
    connection.close()


def main():
    send_matches_th = threading.Thread(
        target=send_matches)
    send_matches_th.start()

    send_players_th = threading.Thread(
        target=send_players)
    send_players_th.start()

    receive_long_matches_ids_th = threading.Thread(
        target=receive_long_matches_ids)
    receive_long_matches_ids_th.start()

    receive_weaker_winner_matches_ids_th = threading.Thread(
        target=receive_weaker_winner_matches_ids)
    receive_weaker_winner_matches_ids_th.start()

    receive_winner_rate_by_civ_th = threading.Thread(
        target=receive_winner_rate_by_civ)
    receive_winner_rate_by_civ_th.start()

    receive_top_5_used_civs_th = threading.Thread(
        target=receive_top_5_used_civs)
    receive_top_5_used_civs_th.start()

    send_matches_th.join()
    send_players_th.join()
    receive_long_matches_ids_th.join()
    receive_weaker_winner_matches_ids_th.join()
    receive_winner_rate_by_civ_th.join()
    receive_top_5_used_civs_th.join()

if __name__ == '__main__':
    main()




