#!/usr/bin/env python3
import csv
import threading

from config.envvars import CHUCKSIZE_IN_LINES_KEY, ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY, ENTRY_MATCH_DURATION_INDEX_KEY, ENTRY_MATCH_LADDER_INDEX_KEY, ENTRY_MATCH_MAP_INDEX_KEY, ENTRY_MATCH_MIRROR_INDEX_KEY, ENTRY_MATCH_SERVER_INDEX_KEY, ENTRY_MATCH_TOKEN_INDEX_KEY, ENTRY_PLAYER_CIV_INDEX_KEY, ENTRY_PLAYER_MATCH_INDEX_KEY, ENTRY_PLAYER_RATING_INDEX_KEY, ENTRY_PLAYER_TOKEN_INDEX_KEY, ENTRY_PLAYER_WINNER_INDEX_KEY, MATCHES_FILE_NAME_KEY, PLAYERS_FILE_NAME_KEY, get_config_param, get_config_params
from communications.constants import MATCHES_FANOUT_EXCHANGE_NAME, PLAYERS_FANOUT_EXCHANGE_NAME
from communications.rabbitmq_interface import ExchangeInterface, RabbitMQConnection
from logger.logger import Logger

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
        ENTRY_PLAYER_TOKEN_INDEX_KEY,
        ENTRY_PLAYER_MATCH_INDEX_KEY,
        ENTRY_PLAYER_RATING_INDEX_KEY,
        ENTRY_PLAYER_WINNER_INDEX_KEY,
        ENTRY_PLAYER_CIV_INDEX_KEY,
    ], logger)


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
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_TOKEN_INDEX_KEY]], # TODO actualizar el grafico para mostrar que esto tambien esta en las colas ahora
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
                        get_config_param(MATCHES_FILE_NAME_KEY, logger),
                        get_line_string_for_matches)
    logger.info("Finished sending matches to server")
    connection.close()


def send_players():
    connection = RabbitMQConnection()
    exchange = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)

    logger.info("Starting to send players to server")
    send_file_in_chunks(exchange,
                        get_config_param(PLAYERS_FILE_NAME_KEY, logger),
                        get_line_string_for_players)
    logger.info("Finished sending players to server")
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

if __name__ == '__main__':
    main()




