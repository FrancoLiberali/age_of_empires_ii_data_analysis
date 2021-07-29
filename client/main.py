#!/usr/bin/env python3
import csv
import threading
import time

from config.envvars import CHUCKSIZE_IN_LINES_KEY, ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY, ENTRY_MATCH_DURATION_INDEX_KEY, ENTRY_MATCH_LADDER_INDEX_KEY, ENTRY_MATCH_MAP_INDEX_KEY, ENTRY_MATCH_MIRROR_INDEX_KEY, ENTRY_MATCH_SERVER_INDEX_KEY, ENTRY_MATCH_TOKEN_INDEX_KEY, ENTRY_PLAYER_CIV_INDEX_KEY, ENTRY_PLAYER_MATCH_INDEX_KEY, ENTRY_PLAYER_RATING_INDEX_KEY, ENTRY_PLAYER_TOKEN_INDEX_KEY, ENTRY_PLAYER_WINNER_INDEX_KEY, MATCHES_FILE_NAME_KEY, PLAYERS_FILE_NAME_KEY, get_config_param, get_config_params
from communications.constants import AUTHORIZED_CODE, AUTHORIZE_RESPONSE_AUTHORIZATION_INDEX, CLIENTS_REQUESTS_QUEUE_NAME, CLIENTS_RESPONSES_EXCHANGE_NAME, CLIENT_REQUEST_SEPARATOR, CLIENT_REQUEST_TYPE_AUTHORIZE, MATCHES_FANOUT_EXCHANGE_NAME, PLAYERS_FANOUT_EXCHANGE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection
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

def send_data_to_server():
    send_matches_th = threading.Thread(
        target=send_matches)
    send_matches_th.start()

    send_players_th = threading.Thread(
        target=send_players)
    send_players_th.start()

    send_matches_th.join()
    send_players_th.join()

def get_authorize_request(response_queue_name):
    request_list = [response_queue_name, CLIENT_REQUEST_TYPE_AUTHORIZE]
    return CLIENT_REQUEST_SEPARATOR.join(request_list)


def get_handle_authorize_response_function(authorized):
    def handle_authorize_response(queue, received_string, _, __):
        response = received_string.split(CLIENT_REQUEST_SEPARATOR)
        if response[AUTHORIZE_RESPONSE_AUTHORIZATION_INDEX] == AUTHORIZED_CODE:
            logger.info("Authorized to send dataset")
            authorized[0] = True
        else:
            logger.info("Not authorized to send dataset")
        queue.stop_consuming()
    return handle_authorize_response

TIME_BETWEEN_AUTHORIZATION_RETRY_IN_SECONDS = 15

def main():
    connection = RabbitMQConnection()

    response_queue = QueueInterface.newPrivate(connection)
    response_exchange = ExchangeInterface.newDirect(
        connection, CLIENTS_RESPONSES_EXCHANGE_NAME)
    response_queue.bind(response_exchange, routing_key=response_queue.name)
    requests_queue = QueueInterface(connection, CLIENTS_REQUESTS_QUEUE_NAME)
    while True:
        logger.info("Trying to send dataset")
        requests_queue.send_string(
            get_authorize_request(response_queue.name)
        )
        authorized = [False]
        response_queue.consume(get_handle_authorize_response_function(authorized))
        if authorized[0]:
            send_data_to_server()
            connection.close()
            break
        time.sleep(TIME_BETWEEN_AUTHORIZATION_RETRY_IN_SECONDS)

if __name__ == '__main__':
    main()




